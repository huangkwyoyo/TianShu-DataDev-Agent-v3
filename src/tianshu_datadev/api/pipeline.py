"""Pipeline——确定性串联全部组件的执行流水线。

所有步骤使用确定性实现，不需要真实 LLM 或生产数据库。
每次调用独立创建组件实例，无状态泄漏。
API 只返回 artifact 引用和结构化摘要。
"""

from __future__ import annotations

import hashlib
import logging
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from tianshu_datadev.api.templates import TEMPLATES
from tianshu_datadev.artifacts.contract_extractor import DataTransformContractExtractor
from tianshu_datadev.artifacts.packager import PackageInputs, ReviewPackageBuilder
from tianshu_datadev.developer_spec.models import (
    ParsedDeveloperSpec,
    StrictModel,
)
from tianshu_datadev.developer_spec.parser import DeveloperSpecParser
from tianshu_datadev.developer_spec.source_manifest import build_manifest_from_spec
from tianshu_datadev.llm.models import LlmResponse, LlmTraceNode
from tianshu_datadev.planning.cross_validator import cross_validate
from tianshu_datadev.planning.program_factory import (
    build_sql_program,
    build_sql_program_from_chain,
    build_sql_program_from_compute_steps,
)
from tianshu_datadev.planning.relationship_planner import RelationshipPlanner
from tianshu_datadev.planning.spec_enricher import SpecEnricher
from tianshu_datadev.planning.sql_build_plan import SqlBuildPlan, SqlBuildPlanBuilder
from tianshu_datadev.planning.sql_program import SqlProgram
from tianshu_datadev.sql.compiler import DuckDbSqlCompiler
from tianshu_datadev.sql.executor import DuckDBExecutor
from tianshu_datadev.sql.models import (
    CompiledSql,
    ExecutionStatus,
    ExecutionTrace,
    ResultSummary,
    SqlArtifact,
)
from tianshu_datadev.sql.validator import SqlBuildPlanValidator

if TYPE_CHECKING:
    from tianshu_datadev.artifacts.models import DataTransformContractLite, DataTransformContractV1
    from tianshu_datadev.developer_spec.models import OpenQuestion, ParseWarning, SourceManifest
    from tianshu_datadev.llm.adapters.base import ProviderAdapter
    from tianshu_datadev.planning.relationship_hypothesis import RelationshipHypothesis
    from tianshu_datadev.planning.sql_build_plan import SqlBuildPlan
    from tianshu_datadev.planning.sql_program import SqlProgram
    from tianshu_datadev.spark.compiler import SparkCompileResult
    from tianshu_datadev.spark.annotations import AnnotatedSparkPlan
    from tianshu_datadev.spark.models import SparkPlan
    from tianshu_datadev.spark.plan_comparator import PlanComparisonReport
    from tianshu_datadev.spark.snapshot import SnapshotBuilder, SnapshotManifest, SnapshotSourceProvider
    from tianshu_datadev.sql.models import CompiledSql, ExecutionTrace, ResultSummary

from tianshu_datadev.spark.orchestrator import SparkPipelineStage

logger = logging.getLogger(__name__)


def _summarize_open_questions(
    questions: list[OpenQuestion],
) -> list[dict]:
    """将 OpenQuestion 列表转换为 API 摘要格式。"""
    return [
        {
            "question_id": q.question_id,
            "source": q.source,
            "description": q.description,
            "blocking": q.blocking,
        }
        for q in questions
    ]


def _summarize_warnings(warnings: list[ParseWarning]) -> list[dict]:
    """将 ParseWarning 列表转换为 API 摘要格式。"""
    result = []
    for w in warnings:
        severity = w.severity.value if hasattr(w.severity, "value") else str(w.severity)
        result.append({
            "warning_id": w.warning_id,
            "message": w.message,
            "severity": severity,
        })
    return result


def _auto_table_mapping(spec: ParsedDeveloperSpec) -> dict[str, str]:
    """从 DeveloperSpec 的 source_tables 自动构建 table_mapping（别名 → 物理表名）。

    当 API 请求未显式提供 table_mapping 时，用此函数补齐，
    确保编译器能将 table_ref（如 "ue"）解析为物理表名（如 "dwd.user_events"）。

    Args:
        spec: 已解析的 DeveloperSpec

    Returns:
        {alias: physical_table_name} 映射字典
    """
    mapping: dict[str, str] = {}
    for t in spec.input_tables:
        if t.table_alias and t.source_table:
            mapping[t.table_alias] = str(t.source_table)
    return mapping


# ════════════════════════════════════════════
# Phase 9A1: PipelineArtifactBundle——中间产物导出模型
# ════════════════════════════════════════════


class PipelineArtifactBundle(StrictModel):
    """Pipeline 中间产物导出包——将 _results 缓存中的结构化 artifact 暴露给下游消费。

    由 Pipeline.export_artifacts(request_id) 创建。
    不同 Pipeline 路径产出的 artifact 不同——字段为 None 表示该路径未产出对应产物。

    字段说明：
    - request_id: Pipeline 请求 ID（与 run_all/execute 返回值一致）
    - spec_hash: 来源 DeveloperSpec 的 hash
    - sql_build_plan: SQL Pipeline 产出的 SqlBuildPlan（build_plan/execute/run_all 路径均产出）
    - data_transform_contract: 数据转换契约（run_all 路径产出——单表为 DataTransformContractLite，
      多语句为 DataTransformContractV1；execute/build_plan 路径不产出）
    - compiled_sql: DuckDB Compiler 编译产物（execute/run_all 路径产出）
    - execution_trace: DuckDB 执行追踪（execute/run_all 成功路径产出）
    - result_summary: 执行结果摘要（execute/run_all 成功路径产出）
    """

    request_id: str
    spec_hash: str = ""
    sql_build_plan: SqlBuildPlan | None = None
    # 接受 Lite（extract(plan) 产出）和 V1（extract_v1(sql_program) 产出）两种类型
    data_transform_contract: DataTransformContractLite | DataTransformContractV1 | None = None
    compiled_sql: CompiledSql | None = None
    execution_trace: ExecutionTrace | None = None
    result_summary: ResultSummary | None = None
    # ── Phase 9B-P0: Snapshot 集成 ──
    snapshot_manifest: SnapshotManifest | None = None
    # ── Phase 10: Case06 SqlProgram 多语句 DAG ──
    sql_program: SqlProgram | None = None
    # ── Final Hardening: SqlProgram 执行 cleanup 状态 ──
    program_cleanup_status: str | None = None   # "success" | "partial_failure"
    program_cleanup_error: str | None = None     # cleanup 阶段的错误信息（成功时为空）


class Pipeline:
    """执行流水线——确定性串联全部 6 个组件。

    工作流程：
      parse_only: Parser → 摘要
      build_plan:  Parser → Builder → Validator → 摘要
      execute:     Parser → Builder → Validator → Compiler → Executor → 摘要
      run_all:     Parser → Builder → Validator → Compiler → Executor → Contract → Packager → 摘要
      get_package: 内存存储 → 摘要

    内部维护 _results 和 _packages 字典作为临时存储。
    每次 API 调用独立创建组件实例，无状态泄漏。
    """

    def __init__(
        self,
        base_output_dir: str = "generated/review_packages",
        adapter: ProviderAdapter | None = None,
        # ── Phase 9B-P0: Snapshot 集成（可选）──
        snapshot_builder: SnapshotBuilder | None = None,
        snapshot_provider: SnapshotSourceProvider | None = None,
        # ── Phase 9C-R16: 默认 table_paths 回退——E2E 环境无需前端传参 ──
        default_table_paths: dict[str, str] | None = None,
        # ── NYC 数据仓库 DuckDB 文件路径——模板引用 gold/silver schema 表时使用 ──
        duckdb_path: str | None = None,
        # ── Phase 8: SparkDeveloperService 注入（可选）──
        developer_service=None,  # SparkDeveloperService | None，None → SKIPPED
    ):
        """初始化流水线。

        Args:
            base_output_dir: ReviewPackage 输出根目录
            adapter: LLM Provider 适配器——None 时全链路确定性运行（Fake 模式），
                     注入后 RelationshipPlanner + SpecEnricher 均走 LLM 推断。
            default_table_paths: 默认表名→CSV 路径映射——当 API 调用未显式传入
                                 table_paths 时使用此回退值。E2E 测试环境用。
            duckdb_path: 外部 DuckDB 数据库文件路径——ATTACH 后自动创建 schema VIEW
                         桥接，使模板引用的 gold/silver 表可直接查询
        """
        self._base_output_dir = base_output_dir
        self._results: dict[str, dict] = {}  # request_id → 内部产物
        self._packages: dict[str, object] = {}  # request_id → ReviewPackageManifest
        self._timestamps: dict[str, float] = {}  # request_id → 写入时间戳（用于 TTL 过期清理）
        self._ttl_seconds: int = 1800  # 缓存过期时间（秒），默认 30 分钟
        # adapter=None 时退化为纯规则/显式声明模式（确定性）
        self._relationship_planner = RelationshipPlanner(adapter=adapter)
        self._spec_enricher = SpecEnricher(adapter=adapter)
        # ── Phase 9B-P0: Snapshot 集成（可选）──
        self._snapshot_builder = snapshot_builder
        self._snapshot_provider = snapshot_provider
        # ── Phase 9C-R16: table_paths 回退值 ──
        self._default_table_paths = default_table_paths or {}
        # ── 外部 DuckDB 数据库路径 ──
        self._duckdb_path = duckdb_path
        # ── Phase 8: SparkDeveloperService 注入 ──
        self._spark_developer_service = developer_service
        # ── Spark 阶段独立触发——上下文缓存 ──
        self._spark_contexts: dict[str, SparkStageContext] = {}
        # ── LLM 调用追踪（request-scoped cache）──
        self._llm_traces: dict[str, dict[str, LlmTraceNode]] = {}
        # request_id → {node_name: LlmTraceNode}

    # ── 缓存生命周期管理 ──────────────────────────────

    def _store_result(self, request_id: str, data: dict) -> None:
        """缓存中间结果并记录写入时间戳——供 TTL 过期清理使用。"""
        self._results[request_id] = data
        self._timestamps[request_id] = time.monotonic()

    def _store_package(self, request_id: str, package: object) -> None:
        """缓存打包结果并记录写入时间戳——供 TTL 过期清理使用。"""
        self._packages[request_id] = package
        self._timestamps[request_id] = time.monotonic()

    def _purge_expired(self) -> int:
        """清理所有超过 TTL 的缓存条目。

        遍历 _timestamps 字典，移除 _results 和 _packages 中的过期条目。
        每次公共方法入口调用——惰性清理，零额外定时器开销。

        Returns:
            清理的条目数
        """
        now = time.monotonic()
        expired_ids = [
            rid for rid, ts in self._timestamps.items()
            if now - ts > self._ttl_seconds
        ]
        for rid in expired_ids:
            self._results.pop(rid, None)
            self._packages.pop(rid, None)
            self._timestamps.pop(rid, None)
            self._llm_traces.pop(rid, None)
            self._spark_contexts.pop(rid, None)
        if expired_ids:
            logger.debug("TTL 过期清理完成，移除 %d 条缓存", len(expired_ids))
        return len(expired_ids)

    # ── LLM 调用追踪 ───────────────────────────────

    def _record_llm_trace(self, request_id: str, response: LlmResponse) -> None:
        """从 LlmResponse 记录单次 LLM 调用的诊断元数据。

        同一 node_name 多次调用 → 保留最后一次（不聚合）。
        仅在 request-scoped cache 中存储。

        Args:
            request_id: Pipeline 请求 ID
            response: LLM Gateway 返回的 LlmResponse
        """
        if request_id not in self._llm_traces:
            self._llm_traces[request_id] = {}

        # 从 LlmResponse.task 映射到 node_name
        node_name = response.task  # 直接使用 task 字段——值与 node_name 合法值一致

        # 从 validation_status 映射到 trace status
        if response.validation_status == "valid":
            status = "valid"
        elif response.validation_status == "invalid":
            status = "invalid"
        else:
            status = "skipped"

        trace = LlmTraceNode(
            node_name=node_name,
            model=getattr(response, "model", "") or "fake",
            token_usage=response.token_usage or {},
            latency_ms=response.latency_ms,
            status=status,
            error_type=None,  # 当前 LlmResponse 没有 error_type 字段——预留
        )
        self._llm_traces[request_id][node_name] = trace

    def _get_llm_traces(self, request_id: str) -> dict[str, LlmTraceNode] | None:
        """获取指定 request_id 的 LLM 调用追踪数据。

        Args:
            request_id: Pipeline 请求 ID

        Returns:
            {node_name: LlmTraceNode} 字典，无数据时返回 None
        """
        traces = self._llm_traces.get(request_id)
        if not traces:
            return None
        return dict(traces)

    def _record_trace(
        self,
        request_id: str,
        node_name: str,
        model: str = "deterministic",
        token_usage: dict[str, int] | None = None,
        latency_ms: int = 0,
        status: str = "skipped",
        error_type: str | None = None,
    ) -> None:
        """记录单次管线阶段执行的诊断元数据——轻量版（不依赖 LlmResponse）。

        Fake 模式下记录确定性执行的阶段信息（model="deterministic", status="skipped"），
        真实 LLM 模式下可记录实际 token 消耗和延迟。
        """
        if request_id not in self._llm_traces:
            self._llm_traces[request_id] = {}

        trace = LlmTraceNode(
            node_name=node_name,
            model=model,
            token_usage=token_usage or {},
            latency_ms=latency_ms,
            status=status,
            error_type=error_type,
        )
        self._llm_traces[request_id][node_name] = trace

    # ── 核心管线方法 ──────────────────────────────

    @staticmethod
    def _gen_request_id(spec: ParsedDeveloperSpec) -> str:
        """从 spec_hash 生成确定性 request_id。"""
        return f"req_{spec.spec_hash[:12]}"

    def _enrich_and_plan(
        self,
        spec: ParsedDeveloperSpec,
        manifest: SourceManifest,
        table_mapping: dict | None = None,
    ) -> tuple[ParsedDeveloperSpec, RelationshipHypothesis | None, list[OpenQuestion], dict[str, str]]:
        """统一入口：SpecEnricher → RelationshipPlanner → 交叉验证。

        消除 5 个入口点中重复的 15 行代码块。

        Args:
            spec: 已解析的 DeveloperSpec
            manifest: 源数据清单
            table_mapping: 表名映射（None 时自动推断）

        Returns:
            (spec, hypothesis, extra_questions, table_mapping)
        """
        # 自动表映射
        if not table_mapping:
            table_mapping = _auto_table_mapping(spec)

        # SpecEnricher：从业务描述推断缺失指标
        spec = self._spec_enricher.apply_enrichment(spec, manifest)

        # RelationshipPlanner：多表时生成 Join 推测
        extra_questions: list[OpenQuestion] = []
        hypothesis = None
        if len(spec.input_tables) > 1:
            hypothesis, extra_questions = self._relationship_planner.plan(spec, manifest)

        # 交叉验证——指标推断 vs Join 推断一致性检查
        if hypothesis:
            xv_questions = cross_validate(spec, hypothesis, manifest)
            extra_questions.extend(xv_questions)

        return spec, hypothesis, extra_questions, table_mapping or {}

    def _parse_and_enrich(
        self,
        method: str,
        markdown_text: str,
        table_mapping: dict | None = None,
        *,
        pipeline_stages: list[str] | None = None,
    ) -> dict:
        """Stage 1+2 统一入口：Parser + Enrich/Plan。

        build_plan / execute / run_all / build_plan_rich / execute_rich
        五个入口方法共享此逻辑——消除各方法中重复的 25 行 parser+enrich 代码。

        Args:
            method: 调用方方法名（用于日志标记）
            markdown_text: DeveloperSpec Markdown 全文
            table_mapping: table_ref → 物理表名（可选）
            pipeline_stages: 完整阶段列表（run_all 传 7 阶段，其余默认 5 阶段）

        Returns:
            成功时：{"ok": True, "spec": ..., "manifest": ..., "hypothesis": ...,
                      "extra_questions": ..., "table_mapping": ...}
            失败时：{"ok": False, "error_response": {...}}
        """
        # ── Stage 1: Parser ──
        try:
            parser = DeveloperSpecParser()
            spec = parser.parse(markdown_text)
            manifest = build_manifest_from_spec(spec)
        except Exception as e:
            self._log_stage_failure(method, "parser", e)
            error_info = self._capture_error("parser", e)
            return {
                "ok": False,
                "error_response": {
                    "request_id": "",
                    "pipeline_error": error_info,
                    "pipeline_stages": self._build_pipeline_stages(
                        "parser", error_info, pipeline_stages,
                    ),
                },
            }

        # ── Stage 2: Enrich + Plan ──
        try:
            spec, hypothesis, extra_questions, table_mapping = self._enrich_and_plan(
                spec, manifest, table_mapping,
            )
        except Exception as e:
            self._log_stage_failure(method, "enrich", e)
            request_id = self._gen_request_id(spec)
            self._store_result(request_id, {"parsed_spec": spec, "manifest": manifest})
            error_info = self._capture_error("enrich", e)
            return {
                "ok": False,
                "error_response": {
                    "request_id": request_id,
                    "spec_id": spec.spec_id,
                    "pipeline_error": error_info,
                    "pipeline_stages": self._build_pipeline_stages(
                        "enrich", error_info, pipeline_stages,
                    ),
                },
            }

        return {
            "ok": True,
            "spec": spec,
            "manifest": manifest,
            "hypothesis": hypothesis,
            "extra_questions": extra_questions,
            "table_mapping": table_mapping or {},
        }

    # ── 错误处理辅助方法 ─────────────────────────────────

    @staticmethod
    def _capture_error(stage: str, exc: Exception) -> dict:
        """将异常封装为结构化错误信息。

        Args:
            stage: 失败阶段标识（parser/enrich/build/compile/execute）
            exc: 捕获的异常

        Returns:
            含 stage、error_type、error_message 的 dict
        """
        return {
            "stage": stage,
            "error_type": type(exc).__name__,
            "error_message": str(exc),
        }

    @staticmethod
    def _log_stage_failure(
        method: str,
        stage: str,
        exc: Exception,
        request_id: str = "pending",
    ) -> None:
        """统一错误日志输出——使用标准 logging 替代 print。

        Args:
            method: 调用方法名（parse_only/build_plan/execute/run_all/...）
            stage: 失败阶段标识（parser/enrich/build/compile/execute/contract/package）
            exc: 捕获的异常
            request_id: 请求 ID——parser 阶段失败时为 "pending"
        """
        logger.error(
            "%s: %s 阶段失败 - %s: %s [request_id=%s]",
            method,
            stage,
            type(exc).__name__,
            exc,
            request_id,
        )

    @staticmethod
    def _build_pipeline_stages(
        failed_stage: str,
        error_info: dict | None = None,
        all_stages: list[str] | None = None,
    ) -> list[dict]:
        """构建流水线阶段状态列表——失败阶段之前为 ok，自身为 failed，之后为 skipped。

        Args:
            failed_stage: 失败的阶段标识
            error_info: 失败阶段的错误详情（可选，合并到 failed 条目）
            all_stages: 完整阶段列表（默认 5 阶段，run_all 用 7 阶段）

        Returns:
            阶段状态列表，前端据此渲染指示灯
        """
        if all_stages is None:
            all_stages = ["parser", "enrich", "build", "validate", "compile", "execute"]
        stages = []
        for s in all_stages:
            if s == failed_stage:
                entry: dict = {"stage": s, "status": "failed"}
                if error_info:
                    entry.update(error_info)
                stages.append(entry)
            elif all_stages.index(s) < all_stages.index(failed_stage):
                stages.append({"stage": s, "status": "ok"})
            else:
                stages.append({"stage": s, "status": "skipped"})
        return stages

    @staticmethod
    def _stage_name_cn(stage: str) -> str:
        """返回阶段的中文名称。"""
        _names = {
            "parser": "解析",
            "enrich": "增强",
            "build": "构建",
            "validate": "验证",
            "compile": "编译",
            "execute": "执行",
            "contract": "契约",
            "package": "打包",
        }
        return _names.get(stage, stage)

    def _build_validation_blocked_response(
        self,
        spec,
        manifest,
        plan,
        all_questions: list,
        *,
        table_mapping: dict[str, str] | None = None,
        all_stages: list[str] | None = None,
    ) -> dict:
        """构建 Validator 阻断响应——保存已完成产物，返回含 pipeline_error 的响应基底。

        Validator 返回 passed=False 时调用此方法，在进入 compile 前中止流水线。
        已完成产物（spec / manifest / plan）保留到 self._results 供诊断。

        Args:
            spec: 已解析的 DeveloperSpec
            manifest: 事实源清单
            plan: 已构建的 SqlBuildPlan
            all_questions: 全部 OpenQuestion（含 blocking 和非 blocking）
            table_mapping: 表名映射
            all_stages: 完整阶段列表（run_all 传 8 阶段，其余默认 6 阶段）

        Returns:
            含 pipeline_error + pipeline_stages + validation_passed=False 的响应基底，
            调用方需补充方法专属的空字段（如 sql_sha256 / execution_trace 等）。
        """
        blocking = [q for q in all_questions if q.blocking]
        count = len(blocking)
        descriptions = "；".join(q.description for q in blocking[:3])
        if count > 3:
            descriptions += f" …等 {count} 个问题"

        request_id = self._gen_request_id(spec)
        self._store_result(
            request_id,
            {
                "parsed_spec": spec,
                "manifest": manifest,
                "plan": plan,
                "table_mapping": table_mapping or {},
            },
        )

        error_info = {
            "stage": "validate",
            "error_type": "ValidationBlocked",
            "error_message": (
                f"验证阶段发现 {count} 个阻塞问题，编译已中止：{descriptions}"
            ),
        }

        return {
            "request_id": request_id,
            "spec_id": spec.spec_id,
            "plan_id": plan.plan_id,
            "validation_passed": False,
            "open_questions": _summarize_open_questions(all_questions),
            "pipeline_error": error_info,
            "pipeline_stages": self._build_pipeline_stages(
                "validate", error_info, all_stages
            ),
        }

    # ── table_paths 解析辅助 ──────────────────────────

    def _resolve_table_paths(
        self, table_paths: dict[str, str] | None,
    ) -> dict[str, str]:
        """解析 table_paths 参数——区分 None（未传）和 {}（显式传空）。

        None → 回退到 self._default_table_paths（E2E 环境中的 CSV fixture 自动发现结果）
        {}   → 不回退，保持空字典（显式声明"不需要任何 CSV 文件"）

        这是 Phase 9C-R16 边界硬化的核心语义：防止显式传 {} 时意外加载测试数据。
        """
        if table_paths is not None:
            return table_paths
        return self._default_table_paths

    # ── 公共方法 ──────────────────────────────────────────

    def parse_only(self, markdown_text: str, rich: bool = False) -> dict:
        """解析 DeveloperSpec——返回 SpecParseResponse / SpecRichResponse 的 dict。

        解析失败时返回 200 + pipeline_error，保留错误信息供前端展示。

        Args:
            markdown_text: DeveloperSpec Markdown 全文
            rich: True 时返回完整表/字段/指标/维度/Join/时间范围（前端 SPA 用）

        Returns:
            符合 SpecParseResponse 或 SpecRichResponse 结构的 dict
        """
        self._purge_expired()
        # ── Stage: parser ──
        try:
            parser = DeveloperSpecParser()
            spec = parser.parse(markdown_text)
        except Exception as e:
            self._log_stage_failure("parse_only", "parser", e)
            error_info = self._capture_error("parser", e)
            return {
                "request_id": "",
                "pipeline_error": error_info,
                "pipeline_stages": self._build_pipeline_stages("parser", error_info),
            }

        request_id = self._gen_request_id(spec)
        self._store_result(request_id, {"parsed_spec": spec})

        base = {
            "request_id": request_id,
            "spec_id": spec.spec_id,
            "spec_hash": spec.spec_hash,
            "title": spec.title,
            "table_count": len(spec.input_tables),
            "metric_count": len(spec.metrics),
            "dimension_count": len(spec.dimensions),
            "has_joins": bool(spec.joins),
            "has_time_range": spec.time_range is not None,
            "open_question_count": len(spec.open_questions),
            "warning_count": len(spec.parse_warnings),
            "open_questions": _summarize_open_questions(spec.open_questions),
            "parse_warnings": _summarize_warnings(spec.parse_warnings),
        }
        if not rich:
            return base

        # ── Rich 扩展字段 ──
        tables = []
        for t in spec.input_tables:
            tables.append({
                "table_alias": t.table_alias,
                "source_table": str(t.source_table),
                "row_count": t.row_count,
                "role": t.role,
                "column_count": len(t.columns) + len(t.key_columns) + len(t.business_columns),
                "has_time_field": t.time_field is not None,
                "has_partition": t.partition_field is not None,
            })
        joins = []
        for j in (spec.joins or []):
            joins.append({
                "left_table": j.left_table,
                "right_table": j.right_table,
                "left_key": j.left_key,
                "right_key": j.right_key,
                "join_type": _safe_enum_value(j, "join_type"),
            })
        time_range = None
        if spec.time_range:
            time_range = {
                "column_ref": spec.time_range.column_ref,
                "start": spec.time_range.start,
                "end": spec.time_range.end,
            }
        base["tables"] = tables
        base["metrics"] = [
            {"metric_name": m.metric_name, "aggregation": m.aggregation.value,
             "input_column": m.input_column, "alias": m.alias}
            for m in spec.metrics
        ]
        base["dimensions"] = [
            {"dimension_name": d.dimension_name, "column_ref": d.column_ref}
            for d in spec.dimensions
        ]
        base["joins"] = joins
        base["time_range"] = time_range
        base["output_spec"] = {
            "columns": [c.model_dump() for c in spec.output_spec.columns],
            "grain": spec.output_spec.grain,
            "sort_columns": [s.column for s in (spec.output_spec.sort or [])],
            "limit": spec.output_spec.limit,
        }
        return base

    def build_plan(self, markdown_text: str, table_mapping: dict[str, str] | None = None) -> dict:
        """解析 + 构建 SqlBuildPlan + Validator 验证——返回 PlanResponse 的 dict。

        失败时保留已完成产物到 self._results，返回含 pipeline_error 的部分结果。

        Args:
            markdown_text: DeveloperSpec Markdown 全文
            table_mapping: table_ref → 物理表名（可选）

        Returns:
            符合 PlanResponse 结构的 dict，失败时含 pipeline_error + pipeline_stages
        """
        self._purge_expired()
        # ── Stage 1-2: Parser + Enrich ──
        parsed = self._parse_and_enrich("build_plan", markdown_text, table_mapping)
        if not parsed["ok"]:
            return parsed["error_response"]
        spec = parsed["spec"]
        manifest = parsed["manifest"]
        hypothesis = parsed["hypothesis"]
        extra_questions = parsed["extra_questions"]
        table_mapping = parsed["table_mapping"]

        # ── Stage 3: Build + Validate ──
        plan = None
        plan_questions: list = []
        try:
            builder = SqlBuildPlanBuilder()

            if spec.compute_steps and len(spec.compute_steps) > 0:
                # ── ComputeSteps 路径：每步独立聚合 Plan，_temp 串联 ──
                plans = builder.build_from_steps(spec, hypothesis)
                chain_id = hashlib.md5(
                    "|".join(s.step_name for s in spec.compute_steps).encode()
                ).hexdigest()[:8]
                sql_program = build_sql_program_from_compute_steps(
                    plans, spec, chain_id
                )
                validator = SqlBuildPlanValidator()
                passed, val_questions = validator.validate_multi_hop_chain(sql_program)
                plan = plans[-1]
                plan_questions = []
            elif hypothesis and len(hypothesis.candidates) > 1:
                # ── 多跳链路径：每对候选独立 Plan，_temp 串联 ──
                plans = builder.build_multi(spec, hypothesis)
                chain_id = hashlib.md5(
                    "|".join(c.candidate_id for c in hypothesis.candidates).encode()
                ).hexdigest()[:8]
                sql_program = build_sql_program_from_chain(
                    plans, spec.spec_hash, chain_id
                )
                validator = SqlBuildPlanValidator()
                passed, val_questions = validator.validate_multi_hop_chain(sql_program)
                plan = plans[-1]
                plan_questions = []
            else:
                plan, plan_questions = builder.build(spec, hypothesis=hypothesis)
                validator = SqlBuildPlanValidator()
                passed, val_questions = validator.validate(plan, manifest)
                sql_program = build_sql_program(plan, spec.spec_hash)

        except Exception as e:
            self._log_stage_failure("build_plan", "build", e)
            request_id = self._gen_request_id(spec)
            partial: dict = {"parsed_spec": spec, "manifest": manifest}
            if plan is not None:
                partial["plan"] = plan
            self._store_result(request_id, partial)
            error_info = self._capture_error("build", e)
            return {
                "request_id": request_id,
                "spec_id": spec.spec_id,
                "plan_id": plan.plan_id if plan is not None else "",
                "pipeline_error": error_info,
                "pipeline_stages": self._build_pipeline_stages("build", error_info),
            }

        request_id = self._gen_request_id(spec)
        self._store_result(request_id, {
            "parsed_spec": spec,
            "manifest": manifest,
            "plan": plan,
            "table_mapping": table_mapping or {},
        })

        all_questions = list(plan_questions) + list(val_questions) + list(extra_questions)
        return {
            "request_id": request_id,
            "spec_id": spec.spec_id,
            "plan_id": plan.plan_id,
            "step_count": len(plan.steps),
            "step_types": [s.step_type for s in plan.steps],
            "multi_table": plan.multi_table,
            "validation_passed": passed,
            "open_questions": _summarize_open_questions(all_questions),
        }

    def execute(
        self, markdown_text: str,
        table_mapping: dict[str, str] | None = None,
        table_paths: dict[str, str] | None = None,
    ) -> dict:
        """全流程：解析 → 构建 → 验证 → 编译 → 执行——返回 ExecuteResponse 的 dict。

        失败时保留已完成产物到 self._results，返回含 pipeline_error 的部分结果。
        成功路径返回值结构不变。

        Args:
            markdown_text: DeveloperSpec Markdown 全文
            table_mapping: table_ref → 物理表名（传给 Compiler）
            table_paths: 物理表名 → CSV 文件路径（传给 Executor）

        Returns:
            符合 ExecuteResponse 结构的 dict，失败时额外含 pipeline_error + pipeline_stages
        """
        self._purge_expired()
        # ── Stage 1-2: Parser + Enrich ──
        parsed = self._parse_and_enrich("execute", markdown_text, table_mapping)
        if not parsed["ok"]:
            return parsed["error_response"]
        spec = parsed["spec"]
        manifest = parsed["manifest"]
        hypothesis = parsed["hypothesis"]
        extra_questions = parsed["extra_questions"]
        table_mapping = parsed["table_mapping"]

        # ── Stage 3-5: Build → Compile → Execute（按分支） ──
        # 跨阶段变量——初始化为 None，按阶段赋值
        plan = None
        compiled = None
        program_artifact = None
        all_questions: list = []
        validation_passed = False
        stage = "build"

        try:
            builder = SqlBuildPlanBuilder()

            if spec.compute_steps and len(spec.compute_steps) > 0:
                # ── ComputeSteps 路径 ──
                plans = builder.build_from_steps(spec, hypothesis)
                chain_id = hashlib.md5(
                    "|".join(s.step_name for s in spec.compute_steps).encode()
                ).hexdigest()[:8]
                sql_program = build_sql_program_from_compute_steps(
                    plans, spec, chain_id
                )
                validator = SqlBuildPlanValidator()
                _chain_passed, val_questions = validator.validate_multi_hop_chain(sql_program)
                validation_passed = _chain_passed
                plan = plans[-1]
                plan_questions: list = []
                all_questions = list(plan_questions) + list(val_questions) + list(extra_questions)

                if not _chain_passed:
                    blocked = self._build_validation_blocked_response(
                        spec, manifest, plan, all_questions,
                        table_mapping=table_mapping,
                    )
                    blocked.update({
                        "sql_sha256": "",
                        "compiler_version": "",
                        "execution_trace": None,
                        "result_summary": None,
                    })
                    return blocked

                stage = "compile"
                compiler = DuckDbSqlCompiler(table_mapping=table_mapping or {})
                program_artifact = compiler.compile_program(sql_program)

                stage = "execute"
                execute_executor = DuckDBExecutor(
                    table_paths=self._resolve_table_paths(table_paths),
                    duckdb_path=self._duckdb_path,
                )
                program_result = execute_executor.execute_program(
                    program_artifact.compiled
                )
                last_result = (
                    program_result.results[-1]
                    if program_result.results else None
                )
                trace = last_result.trace if last_result is not None else None
                summary = last_result.summary if last_result is not None else None
                compiled = program_artifact.compiled.statements[-1]
            elif hypothesis and len(hypothesis.candidates) > 1:
                # ── 多跳链路径 ──
                plans = builder.build_multi(spec, hypothesis)
                chain_id = hashlib.md5(
                    "|".join(c.candidate_id for c in hypothesis.candidates).encode()
                ).hexdigest()[:8]
                sql_program = build_sql_program_from_chain(
                    plans, spec.spec_hash, chain_id
                )
                validator = SqlBuildPlanValidator()
                _chain_passed, val_questions = validator.validate_multi_hop_chain(sql_program)
                validation_passed = _chain_passed
                plan = plans[-1]
                plan_questions: list = []
                all_questions = list(plan_questions) + list(val_questions) + list(extra_questions)

                if not _chain_passed:
                    blocked = self._build_validation_blocked_response(
                        spec, manifest, plan, all_questions,
                        table_mapping=table_mapping,
                    )
                    blocked.update({
                        "sql_sha256": "",
                        "compiler_version": "",
                        "execution_trace": None,
                        "result_summary": None,
                    })
                    return blocked

                stage = "compile"
                compiler = DuckDbSqlCompiler(table_mapping=table_mapping or {})
                program_artifact = compiler.compile_program(sql_program)

                stage = "execute"
                execute_executor = DuckDBExecutor(
                    table_paths=self._resolve_table_paths(table_paths),
                    duckdb_path=self._duckdb_path,
                )
                program_result = execute_executor.execute_program(
                    program_artifact.compiled
                )
                last_result = (
                    program_result.results[-1]
                    if program_result.results else None
                )
                trace = last_result.trace if last_result is not None else None
                summary = last_result.summary if last_result is not None else None
                compiled = program_artifact.compiled.statements[-1]
            else:
                plan, plan_questions = builder.build(spec, hypothesis=hypothesis)

                # Validator 验证——blocking 问题阻断编译，非 blocking 记录供排查
                validator = SqlBuildPlanValidator()
                _passed, val_questions = validator.validate(plan, manifest)
                validation_passed = _passed
                all_questions = list(plan_questions) + list(val_questions) + list(extra_questions)

                if not _passed:
                    blocked = self._build_validation_blocked_response(
                        spec, manifest, plan, all_questions,
                        table_mapping=table_mapping,
                    )
                    blocked.update({
                        "sql_sha256": "",
                        "compiler_version": "",
                        "execution_trace": None,
                        "result_summary": None,
                    })
                    return blocked

                stage = "compile"
                compiler = DuckDbSqlCompiler(table_mapping=table_mapping or {})
                compiled = compiler.compile(plan)

                stage = "execute"
                execute_executor = DuckDBExecutor(
                    table_paths=self._resolve_table_paths(table_paths),
                    duckdb_path=self._duckdb_path,
                )
                trace, summary = execute_executor.execute(compiled)

            # ── 执行状态检查——RUNTIME_FAIL 阻断，不进入成功路径 ──
            if isinstance(trace.status, ExecutionStatus) and trace.status == ExecutionStatus.RUNTIME_FAIL:
                _plan_id = plan.plan_id if plan is not None else ""
                _sql_sha256 = compiled.sql_sha256 if compiled is not None else ""
                _compiler_ver = compiled.compiler_version if compiled is not None else ""
                request_id = self._gen_request_id(spec)
                self._store_result(request_id, {
                    "parsed_spec": spec,
                    "manifest": manifest,
                    "plan": plan,
                    "compiled": compiled,
                    "trace": trace,
                    "summary": summary,
                    "table_mapping": table_mapping or {},
                })
                error_info = {
                    "stage": "execute",
                    "error_type": "ExecutionFailed",
                    "error_message": trace.error_message or "SQL 执行失败",
                }
                return {
                    "request_id": request_id,
                    "spec_id": spec.spec_id,
                    "plan_id": _plan_id,
                    "sql_sha256": _sql_sha256,
                    "compiler_version": _compiler_ver,
                    "execution_trace": None,
                    "result_summary": None,
                    "validation_passed": validation_passed,
                    "open_questions": _summarize_open_questions(all_questions),
                    "pipeline_error": error_info,
                    "pipeline_stages": self._build_pipeline_stages("execute", error_info),
                }

        except Exception as e:
            # ── 错误处理：日志 + 保留已完成产物 + 返回部分结果 ──
            request_id = self._gen_request_id(spec)
            self._log_stage_failure("execute", stage, e, request_id)
            # 保存已完成产物供事后查询
            partial: dict = {
                "parsed_spec": spec,
                "manifest": manifest,
            }
            if plan is not None:
                partial["plan"] = plan
            if compiled is not None:
                partial["compiled"] = compiled
            elif program_artifact is not None:
                partial["program_artifact"] = program_artifact
            partial["table_mapping"] = table_mapping or {}
            self._store_result(request_id, partial)

            # 提取部分可用字段——根据已完成的阶段
            _plan_id = plan.plan_id if plan is not None else ""
            _sql_sha256 = ""
            _compiler_ver = ""
            if compiled is not None:
                _sql_sha256 = getattr(compiled, "sql_sha256", "")
                _compiler_ver = getattr(compiled, "compiler_version", "")
            elif program_artifact is not None:
                try:
                    _sql_sha256 = program_artifact.compiled.statements[-1].sql_sha256
                except (IndexError, AttributeError):
                    pass
                _compiler_ver = getattr(program_artifact, "compiler_version", "")

            error_info = self._capture_error(stage, e)
            return {
                "request_id": request_id,
                "spec_id": spec.spec_id,
                "plan_id": _plan_id,
                "sql_sha256": _sql_sha256,
                "compiler_version": _compiler_ver,
                "execution_trace": None,
                "result_summary": None,
                "validation_passed": validation_passed,
                "open_questions": [],
                "pipeline_error": error_info,
                "pipeline_stages": self._build_pipeline_stages(stage, error_info),
            }

        # ── 确定性抽取 Contract（供 Spark 管线使用）──
        # 前提：plan 已通过 Validator 校验，compiled/trace/summary 已在当前作用域
        contract = None
        try:
            extractor = DataTransformContractExtractor()
            contract = extractor.extract(plan)
        except Exception as contract_err:
            logger.warning("Contract 抽取失败（非阻断）：%s", contract_err)

        # ── 成功路径——现有逻辑不变 ──
        request_id = self._gen_request_id(spec)
        self._store_result(request_id, {
            "parsed_spec": spec,
            "manifest": manifest,
            "plan": plan,
            "compiled": compiled,
            "trace": trace,
            "summary": summary,
            "table_mapping": table_mapping or {},
            "contract": contract,  # 新增——供 Spark 管线使用
        })

        return {
            "request_id": request_id,
            "spec_id": spec.spec_id,
            "plan_id": plan.plan_id,
            "execution_trace": {
                "trace_id": trace.trace_id,
                "status": trace.status.value if hasattr(trace.status, "value") else str(trace.status),
                "row_count": trace.row_count,
                "execution_time_ms": trace.execution_time_ms,
                "error_message": trace.error_message,
            },
            "result_summary": {
                "summary_id": summary.summary_id,
                "columns": summary.columns,
                "column_types": summary.column_types,
                "row_count": summary.row_count,
                "null_counts": summary.null_counts,
                "numeric_sums": summary.numeric_sums,
            },
            "sql_sha256": compiled.sql_sha256,
            "compiler_version": compiled.compiler_version,
            "validation_passed": validation_passed,
            "open_questions": _summarize_open_questions(all_questions),
        }

    def run_all(
        self, markdown_text: str,
        table_mapping: dict[str, str] | None = None,
        table_paths: dict[str, str] | None = None,
        *,
        rich: bool = False,
    ) -> dict:
        """全流程 + ReviewPackage 打包——返回 RunAllResponse 的 dict。

        失败时保留已完成产物到 self._results，返回含 pipeline_error 的部分结果。
        7 阶段：parser → enrich → build → compile → execute → contract → package。

        Args:
            markdown_text: DeveloperSpec Markdown 全文
            table_mapping: table_ref → 物理表名
            table_paths: 物理表名 → CSV 文件路径

        Returns:
            符合 RunAllResponse 结构的 dict，失败时含 pipeline_error + pipeline_stages
        """
        self._purge_expired()
        _run_all_stages = [
            "parser", "enrich", "build", "validate",
            "compile", "execute", "contract", "package",
        ]

        # ── Stage 1-2: Parser + Enrich ──
        parsed = self._parse_and_enrich(
            "run_all", markdown_text, table_mapping,
            pipeline_stages=_run_all_stages,
        )
        if not parsed["ok"]:
            return parsed["error_response"]
        spec = parsed["spec"]
        manifest = parsed["manifest"]
        hypothesis = parsed["hypothesis"]
        extra_questions = parsed["extra_questions"]
        table_mapping = parsed["table_mapping"]

        # ── Stage 3-7: Build → Compile → Execute → Contract → Package ──
        plan = None
        compiled_sql = None
        program_artifact = None
        artifact = None
        contract = None
        package_manifest = None
        trace = None
        summary = None
        execution_trace = None
        # ── Final Hardening: cleanup 状态（ComputeSteps / 多跳链路径会在赋值后覆盖）──
        program_cleanup_status: str | None = None
        program_cleanup_error: str | None = None
        plan_questions: list = []
        val_questions: list = []
        passed = False
        stage = "build"

        try:
            builder = SqlBuildPlanBuilder()

            if spec.compute_steps and len(spec.compute_steps) > 0:
                # ── ComputeSteps 路径 ──
                plans = builder.build_from_steps(spec, hypothesis)
                chain_id = hashlib.md5(
                    "|".join(s.step_name for s in spec.compute_steps).encode()
                ).hexdigest()[:8]
                sql_program = build_sql_program_from_compute_steps(
                    plans, spec, chain_id
                )
                plan = plans[-1]
                plan_questions = []

                validator = SqlBuildPlanValidator()
                passed, val_questions = validator.validate_multi_hop_chain(sql_program)

                if not passed:
                    all_qs = list(plan_questions) + list(val_questions) + list(extra_questions)
                    blocked = self._build_validation_blocked_response(
                        spec, manifest, plan, all_qs,
                        table_mapping=table_mapping, all_stages=_run_all_stages,
                    )
                    blocked.update({
                        "execution_status": "not_executed",
                        "row_count": 0,
                        "elapsed_ms": 0,
                    })
                    return blocked

                stage = "compile"
                compiler = DuckDbSqlCompiler(table_mapping=table_mapping or {})
                program_artifact = compiler.compile_program(sql_program)
                compiled_sql = program_artifact.compiled.statements[-1]

                stage = "execute"
                executor = DuckDBExecutor(
                    table_paths=self._resolve_table_paths(table_paths),
                    duckdb_path=self._duckdb_path,
                )
                program_result = executor.execute_program(
                    program_artifact.compiled
                )
                execution_trace = program_result.results[-1].trace if program_result.results else None
                execution_summary = (
                    program_result.results[-1].summary
                    if program_result and program_result.results else None
                )
                # ── Final Hardening: 捕获 cleanup 状态 ──
                program_cleanup_status = program_result.cleanup_status if program_result else None
                program_cleanup_error = program_result.cleanup_error if program_result else None

                # ── Contract 提取（执行状态检查之前——contract 不依赖执行结果）──
                extractor = DataTransformContractExtractor()
                contract = extractor.extract_v1(sql_program)

                # ── 执行状态检查——RUNTIME_FAIL 阻断 ──
                if execution_trace is not None \
                        and isinstance(execution_trace.status, ExecutionStatus) \
                        and execution_trace.status == ExecutionStatus.RUNTIME_FAIL:
                    request_id = self._gen_request_id(spec)
                    self._store_result(request_id, {
                        "parsed_spec": spec,
                        "manifest": manifest,
                        "plan": plan,
                        "compiled": program_artifact,
                        "contract": contract,
                        "sql_program": sql_program,
                        "trace": execution_trace,
                        "summary": execution_summary,
                        "table_mapping": table_mapping or {},
                        # ── Final Hardening: cleanup 状态 ──
                        "program_cleanup_status": program_cleanup_status,
                        "program_cleanup_error": program_cleanup_error,
                    })
                    error_info = {
                        "stage": "execute",
                        "error_type": "ExecutionFailed",
                        "error_message": execution_trace.error_message or "SQL 执行失败",
                    }
                    return {
                        "request_id": request_id,
                        "spec_id": spec.spec_id,
                        "plan_id": plan.plan_id,
                        "validation_passed": passed,
                        "execution_status": "runtime_failed",
                        "row_count": 0,
                        "elapsed_ms": 0,
                        "open_questions": _summarize_open_questions(
                            list(plan_questions) + list(val_questions) + list(extra_questions)
                        ),
                        "pipeline_error": error_info,
                        "pipeline_stages": self._build_pipeline_stages(
                            "execute", error_info, _run_all_stages,
                        ),
                    }

                stage = "contract"

                # ── Phase 9B-P0: Snapshot 阶段（可选——仅当注入 SnapshotBuilder + Provider 时执行）──
                # 必须在 contract 提取之后——依赖 contract 的 hash
                snapshot_manifest = None
                if self._snapshot_builder is not None and self._snapshot_provider is not None:
                    try:
                        # 计算 contract_hash——使用 Contract 模型的静态方法
                        from tianshu_datadev.artifacts.models import (
                            DataTransformContractLite as _Lite,
                        )
                        from tianshu_datadev.artifacts.models import (
                            DataTransformContractV1 as _V1,  # noqa: N814
                        )
                        if isinstance(contract, _V1):
                            contract_hash = _V1.compute_contract_hash(contract)
                        else:
                            contract_hash = _Lite.compute_contract_hash(contract)

                        # 从 table_paths 推导 source_tables——与 provider 白名单交集
                        source_tables = list(table_paths.keys()) if table_paths else []
                        allowlisted = set(self._snapshot_provider.allowlisted_tables)
                        source_tables = [t for t in source_tables if t in allowlisted]

                        if source_tables:
                            snapshot_manifest = self._snapshot_builder.build(
                                contract_hash=contract_hash,
                                source_tables=source_tables,
                                provider=self._snapshot_provider,
                            )
                            logger.info(
                                "Snapshot 构建成功——snapshot_id=%s，文件数=%d",
                                snapshot_manifest.snapshot_id,
                                len(snapshot_manifest.files),
                            )
                    except Exception as snap_err:
                        # Snapshot 失败不阻断主流程——记录日志，继续 Package
                        logger.warning("Snapshot 构建失败（非阻断）：%s", snap_err)
                        snapshot_manifest = None

                stage = "package"
                request_id = self._gen_request_id(spec)
                package_inputs = PackageInputs(
                    request_id=request_id,
                    original_spec_md=markdown_text,
                    parsed_spec=spec.model_dump(),
                    source_manifest=manifest.model_dump(),
                    sql_build_plan=plan.model_dump(),
                    sql_artifact=SqlArtifact(
                        artifact_id=SqlArtifact.generate_artifact_id(
                            plan.plan_id, program_artifact.compiler_version
                        ),
                        compiled_sql=compiled_sql,
                        spec_hash=spec.spec_hash,
                        plan_id=plan.plan_id,
                    ).model_dump(),
                    execution_trace=execution_trace.model_dump() if execution_trace else {},
                    result_summary=(
                        program_result.results[-1].summary.model_dump()
                        if program_result and program_result.results else {}
                    ),
                    data_transform_contract=contract.model_dump(),
                    open_questions=[],
                    validation_questions=[],
                    perf_results=[],
                    retry_count=0,
                    sql_program=sql_program.model_dump(),                    # SqlProgram 元数据
                    sql_program_artifact=program_artifact.model_dump(),      # 编译产物元数据
                    # ── Phase 9B-P0 ──
                    snapshot_manifest=snapshot_manifest.model_dump() if snapshot_manifest else None,
                )
                packager = ReviewPackageBuilder()
                package_manifest = packager.build(package_inputs)
                self._store_result(request_id, {
                    "package": package_manifest,
                    "sql_artifact": SqlArtifact(
                        artifact_id=SqlArtifact.generate_artifact_id(
                            plan.plan_id, program_artifact.compiler_version
                        ),
                        compiled_sql=compiled_sql,
                        spec_hash=spec.spec_hash,
                        plan_id=plan.plan_id,
                    ),
                    "contract": contract,
                    "plan": plan,
                    "parsed_spec": spec,
                    "sql_program": sql_program,             # SqlProgram 实例——供 Spark Comparator 多语句对比
                    "manifest": manifest,
                    "table_mapping": table_mapping or {},
                    # ── Phase 9B-P0 ──
                    "snapshot_manifest": snapshot_manifest,
                    # ── Final Hardening: cleanup 状态 ──
                    "program_cleanup_status": program_cleanup_status,
                    "program_cleanup_error": program_cleanup_error,
                })

                # ComputeSteps 路径独立返回
                return {
                    "request_id": request_id,
                    "spec_id": spec.spec_id,
                    "plan_id": plan.plan_id,
                    "validation_passed": passed,
                    "execution_status": execution_trace.status if execution_trace else "not_executed",
                    "row_count": execution_trace.row_count if execution_trace else 0,
                    "elapsed_ms": execution_trace.execution_time_ms if execution_trace else 0,
                    "open_questions": _summarize_open_questions(
                        list(plan_questions) + list(val_questions) + list(extra_questions)
                    ),
                    "contract_id": contract.contract_id,
                    "package_id": package_manifest.package_id,
                    "package_dir": f"{self._base_output_dir}/{request_id}",
                    "execution_trace": {
                        "trace_id": execution_trace.trace_id,
                        "status": (
                            execution_trace.status.value
                            if hasattr(execution_trace.status, "value")
                            else str(execution_trace.status)
                        ),
                        "row_count": execution_trace.row_count,
                        "execution_time_ms": execution_trace.execution_time_ms,
                        "error_message": execution_trace.error_message,
                    } if execution_trace else None,
                    "result_summary": {
                        "summary_id": execution_summary.summary_id,
                        "columns": execution_summary.columns,
                        "column_types": execution_summary.column_types,
                        "row_count": execution_summary.row_count,
                        "null_counts": execution_summary.null_counts,
                        "numeric_sums": execution_summary.numeric_sums,
                    } if execution_summary else None,
                    "artifact_count": len(package_manifest.artifacts),
                    "contract": contract.model_dump() if hasattr(contract, "model_dump") else {},
                    "compiled": compiled_sql,
                    "package_manifest": package_manifest.model_dump(
                        exclude_none=True
                    ) if hasattr(package_manifest, "model_dump") else {},
                    "llm_traces": self._get_llm_traces(request_id),  # 新增——LLM 调用追踪
                }

            elif hypothesis and len(hypothesis.candidates) > 1:
                # ── 多跳链路径 ──
                plans = builder.build_multi(spec, hypothesis)
                chain_id = hashlib.md5(
                    "|".join(c.candidate_id for c in hypothesis.candidates).encode()
                ).hexdigest()[:8]
                sql_program = build_sql_program_from_chain(
                    plans, spec.spec_hash, chain_id
                )
                plan = plans[-1]
                plan_questions = []

                validator = SqlBuildPlanValidator()
                passed, val_questions = validator.validate_multi_hop_chain(sql_program)

                if not passed:
                    all_qs = list(plan_questions) + list(val_questions) + list(extra_questions)
                    blocked = self._build_validation_blocked_response(
                        spec, manifest, plan, all_qs,
                        table_mapping=table_mapping, all_stages=_run_all_stages,
                    )
                    blocked.update({
                        "execution_status": "not_executed",
                        "row_count": 0,
                        "elapsed_ms": 0,
                    })
                    return blocked

                stage = "compile"
                compiler = DuckDbSqlCompiler(table_mapping=table_mapping or {})
                program_artifact = compiler.compile_program(sql_program)
                compiled_sql = program_artifact.compiled.statements[-1]

                stage = "execute"
                execute_executor = DuckDBExecutor(
                    table_paths=self._resolve_table_paths(table_paths),
                    duckdb_path=self._duckdb_path,
                )
                program_result = execute_executor.execute_program(
                    program_artifact.compiled
                )
                trace = (
                    program_result.results[-1].trace
                    if program_result.results else None
                )
                summary = (
                    program_result.results[-1].summary
                    if program_result.results else None
                )
                # ── Final Hardening: 捕获 cleanup 状态 ──
                program_cleanup_status = program_result.cleanup_status if program_result else None
                program_cleanup_error = program_result.cleanup_error if program_result else None
            else:
                plan, plan_questions = builder.build(spec, hypothesis=hypothesis)

                validator = SqlBuildPlanValidator()
                passed, val_questions = validator.validate(plan, manifest)

                if not passed:
                    all_qs = list(plan_questions) + list(val_questions) + list(extra_questions)
                    blocked = self._build_validation_blocked_response(
                        spec, manifest, plan, all_qs,
                        table_mapping=table_mapping, all_stages=_run_all_stages,
                    )
                    blocked.update({
                        "execution_status": "not_executed",
                        "row_count": 0,
                        "elapsed_ms": 0,
                    })
                    return blocked

                stage = "compile"
                compiler = DuckDbSqlCompiler(table_mapping=table_mapping or {})
                artifact = compiler.compile_to_artifact(plan, spec.spec_hash)
                compiled_sql = artifact.compiled_sql

                stage = "execute"
                execute_executor = DuckDBExecutor(
                    table_paths=self._resolve_table_paths(table_paths),
                    duckdb_path=self._duckdb_path,
                )
                trace, summary = execute_executor.execute(compiled_sql)

                sql_program = build_sql_program(plan, spec.spec_hash)

            # ── 执行状态检查——RUNTIME_FAIL 阻断，不进入 Contract + Package ──
            if isinstance(trace.status, ExecutionStatus) and trace.status == ExecutionStatus.RUNTIME_FAIL:
                request_id = self._gen_request_id(spec)
                self._store_result(request_id, {
                    "parsed_spec": spec,
                    "manifest": manifest,
                    "plan": plan,
                    "compiled": compiled_sql,
                    "trace": trace,
                    "summary": summary,
                    "table_mapping": table_mapping or {},
                    # ── Final Hardening: cleanup 状态 ──
                    "program_cleanup_status": program_cleanup_status,
                    "program_cleanup_error": program_cleanup_error,
                })
                error_info = {
                    "stage": "execute",
                    "error_type": "ExecutionFailed",
                    "error_message": trace.error_message or "SQL 执行失败",
                }
                return {
                    "request_id": request_id,
                    "spec_id": spec.spec_id,
                    "plan_id": plan.plan_id,
                    "validation_passed": passed,
                    "execution_status": "runtime_failed",
                    "row_count": 0,
                    "elapsed_ms": 0,
                    "open_questions": _summarize_open_questions(
                        list(plan_questions) + list(val_questions) + list(extra_questions)
                    ),
                    "contract_id": "",
                    "package_id": "",
                    "contract": {},
                    "compiled": compiled_sql,
                    "pipeline_error": error_info,
                    "pipeline_stages": self._build_pipeline_stages(
                        "execute", error_info, _run_all_stages,
                    ),
                }

            # ── 公共阶段：Contract + Package（所有路径——ComputeSteps 和非 ComputeSteps）──
            stage = "contract"
            contract_extractor = DataTransformContractExtractor()
            if len(sql_program.statements) > 1:
                contract = contract_extractor.extract_v1(sql_program)
            else:
                contract = contract_extractor.extract(plan)

            # ── Phase 9B-P0: Snapshot 阶段（可选——仅当注入 SnapshotBuilder + Provider 时执行）──
            # 必须在 contract 提取之后——依赖 contract 的 hash
            snapshot_manifest = None
            if self._snapshot_builder is not None and self._snapshot_provider is not None:
                try:
                    # 计算 contract_hash——使用 Contract 模型的静态方法
                    from tianshu_datadev.artifacts.models import (
                        DataTransformContractLite as _Lite,
                    )
                    from tianshu_datadev.artifacts.models import (
                        DataTransformContractV1 as _V1,  # noqa: N814
                    )
                    if isinstance(contract, _V1):
                        contract_hash = _V1.compute_contract_hash(contract)
                    else:
                        contract_hash = _Lite.compute_contract_hash(contract)

                    # 从 table_paths 推导 source_tables——与 provider 白名单交集
                    source_tables = list(table_paths.keys()) if table_paths else []
                    allowlisted = set(self._snapshot_provider.allowlisted_tables)
                    source_tables = [t for t in source_tables if t in allowlisted]

                    if source_tables:
                        snapshot_manifest = self._snapshot_builder.build(
                            contract_hash=contract_hash,
                            source_tables=source_tables,
                            provider=self._snapshot_provider,
                        )
                        logger.info(
                            "Snapshot 构建成功——snapshot_id=%s，文件数=%d",
                            snapshot_manifest.snapshot_id,
                            len(snapshot_manifest.files),
                        )
                except Exception as snap_err:
                    # Snapshot 失败不阻断主流程——记录日志，继续 Package
                    logger.warning("Snapshot 构建失败（非阻断）：%s", snap_err)
                    snapshot_manifest = None

            stage = "package"
            request_id = self._gen_request_id(spec)
            packager = ReviewPackageBuilder(self._base_output_dir)
            package_inputs = PackageInputs(
                request_id=request_id,
                original_spec_md=markdown_text,
                parsed_spec=spec.model_dump(),
                source_manifest=manifest.model_dump(),
                sql_build_plan=plan.model_dump(),
                sql_artifact=(
                    artifact.model_dump()
                    if artifact is not None
                    else program_artifact.model_dump()
                ),
                execution_trace=trace.model_dump(),
                result_summary=summary.model_dump(),
                data_transform_contract=contract.model_dump(),
                open_questions=[
                    q.model_dump()
                    for q in spec.open_questions + plan_questions + extra_questions
                ],
                validation_questions=[q.model_dump() for q in val_questions],
                perf_results=[],
                retry_count=0,
                sql_program=sql_program.model_dump(),                        # SqlProgram 元数据
                sql_program_artifact=(
                    program_artifact.model_dump()
                    if program_artifact is not None
                    else None
                ),
                # ── Phase 9B-P0 ──
                snapshot_manifest=snapshot_manifest.model_dump() if snapshot_manifest else None,
                # 编译产物元数据（单表路径为 None）
            )
            package_manifest = packager.build(package_inputs)

        except Exception as e:
            request_id = self._gen_request_id(spec)
            self._log_stage_failure("run_all", stage, e, request_id)
            # 保存已完成产物
            partial: dict = {"parsed_spec": spec, "manifest": manifest}
            if plan is not None:
                partial["plan"] = plan
            if compiled_sql is not None:
                partial["compiled"] = compiled_sql
            elif program_artifact is not None:
                partial["program_artifact"] = program_artifact
            elif artifact is not None:
                partial["artifact"] = artifact
            if contract is not None:
                partial["contract"] = contract
            partial["table_mapping"] = table_mapping or {}
            self._store_result(request_id, partial)

            _plan_id = plan.plan_id if plan is not None else ""
            _contract_id = contract.contract_id if contract is not None else ""
            _package_id = package_manifest.package_id if package_manifest is not None else ""

            error_info = self._capture_error(stage, e)
            return {
                "request_id": request_id,
                "spec_id": spec.spec_id,
                "plan_id": _plan_id,
                "validation_passed": passed,
                "execution_status": "not_executed",
                "row_count": 0,
                "elapsed_ms": 0,
                "open_questions": [],
                "contract_id": _contract_id,
                "package_id": _package_id,
                "contract": (
                    contract.model_dump()
                    if contract is not None and hasattr(contract, "model_dump")
                    else {}
                ),
                "compiled": compiled_sql,
                "pipeline_error": error_info,
                "pipeline_stages": self._build_pipeline_stages(stage, error_info, _run_all_stages),
            }

        # ── 成功路径（非 ComputeSteps） ──
        self._store_result(request_id, {
            "parsed_spec": spec,
            "manifest": manifest,
            "plan": plan,
            "compiled": compiled_sql,
            "trace": trace,
            "summary": summary,
            "contract": contract,
            "llm_traces": self._get_llm_traces(request_id),  # 新增——LLM 调用追踪
            "table_mapping": table_mapping or {},
            # ── Phase 9B-P0 ──
            "snapshot_manifest": snapshot_manifest,
            # ── Final Hardening: cleanup 状态 ──
            "program_cleanup_status": program_cleanup_status,
            "program_cleanup_error": program_cleanup_error,
        })
        self._store_package(request_id, package_manifest)

        result: dict = {
            "request_id": request_id,
            "spec_id": spec.spec_id,
            "plan_id": plan.plan_id,
            "package_id": package_manifest.package_id,
            "package_dir": f"{self._base_output_dir}/{request_id}",
            "validation_passed": passed,
            "execution_trace": {
                "trace_id": trace.trace_id,
                "status": trace.status.value if hasattr(trace.status, "value") else str(trace.status),
                "row_count": trace.row_count,
                "execution_time_ms": trace.execution_time_ms,
                "error_message": trace.error_message,
            },
            "result_summary": {
                "summary_id": summary.summary_id,
                "columns": summary.columns,
                "column_types": summary.column_types,
                "row_count": summary.row_count,
                "null_counts": summary.null_counts,
                "numeric_sums": summary.numeric_sums,
            },
            "open_questions": _summarize_open_questions(
                list(plan_questions) + list(val_questions) + list(extra_questions)
            ),
            "artifact_count": len(package_manifest.artifacts),
            "llm_traces": self._get_llm_traces(request_id),  # 新增——LLM 调用追踪
        }
        if rich:
            # 提取 SQL 文本——兼容 CompiledSql 对象和纯字符串
            if hasattr(compiled_sql, "sql"):
                result["generated_sql"] = compiled_sql.sql
                result["sql_sha256"] = compiled_sql.sql_sha256
                result["compiler_version"] = compiled_sql.compiler_version
            else:
                result["generated_sql"] = compiled_sql or ""
                result["sql_sha256"] = ""
                result["compiler_version"] = ""
            # 步骤摘要 + Join 证据（来自 PlanRichResponse）
            result["steps"] = [self._step_to_summary(s) for s in plan.steps]
            result["join_evidence"] = self._extract_join_evidence(plan)
            # 文件树（来自 PackageRichResponse）
            result["file_tree"] = self._build_file_tree(package_manifest.artifacts)
        return result

    def run_all_rich(
        self, markdown_text: str,
        table_mapping: dict[str, str] | None = None,
        table_paths: dict[str, str] | None = None,
    ) -> dict:
        """前端专用：全流程+打包+富结果——返回 RunAllRichResponse dict。

        一步获得 PlanRich + ExecuteRich + PackageRich 的全部信息，
        前端无需分两次请求。

        Args:
            markdown_text: DeveloperSpec Markdown 全文
            table_mapping: table_ref → 物理表名
            table_paths: 物理表名 → CSV 文件路径

        Returns:
            符合 RunAllRichResponse 结构的 dict，失败时含 pipeline_error + pipeline_stages
        """
        return self.run_all(
            markdown_text, table_mapping, table_paths, rich=True,
        )

    def get_package(self, request_id: str, rich: bool = False) -> dict | None:
        """获取已打包的 ReviewPackageManifest。

        Args:
            request_id: 请求唯一标识
            rich: True 时返回 file_tree（前端 SPA 用），False 时返回平面 artifact 列表

        Returns:
            符合 PackageResponse / PackageRichResponse 结构的 dict，不存在时返回 None
        """
        self._purge_expired()
        manifest = self._packages.get(request_id)
        if manifest is None:
            return None
        base = {
            "request_id": manifest.request_id,
            "package_id": manifest.package_id,
            "created_at": manifest.created_at,
            "artifact_count": len(manifest.artifacts),
            "spec_hash": manifest.spec_hash,
            "retry_count": manifest.retry_count,
        }
        if rich:
            base["file_tree"] = self._build_file_tree(manifest.artifacts)
        else:
            base["artifacts"] = [a.model_dump() for a in manifest.artifacts]
        return base

    # ── Phase 9A1: 中间产物导出 ──────────────────────────

    def export_artifacts(self, request_id: str) -> PipelineArtifactBundle | None:
        """导出指定 request_id 的 Pipeline 中间产物——供下游 Spark Orchestrator / Harness Runner 消费。

        从 _results 内存缓存中提取指定请求的结构化 artifact（SqlBuildPlan / DataTransformContract /
        CompiledSql / ExecutionTrace / ResultSummary），封装为 PipelineArtifactBundle。

        Pipeline 各路径产出的 artifact 不同——缺失字段为 None：
        - build_plan: 仅有 sql_build_plan
        - execute: 有 sql_build_plan + compiled_sql + trace + summary（无 contract）
        - run_all（非 ComputeSteps）: 有 sql_build_plan + compiled_sql + trace + summary + contract
        - run_all（ComputeSteps）: 有 sql_build_plan + data_transform_contract + sql_artifact

        Args:
            request_id: Pipeline 请求 ID（run_all/execute/build_plan 返回值中的 request_id）

        Returns:
            PipelineArtifactBundle——含所有已缓存的结构化产物；缓存不存在或 TTL 过期时返回 None
        """
        self._purge_expired()
        data = self._results.get(request_id)
        if data is None:
            return None

        # 提取 spec_hash——从 ParsedDeveloperSpec 获取
        spec_hash = ""
        parsed_spec = data.get("parsed_spec")
        if parsed_spec is not None:
            spec_hash = getattr(parsed_spec, "spec_hash", "")

        # 提取 contract——Phase 9A1 已修复：run_all / execute / build_plan 所有路径均存储 contract
        contract = data.get("contract")

        # 提取 compiled——execute/run_all 单表路径存储为 "compiled"
        # 防御：ComputeSteps 失败路径存储 SqlProgramArtifact 而非 CompiledSql，
        # 此处仅当类型匹配时才传递，否则置 None
        compiled = data.get("compiled")
        if compiled is not None and not isinstance(compiled, CompiledSql):
            compiled = None

        # ── Phase 9B-P0: 提取 snapshot_manifest ──
        snapshot_manifest = data.get("snapshot_manifest")

        return PipelineArtifactBundle(
            request_id=request_id,
            spec_hash=spec_hash,
            sql_build_plan=data.get("plan"),
            data_transform_contract=contract,
            compiled_sql=compiled,
            execution_trace=data.get("trace"),
            result_summary=data.get("summary"),
            # ── Phase 9B-P0 ──
            snapshot_manifest=snapshot_manifest,
            # ── Phase 10: Case06 SqlProgram 多语句 DAG ──
            sql_program=data.get("sql_program"),
            # ── Final Hardening: cleanup 状态 ──
            program_cleanup_status=data.get("program_cleanup_status"),
            program_cleanup_error=data.get("program_cleanup_error"),
        )

    # ── Phase 4.5B 前端 SPA 专用方法 ──────────────────────

    def get_templates(self) -> list[dict]:
        """获取预设的 DeveloperSpec 模板列表。

        Returns:
            模板定义列表（不含 markdown_template 时的精简版用于列表展示）
        """
        return [
            {
                "template_id": t["template_id"],
                "name": t["name"],
                "description": t["description"],
                "category": t["category"],
            }
            for t in TEMPLATES
        ]

    def get_template(self, template_id: str) -> dict | None:
        """获取指定模板的完整定义（含 markdown_template）。

        Args:
            template_id: 模板唯一标识

        Returns:
            完整模板定义 dict，不存在时返回 None
        """
        for t in TEMPLATES:
            if t["template_id"] == template_id:
                return dict(t)
        return None

    @staticmethod
    def _step_to_summary(step) -> dict:
        """将单个 SqlBuildPlan step 转换为前端可用的摘要。

        根据 step_type 提取关键信息生成人类可读的描述。
        """
        desc_parts = []
        stype = step.step_type
        if stype == "scan":
            cols = [c.column_name for c in step.required_columns[:5]]
            more = f" +{len(step.required_columns) - 5}" if len(step.required_columns) > 5 else ""
            desc_parts.append(f"扫描表 {step.table_ref}，读取列: {', '.join(cols)}{more}")
        elif stype == "filter":
            desc_parts.append(f"过滤: {step.predicate.operator}")
        elif stype == "join":
            keys = [f"{lk.column_name}={rk.column_name}" for lk, rk in step.join_keys]
            desc_parts.append(f"Join {step.right_table_ref} ({step.join_type}) ON {', '.join(keys)}")
        elif stype == "aggregate":
            gk = [k.column_name for k in step.group_keys]
            ms = [m.alias for m in step.metrics]
            desc_parts.append(f"按 {', '.join(gk)} 分组，聚合: {', '.join(ms)}")
        elif stype == "project":
            cols = [a.alias for a in step.columns[:5]]
            if len(step.columns) > 5:
                cols.append(f"+{len(step.columns) - 5}")
            desc_parts.append(f"投影列: {', '.join(cols)}")
        elif stype == "sort":
            sc = [f"{s.column} {s.direction}" for s in step.sort_keys]
            desc_parts.append(f"排序: {', '.join(sc)}")
        elif stype == "limit":
            desc_parts.append(f"限制行数: {step.limit_count}")
        elif stype == "case_when":
            desc_parts.append(f"CASE WHEN 分支数: {len(step.branches)}")
        else:
            desc_parts.append(f"步骤类型: {stype}")
        return {
            "step_type": stype,
            "step_id": step.step_id,
            "description": "；".join(desc_parts) if desc_parts else stype,
        }

    @staticmethod
    def _extract_join_evidence(plan: SqlBuildPlan) -> list[dict]:
        """从 SqlBuildPlan 的 join_hypothesis 中提取 Join 证据。

        Args:
            plan: SqlBuildPlan 实例

        Returns:
            JoinEvidenceItem dict 列表
        """
        evidence_list = []
        if not hasattr(plan, "join_hypothesis") or plan.join_hypothesis is None:
            return evidence_list
        hypothesis = plan.join_hypothesis
        if not hasattr(hypothesis, "candidates"):
            return evidence_list
        for candidate in hypothesis.candidates:
            item = {
                "evidence_id": getattr(candidate, "candidate_id", ""),
                "level": _safe_enum_value(candidate, "level"),
                "action": _safe_enum_value(candidate, "action"),
                "left_table": getattr(candidate, "left_table", ""),
                "right_table": getattr(candidate, "right_table", ""),
                "left_key_raw": getattr(candidate, "left_key_raw", ""),
                "right_key_raw": getattr(candidate, "right_key_raw", ""),
                "left_key_normalized": getattr(candidate, "left_key_normalized", ""),
                "right_key_normalized": getattr(candidate, "right_key_normalized", ""),
                "evidence_checks": list(getattr(candidate, "evidence_checks", [])),
                "detail": getattr(candidate, "detail", ""),
                "evidence_chain_yaml": getattr(candidate, "evidence_chain_yaml", ""),
            }
            evidence_list.append(item)
        return evidence_list

    @staticmethod
    def _build_file_tree(artifacts: list) -> list[dict]:
        """从 artifact 清单构建文件树结构。

        将扁平的 artifact 路径列表转换为嵌套树结构供前端渲染。

        Args:
            artifacts: Artifact 模型列表（每项含 path、sha256 属性）

        Returns:
            树节点 dict 列表
        """
        # 按路径分组构建树
        tree_root: dict[str, dict] = {}

        for a in artifacts:
            path = getattr(a, "path", "")
            sha = getattr(a, "sha256", "")
            if not path:
                continue
            parts = path.replace("\\", "/").split("/")
            current = tree_root
            for i, part in enumerate(parts):
                if part not in current:
                    is_file = (i == len(parts) - 1)
                    current[part] = {
                        "name": part,
                        "path": "/".join(parts[: i + 1]),
                        "kind": "file" if is_file else "directory",
                        "sha256": sha if is_file else None,
                        "_children": {},
                    }
                node = current[part]
                if i < len(parts) - 1:
                    current = node["_children"]
                else:
                    # 文件节点：更新 sha256
                    node["sha256"] = sha

        def _to_list(node_dict: dict) -> list[dict]:
            """将内部 dict 树转换为有序列表，去除 _children 内部键。"""
            result = []
            for name, node in sorted(node_dict.items()):
                children = _to_list(node.pop("_children", {}))
                node["children"] = children
                result.append(node)
            return result

        return _to_list(tree_root)

    def parse_rich(self, markdown_text: str) -> dict:
        """前端专用：完整解析 DeveloperSpec。委托到 parse_only(rich=True)。"""
        return self.parse_only(markdown_text, rich=True)

    def build_plan_rich(
        self, markdown_text: str, table_mapping: dict[str, str] | None = None,
    ) -> dict:
        """前端专用：解析 + 构建 Plan + 提取 Join 证据——返回 PlanRichResponse dict。

        失败时保留已完成产物到 self._results，返回含 pipeline_error 的部分结果。

        Args:
            markdown_text: DeveloperSpec Markdown 全文
            table_mapping: table_ref → 物理表名（可选）

        Returns:
            符合 PlanRichResponse 结构的 dict，失败时含 pipeline_error + pipeline_stages
        """
        self._purge_expired()
        # ── Stage 1-2: Parser + Enrich ──
        parsed = self._parse_and_enrich("build_plan_rich", markdown_text, table_mapping)
        if not parsed["ok"]:
            return parsed["error_response"]
        spec = parsed["spec"]
        manifest = parsed["manifest"]
        hypothesis = parsed["hypothesis"]
        extra_questions = parsed["extra_questions"]
        table_mapping = parsed["table_mapping"]

        # ── Stage 3: Build + Validate ──
        plan = None
        try:
            builder = SqlBuildPlanBuilder()
            plan, plan_questions = builder.build(spec, hypothesis=hypothesis)

            validator = SqlBuildPlanValidator()
            passed, val_questions = validator.validate(plan, manifest)
        except Exception as e:
            self._log_stage_failure("build_plan_rich", "build", e)
            request_id = self._gen_request_id(spec)
            partial: dict = {"parsed_spec": spec, "manifest": manifest}
            if plan is not None:
                partial["plan"] = plan
            self._store_result(request_id, partial)
            error_info = self._capture_error("build", e)
            return {
                "request_id": request_id,
                "spec_id": spec.spec_id,
                "plan_id": plan.plan_id if plan is not None else "",
                "pipeline_error": error_info,
                "pipeline_stages": self._build_pipeline_stages("build", error_info),
            }

        request_id = self._gen_request_id(spec)
        self._store_result(request_id, {
            "parsed_spec": spec, "manifest": manifest, "plan": plan,
            "table_mapping": table_mapping or {},
        })

        all_questions = list(plan_questions) + list(val_questions) + list(extra_questions)

        # 提取步骤摘要
        steps = [self._step_to_summary(s) for s in plan.steps]

        # 提取 Join 证据
        join_evidence = self._extract_join_evidence(plan)

        return {
            "request_id": request_id,
            "spec_id": spec.spec_id,
            "plan_id": plan.plan_id,
            "step_count": len(plan.steps),
            "step_types": [s.step_type for s in plan.steps],
            "steps": steps,
            "multi_table": plan.multi_table,
            "validation_passed": passed,
            "open_questions": _summarize_open_questions(all_questions),
            "join_evidence": join_evidence,
        }

    def execute_rich(
        self, markdown_text: str,
        table_mapping: dict[str, str] | None = None,
        table_paths: dict[str, str] | None = None,
    ) -> dict:
        """前端专用：全流程编译+执行——返回 ExecuteRichResponse dict（含 SQL 文本）。

        失败时保留已完成产物到 self._results，返回含 pipeline_error 的部分结果。

        Args:
            markdown_text: DeveloperSpec Markdown 全文
            table_mapping: table_ref → 物理表名
            table_paths: 物理表名 → CSV 文件路径

        Returns:
            符合 ExecuteRichResponse 结构的 dict，失败时含 pipeline_error + pipeline_stages
        """
        self._purge_expired()
        # ── Stage 1-2: Parser + Enrich ──
        parsed = self._parse_and_enrich("execute_rich", markdown_text, table_mapping)
        if not parsed["ok"]:
            return parsed["error_response"]
        spec = parsed["spec"]
        manifest = parsed["manifest"]
        hypothesis = parsed["hypothesis"]
        extra_questions = parsed["extra_questions"]
        table_mapping = parsed["table_mapping"]

        # ── 记录 Parse + Relationship 阶段的 LLM 追踪（Fake 模式下为 skipped）──
        _t0 = time.time()
        request_id = self._gen_request_id(spec)
        self._record_trace(
            request_id, "parse_developer_spec",
            status="skipped", latency_ms=int((time.time() - _t0) * 1000),
        )
        self._record_trace(
            request_id, "relationship_planner",
            status="skipped", latency_ms=1,
        )

        # ── Stage 3-5: Build → Compile → Execute ──
        plan = None
        compiled = None
        all_questions: list = []
        stage = "build"

        try:
            _build_start = time.time()
            builder = SqlBuildPlanBuilder()
            plan, plan_questions = builder.build(spec, hypothesis=hypothesis)
            self._record_trace(
                request_id, "sql_build_planner",
                status="skipped", latency_ms=int((time.time() - _build_start) * 1000),
            )

            validator = SqlBuildPlanValidator()
            _passed, val_questions = validator.validate(plan, manifest)
            all_questions = list(plan_questions) + list(val_questions) + list(extra_questions)

            if not _passed:
                blocked = self._build_validation_blocked_response(
                    spec, manifest, plan, all_questions,
                    table_mapping=table_mapping,
                )
                blocked.update({
                    "generated_sql": "",
                    "sql_sha256": "",
                    "compiler_version": "",
                    "execution_trace": None,
                    "result_summary": None,
                    "llm_traces": self._get_llm_traces(request_id),
                })
                return blocked

            stage = "compile"
            _compile_start = time.time()
            compiler = DuckDbSqlCompiler(table_mapping=table_mapping or {})
            compiled = compiler.compile(plan)
            self._record_trace(
                request_id, "sql_program_planner",
                status="skipped", latency_ms=int((time.time() - _compile_start) * 1000),
            )

            stage = "execute"
            executor = DuckDBExecutor(
                table_paths=self._resolve_table_paths(table_paths),
                duckdb_path=self._duckdb_path,
            )
            trace, summary = executor.execute(compiled)

            # ── 执行状态检查——RUNTIME_FAIL 阻断，不进入成功路径 ──
            if isinstance(trace.status, ExecutionStatus) and trace.status == ExecutionStatus.RUNTIME_FAIL:
                _plan_id = plan.plan_id if plan is not None else ""
                _sql_sha256 = compiled.sql_sha256 if compiled is not None else ""
                _compiler_ver = compiled.compiler_version if compiled is not None else ""
                request_id = self._gen_request_id(spec)
                self._store_result(request_id, {
                    "parsed_spec": spec,
                    "manifest": manifest,
                    "plan": plan,
                    "compiled": compiled,
                    "trace": trace,
                    "summary": summary,
                    "table_mapping": table_mapping or {},
                })
                error_info = {
                    "stage": "execute",
                    "error_type": "ExecutionFailed",
                    "error_message": trace.error_message or "SQL 执行失败",
                }
                return {
                    "request_id": request_id,
                    "spec_id": spec.spec_id,
                    "plan_id": _plan_id,
                    "validation_passed": _passed,
                    "generated_sql": compiled.sql if compiled is not None else "",
                    "sql_sha256": _sql_sha256,
                    "compiler_version": _compiler_ver,
                    "execution_trace": None,
                    "result_summary": None,
                    "open_questions": _summarize_open_questions(all_questions),
                    "pipeline_error": error_info,
                    "pipeline_stages": self._build_pipeline_stages("execute", error_info),
                    "llm_traces": self._get_llm_traces(request_id),
                }

        except Exception as e:
            request_id = self._gen_request_id(spec)
            self._log_stage_failure("execute_rich", stage, e, request_id)
            partial: dict = {"parsed_spec": spec, "manifest": manifest}
            if plan is not None:
                partial["plan"] = plan
            if compiled is not None:
                partial["compiled"] = compiled
            partial["table_mapping"] = table_mapping or {}
            self._store_result(request_id, partial)

            _plan_id = plan.plan_id if plan is not None else ""
            _sql_sha256 = getattr(compiled, "sql_sha256", "") if compiled is not None else ""
            _compiler_ver = getattr(compiled, "compiler_version", "") if compiled is not None else ""

            error_info = self._capture_error(stage, e)
            return {
                "request_id": request_id,
                "spec_id": spec.spec_id,
                "plan_id": _plan_id,
                "validation_passed": False,
                "generated_sql": getattr(compiled, "sql", "") if compiled is not None else "",
                "sql_sha256": _sql_sha256,
                "compiler_version": _compiler_ver,
                "execution_trace": None,
                "result_summary": None,
                "open_questions": [],
                "pipeline_error": error_info,
                "pipeline_stages": self._build_pipeline_stages(stage, error_info),
                "llm_traces": self._get_llm_traces(request_id),
            }

        request_id = self._gen_request_id(spec)

        # ── 确定性抽取 Contract（供 Spark 管线使用）──
        # 前提：plan 已通过 Validator 校验，compiled/trace/summary 已在当前作用域
        contract = None
        try:
            extractor = DataTransformContractExtractor()
            contract = extractor.extract(plan)
        except Exception as contract_err:
            logger.warning("Contract 抽取失败（非阻断）：%s", contract_err)

        self._store_result(request_id, {
            "parsed_spec": spec, "manifest": manifest, "plan": plan,
            "compiled": compiled, "trace": trace, "summary": summary,
            "table_mapping": table_mapping or {},
            "contract": contract,  # 新增——供 Spark 管线使用
            "llm_traces": self._get_llm_traces(request_id),  # 新增——LLM 调用追踪
        })

        return {
            "request_id": request_id,
            "spec_id": spec.spec_id,
            "plan_id": plan.plan_id,
            "validation_passed": True,
            "generated_sql": compiled.sql,
            "sql_sha256": compiled.sql_sha256,
            "compiler_version": compiled.compiler_version,
            "execution_trace": {
                "trace_id": trace.trace_id,
                "status": _safe_enum_value(trace, "status"),
                "row_count": trace.row_count,
                "execution_time_ms": trace.execution_time_ms,
                "error_message": trace.error_message,
            },
            "result_summary": {
                "summary_id": summary.summary_id,
                "columns": summary.columns,
                "column_types": summary.column_types,
                "row_count": summary.row_count,
                "null_counts": summary.null_counts,
                "numeric_sums": summary.numeric_sums,
            },
            "open_questions": _summarize_open_questions(all_questions),
            "llm_traces": self._get_llm_traces(request_id),  # 新增
        }


    # ════════════════════════════════════════════
    # Spark 阶段独立触发——辅助方法与阶段入口
    # ════════════════════════════════════════════

    def _get_or_create_spark_context(self, request_id: str) -> SparkStageContext:
        """获取或创建 request_id 的 Spark 阶段上下文。"""
        if request_id not in self._spark_contexts:
            self._spark_contexts[request_id] = SparkStageContext()
        return self._spark_contexts[request_id]

    def _check_stage_dependencies(
        self,
        stage: "SparkPipelineStage",
        context: SparkStageContext,
        artifacts: PipelineArtifactBundle,
    ) -> None:
        """Spark 阶段依赖门禁——检查前置产物是否就绪。

        Raises:
            SparkDependencyMissingError: 前置产物缺失
        """
        missing: list[str] = []

        if stage == SparkPipelineStage.MAPPER:
            if artifacts.data_transform_contract is None:
                missing.append("data_transform_contract（请先执行 编译执行 生成 Contract）")

        elif stage == SparkPipelineStage.DEVELOPER:
            if context.spark_plan is None:
                missing.append("spark_plan（请先执行 MAPPER 阶段）")

        elif stage == SparkPipelineStage.COMPILER:
            if context.spark_plan is None:
                missing.append("spark_plan（请先执行 MAPPER 阶段）")

        elif stage == SparkPipelineStage.VALIDATOR:
            if context.compile_result is None:
                missing.append("compile_result（请先执行 COMPILER 阶段）")

        elif stage == SparkPipelineStage.COMPARATOR:
            if artifacts.sql_build_plan is None:
                missing.append("sql_build_plan")
            if context.spark_plan is None:
                missing.append("spark_plan（请先执行 MAPPER 阶段）")
            if artifacts.data_transform_contract is None:
                missing.append("data_transform_contract")

        elif stage == SparkPipelineStage.PHYSICAL_VERIFIER:
            if artifacts.compiled_sql is None:
                missing.append("compiled_sql")
            if context.compile_result is None:
                missing.append("spark compile_result（请先执行 COMPILER 阶段）")

        if missing:
            raise SparkDependencyMissingError(stage, missing)

    def run_spark_stage(
        self,
        request_id: str,
        stage: "SparkPipelineStage",
    ) -> dict:
        """执行单个 Spark 管线阶段。

        流程：
        1. export_artifacts(request_id) → 获取 contract + sql_plan
        2. _get_or_create_spark_context(request_id) → 获取或创建阶段上下文
        3. _check_stage_dependencies(stage, context, artifacts) → 依赖门禁
        4. 执行该阶段（复用现有组件，不通过 SparkOrchestrator.run()）
        5. 缓存中间产物到 SparkStageContext
        6. 收集 llm_traces
        7. 返回 SparkStageResponse 风格 dict

        Raises:
            SparkDependencyMissingError: 前置产物缺失
        """
        # TODO: 当前返回 dict，待 Task 4 定义 SparkStageResponse 模型后应返回该类型

        # 获取阶段的字符串值，后续多处使用
        stage_val = stage.value

        # Step 1: 导出 artifacts
        artifacts = self.export_artifacts(request_id)
        if artifacts is None:
            raise SparkDependencyMissingError(
                stage, [f"request_id '{request_id}' 对应的 artifacts 不存在或已过期"]
            )

        # Step 2: 获取 Spark 上下文
        context = self._get_or_create_spark_context(request_id)

        # Step 3: 依赖门禁
        self._check_stage_dependencies(stage, context, artifacts)

        # Step 4: 执行阶段
        try:
            if stage == SparkPipelineStage.MAPPER:
                self._do_spark_map(artifacts, context)
            elif stage == SparkPipelineStage.DEVELOPER:
                self._do_spark_develop(context)
            elif stage == SparkPipelineStage.COMPILER:
                self._do_spark_compile(context)
            elif stage == SparkPipelineStage.VALIDATOR:
                self._do_spark_validate(context)
            elif stage == SparkPipelineStage.COMPARATOR:
                self._do_spark_compare(artifacts, context)
            elif stage == SparkPipelineStage.PHYSICAL_VERIFIER:
                self._do_spark_physical_verify(artifacts, context)
        except Exception as e:
            context.stage_results[stage_val] = "FAILURE"
            context.errors.append(f"[{stage_val}] 异常：{e}")

        # Step 5: 构建响应
        status_map = {
            "SUCCESS": "ok",
            "FAILURE": "failed",
            "SKIPPED": "skipped",
            "NOT_EXECUTED": "skipped",
        }
        spark_stages: list[dict] = []
        for s_name, s_result in context.stage_results.items():
            spark_stages.append({
                "stage": s_name,
                "status": status_map.get(s_result, "skipped"),
            })

        current_status = status_map.get(
            context.stage_results.get(stage_val, "NOT_EXECUTED"), "skipped"
        )

        # ── Phase 8: DEVELOPER 结果构建（含标注数据）──
        result: dict | None = None
        if stage == SparkPipelineStage.DEVELOPER:
            if current_status == "ok" and context.annotation_result is not None:
                ann = context.annotation_result
                result = {
                    "type": "developer",
                    "message": f"LLM 语义标注完成——{len(ann.annotations)} 个步骤",
                    "annotation_count": len(ann.annotations),
                    "annotations": [
                        {
                            "step_id": a.step_id,
                            "intent": a.intent.value if hasattr(a.intent, "value") else str(a.intent),
                            "intent_detail": a.intent_detail,
                            "operation_summary": a.operation_summary,
                        }
                        for a in ann.annotations
                    ],
                    "warnings": [
                        {
                            "warning_id": w.warning_id,
                            "severity": w.severity,
                            "description": w.description,
                        }
                        for w in ann.warnings
                    ],
                }
            else:
                result = {
                    "type": "developer",
                    "message": (
                        "LLM 语义标注失败"
                        if current_status == "failed"
                        else "LLM 语义标注阶段——未注入 SparkDeveloperService，已标记 SKIPPED"
                    ),
                    "skipped": current_status == "skipped",
                }

        return {
            "request_id": request_id,
            "stage": stage_val,
            "status": current_status,
            "missing_dependencies": [],
            "errors": list(context.errors),
            "spark_stages": spark_stages,
            "llm_traces": self._get_llm_traces(request_id),
            "result": result,
        }

    # ════════════════════════════════════════════
    # 各阶段私有实现方法
    # ════════════════════════════════════════════

    def _do_spark_map(
        self, artifacts: PipelineArtifactBundle, context: SparkStageContext,
    ) -> None:
        """执行 MAPPER 阶段——Contract → SparkPlan。"""
        from tianshu_datadev.artifacts.models import DataTransformContractV1
        from tianshu_datadev.spark.contract_adapter import adapt_lite_to_v1
        from tianshu_datadev.spark.mapper import map_contract_to_spark_plan

        raw_contract = artifacts.data_transform_contract
        if isinstance(raw_contract, DataTransformContractV1):
            v1_contract = raw_contract
        else:
            v1_contract = adapt_lite_to_v1(raw_contract)

        result = map_contract_to_spark_plan(v1_contract)
        if result.success and result.spark_plan is not None:
            context.spark_plan = result.spark_plan
            context.stage_results["MAPPER"] = "SUCCESS"
        else:
            context.stage_results["MAPPER"] = "FAILURE"
            gap_msgs = [g.message for g in result.gaps] if result.gaps else ["未知错误"]
            context.errors.append(f"[MAPPER] 映射失败：{'; '.join(gap_msgs)}")

    def _do_spark_develop(self, context: SparkStageContext) -> None:
        """执行 DEVELOPER 阶段——LLM 语义标注。

        Phase 8: 注入 SparkDeveloperService 后调用真实 LLM 标注，
        异常时标记 FAILURE，不阻断后续阶段。
        """
        if self._spark_developer_service is None:
            context.stage_results["DEVELOPER"] = "SKIPPED"
            context.errors.append("[DEVELOPER] SKIPPED: 未注入 SparkDeveloperService")
            return

        if context.spark_plan is None:
            context.stage_results["DEVELOPER"] = "SKIPPED"
            context.errors.append("[DEVELOPER] SKIPPED: 无 SparkPlan（MAPPER 未执行或失败）")
            return

        try:
            annotated = self._spark_developer_service.annotate(context.spark_plan)
            context.annotation_result = annotated
            context.stage_results["DEVELOPER"] = "SUCCESS"
        except Exception as e:
            context.stage_results["DEVELOPER"] = "FAILURE"
            context.errors.append(f"[DEVELOPER] 标注异常：{e}")

    def _do_spark_compile(self, context: SparkStageContext) -> None:
        """执行 COMPILER 阶段——SparkPlan → PySpark DSL。"""
        from tianshu_datadev.spark.compiler import SparkCompiler

        compiler = SparkCompiler()
        result = compiler.compile(context.spark_plan)
        context.compile_result = result
        context.stage_results["COMPILER"] = "SUCCESS"

    def _do_spark_validate(self, context: SparkStageContext) -> None:
        """执行 VALIDATOR 阶段——PySpark DSL 安全校验。"""
        from tianshu_datadev.spark.validator import SparkStaticValidator

        validator = SparkStaticValidator()
        validation = validator.validate(context.compile_result.raw_pyspark)
        if validation.is_valid:
            context.stage_results["VALIDATOR"] = "SUCCESS"
        else:
            context.stage_results["VALIDATOR"] = "FAILURE"
            for e in validation.errors:
                context.errors.append(f"[VALIDATOR] {e.error_code}: {e.detail}")

    def _do_spark_compare(
        self,
        artifacts: PipelineArtifactBundle,
        context: SparkStageContext,
    ) -> None:
        """执行 COMPARATOR 阶段——SQL ↔ Spark 逻辑对比。"""
        from tianshu_datadev.spark.plan_comparator import PlanComparator

        comparator = PlanComparator()
        sql_plan = artifacts.sql_build_plan
        sql_program = artifacts.sql_program

        if sql_program is not None:
            target_grain = None
            raw_contract = artifacts.data_transform_contract
            if raw_contract is not None and hasattr(raw_contract, "grouping_keys"):
                target_grain = (
                    raw_contract.grouping_keys
                    if raw_contract.grouping_keys
                    else None
                )
            report = comparator.compare_program(
                sql_program, context.spark_plan,
                target_grain=target_grain,
            )
        elif sql_plan is not None:
            report = comparator.compare(sql_plan, context.spark_plan)
        else:
            context.stage_results["COMPARATOR"] = "SKIPPED"
            context.errors.append("[COMPARATOR] SKIPPED: 无 SqlBuildPlan/SqlProgram，无法执行逻辑对比")
            return

        context.comparator_report = report
        context.stage_results["COMPARATOR"] = "SUCCESS"

    def _do_spark_physical_verify(
        self, artifacts: PipelineArtifactBundle, context: SparkStageContext,
    ) -> None:
        """执行 PHYSICAL_VERIFIER 阶段——双引擎物理结果对比。

        当前需要 Spark 运行时环境，标记 SKIPPED。
        """
        context.stage_results["PHYSICAL_VERIFIER"] = "SKIPPED"
        context.errors.append(
            "[PHYSICAL_VERIFIER] SKIPPED: 需要 Spark 运行时环境，当前未配置"
        )


def _safe_enum_value(obj, attr: str) -> str:
    """安全获取枚举属性的字符串值——兼容 Enum 和普通属性。"""
    val = getattr(obj, attr, "")
    if hasattr(val, "value"):
        return val.value
    return str(val)


# ════════════════════════════════════════════
# Spark 阶段独立触发——上下文缓存与异常
# ════════════════════════════════════════════


@dataclass
class SparkStageContext:
    """request_id 级别的 Spark 阶段中间产物缓存。

    由 Pipeline._get_or_create_spark_context() 创建和管理，
    独立于 SparkOrchestrator 的内部缓存。
    """
    spark_plan: "SparkPlan | None" = None
    compile_result: "SparkCompileResult | None" = None
    comparator_report: "PlanComparisonReport | None" = None
    # ── Phase 8: DEVELOPER 阶段产物缓存 ──
    annotation_result: "AnnotatedSparkPlan | None" = None
    stage_results: dict[str, str] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)


class SparkDependencyMissingError(Exception):
    """Spark 阶段依赖缺失异常——由 _check_stage_dependencies 抛出。

    当用户跳过前置阶段直接触发后续阶段时抛出。
    routes.py 的 _handle_spark_stage() 捕获此异常返回 422。
    """
    def __init__(self, stage: "SparkPipelineStage", missing: list[str]):
        self.stage = stage
        self.missing = missing
        super().__init__(
            f"阶段 {stage.value} 缺少前置产物：{', '.join(missing)}"
        )


# ── Phase 9A1: 延迟重建 PipelineArtifactBundle——确保 Contract 类型已导入 ──
from tianshu_datadev.artifacts.models import (  # noqa: E402
    DataTransformContractLite,
    DataTransformContractV1,
)
from tianshu_datadev.spark.snapshot import SnapshotManifest  # noqa: E402

PipelineArtifactBundle.model_rebuild()
