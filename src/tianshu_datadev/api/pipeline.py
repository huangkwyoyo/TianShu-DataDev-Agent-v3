"""Pipeline——确定性串联全部组件的执行流水线。

所有步骤使用确定性实现，不需要真实 LLM 或生产数据库。
每次调用独立创建组件实例，无状态泄漏。
API 只返回 artifact 引用和结构化摘要。
"""

from __future__ import annotations

import hashlib
import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tianshu_datadev.cre_models import CreShadowReport

from tianshu_datadev.api.streaming import _sanitize_stream_error
from tianshu_datadev.api.templates import TEMPLATES
from tianshu_datadev.artifacts.contract_extractor import DataTransformContractExtractor
from tianshu_datadev.artifacts.packager import PackageInputs, ReviewPackageBuilder
from tianshu_datadev.developer_spec.models import (
    DatasetType,
    OpenQuestion,
    ParsedDeveloperSpec,
    RequirementPlannerOutput,
    RequirementProposal,
    StrictModel,
    UncertaintyEntry,
)
from tianshu_datadev.developer_spec.parser import DeveloperSpecParser
from tianshu_datadev.developer_spec.source_manifest import build_manifest_from_spec
from tianshu_datadev.labels.resolver import _find_unresolved_derived_columns
from tianshu_datadev.llm.models import LlmResponse, LlmTraceNode
from tianshu_datadev.monitor import get_collector
from tianshu_datadev.planning.cross_validator import cross_validate
from tianshu_datadev.planning.models import ColumnRef
from tianshu_datadev.planning.program_factory import (
    build_sql_program,
    build_sql_program_from_chain,
    build_sql_program_from_compute_steps,
)
from tianshu_datadev.planning.proposal_promotion import ProposalPromotion
from tianshu_datadev.planning.proposal_validator import ProposalValidator
from tianshu_datadev.planning.relationship_planner import RelationshipPlanner
from tianshu_datadev.planning.requirement_planner import RequirementPlanner
from tianshu_datadev.planning.spec_enricher import SpecEnricher
from tianshu_datadev.planning.sql_build_plan import SqlBuildPlan, SqlBuildPlanBuilder
from tianshu_datadev.planning.sql_program import SqlProgram
from tianshu_datadev.sql.compiler import DuckDbSqlCompiler
from tianshu_datadev.sql.executor import DuckDBExecutor
from tianshu_datadev.sql.models import (
    CompiledSql,
    ExecutionStatus,
    ExecutionTrace,
    ProgramCompiledSql,
    ResultSummary,
    SqlArtifact,
    SqlProgramArtifact,
)
from tianshu_datadev.sql.validator import SqlBuildPlanValidator

if TYPE_CHECKING:
    from tianshu_datadev.artifacts.models import DataTransformContractLite, DataTransformContractV1
    from tianshu_datadev.developer_spec.models import OpenQuestion, ParseWarning, SourceManifest
    from tianshu_datadev.llm.adapters.base import ProviderAdapter
    from tianshu_datadev.planning.relationship_hypothesis import RelationshipHypothesis
    from tianshu_datadev.planning.sql_build_plan import SqlBuildPlan
    from tianshu_datadev.planning.sql_program import SqlProgram
    from tianshu_datadev.spark.annotations import AnnotatedSparkPlan
    from tianshu_datadev.spark.compiler import SparkCompileResult
    from tianshu_datadev.spark.models import SparkPlan, SparkStep
    from tianshu_datadev.spark.physical_verifier import PhysicalVerificationReport
    from tianshu_datadev.spark.plan_comparator import ComparisonStatus, PlanComparisonReport
    from tianshu_datadev.spark.snapshot import SnapshotBuilder, SnapshotManifest, SnapshotSourceProvider
    from tianshu_datadev.sql.models import CompiledSql, ExecutionTrace, ResultSummary

from tianshu_datadev.spark.orchestrator import SparkPipelineStage
from tianshu_datadev.spark.physical_verifier import PhysicalVerificationStatus

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


def _aliases_from_table_mapping(table_mapping: dict[str, str] | None) -> dict[str, str]:
    """把 {别名: 物理表名} 反转为 {物理表名: 别名}。

    供 SnapshotBuilder.build() 的 table_aliases 参数使用，
    让快照 source_name（_inputs_index.json 的 key）与 PySpark 代码中的别名对齐。

    多别名映射到同一物理表时后者覆盖前者（正常 1:1，不预期冲突）。
    """
    if not table_mapping:
        return {}
    return {physical: alias for alias, physical in table_mapping.items()}


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
    compiled_program: ProgramCompiledSql | None = None
    execution_trace: ExecutionTrace | None = None
    result_summary: ResultSummary | None = None
    # ── Phase 9B-P0: Snapshot 集成 ──
    snapshot_manifest: SnapshotManifest | None = None
    # ── Phase 10: Case06 SqlProgram 多语句 DAG ──
    sql_program: SqlProgram | None = None
    # ── Final Hardening: SqlProgram 执行 cleanup 状态 ──
    program_cleanup_status: str | None = None   # "success" | "partial_failure"
    program_cleanup_error: str | None = None     # cleanup 阶段的错误信息（成功时为空）


class LabelTableConfigError(Exception):
    """label_table 配置或提取失败——禁止静默回退，必须返回结构化错误。

    触发条件：
    - label_extractor 未注入（缺少 API Key）
    - LLM 调用异常
    - LLM 未返回任何标签规则候选
    """


class ConfigError(Exception):
    """Pipeline 配置错误——缺少 LLM Adapter 或 Planner 后仍有未解析列导致的阻断。"""


def _gen_uuid() -> str:
    """生成确定性短 UUID——用于 Proposal 标识。"""
    return uuid.uuid4().hex[:12]


def _get_output_kind(
    column_name: str,
    uncertainties: list[UncertaintyEntry],
) -> str:
    """查询 Planner 对未解析列的分类。默认返回 "UNKNOWN"。

    路由规则：
    1. 精确匹配 output_column（路由主键）
    2. output_column 为 None → 跳过
    3. 不解析 field_ref 字符串
    4. 无匹配 → "UNKNOWN"
    """
    for u in uncertainties:
        if u.output_column is not None and u.output_column == column_name:
            return u.output_kind
    return "UNKNOWN"


def _check_label_rule_conflicts(spec: ParsedDeveloperSpec) -> list[OpenQuestion]:
    """检查 label_rules 和 case_when_rules 的 output_column 冲突。

    同一 output_column 出现两条不同规则 → blocking OpenQuestion。
    不根据 evaluation_phase 猜测来源——每条冲突都必须人工裁决。
    """
    label_cols = {r.output_column for r in spec.label_rules}
    cw_cols = {r.output_column for r in spec.case_when_rules}
    overlap = label_cols & cw_cols

    if not overlap:
        return []

    return [
        OpenQuestion(
            question_id=f"LABEL_CONFLICT_{col}",
            source="label_conflict",
            field_ref=col,
            description=(
                f"输出列 '{col}' 在 label_rules 和 case_when_rules "
                f"中存在两条不同规则——需人工裁决保留哪一条"
            ),
            blocking=True,
        )
        for col in sorted(overlap)
    ]


def _merge_uncertainties(
    existing: list[UncertaintyEntry],
    incoming: list[UncertaintyEntry],
) -> list[UncertaintyEntry]:
    """确定性合并 uncertainties——按 (output_column, field_ref) 去重。

    新项覆盖同键旧项（Planner 最新输出优先），保留其他旧项。
    不整体覆盖——避免丢弃与其他组件写入的诊断信息。
    """
    if not incoming:
        return list(existing)

    merged: dict[tuple[str | None, str], UncertaintyEntry] = {}
    for u in existing:
        key = (u.output_column, u.field_ref)
        merged[key] = u
    for u in incoming:
        key = (u.output_column, u.field_ref)
        merged[key] = u  # 同键覆盖

    return list(merged.values())


def _apply_uncertainties_to_spec(
    spec: ParsedDeveloperSpec,
    uncertainties: list[UncertaintyEntry],
) -> ParsedDeveloperSpec:
    """确定性合并 Planner 分类结果到 spec。

    即使 Validator 失败也保留——artifact 审查需要分类证据。
    使用 _merge_uncertainties 而非整体覆盖。
    """
    if not uncertainties:
        return spec
    merged = _merge_uncertainties(spec.uncertainties, uncertainties)
    return spec.model_copy(update={"uncertainties": merged})


def _extract_case_when_parse_errors(
    planner_output: RequirementPlannerOutput,
) -> list[OpenQuestion]:
    """从 Planner 输出的 uncertainties 中提取 CASE WHEN 解析失败，转为阻断 OpenQuestion。

    约定：_parse_response 在逐规则解析 CASE WHEN 时，将失败规则的 UncertaintyEntry
    以 field_ref="case_when_rules.parse_error.<output_column>" 格式记录。
    此函数提取这些条目并转为阻断级 OpenQuestion。
    """
    questions: list[OpenQuestion] = []
    for u in planner_output.uncertainties:
        if u.field_ref.startswith("case_when_rules.parse_error."):
            questions.append(OpenQuestion(
                question_id="CW_PARSE_ERROR",
                source="requirement_planner",
                field_ref=u.field_ref,
                description=u.description,
                blocking=True,
            ))
    return questions


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
        # ── v4-light 最终版: LabelExtractor 注入（label_table 必需）──
        label_extractor=None,  # LabelExtractor | None——None 时 label_table 请求报错
        # ── v3.1: RequirementPlanner 注入（可选）──
        requirement_planner: RequirementPlanner | None = None,
    ):
        """初始化流水线。

        Args:
            base_output_dir: ReviewPackage 输出根目录
            adapter: LLM Provider 适配器——None 时全链路确定性运行（Fake 模式），
                     注入后 RequirementPlanner + RelationshipPlanner + SpecEnricher 均走 LLM 推断。
            default_table_paths: 默认表名→CSV 路径映射——当 API 调用未显式传入
                                 table_paths 时使用此回退值。E2E 测试环境用。
            duckdb_path: 外部 DuckDB 数据库文件路径——ATTACH 后自动创建 schema VIEW
                         桥接，使模板引用的 gold/silver 表可直接查询
            label_extractor: 标签提取器——label_table 类型 Spec 的处理入口。
                            None 时 label_table 请求返回结构化错误（禁止静默回退）。
            requirement_planner: RequirementPlanner 实例——None 时跳过 Planner 阶段。
        """
        self._base_output_dir = base_output_dir
        self._results: dict[str, dict] = {}  # request_id → 内部产物
        self._packages: dict[str, object] = {}  # request_id → ReviewPackageManifest
        self._timestamps: dict[str, float] = {}  # request_id → 写入时间戳（用于 TTL 过期清理）
        self._ttl_seconds: int = 1800  # 缓存过期时间（秒），默认 30 分钟
        # ── v3.1: 保存 adapter 引用——供 ConfigError 的条件判断 ──
        self._adapter = adapter
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
        # ── v4-light 最终版: LabelExtractor 注入 ──
        self._label_extractor = label_extractor
        # ── Spark 阶段独立触发——上下文缓存 ──
        self._spark_contexts: dict[str, SparkStageContext] = {}
        # ── LLM 调用追踪（request-scoped cache）──
        self._llm_traces: dict[str, dict[str, LlmTraceNode]] = {}
        # ── 标签 Artifact 追踪——独立存储，不被 _store_result 覆盖 ──
        self._label_artifacts: dict[str, dict] = {}
        # ── v3.1: RequirementPlanner 管线集成 ──
        self._requirement_planner = requirement_planner
        self._proposal_validator = ProposalValidator()
        self._proposal_promotion = ProposalPromotion()

    def inject_snapshot_deps(
        self,
        snapshot_builder: SnapshotBuilder,
        snapshot_provider: SnapshotSourceProvider,
    ) -> None:
        """注入 SnapshotBuilder + SnapshotSourceProvider——供 create_app 延迟注入。

        Pipeline.__init__ 中这两个依赖默认为 None，因为生产环境不需要快照功能。
        E2E 测试模式下，create_app 发现 CSV fixture 文件后通过此方法注入。

        Args:
            snapshot_builder: SnapshotBuilder 实例
            snapshot_provider: SnapshotSourceProvider 实例（白名单来自显式配置）
        """
        self._snapshot_builder = snapshot_builder
        self._snapshot_provider = snapshot_provider
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
            self._label_artifacts.pop(rid, None)
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

    def get_label_artifacts(self, request_id: str) -> dict | None:
        """获取指定 request_id 的标签提取和提升 Artifact。

        供 API 响应追溯用——返回 extraction 和 promotion artifact。
        仅 label_table 类型 Spec 才有此数据。

        Args:
            request_id: Pipeline 请求 ID

        Returns:
            {"extraction": LabelExtractionArtifact, "promotion": LabelPromotionArtifact}
            或 None（无标签数据时）
        """
        return self._label_artifacts.get(request_id)

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
        """统一入口：RequirementPlanner → SpecEnricher(full) → unresolved 检查 → RelationshipPlanner。

        v3.1 执行顺序反转——RequirementPlanner 先执行解出派生列，SpecEnricher 后执行完整 scope，
        最后统一 unresolved 检查 + 交叉验证。

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

        extra_questions: list[OpenQuestion] = []

        # ── 1. RequirementPlanner：有 Adapter 时先执行（v3.1 反转）──
        if self._requirement_planner is not None:
            unresolved_before = _find_unresolved_derived_columns(spec)
            if unresolved_before:
                spec, planner_questions = self._run_requirement_planner(spec, manifest)
                extra_questions.extend(planner_questions)

        # ── 2. SpecEnricher：完整 scope，后执行 ──
        spec = self._spec_enricher.apply_enrichment(spec, manifest)

        # ── 2.5. 标签规则处理——合并候选 + Extractor + Validator + Promotion
        spec = self._prepare_labels(spec, manifest)

        # ── 3. 统一 unresolved 检查（跳过有 compute_steps 的 spec——build 阶段自行解析）──
        unresolved_after = _find_unresolved_derived_columns(spec)
        has_compute_steps = bool(spec.compute_steps)
        if unresolved_after and not has_compute_steps:
            if self._adapter is None:
                raise ConfigError(
                    f"以下输出列无法解析且无 LLM Adapter 可用：{unresolved_after}"
                )
            else:
                raise ConfigError(
                    f"RequirementPlanner + SpecEnricher 后仍存在未解析列: {unresolved_after}"
                )

        # ── 4. RelationshipPlanner ──
        hypothesis = None
        if len(spec.input_tables) > 1:
            hypothesis, rel_questions = self._relationship_planner.plan(spec, manifest)
            extra_questions.extend(rel_questions)
            # 交叉验证——指标推断 vs Join 推断一致性检查
            if hypothesis:
                xv_questions = cross_validate(spec, hypothesis, manifest)
                extra_questions.extend(xv_questions)

        return spec, hypothesis, extra_questions, table_mapping or {}

    def _run_requirement_planner(
        self,
        spec: ParsedDeveloperSpec,
        manifest: SourceManifest,
    ) -> tuple[ParsedDeveloperSpec, list[OpenQuestion]]:
        """执行 RequirementPlanner → ProposalValidator → ProposalPromotion 流水线。

        Args:
            spec: 已解析的 DeveloperSpec
            manifest: 源数据清单

        Returns:
            (更新后的 spec, 验证产生的 OpenQuestion 列表)
        """
        t0 = time.monotonic()

        planner_output = self._requirement_planner.plan(spec, manifest)
        elapsed_ms = int((time.monotonic() - t0) * 1000)

        proposal = RequirementProposal(
            proposal_id=_gen_uuid(),
            spec_hash=spec.spec_hash,
            dimensions=planner_output.dimensions,
            derived_dimensions=planner_output.derived_dimensions,
            metrics=planner_output.metrics,
            case_when_rules=planner_output.case_when_rules,
            uncertainties=planner_output.uncertainties,
            llm_model=self._adapter.provider_name() if self._adapter else "",
            inference_time_ms=elapsed_ms,
            total_inferred=(len(planner_output.dimensions)
                            + len(planner_output.derived_dimensions)
                            + len(planner_output.metrics)
                            + len(planner_output.case_when_rules)),
        )

        # CASE WHEN 逐规则解析失败 → 阻断级 OpenQuestion
        case_when_parse_errors = _extract_case_when_parse_errors(planner_output)
        if case_when_parse_errors:
            spec = _apply_uncertainties_to_spec(spec, planner_output.uncertainties)
            return spec, case_when_parse_errors

        valid, questions = self._proposal_validator.validate(proposal, spec, manifest)
        if not valid:
            spec = _apply_uncertainties_to_spec(spec, proposal.uncertainties)
            return spec, questions

        spec = self._proposal_promotion.promote(proposal, spec)
        return spec, questions

    def _prepare_labels(
        self, spec: ParsedDeveloperSpec, manifest,
    ) -> ParsedDeveloperSpec:
        """统一标签规则处理——在 Planner/Enricher 之后执行。

        两条独立路径：
        A) case_when_rules → ProposalValidator → spec.case_when_rules（已在 Planner 中处理）
        B) LabelExtractor Proposal → LabelRuleValidator → Promotion → spec.label_rules

        LabelExtractor 仅在以下条件全部满足时调用（修正 I1）：
        1. dataset_type == LABEL_TABLE
        2. Planner 标记 output_kind=LABEL
        3. 列仍 unresolved
        4. Planner 未生成该列的 case_when_rules

        最后做覆盖冲突检查——同 output_column 两条规则 → blocking OpenQuestion。
        """
        # ── 路径 A：case_when_rules 已由 Planner 写入——无需额外处理

        # ── 路径 B：LabelExtractor fallback（严格兜底条件）
        if spec.dataset_type == DatasetType.LABEL_TABLE:
            unresolved = _find_unresolved_derived_columns(spec)
            if unresolved:
                # 获取 Planner 已覆盖的输出列集合
                planner_covered_cols = {r.output_column for r in spec.case_when_rules}
                planner_covered_cols.update(r.output_column for r in spec.label_rules)

                # 仅处理 Planner 标记为 LABEL、仍 unresolved、且 Planner 未生成规则的列
                label_candidates = [
                    col for col in unresolved
                    if _get_output_kind(col, spec.uncertainties) == "LABEL"
                    and col not in planner_covered_cols
                ]
                if label_candidates:
                    if self._label_extractor is None:
                        raise LabelTableConfigError(
                            "label_table 需要 LlmLabelExtractor，但未配置——"
                            "请设置 DEEPSEEK_API_KEY 环境变量"
                        )
                    proposals, extraction_artifact = self._label_extractor.extract(
                        spec, label_candidates,
                    )
                    if proposals:
                        from tianshu_datadev.labels.label_rule_validator import (
                            LabelRuleValidator,
                        )
                        from tianshu_datadev.labels.promotion import Promotion
                        validator = LabelRuleValidator()
                        reports = [validator.validate(p, spec) for p in proposals]
                        promoter = Promotion()
                        promoted, promotion_artifact = promoter.promote(
                            spec.spec_hash, proposals, reports, extraction_artifact,
                        )
                        spec = spec.model_copy(update={
                            "label_rules": spec.label_rules + promoted,
                        })
                        request_id = self._gen_request_id(spec)
                        self._label_artifacts[request_id] = {
                            "extraction": extraction_artifact,
                            "promotion": promotion_artifact,
                        }

        # ── 覆盖冲突检查——同 output_column 两条规则 → blocking OpenQuestion
        conflict_questions = _check_label_rule_conflicts(spec)
        if conflict_questions:
            spec = spec.model_copy(update={
                "open_questions": spec.open_questions + conflict_questions,
            })

        # ── label_table 门禁：至少一个合法标签列
        if spec.dataset_type == DatasetType.LABEL_TABLE:
            has_labels = bool(spec.label_rules) or bool(spec.case_when_rules)
            if not has_labels:
                raise LabelTableConfigError(
                    "label_table 至少需要一个合法标签列——"
                    "label_rules 和 case_when_rules 均为空"
                )

        return spec

    def _parse_and_enrich(
        self,
        method: str,
        markdown_text: str,
        table_mapping: dict | None = None,
        *,
        pipeline_stages: list[str] | None = None,
        collector=None,
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
        if collector is None:
            collector = get_collector()
        # ── Stage 1: Parser ──
        try:
            parser = DeveloperSpecParser()
            with collector.stage("sql_parser", "") as ctx:
                spec = parser.parse(markdown_text)
                ctx.set_result(artifact_path=f"spec/{spec.spec_hash[:12]}")
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
        request_id = self._gen_request_id(spec)
        try:
            with collector.stage("sql_enricher", request_id) as ctx:
                spec, hypothesis, extra_questions, table_mapping = self._enrich_and_plan(
                    spec, manifest, table_mapping,
                )
                ctx.set_result(artifact_path=f"spec/{spec.spec_hash[:12]}/enriched")
        except Exception as e:
            self._log_stage_failure(method, "enrich", e)
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
            stage: 失败阶段标识（parser/enrich/build/compile/contract/snapshot/execute）
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
            stage: 失败阶段标识（parser/enrich/build/compile/contract/snapshot/execute/package）
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
            all_stages: 完整阶段列表（默认 6 阶段，run_all 用 9 阶段）

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
            "contract": "契约",
            "snapshot": "快照",
            "execute": "执行",
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
            plan: 已构建的 SqlBuildPlan（enrich 阶段 blocking 门禁触发时为 None——尚未构建）
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
            "plan_id": plan.plan_id if plan else "",
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

    @staticmethod
    def _build_snapshot_time_filter(spec: ParsedDeveloperSpec | None):
        """将已验证时间范围转换为封闭的锚表快照过滤规格。"""
        if spec is None or spec.time_range is None:
            return None

        from datetime import date, timedelta

        from tianshu_datadev.spark.snapshot import (
            SnapshotMaterializationError,
            SnapshotTimeFilter,
        )

        time_range = spec.time_range
        if time_range.relative_range:
            raise SnapshotMaterializationError(
                "相对时间范围暂不支持确定性快照，请改用固定 start/end"
            )

        start = time_range.start
        end = time_range.end
        if time_range.calendar_type != "calendar" and time_range.fiscal_year:
            fiscal_year = time_range.fiscal_year
            if time_range.calendar_type == "fiscal_jul":
                start, end = f"{fiscal_year}-07-01", f"{fiscal_year + 1}-06-30"
            elif time_range.calendar_type == "fiscal_apr":
                start, end = f"{fiscal_year}-04-01", f"{fiscal_year + 1}-03-31"

        if not start or not end:
            raise SnapshotMaterializationError("时间范围缺少固定 start/end")

        column = time_range.column_ref
        if not column:
            time_field_tables = [
                table for table in spec.input_tables if table.time_field
            ]
            if len(time_field_tables) == 1:
                column = time_field_tables[0].time_field or ""
        candidates = []
        for table in spec.input_tables:
            declared_columns = {
                item.column_name
                for group in (table.columns, table.key_columns, table.business_columns)
                for item in group
            }
            if table.time_field == column or column in declared_columns:
                candidates.append(table)
        if len(candidates) > 1:
            fact_candidates = [table for table in candidates if table.role == "fact"]
            candidates = fact_candidates if len(fact_candidates) == 1 else candidates
        if not candidates and len(spec.input_tables) == 1:
            candidates = [spec.input_tables[0]]
        if len(candidates) != 1:
            raise SnapshotMaterializationError(
                f"无法唯一确定时间字段 '{column}' 所属的快照锚表"
            )

        end_operator = "LTE"
        try:
            if len(start) == 10 and len(end) == 10:
                date.fromisoformat(start)
                end = (date.fromisoformat(end) + timedelta(days=1)).isoformat()
                end_operator = "LT"
        except ValueError:
            end_operator = "LTE"

        return SnapshotTimeFilter(
            table_alias=candidates[0].table_alias,
            column=column,
            start=start,
            end=end,
            end_operator=end_operator,
        )

    def _prepare_run_all_snapshot(
        self,
        *,
        contract,
        table_mapping: dict[str, str],
        table_paths: dict[str, str] | None,
        spec: ParsedDeveloperSpec | None = None,
    ) -> tuple[SnapshotManifest | None, dict[str, str]]:
        """在 SQL 执行前准备同源快照，并返回 Executor 的表路径。"""
        from pathlib import Path

        from tianshu_datadev.artifacts.models import (
            DataTransformContractLite,
            DataTransformContractV1,
        )
        from tianshu_datadev.spark.snapshot import (
            SNAPSHOT_DEFAULT_ROW_LIMIT,
            SamplingSpec,
            SnapshotBuilder,
        )

        resolved_paths = self._resolve_table_paths(table_paths)
        contract_hash = (
            DataTransformContractV1.compute_contract_hash(contract)
            if isinstance(contract, DataTransformContractV1)
            else DataTransformContractLite.compute_contract_hash(contract)
        )

        physical_tables: list[str] = []
        physical_to_alias: dict[str, str] = {}
        physical_to_aliases: dict[str, list[str]] = {}
        for input_table in contract.input_tables:
            physical = (
                table_mapping.get(input_table.table_ref)
                or table_mapping.get(input_table.source_table)
                or input_table.source_table
            )
            if physical not in physical_tables:
                physical_tables.append(physical)
                physical_to_alias[physical] = input_table.table_ref
            physical_to_aliases.setdefault(physical, []).append(input_table.table_ref)

        if self._duckdb_path is not None:
            if not physical_tables:
                raise RuntimeError("Snapshot 构建失败：Contract 未声明输入表")
            anchor_time_filter = self._build_snapshot_time_filter(spec)
            output_dir = str(
                Path(__file__).resolve().parents[3]
                / ".tianshu_cache"
                / "snapshots"
            )
            snapshot_executor = DuckDBExecutor(
                duckdb_path=self._duckdb_path,
                memory_limit="1GB",
                threads=1,
                max_temp_directory_size="4GB",
                process_memory_limit_mb=1536,
            )
            snapshot_manifest = snapshot_executor.materialize_snapshot(
                output_dir=output_dir,
                contract_hash=contract_hash,
                source_tables=physical_tables,
                joins=[join.model_dump(mode="json") for join in contract.join_relationships],
                table_aliases=physical_to_alias,
                table_role_aliases=physical_to_aliases,
                sampling=SamplingSpec(
                    mode="head",
                    limit=SNAPSHOT_DEFAULT_ROW_LIMIT,
                ).model_dump(mode="json"),
                anchor_time_filter=(
                    anchor_time_filter.model_dump(mode="json")
                    if anchor_time_filter is not None else None
                ),
            )
            alias_targets = {
                alias: physical_to_alias[physical]
                for physical, aliases in physical_to_aliases.items()
                for alias in aliases[1:]
            }
            if alias_targets:
                SnapshotBuilder._write_inputs_index(
                    snapshot_manifest.snapshot_dir,
                    snapshot_manifest.files,
                    alias_targets=alias_targets,
                )
            files_by_alias = {
                snapshot_file.source_name: snapshot_file.file_path
                for snapshot_file in snapshot_manifest.files
            }
            snapshot_paths = {
                physical: files_by_alias[alias]
                for physical, alias in physical_to_alias.items()
            }
            return snapshot_manifest, snapshot_paths

        # 本地 fixture 文件本身就是受控开发数据；可选 Builder 仅生成审计副本。
        snapshot_manifest = None
        if self._snapshot_builder is not None and self._snapshot_provider is not None:
            allowlisted = set(self._snapshot_provider.allowlisted_tables)
            source_tables = [table for table in resolved_paths if table in allowlisted]
            if source_tables:
                snapshot_manifest = self._snapshot_builder.build(
                    contract_hash=contract_hash,
                    source_tables=source_tables,
                    provider=self._snapshot_provider,
                    table_aliases=physical_to_alias,
                )
                alias_targets = {
                    alias: physical_to_alias[physical]
                    for physical, aliases in physical_to_aliases.items()
                    for alias in aliases[1:]
                }
                if alias_targets:
                    SnapshotBuilder._write_inputs_index(
                        snapshot_manifest.snapshot_dir,
                        snapshot_manifest.files,
                        alias_targets=alias_targets,
                    )
        return snapshot_manifest, resolved_paths

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
        collector = get_collector()
        # ── Stage: parser ──
        try:
            parser = DeveloperSpecParser()
            with collector.stage("sql_parser", "") as ctx:
                spec = parser.parse(markdown_text)
                ctx.set_result(artifact_path=f"spec/{spec.spec_hash[:12]}")
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
        collector = get_collector()
        # ── Stage 1-2: Parser + Enrich ──
        parsed = self._parse_and_enrich(
            "build_plan", markdown_text, table_mapping,
            collector=collector,
        )
        if not parsed["ok"]:
            return parsed["error_response"]
        spec = parsed["spec"]
        manifest = parsed["manifest"]
        hypothesis = parsed["hypothesis"]
        extra_questions = parsed["extra_questions"]
        table_mapping = parsed["table_mapping"]
        request_id = self._gen_request_id(spec)

        # ── Stage 3: Build + Validate ──
        plan = None
        plan_questions: list = []
        try:
            builder = SqlBuildPlanBuilder()

            if spec.compute_steps and len(spec.compute_steps) > 0:
                # ── ComputeSteps 路径：每步独立聚合 Plan，_temp 串联 ──
                with collector.stage("sql_builder", request_id) as ctx:
                    plans = builder.build_from_steps(spec, hypothesis)
                    plan_snap = plans[-1]
                    ctx.set_result(artifact_path=f"plan/{plan_snap.plan_id}")
                chain_id = hashlib.md5(
                    "|".join(s.step_name for s in spec.compute_steps).encode()
                ).hexdigest()[:8]
                sql_program = build_sql_program_from_compute_steps(
                    plans, spec, chain_id
                )
                validator = SqlBuildPlanValidator()
                with collector.stage("sql_validator", request_id):
                    passed, val_questions = validator.validate_multi_hop_chain(sql_program)
                plan = plans[-1]
                plan_questions = []
            elif hypothesis and len(hypothesis.candidates) > 1:
                # ── 多跳链路径：每对候选独立 Plan，_temp 串联 ──
                with collector.stage("sql_builder", request_id) as ctx:
                    plans = builder.build_multi(spec, hypothesis)
                    plan_snap = plans[-1]
                    ctx.set_result(artifact_path=f"plan/{plan_snap.plan_id}")
                chain_id = hashlib.md5(
                    "|".join(c.candidate_id for c in hypothesis.candidates).encode()
                ).hexdigest()[:8]
                sql_program = build_sql_program_from_chain(
                    plans, spec.spec_hash, chain_id
                )
                validator = SqlBuildPlanValidator()
                with collector.stage("sql_validator", request_id):
                    passed, val_questions = validator.validate_multi_hop_chain(sql_program)
                plan = plans[-1]
                plan_questions = []
            else:
                with collector.stage("sql_builder", request_id) as ctx:
                    plan, plan_questions = builder.build(spec, hypothesis=hypothesis)
                    ctx.set_result(artifact_path=f"plan/{plan.plan_id}")
                validator = SqlBuildPlanValidator()
                with collector.stage("sql_validator", request_id):
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
        collector = get_collector()
        # ── Stage 1-2: Parser + Enrich ──
        parsed = self._parse_and_enrich(
            "execute", markdown_text, table_mapping,
            collector=collector,
        )
        if not parsed["ok"]:
            return parsed["error_response"]
        spec = parsed["spec"]
        manifest = parsed["manifest"]
        hypothesis = parsed["hypothesis"]
        extra_questions = parsed["extra_questions"]
        table_mapping = parsed["table_mapping"]
        request_id = self._gen_request_id(spec)

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
                with collector.stage("sql_builder", request_id) as ctx:
                    plans = builder.build_from_steps(spec, hypothesis)
                    plan_snap = plans[-1]
                    ctx.set_result(artifact_path=f"plan/{plan_snap.plan_id}")
                chain_id = hashlib.md5(
                    "|".join(s.step_name for s in spec.compute_steps).encode()
                ).hexdigest()[:8]
                sql_program = build_sql_program_from_compute_steps(
                    plans, spec, chain_id
                )
                validator = SqlBuildPlanValidator()
                with collector.stage("sql_validator", request_id):
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
                with collector.stage("sql_compiler", request_id) as ctx:
                    program_artifact = compiler.compile_program(sql_program)
                    ctx.set_result(
                        artifact_path=f"compiled/{hashlib.sha256(str(sql_program).encode()).hexdigest()[:12]}"
                    )

                stage = "execute"
                execute_executor = DuckDBExecutor(
                    table_paths=self._resolve_table_paths(table_paths),
                    duckdb_path=self._duckdb_path,
                )
                with collector.stage("sql_executor", request_id) as ctx:
                    program_result = execute_executor.execute_program(
                        program_artifact.compiled
                    )
                    if program_result and program_result.results:
                        last_trace = program_result.results[-1].trace
                        if last_trace:
                            ctx.set_result(row_count=last_trace.row_count)
                last_result = (
                    program_result.results[-1]
                    if program_result.results else None
                )
                trace = last_result.trace if last_result is not None else None
                summary = last_result.summary if last_result is not None else None
                compiled = program_artifact.compiled.statements[-1]
            elif hypothesis and len(hypothesis.candidates) > 1:
                # ── 多跳链路径 ──
                with collector.stage("sql_builder", request_id) as ctx:
                    plans = builder.build_multi(spec, hypothesis)
                    plan_snap = plans[-1]
                    ctx.set_result(artifact_path=f"plan/{plan_snap.plan_id}")
                chain_id = hashlib.md5(
                    "|".join(c.candidate_id for c in hypothesis.candidates).encode()
                ).hexdigest()[:8]
                sql_program = build_sql_program_from_chain(
                    plans, spec.spec_hash, chain_id
                )
                validator = SqlBuildPlanValidator()
                with collector.stage("sql_validator", request_id):
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
                with collector.stage("sql_compiler", request_id) as ctx:
                    program_artifact = compiler.compile_program(sql_program)
                    ctx.set_result(
                        artifact_path=f"compiled/{hashlib.sha256(str(sql_program).encode()).hexdigest()[:12]}"
                    )

                stage = "execute"
                execute_executor = DuckDBExecutor(
                    table_paths=self._resolve_table_paths(table_paths),
                    duckdb_path=self._duckdb_path,
                )
                with collector.stage("sql_executor", request_id) as ctx:
                    program_result = execute_executor.execute_program(
                        program_artifact.compiled
                    )
                    if program_result and program_result.results:
                        last_trace = program_result.results[-1].trace
                        if last_trace:
                            ctx.set_result(row_count=last_trace.row_count)
                last_result = (
                    program_result.results[-1]
                    if program_result.results else None
                )
                trace = last_result.trace if last_result is not None else None
                summary = last_result.summary if last_result is not None else None
                compiled = program_artifact.compiled.statements[-1]
            else:
                with collector.stage("sql_builder", request_id) as ctx:
                    plan, plan_questions = builder.build(spec, hypothesis=hypothesis)
                    ctx.set_result(artifact_path=f"plan/{plan.plan_id}")

                # Validator 验证——blocking 问题阻断编译，非 blocking 记录供排查
                validator = SqlBuildPlanValidator()
                with collector.stage("sql_validator", request_id):
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
                with collector.stage("sql_compiler", request_id) as ctx:
                    compiled = compiler.compile(plan)
                    ctx.set_result(artifact_path=f"compiled/{compiled.sql_sha256[:12]}")

                stage = "execute"
                execute_executor = DuckDBExecutor(
                    table_paths=self._resolve_table_paths(table_paths),
                    duckdb_path=self._duckdb_path,
                )
                with collector.stage("sql_executor", request_id) as ctx:
                    trace, summary = execute_executor.execute(compiled)
                    if trace:
                        ctx.set_result(row_count=trace.row_count)

            # 只有 RUNTIME_PASS 能进入成功路径，资源限制和超时必须阻断
            if isinstance(trace.status, ExecutionStatus) and trace.status != ExecutionStatus.RUNTIME_PASS:
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
            # 构建 evidence_map——将 RelationshipPlanner 产出的证据链传入 Contract
            evidence_map_sp = {}
            if hypothesis:
                for c in hypothesis.candidates:
                    if c.evidence:
                        evidence_map_sp[c.candidate_id] = c.evidence
            extractor = DataTransformContractExtractor()
            sql_program = build_sql_program(plan, spec.spec_hash)
            contract = extractor.extract_v1(
                sql_program,
                evidence_map=evidence_map_sp,
                output_grain=spec.output_spec.grain,
            )
        except Exception as contract_err:
            logger.warning("Contract 抽取失败（非阻断）：%s", contract_err)

        # ── 成功路径——现有逻辑不变 ──
        request_id = self._gen_request_id(spec)
        self._store_result(request_id, {
            "parsed_spec": spec,
            "manifest": manifest,
            "plan": plan,
            "compiled": compiled,
            "compiled_program": (
                program_artifact.compiled if program_artifact is not None else None
            ),
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
        9 阶段：parser → enrich → build → validate → compile → contract → snapshot → execute → package。

        Args:
            markdown_text: DeveloperSpec Markdown 全文
            table_mapping: table_ref → 物理表名
            table_paths: 物理表名 → CSV 文件路径

        Returns:
            符合 RunAllResponse 结构的 dict，失败时含 pipeline_error + pipeline_stages
        """
        self._purge_expired()
        collector = get_collector()
        _run_all_stages = [
            "parser", "enrich", "build", "validate",
            "compile", "contract", "snapshot", "execute", "package",
        ]

        # ── Stage 1-2: Parser + Enrich ──
        parsed = self._parse_and_enrich(
            "run_all", markdown_text, table_mapping,
            pipeline_stages=_run_all_stages,
            collector=collector,
        )
        if not parsed["ok"]:
            return parsed["error_response"]
        spec = parsed["spec"]
        manifest = parsed["manifest"]
        hypothesis = parsed["hypothesis"]
        extra_questions = parsed["extra_questions"]
        table_mapping = parsed["table_mapping"]
        request_id = self._gen_request_id(spec)

        # ── blocking extra_questions 门禁——先于 build 阻断 ──
        # enrich 阶段（如 LEFT JOIN 唯一性安全门禁）产生的 blocking 问题必须阻断流水线，
        # 否则 Join 候选被丢弃后计划静默退化为单表，最终在 execute 阶段以晦涩的
        # Binder Error 暴露（NYC Case03/04 存量失败根因）。
        # 必须在 build 之前拦截——builder 对空候选 + 跨表输出列会抛 ValueError，
        # 若放行到 build 会以异常形式掩盖真实原因（blocking OpenQuestion）
        has_blocking_extra = any(q.blocking for q in extra_questions)
        if has_blocking_extra:
            blocked = self._build_validation_blocked_response(
                spec, manifest, None, list(extra_questions),
                table_mapping=table_mapping, all_stages=_run_all_stages,
            )
            blocked.update({
                "execution_status": "not_executed",
                "row_count": 0,
                "elapsed_ms": 0,
            })
            return blocked

        # ── Stage 3-9: Build → Compile → Contract → Snapshot → Execute → Package ──
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
                with collector.stage("sql_builder", request_id) as ctx:
                    plans = builder.build_from_steps(spec, hypothesis)
                    plan_snap = plans[-1]
                    ctx.set_result(artifact_path=f"plan/{plan_snap.plan_id}")
                chain_id = hashlib.md5(
                    "|".join(s.step_name for s in spec.compute_steps).encode()
                ).hexdigest()[:8]
                sql_program = build_sql_program_from_compute_steps(
                    plans, spec, chain_id
                )
                plan = plans[-1]
                plan_questions = []

                validator = SqlBuildPlanValidator()
                with collector.stage("sql_validator", request_id):
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
                with collector.stage("sql_compiler", request_id) as ctx:
                    program_artifact = compiler.compile_program(sql_program)
                    ctx.set_result(
                        artifact_path=f"compiled/{hashlib.sha256(str(sql_program).encode()).hexdigest()[:12]}"
                    )
                compiled_sql = program_artifact.compiled.statements[-1]

                stage = "contract"
                extractor = DataTransformContractExtractor()
                with collector.stage("contract_extractor", request_id) as ctx:
                    contract = extractor.extract_v1(
                        sql_program,
                        output_grain=spec.output_spec.grain,
                    )
                    ctx.set_result(artifact_path=f"contract/{contract.contract_id[:12]}")

                stage = "snapshot"
                with collector.stage("snapshot_builder", request_id) as ctx:
                    snapshot_manifest, execution_paths = self._prepare_run_all_snapshot(
                        contract=contract,
                        table_mapping=table_mapping or {},
                        table_paths=table_paths,
                        spec=spec,
                    )
                    if snapshot_manifest is not None:
                        ctx.set_result(
                            artifact_path=f"snapshot/{snapshot_manifest.snapshot_id}",
                            row_count=len(snapshot_manifest.files),
                        )

                stage = "execute"
                executor = DuckDBExecutor(table_paths=execution_paths)
                with collector.stage("sql_executor", request_id) as ctx:
                    program_result = executor.execute_program(
                        program_artifact.compiled
                    )
                    if program_result and program_result.results:
                        last_trace = program_result.results[-1].trace
                        if last_trace:
                            ctx.set_result(row_count=last_trace.row_count)
                execution_trace = program_result.results[-1].trace if program_result.results else None
                execution_summary = (
                    program_result.results[-1].summary
                    if program_result and program_result.results else None
                )
                # ── Final Hardening: 捕获 cleanup 状态 ──
                program_cleanup_status = program_result.cleanup_status if program_result else None
                program_cleanup_error = program_result.cleanup_error if program_result else None

                # 只有 RUNTIME_PASS 能进入成功路径
                if execution_trace is not None \
                        and isinstance(execution_trace.status, ExecutionStatus) \
                        and execution_trace.status != ExecutionStatus.RUNTIME_PASS:
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
                        "execution_status": execution_trace.status.value.lower(),
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
                with collector.stage("packager", request_id) as ctx:
                    package_manifest = packager.build(package_inputs)
                    ctx.set_result(artifact_path=f"package/{package_manifest.package_id}")
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
                    "compiled": compiled_sql,
                    "compiled_program": program_artifact.compiled,
                    "trace": execution_trace,
                    "summary": execution_summary,
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
                with collector.stage("sql_builder", request_id) as ctx:
                    plans = builder.build_multi(spec, hypothesis)
                    plan_snap = plans[-1]
                    ctx.set_result(artifact_path=f"plan/{plan_snap.plan_id}")
                chain_id = hashlib.md5(
                    "|".join(c.candidate_id for c in hypothesis.candidates).encode()
                ).hexdigest()[:8]
                sql_program = build_sql_program_from_chain(
                    plans, spec.spec_hash, chain_id
                )
                plan = plans[-1]
                plan_questions = []

                validator = SqlBuildPlanValidator()
                with collector.stage("sql_validator", request_id):
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
                with collector.stage("sql_compiler", request_id) as ctx:
                    program_artifact = compiler.compile_program(sql_program)
                    ctx.set_result(
                        artifact_path=f"compiled/{hashlib.sha256(str(sql_program).encode()).hexdigest()[:12]}"
                    )
                compiled_sql = program_artifact.compiled.statements[-1]

            else:
                with collector.stage("sql_builder", request_id) as ctx:
                    plan, plan_questions = builder.build(spec, hypothesis=hypothesis)
                    ctx.set_result(artifact_path=f"plan/{plan.plan_id}")

                validator = SqlBuildPlanValidator()
                with collector.stage("sql_validator", request_id):
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
                with collector.stage("sql_compiler", request_id) as ctx:
                    artifact = compiler.compile_to_artifact(plan, spec.spec_hash)
                    compiled_sql = artifact.compiled_sql
                    ctx.set_result(artifact_path=f"compiled/{compiled_sql.sql_sha256[:12]}")

                sql_program = build_sql_program(plan, spec.spec_hash)

            # Contract 是快照的权威输入，必须先于任何业务 SQL 执行。
            stage = "contract"
            evidence_map: dict = {}
            if hypothesis:
                for candidate in hypothesis.candidates:
                    if candidate.evidence:
                        evidence_map[candidate.candidate_id] = candidate.evidence
            contract_extractor = DataTransformContractExtractor()
            with collector.stage("contract_extractor", request_id) as ctx:
                contract = contract_extractor.extract_v1(
                    sql_program,
                    evidence_map=evidence_map,
                    output_grain=spec.output_spec.grain,
                )
                ctx.set_result(artifact_path=f"contract/{contract.contract_id[:12]}")

            stage = "snapshot"
            with collector.stage("snapshot_builder", request_id) as ctx:
                snapshot_manifest, execution_paths = self._prepare_run_all_snapshot(
                    contract=contract,
                    table_mapping=table_mapping or {},
                    table_paths=table_paths,
                    spec=spec,
                )
                if snapshot_manifest is not None:
                    ctx.set_result(
                        artifact_path=f"snapshot/{snapshot_manifest.snapshot_id}",
                        row_count=len(snapshot_manifest.files),
                    )

            stage = "execute"
            execute_executor = DuckDBExecutor(table_paths=execution_paths)
            with collector.stage("sql_executor", request_id) as ctx:
                if program_artifact is not None:
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
                    program_cleanup_status = program_result.cleanup_status
                    program_cleanup_error = program_result.cleanup_error
                else:
                    trace, summary = execute_executor.execute(compiled_sql)
                if trace:
                    ctx.set_result(row_count=trace.row_count)

            # 只有 RUNTIME_PASS 能进入 Package 阶段
            if isinstance(trace.status, ExecutionStatus) and trace.status != ExecutionStatus.RUNTIME_PASS:
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
                    "execution_status": trace.status.value.lower(),
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
            with collector.stage("packager", request_id) as ctx:
                package_manifest = packager.build(package_inputs)
                ctx.set_result(artifact_path=f"package/{package_manifest.package_id}")

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
            "compiled_program": (
                program_artifact.compiled if program_artifact is not None else None
            ),
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
            # 流式进度支持——SQL 管线阶段状态列表（全部 ok），供 run_all_full_stream 提取
            "pipeline_stages": [{"stage": s, "status": "ok"} for s in _run_all_stages],
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

    def run_all_full(
        self, markdown_text: str,
        table_mapping: dict[str, str] | None = None,
        table_paths: dict[str, str] | None = None,
    ) -> dict:
        """全流程 SQL + Spark 管线——后端轻量编排，复用现有 dispatcher。

        1. SQL 管线：调用 run_all(rich=True) 获取完整产物 + SQL 代码
        2. Spark 管线：顺序调用 run_spark_stage() 执行 6 阶段
        3. 汇总返回 FullRunResponse 风格的聚合 dict

        阶段失败策略：
        - DEVELOPER 可选——失败标记 WARN 后继续
        - MAPPER / COMPILER / VALIDATOR 失败→停止下游
        - PHYSICAL_VERIFIER 仅当 VALIDATOR 通过后执行
        - COMPARATOR 读取其细粒度 comparator_report.status

        PySpark 缺失时返回 NOT_EXECUTED / SKIPPED，
        不声称"全流程成功"。
        """
        from tianshu_datadev.spark.orchestrator import SparkPipelineStage

        logger = logging.getLogger(__name__)

        # ── Step 1: SQL 管线 ──
        sql_result = self.run_all(markdown_text, table_mapping, table_paths, rich=True)
        request_id = sql_result.get("request_id")
        sql_ok = sql_result.get("pipeline_error") is None

        generated_sql = sql_result.get("generated_sql", "") if sql_ok else ""

        # ── Step 2: Spark 管线（仅当 SQL 成功且有 request_id）──
        spark_stages: list[dict] = []
        spark_ok = False
        pyspark_code: str | None = None
        standalone_pyspark: str | None = None
        comparator_status: str | None = None
        all_llm_traces: dict = dict(sql_result.get("llm_traces", {}) or {})

        if sql_ok and request_id:
            stages_sequence = [
                SparkPipelineStage.MAPPER,
                SparkPipelineStage.DEVELOPER,
                SparkPipelineStage.COMPILER,
                SparkPipelineStage.VALIDATOR,
                SparkPipelineStage.COMPARATOR,
                SparkPipelineStage.PHYSICAL_VERIFIER,
            ]

            for stage in stages_sequence:
                stage_val = stage.value
                try:
                    stage_result = self.run_spark_stage(request_id, stage)
                except Exception as exc:
                    logger.warning("Spark 阶段 %s 异常：%s", stage_val, exc)
                    spark_stages.append({
                        "stage": stage_val, "status": "failed",
                        "errors": [str(exc)],
                    })
                    if stage in (SparkPipelineStage.MAPPER, SparkPipelineStage.COMPILER,
                                 SparkPipelineStage.VALIDATOR):
                        break
                    continue

                current_status = stage_result.get("status", "skipped")
                current_errors = stage_result.get("errors", [])

                # 合并 LLM traces
                stage_traces = stage_result.get("llm_traces", {}) or {}
                all_llm_traces.update(stage_traces)

                # 提取 COMPILER 阶段产物
                if stage == SparkPipelineStage.COMPILER:
                    compiler_result = stage_result.get("result", {}) or {}
                    pyspark_code = compiler_result.get("pyspark_code")
                    standalone_pyspark = compiler_result.get("standalone_pyspark")

                # 提取 COMPARATOR 细粒度状态
                if stage == SparkPipelineStage.COMPARATOR:
                    comp_result = stage_result.get("result", {}) or {}
                    comparator_status = comp_result.get("status")

                # ── 失败策略 ──
                # DEVELOPER 可选——失败标记 skipped 后继续
                if stage == SparkPipelineStage.DEVELOPER:
                    if current_status == "failed":
                        spark_stages.append({
                            "stage": stage_val, "status": "skipped",
                            "errors": ["LLM 标注服务不可用，已跳过"],
                        })
                        continue
                    spark_stages.append({
                        "stage": stage_val, "status": current_status,
                        "errors": current_errors,
                    })
                    continue

                # MAPPER / COMPILER / VALIDATOR 失败→停止下游
                if stage in (SparkPipelineStage.MAPPER, SparkPipelineStage.COMPILER,
                             SparkPipelineStage.VALIDATOR):
                    spark_stages.append({
                        "stage": stage_val, "status": current_status,
                        "errors": current_errors,
                    })
                    if current_status == "failed":
                        break
                    continue

                # COMPARATOR——记录细粒度状态
                if stage == SparkPipelineStage.COMPARATOR:
                    spark_stages.append({
                        "stage": stage_val, "status": current_status,
                        "errors": current_errors,
                        "comparator_status": comparator_status,
                    })
                    continue

                # PHYSICAL_VERIFIER——记录执行结果，门禁已在 _do_spark_physical_verify 中
                if stage == SparkPipelineStage.PHYSICAL_VERIFIER:
                    spark_stages.append({
                        "stage": stage_val, "status": current_status,
                        "errors": current_errors,
                    })
                    continue

        # ── Step 3: 汇总 FullRunResponse ──
        # 提取 SQL 管线的 pipeline_error（供 runAction 自动提取）
        sql_pipeline_error = sql_result.get("pipeline_error")
        sql_pipeline_stages = sql_result.get("pipeline_stages", [])
        # 判断整体 Spark 管线是否通过（物理一致 + COMPARATOR LOGIC_EQUIVALENT）
        physver_stage = next(
            (s for s in spark_stages if s["stage"] == "PHYSICAL_VERIFIER"), None,
        )
        spark_ok = (
            physver_stage is not None
            and physver_stage["status"] == "ok"
            and comparator_status == "LOGIC_EQUIVALENT"
        )

        # ── 统一 review_ready 判定：委托 SparkReviewBuilder._compute_review_ready ──
        from tianshu_datadev.spark.review_builder import SparkReviewBuilder
        ctx = self._get_or_create_spark_context(request_id)
        review_ready = SparkReviewBuilder._compute_review_ready(
            dict(ctx.stage_results), comparator_status or "",
        )

        return {
            "request_id": request_id,
            # 兼容 runAction 自动提取
            "pipeline_error": sql_pipeline_error,
            "pipeline_stages": sql_pipeline_stages,
            # SQL 管线摘要
            "sql_ok": sql_ok,
            "sql_pipeline_error": sql_pipeline_error,
            "sql_pipeline_stages": sql_pipeline_stages,
            "generated_sql": generated_sql,
            "spec_id": sql_result.get("spec_id"),
            "plan_id": sql_result.get("plan_id"),
            "package_id": sql_result.get("package_id"),
            # Spark 管线摘要
            "spark_ok": spark_ok,
            "spark_stages": spark_stages,
            "pyspark_code": pyspark_code,
            "standalone_pyspark": standalone_pyspark,
            # 全量 LLM 调用追踪
            "llm_traces": all_llm_traces,
            # COMPARATOR 细粒度状态与审核标记
            "comparator_status": comparator_status,
            "requires_human_review": not review_ready,
            "review_ready": review_ready,
        }

    def run_all_full_stream(
        self, markdown_text: str,
        table_mapping: dict[str, str] | None = None,
        table_paths: dict[str, str] | None = None,
    ):
        """全流程 SQL + Spark 管线——NDJSON 流式生成器。

        通过 queue.Queue 在后台线程和生成器之间传递进度事件。
        每行一个 JSON 对象（NDJSON），前端 ReadableStream 逐行消费。

        事件类型：
        - {"event":"stage","pipeline":"sql"|"spark","stage":"...","status":"..."}
        - {"event":"done","result":{...FullRunResponse...}}
        - {"event":"fatal","error_code":"...","message":"..."}
        - {"event":"heartbeat"}

        连接断开时后台线程继续执行——queue 满时丢弃事件并计数。

        Yields:
            NDJSON 行（str），每行以 \\n 结尾
        """
        import json
        import queue as _queue_mod
        import threading

        event_queue: "_queue_mod.Queue" = _queue_mod.Queue(maxsize=500)
        stop_event = threading.Event()
        logger = logging.getLogger(__name__)

        def _execute():
            """后台线程——执行 SQL + Spark 全流程，将事件推入队列。"""
            try:
                # ── Step 1: SQL 管线 ──
                # 通过 ContextVar 注入流式事件队列——run_all() 内部调用 get_collector()
                # 时，将返回 TeeCollector 包装的 collector，实时推送 SQL 阶段
                # started/completed 事件（含 duration_ms）。
                from tianshu_datadev.monitor.collector import _stream_event_queue
                _token = _stream_event_queue.set(event_queue)
                try:
                    sql_result = self.run_all(markdown_text, table_mapping, table_paths, rich=True)
                finally:
                    _stream_event_queue.reset(_token)

                request_id = sql_result.get("request_id")
                sql_ok = sql_result.get("pipeline_error") is None
                generated_sql = sql_result.get("generated_sql", "") if sql_ok else ""

                # 提取 pipeline_stages 供 done 事件使用（TeeCollector 已在 run_all()
                # 执行期间实时推送了所有 SQL 阶段事件到 event_queue）
                sql_pipeline_stages = sql_result.get("pipeline_stages", []) or []

                # ── Step 2: Spark 管线 ──
                spark_stages: list[dict] = []
                spark_ok = False
                pyspark_code: str | None = None
                standalone_pyspark: str | None = None
                comparator_status: str | None = None
                all_llm_traces: dict = dict(sql_result.get("llm_traces", {}) or {})

                if sql_ok and request_id:
                    stages_sequence = [
                        SparkPipelineStage.MAPPER,
                        SparkPipelineStage.DEVELOPER,
                        SparkPipelineStage.COMPILER,
                        SparkPipelineStage.VALIDATOR,
                        SparkPipelineStage.COMPARATOR,
                        SparkPipelineStage.PHYSICAL_VERIFIER,
                    ]

                    for stage in stages_sequence:
                        stage_val = stage.value

                        # 发送 Spark 阶段 started 事件
                        event_queue.put({
                            "event": "stage",
                            "pipeline": "spark",
                            "stage": stage_val,
                            "status": "started",
                        })

                        stage_start = time.time()
                        try:
                            stage_result = self.run_spark_stage(request_id, stage)
                        except Exception as exc:
                            duration_ms = int((time.time() - stage_start) * 1000)
                            err_msg = _sanitize_stream_error(exc)
                            event_queue.put({
                                "event": "stage",
                                "pipeline": "spark",
                                "stage": stage_val,
                                "status": "failed",
                                "duration_ms": duration_ms,
                                "message": err_msg,
                                "error_type": type(exc).__name__,
                            })
                            spark_stages.append({
                                "stage": stage_val, "status": "failed",
                                "errors": [str(exc)],
                            })
                            if stage in (SparkPipelineStage.MAPPER, SparkPipelineStage.COMPILER,
                                         SparkPipelineStage.VALIDATOR):
                                break
                            continue

                        duration_ms = int((time.time() - stage_start) * 1000)
                        current_status = stage_result.get("status", "skipped")
                        current_errors = stage_result.get("errors", [])

                        # 合并 LLM traces
                        stage_traces = stage_result.get("llm_traces", {}) or {}
                        all_llm_traces.update(stage_traces)

                        # 提取 COMPILER 阶段产物
                        if stage == SparkPipelineStage.COMPILER:
                            compiler_result = stage_result.get("result", {}) or {}
                            pyspark_code = compiler_result.get("pyspark_code")
                            standalone_pyspark = compiler_result.get("standalone_pyspark")

                        # 提取 COMPARATOR 细粒度状态
                        if stage == SparkPipelineStage.COMPARATOR:
                            comp_result = stage_result.get("result", {}) or {}
                            comparator_status = comp_result.get("status")

                        # ── 失败策略 ──
                        # DEVELOPER 可选——失败标记 skipped 后继续
                        if stage == SparkPipelineStage.DEVELOPER:
                            if current_status == "failed":
                                event_queue.put({
                                    "event": "stage",
                                    "pipeline": "spark",
                                    "stage": stage_val,
                                    "status": "skipped",
                                    "duration_ms": duration_ms,
                                    "message": "LLM 标注服务不可用，已跳过",
                                })
                                spark_stages.append({
                                    "stage": stage_val, "status": "skipped",
                                    "errors": ["LLM 标注服务不可用，已跳过"],
                                })
                                continue
                            event_queue.put({
                                "event": "stage",
                                "pipeline": "spark",
                                "stage": stage_val,
                                "status": "completed",
                                "duration_ms": duration_ms,
                            })
                            spark_stages.append({
                                "stage": stage_val, "status": current_status,
                                "errors": current_errors,
                            })
                            continue

                        # MAPPER / COMPILER / VALIDATOR 失败→停止下游
                        if stage in (SparkPipelineStage.MAPPER, SparkPipelineStage.COMPILER,
                                     SparkPipelineStage.VALIDATOR):
                            status_event = "completed" if current_status == "ok" else "failed"
                            event_queue.put({
                                "event": "stage",
                                "pipeline": "spark",
                                "stage": stage_val,
                                "status": status_event,
                                "duration_ms": duration_ms,
                                "message": (
                                    "; ".join(current_errors)
                                    if current_errors and current_status == "failed"
                                    else None
                                ),
                            })
                            spark_stages.append({
                                "stage": stage_val, "status": current_status,
                                "errors": current_errors,
                            })
                            if current_status == "failed":
                                break
                            continue

                        # COMPARATOR——记录细粒度状态
                        if stage == SparkPipelineStage.COMPARATOR:
                            event_queue.put({
                                "event": "stage",
                                "pipeline": "spark",
                                "stage": stage_val,
                                "status": "completed",
                                "duration_ms": duration_ms,
                                "message": f"对比状态: {comparator_status}" if comparator_status else None,
                            })
                            spark_stages.append({
                                "stage": stage_val, "status": current_status,
                                "errors": current_errors,
                                "comparator_status": comparator_status,
                            })
                            continue

                        # PHYSICAL_VERIFIER——仅当 VALIDATOR 通过后执行
                        if stage == SparkPipelineStage.PHYSICAL_VERIFIER:
                            validator_passed = any(
                                s["stage"] == "VALIDATOR" and s["status"] == "ok"
                                for s in spark_stages
                            )
                            if not validator_passed:
                                event_queue.put({
                                    "event": "stage",
                                    "pipeline": "spark",
                                    "stage": stage_val,
                                    "status": "skipped",
                                    "message": "VALIDATOR 未通过，跳过物理验证",
                                })
                                spark_stages.append({
                                    "stage": stage_val, "status": "skipped",
                                    "errors": ["VALIDATOR 未通过，跳过物理验证"],
                                })
                                break

                            # PHYSICAL_VERIFIER 终态：ok/failed/skipped 分别处理
                            if current_status == "skipped":
                                status_event = "skipped"
                            elif current_status == "ok":
                                status_event = "completed"
                            else:
                                status_event = "failed"

                            # 构建物理验证摘要——用于进度面板展示（非泛化"通过"）
                            _physver_msg: str | None = None
                            if current_status == "ok":
                                _pr = stage_result.get("result", {}) or {}
                                _vstatus = _pr.get("verification_status", "")
                                _ddb_rows = _pr.get("duckdb_row_count")
                                _spk_rows = _pr.get("spark_row_count")
                                _rows_match = _pr.get("row_count_match")
                                _schema_match = _pr.get("schema_match")
                                _diffs = _pr.get("total_diff_count", 0)

                                _parts: list[str] = []
                                # 验证结论——区分全量逐行对比 vs 降级抽样对比
                                if _vstatus == "RESULT_CONSISTENT":
                                    _parts.append("全量逐行一致")
                                elif _vstatus == "SAMPLED_CONSISTENT":
                                    _parts.append("抽样一致（溢出降级对比）")
                                else:
                                    _parts.append(_vstatus or "通过")

                                # 行数对比
                                _ddb_str = str(_ddb_rows) if _ddb_rows is not None else "?"
                                _spk_str = str(_spk_rows) if _spk_rows is not None else "?"
                                _parts.append(f"DuckDB {_ddb_str} 行 ↔ Spark {_spk_str} 行")

                                # 对比方式标注
                                if _vstatus == "RESULT_CONSISTENT":
                                    _parts.append("全量对比")
                                elif _vstatus == "SAMPLED_CONSISTENT":
                                    _parts.append("降级对比")

                                # 对比项
                                _checks: list[str] = []
                                if _rows_match is True:
                                    _checks.append("行数✅")
                                elif _rows_match is False:
                                    _checks.append("行数❌")
                                if _schema_match is True:
                                    _checks.append("Schema✅")
                                elif _schema_match is False:
                                    _checks.append("Schema❌")
                                if isinstance(_diffs, int) and _diffs > 0:
                                    _checks.append(f"差异{_diffs}")
                                if _checks:
                                    _parts.append(" · ".join(_checks))

                                _physver_msg = " | ".join(_parts)
                            elif current_errors and current_status != "ok":
                                # 失败时取第一条 PHYSICAL_VERIFIER 错误作为摘要
                                _physver_errors = [
                                    e.split("] ", 1)[1] if "] " in e else e
                                    for e in current_errors
                                    if e.startswith("[PHYSICAL_VERIFIER]")
                                ]
                                _physver_msg = _physver_errors[0] if _physver_errors else (
                                    "; ".join(current_errors) if current_errors else None
                                )

                            event_queue.put({
                                "event": "stage",
                                "pipeline": "spark",
                                "stage": stage_val,
                                "status": status_event,
                                "duration_ms": duration_ms,
                                "message": _physver_msg,
                            })
                            spark_stages.append({
                                "stage": stage_val, "status": current_status,
                                "errors": current_errors,
                            })

                # ── 判断整体 Spark 管线是否通过（物理一致 + COMPARATOR LOGIC_EQUIVALENT）──
                physver_stage = next(
                    (s for s in spark_stages if s["stage"] == "PHYSICAL_VERIFIER"), None,
                )
                spark_ok = (
                    physver_stage is not None
                    and physver_stage["status"] == "ok"
                    and comparator_status == "LOGIC_EQUIVALENT"
                )

                # ── 统一 review_ready 判定：委托 SparkReviewBuilder._compute_review_ready ──
                from tianshu_datadev.spark.review_builder import SparkReviewBuilder
                ctx = self._get_or_create_spark_context(request_id)
                review_ready = SparkReviewBuilder._compute_review_ready(
                    dict(ctx.stage_results), comparator_status or "",
                )

                # ── 汇总 FullRunResponse ──
                sql_pipeline_error = sql_result.get("pipeline_error")

                full_result = {
                    "request_id": request_id,
                    "pipeline_error": sql_pipeline_error,
                    "pipeline_stages": sql_pipeline_stages,
                    "sql_ok": sql_ok,
                    "sql_pipeline_error": sql_pipeline_error,
                    "sql_pipeline_stages": sql_pipeline_stages,
                    "generated_sql": generated_sql,
                    "spec_id": sql_result.get("spec_id"),
                    "plan_id": sql_result.get("plan_id"),
                    "package_id": sql_result.get("package_id"),
                    "spark_ok": spark_ok,
                    "spark_stages": spark_stages,
                    "pyspark_code": pyspark_code,
                    "standalone_pyspark": standalone_pyspark,
                    "llm_traces": all_llm_traces,
                    # COMPARATOR 细粒度状态与审核标记
                "comparator_status": comparator_status,
                "requires_human_review": not review_ready,
                "review_ready": review_ready,
                }

                event_queue.put({"event": "done", "result": full_result})

            except Exception as exc:
                logger.exception("run_all_full_stream 后台线程致命错误")
                event_queue.put({
                    "event": "fatal",
                    "error_code": type(exc).__name__.upper(),
                    "message": _sanitize_stream_error(exc),
                })
            finally:
                stop_event.set()

        # 启动后台线程
        thread = threading.Thread(
            target=_execute, daemon=True, name="run-all-full-stream",
        )
        thread.start()

        # 生成器——从队列读取并 yield NDJSON 行
        while not stop_event.is_set() or not event_queue.empty():
            try:
                event = event_queue.get(timeout=0.5)
                yield json.dumps(event, ensure_ascii=False) + "\n"
                if event.get("event") in ("done", "fatal"):
                    return
            except _queue_mod.Empty:
                # 心跳保持连接
                yield '{"event":"heartbeat"}\n'

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
        compiled_value = data.get("compiled")
        compiled_program = data.get("compiled_program")
        if isinstance(compiled_value, SqlProgramArtifact):
            compiled_program = compiled_value.compiled
            compiled = (
                compiled_program.statements[-1]
                if compiled_program.statements
                else None
            )
        elif isinstance(compiled_value, CompiledSql):
            compiled = compiled_value
        else:
            compiled = None

        program_artifact = data.get("program_artifact")
        if compiled_program is None and isinstance(program_artifact, SqlProgramArtifact):
            compiled_program = program_artifact.compiled
        if not isinstance(compiled_program, ProgramCompiledSql):
            compiled_program = None

        # ── Phase 9B-P0: 提取 snapshot_manifest ──
        snapshot_manifest = data.get("snapshot_manifest")

        return PipelineArtifactBundle(
            request_id=request_id,
            spec_hash=spec_hash,
            sql_build_plan=data.get("plan"),
            data_transform_contract=contract,
            compiled_sql=compiled,
            compiled_program=compiled_program,
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

    def check_artifacts_status(self, request_id: str) -> dict:
        """检查指定 request_id 的 artifacts 是否就绪——供前端 Spark 按钮 gating 使用。

        不返回 artifact 内容，仅返回状态标记和产物类型列表。
        前端可通过此端点判断是否允许触发 Spark 管线阶段。

        Args:
            request_id: Pipeline 请求 ID

        Returns:
            {
                "request_id": str,
                "artifacts_ready": bool,
                "available_artifacts": [str, ...],  # 已就绪的产物类型列表
            }
        """
        bundle = self.export_artifacts(request_id)
        if bundle is None:
            return {
                "request_id": request_id,
                "artifacts_ready": False,
                "available_artifacts": [],
            }
        available: list[str] = []
        if bundle.sql_build_plan is not None:
            available.append("sql_build_plan")
        if bundle.data_transform_contract is not None:
            available.append("data_transform_contract")
        if bundle.compiled_sql is not None:
            available.append("compiled_sql")
        if bundle.snapshot_manifest is not None:
            available.append("snapshot_manifest")
        # artifacts_ready = contract 存在（Spark MAPPER 的最低要求）
        artifacts_ready = bundle.data_transform_contract is not None
        return {
            "request_id": request_id,
            "artifacts_ready": artifacts_ready,
            "available_artifacts": available,
        }

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
            gk = [k.column_name if isinstance(k, ColumnRef) else k.alias for k in step.group_keys]
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
            desc_parts.append(f"CASE WHEN 分支数: {len(step.cases)}")
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
        collector = get_collector()
        # ── Stage 1-2: Parser + Enrich ──
        parsed = self._parse_and_enrich(
            "build_plan_rich", markdown_text, table_mapping,
            collector=collector,
        )
        if not parsed["ok"]:
            # 补齐前端 PlanRichResponse 所需的全部字段——缺失字段会导致 React 渲染崩溃
            err = parsed["error_response"]
            err.setdefault("spec_id", "")
            err.setdefault("plan_id", "")
            err.setdefault("step_count", 0)
            err.setdefault("step_types", [])
            err.setdefault("steps", [])
            err.setdefault("multi_table", False)
            err.setdefault("validation_passed", False)
            err.setdefault("open_questions", [])
            err.setdefault("join_evidence", [])
            return err
        spec = parsed["spec"]
        manifest = parsed["manifest"]
        hypothesis = parsed["hypothesis"]
        extra_questions = parsed["extra_questions"]
        table_mapping = parsed["table_mapping"]
        request_id = self._gen_request_id(spec)

        # ── Stage 3: Build + Validate ──
        plan = None
        try:
            builder = SqlBuildPlanBuilder()
            with collector.stage("sql_builder", request_id) as ctx:
                plan, plan_questions = builder.build(spec, hypothesis=hypothesis)
                ctx.set_result(artifact_path=f"plan/{plan.plan_id}")

            validator = SqlBuildPlanValidator()
            with collector.stage("sql_validator", request_id):
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
                "step_count": 0,
                "step_types": [],
                "steps": [],
                "multi_table": False,
                "validation_passed": False,
                "open_questions": [],
                "join_evidence": [],
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
        collector = get_collector()
        # ── Stage 1-2: Parser + Enrich ──
        parsed = self._parse_and_enrich(
            "execute_rich", markdown_text, table_mapping,
            collector=collector,
        )
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
            with collector.stage("sql_builder", request_id) as ctx:
                plan, plan_questions = builder.build(spec, hypothesis=hypothesis)
                ctx.set_result(artifact_path=f"plan/{plan.plan_id}")
            self._record_trace(
                request_id, "sql_build_planner",
                status="skipped", latency_ms=int((time.time() - _build_start) * 1000),
            )

            validator = SqlBuildPlanValidator()
            with collector.stage("sql_validator", request_id):
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
            with collector.stage("sql_compiler", request_id) as ctx:
                compiled = compiler.compile(plan)
                ctx.set_result(artifact_path=f"compiled/{compiled.sql_sha256[:12]}")
            self._record_trace(
                request_id, "sql_program_planner",
                status="skipped", latency_ms=int((time.time() - _compile_start) * 1000),
            )

            stage = "execute"
            executor = DuckDBExecutor(
                table_paths=self._resolve_table_paths(table_paths),
                duckdb_path=self._duckdb_path,
            )
            with collector.stage("sql_executor", request_id) as ctx:
                trace, summary = executor.execute(compiled)
                if trace:
                    ctx.set_result(row_count=trace.row_count)

            # 只有 RUNTIME_PASS 能进入成功路径
            if isinstance(trace.status, ExecutionStatus) and trace.status != ExecutionStatus.RUNTIME_PASS:
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
            # 构建 evidence_map——将 RelationshipPlanner 产出的证据链传入 Contract
            evidence_map_d: dict = {}
            if hypothesis:
                for c in hypothesis.candidates:
                    if c.evidence:
                        evidence_map_d[c.candidate_id] = c.evidence
            extractor = DataTransformContractExtractor()
            sql_program = build_sql_program(plan, spec.spec_hash)
            contract = extractor.extract_v1(
                sql_program,
                evidence_map=evidence_map_d,
                output_grain=spec.output_spec.grain,
            )
        except Exception as contract_err:
            logger.warning("Contract 抽取失败（非阻断）：%s", contract_err)

        # ── 创建快照——供 PHYSICAL_VERIFIER 使用 ──
        # 从 table_paths 的 CSV 文件生成 Parquet 快照，
        # 使物理验证阶段能通过 _register_parquet_views 注册为 DuckDB 视图
        snapshot_manifest = None
        resolved_paths = self._resolve_table_paths(table_paths)
        if resolved_paths:
            with collector.stage("snapshot_builder", request_id) as ctx:
                try:
                    import os as _os
                    import tempfile as _tempfile

                    from tianshu_datadev.spark.snapshot import SnapshotFile
                    from tianshu_datadev.spark.snapshot import SnapshotManifest as _SnapManifest

                    # 计算 contract_hash——用于快照溯源
                    contract_hash = ""
                    if contract is not None:
                        from tianshu_datadev.artifacts.models import (
                            DataTransformContractLite as _Lite,
                        )
                        from tianshu_datadev.artifacts.models import (
                            DataTransformContractV1 as _V1,  # noqa: N814
                        )
                        if isinstance(contract, _V1):
                            contract_hash = _V1.compute_contract_hash(contract)
                        elif isinstance(contract, _Lite):
                            contract_hash = _Lite.compute_contract_hash(contract)

                    # 创建快照输出目录
                    snap_dir = _tempfile.mkdtemp(prefix="tianshu_snap_")

                    import pyarrow.csv as _pacsv
                    import pyarrow.parquet as _pq

                    # 构建逆向映射：物理表名 → alias（供 source_name 使用）
                    _reverse_mapping = _aliases_from_table_mapping(table_mapping)
                    logger.info(
                        "快照诊断——table_mapping=%s, _reverse_mapping=%s, "
                        "resolved_paths_keys=%s",
                        table_mapping, _reverse_mapping,
                        list(resolved_paths.keys()) if resolved_paths else [],
                    )

                    files: list[SnapshotFile] = []
                    for table_name, csv_path in sorted(resolved_paths.items()):
                        if not _os.path.isfile(csv_path):
                            logger.warning("快照跳过——CSV 文件不存在：%s", csv_path)
                            continue
                        try:
                            # 读取 CSV → PyArrow Table → 写入 Parquet
                            table = _pacsv.read_csv(
                                csv_path,
                                read_options=_pacsv.ReadOptions(),
                                parse_options=_pacsv.ParseOptions(),
                            )
                            # 文件名保留 schema 前缀（如 gold.fact_trips.parquet）
                            parquet_path = _os.path.join(snap_dir, f"{table_name}.parquet")
                            _pq.write_table(table, parquet_path)

                            # 计算行数和文件 hash
                            row_count = int(table.num_rows)
                            file_sha256 = hashlib.sha256()
                            with open(parquet_path, "rb") as _fh:
                                for _chunk in iter(lambda: _fh.read(8192), b""):
                                    file_sha256.update(_chunk)

                            # source_name 用 alias（与 PySpark transform 中 inputs[alias] 对齐）
                            # 无 alias 时回退物理表名（向后兼容单表无 mapping 场景）
                            _source = _reverse_mapping.get(table_name, table_name)
                            files.append(SnapshotFile(
                                source_name=_source,
                                file_path=parquet_path,
                                format="parquet",
                                row_count=row_count,
                                file_sha256=file_sha256.hexdigest(),
                            ))
                        except Exception as _csv_err:
                            logger.warning(
                                "快照创建失败（表 %s）：%s", table_name, _csv_err,
                            )
                            continue

                    if files:
                        # 写 _inputs_index.json 侧车——executor prologue 按别名装载
                        from tianshu_datadev.spark.snapshot import SnapshotBuilder
                        SnapshotBuilder._write_inputs_index(snap_dir, files)
                        # 生成确定性 snapshot_id
                        snap_id = f"snap_{contract_hash[:16] if contract_hash else 'adhoc'}"
                        snapshot_manifest = _SnapManifest(
                            snapshot_id=snap_id,
                            contract_hash=contract_hash,
                            snapshot_dir=snap_dir,
                            files=files,
                            source_type="local_fixture",
                        )
                        ctx.set_result(artifact_path=f"snapshot/{snap_id}", row_count=len(files))
                        logger.info(
                            "快照创建成功——snapshot_id=%s，文件数=%d",
                            snap_id, len(files),
                        )
                except Exception as snap_err:
                    logger.warning("快照创建失败（非阻断）：%s", snap_err)

        self._store_result(request_id, {
            "parsed_spec": spec, "manifest": manifest, "plan": plan,
            "compiled": compiled, "trace": trace, "summary": summary,
            "table_mapping": table_mapping or {},
            "contract": contract,  # 新增——供 Spark 管线使用
            "llm_traces": self._get_llm_traces(request_id),  # 新增——LLM 调用追踪
            "snapshot_manifest": snapshot_manifest,  # 新增——供 PHYSICAL_VERIFIER 使用
            "resolved_table_paths": resolved_paths,  # 新增——供 PHYSICAL_VERIFIER 回溯数据源路径
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
                missing.append("sql_build_plan（请先执行 编译执行 生成 SQL Plan）")
            if context.spark_plan is None:
                missing.append("spark_plan（请先执行 MAPPER 阶段）")
            if artifacts.data_transform_contract is None:
                missing.append("data_transform_contract（请先执行 编译执行 生成 Contract）")

        elif stage == SparkPipelineStage.PHYSICAL_VERIFIER:
            if artifacts.compiled_sql is None:
                missing.append("compiled_sql（请先执行 编译执行 生成 Compiled SQL）")
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
                stage, [
                    f"request_id '{request_id}' 对应的 artifacts 不存在或已过期。"
                    f"请先在编辑器中点击「编译执行」生成基础产物（Contract + SQL Plan），"
                    f"然后再触发 Spark 管线阶段。"
                ]
            )

        # Step 2: 获取 Spark 上下文
        context = self._get_or_create_spark_context(request_id)

        # Step 3: 依赖门禁
        self._check_stage_dependencies(stage, context, artifacts)

        # Step 4: 执行阶段（包装在监控 stage 上下文中）
        # 先清除该阶段的旧错误——同一 request_id 重复点击时不得累积重复错误
        stage_error_prefix = f"[{stage_val}] "
        context.errors = [e for e in context.errors if not e.startswith(stage_error_prefix)]

        stage_node = f"spark_{stage_val.lower()}"
        collector = get_collector()
        try:
            with collector.stage(stage_node, request_id):
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
                    # ── CRE shadow 最终硬化：物理验证后原子追加 CRE 报告到 Review Package ──
                    # ReviewPackageFinalizer 验证已有 Manifest 全部 artifact 哈希后原子追加
                    if context.cre_shadow_report is not None:
                        from tianshu_datadev.artifacts.finalizer import (
                            ReviewPackageFinalizer,
                        )
                        finalizer = ReviewPackageFinalizer(self._base_output_dir)
                        cre_result = finalizer.finalize(
                            request_id, context.cre_shadow_report,
                        )
                        if not cre_result.success:
                            # ── Point 2：失败时 CRE 标记 diagnostic_available=False、
                            #    audit_status=INCOMPLETE ──
                            # 禁止只写 warning——必须在 API/阶段报告中可见
                            context.cre_shadow_report.diagnostic_available = False
                            cre_err_msg = (
                                f"[CRE_FINALIZER] CRE shadow 写入 Review Package 失败："
                                f"audit_status={cre_result.audit_status}，"
                                f"错误={'; '.join(cre_result.errors)}"
                            )
                            context.errors.append(cre_err_msg)
                            logger.error(cre_err_msg)
                        else:
                            logger.info(
                                "CRE shadow 已写入 Review Package：request_id=%s, "
                                "cre_hash=%s, artifacts: %d → %d",
                                request_id,
                                cre_result.cre_shadow_report_hash,
                                cre_result.artifacts_before,
                                cre_result.artifacts_after,
                            )
        except Exception as e:
            context.stage_results[stage_val] = "FAILURE"
            # 去重：同一阶段同一异常不重复追加
            new_error = f"[{stage_val}] 异常：{e}"
            if new_error not in context.errors:
                context.errors.append(new_error)

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

        # PHYSICAL_VERIFIER 特殊处理：物理不一致时标记 failed
        # SAMPLED_CONSISTENT（溢出降级：行数一致 + 抽样一致）视为通过
        if stage == SparkPipelineStage.PHYSICAL_VERIFIER and current_status == "ok":
            report = context.physical_verify_report
            if report is not None and report.status not in (
                PhysicalVerificationStatus.RESULT_CONSISTENT,
                PhysicalVerificationStatus.SAMPLED_CONSISTENT,
            ):
                current_status = "failed"
                # 将验证失败原因写入 errors——确保流式消息和面板有内容展示
                _fail_reason = (
                    report.error_message
                    if report and report.error_message
                    else f"物理验证结论: {report.status.value if report else 'UNKNOWN'}"
                )
                context.errors.append(f"[PHYSICAL_VERIFIER] {_fail_reason}")
                # 同步更新 spark_stages 中的阶段状态——确保 run_spark_stage 响应内部一致
                for s in spark_stages:
                    if s["stage"] == stage_val:
                        s["status"] = current_status

        # ── 构建阶段特有结果内容（供前端面板渲染）──
        result: dict | None = None
        if current_status == "ok":
            if stage == SparkPipelineStage.MAPPER and context.spark_plan is not None:
                result = {
                    "type": "mapper",
                    "steps": [
                        {
                            "step_type": (
                                s.step_type.value
                                if hasattr(s.step_type, "value")
                                else str(s.step_type)
                            ),
                            "description": _summarize_step(s),
                        }
                        for s in context.spark_plan.steps
                    ],
                    "step_count": len(context.spark_plan.steps),
                    "plan_id": context.spark_plan.plan_id,
                }
            elif stage == SparkPipelineStage.COMPILER and context.compile_result is not None:
                result = {
                    "type": "compiler",
                    "pyspark_code": context.compile_result.annotated_pyspark,
                    "raw_hash": context.compile_result.raw_hash,
                    "step_count": len(context.compile_result.step_ids),
                    "standalone_pyspark": context.standalone_pyspark,
                }
            elif stage == SparkPipelineStage.VALIDATOR:
                result = {
                    "type": "validator",
                    "is_valid": context.stage_results.get("VALIDATOR") == "SUCCESS",
                    "errors": [e for e in context.errors if e.startswith("[VALIDATOR]")],
                }
            elif stage == SparkPipelineStage.COMPARATOR and context.comparator_report is not None:
                report = context.comparator_report
                result = {
                    "type": "comparator",
                    "status": (
                        report.status.value
                        if hasattr(report.status, "value")
                        else str(report.status)
                    ),
                    "step_results": [
                        {
                            "step_type": (
                                r.step_type.value
                                if hasattr(r.step_type, "value")
                                else str(r.step_type)
                            ),
                            "verdict": r.verdict,
                        }
                        for r in report.step_results
                    ] if report.step_results else [],
                    "unsupported_types": report.unsupported_types,
                    "uncovered_step_types": report.uncovered_step_types,
                }
            # ── Phase 8: DEVELOPER 结果构建（含标注数据）──
            elif stage == SparkPipelineStage.DEVELOPER and context.annotation_result is not None:
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

        # 信息型阶段——无论 ok/skipped 都返回解释消息
        if stage == SparkPipelineStage.DEVELOPER and current_status != "ok":
            result = {
                "type": "developer",
                "message": (
                    "LLM 语义标注失败"
                    if current_status == "failed"
                    else "LLM 语义标注阶段——未注入 SparkDeveloperService，已标记 SKIPPED"
                ),
                "skipped": current_status == "skipped",
            }

        # PHYSICAL_VERIFIER——无论 ok/skipped/failed 都返回有界摘要
        if stage == SparkPipelineStage.PHYSICAL_VERIFIER:
            if current_status == "ok":
                report = context.physical_verify_report
                # 抽样降级验证通过时透出说明——用户需知道结论基于抽样而非全量
                _sampled_msg = ""
                if report is not None and report.status == PhysicalVerificationStatus.SAMPLED_CONSISTENT:
                    _sampled_msg = report.error_message
                result = {
                    "type": "physical_verify",
                    "status": "ok",
                    "skipped": False,
                    "message": _sampled_msg,
                    # 双引擎行数与耗时——用于前端展示对比摘要
                    "duckdb_row_count": (
                        report.duckdb_result.raw_row_count
                        if report and report.duckdb_result else None
                    ),
                    "spark_row_count": (
                        report.spark_result.raw_row_count
                        if report and report.spark_result else None
                    ),
                    "duckdb_time_ms": (
                        report.duckdb_result.execution_time_ms
                        if report and report.duckdb_result else None
                    ),
                    "spark_time_ms": (
                        report.spark_result.execution_time_ms
                        if report and report.spark_result else None
                    ),
                    # 验证结论——区分全量一致 / 抽样一致
                    "verification_status": report.status.value if report else None,
                    "row_count_match": report.row_count_match if report else None,
                    "schema_match": report.schema_match if report else None,
                    "total_diff_count": report.total_diff_count if report else None,
                    "sample_rows": {
                        "duckdb": (
                            (report.duckdb_result.sample_rows or [])[:5]
                            if report.duckdb_result else []
                        ),
                        "spark": (report.spark_result.sample_rows or [])[:5] if report.spark_result else [],
                    } if report else None,
                }
            elif current_status == "failed":
                # 物理执行成功但结果不一致——返回有界摘要供人工审核诊断
                report = context.physical_verify_report
                # 兜底消息——report 可能为 None 或 error_message 为空时提供最小上下文
                _failed_msg = (
                    report.error_message
                    if report and report.error_message
                    else "物理验证未通过，无详细错误信息"
                )
                result = {
                    "type": "physical_verify",
                    "status": "failed",
                    "skipped": False,
                    "message": _failed_msg,
                    "row_count_match": report.row_count_match if report else None,
                    "schema_match": report.schema_match if report else None,
                    "total_diff_count": report.total_diff_count if report else None,
                    "diffs": report.diffs[:10] if report and report.diffs else [],
                    "sample_rows": {
                        "duckdb": (
                            (report.duckdb_result.sample_rows or [])[:5]
                            if report.duckdb_result else []
                        ),
                        "spark": (report.spark_result.sample_rows or [])[:5] if report.spark_result else [],
                    } if report else None,
                }
            else:
                # skipped——门禁未通过或 PySpark 不可用
                verify_errors = [
                    e.split("] ", 1)[1] if "] " in e else e
                    for e in context.errors
                    if e.startswith("[PHYSICAL_VERIFIER]")
                ]
                reason = verify_errors[0] if verify_errors else "物理验证阶段未执行"
                result = {
                    "type": "physical_verify",
                    "message": reason,
                    "skipped": True,
                    "errors": verify_errors,
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
            new_error = f"[MAPPER] 映射失败：{'; '.join(gap_msgs)}"
            if new_error not in context.errors:
                context.errors.append(new_error)

    def _do_spark_develop(self, context: SparkStageContext) -> None:
        """执行 DEVELOPER 阶段——LLM 语义标注。

        Phase 8: 注入 SparkDeveloperService 后调用真实 LLM 标注，
        异常时标记 FAILURE，不阻断后续阶段。
        """
        if self._spark_developer_service is None:
            context.stage_results["DEVELOPER"] = "SKIPPED"
            err_msg = "[DEVELOPER] SKIPPED: 未注入 SparkDeveloperService"
            if err_msg not in context.errors:
                context.errors.append(err_msg)
            return

        if context.spark_plan is None:
            context.stage_results["DEVELOPER"] = "SKIPPED"
            err_msg = "[DEVELOPER] SKIPPED: 无 SparkPlan（MAPPER 未执行或失败）"
            if err_msg not in context.errors:
                context.errors.append(err_msg)
            return

        try:
            annotated = self._spark_developer_service.annotate(context.spark_plan)
            context.annotation_result = annotated
            context.stage_results["DEVELOPER"] = "SUCCESS"
        except Exception as e:
            context.stage_results["DEVELOPER"] = "FAILURE"
            new_error = f"[DEVELOPER] 标注异常：{e}"
            if new_error not in context.errors:
                context.errors.append(new_error)

    def _do_spark_compile(self, context: SparkStageContext) -> None:
        """执行 COMPILER 阶段——SparkPlan → PySpark DSL。"""
        from tianshu_datadev.spark.compiler import SparkCompiler
        from tianshu_datadev.spark.models import SparkReadStep

        compiler = SparkCompiler()

        # ── Phase 8B: 传入 DEVELOPER 阶段的 LLM 语义标注 ──
        step_annotations = None
        if context.annotation_result is not None:
            step_annotations = context.annotation_result.annotations

        result = compiler.compile(context.spark_plan, annotations=step_annotations)
        context.compile_result = result
        context.stage_results["COMPILER"] = "SUCCESS"

        # ── 生成独立可执行脚本（wrapper 格式，含 SparkSession 引导）──
        # ── Phase 8B: 使用 annotated_pyspark（含 LLM 业务注释）──
        annotated_pyspark = result.annotated_pyspark
        # 提取所有 ReadStep 的 source_name
        input_names: list[str] = []
        for step in context.spark_plan.steps:
            if isinstance(step, SparkReadStep):
                input_names.append(step.source_name)

        # 构建 wrapper 脚本
        wrapper_lines: list[str] = []
        wrapper_lines.append("from pyspark.sql import SparkSession")
        wrapper_lines.append("from pyspark.sql.functions import *")
        wrapper_lines.append("")
        wrapper_lines.append("")
        wrapper_lines.append("# 以下 transform 函数由编译器自动生成")
        wrapper_lines.append("# 数据源需根据实际路径修改")
        wrapper_lines.append("")
        # 嵌入 annotated_pyspark（含 LLM 业务注释的 transform 函数）
        for line in annotated_pyspark.split("\n"):
            wrapper_lines.append(line)
        wrapper_lines.append("")
        wrapper_lines.append("")
        wrapper_lines.append('if __name__ == "__main__":')
        wrapper_lines.append('    spark = SparkSession.builder.appName("tianshu_datadev") \\')
        wrapper_lines.append('        .master("local[*]") \\')
        wrapper_lines.append('        .config("spark.sql.shuffle.partitions", "4") \\')
        wrapper_lines.append('        .getOrCreate()')
        wrapper_lines.append("")
        wrapper_lines.append("    # ======================")
        wrapper_lines.append("    # 1. 加载数据")
        wrapper_lines.append("    # ======================")
        wrapper_lines.append("    inputs = {")
        for i, name in enumerate(input_names):
            comma = "," if i < len(input_names) - 1 else ""
            wrapper_lines.append(f'        "{name}": spark.read.csv("data/{name}.csv", header=True){comma}')
        wrapper_lines.append("    }")
        wrapper_lines.append("")
        wrapper_lines.append("    # ======================")
        wrapper_lines.append("    # 2. 执行转换")
        wrapper_lines.append("    # ======================")
        wrapper_lines.append("    result = transform(inputs, params=None)")
        wrapper_lines.append("")
        # ── Phase 8B: 追加静态字段解读注释（仅注释块，不进可执行代码）──
        if context.annotation_result and context.annotation_result.annotations:
            last_ann = context.annotation_result.annotations[-1]
            safe_detail = compiler.renderer.render_comment_text(last_ann.intent_detail)
            wrapper_lines.append(f"    # 输出字段说明: {safe_detail}")
        wrapper_lines.append("")
        wrapper_lines.append("    # ======================")
        wrapper_lines.append("    # 3. 输出结果")
        wrapper_lines.append("    # ======================")
        wrapper_lines.append('    print("=== 结果概要 ===")')
        wrapper_lines.append("    result.printSchema()")
        wrapper_lines.append('    print(f"行数: {result.count()}")')
        wrapper_lines.append("    result.show(20, truncate=False)")
        wrapper_lines.append("")
        wrapper_lines.append('    print("=== 执行完毕 ===")')
        wrapper_lines.append("    spark.stop()")

        context.standalone_pyspark = "\n".join(wrapper_lines)
        # ── 存储沙箱可执行代码（纯 transform 函数，供 PhysicalVerifier 传入 executor）──
        context.sandbox_transform_code = result.raw_pyspark

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

    @staticmethod
    def _map_comparator_status(status: ComparisonStatus) -> str:
        """将 COMPARATOR 的 ComparisonStatus 映射为 Pipeline 的阶段结果字符串。

        提取为独立方法方便测试——确保测试验证的是生产代码逻辑，而非测试内复制的局部映射表。
        """
        from tianshu_datadev.spark.plan_comparator import ComparisonStatus

        _status_map: dict = {
            ComparisonStatus.LOGIC_EQUIVALENT: "SUCCESS",
            ComparisonStatus.LOGIC_MISMATCH: "FAILURE",
            ComparisonStatus.LOGIC_UNSUPPORTED: "HUMAN_REVIEW",
            ComparisonStatus.NOT_COVERED: "HUMAN_REVIEW",
            ComparisonStatus.NOT_EXECUTED: "SKIPPED",
        }
        return _status_map.get(status, "HUMAN_REVIEW")

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
        # 统一语义：COMPARATOR 阶段始终标记为 SUCCESS（与 Orchestrator 路径一致），
        # 细粒度对比结果由 comparator_report.status 承载，derive_overall_status 消费
        context.stage_results["COMPARATOR"] = "SUCCESS"

    def _try_build_snapshot_for_physical_verify(
        self,
        artifacts: PipelineArtifactBundle,
        context: SparkStageContext,
    ) -> SnapshotManifest | None:
        """尝试为 PHYSICAL_VERIFIER 构建快照——三级回退策略。

        1. SnapshotBuilder 注入 → 走 SnapshotBuilder.build()（E2E CSV fixture 路径）
        2. DuckDB 数据库可用 → 直接从 DuckDB 导出 Parquet（生产路径）
        3. 都不可用 → SNAPSHOT_NOT_READY

        安全边界：
        - snapshot 只能来自已有 SnapshotManifest / SnapshotBuilder / DuckDB 导出
        - 禁止 fallback 到空临时目录

        Args:
            artifacts: Pipeline 中间产物包（snapshot_manifest 字段会被回写）
            context: Spark 阶段上下文（错误写入 stage_results + errors）

        Returns:
            SnapshotManifest——成功构建的快照清单；None 表示失败（context 已写入错误）
        """
        # ── 路径 1：SnapshotBuilder 注入 → 走既有 CSV fixture 快照流程 ──
        if self._snapshot_builder is not None and self._snapshot_provider is not None:
            # 获取 table_paths——优先从 _results 读取 execute_rich 持久化的 resolved_table_paths，
            # 其次回退到 Pipeline 初始化的 default_table_paths
            results_data = self._results.get(artifacts.request_id, {})
            table_paths = results_data.get("resolved_table_paths") or self._default_table_paths
            if not table_paths:
                context.stage_results["PHYSICAL_VERIFIER"] = "SNAPSHOT_NOT_READY"
                context.errors.append(
                    "[PHYSICAL_VERIFIER] SNAPSHOT_NOT_READY: "
                    "缺少 table_paths——无法确定数据源文件路径。"
                    "请在 Pipeline 初始化时注入 default_table_paths，"
                    "或使用「全流程 Run-All」路径。"
                )
                return None

            # 计算 contract_hash——用于快照溯源
            contract_hash = ""
            if artifacts.data_transform_contract is not None:
                from tianshu_datadev.artifacts.models import DataTransformContractV1
                if isinstance(artifacts.data_transform_contract, DataTransformContractV1):
                    contract_hash = DataTransformContractV1.compute_contract_hash(
                        artifacts.data_transform_contract
                    )
                else:
                    contract_hash = hashlib.sha256(
                        str(artifacts.data_transform_contract).encode()
                    ).hexdigest()

            # 按 SnapshotSourceProvider 白名单过滤 source_tables
            source_tables = list(table_paths.keys())
            allowlisted = set(self._snapshot_provider.allowlisted_tables)
            source_tables = [t for t in source_tables if t in allowlisted]

            if not source_tables:
                context.stage_results["PHYSICAL_VERIFIER"] = "SNAPSHOT_NOT_READY"
                context.errors.append(
                    f"[PHYSICAL_VERIFIER] SNAPSHOT_NOT_READY: "
                    f"table_paths 中的表名 {sorted(table_paths.keys())} "
                    f"不在 SnapshotSourceProvider 白名单 "
                    f"{sorted(allowlisted)} 中。请检查 Pipeline 初始化配置。"
                )
                return None

            # 从 _results 读取 table_mapping，反转为 {物理名: 别名}——供 SnapshotBuilder 使用
            _table_mapping = results_data.get("table_mapping") or {}

            # 通过 SnapshotBuilder.build() 创建快照（不手写 PyArrow）
            try:
                snapshot_manifest = self._snapshot_builder.build(
                    contract_hash=contract_hash,
                    source_tables=source_tables,
                    provider=self._snapshot_provider,
                    table_aliases=_aliases_from_table_mapping(_table_mapping),
                )
                # 回写 artifacts——供后续代码引用（PipelineArtifactBundle 非 frozen）
                artifacts.snapshot_manifest = snapshot_manifest
                logger.info(
                    "PHYSICAL_VERIFIER 快照构建成功——snapshot_id=%s，文件数=%d",
                    snapshot_manifest.snapshot_id,
                    len(snapshot_manifest.files),
                )
                return snapshot_manifest
            except Exception as e:
                context.stage_results["PHYSICAL_VERIFIER"] = "SNAPSHOT_NOT_READY"
                context.errors.append(
                    f"[PHYSICAL_VERIFIER] SNAPSHOT_NOT_READY: "
                    f"SnapshotBuilder.build() 执行失败——{e}"
                )
                logger.warning("PHYSICAL_VERIFIER 快照构建失败：%s", e)
                return None

        # ── 路径 2：DuckDB 数据库可用 → 直接从 DuckDB 导出 Parquet 快照 ──
        if self._duckdb_path is not None:
            return self._build_snapshot_from_duckdb(artifacts, context)

        # ── 路径 3：无任何快照数据源 ──
        context.stage_results["PHYSICAL_VERIFIER"] = "SNAPSHOT_NOT_READY"
        context.errors.append(
            "[PHYSICAL_VERIFIER] SNAPSHOT_NOT_READY: "
            "缺少 SnapshotBuilder/SnapshotProvider 注入且无 DuckDB 数据库——"
            "无法创建数据快照。请检查 Pipeline 初始化配置，"
            "或使用「全流程 Run-All」路径（该路径会自动创建快照）。"
        )
        return None

    def _build_snapshot_from_duckdb(
        self,
        artifacts: PipelineArtifactBundle,
        context: SparkStageContext,
    ) -> SnapshotManifest | None:
        """复用 Run-All 的受控快照路径，禁止独立全表导出。"""
        contract = artifacts.data_transform_contract
        if contract is None or not contract.input_tables:
            context.stage_results["PHYSICAL_VERIFIER"] = "SNAPSHOT_NOT_READY"
            context.errors.append(
                "[PHYSICAL_VERIFIER] SNAPSHOT_NOT_READY: "
                "Contract 无 input_tables——无法确定需要快照的源表。"
            )
            return None

        results_data = self._results.get(artifacts.request_id, {})
        try:
            manifest, _ = self._prepare_run_all_snapshot(
                contract=contract,
                table_mapping=results_data.get("table_mapping") or {},
                table_paths=None,
                spec=results_data.get("parsed_spec"),
            )
        except Exception as exc:
            from tianshu_datadev.spark.snapshot import SnapshotEmptyForFilterError

            context.stage_results["PHYSICAL_VERIFIER"] = (
                "SNAPSHOT_EMPTY_FOR_FILTER"
                if isinstance(exc, SnapshotEmptyForFilterError)
                else "SNAPSHOT_NOT_READY"
            )
            context.errors.append(
                "[PHYSICAL_VERIFIER] SNAPSHOT_NOT_READY: "
                f"受控快照构建失败——{exc}"
            )
            logger.warning("PHYSICAL_VERIFIER 受控快照构建失败：%s", exc)
            return None

        if manifest is None:
            context.stage_results["PHYSICAL_VERIFIER"] = "SNAPSHOT_NOT_READY"
            context.errors.append(
                "[PHYSICAL_VERIFIER] SNAPSHOT_NOT_READY: 受控快照未生成清单"
            )
            return None
        artifacts.snapshot_manifest = manifest
        return manifest

    @staticmethod
    def _should_physical_verify(
        validator_ok: bool,
        comparator_report: "PlanComparisonReport | None",
    ) -> bool:
        """判定 PHYSICAL_VERIFIER 是否应执行。

        物理验证是 ground truth——它实际执行 DuckDB 与 Spark 双引擎并对比结果。
        逻辑对比（Comparator）的结论不影响物理验证的必要性：
        - LOGIC_EQUIVALENT：物理验证确认结果一致
        - LOGIC_MISMATCH：物理验证确认不等价是否影响实际输出
        - LOGIC_UNSUPPORTED：逻辑对比无法覆盖，物理验证是唯一验证手段
        - NOT_COVERED：同上，物理验证提供兜底保障

        仅当 Validator 未通过（安全风险）或 Comparator 未执行（缺少前置条件）时跳过。
        """
        from tianshu_datadev.spark.plan_comparator import ComparisonStatus

        if not validator_ok:
            return False
        if comparator_report is None:
            return False
        if comparator_report.status == ComparisonStatus.NOT_EXECUTED:
            return False
        # 所有其他状态均允许物理验证——物理验证是 ground truth
        return True


    def _do_spark_physical_verify(
        self, artifacts: PipelineArtifactBundle, context: SparkStageContext,
    ) -> None:
        """执行 PHYSICAL_VERIFIER 阶段——双引擎物理结果对比。

        检测 PySpark 运行时环境：
        - 可用时：调用 PhysicalVerifier 执行 DuckDB vs Spark 双引擎对比
        - 不可用时：标记 SKIPPED 并记录跳过原因
        """
        # ── 清除上次运行的缓存报告——避免门禁拒绝/提前返回时写出旧数据 ──
        context.physical_verify_report = None
        context.cre_shadow_report = None

        # ── 诊断日志：打印所有早期返回条件的值 ──
        _diag = {
            "validator_ok": context.stage_results.get("VALIDATOR"),
            "comparator_report": (
                type(context.comparator_report).__name__
                if context.comparator_report else None
            ),
            "sandbox_transform_code_set": context.sandbox_transform_code is not None,
            "sandbox_transform_code_len": (
                len(context.sandbox_transform_code)
                if context.sandbox_transform_code else 0
            ),
            "compiled_sql_set": artifacts.compiled_sql is not None,
            "snapshot_manifest_set": artifacts.snapshot_manifest is not None,
            "snapshot_builder_set": self._snapshot_builder is not None,
            "duckdb_path_set": self._duckdb_path is not None,
            "compile_result_set": context.compile_result is not None,
        }
        logger.warning("[PHYSVER_DIAG] _do_spark_physical_verify 入口: %s", _diag)

        # ── 门禁检查 ──
        validator_ok = context.stage_results.get("VALIDATOR") == "SUCCESS"
        # 诊断：打印 COMPARATOR 状态和步骤结论
        if context.comparator_report:
            _cr = context.comparator_report
            _cr_status = _cr.status.value if hasattr(_cr.status, "value") else str(_cr.status)
            _cr_steps = [
                f"{r.step_type}={r.verdict.value if hasattr(r.verdict, 'value') else r.verdict}"
                for r in (_cr.step_results or [])
            ]
            logger.warning("[PHYSVER_DIAG] COMPARATOR状态=%s, 步骤=%s", _cr_status, _cr_steps)
        if not self._should_physical_verify(validator_ok, context.comparator_report):
            logger.warning("[PHYSVER_DIAG] 退出点1: _should_physical_verify 返回 False, validator_ok=%s",
                           validator_ok)
            context.stage_results["PHYSICAL_VERIFIER"] = "SKIPPED"
            context.errors.append(
                "[PHYSICAL_VERIFIER] SKIPPED: 物理验证门禁未通过"
                "（Validator 未通过，或 Comparator 未执行）"
            )
            return

        # Step 1：检查 PySpark 运行时环境
        try:
            import pyspark  # noqa: F401  # 检测是否已安装
        except ImportError:
            logger.warning("[PHYSVER_DIAG] 退出点2: PySpark 未安装")
            context.stage_results["PHYSICAL_VERIFIER"] = "SKIPPED"
            context.errors.append(
                "[PHYSICAL_VERIFIER] SKIPPED: PySpark 未安装——"
                "请执行 pip install pyspark 后重试"
            )
            return

        # Step 2：检查必要产物
        if context.sandbox_transform_code is None:
            logger.warning("[PHYSVER_DIAG] 退出点3: sandbox_transform_code 为 None")
            context.stage_results["PHYSICAL_VERIFIER"] = "FAILURE"
            context.errors.append(
                "[PHYSICAL_VERIFIER] 错误: 缺少沙箱可执行 PySpark 编译产物（sandbox_transform_code）——"
                "请先执行 COMPILER 阶段。不得 fallback 到 standalone_pyspark。"
            )
            return

        if artifacts.compiled_sql is None:
            logger.warning("[PHYSVER_DIAG] 退出点4: compiled_sql 为 None")
            context.stage_results["PHYSICAL_VERIFIER"] = "SKIPPED"
            context.errors.append(
                "[PHYSICAL_VERIFIER] SKIPPED: 缺少 SQL 编译产物（compiled_sql）——"
                "请先执行 COMPILER 阶段"
            )
            return

        # Step 3：确定快照目录
        snapshot_dir: str | None = None
        snapshot_id: str = ""
        if artifacts.snapshot_manifest is not None:
            snapshot_dir = artifacts.snapshot_manifest.snapshot_dir
            snapshot_id = artifacts.snapshot_manifest.snapshot_id

        # 无快照清单时——尝试通过 SnapshotBuilder 实时构建（禁止空目录兜底）
        if snapshot_dir is None:
            snapshot_manifest = self._try_build_snapshot_for_physical_verify(
                artifacts, context,
            )
            if snapshot_manifest is None:
                # _try_build_snapshot_for_physical_verify 已将 SNAPSHOT_NOT_READY 错误写入 context
                return
            snapshot_dir = snapshot_manifest.snapshot_dir
            snapshot_id = snapshot_manifest.snapshot_id

        # Step 4：提取排序键（从 SparkPlan 的 SortStep 中获取）
        order_keys: list[str] = []
        if context.spark_plan is not None:
            order_keys = _extract_spark_order_keys(context.spark_plan)

        # Step 5：获取 unsupported step types（从 Comparator 报告中继承）
        uncovered_types: list[str] = []
        if context.comparator_report is not None:
            uncovered_types = list(context.comparator_report.uncovered_step_types)

        # Step 6：执行双引擎物理验证
        try:
            from tianshu_datadev.cre_models import (
                DecimalStrategy,
                EnvironmentManifest,
                NormalizationColumn,
                NullStrategy,
                SpecialFloatStrategy,
            )
            from tianshu_datadev.spark.physical_verifier import (
                NormalizationConfig,
                PhysicalVerifier,
            )

            # ── 入口适配：DataTransformContractLite → DataTransformContractV1 ──
            # 单表路径（run_all）产出 DataTransformContractLite，而物理验证的所有
            # isinstance(contract, DataTransformContractV1) 检查仅在 V1 路径下提取
            # output_columns / grouping_keys / timezone / 列类型信息。
            # Lite 路径下这些检查全部跳过 → output_cols=0、primary_keys=[]、
            # 类型分析跳过 → 所有类型感知特性失效（float isclose、Decimal quantize、
            # CRE shadow 行对齐）。入口适配一次，后续所有 V1 检查自然命中。
            if artifacts.data_transform_contract is not None:
                from tianshu_datadev.artifacts.models import DataTransformContractLite
                if isinstance(artifacts.data_transform_contract, DataTransformContractLite):
                    from tianshu_datadev.spark.contract_adapter import adapt_lite_to_v1
                    artifacts.data_transform_contract = adapt_lite_to_v1(
                        artifacts.data_transform_contract,
                    )

            contract_hash = ""
            if artifacts.data_transform_contract is not None:
                from tianshu_datadev.artifacts.models import DataTransformContractV1
                if isinstance(artifacts.data_transform_contract, DataTransformContractV1):
                    contract_hash = DataTransformContractV1.compute_contract_hash(
                        artifacts.data_transform_contract
                    )
                else:
                    contract_hash = hashlib.sha256(
                        str(artifacts.data_transform_contract).encode()
                    ).hexdigest()

            sql_query = (
                artifacts.compiled_sql.sql
                if hasattr(artifacts.compiled_sql, "sql")
                else str(artifacts.compiled_sql)
            )
            # 从 Contract output_columns 提取权威 schema 列定义
            norm_columns: list[NormalizationColumn] = []
            primary_keys: list[str] = []
            if artifacts.data_transform_contract is not None:
                from tianshu_datadev.artifacts.models import DataTransformContractV1
                if isinstance(artifacts.data_transform_contract, DataTransformContractV1):
                    for col in artifacts.data_transform_contract.output_columns:
                        norm_columns.append(NormalizationColumn(
                            column_name=col.alias or col.column_name,
                            data_type=col.data_type,
                        ))
                    # 从 Contract grouping_keys 提取权威主键（CRE shadow 行对齐用）
                    # fallback 链：grouping_keys → output_grain → business_keys → 全部非聚合列
                    # 目标：避免聚合列（avg_distance, total_fare）参与排序导致浮点尾差误报
                    if artifacts.data_transform_contract.grouping_keys:
                        primary_keys = list(artifacts.data_transform_contract.grouping_keys)
                    elif artifacts.data_transform_contract.output_grain:
                        primary_keys = list(artifacts.data_transform_contract.output_grain)
                    elif artifacts.data_transform_contract.business_keys:
                        primary_keys = list(artifacts.data_transform_contract.business_keys)

            # ── 构造 CRE EnvironmentManifest——明确优先级（Point 4）──
            # 优先级：Contract 显式策略 > 实际执行环境事实 > UNKNOWN
            # 禁止猜测；UNKNOWN 仅使相关语义进入 HUMAN_REVIEW，不得误伤不涉及该策略的精确一致场景

            # Level 1：检测实际执行环境事实（引擎版本）
            try:
                import duckdb as _duckdb_mod
                _duckdb_ver = _duckdb_mod.__version__
            except ImportError:
                _duckdb_ver = "UNKNOWN"
            try:
                import pyspark as _pyspark_mod
                _spark_ver = _pyspark_mod.__version__
            except ImportError:
                _spark_ver = "UNKNOWN"

            # Level 2：从 Contract 提取显式策略（如有）
            contract_timezone = ""
            has_float_columns = False
            has_decimal_columns = False
            if artifacts.data_transform_contract is not None:
                from tianshu_datadev.artifacts.models import DataTransformContractV1
                if isinstance(artifacts.data_transform_contract, DataTransformContractV1):
                    contract_timezone = getattr(
                        artifacts.data_transform_contract, "timezone", "",
                    ) or ""
                    # 分析输出列类型——确定哪些策略是相关的
                    for col in artifacts.data_transform_contract.output_columns:
                        dt = (col.data_type or "").lower()
                        if dt in ("float", "double", "real", "float4", "float8"):
                            has_float_columns = True
                        elif dt.startswith("decimal") or dt.startswith("numeric"):
                            has_decimal_columns = True

            # Level 3：应用优先级——Contract 显式 > 环境事实 > UNKNOWN
            # - 引擎版本：来自实际执行环境（Level 1）
            # - 时区：来自 Contract（Level 2）
            # - 特殊浮点策略：无 float/double 列 → 安全设为 EQUAL（不会遇到 NaN/Inf）
            #   有 float 列但未声明策略 → UNKNOWN（保守回退）
            # - Decimal 策略：无 decimal 列 → 安全设为 EXACT
            #   有 decimal 列但未声明策略 → UNKNOWN
            # - NULL 策略、ANSI SQL、大小写敏感：无法证明 → UNKNOWN
            cre_env_manifest = EnvironmentManifest(
                duckdb_version=_duckdb_ver,
                spark_version=_spark_ver,
                timezone=contract_timezone,
                # 无法从 Contract 或环境证明的字段——设为 None/UNKNOWN
                ansi_sql=None,
                case_sensitive_compare=None,
                # NaN/Inf 策略：无 float 列则安全，否则 UNKNOWN
                nan_handling=(
                    SpecialFloatStrategy.EQUAL if not has_float_columns
                    else SpecialFloatStrategy.UNKNOWN
                ),
                pos_inf_handling=(
                    SpecialFloatStrategy.EQUAL if not has_float_columns
                    else SpecialFloatStrategy.UNKNOWN
                ),
                neg_inf_handling=(
                    SpecialFloatStrategy.EQUAL if not has_float_columns
                    else SpecialFloatStrategy.UNKNOWN
                ),
                # Decimal 策略：无 decimal 列则安全，否则 UNKNOWN
                decimal_strategy=(
                    DecimalStrategy.EXACT if not has_decimal_columns
                    else DecimalStrategy.UNKNOWN
                ),
                # NULL 策略——无法从 Contract 证明，始终 UNKNOWN
                null_strategy=NullStrategy.UNKNOWN,
            )

            # 构造规范化配置（Phase 9B）
            norm_config = NormalizationConfig(
                output_columns=norm_columns,
                contract_hash=contract_hash,
                primary_keys=primary_keys,
            )
            verifier = PhysicalVerifier(normalization_config=norm_config)
            report = verifier.verify(
                sql_query=sql_query,
                compiled_program=artifacts.compiled_program,
                pyspark_code=context.sandbox_transform_code,
                snapshot_dir=snapshot_dir,
                contract_hash=contract_hash,
                snapshot_id=snapshot_id,
                order_keys=order_keys if order_keys else None,
                uncovered_step_types=uncovered_types if uncovered_types else None,
                duckdb_path=self._duckdb_path,
                # CRE shadow 参数——Pipeline 显式传入
                cre_primary_keys=primary_keys if primary_keys else None,
                cre_timezone=contract_timezone,
                cre_environment_manifest=cre_env_manifest,
            )

            # ── 保存完整物理验证报告到上下文 ──
            context.physical_verify_report = report

            # Step 7：将 CRE shadow 报告存入上下文——严格 Pydantic 模型，清除 dict 逃生口
            # 后续由 RECAP/REVIEWER 阶段经 PackageInputs → ReviewPackageBuilder 一次性写入
            if report.cre_shadow_report is not None:
                context.cre_shadow_report = report.cre_shadow_report

            # Step 8：判定结果
            from tianshu_datadev.spark.physical_verifier import PhysicalVerificationStatus
            if report.status == PhysicalVerificationStatus.RESULT_CONSISTENT:
                context.stage_results["PHYSICAL_VERIFIER"] = "SUCCESS"
                context.errors.append(
                    "[PHYSICAL_VERIFIER] 物理验证通过——双引擎输出一致"
                )
            elif report.status == PhysicalVerificationStatus.SAMPLED_CONSISTENT:
                # 溢出降级验证通过——行数一致 + 键对齐抽样一致（弱于全量一致）
                context.stage_results["PHYSICAL_VERIFIER"] = "SUCCESS"
                context.errors.append(
                    f"[PHYSICAL_VERIFIER] 物理验证通过（抽样）——{report.error_message}"
                )
            else:
                context.stage_results["PHYSICAL_VERIFIER"] = "FAILURE"
                diff_count = len(report.diffs)
                diag_msg = (
                    f"[PHYSICAL_VERIFIER] 物理验证未通过——"
                    f"状态={report.status.value}，"
                    f"差异条目数={diff_count}"
                )
                # 真实差异总数（与返回的详细差异数可能不同）
                if report.total_diff_count != diff_count:
                    diag_msg += f"，总差异数={report.total_diff_count}"
                if report.diffs_truncated:
                    diag_msg += "，差异已截断（最多显示20条）"
                # 附加 report.error_message——包含 DuckDB/Spark 执行失败的详细原因
                if report.error_message:
                    diag_msg += f"，详情={report.error_message}"
                # 附加行数与 schema 一致性——快速判断差异量级
                diag_msg += (
                    f"，行数一致={report.row_count_match}"
                    f"，schema一致={report.schema_match}"
                )
                if report.duckdb_result:
                    diag_msg += f"，DuckDB行数={report.duckdb_result.raw_row_count}"
                if report.spark_result:
                    diag_msg += f"，Spark行数={report.spark_result.raw_row_count}"
                # 附加有效排序键——诊断行错位根因
                # 注意：order_keys 是 pipeline 从 SparkSortStep 提取的（可能为空）；
                # verify() 内部会使用 cre_primary_keys 作为回退。此处显示实际生效的键。
                if order_keys:
                    sort_key_desc = str(order_keys)
                elif primary_keys:
                    sort_key_desc = str(primary_keys)
                else:
                    sort_key_desc = "自动(全部结果列)"
                diag_msg += f"，排序键={sort_key_desc}"
                # 附加差异列分布——区分"少数列有系统偏差"与"全列错位"
                if report.diffs:
                    from collections import Counter
                    col_dist = Counter(d.column for d in report.diffs)
                    diag_msg += f"，差异列分布={col_dist.most_common(10)}"
                # 附加 CRE shadow 状态
                if report.cre_shadow_report is not None:
                    cre_status_val = _safe_enum_value(
                        report.cre_shadow_report, "cre_status",
                    )
                    diag_msg += f"，CRE状态={cre_status_val}"
                # 附加容差配置快照
                if report.normalization_config_snapshot:
                    snap = report.normalization_config_snapshot
                    diag_msg += (
                        f"，容差配置="
                        f"float_abs={snap.get('float_abs_tolerance', 'N/A')}, "
                        f"output_cols={snap.get('output_column_count', 'N/A')}"
                    )
                # 附加 DuckDB 侧错误
                if report.duckdb_result and report.duckdb_result.error_message:
                    diag_msg += f"，DuckDB错误={report.duckdb_result.error_message}"
                # 附加 Spark 侧错误
                if report.spark_result and report.spark_result.error_message:
                    diag_msg += f"，Spark错误={report.spark_result.error_message}"
                context.errors.append(diag_msg)
        except Exception as e:
            context.stage_results["PHYSICAL_VERIFIER"] = "FAILURE"
            context.errors.append(f"[PHYSICAL_VERIFIER] 执行异常：{e}")

def _safe_enum_value(obj, attr: str) -> str:
    """安全获取枚举属性的字符串值——兼容 Enum 和普通属性。"""
    val = getattr(obj, attr, "")
    if hasattr(val, "value"):
        return val.value
    return str(val)


def _extract_spark_order_keys(spark_plan: "SparkPlan") -> list[str]:
    """从首个 Spark Sort 步骤提取列名，仅用于确定性结果对齐。"""
    from tianshu_datadev.spark.models import SparkSortStep

    for step in spark_plan.steps:
        if isinstance(step, SparkSortStep):
            return [item.column for item in step.order_by]
    return []


# ════════════════════════════════════════════
# Spark 阶段独立触发——上下文缓存与异常
# ════════════════════════════════════════════


def _summarize_step(step: SparkStep) -> str:
    """生成 SparkPlan 步骤的人类可读摘要。

    用于前端 SparkStageResultPanel 展示每个步骤的简要描述。
    """
    from tianshu_datadev.spark.models import (
        SparkAggregateStep,
        SparkCaseWhenStep,
        SparkFilterStep,
        SparkJoinStep,
        SparkLimitStep,
        SparkProjectStep,
        SparkReadStep,
        SparkSortStep,
        SparkWindowStep,
    )

    if isinstance(step, SparkReadStep):
        cols = f" ({len(step.required_columns)} 列)" if step.required_columns else ""
        return f"读取 {step.alias} ← {step.source_name}{cols}"
    elif isinstance(step, SparkFilterStep):
        return f"过滤 {step.input_alias}: {step.left} {step.operator} {step.right}"
    elif isinstance(step, SparkJoinStep):
        jt = step.join_type.value if hasattr(step.join_type, "value") else str(step.join_type)
        return f"{jt} JOIN {step.left_alias}.{step.left_key} = {step.right_alias}.{step.right_key}"
    elif isinstance(step, SparkAggregateStep):
        groups = ", ".join(step.group_keys) if step.group_keys else "(无分组)"
        metrics = ", ".join(m.alias for m in step.metrics)
        return f"聚合 {step.input_alias} by [{groups}] → {metrics}"
    elif isinstance(step, SparkProjectStep):
        cols = ", ".join(c.alias for c in step.columns[:5])
        if len(step.columns) > 5:
            cols += f"…(+{len(step.columns) - 5})"
        return f"投影 {step.input_alias} → [{cols}]"
    elif isinstance(step, SparkCaseWhenStep):
        return f"CASE WHEN {step.input_alias} → {step.output_alias} ({len(step.branches)} 分支)"
    elif isinstance(step, SparkWindowStep):
        funcs = ", ".join(e.alias for e in step.expressions[:3])
        if len(step.expressions) > 3:
            funcs += f"…(+{len(step.expressions) - 3})"
        return f"窗口 {step.input_alias}: {funcs}"
    elif isinstance(step, SparkSortStep):
        orders = ", ".join(
            f"{s.column} "
            f"{s.direction.value if hasattr(s.direction, 'value') else s.direction}"
            for s in step.order_by
        )
        return f"排序 {step.input_alias}: {orders}"
    elif isinstance(step, SparkLimitStep):
        return f"LIMIT {step.limit} on {step.input_alias}"
    return f"{step.step_type.value if hasattr(step.step_type, 'value') else step.step_type}: {step}"


@dataclass
class SparkStageContext:
    """request_id 级别的 Spark 阶段中间产物缓存。

    由 Pipeline._get_or_create_spark_context() 创建和管理，
    独立于 SparkOrchestrator 的内部缓存。
    """
    spark_plan: "SparkPlan | None" = None
    compile_result: "SparkCompileResult | None" = None
    standalone_pyspark: str | None = None  # 独立可执行 PySpark 脚本（含 SparkSession 引导，仅人审 artifact）
    sandbox_transform_code: str | None = None  # 沙箱可执行 PySpark 代码（纯 transform 函数，不含 spark.read）
    comparator_report: "PlanComparisonReport | None" = None
    # ── Phase 8: DEVELOPER 阶段产物缓存 ──
    annotation_result: "AnnotatedSparkPlan | None" = None
    stage_results: dict[str, str] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    # ── CRE shadow 最终硬化：严格 Pydantic 模型，清除 dict 逃生口 ──
    cre_shadow_report: "CreShadowReport | None" = None
    physical_verify_report: "PhysicalVerificationReport | None" = None


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
