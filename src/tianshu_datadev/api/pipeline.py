"""Pipeline——确定性串联全部组件的执行流水线。

所有步骤使用确定性实现，不需要真实 LLM 或生产数据库。
每次调用独立创建组件实例，无状态泄漏。
API 只返回 artifact 引用和结构化摘要。
"""

from __future__ import annotations

import hashlib
import logging
import time
from typing import TYPE_CHECKING

logger = logging.getLogger(__name__)

from tianshu_datadev.artifacts.contract_extractor import DataTransformContractExtractor
from tianshu_datadev.artifacts.packager import PackageInputs, ReviewPackageBuilder
from tianshu_datadev.developer_spec.models import (
    ParsedDeveloperSpec,
)
from tianshu_datadev.developer_spec.parser import DeveloperSpecParser
from tianshu_datadev.developer_spec.source_manifest import build_manifest_from_spec
from tianshu_datadev.planning.cross_validator import cross_validate
from tianshu_datadev.planning.relationship_planner import RelationshipPlanner
from tianshu_datadev.planning.spec_enricher import SpecEnricher
from tianshu_datadev.planning.sql_build_plan import SqlBuildPlan, SqlBuildPlanBuilder
from tianshu_datadev.api.templates import TEMPLATES
from tianshu_datadev.planning.program_factory import (
    build_sql_program,
    build_sql_program_from_chain,
    build_sql_program_from_compute_steps,
)
from tianshu_datadev.sql.compiler import DuckDbSqlCompiler
from tianshu_datadev.sql.executor import DuckDBExecutor
from tianshu_datadev.sql.models import SqlArtifact
from tianshu_datadev.sql.validator import SqlBuildPlanValidator

if TYPE_CHECKING:
    from tianshu_datadev.developer_spec.models import OpenQuestion, ParseWarning, SourceManifest
    from tianshu_datadev.llm.adapters.base import ProviderAdapter
    from tianshu_datadev.planning.relationship_hypothesis import RelationshipHypothesis
    from tianshu_datadev.planning.sql_build_plan import SqlBuildPlan


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
    ):
        """初始化流水线。

        Args:
            base_output_dir: ReviewPackage 输出根目录
            adapter: LLM Provider 适配器——None 时全链路确定性运行（Fake 模式），
                     注入后 RelationshipPlanner + SpecEnricher 均走 LLM 推断。
        """
        self._base_output_dir = base_output_dir
        self._results: dict[str, dict] = {}  # request_id → 内部产物
        self._packages: dict[str, object] = {}  # request_id → ReviewPackageManifest
        self._timestamps: dict[str, float] = {}  # request_id → 写入时间戳（用于 TTL 过期清理）
        self._ttl_seconds: int = 1800  # 缓存过期时间（秒），默认 30 分钟
        # adapter=None 时退化为纯规则/显式声明模式（确定性）
        self._relationship_planner = RelationshipPlanner(adapter=adapter)
        self._spec_enricher = SpecEnricher(adapter=adapter)

    # ── 缓存生命周期管理 ──────────────────────────────

    def _store_result(self, request_id: str, data: dict) -> None:
        """缓存中间结果并记录写入时间戳——供 TTL 过期清理使用。"""
        self._results[request_id] = data
        self._timestamps[request_id] = time.time()

    def _store_package(self, request_id: str, package: object) -> None:
        """缓存打包结果并记录写入时间戳——供 TTL 过期清理使用。"""
        self._packages[request_id] = package
        self._timestamps[request_id] = time.time()

    def _purge_expired(self) -> int:
        """清理所有超过 TTL 的缓存条目。

        遍历 _timestamps 字典，移除 _results 和 _packages 中的过期条目。
        每次公共方法入口调用——惰性清理，零额外定时器开销。

        Returns:
            清理的条目数
        """
        now = time.time()
        expired_ids = [
            rid for rid, ts in self._timestamps.items()
            if now - ts > self._ttl_seconds
        ]
        for rid in expired_ids:
            self._results.pop(rid, None)
            self._packages.pop(rid, None)
            self._timestamps.pop(rid, None)
        if expired_ids:
            logger.debug("TTL 过期清理完成，移除 %d 条缓存", len(expired_ids))
        return len(expired_ids)

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
            all_stages = ["parser", "enrich", "build", "compile", "execute"]
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
            "compile": "编译",
            "execute": "执行",
            "contract": "契约",
            "package": "打包",
        }
        return _names.get(stage, stage)

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
                plan = plans[-1]
                plan_questions: list = []
                all_questions = list(plan_questions) + list(val_questions) + list(extra_questions)

                stage = "compile"
                compiler = DuckDbSqlCompiler(table_mapping=table_mapping or {})
                program_artifact = compiler.compile_program(sql_program)

                stage = "execute"
                execute_executor = DuckDBExecutor(table_paths=table_paths or {})
                program_result = execute_executor.execute_program(
                    program_artifact.compiled
                )
                last_result = program_result.results[-1]
                trace = last_result.trace
                summary = last_result.summary
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
                plan = plans[-1]
                plan_questions: list = []
                all_questions = list(plan_questions) + list(val_questions) + list(extra_questions)

                stage = "compile"
                compiler = DuckDbSqlCompiler(table_mapping=table_mapping or {})
                program_artifact = compiler.compile_program(sql_program)

                stage = "execute"
                execute_executor = DuckDBExecutor(table_paths=table_paths or {})
                program_result = execute_executor.execute_program(
                    program_artifact.compiled
                )
                last_result = program_result.results[-1]
                trace = last_result.trace
                summary = last_result.summary
                compiled = program_artifact.compiled.statements[-1]
            else:
                plan, plan_questions = builder.build(spec, hypothesis=hypothesis)

                # Validator 验证（非阻断——记录问题供排查）
                validator = SqlBuildPlanValidator()
                _passed, val_questions = validator.validate(plan, manifest)
                all_questions = list(plan_questions) + list(val_questions) + list(extra_questions)

                stage = "compile"
                compiler = DuckDbSqlCompiler(table_mapping=table_mapping or {})
                compiled = compiler.compile(plan)

                stage = "execute"
                execute_executor = DuckDBExecutor(table_paths=table_paths or {})
                trace, summary = execute_executor.execute(compiled)

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
                "open_questions": [],
                "pipeline_error": error_info,
                "pipeline_stages": self._build_pipeline_stages(stage, error_info),
            }

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
        _RUN_ALL_STAGES = ["parser", "enrich", "build", "compile", "execute", "contract", "package"]

        # ── Stage 1-2: Parser + Enrich ──
        parsed = self._parse_and_enrich(
            "run_all", markdown_text, table_mapping,
            pipeline_stages=_RUN_ALL_STAGES,
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

                stage = "compile"
                compiler = DuckDbSqlCompiler(table_mapping=table_mapping or {})
                program_artifact = compiler.compile_program(sql_program)
                compiled_sql = program_artifact.compiled.statements[-1]

                stage = "execute"
                executor = DuckDBExecutor(table_paths=table_paths or {})
                program_result = executor.execute_program(
                    program_artifact.compiled
                )
                execution_trace = program_result.results[-1].trace if program_result.results else None

                stage = "contract"
                extractor = DataTransformContractExtractor()
                contract = extractor.extract_v1(sql_program)

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
                    "manifest": manifest,
                    "table_mapping": table_mapping or {},
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
                    "contract": contract.model_dump() if hasattr(contract, "model_dump") else {},
                    "compiled": compiled_sql,
                    "package_manifest": package_manifest.model_dump(
                        exclude_none=True
                    ) if hasattr(package_manifest, "model_dump") else {},
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

                stage = "compile"
                compiler = DuckDbSqlCompiler(table_mapping=table_mapping or {})
                program_artifact = compiler.compile_program(sql_program)
                compiled_sql = program_artifact.compiled.statements[-1]

                stage = "execute"
                execute_executor = DuckDBExecutor(table_paths=table_paths or {})
                program_result = execute_executor.execute_program(
                    program_artifact.compiled
                )
                trace = program_result.results[-1].trace
                summary = program_result.results[-1].summary
            else:
                plan, plan_questions = builder.build(spec, hypothesis=hypothesis)

                validator = SqlBuildPlanValidator()
                passed, val_questions = validator.validate(plan, manifest)

                stage = "compile"
                compiler = DuckDbSqlCompiler(table_mapping=table_mapping or {})
                artifact = compiler.compile_to_artifact(plan, spec.spec_hash)
                compiled_sql = artifact.compiled_sql

                stage = "execute"
                execute_executor = DuckDBExecutor(table_paths=table_paths or {})
                trace, summary = execute_executor.execute(compiled_sql)

                sql_program = build_sql_program(plan, spec.spec_hash)

            # ── 公共阶段：Contract + Package（非 ComputeSteps 路径） ──
            stage = "contract"
            contract_extractor = DataTransformContractExtractor()
            if len(sql_program.statements) > 1:
                contract = contract_extractor.extract_v1(sql_program)
            else:
                contract = contract_extractor.extract(plan)

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
                open_questions=[q.model_dump() for q in spec.open_questions + plan_questions + extra_questions],
                validation_questions=[q.model_dump() for q in val_questions],
                perf_results=[],
                retry_count=0,
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
                "contract": contract.model_dump() if contract is not None and hasattr(contract, "model_dump") else {},
                "compiled": compiled_sql,
                "pipeline_error": error_info,
                "pipeline_stages": self._build_pipeline_stages(stage, error_info, _RUN_ALL_STAGES),
            }

        # ── 成功路径（非 ComputeSteps） ──
        self._store_result(request_id, {
            "parsed_spec": spec,
            "manifest": manifest,
            "plan": plan,
            "compiled": compiled_sql,
            "trace": trace,
            "summary": summary,
            "table_mapping": table_mapping or {},
        })
        self._store_package(request_id, package_manifest)

        result: dict = {
            "request_id": request_id,
            "spec_id": spec.spec_id,
            "plan_id": plan.plan_id,
            "package_id": package_manifest.package_id,
            "package_dir": f"{self._base_output_dir}/{request_id}",
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

        # ── Stage 3-5: Build → Compile → Execute ──
        plan = None
        compiled = None
        all_questions: list = []
        stage = "build"

        try:
            builder = SqlBuildPlanBuilder()
            plan, plan_questions = builder.build(spec, hypothesis=hypothesis)

            validator = SqlBuildPlanValidator()
            _passed, val_questions = validator.validate(plan, manifest)
            all_questions = list(plan_questions) + list(val_questions) + list(extra_questions)

            stage = "compile"
            compiler = DuckDbSqlCompiler(table_mapping=table_mapping or {})
            compiled = compiler.compile(plan)

            stage = "execute"
            executor = DuckDBExecutor(table_paths=table_paths or {})
            trace, summary = executor.execute(compiled)

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
                "generated_sql": getattr(compiled, "sql", "") if compiled is not None else "",
                "sql_sha256": _sql_sha256,
                "compiler_version": _compiler_ver,
                "execution_trace": None,
                "result_summary": None,
                "open_questions": [],
                "pipeline_error": error_info,
                "pipeline_stages": self._build_pipeline_stages(stage, error_info),
            }

        request_id = self._gen_request_id(spec)
        self._store_result(request_id, {
            "parsed_spec": spec, "manifest": manifest, "plan": plan,
            "compiled": compiled, "trace": trace, "summary": summary,
            "table_mapping": table_mapping or {},
        })

        return {
            "request_id": request_id,
            "spec_id": spec.spec_id,
            "plan_id": plan.plan_id,
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
        }


def _safe_enum_value(obj, attr: str) -> str:
    """安全获取枚举属性的字符串值——兼容 Enum 和普通属性。"""
    val = getattr(obj, attr, "")
    if hasattr(val, "value"):
        return val.value
    return str(val)
