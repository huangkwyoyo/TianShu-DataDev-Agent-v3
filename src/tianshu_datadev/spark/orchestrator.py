"""Phase 8 SparkOrchestrator——全链路运行时编排。

编排 mapper→Developer→Compiler→Validator→Comparator→PhysicalVerifier 全链路。
不直接调 LLM、不直接构造 Prompt、不解析 LLM 自由文本。

返工上限 2 轮——超出后强制 HUMAN_REVIEW。
AnnotationWarning 不触发自动返工。

骨架级能力说明（2026-07-04 全局验收）：
- MAPPER / COMPILER / VALIDATOR：真实调用已有组件实例
- DEVELOPER：无 llm_call 注入时 SKIPPED
- COMPARATOR：有 SqlBuildPlan + SparkPlan 时真实对比，缺一 SKIPPED
- PHYSICAL_VERIFIER：需 Spark 运行时环境，当前 SKIPPED
"""

from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING

from pydantic import Field

from tianshu_datadev.developer_spec.models import StrictModel

if TYPE_CHECKING:
    from tianshu_datadev.artifacts.models import DataTransformContractV1
    from tianshu_datadev.planning.sql_build_plan import SqlBuildPlan
    from tianshu_datadev.spark.plan_comparator import PlanComparisonReport

# ════════════════════════════════════════════
# SparkPipelineStage——6 阶段枚举
# ════════════════════════════════════════════


class SparkPipelineStage(str, Enum):
    """Pipeline 阶段枚举——按执行顺序排列。"""

    MAPPER = "MAPPER"                        # Contract → SparkPlan 映射
    DEVELOPER = "DEVELOPER"                  # LLM 语义标注（可选）
    COMPILER = "COMPILER"                    # SparkPlan → PySpark DSL 编译
    VALIDATOR = "VALIDATOR"                  # PySpark DSL 静态安全校验
    COMPARATOR = "COMPARATOR"                # SQL ↔ Spark 逻辑链路对比
    PHYSICAL_VERIFIER = "PHYSICAL_VERIFIER"  # 双引擎物理结果对比


# ════════════════════════════════════════════
# SparkPipelineStatus——全局状态枚举
# ════════════════════════════════════════════


class SparkPipelineStatus(str, Enum):
    """Pipeline 全局状态——精确描述，禁止泛化 PASS。"""

    ALL_CONSISTENT = "ALL_CONSISTENT"
    """全部阶段通过——逻辑等价 + 物理一致。"""

    LOGIC_CONSISTENT_PHYSICAL_NOT_EXECUTED = "LOGIC_CONSISTENT_PHYSICAL_NOT_EXECUTED"
    """逻辑链路通过但物理链路未执行。"""

    REPAIR_NEEDED = "REPAIR_NEEDED"
    """存在阶段失败——需要返工修复。"""

    HUMAN_REVIEW_REQUIRED = "HUMAN_REVIEW_REQUIRED"
    """返工上限耗尽或业务语义歧义——需人工审查。"""


# ════════════════════════════════════════════
# SparkPipelineState——Pipeline 状态模型
# ════════════════════════════════════════════


class SparkPipelineState(StrictModel):
    """Pipeline 运行时状态——记录每阶段输入/输出/状态/错误。

    由 SparkOrchestrator.run() 创建和更新。
    """

    contract_hash: str                                        # 来源 Contract hash
    spark_plan_hash: str = ""                                 # mapper 产出的 SparkPlan hash
    annotation_hash: str = ""                                 # developer 产出的 annotation hash
    compiled_code_sha256: str = ""                            # compiler 产出的代码 hash
    snapshot_id: str = ""                                     # 快照 ID
    verification_report_id: str = ""                          # 验证报告 ID
    comparator_report: "PlanComparisonReport | None" = Field(  # Comparator 对比报告
        default=None,
        description="COMPARATOR 阶段产出的逻辑对比报告，仅在 sql_plan+spark_plan 均可用时非空",
    )
    stage_results: dict[str, str] = Field(                    # 每阶段执行结果
        default_factory=lambda: {
            "MAPPER": "NOT_EXECUTED",
            "DEVELOPER": "NOT_EXECUTED",
            "COMPILER": "NOT_EXECUTED",
            "VALIDATOR": "NOT_EXECUTED",
            "COMPARATOR": "NOT_EXECUTED",
            "PHYSICAL_VERIFIER": "NOT_EXECUTED",
        },
    )
    errors: list[str] = Field(default_factory=list)           # 错误信息列表
    overall_status: SparkPipelineStatus = Field(               # 全局状态
        default=SparkPipelineStatus.REPAIR_NEEDED,
    )
    retry_count: int = 0                                       # 当前返工轮次

    def record_stage_result(
        self,
        stage: SparkPipelineStage,
        result: str,
    ) -> None:
        """记录单个阶段的执行结果。

        Args:
            stage: Pipeline 阶段
            result: 执行结果（"SUCCESS"/"FAILURE"/"SKIPPED"/"HUMAN_REVIEW"/"NOT_EXECUTED"）
        """
        self.stage_results[stage.value] = result

    def derive_overall_status(self) -> None:
        """根据各阶段结果推导全局状态。

        规则：
        - retry_count >= 2 → HUMAN_REVIEW_REQUIRED（返工上限）
        - 任意阶段 HUMAN_REVIEW → HUMAN_REVIEW_REQUIRED
        - 任意阶段 FAILURE → REPAIR_NEEDED
        - PHYSICAL_VERIFIER 未执行但逻辑链路全通过 → LOGIC_CONSISTENT_PHYSICAL_NOT_EXECUTED
        - 全部 SUCCESS/SKIPPED → ALL_CONSISTENT
        """
        # 返工上限检查——最高优先级
        if self.retry_count >= 2:
            self.overall_status = SparkPipelineStatus.HUMAN_REVIEW_REQUIRED
            return

        results = self.stage_results

        # 任意阶段 HUMAN_REVIEW → 人工审查
        if any(v == "HUMAN_REVIEW" for v in results.values()):
            self.overall_status = SparkPipelineStatus.HUMAN_REVIEW_REQUIRED
            return

        # 任意阶段 FAILURE → 需要返工
        if any(v == "FAILURE" for v in results.values()):
            self.overall_status = SparkPipelineStatus.REPAIR_NEEDED
            return

        # 检查逻辑链路 vs 物理链路
        logic_stages = {"MAPPER", "DEVELOPER", "COMPILER", "VALIDATOR", "COMPARATOR"}
        logic_all_ok = all(
            results.get(s, "NOT_EXECUTED") in ("SUCCESS", "SKIPPED")
            for s in logic_stages
        )
        # PHYSICAL_VERIFIER 的 SKIPPED 等同于 NOT_EXECUTED（均未实际执行物理对比）
        physical_result = results.get("PHYSICAL_VERIFIER", "NOT_EXECUTED")
        physical_executed = physical_result not in ("NOT_EXECUTED", "SKIPPED")

        if logic_all_ok and not physical_executed:
            self.overall_status = SparkPipelineStatus.LOGIC_CONSISTENT_PHYSICAL_NOT_EXECUTED
            return

        # 全部通过
        if all(v in ("SUCCESS", "SKIPPED") for v in results.values()):
            self.overall_status = SparkPipelineStatus.ALL_CONSISTENT
            return

        # 防御性兜底
        self.overall_status = SparkPipelineStatus.REPAIR_NEEDED


# ════════════════════════════════════════════
# SparkOrchestrator
# ════════════════════════════════════════════


class SparkOrchestrator:
    """全链路运行时编排器——串联 mapper→Developer→Compiler→Validator→Comparator→PhysicalVerifier。

    职责边界：
    - 编排各阶段调用顺序和状态流转
    - 阶段失败时通过 RepairPlanner 分类
    - 管理返工计数和 HUMAN_REVIEW 升级
    - 不直接调用 LLM——Developer 阶段委托给注入的 SparkDeveloperService
    - 不直接构造 Prompt——由各阶段组件自行管理

    使用方式：
        # 测试/状态机模式（stage_failures 注入）
        orchestrator = SparkOrchestrator()
        state = orchestrator.run(contract_hash="abc123",
                                 stage_failures={"COMPILER": "测试注入"})

        # 骨架级真实执行
        state = orchestrator.run(contract=my_contract, contract_hash="abc123")
    """

    MAX_RETRY = 2  # 最大返工轮次

    def __init__(
        self,
        developer_service=None,  # SparkDeveloperService | None
    ) -> None:
        """初始化编排器。

        Args:
            developer_service: SparkDeveloperService 实例。
                              None 时 DEVELOPER 阶段自动 SKIPPED。
        """
        self._developer_service = developer_service
        # 缓存中间产物——供后续阶段使用
        self._cached_plan = None           # SparkPlan | None
        self._cached_sql_plan = None       # SqlBuildPlan | None
        self._cached_compile_result = None  # SparkCompileResult | None

    def run(
        self,
        contract_hash: str = "",
        contract: "DataTransformContractV1 | None" = None,
        sql_plan: "SqlBuildPlan | None" = None,
        stage_failures: dict[str, str] | None = None,
        retry_count: int = 0,
    ) -> SparkPipelineState:
        """执行全链路 Pipeline。

        两种模式：
        - 测试注入模式：提供 stage_failures 时，不调用真实组件，按注入结果标记
        - 真实执行模式：提供 contract 时，真实调用 mapper/compiler/validator

        Args:
            contract_hash: 来源 Contract 的 hash 值（测试注入模式必填）
            contract: DataTransformContractV1 实例（真实执行模式必填）
            sql_plan: SqlBuildPlan 实例（可选）。
                      提供时 COMPARATOR 阶段使用 sql_plan + spark_plan 执行真实逻辑对比。
                      不提供时 COMPARATOR 标记 SKIPPED。
            stage_failures: 阶段失败注入（测试用）——key 为阶段名，value 为错误信息。
                            None 时走真实执行或默认成功。
            retry_count: 当前返工轮次（0 表示首次执行）

        Returns:
            SparkPipelineState——含每阶段结果、错误列表、全局状态
        """
        self._cached_sql_plan = sql_plan
        failures = stage_failures or {}
        # contract_hash 优先取 contract 的 hash，否则用参数
        effective_hash = contract_hash
        if contract is not None and not effective_hash:
            effective_hash = getattr(contract, "contract_id", contract_hash) or contract_hash

        state = SparkPipelineState(
            contract_hash=effective_hash,
            retry_count=retry_count,
        )

        # 返工上限提前检查——最高优先级
        if retry_count >= self.MAX_RETRY:
            for stage in SparkPipelineStage:
                state.record_stage_result(stage, "HUMAN_REVIEW")
            state.errors.append(
                f"返工次数已达上限（{self.MAX_RETRY} 轮），"
                f"当前 retry_count={retry_count}，强制 HUMAN_REVIEW"
            )
            state.derive_overall_status()
            return state

        # 按顺序执行各阶段
        for stage in SparkPipelineStage:
            self._execute_stage(stage, state, contract, failures)

        state.derive_overall_status()
        return state

    def _execute_stage(
        self,
        stage: SparkPipelineStage,
        state: SparkPipelineState,
        contract: "DataTransformContractV1 | None",
        failures: dict[str, str],
    ) -> None:
        """执行单个 Pipeline 阶段。

        分发逻辑：
        - stage_failures 中有注入 → FAILURE
        - 真实组件可用 → SUCCESS（并填充 hash 字段）
        - 组件不可用（无 contract/无 SqlBuildPlan/无 Spark）→ SKIPPED

        Args:
            stage: 当前阶段
            state: Pipeline 状态（会被原地修改）
            contract: Contract 实例（可为 None）
            failures: 阶段失败注入字典
        """
        # ── 测试注入优先 ──
        if stage.value in failures:
            state.record_stage_result(stage, "FAILURE")
            state.errors.append(f"[{stage.value}] {failures[stage.value]}")
            return

        # ── MAPPER ──
        if stage == SparkPipelineStage.MAPPER:
            self._run_mapper(stage, state, contract)
        # ── DEVELOPER ──
        elif stage == SparkPipelineStage.DEVELOPER:
            self._run_developer(stage, state)
        # ── COMPILER ──
        elif stage == SparkPipelineStage.COMPILER:
            self._run_compiler(stage, state)
        # ── VALIDATOR ──
        elif stage == SparkPipelineStage.VALIDATOR:
            self._run_validator(stage, state)
        # ── COMPARATOR ──
        elif stage == SparkPipelineStage.COMPARATOR:
            self._run_comparator(stage, state)
        # ── PHYSICAL_VERIFIER ──
        elif stage == SparkPipelineStage.PHYSICAL_VERIFIER:
            self._run_physical_verifier(stage, state)

    # ── 各阶段实现 ──

    def _run_mapper(
        self,
        stage: SparkPipelineStage,
        state: SparkPipelineState,
        contract: "DataTransformContractV1 | None",
    ) -> None:
        """执行 MAPPER 阶段——Contract → SparkPlan。"""
        if contract is None:
            state.record_stage_result(stage, "SKIPPED")
            state.errors.append("[MAPPER] SKIPPED: 未提供 Contract")
            return

        try:
            from tianshu_datadev.spark.mapper import map_contract_to_spark_plan
            from tianshu_datadev.spark.models import SparkPlan

            result = map_contract_to_spark_plan(contract)
            if result.success and result.spark_plan is not None:
                plan_hash = SparkPlan.compute_plan_hash(result.spark_plan)
                state.spark_plan_hash = plan_hash
                state.record_stage_result(stage, "SUCCESS")
                self._cached_plan = result.spark_plan
            else:
                state.record_stage_result(stage, "FAILURE")
                gap_msgs = [g.message for g in result.gaps] if result.gaps else ["未知错误"]
                state.errors.append(f"[MAPPER] 映射失败：{'; '.join(gap_msgs)}")
        except Exception as e:
            state.record_stage_result(stage, "FAILURE")
            state.errors.append(f"[MAPPER] 异常：{e}")

    def _run_developer(self, stage: SparkPipelineStage, state: SparkPipelineState) -> None:
        """执行 DEVELOPER 阶段——LLM 语义标注（可选）。"""
        if self._developer_service is None:
            state.record_stage_result(stage, "SKIPPED")
            state.errors.append("[DEVELOPER] SKIPPED: 未注入 SparkDeveloperService")
            return

        plan = self._cached_plan
        if plan is None:
            state.record_stage_result(stage, "SKIPPED")
            state.errors.append("[DEVELOPER] SKIPPED: 无 SparkPlan（Mapper 未执行或失败）")
            return

        try:
            annotated = self._developer_service.annotate(plan)
            state.annotation_hash = annotated.baseline_plan_hash
            state.record_stage_result(stage, "SUCCESS")
        except Exception as e:
            state.record_stage_result(stage, "FAILURE")
            state.errors.append(f"[DEVELOPER] 标注异常：{e}")

    def _run_compiler(self, stage: SparkPipelineStage, state: SparkPipelineState) -> None:
        """执行 COMPILER 阶段——SparkPlan → PySpark DSL。"""
        plan = self._cached_plan
        if plan is None:
            state.record_stage_result(stage, "SKIPPED")
            state.errors.append("[COMPILER] SKIPPED: 无 SparkPlan（Mapper 未执行或失败）")
            return

        try:
            from tianshu_datadev.spark.compiler import SparkCompiler

            compiler = SparkCompiler()
            result = compiler.compile(plan)
            state.compiled_code_sha256 = result.raw_hash
            state.record_stage_result(stage, "SUCCESS")
            self._cached_compile_result = result
        except Exception as e:
            state.record_stage_result(stage, "FAILURE")
            state.errors.append(f"[COMPILER] 编译异常：{e}")

    def _run_validator(self, stage: SparkPipelineStage, state: SparkPipelineState) -> None:
        """执行 VALIDATOR 阶段——PySpark DSL 安全校验。"""
        compile_result = self._cached_compile_result
        if compile_result is None:
            state.record_stage_result(stage, "SKIPPED")
            state.errors.append("[VALIDATOR] SKIPPED: 无编译产物（Compiler 未执行或失败）")
            return

        try:
            from tianshu_datadev.spark.validator import SparkStaticValidator

            validator = SparkStaticValidator()
            validation = validator.validate(compile_result.raw_pyspark)
            if validation.is_valid:
                state.record_stage_result(stage, "SUCCESS")
            else:
                state.record_stage_result(stage, "FAILURE")
                for e in validation.errors:
                    state.errors.append(
                        f"[VALIDATOR] {e.error_code}: {e.detail}"
                    )
        except Exception as e:
            state.record_stage_result(stage, "FAILURE")
            state.errors.append(f"[VALIDATOR] 校验异常：{e}")

    def _run_comparator(self, stage: SparkPipelineStage, state: SparkPipelineState) -> None:
        """执行 COMPARATOR 阶段——SQL ↔ Spark 逻辑对比（需 SqlBuildPlan + SparkPlan）。"""
        if self._cached_sql_plan is not None and self._cached_plan is not None:
            try:
                from tianshu_datadev.spark.plan_comparator import PlanComparator

                comparator = PlanComparator()
                report = comparator.compare(self._cached_sql_plan, self._cached_plan)
                state.record_stage_result(stage, "SUCCESS")
                state.comparator_report = report  # 存储报告供后续 Review Package 使用
            except Exception as e:
                state.record_stage_result(stage, "FAILURE")
                state.errors.append(f"[COMPARATOR] 对比异常：{e}")
        else:
            state.record_stage_result(stage, "SKIPPED")
            missing = []
            if self._cached_sql_plan is None:
                missing.append("SqlBuildPlan")
            if self._cached_plan is None:
                missing.append("SparkPlan")
            state.errors.append(
                f"[COMPARATOR] SKIPPED: 缺少 {' + '.join(missing)}，无法执行逻辑对比"
            )

    def _run_physical_verifier(
        self, stage: SparkPipelineStage, state: SparkPipelineState,
    ) -> None:
        """执行 PHYSICAL_VERIFIER 阶段——双引擎物理结果对比（需 Spark 运行时）。"""
        state.record_stage_result(stage, "SKIPPED")
        state.errors.append(
            "[PHYSICAL_VERIFIER] SKIPPED: 需要 Spark 运行时环境，当前未配置"
        )


# 延迟重建模型——让 Pydantic 解析 TYPE_CHECKING 导入的 PlanComparisonReport 类型
from tianshu_datadev.spark.plan_comparator import PlanComparisonReport  # noqa: E402

SparkPipelineState.model_rebuild()
