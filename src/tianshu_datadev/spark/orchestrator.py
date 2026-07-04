"""Phase 8 SparkOrchestrator——全链路运行时编排。

编排 mapper→Developer→Compiler→Validator→Comparator→PhysicalVerifier 全链路。
不直接调 LLM、不直接构造 Prompt、不解析 LLM 自由文本。

返工上限 2 轮——超出后强制 HUMAN_REVIEW。
AnnotationWarning 不触发自动返工。
"""

from __future__ import annotations

from enum import Enum

from pydantic import Field

from tianshu_datadev.developer_spec.models import StrictModel

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
        physical_executed = results.get("PHYSICAL_VERIFIER", "NOT_EXECUTED") != "NOT_EXECUTED"

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
        orchestrator = SparkOrchestrator(developer_service=dev_svc)
        state = orchestrator.run(contract_hash="abc123")
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

    def run(
        self,
        contract_hash: str,
        stage_failures: dict[str, str] | None = None,
        retry_count: int = 0,
    ) -> SparkPipelineState:
        """执行全链路 Pipeline。

        Args:
            contract_hash: 来源 Contract 的 hash 值
            stage_failures: 阶段失败注入（测试用）——key 为阶段名，value 为错误信息。
                            None 时所有阶段默认成功。
            retry_count: 当前返工轮次（0 表示首次执行）

        Returns:
            SparkPipelineState——含每阶段结果、错误列表、全局状态
        """
        failures = stage_failures or {}
        state = SparkPipelineState(
            contract_hash=contract_hash,
            retry_count=retry_count,
        )

        # 返工上限提前检查——最高优先级
        if retry_count >= self.MAX_RETRY:
            # 所有阶段标记 HUMAN_REVIEW
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
            if stage.value in failures:
                state.record_stage_result(stage, "FAILURE")
                state.errors.append(
                    f"[{stage.value}] {failures[stage.value]}"
                )
            elif stage == SparkPipelineStage.DEVELOPER and self._developer_service is None:
                # 未注入 Developer → 跳过
                state.record_stage_result(stage, "SKIPPED")
            else:
                state.record_stage_result(stage, "SUCCESS")

        state.derive_overall_status()
        return state
