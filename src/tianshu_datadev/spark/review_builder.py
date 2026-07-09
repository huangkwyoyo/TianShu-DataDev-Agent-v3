"""Phase 8 SparkReviewBuilder——从 PipelineState 构建 SparkReviewPackage。

复用各阶段产出的 hash 和报告，组装为统一交付物。
不读取 SQL 文本——所有引用均通过 ID。

Phase 9A5 新增 build_review_ready()——在 build() 基础上执行 REVIEW_READY 判定，
将所有关键阶段结果和对比器状态纳入统一交付物。
"""

from __future__ import annotations

import hashlib
import json
from typing import TYPE_CHECKING

from tianshu_datadev.spark.orchestrator import SparkPipelineState
from tianshu_datadev.spark.review_package import (
    CrossReference,
    SparkProvenance,
    SparkReviewPackage,
)

if TYPE_CHECKING:
    from tianshu_datadev.spark.plan_comparator import PlanComparisonReport


class SparkReviewBuilder:
    """构建 SparkReviewPackage——从 PipelineState 提取各阶段产出，组装统一交付物。

    Phase 9A5 增强：
    - build() 自动填充 stage_results / comparator_status / review_ready
    - build_review_ready() 提供显式 REVIEW_READY 装配入口

    使用方式：
        builder = SparkReviewBuilder()
        pkg = builder.build(pipeline_state)           # 含 REVIEW_READY 判定
        pkg = builder.build_review_ready(state, report)  # 显式 REVIEW_READY
    """

    # ── REVIEW_READY 判定所需的关键阶段 ──
    _REVIEW_READY_CRITICAL_STAGES = frozenset({
        "MAPPER", "COMPILER", "VALIDATOR", "COMPARATOR",
    })

    def build(
        self,
        state: SparkPipelineState,
        cross_references: list[CrossReference] | None = None,
    ) -> SparkReviewPackage:
        """从 PipelineState 构建 SparkReviewPackage（含 REVIEW_READY 判定）。

        Args:
            state: Orchestrator 产出的 Pipeline 完整状态
            cross_references: SQL ↔ Spark 交叉引用列表（可选）

        Returns:
            SparkReviewPackage——含完整 provenance + 状态 + 修复建议 + REVIEW_READY 判定
        """
        # 构建 Provenance
        provenance = SparkProvenance(
            contract_hash=state.contract_hash,
            spark_plan_hash=state.spark_plan_hash,
            annotation_hash=state.annotation_hash,
            compiled_code_sha256=state.compiled_code_sha256,
            snapshot_id=state.snapshot_id,
            verification_report_id=state.verification_report_id,
        )

        # 生成确定性 package_id
        package_id = self._generate_package_id(provenance)

        # 收集修复信息
        repair_info: list[str] = []
        if state.errors:
            repair_info.extend(state.errors)

        # 从阶段结果中提取额外修复建议
        for stage_name, result in state.stage_results.items():
            if result == "FAILURE":
                repair_info.append(
                    f"阶段 {stage_name} 执行失败——可能需要检查对应组件。"
                )

        # 全局状态（使用枚举值字符串）
        overall_status = state.overall_status.value if hasattr(
            state.overall_status, "value"
        ) else str(state.overall_status)

        # ── Phase 9A5：阶段结果透传 ──
        stage_results = dict(state.stage_results)

        # ── Phase 9A5：对比器状态提取 ──
        comparator_status = ""
        if state.comparator_report is not None:
            comparator_status = (
                state.comparator_report.status.value
                if hasattr(state.comparator_report.status, "value")
                else str(state.comparator_report.status)
            )

        # ── Phase 9A5：REVIEW_READY 判定 ──
        review_ready = self._compute_review_ready(stage_results, comparator_status)

        return SparkReviewPackage(
            package_id=package_id,
            provenance=provenance,
            cross_references=list(cross_references or []),
            overall_status=overall_status,
            repair_info=repair_info,
            stage_results=stage_results,
            comparator_status=comparator_status,
            review_ready=review_ready,
        )

    def build_review_ready(
        self,
        state: SparkPipelineState,
        comparator_report: "PlanComparisonReport | None" = None,
        cross_references: list[CrossReference] | None = None,
    ) -> SparkReviewPackage:
        """显式 REVIEW_READY 装配——接受独立的 comparator_report 参数。

        当 comparator_report 不在 state 中时（如从外部传入），使用本方法。
        内部仍委托 build() 完成基础装配，再覆盖 comparator_status。

        Args:
            state: Orchestrator 产出的 Pipeline 完整状态
            comparator_report: 外部传入的对比报告（可选，优先于 state.comparator_report）
            cross_references: SQL ↔ Spark 交叉引用列表（可选）

        Returns:
            SparkReviewPackage——含完整 provenance + REVIEW_READY 判定
        """
        # 若传入外部 report，临时设置到 state 上供 build() 使用
        original_report = state.comparator_report
        if comparator_report is not None:
            # 使用 object.__setattr__ 绕过 Pydantic 的严格校验
            object.__setattr__(state, "comparator_report", comparator_report)

        try:
            pkg = self.build(state, cross_references=cross_references)
        finally:
            # 恢复原始 report——不污染调用方的 state
            object.__setattr__(state, "comparator_report", original_report)

        return pkg

    @classmethod
    def _compute_review_ready(
        cls,
        stage_results: dict[str, str],
        comparator_status: str,
    ) -> bool:
        """计算 REVIEW_READY 判定。

        判定规则（Phase 9A5）：
        1. MAPPER + COMPILER + VALIDATOR + COMPARATOR 均为 SUCCESS
        2. comparator_status 为 LOGIC_EQUIVALENT（若有对比报告）

        DEVELOPER 和 PHYSICAL_VERIFIER 不影响判定——它们可 SKIPPED。

        REVIEW_READY 的含义：
        "所有自动化检查已通过，材料完整，可进入人工代码审查"。
        不代表生产上线批准。

        Args:
            stage_results: 各阶段执行结果字典
            comparator_status: 对比器状态字符串（ComparisonStatus 值）

        Returns:
            True 表示所有关键阶段通过，可进入 REVIEW_READY
        """
        # 条件 1：所有关键阶段必须 SUCCESS
        all_critical_ok = all(
            stage_results.get(s, "NOT_EXECUTED") == "SUCCESS"
            for s in cls._REVIEW_READY_CRITICAL_STAGES
        )
        if not all_critical_ok:
            return False

        # 条件 2：对比器必须已执行且结果可接受
        # 空 comparator_status 表示对比器未运行——不允许绕过
        if not comparator_status:
            return False
        if comparator_status not in (
            "LOGIC_EQUIVALENT",
        ):
            return False

        return True

    @staticmethod
    def _generate_package_id(provenance: SparkProvenance) -> str:
        """生成确定性 package_id。

        基于 provenance 的核心 hash 字段——同一 provenance → 同一 package_id。

        Args:
            provenance: 完整溯源链

        Returns:
            "pkg_{hash前12位}" 格式的包 ID
        """
        payload = {
            "contract_hash": provenance.contract_hash,
            "spark_plan_hash": provenance.spark_plan_hash,
            "compiled_code_sha256": provenance.compiled_code_sha256,
        }
        content = json.dumps(payload, sort_keys=True, default=str)
        hash_hex = hashlib.sha256(content.encode()).hexdigest()[:12]
        return f"pkg_{hash_hex}"
