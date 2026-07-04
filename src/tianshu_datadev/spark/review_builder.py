"""Phase 8 SparkReviewBuilder——从 PipelineState 构建 SparkReviewPackage。

复用各阶段产出的 hash 和报告，组装为统一交付物。
不读取 SQL 文本——所有引用均通过 ID。
"""

from __future__ import annotations

import hashlib
import json

from tianshu_datadev.spark.orchestrator import SparkPipelineState
from tianshu_datadev.spark.review_package import (
    CrossReference,
    SparkProvenance,
    SparkReviewPackage,
)


class SparkReviewBuilder:
    """构建 SparkReviewPackage——从 PipelineState 提取各阶段产出，组装统一交付物。

    使用方式：
        builder = SparkReviewBuilder()
        pkg = builder.build(pipeline_state)
    """

    def build(
        self,
        state: SparkPipelineState,
        cross_references: list[CrossReference] | None = None,
    ) -> SparkReviewPackage:
        """从 PipelineState 构建 SparkReviewPackage。

        Args:
            state: Orchestrator 产出的 Pipeline 完整状态
            cross_references: SQL ↔ Spark 交叉引用列表（可选）

        Returns:
            SparkReviewPackage——含完整 provenance + 状态 + 修复建议
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

        return SparkReviewPackage(
            package_id=package_id,
            provenance=provenance,
            cross_references=list(cross_references or []),
            overall_status=overall_status,
            repair_info=repair_info,
        )

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
