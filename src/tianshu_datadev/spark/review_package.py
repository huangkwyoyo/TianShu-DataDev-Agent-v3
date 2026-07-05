"""Phase 8 Review Package——统一交付物模型。

包含：
- CrossReference：SQL artifact ID → Spark step ID 映射（不含 SQL 文本）
- SparkProvenance：完整 hash 溯源链
- SparkReviewPackage：统一交付物——组合以上所有信息
"""

from __future__ import annotations

from pydantic import Field

from tianshu_datadev.developer_spec.models import StrictModel

# ════════════════════════════════════════════
# CrossReference——SQL ↔ Spark 映射
# ════════════════════════════════════════════


class CrossReference(StrictModel):
    """SQL artifact ↔ Spark step ID 交叉引用——不含 SQL 文本。

    使用 sql_artifact_id / sql_step_id 引用 SQL 侧，不嵌入 SQL 文本。
    spark_step_id 引用编译器生成的 step_id。
    """

    sql_artifact_id: str          # SQL 侧的 artifact ID（如 SqlBuildPlan.plan_id）
    sql_step_id: str              # SQL 侧的 step ID（如 "step_filter_0"）
    spark_step_id: str            # Spark 侧的 step ID（如 "SparkFilterStep_0"）


# ════════════════════════════════════════════
# SparkProvenance——完整溯源链
# ════════════════════════════════════════════


class SparkProvenance(StrictModel):
    """Spark 路径完整 hash 溯源链——从 Contract 到物理验证的确定性追溯。

    链顺序：contract_hash → spark_plan_hash → annotation_hash →
            compiled_code_sha256 → snapshot_id → verification_report_id

    所有 hash 由各阶段组件确定性生成——同一输入始终产出同一 hash。
    """

    contract_hash: str                                       # DataTransformContractV1 hash
    spark_plan_hash: str                                     # SparkPlan.compute_plan_hash()
    annotation_hash: str = ""                                # compute_annotation_hash()
    compiled_code_sha256: str                                # SparkCompileResult.raw_hash
    snapshot_id: str = ""                                    # SnapshotManifest.snapshot_id
    verification_report_id: str = ""                         # UnifiedVerificationReport.report_id


# ════════════════════════════════════════════
# SparkReviewPackage——统一交付物
# ════════════════════════════════════════════


class SparkReviewPackage(StrictModel):
    """Phase 8 统一交付物——组合 provenance + cross-references + 验证结论。

    不含 SQL 文本——所有引用均通过 ID。

    Phase 9A5 新增 REVIEW_READY 相关字段：
    - stage_results：各阶段执行结果（来自 SparkPipelineState）
    - comparator_status：对比器状态摘要（来自 PlanComparisonReport）
    - review_ready：REVIEW_READY 判定——所有关键阶段通过 + 逻辑等价
    """

    package_id: str                                          # 确定性包 ID
    provenance: SparkProvenance                              # 完整溯源链
    cross_references: list[CrossReference] = Field(          # SQL ↔ Spark 映射
        default_factory=list,
    )
    overall_status: str = ""                                 # 全局状态（PipelineStatus 值）
    repair_info: list[str] = Field(                          # 修复建议（如有）
        default_factory=list,
    )
    # ── Phase 9A5 新增：REVIEW_READY 终验收字段 ──
    stage_results: dict[str, str] = Field(                   # 各阶段执行结果
        default_factory=dict,
        description="来自 SparkPipelineState.stage_results——MAPPER/DEVELOPER/COMPILER/"
                    "VALIDATOR/COMPARATOR/PHYSICAL_VERIFIER 的执行结果",
    )
    comparator_status: str = ""                              # 对比器状态（ComparisonStatus 值）
    review_ready: bool = False                               # REVIEW_READY 判定——
                                                              # 所有关键阶段 SUCCESS + 逻辑等价
