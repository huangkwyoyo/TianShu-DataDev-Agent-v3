"""IR 数据结构与 Protocol 接口定义。

包含三层 IR 的 Protocol 接口和状态枚举。
Phase 1 将添加具体 dataclass 实现。

注意：ExecutionTrace 和 ResultSummary 已迁移至 tianshu_datadev.sql.models
（严格 Pydantic 模型，替代旧 Protocol）。
"""

from .protocols import (
    CrossValidationResult,
    MergedResult,
    RepairDirective,
    RepairTarget,
    RequestStatus,
    RequirementIR,
    SparkCodeArtifact,
    SQLPlan,
    StepStatus,
    SubIntent,
    TransformationContract,
    TransformParams,
)

__all__ = [
    "CrossValidationResult",
    "MergedResult",
    "RepairDirective",
    "RepairTarget",
    "RequestStatus",
    "RequirementIR",
    "SparkCodeArtifact",
    "SQLPlan",
    "StepStatus",
    "SubIntent",
    "TransformationContract",
    "TransformParams",
]
