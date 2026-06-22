"""IR 数据结构与 Protocol 接口定义。

包含三层 IR 的 Protocol 接口和状态枚举。
Phase 1 将添加具体 dataclass 实现。
"""

from .protocols import (
    CrossValidationResult,
    ExecutionTrace,
    MergedResult,
    RepairDirective,
    RepairTarget,
    RequestStatus,
    RequirementIR,
    ResultSummary,
    SQLPlan,
    StepStatus,
    SubIntent,
)

__all__ = [
    "CrossValidationResult",
    "ExecutionTrace",
    "MergedResult",
    "RepairDirective",
    "RepairTarget",
    "RequestStatus",
    "RequirementIR",
    "ResultSummary",
    "SQLPlan",
    "StepStatus",
    "SubIntent",
]
