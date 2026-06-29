"""PySpark 受控 DSL 生成——Phase 5-7。

Phase 5（当前）：SparkPlan IR 模型 + DataTransformContractV1 → SparkPlan 确定性映射 + PlanEquivalence 规则
Phase 6（后续）：SparkDeveloper——SparkPlan → 受控 PySpark DSL 代码
Phase 7（后续）：SQL/Spark 双链验证——PlanComparator + ResultComparator
"""

from tianshu_datadev.spark.models import (
    ContractGap,
    SparkAggFunction,
    SparkAggregateSpec,
    SparkAggregateStep,
    SparkCaseWhenBranch,
    SparkCaseWhenStep,
    SparkFilterStep,
    SparkJoinStep,
    SparkJoinType,
    SparkLimitStep,
    SparkPlan,
    SparkPlanMappingResult,
    SparkProjectColumn,
    SparkProjectStep,
    SparkReadStep,
    SparkSortDirection,
    SparkSortSpec,
    SparkSortStep,
    SparkStepType,
    SparkWindowExpr,
    SparkWindowFunction,
    SparkWindowStep,
    UnsupportedPattern,
)

__all__ = [
    "SparkPlan",
    "SparkStepType",
    "SparkJoinType",
    "SparkSortDirection",
    "SparkAggFunction",
    "SparkWindowFunction",
    "SparkReadStep",
    "SparkFilterStep",
    "SparkJoinStep",
    "SparkAggregateStep",
    "SparkAggregateSpec",
    "SparkProjectStep",
    "SparkProjectColumn",
    "SparkCaseWhenStep",
    "SparkCaseWhenBranch",
    "SparkWindowStep",
    "SparkWindowExpr",
    "SparkSortStep",
    "SparkSortSpec",
    "SparkLimitStep",
    "SparkPlanMappingResult",
    "UnsupportedPattern",
    "ContractGap",
]
