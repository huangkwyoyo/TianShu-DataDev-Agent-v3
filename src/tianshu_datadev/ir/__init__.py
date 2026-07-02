"""已废弃——此包仅保留 Phase 0 的旧 Protocol 兼容层。

所有 Protocol 已被严格 Pydantic 模型替代，请勿在生产代码中导入。
唯一合法使用方：tests/test_ir_protocols.py（架构演进追溯测试）。

生产代码应直接使用：
  - developer_spec.models（ParsedDeveloperSpec、SourceManifest 等）
  - planning.sql_build_plan（SqlBuildPlan、AggregateStep 等）
  - spark.models（DataTransformContractV1 等）
  - sql.models（ExecutionTrace、ResultSummary 等）
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
