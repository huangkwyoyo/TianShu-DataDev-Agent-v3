"""Planning 模块——Join 推测证据评级与类型安全的 SqlBuildPlan IR。

Phase 1B 实现：
- IR 基础类型：ColumnRef / Literal / Predicate / AggregateSpec / SortSpec
- Join 推测：JoinCandidate / RelationshipEvidence / RelationshipHypothesis
- 证据评级：RelationshipValidator（确定性）
- Fake Planner：FakeRelationshipPlanner（仅处理显式 Join 声明）
- SqlBuildPlan：8 Step 类型 + SqlBuildPlanBuilder（确定性）
"""

# ── IR 基础类型 ──
from .models import (
    AggregateSpec,
    AliasExpr,
    ColumnRef,
    FrameBoundary,
    FrameBoundaryKind,
    JoinType,
    NullOrder,
    Predicate,
    PredicateOperator,
    SafeIdentifier,
    SortSpec,
    SqlLiteral,
    WhenBranch,
    WindowExpr,
    WindowFrame,
    WindowFrameType,
    WindowFunction,
)

# ── Join 推测模型 ──
from .relationship_hypothesis import (
    EvidenceAction,
    JoinCandidate,
    JoinEvidenceLevel,
    RelationshipEvidence,
    RelationshipHypothesis,
)

# ── Join 推测器（Fake + LLM） ──
from .relationship_planner import FakeRelationshipPlanner, RelationshipPlanner

# ── 证据评级器 ──
from .relationship_validator import RelationshipValidator

# ── SqlBuildPlan ──
from .sql_build_plan import (
    AggregateStep,
    CaseWhenStep,
    FilterStep,
    JoinStep,
    LimitStep,
    ProjectStep,
    ScanStep,
    SortStep,
    SqlBuildPlan,
    SqlBuildPlanBuilder,
    WindowStep,
)

# ── SqlProgram（Phase 3A） ──
from .sql_program import (
    SqlProgram,
    SqlProgramBuilder,
    SqlStatement,
    StatementKind,
    topological_sort,
    validate_program_dag,
)

# ── TempTableSpec（Phase 3A） ──
from .temp_table import (
    TempTableSpec,
    validate_consumer_is_declared,
    validate_temp_table_naming,
    validate_temp_table_refs,
)

# ── SpecEnricher（Phase 4D 指标推断） ──
from .spec_enricher import FakeSpecEnricher, SpecEnricher

# ── 交叉验证（Phase 4E 指标↔Join 一致性） ──
from .cross_validator import cross_validate

__all__ = [
    # IR 基础类型
    "AggregateSpec",
    "AliasExpr",
    "ColumnRef",
    "FrameBoundary",
    "FrameBoundaryKind",
    "JoinType",
    "SqlLiteral",
    "NullOrder",
    "Predicate",
    "PredicateOperator",
    "SafeIdentifier",
    "SortSpec",
    "WhenBranch",
    "WindowExpr",
    "WindowFrame",
    "WindowFrameType",
    "WindowFunction",
    # Join 推测
    "EvidenceAction",
    "JoinCandidate",
    "JoinEvidenceLevel",
    "RelationshipEvidence",
    "RelationshipHypothesis",
    # 证据评级
    "RelationshipValidator",
    # Join 推测器
    "FakeRelationshipPlanner",
    "RelationshipPlanner",
    # SqlBuildPlan
    "AggregateStep",
    "CaseWhenStep",
    "FilterStep",
    "JoinStep",
    "LimitStep",
    "ProjectStep",
    "ScanStep",
    "SortStep",
    "WindowStep",
    "SqlBuildPlan",
    "SqlBuildPlanBuilder",
    # SqlProgram（Phase 3A）
    "SqlProgram",
    "SqlProgramBuilder",
    "SqlStatement",
    "StatementKind",
    "topological_sort",
    "validate_program_dag",
    # TempTableSpec（Phase 3A）
    "TempTableSpec",
    "validate_consumer_is_declared",
    "validate_temp_table_naming",
    "validate_temp_table_refs",
    # SpecEnricher（Phase 4D 指标推断）
    "FakeSpecEnricher",
    "SpecEnricher",
    # 交叉验证（Phase 4E 指标↔Join 一致性）
    "cross_validate",
]
