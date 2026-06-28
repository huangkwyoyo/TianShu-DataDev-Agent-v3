"""SQL 确定性编译器和执行器——Phase 1C。

SqlBuildPlan → Validator → PerfValidator → Compiler Passes
→ Compiler → CompiledSql → Executor → ExecutionTrace + ResultSummary。

LLM 不直接生成 SQL 字符串——SQL 只能由 Python 确定性编译器生成。
Executor 只接受 Compiler 产物，拒绝外部 SQL 字符串。
"""

# ── 数据模型 ──
# ── Compiler ──
from .compiler import DuckDbSqlCompiler

# ── CompilerBackend（Phase 3C）──
from .compiler_backend import CompilerBackend, DuckDBBackend

# ── Compiler Passes ──
from .compiler_passes import (
    column_pruning,
    constant_folding,
    predicate_normalization,
    sort_elimination,
)

# ── Executor ──
from .executor import DuckDBExecutor
from .models import (
    CompiledSql,
    CompilerPassRecord,
    ConstantFoldRecord,
    ExecutionStatus,
    ExecutionTrace,
    OptimizedSQLPlan,
    PerfRule,
    PerfRuleLevel,
    PerfValidationResult,
    PredicateNormRecord,
    ProgramCompiledSql,
    ProgramExecutionResult,
    ResultSummary,
    SqlArtifact,
    SqlProgramArtifact,
    StatementExecutionResult,
    types_are_compatible,
)

# ── PerfValidator ──
from .perf_validator import PerfValidator

# ── Validator ──
from .validator import SqlBuildPlanValidator

# ── Write Plan（Phase 3C）──
from .write_plan import (
    FinalWritePlan,
    PartitionOverwriteSpec,
    TempTableStatement,
    WriteValidationCheck,
    validate_partition_format,
)
from .write_plan_builder import FinalWritePlanBuilder
from .write_validator import WriteValidator

__all__ = [
    # 数据模型
    "CompiledSql",
    "CompilerPassRecord",
    "ConstantFoldRecord",
    "ExecutionStatus",
    "ExecutionTrace",
    "OptimizedSQLPlan",
    "PerfRule",
    "PerfRuleLevel",
    "PerfValidationResult",
    "PredicateNormRecord",
    "ProgramCompiledSql",
    "ProgramExecutionResult",
    "ResultSummary",
    "SqlArtifact",
    "SqlProgramArtifact",
    "StatementExecutionResult",
    "types_are_compatible",
    # Validator
    "SqlBuildPlanValidator",
    # PerfValidator
    "PerfValidator",
    # Compiler Passes
    "column_pruning",
    "constant_folding",
    "predicate_normalization",
    "sort_elimination",
    # Compiler
    "DuckDbSqlCompiler",
    # CompilerBackend（Phase 3C）
    "CompilerBackend",
    "DuckDBBackend",
    # Executor
    "DuckDBExecutor",
    # Write Plan（Phase 3C）
    "FinalWritePlan",
    "FinalWritePlanBuilder",
    "PartitionOverwriteSpec",
    "TempTableStatement",
    "WriteValidationCheck",
    "WriteValidator",
    "validate_partition_format",
]
