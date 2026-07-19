"""Phase 1C 数据模型——Compiler 输出、Executor 追踪和 PerfContract。

所有运行时模型继承 StrictModel（extra="forbid"），枚举使用 (str, Enum)。
ExecutionTrace 和 ResultSummary 为严格 Pydantic 模型，替代旧 ir/protocols.py 中的 Protocol。
"""

from __future__ import annotations

import hashlib
from enum import Enum

from tianshu_datadev.developer_spec.models import StrictModel

# ════════════════════════════════════════════
# 执行状态枚举
# ════════════════════════════════════════════


class ExecutionStatus(str, Enum):
    """单次执行的状态——替代旧 StepStatus。"""

    NOT_EXECUTED = "NOT_EXECUTED"  # 步骤未执行
    RUNTIME_PASS = "RUNTIME_PASS"  # 在当前快照上运行成功
    RUNTIME_FAIL = "RUNTIME_FAIL"  # 执行失败（SQL 错误、超时等）
    TIMEOUT = "TIMEOUT"  # 执行超时（被 interrupt() 中断）
    RESULT_TOO_LARGE = "RESULT_TOO_LARGE"  # 结果行数超过 max_result_rows 上限


# ════════════════════════════════════════════
# Compiler 输出模型
# ════════════════════════════════════════════


class CompilerPassRecord(StrictModel):
    """单次 Compiler Pass 的记录——含优化前后 AST 片段。"""

    pass_name: str  # column_pruning | predicate_normalization | sort_elimination | constant_folding
    pass_version: str
    applied: bool  # 是否实际应用（false 表示 pass 未产生变更）
    changes_count: int  # 变更数量
    input_ast_snippet: str  # 优化前 AST 片段描述
    output_ast_snippet: str  # 优化后 AST 片段描述


class PredicateNormRecord(StrictModel):
    """谓词规范化记录——一条谓词从原始形式到规范化形式的变更。"""

    original: str  # 原始谓词描述
    normalized: str  # 规范化后描述
    rule: str  # 应用的规范化规则名


class ConstantFoldRecord(StrictModel):
    """常量折叠记录——一条常量表达式的折叠结果。"""

    original: str  # 原始表达式描述
    folded: str  # 折叠后描述
    rule: str  # 应用的折叠规则名


class OptimizedSQLPlan(StrictModel):
    """优化后的 SQL 计划——记录所有 Pass 的优化决策。

    input_plan_hash 和 output_plan_hash 用于追溯优化前后的 SqlBuildPlan。
    """

    input_plan_hash: str  # 优化前 SqlBuildPlan 的 hash
    output_plan_hash: str  # 优化后 SqlBuildPlan 的 hash（列裁剪等改变 step 结构后）
    applied_passes: list[CompilerPassRecord] = []  # 所有 Pass 记录（含未应用的）
    rejected_directives: list[str] = []  # 未应用的优化指令及理由
    column_pruning_removed: list[str] = []  # 被裁剪的列名
    predicate_normalizations: list[PredicateNormRecord] = []  # 谓词规范化明细
    eliminated_sorts: list[str] = []  # 被消除的无用排序
    constant_folds: list[ConstantFoldRecord] = []  # 常量折叠明细


class CompiledSql(StrictModel):
    """DuckDB SQL 编译产物——Compiler 的唯一输出。

    sql_sha256 基于 sql 内容 + compiler_version 计算，保证确定性。
    相同 SqlBuildPlan + 相同 compiler_version → 相同 sql + 相同 sql_sha256。
    """

    sql: str  # DuckDB SQL 文本
    sql_sha256: str  # SQL 文本的 SHA-256（含 compiler_version 参与 hash）
    optimized_plan: OptimizedSQLPlan  # 优化链路记录
    compiler_version: str  # 编译器版本
    input_plan_hash: str  # 输入 SqlBuildPlan 的 hash

    @staticmethod
    def compute_sql_hash(sql: str, compiler_version: str) -> str:
        """计算 SQL 文本的确定性 SHA-256。

        将 compiler_version 纳入 hash 输入，确保编译器版本变更时 hash 不同。
        """
        content = f"{compiler_version}:{sql}"
        return hashlib.sha256(content.encode()).hexdigest()


class SqlArtifact(StrictModel):
    """完整 SQL 编译产物——含溯源信息。

    一个 SqlArtifact 对应一个 Compiler 编译周期的全部输出。
    """

    artifact_id: str  # 产物唯一标识
    compiled_sql: CompiledSql  # 编译后的 SQL
    spec_hash: str  # 对应的 ParsedDeveloperSpec.spec_hash
    plan_id: str  # 对应的 SqlBuildPlan.plan_id
    hypothesis_id: str | None = None  # 对应的 RelationshipHypothesis.hypothesis_id（单表时为 None）

    @staticmethod
    def generate_artifact_id(plan_id: str, compiler_version: str) -> str:
        """基于 plan_id + compiler_version 的确定性 artifact ID。"""
        content = f"{plan_id}:{compiler_version}"
        hash_hex = hashlib.sha256(content.encode()).hexdigest()[:12]
        return f"artifact_{hash_hex}"


# ════════════════════════════════════════════
# Executor 输出模型（替代旧 Protocol）
# ════════════════════════════════════════════


class ExecutionTrace(StrictModel):
    """单次执行的完整追踪记录——替代旧 ExecutionTrace Protocol。

    记录 DuckDB 执行的全过程，用于问题定位和交叉验证。
    """

    trace_id: str  # 追踪唯一标识
    plan_id: str  # 关联的 SqlBuildPlan.plan_id
    engine: str  # 执行引擎："duckdb"
    generated_sql: str  # 实际执行的 SQL 文本
    status: ExecutionStatus  # 执行状态
    row_count: int  # 返回行数
    execution_time_ms: float  # 执行耗时（毫秒）
    error_message: str | None = None  # 执行失败时的错误信息

    @staticmethod
    def generate_trace_id(plan_id: str) -> str:
        """基于 plan_id 的确定性 trace ID。"""
        hash_hex = hashlib.sha256(plan_id.encode()).hexdigest()[:12]
        return f"trace_{hash_hex}"


class ResultSummary(StrictModel):
    """结构化执行结果摘要——替代旧 ResultSummary Protocol。

    用于交叉验证比对的标准化格式，将执行结果转换为统一格式后比较。
    """

    summary_id: str  # 摘要唯一标识
    trace_id: str  # 关联的 ExecutionTrace.trace_id
    engine: str  # 执行引擎
    columns: list[str]  # 输出列名
    column_types: list[str]  # 规范化后的列类型
    row_count: int  # 行数
    null_counts: dict[str, int]  # 逐列空值计数
    numeric_sums: dict[str, float]  # 数值列合计
    sample_rows: list[list] = []  # 前 N 行抽样数据（最多 20 行）

    @staticmethod
    def generate_summary_id(trace_id: str) -> str:
        """基于 trace_id 的确定性 summary ID。"""
        hash_hex = hashlib.sha256(trace_id.encode()).hexdigest()[:12]
        return f"summary_{hash_hex}"


# ════════════════════════════════════════════
# PerfContract 模型
# ════════════════════════════════════════════


class PerfRuleLevel(str, Enum):
    """性能规则级别——REJECT 阻断编译，WARN 记录不阻断。

    Phase 4B：保留此枚举以兼容旧代码，新代码使用 PerfSeverity。
    """

    REJECT = "REJECT"  # 硬门禁——违反后阻断编译
    WARN = "WARN"  # 软规则——记录到 ExecutionTrace，不阻断


class PerfSeverity(str, Enum):
    """性能规则严重级别——三分流：阻断 / 告警 / 反馈。

    Phase 4B 新增 PERF_FEEDBACK 级别——慢 SQL 执行计划反馈
    进入 artifact，不改变业务语义。
    """

    REJECT = "REJECT"  # 硬门禁——违反后阻断编译
    WARN = "WARN"  # 软规则——记录到 ExecutionTrace，不阻断
    PERF_FEEDBACK = "PERF_FEEDBACK"  # 执行计划反馈——不改变业务语义，进入 artifact


class PerfRule(StrictModel):
    """单条性能契约规则——含触发条件的结构化描述。

    Phase 4B 扩展至 15 条规则（PERF-001 ~ PERF-015），
    新增 check_category 分类字段。
    """

    rule_id: str  # PERF-001 ~ PERF-015
    description: str  # 规则中文描述
    severity: PerfSeverity = PerfSeverity.WARN  # 规则严重级别（默认 WARN）
    condition: str  # 触发条件的人类可读描述
    # 检查类别——column_selection / filtering / join
    # / aggregation / sorting / optimization / execution
    check_category: str = "general"


class PerfCheckResult(StrictModel):
    """单条 PERF 规则的检查结果——含具体被标记项。

    Phase 4B 新增——替代旧 PerfValidationResult 的逐规则结果。
    flagged_items 记录被标记的具体表名、列名等，便于下游定位问题。
    """

    rule_id: str  # PERF-001 ~ PERF-015
    passed: bool  # True = 未违反此规则
    severity: PerfSeverity  # 规则严重级别
    message: str  # 人类可读的验证消息
    flagged_items: list[str] = []  # 被标记的具体项（表名、列名等）


class PerfValidationResult(StrictModel):
    """性能验证聚合结果——Phase 4B 替代原有 tuple[bool, list] 返回值。

    聚合全部 15 条规则的检查结果，按严重级别分类：
    - reject_violations：硬门禁违规——必须修复才能编译
    - warnings：软规则违规——记录但不阻断
    - feedbacks：执行计划反馈——进入 artifact，不改变语义
    """

    plan_id: str  # 被验证的 SqlBuildPlan.plan_id
    all_reject_passed: bool  # 所有 REJECT 规则是否全部通过
    check_results: list[PerfCheckResult] = []  # 所有规则的检查结果（15 条）
    reject_violations: list[PerfCheckResult] = []  # REJECT 违规列表
    warnings: list[PerfCheckResult] = []  # WARN 违规列表
    feedbacks: list[PerfCheckResult] = []  # PERF_FEEDBACK 信息列表


# ════════════════════════════════════════════
# SOURCE_ANOMALY——数据源异常统一标记
# ════════════════════════════════════════════


class SourceAnomalyType(str, Enum):
    """数据源异常类型——统一使用 SOURCE_ 前缀。

    Phase 4B 规定：所有 SourceManifest / 快照异常统一输出为
    SOURCE_ANOMALY，禁止使用 CATALOG_ANOMALY 或其他命名。
    """

    SOURCE_MISSING_COLUMN = "SOURCE_MISSING_COLUMN"  # SourceManifest 声明列在快照中不存在
    SOURCE_TYPE_MISMATCH = "SOURCE_TYPE_MISMATCH"  # 列实际类型与 SourceManifest 声明不一致
    SOURCE_NULL_SURPRISE = "SOURCE_NULL_SURPRISE"  # 声明为非空的列发现大量 NULL
    SOURCE_PARTITION_GAP = "SOURCE_PARTITION_GAP"  # 快照分区不连续（日期缺失）
    SOURCE_ROW_COUNT_DRIFT = "SOURCE_ROW_COUNT_DRIFT"  # 实际行数与 SourceManifest 预估偏差过大
    SOURCE_VALUE_OUTLIER = "SOURCE_VALUE_OUTLIER"  # 列值分布异常（超出声明范围）


class SourceAnomaly(StrictModel):
    """SourceManifest / 快照数据源异常——统一使用 SOURCE_ANOMALY 标记。

    所有数据源层面的异常（表结构、列类型、数据质量、分区完整性）
    都通过此模型记录，可在审查包中引用。
    """

    anomaly_id: str  # 异常唯一标识
    anomaly_type: SourceAnomalyType  # 异常类型
    table_ref: str  # 涉及的表引用
    column_name: str | None = None  # 涉及的列（表级异常时为空）
    description: str  # 人类可读的异常描述
    detected_at: str = ""  # 检测时间（ISO 格式）
    snapshot_ref: str | None = None  # 快照引用
    expected_value: str | None = None  # SourceManifest 声明的期望值
    actual_value: str | None = None  # 实际观测值

    @staticmethod
    def generate_anomaly_id(table_ref: str, anomaly_type: str) -> str:
        """基于表引用和异常类型的确定性 anomaly ID。"""
        content = f"{table_ref}:{anomaly_type}"
        hash_hex = hashlib.sha256(content.encode()).hexdigest()[:12]
        return f"anomaly_{hash_hex}"


# ════════════════════════════════════════════
# EXPLAIN 反馈模型
# ════════════════════════════════════════════


class ExplainFeedback(StrictModel):
    """DuckDB EXPLAIN 执行计划的结构化反馈——不改变业务语义。

    Phase 4B 新增：PERF-015（慢 SQL 基于执行计划优化）的产物。
    flagged_operations 记录被标记的危险操作（全表扫描、笛卡尔积等），
    suggested_optimizations 提供建议的优化方向。
    LLM 不参与性能决策——ExplainFeedback 是确定性解析的产物。
    """

    plan_hash: str  # 关联的 SqlBuildPlan hash
    explain_output: str  # EXPLAIN / EXPLAIN ANALYZE 原始输出文本
    flagged_operations: list[str] = []  # 被标记的危险操作
    suggested_optimizations: list[str] = []  # 建议的优化方向
    parser_version: str = "1.0.0"  # 解析器版本


# ════════════════════════════════════════════
# 类型兼容性工具（复用 source_manifest 逻辑）
# ════════════════════════════════════════════

# 兼容类型组——同一组内的类型视为兼容
_TYPE_COMPAT_GROUPS: list[set[str]] = [
    {"int", "integer", "bigint", "smallint", "tinyint", "int64", "int32"},
    {"varchar", "text", "string", "char", "nvarchar"},
    {"float", "double", "real", "float64"},
    {"decimal", "numeric", "decimal(18,2)", "decimal(10,2)"},
    {"timestamp", "datetime", "timestamptz"},
    {"date"},
    {"boolean", "bool"},
]


def types_are_compatible(type_a: str, type_b: str) -> bool:
    """检查两个 SQL 类型是否兼容。

    规则：同一兼容组内的类型视为兼容（如 int ↔ bigint），
    不同组之间视为不兼容（如 int ↔ varchar）。

    此函数从 source_manifest.py 的 _types_compatible 抽取，
    供 Validator 和 PerfValidator 复用。
    """
    if type_a == type_b:
        return True

    # 未知类型保守处理——视为兼容，由人工裁决
    if not type_a or not type_b or type_a == "unknown" or type_b == "unknown":
        return True

    a_lower = type_a.lower().strip()
    b_lower = type_b.lower().strip()

    for group in _TYPE_COMPAT_GROUPS:
        a_in_group = any(g in a_lower or a_lower in g for g in group)
        b_in_group = any(g in b_lower or b_lower in g for g in group)
        if a_in_group and b_in_group:
            return True

    return False


# ════════════════════════════════════════════
# 字段存在性校验工具
# ════════════════════════════════════════════


def find_column_type(
    table_ref: str,
    column_name: str,
    normalized_name: str,
    columns: list,
) -> str | None:
    """在表的列列表中查找字段类型。

    按 exact match → normalized match 优先级匹配。
    需要从调用方传入列的 column_name/normalized_name 和数据来源的列列表。

    Args:
        table_ref: 表引用名（用于错误消息，此处不参与匹配）
        column_name: 原始字段名
        normalized_name: 归一化后的字段名
        columns: 列定义列表（ManifestColumn 或 ColumnDecl）

    Returns:
        匹配到的 data_type，未找到返回 None
    """
    # 先精确匹配原始名
    for col in columns:
        if col.column_name == column_name:
            return col.data_type
    # 再匹配归一化名
    for col in columns:
        if hasattr(col, "normalized_name") and col.normalized_name == normalized_name:
            return col.data_type
    return None


# ════════════════════════════════════════════
# Phase 3A 多语句编译/执行产物
# ════════════════════════════════════════════


class ProgramCompiledSql(StrictModel):
    """多语句编译产物——按拓扑序排列的 CompiledSql + cleanup 语句。

    编译 SqlProgram 的输出，每条 SqlStatement 对应一个 CompiledSql。
    cleanup_sql 包含所有 _temp 表的 DROP TABLE 语句。
    """

    program_id: str
    statements: list[CompiledSql]  # 按 statement_order 排列的编译产物
    cleanup_sql: list[str] = []  # DROP TABLE IF EXISTS _temp_* 语句列表
    statement_order: list[str] = []  # 对应的 statement_id 顺序


class SqlProgramArtifact(StrictModel):
    """SqlProgram 的完整编译产物——含溯源链。

    与 SqlArtifact（单语句）对应，绑定 spec_id + compiler_version 溯源。
    """

    artifact_id: str  # artifact_prog_{program_id[:8]}_{compiler_version}
    program_id: str
    compiled: ProgramCompiledSql
    spec_id: str
    compiler_version: str

    @staticmethod
    def generate_artifact_id(program_id: str, compiler_version: str) -> str:
        """基于 program_id 和 compiler_version 的确定性 artifact ID。"""
        import hashlib

        key = f"{program_id}:{compiler_version}"
        hash_hex = hashlib.sha256(key.encode()).hexdigest()[:12]
        return f"artifact_prog_{hash_hex}"


class StatementExecutionResult(StrictModel):
    """单个语句的执行结果——绑定 trace + summary。"""

    statement_id: str
    trace: ExecutionTrace
    summary: ResultSummary


class ProgramExecutionResult(StrictModel):
    """多语句 SqlProgram 的执行结果汇总。

    记录每个语句的执行状态、失败位置和 cleanup 结果。
    cleanup_status 为 "success" 表示所有 _temp 表成功清理，
    "partial_failure" 表示部分 DROP 失败。
    """

    program_id: str
    results: list[StatementExecutionResult] = []  # 按执行顺序排列
    completed_count: int = 0  # 成功执行的语句数
    failed_at: str | None = None  # 首个失败的 statement_id（全部成功时为空）
    cleanup_status: str = "success"  # "success" | "partial_failure"
    cleanup_error: str | None = None  # cleanup 阶段的错误信息（成功时为空）
