"""Phase 1A 严格 Pydantic 模型定义。

所有运行时模型使用 extra="forbid"，拒绝未知字段。
枚举类型使用 (str, Enum) 保证 JSON 序列化兼容。

Phase 3B 安全加固：SafePhysicalTableName 类型——所有物理表名字段，
在 Pydantic Schema 层即拒绝非法字符（仅允许字母、数字、下划线和可选的 schema 前缀），
防止 SQL 注入通过 source_table / table_mapping 等字段绕过防线。
"""

from __future__ import annotations

from decimal import Decimal
import re
from enum import Enum
from typing import Annotated, Literal, Union

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, field_validator, model_validator

# ════════════════════════════════════════════
# 基础
# ════════════════════════════════════════════

class StrictModel(BaseModel):
    """所有运行时模型的基类——统一 extra="forbid"，拒绝未知字段。"""

    model_config = ConfigDict(extra="forbid", frozen=False)


# ════════════════════════════════════════════
# SQL 物理表名安全约束——Schema 层防线
# ════════════════════════════════════════════

# 物理表名 allowlist 正则——支持 Unicode 字母 + schema.table 限定名格式
# [^\W\d_] = Unicode 字母（任何书写系统），每个组件必须以字母开头
# 拒绝：数字开头、双点号、点号起止、纯下划线开头
# 注意：SQL 特殊字符（分号、引号、空白等）已被 _PHYSICAL_TABLE_NAME_FORBIDDEN 拦截
_PHYSICAL_TABLE_NAME_RE = re.compile(
    r"^[^\W\d][\w]*(?:\.[^\W\d][\w]*)*$", re.UNICODE
)
# [^\W\d] = 词字符且非数字 = Unicode 字母或下划线（允许 _private 等标识符）

# 物理表名中明确拒绝的字符集合——用于生成清晰的错误消息
_PHYSICAL_TABLE_NAME_FORBIDDEN = frozenset({
    ";", "'", '"', "`", " ", "\t", "\n", "\r",
    "(", ")", "[", "]", "{", "}", "<", ">",
    ",", "=", "*", "/", "\\", "?", "!", "@", "#", "$", "%", "^", "&",
    "+", "|", "~",
})


def _validate_physical_table_name(v: str) -> str:
    """校验物理表名的合法性——三层防线拒绝 SQL 注入 + 结构非法。

    三层防线设计：
    1. 禁止字符检查——逐一扫描，清晰报错（分号、引号、空白等）
    2. 结构合法性检查——拒绝点号起止、连续点号、数字开头
    3. allowlist 正则——确保每个组件以 Unicode 字母开头

    物理表名与 SafeIdentifier 的关键区别：
    - 支持 schema.table 限定名格式（点号分隔，每个组件必须是合法标识符）
    - 支持 Unicode 字母（中文、Cyrillic、Hangul 等）——适配中文数仓
    - 不允许数字开头——未加引号 SQL 标识符不允许

    这是 source_table / table_mapping / table_paths 的统一安全门禁。

    Raises:
        ValueError: 物理表名含非法字符或格式不正确
    """
    if not v:
        raise ValueError("物理表名不能为空字符串")

    # ── 第一道防线：禁止字符逐字检查（提供清晰的错误消息）──
    for ch in v:
        if ch in _PHYSICAL_TABLE_NAME_FORBIDDEN:
            raise ValueError(
                f"物理表名包含非法字符 {repr(ch)}：'{v}'——"
                f"仅允许字母、数字、下划线，可选 schema.table 限定名格式"
            )

    # ── 第二道防线：结构合法性检查 ──
    if v.startswith("."):
        raise ValueError(f"物理表名不能以点号开头：'{v}'")
    if v.endswith("."):
        raise ValueError(f"物理表名不能以点号结尾：'{v}'")
    if ".." in v:
        raise ValueError(f"物理表名不能包含连续点号：'{v}'")
    if v[0].isdigit():
        raise ValueError(
            f"物理表名不能以数字开头：'{v}'——"
            f"未加引号 SQL 标识符必须以字母或下划线开头"
        )

    # ── 第三道防线：allowlist 正则校验 ──
    if not _PHYSICAL_TABLE_NAME_RE.match(v):
        raise ValueError(
            f"物理表名格式不正确：'{v}'——"
            f"每个点号分隔的组件必须以字母开头，仅含字母、数字、下划线"
        )

    return v


# 安全物理表名约束类型——替代裸 str 用于 source_table / table_mapping 等字段
SafePhysicalTableName = Annotated[str, AfterValidator(_validate_physical_table_name)]


# ════════════════════════════════════════════
# SQL CSV 路径安全约束——Schema 层防线
# ════════════════════════════════════════════

# CSV 路径中明确拒绝的字符——SQL 字符串终结符和控制字符
# 允许 Windows 反斜杠 \（路径分隔符）和 Unix 正斜杠 /
_CSV_PATH_FORBIDDEN = frozenset({
    "'",      # SQL 字符串分隔符——可终结字符串字面量
    "\n",     # 换行符
    "\r",     # 回车符
    "\x00",   # 空字节
})


def _validate_csv_path_literal(v: str) -> str:
    """校验 CSV 路径不含 SQL 字符串终结符和控制字符。

    CSV 路径作为 SQL 字符串字面量拼入 read_csv_auto('...')——
    必须拒绝单引号（可终结字符串字面量并开启注入窗口）、
    换行符、回车符和空字节。

    注意：反斜杠（Windows 路径分隔符）是允许的，
    攻击载荷中的反斜杠本身不会终结 SQL 字符串。

    Raises:
        ValueError: CSV 路径含 SQL 字符串终结符或控制字符
    """
    if not v:
        raise ValueError("CSV 路径不能为空字符串")

    for ch in v:
        if ch in _CSV_PATH_FORBIDDEN:
            raise ValueError(
                f"CSV 路径包含非法字符 {repr(ch)}：'{v}'——"
                f"不允许单引号、换行符、回车符、空字节"
            )

    return v


# 安全 CSV 路径约束类型——替代裸 str 用于 table_paths 的 value
SafeCsvPathLiteral = Annotated[str, AfterValidator(_validate_csv_path_literal)]


def _render_sql_string_literal(value: str) -> str:
    """将任意字符串渲染为安全的 SQL 字符串字面量。

    使用 SQL 标准单引号转义规则：将字符串中的每个 ' 替换为 ''，
    并用单引号包裹结果。

    这是 SQL 渲染层纵深防线——即使 Schema 层校验被绕过，
    此转义仍可阻止通过单引号终结字符串字面量的注入攻击。

    Args:
        value: 原始字符串值

    Returns:
        已转义并用单引号包裹的 SQL 字符串字面量
    """
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


# ════════════════════════════════════════════
# 枚举
# ════════════════════════════════════════════

class AggregationType(str, Enum):
    """聚合函数枚举——仅支持已注册的 6 种聚合。"""

    COUNT = "COUNT"
    SUM = "SUM"
    AVG = "AVG"
    MIN = "MIN"
    MAX = "MAX"
    COUNT_DISTINCT = "COUNT_DISTINCT"


class JoinTypeEnum(str, Enum):
    """Join 类型枚举。"""

    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    FULL = "FULL"


class SortDirection(str, Enum):
    """排序方向。"""

    ASC = "ASC"
    DESC = "DESC"


class FieldSource(str, Enum):
    """字段信息来源——用于 SourceManifest 来源追踪。"""

    DEVELOPER_SPEC = "developer_spec"
    SCHEMA_REGISTRY = "schema_registry"
    SNAPSHOT_PROFILE = "snapshot_profile"


class ConflictType(str, Enum):
    """SourceConflict 分类。"""

    TYPE_MISMATCH = "TYPE_MISMATCH"
    ENUM_MISMATCH = "ENUM_MISMATCH"
    UNIQUENESS_MISMATCH = "UNIQUENESS_MISMATCH"
    MISSING_IN_REGISTRY = "MISSING_IN_REGISTRY"


class WarningSeverity(str, Enum):
    """ParseWarning 严重等级。"""

    LOW = "LOW"
    MEDIUM = "MEDIUM"


# ================================================
# v4-light 最终版: DatasetType + CompareOp + LabelPredicateNode discriminator 联合 AST
# ================================================

class DatasetType(str, Enum):
    """数据产品类型——决定验证策略和能力门禁，不驱动 Builder 代码路径分叉。"""
    DETAIL_TABLE = "detail_table"
    AGGREGATE_TABLE = "aggregate_table"
    LABEL_TABLE = "label_table"
    UNSPECIFIED = "unspecified"


class CompareOp(str, Enum):
    """比较操作符——封闭集合。"""
    EQ = "="
    NEQ = "!="
    GT = ">"
    GTE = ">="
    LT = "<"
    LTE = "<="


class LabelColumnRef(StrictModel):
    """列引用叶子——引用源表中已声明的字段。不可作 WHEN 根条件。"""
    node_type: Literal["COLUMN_REF"] = "COLUMN_REF"
    column_name: str


class LabelTypedLiteral(StrictModel):
    """类型化字面量——真实 Python 类型。不可作 WHEN 根条件。"""
    node_type: Literal["LITERAL"] = "LITERAL"
    value: str | Decimal | bool | None
    data_type: Literal["string", "number", "boolean", "null"]


class LabelCompare(StrictModel):
    """二元比较：left OP right。"""
    node_type: Literal["COMPARE"] = "COMPARE"
    left: str
    op: CompareOp
    right: LabelTypedLiteral


class LabelIsNull(StrictModel):
    """IS NULL 检查。"""
    node_type: Literal["IS_NULL"] = "IS_NULL"
    column: str


class LabelIsNotNull(StrictModel):
    """IS NOT NULL 检查。"""
    node_type: Literal["IS_NOT_NULL"] = "IS_NOT_NULL"
    column: str


class LabelAnd(StrictModel):
    """逻辑 AND——至少 2 个子节点。"""
    node_type: Literal["AND"] = "AND"
    children: list["LabelPredicateNode"]


class LabelOr(StrictModel):
    """逻辑 OR——至少 2 个子节点。"""
    node_type: Literal["OR"] = "OR"
    children: list["LabelPredicateNode"]


class LabelNot(StrictModel):
    """逻辑 NOT——单子节点。"""
    node_type: Literal["NOT"] = "NOT"
    child: "LabelPredicateNode"


# ── 完整 AST（8 子类 discriminator 联合）──
LabelPredicateNode = Annotated[
    Union[
        LabelAnd,
        LabelOr,
        LabelNot,
        LabelCompare,
        LabelIsNull,
        LabelIsNotNull,
        LabelColumnRef,
        LabelTypedLiteral,
    ],
    Field(discriminator="node_type"),
]

# ── 根条件类型（仅 6 子类——排除 COLUMN_REF 和 LITERAL）──
# LITERAL/COLUMN_REF 不可作为 WHEN 根条件——LLM 若输出则 Pydantic discriminator 拒绝
LabelPredicateCondition = Annotated[
    Union[
        LabelAnd,
        LabelOr,
        LabelNot,
        LabelCompare,
        LabelIsNull,
        LabelIsNotNull,
    ],
    Field(discriminator="node_type"),
]


# ================================================
# v4-light 最终版: LLM 输出 Schema——LLM 直接产出的结构化数据
# 原则：LLM 只输出规则、标签域和 evidence
#       proposal_id / source_spec_hash / extraction_time 由系统生成
#       LabelBranchProposalOutput.condition 使用 LabelPredicateCondition
#       （排除 LITERAL/COLUMN_REF 根条件）
# ================================================

class LabelDomainOutput(StrictModel):
    """LLM 从原文提取的标签值域——不含系统生成字段。"""
    values: list[str] = []
    source_evidence: str = ""
    is_exhaustive: bool = False
    completeness_evidence: str = ""


class LabelBranchProposalOutput(StrictModel):
    """LLM 输出的单条 WHEN-THEN 分支——condition 仅允许 6 种根条件类型。"""
    condition: LabelPredicateCondition  # ← LITERAL/COLUMN_REF 在 Schema 层拒绝
    then_label: str
    evidence: str = ""  # LLM 层可为空——系统包装时由 Extractor 校验非空


class LabelRuleProposalOutput(StrictModel):
    """LLM 输出的单条标签规则——不含 proposal_id/source_spec_hash。"""
    output_column: str
    branches: list[LabelBranchProposalOutput]
    else_value: str  # LLM 层必填——label_table v1 要求 ELSE
    label_domain: LabelDomainOutput | None = None


class LabelRuleProposalList(StrictModel):
    """LLM 输出的规则列表——顶层 Schema，注册到 _SCHEMA_PATH_MAP。"""
    rules: list[LabelRuleProposalOutput]


# ================================================
# 系统内部模型——由 LlmLabelExtractor 包装 LLM 输出后生成
# proposal_id / source_spec_hash / extraction_time 由系统注入
# else_value / label_domain / evidence 均为必需——缺失时 Promotion 拒绝
# ================================================

class LabelDomain(StrictModel):
    """系统包装的标签值域——含系统生成的 domain_id。"""
    domain_id: str = ""
    values: list[str] = []
    source_evidence: str = ""
    is_exhaustive: bool = False
    completeness_evidence: str = ""


class LabelBranchProposal(StrictModel):
    """系统包装的单条 WHEN-THEN 分支——evidence 必填非空。"""
    condition: LabelPredicateCondition  # ← LITERAL/COLUMN_REF 在 Schema 层拒绝
    then_label: str
    evidence: str  # 必填——Promotion 阶段检查非空


class LabelRuleProposal(StrictModel):
    """系统包装的标签规则候选——proposal_id/source_spec_hash 由系统生成。

    label_table v1 强制要求：
    - else_value: str（必填——ELSE 子句）
    - label_domain: LabelDomain（必填——标签值域）
    - 每个 branch.evidence 非空
    """
    proposal_id: str
    source_spec_hash: str
    output_column: str
    branches: list[LabelBranchProposal]
    else_value: str  # ← 必填 str（非 Optional）
    label_domain: LabelDomain  # ← 必填 LabelDomain（非 Optional）


class LabelPredicateBranch(StrictModel):
    """已验证的类型化 WHEN-THEN 分支——仅含确定性信息。"""
    condition: LabelPredicateCondition
    then_label: str


# ════════════════════════════════════════════
# 字段级声明模型
# ════════════════════════════════════════════

class ColumnDecl(StrictModel):
    """程序员声明的字段定义。

    data_type、enum_values、nullable、unique 均可为 None——允许程序员不声明
    （属于 Parser 允许宽松第 1 项），由 SourceManifest 从 SchemaRegistry 补充。
    """

    column_name: str
    normalized_name: str  # 由 FieldNormalizer 归一化后填入
    data_type: str | None = None
    enum_values: list[str] | None = None
    nullable: bool | None = None
    unique: bool | None = None
    description: str | None = None


class FilterDecl(StrictModel):
    """表级预过滤声明——只支持确定性的操作符，不支持自由 SQL 片段。"""

    column_ref: str
    # 允许的操作符
    operator: str  # "=" | "!=" | ">" | "<" | ">=" | "<=" | "IN" | "BETWEEN" | "IS_NULL" | "IS_NOT_NULL"
    value: str | int | float | list | None = None


class MetricFilterDecl(StrictModel):
    """指标的过滤条件——对应 SQL FILTER (WHERE ...) 子句。

    用于表达条件聚合，如"有标准罚款的车牌数"应生成：
    COUNT(DISTINCT plate_id) FILTER (WHERE fine_status = 'STANDARD')

    所有字段均受 Pydantic 严格校验，禁止自由 SQL 片段。
    """

    column: str  # 过滤列名（必须在 manifest 中存在，由 Validator 校验）
    # 允许的操作符——与 FilterDecl 保持一致
    operator: str  # "eq" | "neq" | "gt" | "gte" | "lt" | "lte" | "in" | "is_null" | "is_not_null"
    value: str  # 过滤值（统一为字符串，由编译器按列类型决定是否加引号）


class MetricVariant(StrictModel):
    """指标的过滤变体——同一基础聚合逻辑 + 不同 filter 条件。

    用于表达"同一指标在不同条件下的多个版本"，
    如 COUNT(DISTINCT user_id) 派生出：
    - total_users（无过滤）
    - active_users（FILTER WHERE status='active'）
    - paying_users（FILTER WHERE status='paying'）

    Builder 负责将 variants 展开为独立的 AggregateSpec 条目。
    """

    variant_name: str  # 变体名称（用于 human-readable 标识）
    filter: MetricFilterDecl | None = None  # 此变体的过滤条件（None 表示无过滤）
    alias: str  # 输出列别名（必须唯一）


class MetricDecl(StrictModel):
    """程序员声明的指标——支持简单聚合、条件聚合、去重聚合、表达式聚合。

    不可表达的场景（有独立 IR 承接）：
    - 窗口函数 → WindowStep
    - CASE WHEN 聚合 → CaseWhenStep
    - 比率计算 → InferredComputedMetric（聚合后计算）

    variants 字段支持同一基础指标的多条件变体——
    Builder 将基础指标 + 每个 variant 展开为独立的 AggregateSpec。
    """

    metric_name: str
    aggregation: AggregationType
    input_column: str | None = None  # COUNT(*) 时为 None
    alias: str
    # ── Phase 4D 新增字段 ──
    filter: MetricFilterDecl | None = None  # 条件聚合（如 FILTER WHERE status='STANDARD'）
    input_expression: str | None = None  # 多字段算术表达式，如 "quantity * unit_price"
    # input_expression 约束：仅允许列引用+算术运算符（+-*/%），禁止 SQL 关键字/函数调用/
    # 注释/引号；非空时经 expression_guard 校验
    distinct: bool = False  # 去重聚合（用于 SUM(DISTINCT col)）
    # ── Phase 5 新增字段 ──
    variants: list[MetricVariant] | None = None  # 多条件变体——同一基础聚合 + 不同 filter


class InferredWindowMetric(StrictModel):
    """SpecEnricher 推断的窗口指标——后续由 Builder 转为 WindowStep。

    不进 MetricDecl 的原因：窗口函数的语义与聚合函数完全不同，
    需要 PARTITION BY / ORDER BY / 窗口帧等额外信息，
    已有独立的 WindowStep IR 承接。
    """

    metric_name: str
    window_function: str  # ROW_NUMBER | RANK | DENSE_RANK | SUM | AVG | LAG | LEAD | NTILE
    input_column: str  # 窗口函数的输入列（必须在 manifest 中存在）
    partition_by: list[str] = []  # PARTITION BY 列名列表
    order_by: list[str] = []  # ORDER BY 列名列表（如 "amount DESC"）
    alias: str
    confidence: str = "medium"  # high | medium | low（LLM 推断置信度）


class InferredComputedMetric(StrictModel):
    """SpecEnricher 推断的计算指标——比率、百分位等聚合后计算。

    不进 MetricDecl 的原因：这是聚合后的标量运算（如 a / b），
    不是 SQL 聚合函数。需要在 SELECT 外层或子查询中计算。
    """

    metric_name: str
    expression: str  # 计算表达式（如 "fined_plate_count / unique_plate_count"）
    # expression 约束：仅允许列引用+算术运算符，禁止 SQL 关键字/注释/引号；经 expression_guard 校验
    depends_on: list[str] = []  # 依赖的其他指标 alias（必须先计算）
    alias: str
    confidence: str = "medium"  # high | medium | low


class EnrichedSpec(StrictModel):
    """SpecEnricher 产出——原始 spec + 推断补充。

    设计原则：
    - original_spec 原封不动保留，所有推断结果放在独立字段
    - 程序员手写的 metrics 优先级最高，不可被覆盖
    - 推断结果标记置信度，low 置信度由 Validator 生成 HumanResolution 问题
    """

    original_spec: ParsedDeveloperSpec
    # 推断的简单聚合指标（可直接合并到 original_spec.metrics）
    inferred_metrics: list[MetricDecl] = []
    # 推断的窗口指标（后续转为 WindowStep）
    inferred_window_metrics: list[InferredWindowMetric] = []
    # 推断的计算指标（聚合后表达式计算）
    inferred_computed_metrics: list[InferredComputedMetric] = []
    # 丰富化元数据
    enrichment_metadata: dict = {}  # 包含 source, confidence_summary, inference_time_ms 等


class DimensionDecl(StrictModel):
    """维度声明——列引用指向已声明的字段。"""

    dimension_name: str
    column_ref: str


class JoinDecl(StrictModel):
    """程序员显式声明的 Join 关系——可选，缺失时由 RelationshipHypothesis 推理。"""

    left_table: str
    right_table: str
    left_key: str
    right_key: str
    join_type: JoinTypeEnum


class TimeRangeDecl(StrictModel):
    """时间范围声明——可选，缺失时生成 W002 警告。

    Phase 5 新增业务日历支持：
    - calendar_type: 日历类型（默认日历年 / 财年7月起 / 财年4月起）
    - relative_range: 相对日期范围（"最近N天" / MTD / YTD）——与 start/end 互斥
    - fiscal_year: 财年编号（仅 calendar_type 非 "calendar" 时有效）
    """

    column_ref: str
    start: str = ""  # 固定起止日期（与 relative_range 互斥）
    end: str = ""
    inclusive: bool = True
    # ── Phase 5 新增字段 ──
    calendar_type: str = "calendar"  # "calendar" | "fiscal_jul" | "fiscal_apr"
    relative_range: str | None = None  # "last_7d" | "last_30d" | "last_90d" | "mtd" | "ytd"（互斥 start/end）
    fiscal_year: int | None = None  # 财年编号（如 2026），仅 fiscal_jul/fiscal_apr 时有效


class SortDecl(StrictModel):
    """输出排序声明——可选，缺失时不拒绝。"""

    column: str
    direction: SortDirection = SortDirection.ASC


# ════════════════════════════════════════════
# 表声明
# ════════════════════════════════════════════

class InputTableDecl(StrictModel):
    """源表声明——包含别名、物理表名、列、过滤、角色等全部声明信息。"""

    table_alias: str
    source_table: SafePhysicalTableName
    row_count: int | None = None  # 规范化后的数字（中文量级已转换）
    raw_row_count: str | None = None  # 原始字符串（如 "~5000万"），用于追溯
    role: str | None = None  # "fact" | "dim" | None
    description: str | None = None
    columns: list[ColumnDecl] = []
    filters: list[FilterDecl] = []
    partition_field: str | None = None
    time_field: str | None = None
    key_columns: list[ColumnDecl] = []  # 从 YAML key_columns 解析
    business_columns: list[ColumnDecl] = []  # 从 YAML business_columns 解析
    # ── V2 新增 ──
    unique_keys: list[list[str]] | None = None
    """开发者显式声明的唯一键集合。每个元素是一组列名，表示这组列在表中值唯一。
    示例：unique_keys: [["location_id"], ["zone_name", "borough"]]
    由 _parse_input_tables 从 YAML source_tables[*].unique_keys 解析。"""


class LegacyDescriptionDSLWarning(UserWarning):
    """旧格式 description DSL 兼容警告——提示迁移到结构化 hint 字段。"""


class OutputColumnDecl(StrictModel):
    """输出列声明——包含名称、类型和可选的 SQL 语义描述。

    结构化 hint 字段（推荐——Phase 7B 新增）：
    - metric_hint: 聚合指标（如 COUNT、SUM、AVG）
    - computed_hint: 计算指标（如比率 a / b）
    - window_hint: 窗口指标（如 ROW_NUMBER、RANK）
    三者互斥——每个输出列最多设置一个。

    description 字段承载旧格式 DSL（已废弃，保留兼容）：
    - 简单聚合: "COUNT(*)" / "SUM(amount)" / "COUNT(DISTINCT user_id)"
    - 计算指标: "fined_count / total_count，范围 [0, 1]"
    - 窗口函数: "ROW_NUMBER() OVER (PARTITION BY dt ORDER BY cnt DESC)"
    旧格式仍可机械解析（白名单模式），但会触发 LEGACY_DESCRIPTION_DSL 警告。
    建议迁移到结构化 hint 字段。

    user_description 字段：人类可读说明（不参与任何 SQL 语义解析）。
    """

    name: str  # 输出列名
    type: str = "varchar"  # 输出列类型（date / bigint / decimal / varchar）
    description: str | None = None  # 旧格式 description DSL（已废弃，保留兼容）
    metric_hint: MetricDecl | None = None  # 结构化聚合指标提示（推荐）
    computed_hint: InferredComputedMetric | None = None  # 结构化计算指标提示（推荐）
    window_hint: InferredWindowMetric | None = None  # 结构化窗口指标提示（推荐）
    user_description: str | None = None  # 人类可读说明（不参与 SQL 语义解析）

    @model_validator(mode="after")
    def _validate_hint_exclusivity(self):
        """互斥校验：metric_hint / computed_hint / window_hint 最多一个非空。"""
        hints = [
            self.metric_hint is not None,
            self.computed_hint is not None,
            self.window_hint is not None,
        ]
        if sum(hints) > 1:
            raise ValueError(
                "metric_hint / computed_hint / window_hint 互斥，只能设置其中一个"
            )
        return self


class OutputSpecDecl(StrictModel):
    """输出规格——声明输出列、粒度和可选排序/行限制。

    Phase 4D 升级：columns 从 list[str] → list[OutputColumnDecl]，
    支持每列附带 type 和 description，DescriptionParser 从中解析指标。
    向后兼容：YAML 中写字符串 "col_name" 自动转为 OutputColumnDecl(name="col_name")。
    """

    columns: list[OutputColumnDecl]  # 输出列列表（含 name/type/description）
    grain: list[str]  # 输出粒度
    sort: list[SortDecl] | None = None
    limit: int | None = None

    @field_validator("columns", mode="before")
    @classmethod
    def _coerce_columns(cls, v: list) -> list:
        """向后兼容：list[str] 自动转为 list[OutputColumnDecl] 的 dict 形式。"""
        result = []
        for item in v:
            if isinstance(item, str):
                result.append({"name": item})
            elif isinstance(item, dict):
                result.append(item)
            else:
                result.append(item)
        return result


# ════════════════════════════════════════════
# CASE WHEN 条件分支声明（Phase 6 Spec Schema 扩展）
# ════════════════════════════════════════════


class CaseWhenBranchDecl(StrictModel):
    """CASE WHEN 分支声明——合并步骤中单条 WHEN-THEN 规则。

    支持两种模式（根据字段存在性自动选择）：
    1. 字符串模式（when/then）——用于复杂布尔表达式，如 risk_label
       WHEN crash_per_million_trips >= 800 OR violation_per_thousand_trips >= 15 THEN '高风险'
       when 直接写入 SQL WHEN 子句——通过 Compiler SqlRawExpression 校验
    2. 类型化模式（condition_column/condition_operator/condition_value/result_column）——
       用于简单单列比较，如 WHEN status = 'VIP' THEN vip_amount
    """

    # ── 字符串模式（复杂布尔条件）──
    when: str | None = None  # 条件表达式字符串（如 "a >= 800 OR b >= 15"）
    then: str | None = None  # 结果标签字符串（如 "高风险"）

    # ── 类型化模式（简单单列比较）──
    condition_column: str | None = None  # 条件列名（必须在 _temp 表中存在）
    condition_operator: str | None = None  # "=" | "!=" | ">" | "<" | ">=" | "<=" | "IN"
    condition_value: str | None = None  # 条件值（如 "VIP"）
    result_column: str | None = None  # THEN 取值——引用上游步骤的指标 alias


class CaseWhenDecl(StrictModel):
    """CASE WHEN 声明——合并步骤的条件选择逻辑。

    branches 按顺序求值（短路语义）——首个匹配的 WHEN 返回其 THEN 值。
    else_value 为 None 时无 ELSE 子句（不匹配任何条件时为 NULL）。
    """

    branches: list[CaseWhenBranchDecl] = []  # WHEN-THEN 分支列表（至少 1 个）
    else_value: str | None = None  # ELSE 默认值（None 表示无 ELSE）
    output_column: str = ""  # 输出列别名（对应 output_spec.columns 中的列名）
    # ── v4-light 最终版: 类型化的标签分支（Label Extractor 填充）──
    typed_branches: list[LabelPredicateBranch] = []  # 类型化 WHEN-THEN 分支


# ════════════════════════════════════════════
# 分步计算声明（Phase 5+ Spec Schema 扩展）
# ════════════════════════════════════════════


class ComputeStepExpression(StrictModel):
    """compute_step 中的派生表达式——如 crash_per_million_trips = total_crashes * 1e6 / total_trip_count。

    表达式字符串通过 Compiler SqlRawExpression 安全校验后直接渲染到 SELECT 子句，
    避免在 Builder 中构建表达式 AST。
    """

    name: str  # 输出列名（如 crash_per_million_trips）
    expression: str  # 算术表达式字符串（如 "total_crashes * 1000000.0 / NULLIF(total_trip_count, 0)"）
    type: str = "double"  # 输出列类型


class ComputeStep(StrictModel):
    """一次分步计算声明——单步聚合逻辑 + 输入来源 + 输出别名。

    多个 ComputeStep 构成有向无环图（DAG）。每个步骤等价于：
    SELECT group_by_cols, agg_func(input_column) AS alias
    FROM <source> GROUP BY group_by_cols

    source 取值：
    - "input"：从源表计算
    - "step_name"（字符串）：从前面步骤的 _temp 表读取（线性链）
    - ["step_a", "step_b"]（列表）：从多个上游步骤的 _temp 表读取（分支合流），
      Builder 生成 Scan+Join+Aggregate 合并 Plan，Join 键从 spec.joins 中查找

    step_name 在同一个 Spec 内必须唯一。
    """

    step_name: str  # 步骤唯一名称（Spec 内不重复）
    source: str | list[str] = "input"  # "input" / step_name / [step_a, step_b]——数据来源
    group_by: list[str] = []  # GROUP BY 列名列表
    metrics: list[MetricDecl] = []  # 此步骤的聚合指标（复用已有模型）
    # ── Phase 6/7 新增字段 ──
    expressions: list[ComputeStepExpression] = []  # 派生表达式——用于 compute_ratios 等比率计算步骤
    output_alias: str = ""  # 产出别名——Builder 据此命名 _temp 表（如 "_temp_<alias>"）
    # ── Phase 6 新增字段 ──
    case_when: CaseWhenDecl | None = None  # 合并步骤的 CASE WHEN 逻辑——仅合流步骤（source 为列表）有效
    joins: list[JoinDecl] | None = None  # 源表间的 Join 声明——当 source="input" 且需多表 Join 时使用

    @field_validator("source", mode="before")
    @classmethod
    def _normalize_source(cls, v: str | list[str]) -> str | list[str]:
        """归一化 source 字段——单元素列表自动展开为字符串。

        空列表被拒绝（必须有数据来源），单元素列表等价于标量字符串。
        """
        if isinstance(v, list):
            if len(v) == 0:
                raise ValueError("compute_step source 列表不能为空——必须至少有 'input' 或一个 step_name")
            if len(v) == 1:
                return v[0]  # 单元素列表归一化为字符串
            # 多元素列表——每个元素必须是字符串
            for item in v:
                if not isinstance(item, str):
                    raise ValueError(f"compute_step source 列表中每个元素必须是字符串，收到: {type(item)}")
            # 检查列表内无重复
            if len(set(v)) != len(v):
                raise ValueError(f"compute_step source 列表中存在重复引用: {v}")
            return v
        return v


# ════════════════════════════════════════════
# 开放问题与警告
# ════════════════════════════════════════════

class HumanResolution(StrictModel):
    """程序员对 OpenQuestion 的人工裁决。"""

    resolved_by: str  # 裁决人标识
    resolved_at: str  # ISO 时间戳
    answer: str  # 裁决内容
    confidence: str = "confirmed"  # "confirmed" | "estimated"


class OpenQuestion(StrictModel):
    """Parser 或 SourceManifest 无法确定的问题。

    blocking=True 时阻断后续流程（Phase 1A 中 SOURCE_CONFLICT 必须 blocking）。
    """

    question_id: str
    source: str  # "parser" | "source_manifest" | "relationship"
    field_ref: str | None = None
    description: str
    blocking: bool = False
    resolution: HumanResolution | None = None


class ParseWarning(StrictModel):
    """非阻断解析警告。"""

    warning_id: str
    field_ref: str | None = None
    message: str
    severity: WarningSeverity = WarningSeverity.LOW


# ════════════════════════════════════════════
# SourceManifest 模型
# ════════════════════════════════════════════

class SourceConflict(StrictModel):
    """DeveloperSpec 与 SchemaRegistry 之间的字段级冲突。"""

    field_ref: str
    table_ref: str
    developer_spec_value: str
    schema_registry_value: str
    conflict_type: ConflictType


class ManifestAnomaly(StrictModel):
    """快照采样或注册表查询中发现的异常——非冲突，但值得记录。"""

    anomaly_id: str
    table_ref: str
    column_ref: str | None = None
    description: str
    # 取值: MISSING_IN_REGISTRY | TYPE_CONFLICT | UNEXPECTED_NULL | TABLE_NOT_FOUND
    anomaly_type: str


class ForeignKeyRef(StrictModel):
    """外键引用。"""

    column: str
    ref_table: str
    ref_column: str


class ManifestColumn(StrictModel):
    """SourceManifest 中的字段条目——含来源标记。"""

    column_name: str
    normalized_name: str
    data_type: str
    nullable: bool = False
    unique: bool | None = None
    enum_values: list[str] | None = None
    source: FieldSource = FieldSource.DEVELOPER_SPEC


class ManifestTable(StrictModel):
    """SourceManifest 中的表条目。"""

    table_ref: str
    source_table: SafePhysicalTableName
    columns: list[ManifestColumn] = []
    primary_key: list[str] | None = None
    foreign_keys: list[ForeignKeyRef] | None = None
    unique_keys: list[list[str]] | None = None
    """已知唯一键集合——每个元素是一组列名的列表，表示这组列在表中值唯一。

    primary_key 自动视为唯一键之一，构建时由 SourceManifestBuilder 同步写入。
    SchemaRegistry 可补充额外的唯一索引信息。
    用于 LEFT JOIN 右表唯一性安全门禁——无覆盖 join key 的唯一键时阻断。
    """
    # ── V2 新增 ──
    role: str | None = None
    """表角色——"fact" | "dim" | None。从 InputTableDecl.role 透传。
    用于 LEFT JOIN 安全门禁生成更精准的阻断提示。"""
    key_column_names_normalized: list[str] = Field(default_factory=list)
    """key_columns 的归一化列名集合。由 SourceManifestBuilder 从
    InputTableDecl.key_columns 提取，经 FieldNormalizer.normalize() 处理。
    用于 LEFT JOIN 安全门禁判断 join key 是否属于维度键。"""
    estimated_row_count: int | None = None


# ════════════════════════════════════════════
# 顶层模型
# ════════════════════════════════════════════

class ParsedDeveloperSpec(StrictModel):
    """确定性解析后的 DeveloperSpec——Phase 1A 核心输出。

    open_questions 中 blocking=True 的条目阻断后续流程。
    parse_warnings 记录非阻断警告，供人工审查。
    """

    spec_id: str
    spec_hash: str  # normalized_spec_hash
    title: str
    description: str  # YAML summary + Markdown 正文
    input_tables: list[InputTableDecl]
    metrics: list[MetricDecl]
    dimensions: list[DimensionDecl]
    joins: list[JoinDecl] | None = None
    time_range: TimeRangeDecl | None = None
    output_spec: OutputSpecDecl
    compute_steps: list[ComputeStep] | None = None  # 分步计算声明——None 时走原路径
    inferred_window_metrics: list[InferredWindowMetric] = []  # SpecEnricher 推断的窗口指标
    # ── v4-light 最终版: 标签表支持 ──
    dataset_type: DatasetType = DatasetType.UNSPECIFIED  # 数据产品类型——Label Extractor 推断
    label_rules: list[LabelRuleProposal] = []  # 标签规则候选——仅 LABEL_TABLE 时非空
    open_questions: list[OpenQuestion] = []
    parse_warnings: list[ParseWarning] = []


class SourceManifest(StrictModel):
    """表字段事实源追踪——Phase 1A 核心输出。

    每个字段标记来源：developer_spec | schema_registry | snapshot_profile。
    conflicts 记录 developer_spec 与 SchemaRegistry 的冲突条目。
    """

    manifest_id: str
    spec_hash: str
    tables: list[ManifestTable] = []
    conflicts: list[SourceConflict] = []
    anomalies: list[ManifestAnomaly] = []
