"""Phase 1B IR 基础类型——ColumnRef / SqlLiteral / Predicate / AggregateSpec 等。

所有类型均为 StrictModel（extra="forbid"），确保 LLM structured output 安全。
"""

from __future__ import annotations

from enum import Enum

from tianshu_datadev.developer_spec.models import SortDirection, StrictModel

# ════════════════════════════════════════════
# 枚举
# ════════════════════════════════════════════


class JoinType(str, Enum):
    """Join 类型枚举——比 developer_spec.JoinTypeEnum 多 CROSS。"""

    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    FULL = "FULL"
    CROSS = "CROSS"


class PredicateOperator(str, Enum):
    """谓词操作符——覆盖 SQL WHERE / HAVING / JOIN ON 中的常见操作。

    包含逻辑操作符（AND/OR/NOT）以支持 Predicate 嵌套。
    """

    EQ = "EQ"
    NEQ = "NEQ"
    GT = "GT"
    GTE = "GTE"
    LT = "LT"
    LTE = "LTE"
    IN = "IN"
    NOT_IN = "NOT_IN"
    BETWEEN = "BETWEEN"
    IS_NULL = "IS_NULL"
    IS_NOT_NULL = "IS_NOT_NULL"
    LIKE = "LIKE"
    AND = "AND"
    OR = "OR"
    NOT = "NOT"


class NullOrder(str, Enum):
    """NULL 排序策略——FIRST 或 LAST。"""

    FIRST = "FIRST"
    LAST = "LAST"


# ════════════════════════════════════════════
# 基础值对象
# ════════════════════════════════════════════


class ColumnRef(StrictModel):
    """列引用——(table_ref, column_name) 二元组 + 归一化字段名。

    table_ref 对应 SourceManifest 中注册的表引用标识。
    column_name 保留原始字段名用于 SQL 生成，normalized_name 用于 Join 匹配。
    """

    table_ref: str
    column_name: str
    normalized_name: str


class SqlLiteral(StrictModel):
    """字面量值——SQL 中的常量表达式。

    支持 str / int / float / bool / None（NULL）。
    使用模型包装而非裸 Python 字面量，确保 Schema 约束和序列化一致。

    注意：命名为 SqlLiteral 而非 Literal，避免与 typing.Literal 冲突。
    """

    value: str | int | float | bool | None


class Predicate(StrictModel):
    """谓词——左操作数 operator 右操作数。

    left 可以是 ColumnRef 或嵌套 Predicate（用于 AND/OR/NOT 组合）。
    右操作数可为 None（IS_NULL / IS_NOT_NULL 不需要右值）。
    IN 操作符时 right 为 list[SqlLiteral]。
    """

    left: ColumnRef | Predicate
    operator: PredicateOperator
    right: ColumnRef | Predicate | SqlLiteral | list[SqlLiteral] | None = None


class AggregateSpec(StrictModel):
    """聚合规格——函数名 + 输入列 + 输出别名。

    input_column 为 None 时表示 COUNT(*)。
    aggregation 使用 developer_spec 中已注册的 AggregationType 枚举值。
    """

    aggregation: str  # COUNT | SUM | AVG | MIN | MAX | COUNT_DISTINCT
    input_column: str | None = None  # None 表示 COUNT(*)
    alias: str


class SortSpec(StrictModel):
    """排序规格——排序列 + 方向 + NULL 排序策略。"""

    column: str  # 列名（已归一化）
    direction: SortDirection = SortDirection.ASC
    null_order: NullOrder = NullOrder.LAST


class AliasExpr(StrictModel):
    """别名表达式——列引用 + 输出别名。

    Phase 1B 仅支持 ColumnRef 表达式，Phase 3B 扩展支持 CaseWhenStep。
    """

    expression: ColumnRef
    alias: str


class WhenBranch(StrictModel):
    """CASE WHEN 分支——Phase 3B 占位。

    条件谓词 + 结果字面量。
    """

    condition: Predicate
    result: SqlLiteral
