"""Phase 1B IR 基础类型——ColumnRef / SqlLiteral / Predicate / AggregateSpec 等。

所有类型均为 StrictModel（extra="forbid"），确保 LLM structured output 安全。

Phase 3B 安全加固：SafeIdentifier 类型——所有进入 SQL 渲染的标识符字段，
在 Pydantic Schema 层即拒绝非法字符（仅允许字母、数字、下划线），
防止 SQL 注入通过 alias / column_name / table_ref 等字段绕过 Validator。
"""

from __future__ import annotations

import re
from enum import Enum
from typing import Annotated

from pydantic import AfterValidator

from tianshu_datadev.developer_spec.models import (
    AggregationType,
    MetricFilterDecl,
    SortDirection,
    StrictModel,
)

# ════════════════════════════════════════════
# SQL 标识符安全约束——Schema 层第一道防线
# ════════════════════════════════════════════

# SQL 未加引号的标识符规范：字母或下划线开头 + 字母数字下划线
_SQL_ID_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_sql_identifier(v: str) -> str:
    """校验 SQL 标识符仅含字母、数字、下划线，且以字母或下划线开头。

    这是所有进入 SQL 渲染链路的 str 字段的统一安全门禁——
    任何包含空格、分号、引号、括号等特殊字符的标识符在 Schema 层即被拒绝。

    空字符串为合法值——表示"无别名"（如 CaseWhenStep 不产生输出列时）。

    Raises:
        ValueError: 标识符不符合 SQL 未加引号标识符规范
    """
    if v == "":
        return v  # 空字符串——表示无别名，合法
    if not _SQL_ID_RE.match(v):
        raise ValueError(
            f"非法 SQL 标识符: '{v}'——"
            f"必须匹配 {_SQL_ID_RE.pattern}（字母/下划线开头，仅含字母数字下划线）"
        )
    return v


# 安全标识符约束类型——替代裸 str 用于所有进入 SQL 渲染的标识符字段
SafeIdentifier = Annotated[str, AfterValidator(_validate_sql_identifier)]

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


class WindowFunction(str, Enum):
    """窗口函数白名单——Phase 3B 支持 9 种窗口函数。

    禁止不在白名单中的窗口函数名进入 IR。
    """

    ROW_NUMBER = "ROW_NUMBER"
    RANK = "RANK"
    DENSE_RANK = "DENSE_RANK"
    NTILE = "NTILE"
    LAG = "LAG"
    LEAD = "LEAD"
    SUM_OVER = "SUM_OVER"
    AVG_OVER = "AVG_OVER"
    COUNT_OVER = "COUNT_OVER"


class WindowFrameType(str, Enum):
    """窗口帧类型——ROWS 或 RANGE。"""

    ROWS = "ROWS"
    RANGE = "RANGE"


class FrameBoundaryKind(str, Enum):
    """窗口帧边界类型——CURRENT_ROW / UNBOUNDED / N 偏移。"""

    CURRENT_ROW = "CURRENT_ROW"
    UNBOUNDED_PRECEDING = "UNBOUNDED_PRECEDING"
    UNBOUNDED_FOLLOWING = "UNBOUNDED_FOLLOWING"
    N_PRECEDING = "N_PRECEDING"
    N_FOLLOWING = "N_FOLLOWING"


# ════════════════════════════════════════════
# 基础值对象
# ════════════════════════════════════════════


class ColumnRef(StrictModel):
    """列引用——(table_ref, column_name) 二元组 + 归一化字段名。

    table_ref 对应 SourceManifest 中注册的表引用标识。
    column_name 保留原始字段名用于 SQL 生成，normalized_name 用于 Join 匹配。

    所有标识符字段使用 SafeIdentifier 约束——非法字符在 Schema 层即被拒绝。
    """

    table_ref: SafeIdentifier
    column_name: SafeIdentifier
    normalized_name: SafeIdentifier


class SqlLiteral(StrictModel):
    """字面量值——SQL 中的常量表达式。

    支持 str / int / float / bool / None（NULL）。
    使用模型包装而非裸 Python 字面量，确保 Schema 约束和序列化一致。

    Phase 5 新增 is_sql_expr——当为 True 时 value 为可信 SQL 表达式片段，
    Compiler 直接渲染 value 而非加引号包裹。仅 Builder 确定性代码可设置。
    用于相对日期渲染（如 CURRENT_DATE - INTERVAL 30 DAY）。

    注意：命名为 SqlLiteral 而非 Literal，避免与 typing.Literal 冲突。
    """

    value: str | int | float | bool | None
    is_sql_expr: bool = False  # True 时 value 为可信 SQL 表达式——不加引号直接渲染


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
    """聚合规格——函数名 + 输入列 + 输出别名 + 可选过滤/去重/表达式。

    input_column 为 None 时表示 COUNT(*)。
    aggregation 使用 AggregationType 枚举——拒绝自由字符串进入 SQL 渲染。

    Phase 4D 新增：
    - filter: 条件聚合 FILTER (WHERE ...)
    - input_expression: 多字段表达式（如 "quantity * unit_price"）
    - distinct: SUM(DISTINCT col) 等去重聚合

    Phase 5 新增：
    - source_table: 源表别名——自引用/多表场景下消除列歧义（如 "emp_self_left.id"）
    """

    aggregation: AggregationType  # 封闭枚举：COUNT | SUM | AVG | MIN | MAX | COUNT_DISTINCT
    input_column: SafeIdentifier | None = None  # None 表示 COUNT(*)
    alias: SafeIdentifier  # 输出列别名
    # ── Phase 4D 新增字段 ──
    filter: MetricFilterDecl | None = None  # 条件聚合 FILTER (WHERE ...)
    input_expression: str | None = None  # 多字段表达式（如 "quantity * unit_price"）
    distinct: bool = False  # 去重聚合（用于 SUM(DISTINCT col)，COUNT_DISTINCT 已独立处理）
    # ── Phase 5 新增字段 ──
    source_table: str | None = None  # 源表别名——多表场景下消除列歧义


class SortSpec(StrictModel):
    """排序规格——排序列 + 方向 + NULL 排序策略。"""

    column: SafeIdentifier  # 列名（已归一化）——SafeIdentifier 防止 ORDER BY 注入
    direction: SortDirection = SortDirection.ASC
    null_order: NullOrder = NullOrder.LAST


class FrameBoundary(StrictModel):
    """窗口帧边界——描述窗口帧的起止位置。

    N_PRECEDING / N_FOLLOWING 时 offset 必填，其他情况为 None。
    """

    kind: FrameBoundaryKind
    offset: int | None = None


class WindowFrame(StrictModel):
    """窗口帧规格——ROWS 或 RANGE 模式的帧定义。

    仅 ROWS 和 RANGE 模式支持自定义 frame；
    ROW_NUMBER / RANK / DENSE_RANK / LAG / LEAD 等函数不需要 frame。
    """

    frame_type: WindowFrameType
    start: FrameBoundary
    end: FrameBoundary


class WindowExpr(StrictModel):
    """窗口函数表达式——结构化窗口函数调用。

    仅白名单中的 8 种函数可进入 IR。
    input 为 None 时表示无参数窗口函数（ROW_NUMBER / RANK / DENSE_RANK）。
    frame 为 None 时表示使用默认窗口帧（RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW）。
    """

    function: WindowFunction
    input: ColumnRef | SqlLiteral | None = None
    partition_by: list[ColumnRef] = []
    order_by: list[SortSpec] = []
    frame: WindowFrame | None = None
    alias: SafeIdentifier  # 输出列别名——SafeIdentifier 防止 AS 子句注入


class AliasExpr(StrictModel):
    """别名表达式——列引用或窗口表达式 + 输出别名。

    Phase 1B 仅支持 ColumnRef 表达式，
    Phase 3B 扩展支持 WindowExpr。
    """

    expression: ColumnRef | WindowExpr
    alias: SafeIdentifier  # 输出列别名——SafeIdentifier 防止 AS 子句注入


class WhenBranch(StrictModel):
    """CASE WHEN 分支——条件谓词 + 结果字面量。"""

    condition: Predicate
    result: SqlLiteral
