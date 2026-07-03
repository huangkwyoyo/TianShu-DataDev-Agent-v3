"""Phase 6 SparkCodeRenderer——PySpark DSL 安全渲染器。

所有代码片段必须通过本渲染器生成，禁止直接 f-string 拼接。
渲染参数全部来自封闭模型字段或白名单枚举。
"""

from __future__ import annotations

import re

from tianshu_datadev.spark.models import (
    SparkAggFunction,
    SparkJoinType,
    SparkSortDirection,
    SparkWindowFunction,
)

# ════════════════════════════════════════════
# 常量
# ════════════════════════════════════════════

# 安全标识符——字母开头，字母数字下划线
_SAFE_IDENTIFIER = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

# Python 操作符映射——来自 FilterStep.operator → Python 比较/逻辑操作符
_OPERATOR_MAP: dict[str, str] = {
    "GT": ">",
    "GTE": ">=",
    "LT": "<",
    "LTE": "<=",
    "EQ": "==",
    "NEQ": "!=",
    "AND": "&",
    "OR": "|",
    "NOT": "~",
}

# 不需要右侧操作数的操作符
_UNARY_OPERATORS: set[str] = {"IS_NULL", "IS_NOT_NULL"}


class RenderError(Exception):
    """渲染器安全错误——输入未通过白名单校验时抛出。"""

    pass


# ════════════════════════════════════════════
# SparkCodeRenderer
# ════════════════════════════════════════════


class SparkCodeRenderer:
    """PySpark DSL 安全渲染器——所有代码片段必须通过本渲染器生成。

    规则：
    1. 变量名——必须匹配 SafeIdentifier 正则（字母开头，字母数字下划线）
    2. 列名——来自封闭模型字段，经反引号转义
    3. 字面量——按类型渲染（str → 单引号包围并转义，int/float → 直接渲染）
    4. 函数名——来自 SparkAggFunction / SparkWindowFunction 枚举
    5. Join how——来自 SparkJoinType 枚举
    6. Sort direction——来自 SparkSortDirection 枚举
    7. 禁止直接拼接表达式字符串
    """

    # ── 标识符校验 ──

    @staticmethod
    def validate_identifier(name: str, context: str = "identifier") -> str:
        """校验标识符是否安全，不安全则抛出 RenderError。

        Args:
            name: 待校验的标识符
            context: 上下文描述（用于错误信息）

        Returns:
            校验通过的标识符（原样返回）

        Raises:
            RenderError: 标识符不匹配安全正则
        """
        if not _SAFE_IDENTIFIER.match(name):
            raise RenderError(
                f"非法标识符 '{name}'（{context}）："
                f"必须匹配 {_SAFE_IDENTIFIER.pattern}"
            )
        return name

    # ── 列名渲染 ──

    @staticmethod
    def render_column(column_name: str) -> str:
        """渲染列引用——F.col("column_name")。

        列名可以是纯列名或表前缀格式（如 "user_id" 或 "od.user_id"）。
        点号分隔的格式用于消除歧义，但每段必须通过安全标识符校验。
        不允许引号、分号等注入字符。

        Args:
            column_name: 列名

        Returns:
            'F.col("column_name")' 形式的字符串
        """
        # 校验每段是否为安全标识符（点号分隔时逐段检查）
        parts = column_name.split(".")
        for part in parts:
            SparkCodeRenderer.validate_identifier(part, f"column_name 段 '{part}'")
        # 防止引号注入
        if '"' in column_name or "'" in column_name or ";" in column_name:
            raise RenderError(f"列名含非法字符：'{column_name}'")
        return f'F.col("{column_name}")'

    # ── 字面量渲染 ──

    @staticmethod
    def render_literal(value: str | int | float | bool) -> str:
        """按类型渲染 Python 字面量。

        Args:
            value: 字面量值

        Returns:
            Python 字面量字符串表示
        """
        if isinstance(value, str):
            # 字符串——单引号包围，内部单引号转义
            escaped = value.replace("\\", "\\\\").replace("'", "\\'")
            return f"'{escaped}'"
        if isinstance(value, bool):
            return "True" if value else "False"
        if isinstance(value, (int, float)):
            return str(value)
        raise RenderError(f"不支持的字面量类型：{type(value).__name__}")

    # ── 函数名渲染 ──

    @staticmethod
    def render_agg_function(func: SparkAggFunction) -> str:
        """渲染聚合函数名——来自 SparkAggFunction 枚举。

        Args:
            func: 聚合函数枚举值

        Returns:
            PySpark 聚合函数名字符串
        """
        # COUNT_DISTINCT → F.countDistinct, 其余 → F.{lower}
        _fn_map: dict[SparkAggFunction, str] = {
            SparkAggFunction.COUNT: "F.count",
            SparkAggFunction.COUNT_DISTINCT: "F.countDistinct",
            SparkAggFunction.SUM: "F.sum",
            SparkAggFunction.AVG: "F.avg",
            SparkAggFunction.MIN: "F.min",
            SparkAggFunction.MAX: "F.max",
        }
        return _fn_map[func]

    @staticmethod
    def render_window_function(func: SparkWindowFunction) -> str:
        """渲染窗口函数名——来自 SparkWindowFunction 枚举。

        Args:
            func: 窗口函数枚举值

        Returns:
            PySpark 窗口函数名字符串
        """
        _fn_map: dict[SparkWindowFunction, str] = {
            SparkWindowFunction.ROW_NUMBER: "F.row_number",
            SparkWindowFunction.RANK: "F.rank",
            SparkWindowFunction.DENSE_RANK: "F.dense_rank",
            SparkWindowFunction.NTILE: "F.ntile",
            SparkWindowFunction.LAG: "F.lag",
            SparkWindowFunction.LEAD: "F.lead",
            SparkWindowFunction.SUM_OVER: "F.sum",
            SparkWindowFunction.AVG_OVER: "F.avg",
            SparkWindowFunction.COUNT_OVER: "F.count",
        }
        return _fn_map[func]

    # ── Join/Sort 枚举渲染 ──

    @staticmethod
    def render_join_type(join_type: SparkJoinType) -> str:
        """渲染 Join 类型——来自 SparkJoinType 枚举。

        Args:
            join_type: Join 类型枚举值

        Returns:
            PySpark join how 字符串
        """
        return f'"{join_type.value}"'

    @staticmethod
    def render_sort_direction(direction: SparkSortDirection) -> str:
        """渲染排序方向——来自 SparkSortDirection 枚举。

        Args:
            direction: 排序方向枚举值

        Returns:
            PySpark 排序方向函数调用字符串
        """
        if direction == SparkSortDirection.ASC:
            return "F.asc"
        elif direction == SparkSortDirection.DESC:
            return "F.desc"

    # ── 操作符渲染 ──

    @staticmethod
    def render_operator(operator: str) -> str:
        """渲染过滤操作符——从白名单映射到 Python 操作符。

        Args:
            operator: FilterStep.operator 值（如 "GT", "EQ"）

        Returns:
            Python 操作符字符串（如 ">", "=="）

        Raises:
            RenderError: 操作符不在白名单内
        """
        py_op = _OPERATOR_MAP.get(operator.upper())
        if py_op is None:
            if operator.upper() in _UNARY_OPERATORS:
                return operator.upper()  # IS_NULL / IS_NOT_NULL 在编译器层特殊处理
            raise RenderError(f"不支持的过滤操作符：'{operator}'")
        return py_op

    @staticmethod
    def is_unary_operator(operator: str) -> bool:
        """检查操作符是否需要右侧操作数。

        Args:
            operator: FilterStep.operator 值

        Returns:
            True 表示一元操作符（无需 right 操作数）
        """
        return operator.upper() in _UNARY_OPERATORS

    # ── 注释行渲染 ──

    @staticmethod
    def render_comment_line(line: str) -> str:
        """渲染单行注释——安全清洗控制字符和注入字符。

        Args:
            line: 注释文本（不含 "# " 前缀）

        Returns:
            安全的注释行字符串
        """
        # 移除控制字符（保留可打印字符）
        cleaned = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", line)
        # 防止注释注入换行
        cleaned = cleaned.replace("\n", " ").replace("\r", " ")
        # 防止 "--" 被误解为 SQL 注释
        cleaned = cleaned.replace("--", "——")
        return f"# {cleaned}"

    # ── 导入语句渲染 ──

    @staticmethod
    def render_imports() -> str:
        """渲染标准导入块——固定白名单导入。

        Returns:
            标准 import 语句块
        """
        return (
            "from pyspark.sql import DataFrame\n"
            "from pyspark.sql import functions as F\n"
            "from pyspark.sql.window import Window\n"
            "from typing import Mapping\n"
        )

    # ── 函数签名渲染 ──

    @staticmethod
    def render_function_signature() -> str:
        """渲染固定函数签名。

        Returns:
            transform 函数签名
        """
        return (
            "def transform(\n"
            "    inputs: Mapping[str, DataFrame],\n"
            "    params: TransformParams | None = None,\n"
            ") -> DataFrame:"
        )
