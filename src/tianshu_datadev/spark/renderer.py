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

    # ── 控制字符转义（render_dict_key / render_comment_text 共用） ──

    # 控制字符 → Python 转义序列映射
    _CONTROL_CHAR_ESCAPE: dict[str, str] = {
        "\n": "\\n", "\r": "\\r", "\t": "\\t",
    }

    @staticmethod
    def _escape_control_chars(s: str) -> str:
        """转义字符串中的全部 ASCII 控制字符（0x00-0x1F, 0x7F）。

        已知字符使用简短转义（\\n/\\r/\\t），其余使用 \\xNN 格式。
        用于 render_dict_key 生成安全的 Python 字符串字面量。
        """
        result: list[str] = []
        for ch in s:
            cp = ord(ch)
            if cp < 0x20 or cp == 0x7F:
                known = SparkCodeRenderer._CONTROL_CHAR_ESCAPE.get(ch)
                if known:
                    result.append(known)
                else:
                    result.append(f"\\x{cp:02x}")
            else:
                result.append(ch)
        return "".join(result)

    # ── 字典键渲染 ──

    @staticmethod
    def render_dict_key(key: str) -> str:
        """渲染字典键字符串——转义双引号、反斜杠和全部控制字符，防止注入。

        用于生成 inputs["key"] 形式的代码。key 不是 Python 变量名，
        允许点号等字符，但必须转义可能破坏字符串边界的字符。

        Args:
            key: 字典键原始字符串

        Returns:
            带双引号包围的安全字符串
        """
        # 先转义反斜杠（必须在其他转义之前），再转义双引号
        escaped = key.replace("\\", "\\\\").replace('"', '\\"')
        # 转义全部 ASCII 控制字符（0x00-0x1F, 0x7F）
        escaped = SparkCodeRenderer._escape_control_chars(escaped)
        return f'"{escaped}"'

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

    # ── 过滤右值渲染 ──

    # 右值中禁止出现的危险模式
    _FORBIDDEN_RIGHT_PATTERNS: tuple[str, ...] = (
        "exec(", "eval(", "compile(",
        "__import__", "import ",
        "spark.read", "spark.table", "spark.sql", "spark._jspark",
        "subprocess", "os.system", "os.popen",
    )

    @staticmethod
    def render_filter_right(value: str) -> str:
        """渲染过滤条件右值——列引用走 render_column()，字面量经安全扫描后返回。

        规则：
        1. 含 "." 且不含引号 → 列引用，委托 render_column()
        2. 其他 → 预格式化表达式，安全扫描后通过（拒绝危险模式和控制字符）

        Args:
            value: 过滤条件右值字符串

        Returns:
            安全渲染后的 Python 表达式片段

        Raises:
            RenderError: 检测到危险内容或格式异常
        """
        # 列引用检测——含点号且不含任何引号
        if "." in value and "'" not in value and '"' not in value:
            return SparkCodeRenderer.render_column(value)

        # 安全扫描——拒绝危险模式
        for pattern in SparkCodeRenderer._FORBIDDEN_RIGHT_PATTERNS:
            if pattern.startswith("spark."):
                # spark.* 检查大小写不敏感——SparK.Read 也会被拦截
                if pattern in value.lower():
                    raise RenderError(f"过滤右值含危险模式：'{pattern}'")
            elif pattern in value:
                raise RenderError(f"过滤右值含危险模式：'{pattern}'")

        # 拒绝控制字符——换行/回车/空字节等可破坏代码结构
        for ch in ["\n", "\r", "\x00", "\x1b"]:
            if ch in value:
                raise RenderError(f"过滤右值含控制字符：{repr(ch)}")

        # 引号配对检查——防止字符串字面量逃逸
        for quote in ("'", '"'):
            if value.count(quote) % 2 != 0:
                raise RenderError(f"过滤右值引号不配对（{quote} 出现 {value.count(quote)} 次）")

        return value

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

    # ── Window 帧边界渲染 ──

    # 合法的帧边界符号值
    _FRAME_BOUNDARY_SYMBOLS: frozenset[str] = frozenset({
        "unbounded_preceding", "unbounded_following", "current_row",
    })

    # 帧边界符号 snake_case → PySpark camelCase 显式映射
    _FRAME_BOUNDARY_CAMEL_MAP: dict[str, str] = {
        "unbounded_preceding": "Window.unboundedPreceding",
        "unbounded_following": "Window.unboundedFollowing",
        "current_row": "Window.currentRow",
    }

    @staticmethod
    def render_frame_boundary(boundary: str) -> str:
        """渲染窗口帧边界值——白名单符号或非负整数。

        合法值：
        - 符号值："unbounded_preceding" / "unbounded_following" / "current_row"
          → 映射为 PySpark camelCase 常量（Window.unboundedPreceding 等）
        - 整数字面量：如 "0"、"5"（非负整数）

        Args:
            boundary: 帧边界字符串表示

        Returns:
            PySpark Window 帧边界表达式

        Raises:
            RenderError: 边界值不在白名单且不是非负整数
        """
        b = boundary.strip().lower()
        # 符号值——显式映射到 PySpark camelCase 常量
        camel = SparkCodeRenderer._FRAME_BOUNDARY_CAMEL_MAP.get(b)
        if camel is not None:
            return camel
        # 非负整数字面量
        if b.isdigit():
            return boundary.strip()
        raise RenderError(f"非法的窗口帧边界值：'{boundary}'")

    @staticmethod
    def render_frame_type(frame_type: str) -> str:
        """渲染窗口帧类型——rows 或 range。

        Args:
            frame_type: "rows" 或 "range"

        Returns:
            PySpark Window 帧类型方法名

        Raises:
            RenderError: 帧类型不在白名单
        """
        ft = frame_type.strip().lower()
        if ft == "rows":
            return "rowsBetween"
        elif ft == "range":
            return "rangeBetween"
        raise RenderError(f"非法的窗口帧类型：'{frame_type}'，仅支持 rows / range")

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
    def render_join_key(dataframe_alias: str, column_name: str) -> str:
        """渲染 Join 键引用——df["col"] 格式，用于消除同名列歧义。

        对 dataframe_alias 做标识符校验，对 column_name 做安全校验。
        返回 df["col"] 格式而非 F.col("col")，避免同名歧义。

        Args:
            dataframe_alias: DataFrame 变量名（如 "od"）
            column_name: Join 键列名（如 "user_id"）

        Returns:
            'od["user_id"]' 格式的引用字符串

        Raises:
            RenderError: 标识符或列名不安全
        """
        SparkCodeRenderer.validate_identifier(
            dataframe_alias, "join DataFrame 别名"
        )
        SparkCodeRenderer.validate_identifier(
            column_name, "join 键列名"
        )
        if '"' in column_name or "'" in column_name or ";" in column_name:
            raise RenderError(f"Join 键含非法字符：'{column_name}'")
        return f'{dataframe_alias}["{column_name}"]'

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

    # ── 注释文本渲染 ──

    @staticmethod
    def render_comment_text(text: str) -> str:
        """清洗注释文本——移除/替换控制字符、防止换行注入。

        用于 _build_comment_block 中各字段（intent/operation/inputs/output）
        的文本清洗。返回单行安全文本，不含 "# " 前缀。

        Args:
            text: 原始注释文本（可能含换行、控制字符）

        Returns:
            单行安全文本，控制字符已移除或替换
        """
        # 移除 ASCII 控制字符（0x00-0x1F, 0x7F），保留 \t 转为空格
        cleaned: list[str] = []
        for ch in text:
            cp = ord(ch)
            if cp == 0x09:  # \t → 空格
                cleaned.append(" ")
            elif cp < 0x20 or cp == 0x7F:
                # 换行/回车 → 空格（防止注释逃逸为裸代码行）
                # 其他控制字符 → 丢弃
                if ch in ("\n", "\r"):
                    cleaned.append(" ")
                # 其余控制字符直接丢弃
            else:
                cleaned.append(ch)
        result = "".join(cleaned)
        # 防止 SQL 注释注入
        result = result.replace("--", "——")
        return result

    @staticmethod
    def render_comment_line(line: str) -> str:
        """渲染单行注释——清洗后添加 "# " 前缀。

        Args:
            line: 注释文本（不含 "# " 前缀）

        Returns:
            安全的注释行字符串（"# " 开头，单行）
        """
        cleaned = SparkCodeRenderer.render_comment_text(line)
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
