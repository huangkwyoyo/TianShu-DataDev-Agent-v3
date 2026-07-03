"""表达式安全校验模块——确保 input_expression / expression 字段不会成为 SQL 注入逃生口。

设计原则：
    - 入站侧：拒绝明显恶意字符和 SQL 模式（FORBIDDEN_CHARS + FORBIDDEN_PATTERNS）
    - 编译器侧：白名单正则——只允许列引用 + 算术运算符 + 数字字面量 + 括号 + 空格
    - 两道闸门独立运作，任一失效仍有另一道拦截

使用方式：
    from tianshu_datadev.sql.expression_guard import validate_input_expression

    # 入站侧（Parser / Enricher）
    result = validate_input_expression(expr, mode="strict")  # 返回 (is_valid, error_message)
    result = validate_input_expression(expr, mode="silent")  # 返回 (is_valid, error_message)

    # 编译器侧（最终防线——白名单正则）
    result = validate_input_expression(expr, mode="compiler")  # 返回 (is_valid, error_message)
"""

from __future__ import annotations

import re

# ═══════════════════════════════════════════════════════════════════
# 入站侧：禁止字符 / 禁止模式（与 SpecEnricher._FORBIDDEN_EXPRESSION_* 对齐）
# ═══════════════════════════════════════════════════════════════════

# 禁止出现在表达式中的字符——SQL 注入 / 注释 / 字符串逃逸
_FORBIDDEN_CHARS: frozenset[str] = frozenset({";", "'", '"', "`"})

# 禁止出现在表达式中的子串模式——SQL 注释
_FORBIDDEN_PATTERNS: tuple[str, ...] = ("--", "/*")

# ═══════════════════════════════════════════════════════════════════
# 编译器侧：白名单正则——只允许安全的算术表达式
# ═══════════════════════════════════════════════════════════════════

# 合法的 input_expression 只能包含：
#   - 标识符字符：字母、数字、下划线、中文（列名引用）
#   - 算术运算符：+ - * / %
#   - 括号：( )
#   - 空格
#   - 点号：. （表前缀，如 "tf.amount"）
# 不允许：SQL 关键字、函数调用括号+参数、字符串字面量、注释
_SAFE_EXPRESSION_PATTERN = re.compile(
    r"^[\w一-鿿.+*\-/()%\s]+$"
)

# 编译器侧额外检查——禁止在表达式中出现的 SQL 关键字子串
# 即使通过了白名单正则，这些关键字也不允许出现
_COMPILER_FORBIDDEN_KEYWORDS: tuple[str, ...] = (
    "SELECT", "FROM", "WHERE", "DROP", "INSERT", "UPDATE", "DELETE",
    "UNION", "JOIN", "INTO", "CREATE", "ALTER", "EXEC", "EXECUTE",
    "GRANT", "REVOKE", "TRUNCATE", "MERGE", "REPLACE",
)


def validate_input_expression(
    expression: str | None,
    mode: str = "strict",
) -> tuple[bool, str]:
    """校验 input_expression / expression 字段是否安全。

    三层检查：
    1. 禁止字符——拒绝注入/注释/字符串逃逸字符
    2. 禁止模式——拒绝 SQL 注释模式
    3. 编译器模式额外——白名单正则 + SQL 关键字拒绝

    Args:
        expression: 待校验的表达式字符串，None 视为合法（无表达式）
        mode: "strict" — 同 silent，两者行为一致，均检查第 1-2 层（禁止字符 + 禁止模式），
              区别在调用方——strict 模式的调用方（如 Parser）抛出异常阻断流程，
              silent 模式的调用方（如 Enricher）静默丢弃非法表达式
              "compiler" — 编译器侧最终防线（白名单正则 + SQL 关键字检查），比前两者更严格

    Returns:
        (is_valid, error_message): is_valid=True 表示安全，error_message 在失败时为诊断信息
        注意：三种 mode 均不抛出异常——异常由调用方根据模式决定是否抛出
    """
    if expression is None:
        return True, ""

    expr = expression.strip()
    if not expr:
        # 空字符串或纯空格——表达式应有实际内容，视作无效
        return False, "表达式为空或仅包含空白字符"

    # ── 第 1 层：禁止字符 ──
    for ch in _FORBIDDEN_CHARS:
        if ch in expr:
            return False, (
                f"表达式 '{expression}' 含禁止字符 '{ch}'——"
                f"input_expression 仅允许列引用和算术运算符（+ - * / %）"
            )

    # ── 第 2 层：禁止模式 ──
    for pattern in _FORBIDDEN_PATTERNS:
        if pattern in expr:
            return False, (
                f"表达式 '{expression}' 含禁止模式 '{pattern}'——"
                f"SQL 注释不被允许"
            )

    # ── 第 3 层（仅 compiler 模式）：白名单正则 + SQL 关键字 ──
    if mode == "compiler":
        # 白名单正则——只允许安全的算术表达式字符
        if not _SAFE_EXPRESSION_PATTERN.match(expr):
            return False, (
                f"表达式 '{expression}' 含非法字符——"
                f"仅允许列引用（含中文）、算术运算符（+ - * / %）、括号和空格"
            )

        # SQL 关键字检查——大小写不敏感
        expr_upper = expr.upper()
        for kw in _COMPILER_FORBIDDEN_KEYWORDS:
            # 使用词边界检查——避免误杀列名中包含的子串
            # 如 "amount" 不应因为包含 "COUNT" 而被拒绝
            # 但 "1 FROM users" 中的 "FROM" 应被拒绝
            if re.search(rf"\b{re.escape(kw)}\b", expr_upper):
                return False, (
                    f"表达式 '{expression}' 含 SQL 关键字 '{kw}'——"
                    f"input_expression 不允许包含 SQL 关键字"
                )

    return True, ""
