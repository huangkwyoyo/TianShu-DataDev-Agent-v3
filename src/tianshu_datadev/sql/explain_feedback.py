"""EXPLAIN 执行计划反馈解析器——Phase 4B。

将 DuckDB EXPLAIN / EXPLAIN ANALYZE 的文本输出解析为结构化的
ExplainFeedback 对象。识别危险操作并提供优化建议。

设计原则：
- 确定性解析——相同 EXPLAIN 文本 → 相同 ExplainFeedback
- LLM 不参与性能决策——ExplainFeedback 由代码确定性地生成
- 不改变业务语义——反馈仅记录，不修改 SqlBuildPlan
"""

from __future__ import annotations

import hashlib

from .models import ExplainFeedback

# ════════════════════════════════════════════
# 危险操作标记规则
# ════════════════════════════════════════════

# 危险操作关键词 → 优化建议映射
_DANGEROUS_OP_PATTERNS: dict[str, str] = {
    "SEQ_SCAN": "全表扫描——建议添加索引或分区过滤以启用分区裁剪",
    "CROSS_PRODUCT": "笛卡尔积——检查是否缺少 Join 条件或 Join key 拼写错误",
    "HASH_JOIN": "大表 Hash Join——检查 Join key 类型是否一致以避免隐式 CAST，考虑预聚合",
    "FULL_OUTER_JOIN": "全外连接——数据量可能极大，确认是否必须使用 FULL OUTER",
    "UNGROUPED_AGGREGATE": "无分组聚合——确认是否需要添加 GROUP BY",
    "WINDOW_FULL_SORT": "窗口函数全排序——考虑先聚合缩小数据范围再应用窗口函数",
    "NESTED_LOOP_JOIN": "嵌套循环 Join——数据量大时性能极差，检查 Join 条件",
    "TABLE_SCAN": "表扫描——确认 projection 是否裁剪了不需要的列",
}


def parse_explain_output(
    explain_text: str,
    plan_hash: str = "",
) -> ExplainFeedback:
    """解析 DuckDB EXPLAIN 输出文本，生成结构化反馈。

    识别 EXPLAIN 文本中的危险操作（全表扫描、笛卡尔积等），
    并生成对应的优化建议。

    Args:
        explain_text: EXPLAIN 或 EXPLAIN ANALYZE 的原始文本输出
        plan_hash: 关联的 SqlBuildPlan hash（用于追溯）

    Returns:
        ExplainFeedback——含标记操作和优化建议
    """
    flagged_operations: list[str] = []
    suggested_optimizations: list[str] = []

    for pattern, suggestion in _DANGEROUS_OP_PATTERNS.items():
        if pattern in explain_text:
            flagged_operations.append(pattern)
            if suggestion not in suggested_optimizations:
                suggested_optimizations.append(suggestion)

    # 若无危险操作标记，说明执行计划健康
    if not flagged_operations:
        suggested_optimizations.append("执行计划中未检测到危险操作——当前计划性能合理")

    # 若 explain_text 为空，生成占位反馈
    if not explain_text.strip():
        return ExplainFeedback(
            plan_hash=plan_hash,
            explain_output="",
            flagged_operations=[],
            suggested_optimizations=["未提供 EXPLAIN 输出——建议运行 EXPLAIN ANALYZE 获取执行计划"],
        )

    return ExplainFeedback(
        plan_hash=plan_hash,
        explain_output=explain_text,
        flagged_operations=flagged_operations,
        suggested_optimizations=suggested_optimizations,
    )


def generate_plan_hash(sql: str) -> str:
    """为 SQL 文本生成确定性 plan hash。

    Args:
        sql: SQL 文本

    Returns:
        SHA-256 前 16 位 hex
    """
    return hashlib.sha256(sql.encode()).hexdigest()[:16]


def flag_full_table_scan(explain_text: str) -> bool:
    """快速检查 EXPLAIN 输出中是否包含全表扫描。

    Args:
        explain_text: EXPLAIN 输出文本

    Returns:
        True 表示检测到全表扫描
    """
    return "SEQ_SCAN" in explain_text


def flag_cross_join(explain_text: str) -> bool:
    """快速检查 EXPLAIN 输出中是否包含笛卡尔积。

    Args:
        explain_text: EXPLAIN 输出文本

    Returns:
        True 表示检测到笛卡尔积
    """
    return "CROSS_PRODUCT" in explain_text
