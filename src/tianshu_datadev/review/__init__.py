"""Review Package——Phase 3B 审查材料生成。

从 SqlBuildPlan 中提取标签规则（CASE WHEN）、窗口表达式（WindowExpr）
和字段血缘（Column Lineage），生成人类可读的审查报告。

审查包结构：
- label_rules：CASE WHEN 标签规则列表
- window_exprs：窗口函数表达式列表
- field_lineage：字段血缘映射（源 → 转换 → 目标）
"""

from __future__ import annotations

from .models import (
    FieldLineageEntry,
    LabelRuleEntry,
    ReviewReport,
    WindowExprEntry,
)
from .review_builder import ReviewBuilder

__all__ = [
    "ReviewBuilder",
    "FieldLineageEntry",
    "LabelRuleEntry",
    "ReviewReport",
    "WindowExprEntry",
]
