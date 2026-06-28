"""EXPLAIN 反馈解析测试。

验证：parse_explain_output 正确识别危险操作并生成优化建议。
"""

from __future__ import annotations

from tianshu_datadev.sql.explain_feedback import (
    flag_cross_join,
    flag_full_table_scan,
    generate_plan_hash,
    parse_explain_output,
)
from tianshu_datadev.sql.models import ExplainFeedback


class TestExplainFeedback:
    """EXPLAIN 反馈解析。"""

    def test_explain_detects_seq_scan(self):
        """EXPLAIN 文本包含 SEQ_SCAN → flagged_operations 包含 SEQ_SCAN。"""
        explain_text = """
        Physical Plan:
        ┌───────────────────────────┐
        │         PROJECTION        │
        │    ────────────────────   │
        │          id, name         │
        └─────────────┬─────────────┘
        ┌─────────────┴─────────────┐
        │          SEQ_SCAN         │
        │    ────────────────────   │
        │          large_table      │
        │       Rows: 5000000       │
        └───────────────────────────┘
        """

        result = parse_explain_output(explain_text, plan_hash="abc123")
        assert isinstance(result, ExplainFeedback)
        assert "SEQ_SCAN" in result.flagged_operations
        assert any("全表扫描" in s for s in result.suggested_optimizations)

    def test_explain_detects_cross_product(self):
        """EXPLAIN 文本包含 CROSS_PRODUCT → flagged_operations。"""
        explain_text = """
        Physical Plan:
        ┌───────────────────────────┐
        │        CROSS_PRODUCT      │
        │    ────────────────────   │
        │       t1 × t2 (no key)    │
        └───────────────────────────┘
        """

        result = parse_explain_output(explain_text, plan_hash="xyz789")
        assert "CROSS_PRODUCT" in result.flagged_operations
        assert any("笛卡尔积" in s for s in result.suggested_optimizations)

    def test_explain_detects_hash_join(self):
        """EXPLAIN 文本包含 HASH_JOIN → 被标记。"""
        explain_text = """
        Physical Plan:
        ┌───────────────────────────┐
        │         HASH_JOIN         │
        │    ────────────────────   │
        │       ON t1.id = t2.id    │
        │       (5000000 rows)      │
        └───────────────────────────┘
        """

        result = parse_explain_output(explain_text, plan_hash="hash_join_01")
        assert "HASH_JOIN" in result.flagged_operations

    def test_explain_clean_plan(self):
        """EXPLAIN 文本无危险操作 → flagged_operations 为空，有健康提示。"""
        explain_text = """
        Physical Plan:
        ┌───────────────────────────┐
        │         PROJECTION        │
        │          id, name         │
        └─────────────┬─────────────┘
        ┌─────────────┴─────────────┐
        │        INDEX_SCAN         │
        │       idx_large_table     │
        │       Rows: 100           │
        └───────────────────────────┘
        """

        result = parse_explain_output(explain_text, plan_hash="clean_01")
        assert result.flagged_operations == []
        assert any("未检测到危险操作" in s for s in result.suggested_optimizations)

    def test_explain_empty_text(self):
        """空 EXPLAIN 文本 → 提示运行 EXPLAIN。"""
        result = parse_explain_output("", plan_hash="empty")
        assert result.flagged_operations == []
        assert any("未提供 EXPLAIN" in s for s in result.suggested_optimizations)

    def test_flag_full_table_scan_helper(self):
        """flag_full_table_scan 辅助函数正确检测 SEQ_SCAN。"""
        assert flag_full_table_scan("Physical Plan:\nSEQ_SCAN\nlarge_table") is True
        assert flag_full_table_scan("Physical Plan:\nINDEX_SCAN\nsmall_table") is False

    def test_flag_cross_join_helper(self):
        """flag_cross_join 辅助函数正确检测 CROSS_PRODUCT。"""
        assert flag_cross_join("Physical Plan:\nCROSS_PRODUCT\nt1, t2") is True
        assert flag_cross_join("Physical Plan:\nHASH_JOIN\nt1, t2") is False

    def test_generate_plan_hash_deterministic(self):
        """generate_plan_hash 是确定性的——相同输入 → 相同输出。"""
        sql = "SELECT id, name FROM users WHERE dt >= '2026-01-01'"
        h1 = generate_plan_hash(sql)
        h2 = generate_plan_hash(sql)
        assert h1 == h2
        assert len(h1) == 16
