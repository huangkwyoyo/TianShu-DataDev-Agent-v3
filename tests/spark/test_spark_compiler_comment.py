"""Phase 6 注释块格式测试——5 行固定格式（Step/Intent/Operation/Inputs/Output）。

验证：
- 每步生成 5 行注释
- 不含 SQL 文本
- 删除注释后执行代码等价
"""

from __future__ import annotations

import re

from tianshu_datadev.spark.compiler import SparkCompiler
from tianshu_datadev.spark.models import (
    SparkFilterStep,
    SparkLimitStep,
    SparkPlan,
    SparkProjectColumn,
    SparkProjectStep,
    SparkReadStep,
    SparkSortDirection,
    SparkSortSpec,
    SparkSortStep,
)


def _make_plan(*steps) -> SparkPlan:
    return SparkPlan(
        plan_id="test",
        version="v1",
        source_phase="phase-6",
        source_contract_hash="test_hash",
        steps=list(steps),
    )


class TestCommentFormat:
    """5 行注释格式测试。"""

    def test_comment_has_five_lines(self):
        """每个步骤生成恰好 5 行注释。"""
        plan = _make_plan(
            SparkReadStep(alias="od", source_name="dwd.order_detail", input_key="od"),
        )
        compiler = SparkCompiler()
        result = compiler.compile(plan)

        comments = [
            l for l in result.annotated_pyspark.split("\n")
            if l.strip().startswith("# Step:") or l.strip().startswith("# Intent:")
            or l.strip().startswith("# Operation:") or l.strip().startswith("# Inputs:")
            or l.strip().startswith("# Output:")
        ]
        # 每个 step 5 行注释
        assert len(comments) == 5

    def test_comment_keys_present(self):
        """5 个固定 key 全部出现在注释中。"""
        plan = _make_plan(
            SparkReadStep(alias="od", source_name="dwd.order_detail", input_key="od"),
        )
        compiler = SparkCompiler()
        result = compiler.compile(plan)

        assert "# Step:" in result.annotated_pyspark
        assert "# Intent:" in result.annotated_pyspark
        assert "# Operation:" in result.annotated_pyspark
        assert "# Inputs:" in result.annotated_pyspark
        assert "# Output:" in result.annotated_pyspark

    def test_comment_missing_from_raw(self):
        """raw_pyspark 不含注释。"""
        plan = _make_plan(
            SparkReadStep(alias="od", source_name="dwd.order_detail", input_key="od"),
        )
        compiler = SparkCompiler()
        result = compiler.compile(plan)

        assert "# Step:" not in result.raw_pyspark
        assert "# Intent:" not in result.raw_pyspark

    def test_comment_index_format(self):
        """注释包含索引信息（索引 N/总数）。"""
        plan = _make_plan(
            SparkReadStep(alias="od", source_name="dwd.order_detail", input_key="od"),
        )
        compiler = SparkCompiler()
        result = compiler.compile(plan)

        assert "索引 1/1" in result.annotated_pyspark


class TestCommentNoSQL:
    """注释中不含 SQL 文本。"""

    def test_no_sql_keywords_in_comment(self):
        """注释中不含 SELECT/FROM/WHERE/JOIN 等 SQL 关键字。"""
        plan = _make_plan(
            SparkReadStep(alias="od", source_name="dwd.order_detail", input_key="od"),
            SparkFilterStep(input_alias="od", operator="EQ", left="od.status", right="'paid'"),
            SparkProjectStep(
                input_alias="_f1",
                columns=[SparkProjectColumn(column_name="amount", alias="amount")],
            ),
            SparkSortStep(
                input_alias="_p2",
                order_by=[SparkSortSpec(column="amount", direction=SparkSortDirection.DESC)],
            ),
            SparkLimitStep(input_alias="_s3", limit=100),
        )
        compiler = SparkCompiler()
        result = compiler.compile(plan)

        # 提取注释行
        comment_lines = [
            l for l in result.annotated_pyspark.split("\n")
            if l.strip().startswith("#")
        ]

        # SQL 关键字正则（仅匹配完整单词）
        sql_pattern = re.compile(
            r"\b(SELECT|FROM|WHERE|JOIN|GROUP\s+BY|ORDER\s+BY|HAVING|UNION|INSERT|UPDATE|DELETE)\b",
            re.IGNORECASE,
        )

        for line in comment_lines:
            # 去掉 "# " 前缀
            content = line.strip()[2:] if line.strip().startswith("# ") else line.strip()[1:]
            match = sql_pattern.search(content)
            if match:
                # 允许 "WHERE" 出现在合理上下文中（如 "下游消费者" 不含 SQL 语义）
                # 但如果出现在明显的 SQL 语句中，则告警
                pass
            # 不强制 assert——注释中的 SQL 关键字检查由残留扫描脚本负责
            assert sql_pattern.search(content) is None, (
                f"注释行含 SQL 关键字：{line.strip()}"
            )


class TestAnnotationsRemovable:
    """删除注释后执行代码等价测试。"""

    def test_raw_equals_annotated_minus_comments(self):
        """raw_pyspark 与 annotated_pyspark 去注释后完全一致。"""
        plan = _make_plan(
            SparkReadStep(alias="od", source_name="dwd.order_detail", input_key="od"),
            SparkFilterStep(input_alias="od", operator="EQ", left="od.status", right="'paid'"),
            SparkProjectStep(
                input_alias="_f1",
                columns=[SparkProjectColumn(column_name="status", alias="status")],
            ),
        )
        compiler = SparkCompiler()
        result = compiler.compile(plan)

        # 去除注释行
        def _strip_comments(code: str) -> str:
            lines = [
                l for l in code.split("\n")
                if not l.strip().startswith("#")
            ]
            return "\n".join(lines)

        raw_stripped = _strip_comments(result.raw_pyspark)
        ann_stripped = _strip_comments(result.annotated_pyspark)

        assert raw_stripped == ann_stripped
