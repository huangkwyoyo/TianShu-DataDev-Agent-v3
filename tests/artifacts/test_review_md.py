"""测试 review.md 生成器——可读性验证。

覆盖：
- review.md 包含所有关键章节
- 不含代码实现细节
- 面向数据工程师可读
"""

import os

from tianshu_datadev.artifacts.models import HumanReviewItem, PackageInputs
from tianshu_datadev.artifacts.review_md import generate_review_md
from tianshu_datadev.developer_spec.parser import DeveloperSpecParser
from tianshu_datadev.planning.sql_build_plan import SqlBuildPlan, SqlBuildPlanBuilder
from tianshu_datadev.sql.compiler import DuckDbSqlCompiler

# ── 辅助 ──


def _read_fixture(path: str) -> str:
    abs_path = os.path.join(os.path.dirname(__file__), "..", path)
    with open(abs_path, "r", encoding="utf-8") as f:
        return f.read()


def _build_minimal_inputs() -> PackageInputs:
    """构建最小合法 PackageInputs。"""
    spec_text = _read_fixture("fixtures/golden/golden_no_time_range.md")
    parser = DeveloperSpecParser()
    spec = parser.parse(spec_text)

    builder = SqlBuildPlanBuilder()
    plan, _ = builder.build(spec)

    compiler = DuckDbSqlCompiler()
    artifact = compiler.compile_to_artifact(plan, spec_hash=spec.spec_hash)

    return PackageInputs(
        request_id="test_req_review",
        original_spec_md=spec_text,
        parsed_spec=spec.model_dump(),
        source_manifest={
            "manifest_id": f"manifest_{spec.spec_hash[:12]}",
            "spec_hash": spec.spec_hash,
            "tables": [],
            "conflicts": [],
            "anomalies": [],
        },
        hypothesis=None,
        sql_build_plan=plan.model_dump(),
        sql_artifact=artifact.model_dump(),
        execution_trace={
            "trace_id": "trace_test",
            "plan_id": plan.plan_id,
            "engine": "duckdb",
            "generated_sql": artifact.compiled_sql.sql,
            "status": "RUNTIME_PASS",
            "row_count": 100,
            "execution_time_ms": 15.5,
        },
        result_summary={
            "summary_id": "summary_test",
            "trace_id": "trace_test",
            "engine": "duckdb",
            "columns": ["zone", "total_amount"],
            "column_types": ["varchar", "double"],
            "row_count": 100,
            "null_counts": {},
            "numeric_sums": {},
        },
        data_transform_contract={
            "contract_id": "dtc_lite_test",
            "version": "lite",
            "source_phase": "phase-2",
            "source_sqlbuildplan_hash": SqlBuildPlan.generate_plan_hash(plan),
            "input_tables": [],
            "input_columns": [],
            "join_relationships": [],
            "filters": [],
            "aggregations": [],
            "grouping_keys": [],
            "output_columns": [],
            "output_grain": [],
            "business_keys": [],
            "semantic_policy_ref": "",
        },
    )


# ════════════════════════════════════════════
# Review.md 可读性测试
# ════════════════════════════════════════════


class TestReviewMd:
    """review.md 可读性测试。"""

    def test_review_md_contains_all_sections(self):
        """review.md 必须包含 8 个核心章节。"""
        inputs = _build_minimal_inputs()
        review_items = [
            HumanReviewItem(
                item_id="hr_001",
                category="join_evidence",
                description="确认 Join 关系正确",
                severity="warning",
                related_artifact="planning/relationship_hypotheses.md",
            ),
        ]
        md = generate_review_md(inputs, review_items)

        # 验证 8 个章节标题
        required_sections = [
            "## 1. 项目目标",
            "## 2. 数据结构化理解",
            "## 3. Join 证据链",
            "## 4. SQL（编译产物）",
            "## 5. 执行摘要",
            "## 6. 人工审查清单",
            "## 7. 开放问题",
            "## 8. 性能建议",
        ]
        for section in required_sections:
            assert section in md, f"review.md 缺少章节: {section}"

    def test_review_md_no_implementation_details(self):
        """review.md 不应包含实现细节（如 Compiler Pass、AST 节点名）。"""
        inputs = _build_minimal_inputs()
        md = generate_review_md(inputs)

        # 不应暴露内部实现细节
        banned_terms = [
            "CompilerPass",
            "SqlBuildPlan",
            "StepNode",
            "PredicateNorm",
            "ConstantFold",
            "OptimizedSQLPlan",
            "column_pruning",
            "sort_elimination",
            "predicate_normalization",
            "constant_folding",
        ]
        for term in banned_terms:
            assert term not in md, (
                f"review.md 暴露了实现细节: '{term}'"
            )

    def test_review_md_includes_sql(self):
        """review.md 应包含编译后的 SQL。"""
        inputs = _build_minimal_inputs()
        md = generate_review_md(inputs)

        assert "```sql" in md
        assert "SELECT" in md
        assert "FROM" in md

    def test_review_md_includes_execution_summary(self):
        """review.md 应包含执行摘要（状态、行数、耗时）。"""
        inputs = _build_minimal_inputs()
        md = generate_review_md(inputs)

        assert "RUNTIME_PASS" in md
        assert "100" in md  # row_count
        assert "15.5" in md  # execution_time_ms

    def test_review_md_handles_missing_trace(self):
        """无 ExecutionTrace 时不应崩溃。"""
        inputs = _build_minimal_inputs()
        inputs.execution_trace = None
        inputs.result_summary = None

        md = generate_review_md(inputs)
        assert "未执行" in md
