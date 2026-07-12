"""PlanComparator 测试——Sort/Project/Limit/CaseWhen 步骤等价性对比。

从 test_plan_comparator.py 拆分（Phase 6.2 Comparator 文件拆分）。
公共构建器见 tests/spark/plan_comparator_fixtures.py。
"""

from __future__ import annotations

from tests.spark.plan_comparator_fixtures import (
    _make_spark_limit_step,
    _make_spark_plan,
    _make_spark_project_step,
    _make_spark_read_step,
    _make_spark_sort_step,
    _make_sql_limit_step,
    _make_sql_plan,
    _make_sql_project_step,
    _make_sql_scan_step,
    _make_sql_sort_step,
)
from tianshu_datadev.planning.models import (
    ColumnRef,
    Predicate,
    PredicateOperator,
    SortDirection,
    SortSpec,
    SqlLiteral,
    WhenBranch,
)
from tianshu_datadev.planning.sql_build_plan import (
    SortStep,
)
from tianshu_datadev.spark.models import (
    SparkLimitStep,
    SparkProjectColumn,
    SparkProjectStep,
    SparkSortDirection,
    SparkSortSpec,
    SparkSortStep,
    SparkStepType,
)
from tianshu_datadev.spark.plan_comparator import (
    ComparisonStatus,
    PlanComparator,
)
from tianshu_datadev.spark.plan_equivalence import EquivalenceVerdict


class TestPlanComparatorProjectEquivalence:
    """Project 逻辑等价性对比。"""

    def test_project_equivalent(self):
        """相同投影列 → LOGIC_EQUIVALENT。"""
        sql_plan = _make_sql_plan(
            [
                _make_sql_scan_step(),
                _make_sql_project_step(),
            ]
        )
        spark_plan = _make_spark_plan(
            [
                _make_spark_read_step(),
                _make_spark_project_step(),
            ]
        )

        comparator = PlanComparator()
        report = comparator.compare(sql_plan, spark_plan)

        assert report.status == ComparisonStatus.LOGIC_EQUIVALENT

    def test_project_not_equivalent(self):
        """投影列不一致 → LOGIC_MISMATCH。"""
        sql_plan = _make_sql_plan(
            [
                _make_sql_scan_step(),
                _make_sql_project_step(),
            ]
        )
        spark_plan = _make_spark_plan(
            [
                _make_spark_read_step(),
                SparkProjectStep(
                    step_type=SparkStepType.PROJECT,
                    input_alias="od",
                    columns=[
                        SparkProjectColumn(
                            column_name="different_col",
                            alias="different_col",
                        ),
                    ],
                ),
            ]
        )

        comparator = PlanComparator()
        report = comparator.compare(sql_plan, spark_plan)

        assert report.status == ComparisonStatus.LOGIC_MISMATCH

class TestPlanComparatorSortEquivalence:
    """Sort 逻辑等价性对比。"""

    def test_sort_equivalent(self):
        """相同排序规格 → LOGIC_EQUIVALENT。

        使用 DESC——两侧默认 null_order 均为 LAST（Spark ORDER BY DESC→NULLS LAST，SQL SortSpec 默认 LAST）。
        ASC 场景两侧默认不同（SQL LAST vs Spark FIRST），参见 test_asc_default_nulls_mismatch_detected。
        """
        sql_plan = _make_sql_plan(
            [
                _make_sql_scan_step(),
                _make_sql_sort_step(),
            ]
        )
        spark_plan = _make_spark_plan(
            [
                _make_spark_read_step(),
                _make_spark_sort_step(),
            ]
        )

        comparator = PlanComparator()
        report = comparator.compare(sql_plan, spark_plan)

        assert report.status == ComparisonStatus.LOGIC_EQUIVALENT

    def test_sort_not_equivalent(self):
        """排序方向不一致 → LOGIC_MISMATCH。"""
        sql_plan = _make_sql_plan(
            [
                _make_sql_scan_step(),
                _make_sql_sort_step(),  # DESC
            ]
        )
        spark_plan = _make_spark_plan(
            [
                _make_spark_read_step(),
                SparkSortStep(
                    step_type=SparkStepType.SORT,
                    input_alias="od",
                    order_by=[
                        SparkSortSpec(
                            column="amount",
                            direction=SparkSortDirection.ASC,
                        ),
                    ],
                ),
            ]
        )

        comparator = PlanComparator()
        report = comparator.compare(sql_plan, spark_plan)

        assert report.status == ComparisonStatus.LOGIC_MISMATCH

    def test_sort_nulls_first_vs_desc_default_not_equivalent(self):
        """SQL DESC NULLS FIRST vs Spark DESC 默认 NULLS LAST → NOT_EQUIVALENT。

        Spark ORDER BY DESC 默认 NULLS LAST，SQL 显式 NULLS FIRST 产生差异。
        """
        from tianshu_datadev.planning.models import NullOrder

        sql_plan = _make_sql_plan([
            _make_sql_scan_step(),
            SortStep(
                step_type="sort",
                step_id="step_sort_001",
                order_by=[
                    SortSpec(
                        column="amount",
                        direction=SortDirection.DESC,
                        null_order=NullOrder.FIRST,
                    ),
                ],
            ),
        ])
        spark_plan = _make_spark_plan([
            _make_spark_read_step(),
            _make_spark_sort_step(),  # 默认 DESC → NULLS LAST
        ])

        comparator = PlanComparator()
        report = comparator.compare(sql_plan, spark_plan)

        # null_order 不同 → NOT_EQUIVALENT
        sort_results = [r for r in report.step_results if r.step_type == "sort"]
        assert len(sort_results) == 1
        assert sort_results[0].verdict == EquivalenceVerdict.NOT_EQUIVALENT

    def test_desc_default_nulls_match(self):
        """SQL DESC（默认 NULLS LAST）vs Spark DESC（默认 NULLS LAST）→ EQUIVALENT。

        Spark SQL ORDER BY DESC 默认 NULLS LAST，SQL SortSpec 也默认 LAST。
        两侧一致，应为等价。
        """
        sql_plan = _make_sql_plan([
            _make_sql_scan_step(),
            SortStep(
                step_type="sort",
                step_id="step_sort_001",
                order_by=[
                    SortSpec(
                        column="amount",
                        direction=SortDirection.DESC,
                        # 不显式指定 null_order → SQL 默认 LAST
                    ),
                ],
            ),
        ])
        spark_plan = _make_spark_plan([
            _make_spark_read_step(),
            _make_spark_sort_step(),  # 默认 DESC → NULLS LAST
        ])

        comparator = PlanComparator()
        report = comparator.compare(sql_plan, spark_plan)

        sort_results = [r for r in report.step_results if r.step_type == "sort"]
        assert len(sort_results) == 1
        assert sort_results[0].verdict == EquivalenceVerdict.EQUIVALENT, (
            f"DESC 默认 null_order 应等价，实际：{sort_results[0].verdict}"
        )

    def test_asc_default_nulls_mismatch_detected(self):
        """SQL ASC（默认 NULLS LAST）vs Spark ASC（默认 NULLS FIRST）→ NOT_EQUIVALENT。

        Spark SQL ORDER BY ASC 默认 NULLS FIRST，SQL SortSpec 默认 LAST。
        此差异应被 comparator 检测。
        """
        sql_plan = _make_sql_plan([
            _make_sql_scan_step(),
            SortStep(
                step_type="sort",
                step_id="step_sort_001",
                order_by=[
                    SortSpec(
                        column="amount",
                        direction=SortDirection.ASC,
                        # 不显式指定 null_order → SQL 默认 LAST
                    ),
                ],
            ),
        ])
        spark_plan = _make_spark_plan([
            _make_spark_read_step(),
            _make_spark_sort_step(),  # 默认 DESC
        ])
        # 对齐方向为 ASC
        spark_plan.steps[1].order_by[0].direction = SparkSortDirection.ASC

        comparator = PlanComparator()
        report = comparator.compare(sql_plan, spark_plan)

        sort_results = [r for r in report.step_results if r.step_type == "sort"]
        assert len(sort_results) == 1
        assert sort_results[0].verdict == EquivalenceVerdict.NOT_EQUIVALENT, (
            f"ASC null_order 应检测到不匹配，实际：{sort_results[0].verdict}"
        )

class TestPlanComparatorLimitEquivalence:
    """Limit 逻辑等价性对比。"""

    def test_limit_equivalent(self):
        """相同 limit 值 → LOGIC_EQUIVALENT。"""
        sql_plan = _make_sql_plan(
            [
                _make_sql_scan_step(),
                _make_sql_limit_step(),
            ]
        )
        spark_plan = _make_spark_plan(
            [
                _make_spark_read_step(),
                _make_spark_limit_step(),
            ]
        )

        comparator = PlanComparator()
        report = comparator.compare(sql_plan, spark_plan)

        assert report.status == ComparisonStatus.LOGIC_EQUIVALENT

    def test_limit_not_equivalent(self):
        """不同 limit 值 → LOGIC_MISMATCH。"""
        sql_plan = _make_sql_plan(
            [
                _make_sql_scan_step(),
                _make_sql_limit_step(),  # LIMIT 100
            ]
        )
        spark_plan = _make_spark_plan(
            [
                _make_spark_read_step(),
                SparkLimitStep(
                    step_type=SparkStepType.LIMIT,
                    input_alias="od",
                    limit=50,
                ),
            ]
        )

        comparator = PlanComparator()
        report = comparator.compare(sql_plan, spark_plan)

        assert report.status == ComparisonStatus.LOGIC_MISMATCH


# ════════════════════════════════════════════
# PlanComparator——Join 逻辑对比测试
# ════════════════════════════════════════════

class TestPlanComparatorCaseWhenEquivalence:
    """CaseWhen ↔ CaseWhen 逻辑等价性对比（Phase 7B 收口）。"""

    def test_case_when_not_equivalent_no_cases(self):
        """SQL 侧无 cases，Spark 侧有 branches → LOGIC_MISMATCH。"""
        from tianshu_datadev.planning.sql_build_plan import CaseWhenStep
        from tianshu_datadev.spark.models import (
            SparkCaseWhenBranch,
            SparkCaseWhenStep,
        )

        sql_plan = _make_sql_plan(
            [
                _make_sql_scan_step(),
                CaseWhenStep(
                    step_type="case_when",
                    step_id="step_cw_001",
                    cases=[],
                    else_value=None,
                    alias="label",
                ),
            ]
        )
        spark_plan = _make_spark_plan(
            [
                _make_spark_read_step(),
                SparkCaseWhenStep(
                    step_type=SparkStepType.CASE_WHEN,
                    input_alias="od",
                    output_alias="label",
                    branches=[SparkCaseWhenBranch(label="normal")],
                    else_value="other",
                ),
            ]
        )

        comparator = PlanComparator()
        report = comparator.compare(sql_plan, spark_plan)

        assert report.status == ComparisonStatus.LOGIC_MISMATCH

    def test_case_when_equivalent(self):
        """SQL cases 与 Spark branches 标签一致 → LOGIC_EQUIVALENT（无 condition，仅 labels）。"""
        from tianshu_datadev.planning.sql_build_plan import CaseWhenStep
        from tianshu_datadev.spark.models import (
            SparkCaseWhenBranch,
            SparkCaseWhenStep,
        )

        sql_plan = _make_sql_plan(
            [
                _make_sql_scan_step(),
                CaseWhenStep(
                    step_type="case_when",
                    step_id="step_cw_001",
                    cases=[
                        WhenBranch(
                            result=SqlLiteral(value="normal"),
                        ),
                    ],
                    else_value=SqlLiteral(value="other"),
                    alias="label",
                ),
            ]
        )
        spark_plan = _make_spark_plan(
            [
                _make_spark_read_step(),
                SparkCaseWhenStep(
                    step_type=SparkStepType.CASE_WHEN,
                    input_alias="od",
                    output_alias="label",
                    branches=[SparkCaseWhenBranch(label="normal")],
                    else_value="other",
                ),
            ]
        )

        comparator = PlanComparator()
        report = comparator.compare(sql_plan, spark_plan)

        assert report.status == ComparisonStatus.LOGIC_EQUIVALENT

    def test_case_when_alias_mismatch(self):
        """SQL alias 与 Spark output_alias 不一致 → LOGIC_MISMATCH。"""
        from tianshu_datadev.planning.sql_build_plan import CaseWhenStep
        from tianshu_datadev.spark.models import (
            SparkCaseWhenBranch,
            SparkCaseWhenStep,
        )

        sql_plan = _make_sql_plan(
            [
                _make_sql_scan_step(),
                CaseWhenStep(
                    step_type="case_when",
                    step_id="step_cw_001",
                    cases=[
                        WhenBranch(
                            condition=Predicate(
                                left=ColumnRef(
                                    table_ref="order_info",
                                    column_name="status",
                                    normalized_name="status",
                                ),
                                operator=PredicateOperator.EQ,
                                right=SqlLiteral(value="paid"),
                            ),
                            result=SqlLiteral(value="normal"),
                        ),
                    ],
                    else_value=SqlLiteral(value="other"),
                    alias="label",
                ),
            ]
        )
        spark_plan = _make_spark_plan(
            [
                _make_spark_read_step(),
                SparkCaseWhenStep(
                    step_type=SparkStepType.CASE_WHEN,
                    input_alias="od",
                    output_alias="wrong_label",
                    branches=[SparkCaseWhenBranch(label="normal")],
                    else_value="other",
                ),
            ]
        )

        comparator = PlanComparator()
        report = comparator.compare(sql_plan, spark_plan)

        assert report.status == ComparisonStatus.LOGIC_MISMATCH

    def test_case_when_alias_both_empty(self):
        """两侧 alias/output_alias 均为空字符串 → LOGIC_EQUIVALENT（边界行为，无 condition）。"""
        from tianshu_datadev.planning.sql_build_plan import CaseWhenStep
        from tianshu_datadev.spark.models import (
            SparkCaseWhenBranch,
            SparkCaseWhenStep,
        )

        sql_plan = _make_sql_plan(
            [
                _make_sql_scan_step(),
                CaseWhenStep(
                    step_type="case_when",
                    step_id="step_cw_001",
                    cases=[
                        WhenBranch(
                            result=SqlLiteral(value="normal"),
                        ),
                    ],
                    else_value=SqlLiteral(value="other"),
                    alias="",
                ),
            ]
        )
        spark_plan = _make_spark_plan(
            [
                _make_spark_read_step(),
                SparkCaseWhenStep(
                    step_type=SparkStepType.CASE_WHEN,
                    input_alias="od",
                    output_alias="",
                    branches=[SparkCaseWhenBranch(label="normal")],
                    else_value="other",
                ),
            ]
        )

        comparator = PlanComparator()
        report = comparator.compare(sql_plan, spark_plan)

        assert report.status == ComparisonStatus.LOGIC_EQUIVALENT

    def test_case_when_condition_triggers_unsupported(self):
        """CASE WHEN 含 condition → LOGIC_UNSUPPORTED。"""
        from tianshu_datadev.planning.sql_build_plan import CaseWhenStep
        from tianshu_datadev.spark.models import (
            SparkCaseWhenBranch,
            SparkCaseWhenStep,
        )

        cond = Predicate(
            left=ColumnRef(table_ref="od", column_name="amount",
                           normalized_name="amount"),
            operator=PredicateOperator.GT,
            right=SqlLiteral(value="100", is_sql_expr=False),
        )
        sql_cw = CaseWhenStep(
            step_type="case_when", step_id="step_cw_001",
            cases=[
                WhenBranch(condition=cond, result=SqlLiteral(value="high", is_sql_expr=False)),
            ],
            else_value=SqlLiteral(value="low", is_sql_expr=False),
            alias="level",
        )
        sql_plan = _make_sql_plan([_make_sql_scan_step(), sql_cw])

        # Spark 侧：相等的 labels
        spark_cw = SparkCaseWhenStep(
            step_type=SparkStepType.CASE_WHEN,
            input_alias="od", output_alias="level",
            branches=[SparkCaseWhenBranch(label="high")],
            else_value="low",
        )
        spark_plan = _make_spark_plan([_make_spark_read_step(), spark_cw])

        comparator = PlanComparator()
        report = comparator.compare(sql_plan, spark_plan)

        # condition 虽存在但 labels 相同 → UNSUPPORTED_COMPARISON（非 EQUIVALENT）
        cw_results = [r for r in report.step_results if r.step_type == "case_when"]
        assert len(cw_results) > 0
        assert cw_results[0].verdict == EquivalenceVerdict.UNSUPPORTED_COMPARISON
        # 检查状态传播：step UNSUPPORTED_COMPARISON → report LOGIC_UNSUPPORTED
        assert report.status == ComparisonStatus.LOGIC_UNSUPPORTED, (
            f"CASE WHEN condition 应使 report.status=LOGIC_UNSUPPORTED，"
            f"实际={report.status}"
        )

    def test_case_when_no_condition_still_equivalent(self):
        """无 condition 的 CASE WHEN（仅 labels）→ 不变，仍为 EQUIVALENT。"""
        from tianshu_datadev.planning.sql_build_plan import CaseWhenStep
        from tianshu_datadev.spark.models import (
            SparkCaseWhenBranch,
            SparkCaseWhenStep,
        )

        sql_plan = _make_sql_plan(
            [
                _make_sql_scan_step(),
                CaseWhenStep(
                    step_type="case_when",
                    step_id="step_cw_001",
                    cases=[
                        WhenBranch(
                            result=SqlLiteral(value="high"),
                        ),
                    ],
                    else_value=SqlLiteral(value="low"),
                    alias="level",
                ),
            ]
        )
        spark_plan = _make_spark_plan(
            [
                _make_spark_read_step(),
                SparkCaseWhenStep(
                    step_type=SparkStepType.CASE_WHEN,
                    input_alias="od",
                    output_alias="level",
                    branches=[SparkCaseWhenBranch(label="high")],
                    else_value="low",
                ),
            ]
        )

        comparator = PlanComparator()
        report = comparator.compare(sql_plan, spark_plan)

        assert report.status == ComparisonStatus.LOGIC_EQUIVALENT
