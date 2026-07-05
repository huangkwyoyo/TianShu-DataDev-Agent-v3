"""Phase 7B PlanComparator 测试——SQL Plan ↔ Spark Plan 逻辑链路对比。

覆盖：
- 8 种 step 类型（scan/filter/project/sort/limit/aggregate/join/case_when）逻辑对比
- window 类型标记 NOT_COVERED
- PlanComparisonReport 结构完整性
- PlanComparator 只读 SqlBuildPlan 结构化 artifact（不读 SQL 文本）
- 状态禁止泛化 PASS
"""

from __future__ import annotations

from tianshu_datadev.planning.models import (
    AggregateSpec,
    AggregationType,
    AliasExpr,
    ColumnRef,
    Predicate,
    PredicateOperator,
    SortDirection,
    SortSpec,
    SqlLiteral,
    WhenBranch,
)
from tianshu_datadev.planning.sql_build_plan import (
    FilterStep,
    LimitStep,
    ProjectStep,
    ScanStep,
    SortStep,
    SqlBuildPlan,
)
from tianshu_datadev.planning.sql_program import (
    SqlProgram,
    SqlStatement,
    StatementKind,
)
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
    SparkStepType,
)
from tianshu_datadev.spark.plan_comparator import (
    ComparisonStatus,
    PlanComparator,
)
from tianshu_datadev.spark.plan_equivalence import EquivalenceVerdict

# ════════════════════════════════════════════
# Fixtures——最小合法 SqlBuildPlan 和 SparkPlan
# ════════════════════════════════════════════


def _make_sql_scan_step(
    step_id: str = "step_scan_001",
    table_ref: str = "od",
) -> ScanStep:
    """构造最小合法 SQL ScanStep——table_ref 默认与 Spark alias 一致。"""
    return ScanStep(
        step_type="scan",
        step_id=step_id,
        table_ref=table_ref,
        required_columns=[
            ColumnRef(
                table_ref=table_ref,
                column_name="order_id",
                normalized_name="order_id",
            ),
            ColumnRef(
                table_ref=table_ref,
                column_name="amount",
                normalized_name="amount",
            ),
        ],
    )


def _make_sql_filter_step(step_id: str = "step_filter_001") -> FilterStep:
    """构造最小合法 SQL FilterStep。"""
    return FilterStep(
        step_type="filter",
        step_id=step_id,
        predicate=Predicate(
            left=ColumnRef(
                table_ref="order_info",
                column_name="amount",
                normalized_name="amount",
            ),
            operator=PredicateOperator.GT,
            right=ColumnRef(
                table_ref="order_info",
                column_name="threshold",
                normalized_name="threshold",
            ),
        ),
    )


def _make_sql_project_step(step_id: str = "step_project_001") -> ProjectStep:
    """构造最小合法 SQL ProjectStep。"""
    return ProjectStep(
        step_type="project",
        step_id=step_id,
        columns=[
            AliasExpr(
                expression=ColumnRef(
                    table_ref="order_info",
                    column_name="order_id",
                    normalized_name="order_id",
                ),
                alias="order_id",
            ),
            AliasExpr(
                expression=ColumnRef(
                    table_ref="order_info",
                    column_name="amount",
                    normalized_name="amount",
                ),
                alias="amount",
            ),
        ],
    )


def _make_sql_sort_step(step_id: str = "step_sort_001") -> SortStep:
    """构造最小合法 SQL SortStep。"""
    return SortStep(
        step_type="sort",
        step_id=step_id,
        order_by=[
            SortSpec(
                column="amount",
                direction=SortDirection.DESC,
            ),
        ],
    )


def _make_sql_limit_step(step_id: str = "step_limit_001") -> LimitStep:
    """构造最小合法 SQL LimitStep。"""
    return LimitStep(
        step_type="limit",
        step_id=step_id,
        limit=100,
    )


def _make_sql_plan(steps: list) -> SqlBuildPlan:
    """构造最小合法 SqlBuildPlan。"""
    spec_hash = "test_spec_hash_abc123"
    plan = SqlBuildPlan(
        plan_id=SqlBuildPlan.generate_plan_id(spec_hash),
        spec_hash=spec_hash,
        steps=steps,
    )
    return plan


def _make_spark_read_step(alias: str = "od") -> SparkReadStep:
    """构造最小合法 SparkReadStep。"""
    return SparkReadStep(
        step_type=SparkStepType.READ,
        alias=alias,
        source_name="order_info",
        input_key="order_info_key",
        required_columns=["order_id", "amount"],
    )


def _make_spark_filter_step(input_alias: str = "od") -> SparkFilterStep:
    """构造最小合法 SparkFilterStep。"""
    return SparkFilterStep(
        step_type=SparkStepType.FILTER,
        input_alias=input_alias,
        operator="GT",
        left="amount",
        right="threshold",
    )


def _make_spark_project_step(input_alias: str = "od") -> SparkProjectStep:
    """构造最小合法 SparkProjectStep。"""
    return SparkProjectStep(
        step_type=SparkStepType.PROJECT,
        input_alias=input_alias,
        columns=[
            SparkProjectColumn(column_name="order_id", alias="order_id"),
            SparkProjectColumn(column_name="amount", alias="amount"),
        ],
    )


def _make_spark_sort_step(input_alias: str = "od") -> SparkSortStep:
    """构造最小合法 SparkSortStep。"""
    return SparkSortStep(
        step_type=SparkStepType.SORT,
        input_alias=input_alias,
        order_by=[
            SparkSortSpec(column="amount", direction=SparkSortDirection.DESC),
        ],
    )


def _make_spark_limit_step(input_alias: str = "od") -> SparkLimitStep:
    """构造最小合法 SparkLimitStep。"""
    return SparkLimitStep(
        step_type=SparkStepType.LIMIT,
        input_alias=input_alias,
        limit=100,
    )


def _make_spark_plan(steps: list) -> SparkPlan:
    """构造最小合法 SparkPlan。"""
    contract_hash = "test_contract_hash_abc123"
    plan = SparkPlan(
        plan_id=SparkPlan.generate_plan_id(contract_hash),
        version="v1",
        source_phase="phase-5",
        source_contract_hash=contract_hash,
        source_contract_version="v1",
        steps=steps,
    )
    return plan


# ════════════════════════════════════════════
# PlanComparator——5 种基础类型逻辑对比测试
# ════════════════════════════════════════════


class TestPlanComparatorScanEquivalence:
    """Scan ↔ Read 逻辑等价性对比。"""

    def test_scan_read_equivalent(self):
        """相同表别名 → LOGIC_EQUIVALENT。"""
        sql_plan = _make_sql_plan([_make_sql_scan_step()])
        spark_plan = _make_spark_plan([_make_spark_read_step()])

        comparator = PlanComparator()
        report = comparator.compare(sql_plan, spark_plan)

        assert report.status == ComparisonStatus.LOGIC_EQUIVALENT
        assert any(
            r.step_type == "scan" and r.verdict == EquivalenceVerdict.EQUIVALENT
            for r in report.step_results
        )

    def test_scan_read_not_equivalent_different_alias(self):
        """不同表别名 → LOGIC_MISMATCH。"""
        sql_plan = _make_sql_plan([_make_sql_scan_step()])
        spark_plan = _make_spark_plan([
            _make_spark_read_step(alias="different_alias"),
        ])

        comparator = PlanComparator()
        report = comparator.compare(sql_plan, spark_plan)

        assert report.status == ComparisonStatus.LOGIC_MISMATCH


class TestPlanComparatorFilterEquivalence:
    """Filter 逻辑等价性对比。"""

    def test_filter_equivalent(self):
        """相同过滤条件 → LOGIC_EQUIVALENT。"""
        sql_plan = _make_sql_plan([
            _make_sql_scan_step(),
            _make_sql_filter_step(),
        ])
        spark_plan = _make_spark_plan([
            _make_spark_read_step(),
            _make_spark_filter_step(),
        ])

        comparator = PlanComparator()
        report = comparator.compare(sql_plan, spark_plan)

        assert report.status == ComparisonStatus.LOGIC_EQUIVALENT

    def test_filter_not_equivalent(self):
        """不同过滤操作符 → LOGIC_MISMATCH。"""
        sql_plan = _make_sql_plan([
            _make_sql_scan_step(),
            _make_sql_filter_step(),
        ])
        # Spark 侧用 EQ 而非 GT
        spark_plan = _make_spark_plan([
            _make_spark_read_step(),
            SparkFilterStep(
                step_type=SparkStepType.FILTER,
                input_alias="od",
                operator="EQ",
                left="amount",
                right="threshold",
            ),
        ])

        comparator = PlanComparator()
        report = comparator.compare(sql_plan, spark_plan)

        assert report.status == ComparisonStatus.LOGIC_MISMATCH

    def test_filter_between_equivalent_different_literal_formats(self):
        """BETWEEN 右值列表表示形式不同但值相同 → LOGIC_EQUIVALENT。

        SQL 侧 right 是 SqlLiteral 对象列表（model_dump 后为 dict 列表），
        Spark 侧 right 是 Python repr 字符串（Mapper 直传 ContractPredicate.right）。
        两种形式在语义上等价——Comparator 应归一化后判定为等价。
        """
        from tianshu_datadev.planning.models import Predicate, SqlLiteral

        # SQL 侧：BETWEEN 右值为 SqlLiteral 对象列表
        sql_predicate = Predicate(
            left=ColumnRef(
                table_ref="ft",
                column_name="pickup_date_key",
                normalized_name="pickup_date_key",
            ),
            operator=PredicateOperator.BETWEEN,
            right=[
                SqlLiteral(value="20260101", is_sql_expr=False),
                SqlLiteral(value="20260331", is_sql_expr=False),
            ],
        )
        sql_filter = FilterStep(
            step_type="filter",
            step_id="step_filter_between",
            predicate=sql_predicate,
        )

        # Spark 侧：BETWEEN 右值为 Python repr 字符串（模拟 Mapper 产出）
        spark_filter = SparkFilterStep(
            step_type=SparkStepType.FILTER,
            input_alias="ft",
            operator="BETWEEN",
            left="ft.pickup_date_key",
            right="[SqlLiteral(value='20260101', is_sql_expr=False),"
                  " SqlLiteral(value='20260331', is_sql_expr=False)]",
        )

        sql_plan = _make_sql_plan([
            ScanStep(
                step_type="scan",
                step_id="scan_ft",
                table_ref="ft",
                required_columns=[
                    ColumnRef(
                        table_ref="ft",
                        column_name="pickup_date_key",
                        normalized_name="pickup_date_key",
                    ),
                ],
            ),
            sql_filter,
        ])
        spark_plan = _make_spark_plan([
            SparkReadStep(
                step_type=SparkStepType.READ,
                alias="ft",
                source_name="fact_trips",
                input_key="fact_trips_key",
            ),
            spark_filter,
        ])

        comparator = PlanComparator()
        report = comparator.compare(sql_plan, spark_plan)

        # 关键断言：BETWEEN 右值只是表示形式不同（dict vs SqlLiteral repr），
        # 值相同 → 应判定为 LOGIC_EQUIVALENT
        assert report.status == ComparisonStatus.LOGIC_EQUIVALENT, (
            f"BETWEEN 右值不同表示形式应归一化后等价，"
            f"实际 status={report.status}，"
            f"filter_result={[(r.step_type, r.verdict.value, r.detail[:100]) for r in report.step_results]}"
        )


class TestPlanComparatorProjectEquivalence:
    """Project 逻辑等价性对比。"""

    def test_project_equivalent(self):
        """相同投影列 → LOGIC_EQUIVALENT。"""
        sql_plan = _make_sql_plan([
            _make_sql_scan_step(),
            _make_sql_project_step(),
        ])
        spark_plan = _make_spark_plan([
            _make_spark_read_step(),
            _make_spark_project_step(),
        ])

        comparator = PlanComparator()
        report = comparator.compare(sql_plan, spark_plan)

        assert report.status == ComparisonStatus.LOGIC_EQUIVALENT

    def test_project_not_equivalent(self):
        """投影列不一致 → LOGIC_MISMATCH。"""
        sql_plan = _make_sql_plan([
            _make_sql_scan_step(),
            _make_sql_project_step(),
        ])
        spark_plan = _make_spark_plan([
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
        ])

        comparator = PlanComparator()
        report = comparator.compare(sql_plan, spark_plan)

        assert report.status == ComparisonStatus.LOGIC_MISMATCH


class TestPlanComparatorSortEquivalence:
    """Sort 逻辑等价性对比。"""

    def test_sort_equivalent(self):
        """相同排序规格 → LOGIC_EQUIVALENT。"""
        sql_plan = _make_sql_plan([
            _make_sql_scan_step(),
            _make_sql_sort_step(),
        ])
        spark_plan = _make_spark_plan([
            _make_spark_read_step(),
            _make_spark_sort_step(),
        ])

        comparator = PlanComparator()
        report = comparator.compare(sql_plan, spark_plan)

        assert report.status == ComparisonStatus.LOGIC_EQUIVALENT

    def test_sort_not_equivalent(self):
        """排序方向不一致 → LOGIC_MISMATCH。"""
        sql_plan = _make_sql_plan([
            _make_sql_scan_step(),
            _make_sql_sort_step(),  # DESC
        ])
        spark_plan = _make_spark_plan([
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
        ])

        comparator = PlanComparator()
        report = comparator.compare(sql_plan, spark_plan)

        assert report.status == ComparisonStatus.LOGIC_MISMATCH


class TestPlanComparatorLimitEquivalence:
    """Limit 逻辑等价性对比。"""

    def test_limit_equivalent(self):
        """相同 limit 值 → LOGIC_EQUIVALENT。"""
        sql_plan = _make_sql_plan([
            _make_sql_scan_step(),
            _make_sql_limit_step(),
        ])
        spark_plan = _make_spark_plan([
            _make_spark_read_step(),
            _make_spark_limit_step(),
        ])

        comparator = PlanComparator()
        report = comparator.compare(sql_plan, spark_plan)

        assert report.status == ComparisonStatus.LOGIC_EQUIVALENT

    def test_limit_not_equivalent(self):
        """不同 limit 值 → LOGIC_MISMATCH。"""
        sql_plan = _make_sql_plan([
            _make_sql_scan_step(),
            _make_sql_limit_step(),  # LIMIT 100
        ])
        spark_plan = _make_spark_plan([
            _make_spark_read_step(),
            SparkLimitStep(
                step_type=SparkStepType.LIMIT,
                input_alias="od",
                limit=50,
            ),
        ])

        comparator = PlanComparator()
        report = comparator.compare(sql_plan, spark_plan)

        assert report.status == ComparisonStatus.LOGIC_MISMATCH


# ════════════════════════════════════════════
# PlanComparator——Join 逻辑对比测试
# ════════════════════════════════════════════


class TestPlanComparatorJoinEquivalence:
    """Join ↔ Join 逻辑等价性对比（Phase 7B 收口）。"""

    def test_join_not_equivalent_different_aliases(self):
        """不同别名 → LOGIC_MISMATCH（SQL 侧用 table_ref，Spark 侧用 alias）。"""
        from tianshu_datadev.planning.sql_build_plan import JoinStep
        from tianshu_datadev.spark.models import SparkJoinStep, SparkJoinType

        sql_plan = _make_sql_plan([
            _make_sql_scan_step(),
            JoinStep(
                step_type="join",
                step_id="step_join_001",
                right_table_ref="user_profile",
                join_type="INNER",
                join_keys=[
                    (
                        ColumnRef(
                            table_ref="order_info",
                            column_name="user_id",
                            normalized_name="user_id",
                        ),
                        ColumnRef(
                            table_ref="user_profile",
                            column_name="user_id",
                            normalized_name="user_id",
                        ),
                    ),
                ],
                relationship_ref="rel_001",
            ),
        ])
        spark_plan = _make_spark_plan([
            _make_spark_read_step(),
            SparkJoinStep(
                step_type=SparkStepType.JOIN,
                left_alias="od",
                right_alias="up",
                left_key="user_id",
                right_key="user_id",
                join_type=SparkJoinType.INNER,
            ),
        ])

        comparator = PlanComparator()
        report = comparator.compare(sql_plan, spark_plan)

        # SQL left_table_ref="order_info" ≠ Spark left_alias="od" → LOGIC_MISMATCH
        assert report.status == ComparisonStatus.LOGIC_MISMATCH
        assert "join" in [r.step_type for r in report.step_results]

    def test_join_equivalent_same_keys(self):
        """相同 join 键和类型 → LOGIC_EQUIVALENT。"""
        from tianshu_datadev.planning.sql_build_plan import JoinStep
        from tianshu_datadev.spark.models import SparkJoinStep, SparkJoinType

        sql_plan = _make_sql_plan([
            _make_sql_scan_step(),
            JoinStep(
                step_type="join",
                step_id="step_join_001",
                right_table_ref="od",
                join_type="INNER",
                join_keys=[
                    (
                        ColumnRef(
                            table_ref="od",
                            column_name="user_id",
                            normalized_name="user_id",
                        ),
                        ColumnRef(
                            table_ref="od",
                            column_name="user_id",
                            normalized_name="user_id",
                        ),
                    ),
                ],
                relationship_ref="rel_001",
            ),
        ])
        spark_plan = _make_spark_plan([
            _make_spark_read_step(),
            SparkJoinStep(
                step_type=SparkStepType.JOIN,
                left_alias="od",
                right_alias="od",
                left_key="user_id",
                right_key="user_id",
                join_type=SparkJoinType.INNER,
            ),
        ])

        comparator = PlanComparator()
        report = comparator.compare(sql_plan, spark_plan)

        assert report.status == ComparisonStatus.LOGIC_EQUIVALENT


# ════════════════════════════════════════════
# PlanComparator——Aggregate 逻辑对比测试
# ════════════════════════════════════════════


class TestPlanComparatorAggregateEquivalence:
    """Aggregate ↔ Aggregate 逻辑等价性对比（Phase 7B 收口）。"""

    def test_aggregate_not_equivalent_different_metrics(self):
        """SQL 侧无 metrics，Spark 侧有 metrics → LOGIC_MISMATCH。"""
        from tianshu_datadev.planning.sql_build_plan import AggregateStep
        from tianshu_datadev.spark.models import (
            SparkAggFunction,
            SparkAggregateSpec,
            SparkAggregateStep,
        )

        sql_plan = _make_sql_plan([
            _make_sql_scan_step(),
            AggregateStep(
                step_type="aggregate",
                step_id="step_agg_001",
                group_keys=[
                    ColumnRef(
                        table_ref="order_info",
                        column_name="region",
                        normalized_name="region",
                    ),
                ],
                metrics=[],
            ),
        ])
        spark_plan = _make_spark_plan([
            _make_spark_read_step(),
            SparkAggregateStep(
                step_type=SparkStepType.AGGREGATE,
                input_alias="od",
                group_keys=["region"],
                metrics=[
                    SparkAggregateSpec(
                        function=SparkAggFunction.COUNT,
                        input_column=None,
                        alias="cnt",
                    ),
                ],
            ),
        ])

        comparator = PlanComparator()
        report = comparator.compare(sql_plan, spark_plan)

        assert report.status == ComparisonStatus.LOGIC_MISMATCH

    def test_aggregate_equivalent(self):
        """相同 group_keys 和 metrics → LOGIC_EQUIVALENT。"""
        from tianshu_datadev.planning.sql_build_plan import AggregateStep
        from tianshu_datadev.spark.models import (
            SparkAggFunction,
            SparkAggregateSpec,
            SparkAggregateStep,
        )

        sql_plan = _make_sql_plan([
            _make_sql_scan_step(),
            AggregateStep(
                step_type="aggregate",
                step_id="step_agg_001",
                group_keys=[
                    ColumnRef(
                        table_ref="order_info",
                        column_name="region",
                        normalized_name="region",
                    ),
                ],
                metrics=[
                    AggregateSpec(
                        aggregation=AggregationType.COUNT,
                        input_column=None,
                        alias="cnt",
                    ),
                ],
            ),
        ])
        spark_plan = _make_spark_plan([
            _make_spark_read_step(),
            SparkAggregateStep(
                step_type=SparkStepType.AGGREGATE,
                input_alias="od",
                group_keys=["region"],
                metrics=[
                    SparkAggregateSpec(
                        function=SparkAggFunction.COUNT,
                        input_column=None,
                        alias="cnt",
                    ),
                ],
            ),
        ])

        comparator = PlanComparator()
        report = comparator.compare(sql_plan, spark_plan)

        assert report.status == ComparisonStatus.LOGIC_EQUIVALENT


# ════════════════════════════════════════════
# PlanComparator——CaseWhen 逻辑对比测试
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

        sql_plan = _make_sql_plan([
            _make_sql_scan_step(),
            CaseWhenStep(
                step_type="case_when",
                step_id="step_cw_001",
                cases=[],
                else_value=None,
                alias="label",
            ),
        ])
        spark_plan = _make_spark_plan([
            _make_spark_read_step(),
            SparkCaseWhenStep(
                step_type=SparkStepType.CASE_WHEN,
                input_alias="od",
                output_alias="label",
                branches=[SparkCaseWhenBranch(label="normal")],
                else_value="other",
            ),
        ])

        comparator = PlanComparator()
        report = comparator.compare(sql_plan, spark_plan)

        assert report.status == ComparisonStatus.LOGIC_MISMATCH

    def test_case_when_equivalent(self):
        """SQL cases 与 Spark branches 标签一致 → LOGIC_EQUIVALENT。"""
        from tianshu_datadev.planning.sql_build_plan import CaseWhenStep
        from tianshu_datadev.spark.models import (
            SparkCaseWhenBranch,
            SparkCaseWhenStep,
        )

        sql_plan = _make_sql_plan([
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
        ])
        spark_plan = _make_spark_plan([
            _make_spark_read_step(),
            SparkCaseWhenStep(
                step_type=SparkStepType.CASE_WHEN,
                input_alias="od",
                output_alias="label",
                branches=[SparkCaseWhenBranch(label="normal")],
                else_value="other",
            ),
        ])

        comparator = PlanComparator()
        report = comparator.compare(sql_plan, spark_plan)

        assert report.status == ComparisonStatus.LOGIC_EQUIVALENT


# ════════════════════════════════════════════
# PlanComparator——NOT_COVERED 标记测试（仅 window）
# ════════════════════════════════════════════


class TestPlanComparatorNotCovered:
    """Phase 7B 未覆盖类型 → NOT_COVERED（仅 window，Phase 6C 覆盖）。"""

    def test_window_not_in_enabled_types(self):
        """Window 类型 → NOT_COVERED。"""
        from tianshu_datadev.planning.sql_build_plan import WindowStep
        from tianshu_datadev.spark.models import (
            SparkWindowExpr,
            SparkWindowFunction,
            SparkWindowStep,
        )

        sql_plan = _make_sql_plan([
            _make_sql_scan_step(),
            WindowStep(
                step_type="window",
                step_id="step_window_001",
                window_exprs=[],
            ),
        ])
        spark_plan = _make_spark_plan([
            _make_spark_read_step(),
            SparkWindowStep(
                step_type=SparkStepType.WINDOW,
                input_alias="od",
                expressions=[
                    SparkWindowExpr(
                        function=SparkWindowFunction.ROW_NUMBER,
                        alias="rn",
                        partition_by=["region"],
                        order_by=["amount"],
                    ),
                ],
            ),
        ])

        comparator = PlanComparator()
        report = comparator.compare(sql_plan, spark_plan)

        assert report.status == ComparisonStatus.NOT_COVERED
        assert "window" in report.uncovered_step_types


# ════════════════════════════════════════════
# PlanComparator——混合场景
# ════════════════════════════════════════════


class TestPlanComparatorMixedScenarios:
    """混合 step 类型场景——部分已覆盖 + 部分未覆盖（Phase 7B）。"""

    def test_covered_steps_with_uncovered_window(
        self,
    ):
        """已覆盖部分 + window 未覆盖 → NOT_COVERED。"""
        from tianshu_datadev.planning.sql_build_plan import WindowStep
        from tianshu_datadev.spark.models import (
            SparkWindowExpr,
            SparkWindowFunction,
            SparkWindowStep,
        )

        # SQL 侧：scan + filter（已覆盖）+ window（未覆盖）
        sql_plan = _make_sql_plan([
            _make_sql_scan_step(),
            _make_sql_filter_step(),
            WindowStep(
                step_type="window",
                step_id="step_window_001",
                window_exprs=[],
            ),
        ])
        # Spark 侧：read + filter（已覆盖）+ window（未覆盖）
        spark_plan = _make_spark_plan([
            _make_spark_read_step(),
            _make_spark_filter_step(),
            SparkWindowStep(
                step_type=SparkStepType.WINDOW,
                input_alias="od",
                expressions=[
                    SparkWindowExpr(
                        function=SparkWindowFunction.ROW_NUMBER,
                        alias="rn",
                        partition_by=["region"],
                        order_by=["amount"],
                    ),
                ],
            ),
        ])

        comparator = PlanComparator()
        report = comparator.compare(sql_plan, spark_plan)

        # 已覆盖部分等价 → 但存在未覆盖类型 → NOT_COVERED
        assert report.status == ComparisonStatus.NOT_COVERED
        assert "window" in report.uncovered_step_types
        # 已覆盖部分应等价
        covered_results = [
            r for r in report.step_results
            if r.step_type in ("scan", "filter")
        ]
        assert all(
            r.verdict == EquivalenceVerdict.EQUIVALENT
            for r in covered_results
        )

    def test_all_covered_but_mismatched_join(self):
        """全部已覆盖（含 join），但 join 别名不匹配 → LOGIC_MISMATCH。"""
        from tianshu_datadev.planning.sql_build_plan import JoinStep
        from tianshu_datadev.spark.models import SparkJoinStep, SparkJoinType

        sql_plan = _make_sql_plan([
            JoinStep(
                step_type="join",
                step_id="step_join_001",
                right_table_ref="user_profile",
                join_type="INNER",
                join_keys=[
                    (
                        ColumnRef(
                            table_ref="order_info",
                            column_name="user_id",
                            normalized_name="user_id",
                        ),
                        ColumnRef(
                            table_ref="user_profile",
                            column_name="user_id",
                            normalized_name="user_id",
                        ),
                    ),
                ],
                relationship_ref="rel_001",
            ),
        ])
        spark_plan = _make_spark_plan([
            SparkJoinStep(
                step_type=SparkStepType.JOIN,
                left_alias="od",
                right_alias="up",
                left_key="user_id",
                right_key="user_id",
                join_type=SparkJoinType.INNER,
            ),
        ])

        comparator = PlanComparator()
        report = comparator.compare(sql_plan, spark_plan)

        # join 在 Phase 7B 已覆盖——left_table_ref="order_info" ≠ left_alias="od"
        assert report.status == ComparisonStatus.LOGIC_MISMATCH

    def test_empty_both_sides(self):
        """双方均为空 steps——对比规则不支持（UNSUPPORTED_COMPARISON）。"""
        sql_plan = _make_sql_plan([])
        spark_plan = _make_spark_plan([])

        comparator = PlanComparator()
        report = comparator.compare(sql_plan, spark_plan)

        # 空 steps → 没有可对比的 → LOGIC_UNSUPPORTED（对比规则不支持空集）
        assert report.status == ComparisonStatus.LOGIC_UNSUPPORTED


# ════════════════════════════════════════════
# PlanComparisonReport 结构测试
# ════════════════════════════════════════════


class TestPlanComparisonReportStructure:
    """PlanComparisonReport 结构完整性测试。"""

    def test_report_contains_all_required_fields(self):
        """报告包含所有必要字段。"""
        sql_plan = _make_sql_plan([_make_sql_scan_step()])
        spark_plan = _make_spark_plan([_make_spark_read_step()])

        comparator = PlanComparator()
        report = comparator.compare(sql_plan, spark_plan)

        # 所有字段非空
        assert report.report_id
        assert report.contract_hash
        assert report.sql_plan_hash
        assert report.spark_plan_hash
        assert report.status in ComparisonStatus
        assert isinstance(report.step_results, list)
        assert isinstance(report.unsupported_types, list)
        assert isinstance(report.uncovered_step_types, list)
        assert isinstance(report.annotation_warnings, list)

    def test_report_id_is_deterministic(self):
        """相同输入 → 相同 report_id。"""
        sql_plan = _make_sql_plan([_make_sql_scan_step()])
        spark_plan = _make_spark_plan([_make_spark_read_step()])

        comparator = PlanComparator()
        report1 = comparator.compare(sql_plan, spark_plan)
        report2 = comparator.compare(sql_plan, spark_plan)

        assert report1.report_id == report2.report_id

    def test_status_not_generic_pass(self):
        """状态不包含泛化 "PASS" 字符串。"""
        sql_plan = _make_sql_plan([_make_sql_scan_step()])
        spark_plan = _make_spark_plan([_make_spark_read_step()])

        comparator = PlanComparator()
        report = comparator.compare(sql_plan, spark_plan)

        # 状态值不得为 "PASS"
        assert report.status.value != "PASS"
        assert "PASS" not in report.status.value

    def test_uncovered_types_marked(self):
        """未覆盖类型在 uncovered_step_types 中正确标记——window 应在列表中。"""
        from tianshu_datadev.planning.sql_build_plan import WindowStep
        from tianshu_datadev.spark.models import (
            SparkWindowExpr,
            SparkWindowFunction,
            SparkWindowStep,
        )

        sql_plan = _make_sql_plan([
            _make_sql_scan_step(),
            WindowStep(
                step_type="window",
                step_id="step_window_001",
                window_exprs=[],
            ),
        ])
        spark_plan = _make_spark_plan([
            _make_spark_read_step(),
            SparkWindowStep(
                step_type=SparkStepType.WINDOW,
                input_alias="od",
                expressions=[
                    SparkWindowExpr(
                        function=SparkWindowFunction.ROW_NUMBER,
                        alias="rn",
                        partition_by=["region"],
                        order_by=["amount"],
                    ),
                ],
            ),
        ])

        comparator = PlanComparator()
        report = comparator.compare(sql_plan, spark_plan)

        assert report.status == ComparisonStatus.NOT_COVERED
        assert "window" in report.uncovered_step_types


# ════════════════════════════════════════════
# PlanComparator——自定义启用类型
# ════════════════════════════════════════════


class TestPlanComparatorCustomEnabledTypes:
    """自定义启用类型覆盖默认 Phase 7A 范围。"""

    def test_custom_enabled_types(self):
        """允许覆盖默认启用类型。"""
        sql_plan = _make_sql_plan([_make_sql_scan_step()])
        spark_plan = _make_spark_plan([_make_spark_read_step()])

        # 启用空集——所有类型都标记 NOT_COVERED（未启用任何对比）
        comparator = PlanComparator(enabled_step_types=set())
        report = comparator.compare(sql_plan, spark_plan)

        assert report.status == ComparisonStatus.NOT_COVERED

    def test_custom_enabled_all_types(self):
        """启用所有 Phase 7B 已支持扁平化的 8 种类型。

        scan/filter/project/sort/limit/aggregate/join/case_when。
        window 的扁平化属于 Phase 6C 范围。
        """
        from tianshu_datadev.planning.sql_build_plan import (
            AggregateStep,
            CaseWhenStep,
            JoinStep,
        )
        from tianshu_datadev.spark.models import (
            SparkAggFunction,
            SparkAggregateSpec,
            SparkAggregateStep,
            SparkCaseWhenBranch,
            SparkCaseWhenStep,
            SparkJoinStep,
            SparkJoinType,
        )

        sql_plan = _make_sql_plan([
            _make_sql_scan_step(),
            _make_sql_filter_step(),
            _make_sql_project_step(),
            _make_sql_sort_step(),
            _make_sql_limit_step(),
            AggregateStep(
                step_type="aggregate",
                step_id="step_agg_001",
                group_keys=[
                    ColumnRef(
                        table_ref="order_info",
                        column_name="region",
                        normalized_name="region",
                    ),
                ],
                metrics=[
                    AggregateSpec(
                        aggregation=AggregationType.COUNT,
                        input_column=None,
                        alias="cnt",
                    ),
                ],
            ),
            JoinStep(
                step_type="join",
                step_id="step_join_001",
                right_table_ref="od",
                join_type="INNER",
                join_keys=[
                    (
                        ColumnRef(
                            table_ref="od", column_name="user_id",
                            normalized_name="user_id",
                        ),
                        ColumnRef(
                            table_ref="od", column_name="user_id",
                            normalized_name="user_id",
                        ),
                    ),
                ],
                relationship_ref="rel_001",
            ),
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
        ])
        spark_plan = _make_spark_plan([
            _make_spark_read_step(),
            _make_spark_filter_step(),
            _make_spark_project_step(),
            _make_spark_sort_step(),
            _make_spark_limit_step(),
            SparkAggregateStep(
                step_type=SparkStepType.AGGREGATE,
                input_alias="od",
                group_keys=["region"],
                metrics=[
                    SparkAggregateSpec(
                        function=SparkAggFunction.COUNT,
                        input_column=None,
                        alias="cnt",
                    ),
                ],
            ),
            SparkJoinStep(
                step_type=SparkStepType.JOIN,
                left_alias="od",
                right_alias="od",
                left_key="user_id",
                right_key="user_id",
                join_type=SparkJoinType.INNER,
            ),
            SparkCaseWhenStep(
                step_type=SparkStepType.CASE_WHEN,
                input_alias="od",
                output_alias="label",
                branches=[SparkCaseWhenBranch(label="normal")],
                else_value="other",
            ),
        ])

        # 启用所有 8 种 Phase 7B 类型
        all_types = {
            "scan", "filter", "project", "sort", "limit",
            "aggregate", "join", "case_when",
        }
        comparator = PlanComparator(enabled_step_types=all_types)
        report = comparator.compare(sql_plan, spark_plan)

        # 全部在对比范围内且等价
        assert report.status == ComparisonStatus.LOGIC_EQUIVALENT
        assert len(report.uncovered_step_types) == 0


# ════════════════════════════════════════════
# C3 集成链路——同一 Contract 驱动双管线 → Comparator
# ════════════════════════════════════════════


def _contract_to_sql_steps(
    contract,  # DataTransformContractV1——类型注释在函数内部延迟导入
) -> list:
    """从 DataTransformContractV1 确定性构造 SqlBuildPlan step 列表——C3 双管线桥接。

    已生产化到 src/tianshu_datadev/spark/contract_sql_bridge.py，保留用于向后兼容。
    功能与 contract_to_sql_steps() 完全一致，新代码请直接导入生产模块使用。

    映射规则（与 Mapper 的 SparkPlan 映射对称）：
    - input_tables → ScanStep（每表一个）
    - filters → FilterStep
    - join_relationships → JoinStep
    - aggregations + grouping_keys → AggregateStep
    - case_when_labels → CaseWhenStep（标签级，不含完整谓词）
    - output_columns → ProjectStep
    - sort_spec → SortStep
    - limit_spec → LimitStep

    不映射 window_specs——Phase 7B Comparator 不启用 window。
    """
    from tianshu_datadev.planning.models import (
        AggregateSpec,
        AggregationType,
        AliasExpr,
        ColumnRef,
        Predicate,
        PredicateOperator,
        SortDirection,
        SortSpec,
        SqlLiteral,
        WhenBranch,
    )
    from tianshu_datadev.planning.sql_build_plan import (
        AggregateStep,
        CaseWhenStep,
        FilterStep,
        JoinStep,
        LimitStep,
        ProjectStep,
        ScanStep,
        SortStep,
    )

    steps: list = []

    # 1. input_tables → ScanStep
    for tbl in contract.input_tables:
        steps.append(ScanStep(
            step_type="scan",
            step_id=f"scan_{tbl.table_ref}",
            table_ref=tbl.table_ref,
            required_columns=[],  # 测试级——Comparator 不逐列对比 required_columns
        ))

    # 2. filters → FilterStep
    for i, f in enumerate(contract.filters):
        left_parts = f.left.split(".", 1)
        left_col = left_parts[1] if len(left_parts) > 1 else left_parts[0]
        left_table = left_parts[0] if len(left_parts) > 1 else ""
        steps.append(FilterStep(
            step_type="filter",
            step_id=f"filter_{i:03d}",
            predicate=Predicate(
                left=ColumnRef(table_ref=left_table, column_name=left_col, normalized_name=left_col),
                operator=PredicateOperator(f.operator),
                right=SqlLiteral(value=f.right.strip("'").strip('"')),
            ),
        ))

    # 3. join_relationships → JoinStep
    for j in contract.join_relationships:
        steps.append(JoinStep(
            step_type="join",
            step_id=f"join_{j.join_id}",
            right_table_ref=j.right_table,
            join_type=j.join_type,
            join_keys=[(
                ColumnRef(table_ref=j.left_table, column_name=j.left_key, normalized_name=j.left_key),
                ColumnRef(table_ref=j.right_table, column_name=j.right_key, normalized_name=j.right_key),
            )],
            relationship_ref=j.join_id,
        ))

    # 4. aggregations + grouping_keys → AggregateStep
    if contract.aggregations:
        agg_metrics = []
        for agg in contract.aggregations:
            agg_type = AggregationType(agg.function) if agg.function else AggregationType.COUNT
            # 从 "od.amount" 提取列名 "amount"
            raw_col = agg.input_column or ""
            col_name = raw_col.split(".", 1)[1] if "." in raw_col else raw_col
            agg_metrics.append(AggregateSpec(
                aggregation=agg_type,
                input_column=col_name,
                alias=agg.alias,
            ))
        group_keys = []
        for gk in contract.grouping_keys:
            parts = gk.split(".", 1)
            col = parts[1] if len(parts) > 1 else parts[0]
            tbl = parts[0] if len(parts) > 1 else ""
            group_keys.append(ColumnRef(table_ref=tbl, column_name=col, normalized_name=col))
        steps.append(AggregateStep(
            step_type="aggregate",
            step_id="agg_001",
            group_keys=group_keys,
            metrics=agg_metrics,
        ))

    # 5. case_when_labels → CaseWhenStep（标签级——完整谓词在 Phase 7 做）
    if contract.case_when_labels:
        for cw in contract.case_when_labels:
            cases = []
            for label in cw.labels:
                cases.append(WhenBranch(
                    condition=Predicate(
                        left=ColumnRef(table_ref="", column_name="", normalized_name=""),
                        operator=PredicateOperator.EQ,
                        right=SqlLiteral(value=""),
                    ),
                    result=SqlLiteral(value=label),
                ))
            steps.append(CaseWhenStep(
                step_type="case_when",
                step_id=f"cw_{cw.statement_id}",
                cases=cases,
                else_value=SqlLiteral(value=cw.else_label) if cw.else_label else None,
                alias=cw.output_alias,
            ))

    # 6. output_columns → ProjectStep
    if contract.output_columns:
        proj_cols = []
        for oc in contract.output_columns:
            col_ref = ColumnRef(
                table_ref="", column_name=oc.column_name,
                normalized_name=oc.column_name,
            )
            proj_cols.append(AliasExpr(expression=col_ref, alias=oc.alias))
        steps.append(ProjectStep(
            step_type="project",
            step_id="proj_001",
            columns=proj_cols,
        ))

    # 7. sort_spec → SortStep
    if contract.sort_spec:
        sort_specs = []
        for s in contract.sort_spec:
            sort_specs.append(SortSpec(
                column=s.column,
                direction=SortDirection(s.direction),
            ))
        steps.append(SortStep(
            step_type="sort",
            step_id="sort_001",
            order_by=sort_specs,
        ))

    # 8. limit_spec → LimitStep
    if contract.limit_spec:
        steps.append(LimitStep(
            step_type="limit",
            step_id="limit_001",
            limit=contract.limit_spec.limit,
        ))

    return steps


class TestPlanComparatorContractIntegration:
    """C3 集成链路：同一 DataTransformContractV1 → 双管线自动产出 → Comparator。

    Spark 管线：Contract → Mapper（map_contract_to_spark_plan）→ SparkPlan
    SQL 管线：  Contract → _contract_to_sql_steps() 桥接 → SqlBuildPlan

    桥接函数是测试内的最小确定性映射——不修改生产代码。
    后续若生产代码提供 Contract → SqlBuildPlan 的正式路径，替换此桥接即可。
    """

    def test_contract_to_spark_via_mapper_then_compare_all_eight_types(
        self,
    ):
        """同一 Contract → Mapper + 手工 SqlBuildPlan → Comparator——8 种已启用类型全部等价。

        覆盖 scan/filter/project/sort/limit/aggregate/join/case_when（Phase 7B 启用）。
        window 在 Phase 7B 为 NOT_COVERED——后续 Phase 覆盖。
        """
        from tianshu_datadev.artifacts.models import (
            CaseWhenLabelSpec,
            ContractAggregation,
            ContractInputTable,
            ContractJoin,
            ContractLimit,
            ContractOutputColumn,
            ContractPredicate,
            ContractSort,
            DataTransformContractV1,
        )
        from tianshu_datadev.spark.mapper import map_contract_to_spark_plan

        # ── Step 1: 构造覆盖 9 种 step 的 Contract ──
        program_id = "prog_c3_integration"
        contract_id = DataTransformContractV1.generate_contract_id(program_id)
        contract = DataTransformContractV1(
            contract_id=contract_id,
            version="v1",
            source_phase="phase-3",
            source_sqlprogram_hash=program_id,
            input_tables=[
                ContractInputTable(
                    table_ref="od",
                    source_table="dwd.order_detail",
                ),
                ContractInputTable(
                    table_ref="ri",
                    source_table="dim.region_info",
                ),
            ],
            join_relationships=[
                ContractJoin(
                    join_id="join_od_ri",
                    left_table="od",
                    right_table="ri",
                    left_key="region_code",
                    right_key="region_code",
                    join_type="INNER",
                    evidence_chain={
                        "level": "STRONG",
                        "action": "ACCEPT",
                        "left_field": {"raw": "region_code", "normalized": "region_code"},
                        "right_field": {"raw": "region_code", "normalized": "region_code"},
                        "evidence_checks": {
                            "exact_name_match": True, "type_match": True, "unique_match": True,
                        },
                    },
                    level="STRONG",
                ),
            ],
            filters=[
                ContractPredicate(operator="GT", left="od.amount", right="0"),
            ],
            aggregations=[
                ContractAggregation(function="SUM", input_column="od.amount", alias="total_amt"),
            ],
            grouping_keys=["od.region_code"],
            output_columns=[
                ContractOutputColumn(column_name="region_code", alias="region_code"),
                ContractOutputColumn(column_name="total_amt", alias="total_amt"),
            ],
            output_grain=["region_code"],
            sort_spec=[ContractSort(column="total_amt", direction="DESC")],
            limit_spec=ContractLimit(limit=100),
            business_keys=["region_code"],
            step_dag={"stmt_main": []},
            temp_tables=[],
            case_when_labels=[
                CaseWhenLabelSpec(
                    statement_id="stmt_label",
                    output_alias="value_level",
                    branch_count=2,
                    labels=["high", "low"],
                    else_label="mid",
                ),
            ],
            window_specs=[],
        )

        # ── Step 2: Spark 管线——Contract → Mapper → SparkPlan ──
        mapping_result = map_contract_to_spark_plan(contract)
        assert mapping_result.success, (
            f"Mapper 应成功映射，实际失败：gaps={mapping_result.gaps}, "
            f"unsupported={mapping_result.unsupported}"
        )
        spark_plan = mapping_result.spark_plan
        assert spark_plan is not None

        # ── Step 3: SQL 管线——Contract → _contract_to_sql_steps() 桥接 → SqlBuildPlan ──
        sql_steps = _contract_to_sql_steps(contract)

        # 验证生产模块函数与本地桥接函数产出相同的 steps
        from tianshu_datadev.spark.contract_sql_bridge import (
            contract_to_sql_steps as prod_contract_to_sql_steps,
        )
        prod_steps = prod_contract_to_sql_steps(contract)
        assert prod_steps == sql_steps, (
            "生产模块 contract_to_sql_steps() 应与本地 _contract_to_sql_steps() 产出一致"
        )

        sql_plan = _make_sql_plan(sql_steps)

        # ── Step 4: Comparator 对比 ──
        comparator = PlanComparator()
        report = comparator.compare(sql_plan, spark_plan)

        # ── Step 5: 验证——8 种已启用类型全部等价 ──
        # Mapper 产出的 SparkPlan 含 read+filter+join+aggregate+case_when+project+sort+limit
        # SqlBuildPlan 含 scan+scan+filter+join+aggregate+case_when+project+sort+limit
        # 全部在 Phase 7B 启用范围内
        assert report.status == ComparisonStatus.LOGIC_EQUIVALENT, (
            f"预期 LOGIC_EQUIVALENT，实际 {report.status}，"
            f"step_results={[(r.step_type, r.verdict.value) for r in report.step_results]}"
        )
        assert len(report.uncovered_step_types) == 0, (
            f"不应有任何未覆盖类型，实际 {report.uncovered_step_types}"
        )

        # 验证所有 step 类型都出现在结果中
        result_types = {r.step_type for r in report.step_results}
        expected_types = {"scan", "filter", "join", "aggregate", "case_when", "project", "sort", "limit"}
        for etype in expected_types:
            assert etype in result_types, f"step 类型 '{etype}' 未出现在对比结果中"

    def test_contract_with_window_marked_not_covered(self):
        """Contract 含 window → Mapper 产出含 WindowStep → Comparator 标记 NOT_COVERED。"""
        from tianshu_datadev.artifacts.models import (
            ContractInputTable,
            ContractOutputColumn,
            DataTransformContractV1,
            WindowSpecSummary,
        )
        from tianshu_datadev.planning.models import (
            ColumnRef,
        )
        from tianshu_datadev.planning.sql_build_plan import WindowStep
        from tianshu_datadev.spark.mapper import map_contract_to_spark_plan

        # 构造含 window 的最小 Contract
        program_id = "prog_c3_window"
        contract_id = DataTransformContractV1.generate_contract_id(program_id)
        contract = DataTransformContractV1(
            contract_id=contract_id,
            version="v1",
            source_phase="phase-3",
            source_sqlprogram_hash=program_id,
            input_tables=[
                ContractInputTable(table_ref="od", source_table="dwd.order_detail"),
            ],
            output_columns=[
                ContractOutputColumn(column_name="order_id", alias="order_id"),
                ContractOutputColumn(column_name="rn", alias="rn"),
            ],
            window_specs=[
                WindowSpecSummary(
                    statement_id="stmt_rank",
                    function="ROW_NUMBER",
                    alias="rn",
                    partition_by=["order_id"],
                    order_by=["amount"],
                ),
            ],
        )

        # Mapper → SparkPlan（含 WindowStep）
        mapping_result = map_contract_to_spark_plan(contract)
        assert mapping_result.success, f"Mapper 失败: {mapping_result.gaps}"
        spark_plan = mapping_result.spark_plan

        # SQL 侧：手工构造对等的 SqlBuildPlan（scan + window）
        sql_steps = [
            ScanStep(
                step_type="scan",
                step_id="scan_od",
                table_ref="od",
                required_columns=[
                    ColumnRef(table_ref="od", column_name="order_id", normalized_name="order_id"),
                    ColumnRef(table_ref="od", column_name="amount", normalized_name="amount"),
                ],
            ),
            WindowStep(
                step_type="window",
                step_id="win_001",
                window_exprs=[],
            ),
        ]
        sql_plan = _make_sql_plan(sql_steps)

        # Comparator → window 标记 NOT_COVERED
        comparator = PlanComparator()
        report = comparator.compare(sql_plan, spark_plan)

        assert report.status == ComparisonStatus.NOT_COVERED
        assert "window" in report.uncovered_step_types

    def test_contract_to_sql_steps_from_production_module(self):
        """验证生产模块 contract_to_sql_steps() 对标准 Contract 产出 8 种 step 类型。"""
        from tianshu_datadev.artifacts.models import (
            CaseWhenLabelSpec,
            ContractAggregation,
            ContractInputTable,
            ContractJoin,
            ContractLimit,
            ContractOutputColumn,
            ContractPredicate,
            ContractSort,
            DataTransformContractV1,
        )
        from tianshu_datadev.spark.contract_sql_bridge import (
            contract_to_sql_steps,
        )

        # 构造覆盖 8 种 step 的标准 Contract（与 C3 集成测试一致）
        program_id = "prog_c3_module_test"
        contract_id = DataTransformContractV1.generate_contract_id(program_id)
        contract = DataTransformContractV1(
            contract_id=contract_id,
            version="v1",
            source_phase="phase-3",
            source_sqlprogram_hash=program_id,
            input_tables=[
                ContractInputTable(table_ref="od", source_table="dwd.order_detail"),
                ContractInputTable(table_ref="ri", source_table="dim.region_info"),
            ],
            join_relationships=[
                ContractJoin(
                    join_id="join_od_ri",
                    left_table="od",
                    right_table="ri",
                    left_key="region_code",
                    right_key="region_code",
                    join_type="INNER",
                    evidence_chain={
                        "level": "STRONG",
                        "action": "ACCEPT",
                        "left_field": {"raw": "region_code", "normalized": "region_code"},
                        "right_field": {"raw": "region_code", "normalized": "region_code"},
                        "evidence_checks": {
                            "exact_name_match": True, "type_match": True, "unique_match": True,
                        },
                    },
                    level="STRONG",
                ),
            ],
            filters=[
                ContractPredicate(operator="GT", left="od.amount", right="0"),
            ],
            aggregations=[
                ContractAggregation(function="SUM", input_column="od.amount", alias="total_amt"),
            ],
            grouping_keys=["od.region_code"],
            output_columns=[
                ContractOutputColumn(column_name="region_code", alias="region_code"),
                ContractOutputColumn(column_name="total_amt", alias="total_amt"),
            ],
            output_grain=["region_code"],
            sort_spec=[ContractSort(column="total_amt", direction="DESC")],
            limit_spec=ContractLimit(limit=100),
            business_keys=["region_code"],
            step_dag={"stmt_main": []},
            temp_tables=[],
            case_when_labels=[
                CaseWhenLabelSpec(
                    statement_id="stmt_label",
                    output_alias="value_level",
                    branch_count=2,
                    labels=["high", "low"],
                    else_label="mid",
                ),
            ],
            window_specs=[],
        )

        # 调用生产模块的桥接函数
        steps = contract_to_sql_steps(contract)

        # 验证产出 8 种 step 类型
        step_types = {s.step_type for s in steps}
        expected_types = {"scan", "filter", "join", "aggregate", "case_when", "project", "sort", "limit"}
        for etype in expected_types:
            assert etype in step_types, f"step 类型 '{etype}' 未在结果中"

        # scan 类型应出现 2 次（2 个输入表）
        scan_types = [s for s in steps if s.step_type == "scan"]
        assert len(scan_types) == 2, f"预期 2 个 scan，实际 {len(scan_types)}"

        # 其他类型各出现 1 次
        for etype in {"filter", "join", "aggregate", "case_when", "project", "sort", "limit"}:
            matching = [s for s in steps if s.step_type == etype]
            assert len(matching) == 1, f"预期 1 个 {etype}，实际 {len(matching)}"

    def test_contract_to_sql_steps_empty_input(self):
        """验证 input_tables 为空时返回空列表（防御行为）。"""
        from tianshu_datadev.artifacts.models import (
            DataTransformContractV1,
        )
        from tianshu_datadev.spark.contract_sql_bridge import (
            contract_to_sql_steps,
        )

        # 构造空输入表的 Contract
        contract = DataTransformContractV1(
            contract_id="test_empty_input",
            version="v1",
            source_phase="phase-3",
            source_sqlprogram_hash="empty_test",
            input_tables=[],
            output_columns=[],
        )

        steps = contract_to_sql_steps(contract)
        assert steps == [], f"预期空列表，实际 {steps}"


# ════════════════════════════════════════════
# Phase 10 Case06——多语句 SqlProgram 扁平化对比测试
# ════════════════════════════════════════════


class TestPlanComparatorMultiStatementFlatten:
    """多语句 SqlProgram → 扁平化 step 列表 → SparkPlan 对比。

    Case06 SqlProgram 多语句 DAG Comparator 缺口收口——验证 _flatten_sql_program_steps()
    正确过滤 _temp_* scan（内部管道）并保留所有语义 step。
    """

    @staticmethod
    def _make_minimal_sql_program(
        statements: list[SqlStatement],
        spec_id: str = "test_spec_case06",
    ) -> SqlProgram:
        """构造最小合法 SqlProgram——用于多语句扁平化测试。"""
        program_id = SqlProgram.generate_program_id(spec_id)
        return SqlProgram(
            program_id=program_id,
            spec_id=spec_id,
            statements=statements,
            topological_order=[s.statement_id for s in statements],
        )

    @staticmethod
    def _make_statement(
        statement_id: str,
        plan: SqlBuildPlan,
        kind: StatementKind = StatementKind.PRODUCER,
        depends_on: list[str] | None = None,
        produces: str | None = None,
    ) -> SqlStatement:
        """构造最小合法 SqlStatement。"""
        return SqlStatement(
            statement_id=statement_id,
            plan=plan,
            kind=kind,
            depends_on=depends_on or [],
            produces=produces,
        )

    def test_flatten_excludes_temp_table_scans(self):
        """_temp_* scan 应从扁平化结果中排除——它们是 DAG 内部管道，Spark 侧无对应。"""
        from tianshu_datadev.planning.sql_build_plan import (
            ScanStep,
        )
        from tianshu_datadev.planning.sql_program import (
            SqlProgram,
            SqlStatement,
            StatementKind,
        )

        # 构造 2 语句 SqlProgram：stmt_0 产生 _temp_c0_trip_agg，stmt_1 读取它
        stmt_0_plan = _make_sql_plan([
            ScanStep(
                step_type="scan", step_id="scan_ft",
                table_ref="fact_trips",
                required_columns=[
                    ColumnRef(table_ref="fact_trips", column_name="boro", normalized_name="boro"),
                ],
            ),
            _make_sql_filter_step("filter_001"),
            _make_sql_project_step("proj_001"),
        ])
        stmt_1_plan = _make_sql_plan([
            ScanStep(
                step_type="scan", step_id="scan_temp",
                table_ref="_temp_c0_trip_agg",  # ← 应被过滤
                required_columns=[
                    ColumnRef(table_ref="_temp_c0_trip_agg", column_name="boro", normalized_name="boro"),
                ],
            ),
            _make_sql_filter_step("filter_002"),
        ])

        sql_program = SqlProgram(
            program_id=SqlProgram.generate_program_id("test_flatten_temp"),
            spec_id="test_flatten_temp",
            statements=[
                SqlStatement(
                    statement_id="stmt_0",
                    plan=stmt_0_plan,
                    kind=StatementKind.PRODUCER,
                    produces="_temp_c0_trip_agg",
                ),
                SqlStatement(
                    statement_id="stmt_1",
                    plan=stmt_1_plan,
                    kind=StatementKind.CONSUMER,
                    depends_on=["stmt_0"],
                ),
            ],
            topological_order=["stmt_0", "stmt_1"],
        )

        flat = PlanComparator._flatten_sql_program_steps(sql_program)

        # _temp_* scan 应被排除
        temp_scans = [
            s for s in flat
            if s.get("step_type") == "scan" and str(s.get("table_ref", "")).startswith("_temp_")
        ]
        assert len(temp_scans) == 0, (
            f"_temp_* scan 应被排除，实际仍有 {len(temp_scans)} 个：{temp_scans}"
        )

        # 源表 scan（fact_trips）应保留
        source_scans = [
            s for s in flat
            if s.get("step_type") == "scan" and s.get("table_ref") == "fact_trips"
        ]
        assert len(source_scans) == 1, f"源表 scan 应保留 1 个，实际 {len(source_scans)}"

        # 语义 step 应保留
        filter_count = sum(1 for s in flat if s.get("step_type") == "filter")
        project_count = sum(1 for s in flat if s.get("step_type") == "project")
        assert filter_count == 2, f"应保留 2 个 filter，实际 {filter_count}"
        assert project_count == 1, f"应保留 1 个 project，实际 {project_count}"

    def test_flatten_preserves_all_semantic_steps(self):
        """扁平化后所有语义 step（filter/aggregate/join/project）的类型和数量应完整保留。"""
        from tianshu_datadev.planning.sql_build_plan import (
            AggregateStep,
            JoinStep,
            ScanStep,
        )
        from tianshu_datadev.planning.sql_program import (
            SqlProgram,
            SqlStatement,
            StatementKind,
        )

        # 构造含多种语义 step 的 3 语句 SqlProgram
        stmt_0_plan = _make_sql_plan([
            ScanStep(
                step_type="scan", step_id="scan_od",
                table_ref="order_detail",
                required_columns=[
                    ColumnRef(table_ref="order_detail", column_name="order_id", normalized_name="order_id"),
                ],
            ),
            _make_sql_filter_step("filter_agg"),
            AggregateStep(
                step_type="aggregate", step_id="agg_001",
                group_keys=[
                    ColumnRef(table_ref="order_detail", column_name="region", normalized_name="region"),
                ],
                metrics=[
                    AggregateSpec(aggregation=AggregationType.COUNT, input_column=None, alias="cnt"),
                ],
            ),
            _make_sql_project_step("proj_agg"),
        ])

        stmt_1_plan = _make_sql_plan([
            ScanStep(
                step_type="scan", step_id="scan_od2",
                table_ref="order_detail",
                required_columns=[
                    ColumnRef(table_ref="order_detail", column_name="order_id", normalized_name="order_id"),
                ],
            ),
            JoinStep(
                step_type="join", step_id="join_001",
                right_table_ref="od",
                join_type="INNER",
                join_keys=[(
                    ColumnRef(table_ref="od", column_name="user_id", normalized_name="user_id"),
                    ColumnRef(table_ref="od", column_name="user_id", normalized_name="user_id"),
                )],
                relationship_ref="rel_001",
            ),
            _make_sql_project_step("proj_join"),
        ])

        sql_program = SqlProgram(
            program_id=SqlProgram.generate_program_id("test_preserve_semantic"),
            spec_id="test_preserve_semantic",
            statements=[
                SqlStatement(
                    statement_id="stmt_0", plan=stmt_0_plan,
                    kind=StatementKind.PRODUCER, produces="_temp_c0_agg",
                ),
                SqlStatement(
                    statement_id="stmt_1", plan=stmt_1_plan,
                    kind=StatementKind.CONSUMER,
                ),
            ],
            topological_order=["stmt_0", "stmt_1"],
        )

        flat = PlanComparator._flatten_sql_program_steps(sql_program)

        # 统计各类型数量
        type_counts: dict[str, int] = {}
        for s in flat:
            t = s.get("step_type", "")
            type_counts[t] = type_counts.get(t, 0) + 1

        # scan: 2 个源表 scan（两个 order_detail）
        assert type_counts.get("scan", 0) == 2, f"应保留 2 个源表 scan，实际 {type_counts.get('scan', 0)}"
        # filter: 1 个
        assert type_counts.get("filter", 0) == 1
        # aggregate: 1 个
        assert type_counts.get("aggregate", 0) == 1
        # join: 1 个
        assert type_counts.get("join", 0) == 1
        # project: 2 个
        assert type_counts.get("project", 0) == 2

    def test_compare_program_vs_spark_plan_equivalent(self):
        """最小 2 语句 SqlProgram（scan+filter → agg+proj）vs 等价 SparkPlan → LOGIC_EQUIVALENT。"""
        from tianshu_datadev.planning.sql_build_plan import (
            AggregateStep,
            ScanStep,
        )
        from tianshu_datadev.planning.sql_program import (
            SqlProgram,
            SqlStatement,
            StatementKind,
        )
        from tianshu_datadev.spark.models import (
            SparkAggFunction,
            SparkAggregateSpec,
            SparkAggregateStep,
        )

        # SQL 侧：2 语句 SqlProgram
        stmt_0_plan = _make_sql_plan([
            ScanStep(
                step_type="scan", step_id="scan_od",
                table_ref="od",
                required_columns=[
                    ColumnRef(table_ref="od", column_name="amount", normalized_name="amount"),
                    ColumnRef(table_ref="od", column_name="region", normalized_name="region"),
                ],
            ),
            _make_sql_filter_step("filter_001"),
            AggregateStep(
                step_type="aggregate", step_id="agg_001",
                group_keys=[
                    ColumnRef(table_ref="od", column_name="region", normalized_name="region"),
                ],
                metrics=[
                    AggregateSpec(aggregation=AggregationType.SUM, input_column="amount", alias="total_amt"),
                ],
            ),
            _make_sql_project_step("proj_001"),
        ])
        # stmt_1 读取 _temp 表再做一次 project（模拟 FINAL 包装）
        stmt_1_plan = _make_sql_plan([
            ScanStep(
                step_type="scan", step_id="scan_temp",
                table_ref="_temp_c0_agg",  # ← 应被过滤
                required_columns=[
                    ColumnRef(table_ref="_temp_c0_agg", column_name="region", normalized_name="region"),
                ],
            ),
            ProjectStep(
                step_type="project", step_id="proj_final",
                columns=[
                    AliasExpr(
                        expression=ColumnRef(
                            table_ref="_temp_c0_agg",
                            column_name="region", normalized_name="region",
                        ),
                        alias="region",
                    ),
                    AliasExpr(
                        expression=ColumnRef(
                            table_ref="_temp_c0_agg",
                            column_name="total_amt", normalized_name="total_amt",
                        ),
                        alias="total_amt",
                    ),
                ],
            ),
        ])

        sql_program = SqlProgram(
            program_id=SqlProgram.generate_program_id("test_eq"),
            spec_id="test_eq",
            statements=[
                SqlStatement(
                    statement_id="stmt_0", plan=stmt_0_plan,
                    kind=StatementKind.PRODUCER, produces="_temp_c0_agg",
                ),
                SqlStatement(
                    statement_id="stmt_1", plan=stmt_1_plan,
                    kind=StatementKind.FINAL, depends_on=["stmt_0"],
                ),
            ],
            topological_order=["stmt_0", "stmt_1"],
        )

        # Spark 侧：等价 SparkPlan（read+filter+aggregate+project+project）
        spark_plan = _make_spark_plan([
            SparkReadStep(
                step_type=SparkStepType.READ, alias="od",
                source_name="order_detail", input_key="order_detail_key",
                required_columns=["amount", "region"],
            ),
            SparkFilterStep(
                step_type=SparkStepType.FILTER, input_alias="od",
                operator="GT", left="amount", right="threshold",
            ),
            SparkAggregateStep(
                step_type=SparkStepType.AGGREGATE, input_alias="od",
                group_keys=["region"],
                metrics=[
                    SparkAggregateSpec(
                        function=SparkAggFunction.SUM,
                        input_column="amount", alias="total_amt",
                    ),
                ],
            ),
            SparkProjectStep(
                step_type=SparkStepType.PROJECT, input_alias="od",
                columns=[
                    SparkProjectColumn(column_name="order_id", alias="order_id"),
                    SparkProjectColumn(column_name="amount", alias="amount"),
                ],
            ),
            SparkProjectStep(
                step_type=SparkStepType.PROJECT, input_alias="od",
                columns=[
                    SparkProjectColumn(column_name="region", alias="region"),
                    SparkProjectColumn(column_name="total_amt", alias="total_amt"),
                ],
            ),
        ])

        comparator = PlanComparator()
        report = comparator.compare_program(sql_program, spark_plan)

        # _temp_* scan 已被过滤 → 对比范围不含 _temp_*，全部为已启用类型
        assert report.status == ComparisonStatus.LOGIC_EQUIVALENT, (
            f"预期 LOGIC_EQUIVALENT，实际 {report.status}，"
            f"step_results={[(r.step_type, r.verdict.value) for r in report.step_results]}"
        )

    def test_compare_program_vs_spark_plan_mismatch(self):
        """SqlProgram vs 不等价 SparkPlan（移除 filter）→ LOGIC_MISMATCH。"""
        from tianshu_datadev.planning.sql_build_plan import (
            ScanStep,
        )
        from tianshu_datadev.planning.sql_program import (
            SqlProgram,
            SqlStatement,
            StatementKind,
        )

        # SQL 侧：scan + filter + project
        stmt_plan = _make_sql_plan([
            ScanStep(
                step_type="scan", step_id="scan_od",
                table_ref="od",
                required_columns=[
                    ColumnRef(table_ref="od", column_name="amount", normalized_name="amount"),
                ],
            ),
            _make_sql_filter_step("filter_001"),
            _make_sql_project_step("proj_001"),
        ])

        sql_program = SqlProgram(
            program_id=SqlProgram.generate_program_id("test_mismatch"),
            spec_id="test_mismatch",
            statements=[
                SqlStatement(statement_id="stmt_0", plan=stmt_plan, kind=StatementKind.STANDALONE),
            ],
            topological_order=["stmt_0"],
        )

        # Spark 侧：仅有 read + project（缺少 filter）——不等价
        spark_plan = _make_spark_plan([
            SparkReadStep(
                step_type=SparkStepType.READ, alias="od",
                source_name="order_detail", input_key="order_detail_key",
            ),
            SparkProjectStep(
                step_type=SparkStepType.PROJECT, input_alias="od",
                columns=[
                    SparkProjectColumn(column_name="order_id", alias="order_id"),
                    SparkProjectColumn(column_name="amount", alias="amount"),
                ],
            ),
        ])

        comparator = PlanComparator()
        report = comparator.compare_program(sql_program, spark_plan)

        assert report.status == ComparisonStatus.LOGIC_MISMATCH, (
            f"预期 LOGIC_MISMATCH（Spark 侧缺 filter），实际 {report.status}"
        )


# ── _normalize_dag_steps 单元测试 ──


class TestNormalizeDagSteps:
    """Comparator DAG 归一化——_normalize_dag_steps() 的单元测试。

    验证扁平化后的多个同类型 step 被正确合并为单一步骤。
    """

    def test_merges_multiple_aggregates(self):
        """3 个 aggregate step → 归一化为 1 个，metrics 和 group_keys 合并去重。"""
        steps = [
            {"step_type": "scan", "table_ref": "fc"},
            {"step_type": "aggregate", "group_keys": ["borough"],
             "metrics": [{"function": "COUNT", "alias": "total_crashes"}]},
            {"step_type": "aggregate", "group_keys": ["borough"],
             "metrics": [{"function": "SUM", "alias": "total_injured"}]},
            {"step_type": "aggregate", "group_keys": ["violation_county"],
             "metrics": [{"function": "SUM", "alias": "total_violations"}]},
        ]
        result = PlanComparator._normalize_dag_steps(steps)
        agg_steps = [s for s in result if s.get("step_type") == "aggregate"]
        assert len(agg_steps) == 1, f"预期 1 个 aggregate，实际 {len(agg_steps)}"
        assert len(agg_steps[0]["metrics"]) == 3
        assert set(agg_steps[0]["group_keys"]) == {"borough", "violation_county"}

    def test_merges_multiple_projects(self):
        """7 个 project step → 归一化为 1 个，columns 合并去重。"""
        steps = [
            {"step_type": "project", "columns": [
                {"column_name": "borough", "alias": "borough"},
                {"column_name": "total_crashes", "alias": "total_crashes"},
            ]},
            {"step_type": "project", "columns": [
                {"column_name": "total_injured", "alias": "total_injured"},
            ]},
            {"step_type": "project", "columns": [
                {"column_name": "borough", "alias": "borough"},  # 重复——应去重
                {"column_name": "total_killed", "alias": "total_killed"},
            ]},
        ]
        result = PlanComparator._normalize_dag_steps(steps)
        proj_steps = [s for s in result if s.get("step_type") == "project"]
        assert len(proj_steps) == 1, f"预期 1 个 project，实际 {len(proj_steps)}"
        assert len(proj_steps[0]["columns"]) == 4  # borough 去重后 4 个唯一列（borough/total_crashes/total_injured/total_killed）

    def test_preserves_other_types(self):
        """scan/filter/join/case_when 不受归一化影响——原样保留。"""
        steps = [
            {"step_type": "scan", "table_ref": "fc"},
            {"step_type": "filter", "operator": "GT"},
            {"step_type": "join", "join_type": "LEFT"},
            {"step_type": "case_when", "labels": ["高风险"]},
            {"step_type": "aggregate", "group_keys": ["x"], "metrics": []},
            {"step_type": "project", "columns": []},
        ]
        result = PlanComparator._normalize_dag_steps(steps)
        types = [s.get("step_type") for s in result]
        assert types.count("scan") == 1
        assert types.count("filter") == 1
        assert types.count("join") == 1
        assert types.count("case_when") == 1
        assert types.count("aggregate") == 1
        assert types.count("project") == 1
