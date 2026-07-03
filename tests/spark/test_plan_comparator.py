"""Phase 7A PlanComparator 测试——SQL Plan ↔ Spark Plan 逻辑链路对比。

覆盖：
- 5 种 step 类型（scan/filter/project/sort/limit）逻辑对比
- 非 5 种 step 类型标记 NOT_COVERED
- PlanComparisonReport 结构完整性
- PlanComparator 只读 SqlBuildPlan 结构化 artifact（不读 SQL 文本）
- 状态禁止泛化 PASS
"""

from __future__ import annotations

import pytest

from tianshu_datadev.planning.models import (
    AliasExpr,
    ColumnRef,
    Predicate,
    PredicateOperator,
    SortDirection,
    SortSpec,
)
from tianshu_datadev.planning.sql_build_plan import (
    FilterStep,
    LimitStep,
    ProjectStep,
    ScanStep,
    SortStep,
    SqlBuildPlan,
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
    PlanComparisonReport,
)
from tianshu_datadev.spark.plan_equivalence import (
    EquivalenceVerdict,
)


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
# PlanComparator——NOT_COVERED 标记测试
# ════════════════════════════════════════════


class TestPlanComparatorNotCovered:
    """非 Phase 7A 覆盖类型 → NOT_COVERED（本 Phase 未覆盖，后续 Phase 会覆盖）。"""

    def test_join_not_in_enabled_types(self):
        """Join 类型不在 Phase 7A 启用列表 → NOT_COVERED。"""
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

        # Join 不在 Phase 7A 的 5 种类型中 → NOT_COVERED
        assert report.status == ComparisonStatus.NOT_COVERED
        assert "join" in report.uncovered_step_types

    def test_aggregate_not_in_enabled_types(self):
        """Aggregate 类型 → NOT_COVERED。"""
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

        assert report.status == ComparisonStatus.NOT_COVERED
        assert "aggregate" in report.uncovered_step_types

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

    def test_case_when_not_in_enabled_types(self):
        """CaseWhen 类型 → NOT_COVERED。"""
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

        assert report.status == ComparisonStatus.NOT_COVERED
        assert "case_when" in report.uncovered_step_types


# ════════════════════════════════════════════
# PlanComparator——混合场景
# ════════════════════════════════════════════


class TestPlanComparatorMixedScenarios:
    """混合 step 类型场景——部分已覆盖 + 部分未覆盖。"""

    def test_covered_steps_equivalent_with_uncovered(
        self,
    ):
        """已覆盖部分等价 + 未覆盖部分 → NOT_COVERED（已覆盖部分结果有效）。"""
        from tianshu_datadev.planning.sql_build_plan import JoinStep
        from tianshu_datadev.spark.models import SparkJoinStep, SparkJoinType

        # SQL 侧：scan + filter（已覆盖，应等价）+ join（未覆盖）
        sql_plan = _make_sql_plan([
            _make_sql_scan_step(),
            _make_sql_filter_step(),
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
        # Spark 侧：read + filter（已覆盖，应等价）+ join（未覆盖）
        spark_plan = _make_spark_plan([
            _make_spark_read_step(),
            _make_spark_filter_step(),
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

        # 已覆盖部分等价 → 但存在未覆盖类型 → NOT_COVERED
        assert report.status == ComparisonStatus.NOT_COVERED
        assert "join" in report.uncovered_step_types
        # 已覆盖部分应等价（step_results 中可查）
        covered_results = [
            r for r in report.step_results
            if r.step_type in ("scan", "filter")
        ]
        assert all(
            r.verdict == EquivalenceVerdict.EQUIVALENT
            for r in covered_results
        )

    def test_all_steps_not_covered(self):
        """全部 step 均为未覆盖类型 → NOT_COVERED。"""
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

        assert report.status == ComparisonStatus.NOT_COVERED

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
        """未覆盖类型在 uncovered_step_types 中正确标记。"""
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

        assert "join" in report.uncovered_step_types


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
        """启用所有 Phase 7A 已支持扁平化的类型——scan/filter/project/sort/limit。

        join/aggregate/case_when/window 的扁平化属于 Phase 6B/6C 范围。
        """
        sql_plan = _make_sql_plan([
            _make_sql_scan_step(),
            _make_sql_filter_step(),
            _make_sql_project_step(),
            _make_sql_sort_step(),
            _make_sql_limit_step(),
        ])
        spark_plan = _make_spark_plan([
            _make_spark_read_step(),
            _make_spark_filter_step(),
            _make_spark_project_step(),
            _make_spark_sort_step(),
            _make_spark_limit_step(),
        ])

        # 启用所有 5 种 Phase 7A 类型
        all_types = {"scan", "filter", "project", "sort", "limit"}
        comparator = PlanComparator(enabled_step_types=all_types)
        report = comparator.compare(sql_plan, spark_plan)

        # 全部在对比范围内且等价
        assert report.status == ComparisonStatus.LOGIC_EQUIVALENT
        assert len(report.uncovered_step_types) == 0
