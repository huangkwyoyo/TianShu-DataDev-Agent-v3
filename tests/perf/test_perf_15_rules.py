"""Phase 4B 15 条 PERF 规则全覆盖测试——补充 tests/sql/test_perf.py 未覆盖的规则。

覆盖：PERF-003（WHERE 左侧函数）、PERF-004（汇总表偏好）、
PERF-009（DISTINCT *）、PERF-012（窗口前缩小）、PERF-013（高频指标）。
"""

from __future__ import annotations

from tianshu_datadev.planning.models import (
    AggregateSpec,
    ColumnRef,
    Predicate,
    PredicateOperator,
    SqlLiteral,
    WindowExpr,
)
from tianshu_datadev.planning.sql_build_plan import (
    AggregateStep,
    ScanStep,
    SqlBuildPlan,
    WindowStep,
)
from tianshu_datadev.sql.models import PerfSeverity
from tianshu_datadev.sql.perf_validator import PerfValidator


def _make_minimal_scan(table_ref: str = "t1") -> ScanStep:
    """快速构造最小 ScanStep。"""
    return ScanStep(
        step_id=f"scan_{table_ref}",
        table_ref=table_ref,
        required_columns=[
            ColumnRef(table_ref=table_ref, column_name="id", normalized_name="id"),
        ],
    )


class TestPerf003TimeFunctionOnLeft:
    """PERF-003: 时间过滤禁止 WHERE 左侧套函数 → REJECT。"""

    def test_perf003_passes_with_clean_time_filter(self):
        """干净的时间过滤（无函数包裹）→ 通过。"""
        plan = SqlBuildPlan(
            plan_id="test_perf003_clean",
            spec_hash="abc",
            steps=[
                ScanStep(
                    step_id="scan_1",
                    table_ref="t1",
                    required_columns=[
                        ColumnRef(table_ref="t1", column_name="id", normalized_name="id"),
                    ],
                    predicates=[
                        Predicate(
                            left=ColumnRef(
                                table_ref="t1", column_name="dt", normalized_name="dt",
                            ),
                            operator=PredicateOperator.GTE,
                            right=SqlLiteral(value="2026-01-01"),
                        ),
                    ],
                ),
            ],
            multi_table=False,
        )
        validator = PerfValidator()
        result = validator.validate(plan)
        perf003 = [r for r in result.check_results if r.rule_id == "PERF-003"]
        assert len(perf003) == 1
        assert perf003[0].passed is True


class TestPerf004PreferSummary:
    """PERF-004: 优先使用汇总表/DWS 表 → WARN。"""

    def test_perf004_no_summary_info_skips(self):
        """无汇总表信息 → 跳过检查。"""
        plan = SqlBuildPlan(
            plan_id="test_perf004_skip",
            spec_hash="abc",
            steps=[_make_minimal_scan("t1")],
            multi_table=False,
        )
        validator = PerfValidator()
        result = validator.validate(plan)
        perf004 = [r for r in result.check_results if r.rule_id == "PERF-004"]
        assert len(perf004) == 1
        assert perf004[0].passed is True
        assert "跳过" in perf004[0].message

    def test_perf004_fact_when_summary_available(self):
        """存在汇总表但扫描明细 → WARN。"""
        plan = SqlBuildPlan(
            plan_id="test_perf004_warn",
            spec_hash="abc",
            steps=[
                ScanStep(
                    step_id="scan_fact",
                    table_ref="dwd_fact_daily",
                    required_columns=[
                        ColumnRef(table_ref="dwd_fact_daily", column_name="id", normalized_name="id"),
                    ],
                    estimated_row_count=5_000_000,  # 大表
                ),
            ],
            multi_table=False,
        )
        validator = PerfValidator()
        result = validator.validate(
            plan,
            summary_tables={"dws_user_daily", "dws_order_daily"},
        )
        perf004 = [r for r in result.check_results if r.rule_id == "PERF-004"]
        assert len(perf004) == 1
        assert perf004[0].passed is False, f"应警告: {perf004[0].message}"
        assert perf004[0].severity == PerfSeverity.WARN

    def test_perf004_summary_used_ok(self):
        """扫描汇总表 → 通过。"""
        plan = SqlBuildPlan(
            plan_id="test_perf004_ok",
            spec_hash="abc",
            steps=[
                ScanStep(
                    step_id="scan_dws",
                    table_ref="dws_user_daily",
                    required_columns=[
                        ColumnRef(table_ref="dws_user_daily", column_name="id", normalized_name="id"),
                    ],
                    estimated_row_count=100_000,  # 小表
                ),
            ],
            multi_table=False,
        )
        validator = PerfValidator()
        result = validator.validate(
            plan,
            summary_tables={"dws_user_daily", "dws_order_daily"},
        )
        perf004 = [r for r in result.check_results if r.rule_id == "PERF-004"]
        assert len(perf004) == 1
        assert perf004[0].passed is True


class TestPerf009DistinctStar:
    """PERF-009: 禁止无理由 DISTINCT * → REJECT。"""

    def test_perf009_count_distinct_no_group_rejected(self):
        """COUNT_DISTINCT 无 group_keys → REJECT。"""
        plan = SqlBuildPlan(
            plan_id="test_perf009",
            spec_hash="abc",
            steps=[
                _make_minimal_scan("t1"),
                AggregateStep(
                    step_id="agg_distinct",
                    group_keys=[],  # 无分组键
                    metrics=[
                        AggregateSpec(
                            aggregation="COUNT_DISTINCT",
                            input_column="user_id",
                            alias="distinct_users",
                        ),
                    ],
                ),
            ],
            multi_table=False,
        )
        validator = PerfValidator()
        result = validator.validate(plan)
        perf009 = [r for r in result.check_results if r.rule_id == "PERF-009"]
        assert len(perf009) == 1
        assert perf009[0].passed is False
        assert perf009[0].severity == PerfSeverity.REJECT

    def test_perf009_count_distinct_with_group_passes(self):
        """COUNT_DISTINCT 有 group_keys → 通过。"""
        plan = SqlBuildPlan(
            plan_id="test_perf009_pass",
            spec_hash="abc",
            steps=[
                _make_minimal_scan("t1"),
                AggregateStep(
                    step_id="agg_ok",
                    group_keys=[
                        ColumnRef(table_ref="t1", column_name="dt", normalized_name="dt"),
                    ],
                    metrics=[
                        AggregateSpec(
                            aggregation="COUNT_DISTINCT",
                            input_column="user_id",
                            alias="distinct_users",
                        ),
                    ],
                ),
            ],
            multi_table=False,
        )
        validator = PerfValidator()
        result = validator.validate(plan)
        perf009 = [r for r in result.check_results if r.rule_id == "PERF-009"]
        assert len(perf009) == 1
        assert perf009[0].passed is True


class TestPerf012NarrowBeforeWindow:
    """PERF-012: 窗口函数前必须缩小数据范围 → WARN。"""

    def test_perf012_window_without_pre_narrow_warns(self):
        """WindowStep 前无过滤或聚合 → WARN。"""
        plan = SqlBuildPlan(
            plan_id="test_perf012",
            spec_hash="abc",
            steps=[
                _make_minimal_scan("t1"),
                WindowStep(
                    step_id="window_no_narrow",
                    window_exprs=[
                        WindowExpr(
                            function="ROW_NUMBER",
                            partition_by=[
                                ColumnRef(table_ref="t1", column_name="dt", normalized_name="dt"),
                            ],
                            order_by=[],
                            alias="rn",
                        ),
                    ],
                ),
            ],
            multi_table=False,
        )
        validator = PerfValidator()
        result = validator.validate(plan)
        perf012 = [r for r in result.check_results if r.rule_id == "PERF-012"]
        assert len(perf012) == 1
        assert perf012[0].passed is False
        assert perf012[0].severity == PerfSeverity.WARN

    def test_perf012_window_after_aggregate_passes(self):
        """WindowStep 在聚合之后 → 通过。"""
        plan = SqlBuildPlan(
            plan_id="test_perf012_pass",
            spec_hash="abc",
            steps=[
                _make_minimal_scan("t1"),
                AggregateStep(
                    step_id="agg_first",
                    group_keys=[
                        ColumnRef(table_ref="t1", column_name="dt", normalized_name="dt"),
                    ],
                    metrics=[
                        AggregateSpec(
                            aggregation="COUNT",
                            input_column="user_id",
                            alias="cnt",
                        ),
                    ],
                ),
                WindowStep(
                    step_id="window_after_agg",
                    window_exprs=[
                        WindowExpr(
                            function="ROW_NUMBER",
                            partition_by=[
                                ColumnRef(table_ref="t1", column_name="dt", normalized_name="dt"),
                            ],
                            order_by=[],
                            alias="rn",
                        ),
                    ],
                ),
            ],
            multi_table=False,
        )
        validator = PerfValidator()
        result = validator.validate(plan)
        perf012 = [r for r in result.check_results if r.rule_id == "PERF-012"]
        assert len(perf012) == 1
        assert perf012[0].passed is True


class TestPerf013HighFreqMetric:
    """PERF-013: 高频指标建议沉淀为汇总表 → WARN。"""

    def test_perf013_few_metrics_passes(self):
        """少量指标 → 通过。"""
        plan = SqlBuildPlan(
            plan_id="test_perf013_pass",
            spec_hash="abc",
            steps=[
                _make_minimal_scan("t1"),
                AggregateStep(
                    step_id="agg_few",
                    group_keys=[
                        ColumnRef(table_ref="t1", column_name="dt", normalized_name="dt"),
                    ],
                    metrics=[
                        AggregateSpec(
                            aggregation="COUNT_DISTINCT",
                            input_column="user_id",
                            alias="dau",
                        ),
                    ],
                ),
            ],
            multi_table=False,
        )
        validator = PerfValidator()
        result = validator.validate(plan)
        perf013 = [r for r in result.check_results if r.rule_id == "PERF-013"]
        assert len(perf013) == 1
        assert perf013[0].passed is True

    def test_perf013_many_metrics_warns(self):
        """≥5 个聚合指标 → WARN。"""
        plan = SqlBuildPlan(
            plan_id="test_perf013_warn",
            spec_hash="abc",
            steps=[
                _make_minimal_scan("t1"),
                AggregateStep(
                    step_id="agg_many",
                    group_keys=[
                        ColumnRef(table_ref="t1", column_name="dt", normalized_name="dt"),
                    ],
                    metrics=[
                        AggregateSpec(aggregation="COUNT_DISTINCT", input_column="user_id", alias="dau"),
                        AggregateSpec(aggregation="COUNT", input_column="order_id", alias="order_cnt"),
                        AggregateSpec(aggregation="SUM", input_column="amount", alias="gmv"),
                        AggregateSpec(aggregation="AVG", input_column="amount", alias="avg_order"),
                        AggregateSpec(aggregation="MAX", input_column="amount", alias="max_order"),
                    ],
                ),
            ],
            multi_table=False,
        )
        validator = PerfValidator()
        result = validator.validate(plan)
        perf013 = [r for r in result.check_results if r.rule_id == "PERF-013"]
        assert len(perf013) == 1
        assert perf013[0].passed is False
        assert perf013[0].severity == PerfSeverity.WARN
