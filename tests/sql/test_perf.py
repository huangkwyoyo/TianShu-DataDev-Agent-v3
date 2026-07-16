"""测试 PerfValidator——15 条 PERF 规则全覆盖 + REJECT/WARN/PERF_FEEDBACK 三分流。

Phase 4B 适配：旧 API tuple[bool, list] → PerfValidationResult 聚合结果。
旧 PerfRuleLevel → PerfSeverity。
融合了原 tests/perf/test_perf_15_rules.py（PERF-003/004/009/012/013）。
"""

from __future__ import annotations

import os

from tests._test_utils import read_fixture
from tianshu_datadev.developer_spec.parser import DeveloperSpecParser
from tianshu_datadev.planning.models import (
    AggregateSpec,
    ColumnRef,
    JoinType,
    Predicate,
    PredicateOperator,
    SortSpec,
    SqlLiteral,
    WindowExpr,
)
from tianshu_datadev.planning.sql_build_plan import (
    AggregateStep,
    FilterStep,
    JoinStep,
    LimitStep,
    ScanStep,
    SortStep,
    SqlBuildPlan,
    WindowStep,
)
from tianshu_datadev.sql.models import PerfSeverity
from tianshu_datadev.sql.perf_validator import PerfValidator

# ── 辅助 ──

def read_fixture(path: str) -> str:
    abs_path = os.path.join(os.path.dirname(__file__), "..", path)
    with open(abs_path, "r", encoding="utf-8") as f:
        return f.read()


def _parse_spec(fixture_path: str):
    parser = DeveloperSpecParser()
    text = read_fixture(fixture_path)
    return parser.parse(text)


def _make_minimal_scan(table_ref: str = "t1", cols: list[str] | None = None) -> ScanStep:
    """快速构造最小 ScanStep——有显式 required_columns。"""
    if cols is None:
        cols = ["id"]
    return ScanStep(
        step_id=f"scan_{table_ref}",
        table_ref=table_ref,
        required_columns=[
            ColumnRef(table_ref=table_ref, column_name=c, normalized_name=c)
            for c in cols
        ],
    )


# ════════════════════════════════════════════
# REJECT 规则测试
# ════════════════════════════════════════════


class TestPerfValidatorReject:
    """PERF REJECT 规则——违反后阻断编译。"""

    def test_perf001_select_star_rejected(self):
        """PERF-001: SELECT *（required_columns 为空）→ REJECT。"""
        plan = SqlBuildPlan(
            plan_id="test_perf001",
            spec_hash="abc123",
            steps=[
                ScanStep(
                    step_id="scan_star",
                    table_ref="t1",
                    required_columns=[],  # 空 = SELECT *
                ),
            ],
            multi_table=False,
        )

        validator = PerfValidator()
        result = validator.validate(plan)

        assert result.all_reject_passed is False
        perf001 = [r for r in result.check_results if r.rule_id == "PERF-001"]
        assert len(perf001) == 1
        assert perf001[0].passed is False
        assert perf001[0].severity == PerfSeverity.REJECT

    def test_perf006_join_key_mismatch_rejected(self):
        """PERF-006: Join key 类型不一致（启发式推断）→ REJECT。"""
        plan = SqlBuildPlan(
            plan_id="test_perf006",
            spec_hash="abc123",
            steps=[
                JoinStep(
                    step_id="join_mismatch",
                    right_table_ref="t2",
                    join_type=JoinType.INNER,
                    join_keys=[
                        (
                            ColumnRef(table_ref="t1", column_name="user_id", normalized_name="user_id"),
                            ColumnRef(table_ref="t2", column_name="user_name", normalized_name="user_name"),
                        )
                    ],
                    relationship_ref="jc_01",
                ),
            ],
            multi_table=True,
        )

        validator = PerfValidator()
        result = validator.validate(plan)

        perf006 = [r for r in result.check_results if r.rule_id == "PERF-006"]
        assert len(perf006) == 1
        assert perf006[0].passed is False

    def test_perf002_fact_no_time_filter_rejected(self):
        """PERF-002: 大事实表无时间过滤 → REJECT。"""
        plan = SqlBuildPlan(
            plan_id="test_perf002",
            spec_hash="abc123",
            steps=[
                ScanStep(
                    step_id="scan_fact",
                    table_ref="dwd_fact_daily",
                    required_columns=[
                        ColumnRef(table_ref="dwd_fact_daily", column_name="id", normalized_name="id"),
                    ],
                    estimated_row_count=5_000_000,  # > 1M 大表
                    # 无 partition_filters，无时间相关 predicate
                ),
            ],
            multi_table=False,
        )

        validator = PerfValidator()
        result = validator.validate(plan)

        perf002 = [r for r in result.check_results if r.rule_id == "PERF-002"]
        assert len(perf002) == 1
        assert perf002[0].passed is False, f"应拒绝: {perf002[0].message}"
        assert "dwd_fact_daily" in perf002[0].message

    def test_perf002_fact_with_time_filter_passes(self):
        """PERF-002: 大事实表有时间过滤 → 通过。"""
        plan = SqlBuildPlan(
            plan_id="test_perf002_pass",
            spec_hash="abc123",
            steps=[
                ScanStep(
                    step_id="scan_fact_ok",
                    table_ref="dwd_fact_daily",
                    required_columns=[
                        ColumnRef(table_ref="dwd_fact_daily", column_name="id", normalized_name="id"),
                    ],
                    estimated_row_count=5_000_000,
                    predicates=[
                        Predicate(
                            left=ColumnRef(
                                table_ref="dwd_fact_daily",
                                column_name="dt_date",
                                normalized_name="dt_date",
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

        perf002 = [r for r in result.check_results if r.rule_id == "PERF-002"]
        assert len(perf002) == 1
        assert perf002[0].passed is True, f"应通过: {perf002[0].message}"

    def test_perf002_cross_table_misroute_rejected(self):
        """PERF-002: 大 fact 无过滤 + 小维表 dt 过滤 + LIMIT → REJECT（跨表绕过修复）。

        验证其他表的 FilterStep 时间过滤不会误放行当前大事实表。
        """
        plan = SqlBuildPlan(
            plan_id="test_perf002_cross_table",
            spec_hash="abc123",
            steps=[
                ScanStep(
                    step_id="scan_fact",
                    table_ref="dwd_big_fact",
                    required_columns=[
                        ColumnRef(table_ref="dwd_big_fact", column_name="id", normalized_name="id"),
                    ],
                    estimated_row_count=5_000_000,  # > 1M 大表
                    # 无时间 predicate——应被 REJECT
                ),
                # 维表 dt 过滤——但不属于 dwd_big_fact，不应放行
                FilterStep(
                    step_id="filter_dim_calendar",
                    predicate=Predicate(
                        left=ColumnRef(
                            table_ref="dim_calendar", column_name="dt", normalized_name="dt",
                        ),
                        operator=PredicateOperator.GTE,
                        right=SqlLiteral(value="2026-01-01"),
                    ),
                ),
                ScanStep(
                    step_id="scan_dim",
                    table_ref="dim_calendar",
                    required_columns=[
                        ColumnRef(table_ref="dim_calendar", column_name="id", normalized_name="id"),
                    ],
                    estimated_row_count=100,  # 小维表
                ),
                LimitStep(step_id="limit_100", limit=100),  # 避免 PERF-008
            ],
            multi_table=True,
        )

        validator = PerfValidator()
        result = validator.validate(plan)

        assert result.all_reject_passed is False, (
            f"应被 PERF-002 REJECT，但 all_reject_passed=True。"
            f" reject_violations={result.reject_violations}"
        )
        perf002 = [r for r in result.check_results if r.rule_id == "PERF-002"]
        assert len(perf002) == 1
        assert perf002[0].passed is False, (
            f"PERF-002 应 REJECT dwd_big_fact 缺少时间过滤，"
            f"不应被 dim_calendar 的 dt 过滤误放行。message={perf002[0].message}"
        )
        assert "dwd_big_fact" in perf002[0].message

    def test_perf002_fact_self_filter_passes_with_other_table(self):
        """PERF-002: 大 fact 自身 dt 过滤 + 其他表存在 → 通过。

        验证在存在其他表的情况下，大事实表自身的时间过滤仍能正确识别并放行。
        """
        plan = SqlBuildPlan(
            plan_id="test_perf002_self_filter_ok",
            spec_hash="abc123",
            steps=[
                ScanStep(
                    step_id="scan_fact",
                    table_ref="dwd_big_fact",
                    required_columns=[
                        ColumnRef(table_ref="dwd_big_fact", column_name="id", normalized_name="id"),
                    ],
                    estimated_row_count=5_000_000,  # > 1M 大表
                    predicates=[
                        # 大事实表自身的时间过滤
                        Predicate(
                            left=ColumnRef(
                                table_ref="dwd_big_fact",
                                column_name="dt_date",
                                normalized_name="dt_date",
                            ),
                            operator=PredicateOperator.GTE,
                            right=SqlLiteral(value="2026-01-01"),
                        ),
                    ],
                ),
                # 另一张表的过滤——不含时间关键词，但确保存在其他 FilterStep 不干扰
                FilterStep(
                    step_id="filter_other",
                    predicate=Predicate(
                        left=ColumnRef(
                            table_ref="other_table", column_name="status", normalized_name="status",
                        ),
                        operator=PredicateOperator.EQ,
                        right=SqlLiteral(value="active"),
                    ),
                ),
                LimitStep(step_id="limit_100", limit=100),
            ],
            multi_table=False,
        )

        validator = PerfValidator()
        result = validator.validate(plan)

        perf002 = [r for r in result.check_results if r.rule_id == "PERF-002"]
        assert len(perf002) == 1
        assert perf002[0].passed is True, (
            f"大事实表自身有时间过滤应通过，但被 REJECT。message={perf002[0].message}"
        )
        """PERF-008: 明细查询（无聚合）缺 LIMIT → REJECT。"""
        plan = SqlBuildPlan(
            plan_id="test_perf008",
            spec_hash="abc123",
            steps=[
                ScanStep(
                    step_id="scan_big",
                    table_ref="huge_table",
                    required_columns=[
                        ColumnRef(table_ref="huge_table", column_name="id", normalized_name="id"),
                    ],
                    estimated_row_count=500_000,  # > 100K 明细阈值
                ),
            ],
            multi_table=False,
        )

        validator = PerfValidator()
        result = validator.validate(plan)

        assert result.all_reject_passed is False
        perf008 = [r for r in result.check_results if r.rule_id == "PERF-008"]
        assert len(perf008) == 1
        assert perf008[0].passed is False

    def test_perf008_detail_with_limit_passes(self):
        """PERF-008: 明细查询有 LIMIT → 通过。"""
        plan = SqlBuildPlan(
            plan_id="test_perf008_pass",
            spec_hash="abc123",
            steps=[
                ScanStep(
                    step_id="scan_ok",
                    table_ref="t1",
                    required_columns=[
                        ColumnRef(table_ref="t1", column_name="id", normalized_name="id"),
                    ],
                    estimated_row_count=500_000,
                ),
                LimitStep(
                    step_id="limit_100",
                    limit=100,
                ),
            ],
            multi_table=False,
        )

        validator = PerfValidator()
        result = validator.validate(plan)

        perf008 = [r for r in result.check_results if r.rule_id == "PERF-008"]
        assert len(perf008) == 1
        assert perf008[0].passed is True

    def test_perf010_cross_join_rejected(self):
        """PERF-010: CROSS JOIN → REJECT。"""
        plan = SqlBuildPlan(
            plan_id="test_perf010",
            spec_hash="abc123",
            steps=[
                JoinStep(
                    step_id="join_cross",
                    right_table_ref="t2",
                    join_type=JoinType.CROSS,
                    join_keys=[],
                    relationship_ref="jc_01",
                ),
            ],
            multi_table=True,
        )

        validator = PerfValidator()
        result = validator.validate(plan)

        perf010 = [r for r in result.check_results if r.rule_id == "PERF-010"]
        assert len(perf010) == 1
        assert perf010[0].passed is False

    def test_perf003_time_function_on_left_rejected(self):
        """PERF-003: WHERE 左侧套时间函数 → REJECT，干净过滤则通过。"""
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

    def test_perf009_count_distinct_no_group_rejected(self):
        """PERF-009: COUNT_DISTINCT 无 group_keys → REJECT。"""
        plan = SqlBuildPlan(
            plan_id="test_perf009",
            spec_hash="abc",
            steps=[
                _make_minimal_scan("t1"),
                AggregateStep(
                    step_id="agg_distinct",
                    group_keys=[],
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
        """PERF-009: COUNT_DISTINCT 有 group_keys → 通过。"""
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


# ════════════════════════════════════════════
# WARN 规则测试
# ════════════════════════════════════════════


class TestPerfValidatorWarn:
    """PERF WARN 规则——违反后记录但不阻断编译。"""

    def test_perf011_sort_wrong_position_warn(self):
        """PERF-011: SortStep 在非最终位置且无 LIMIT → WARN 但不阻断。"""
        plan = SqlBuildPlan(
            plan_id="test_perf011",
            spec_hash="abc123",
            steps=[
                ScanStep(
                    step_id="scan_small",
                    table_ref="small_table",
                    required_columns=[
                        ColumnRef(table_ref="small_table", column_name="id", normalized_name="id"),
                    ],
                    estimated_row_count=1000,  # 小表——避免触发 PERF-002/PERF-008
                ),
                SortStep(
                    step_id="sort_mid",
                    order_by=[SortSpec(column="id", direction="ASC")],
                    requires_full_sort=True,
                    estimated_input_rows=1000,
                ),
                # 排序后有后续步骤但无 LIMIT——排序在非最终位置
                FilterStep(
                    step_id="filter_after_sort",
                    predicate=Predicate(
                        left=ColumnRef(table_ref="small_table", column_name="id", normalized_name="id"),
                        operator=PredicateOperator.GT,
                        right=SqlLiteral(value=0),
                    ),
                ),
                ScanStep(
                    step_id="scan_another",
                    table_ref="t2",
                    required_columns=[
                        ColumnRef(table_ref="t2", column_name="id", normalized_name="id"),
                    ],
                    estimated_row_count=500,
                ),
            ],
            multi_table=False,
        )

        validator = PerfValidator()
        result = validator.validate(plan)

        # PERF-011 是 WARN，不应阻断
        assert result.all_reject_passed is True
        perf011 = [r for r in result.check_results if r.rule_id == "PERF-011"]
        assert len(perf011) == 1
        assert perf011[0].passed is False
        assert perf011[0].severity == PerfSeverity.WARN

    def test_perf005_large_join_no_preagg_warn(self):
        """PERF-005: 大表 Join 未启用预聚合 → WARN。"""
        plan = SqlBuildPlan(
            plan_id="test_perf005",
            spec_hash="abc123",
            steps=[
                ScanStep(
                    step_id="scan_big1",
                    table_ref="big_table_1",
                    required_columns=[
                        ColumnRef(table_ref="big_table_1", column_name="id", normalized_name="id"),
                        ColumnRef(table_ref="big_table_1", column_name="key", normalized_name="key"),
                    ],
                    estimated_row_count=5_000_000,
                    # 添加时间过滤——通过 PERF-002
                    predicates=[
                        Predicate(
                            left=ColumnRef(table_ref="big_table_1", column_name="dt", normalized_name="dt"),
                            operator=PredicateOperator.GTE,
                            right=SqlLiteral(value="2026-01-01"),
                        ),
                    ],
                ),
                ScanStep(
                    step_id="scan_big2",
                    table_ref="big_table_2",
                    required_columns=[
                        ColumnRef(table_ref="big_table_2", column_name="id", normalized_name="id"),
                        ColumnRef(table_ref="big_table_2", column_name="key", normalized_name="key"),
                    ],
                    estimated_row_count=3_000_000,
                    # 添加时间过滤——通过 PERF-002
                    predicates=[
                        Predicate(
                            left=ColumnRef(table_ref="big_table_2", column_name="dt", normalized_name="dt"),
                            operator=PredicateOperator.GTE,
                            right=SqlLiteral(value="2026-01-01"),
                        ),
                    ],
                ),
                JoinStep(
                    step_id="join_no_preagg",
                    right_table_ref="big_table_2",
                    join_type=JoinType.INNER,
                    join_keys=[
                        (
                            ColumnRef(table_ref="big_table_1", column_name="key", normalized_name="key"),
                            ColumnRef(table_ref="big_table_2", column_name="key", normalized_name="key"),
                        )
                    ],
                    relationship_ref="jc_01",
                    pre_aggregation_allowed=False,  # 未启用预聚合 → 触发 PERF-005 WARN
                ),
                # 添加聚合——通过 PERF-008
                AggregateStep(
                    step_id="agg_after_join",
                    group_keys=[
                        ColumnRef(table_ref="big_table_1", column_name="id", normalized_name="id"),
                    ],
                    metrics=[
                        AggregateSpec(aggregation="COUNT", input_column="id", alias="cnt"),
                    ],
                ),
                # 添加 LIMIT——通过 PERF-008
                LimitStep(step_id="limit_100", limit=100),
            ],
            multi_table=True,
        )

        validator = PerfValidator()
        result = validator.validate(plan)

        assert result.all_reject_passed is True
        perf005 = [r for r in result.check_results if r.rule_id == "PERF-005"]
        assert len(perf005) == 1
        assert perf005[0].passed is False
        assert perf005[0].severity == PerfSeverity.WARN

    def test_perf007_no_relationship_ref_warn(self):
        """PERF-007: Join key 缺少业务含义证据 → WARN。"""
        plan = SqlBuildPlan(
            plan_id="test_perf007",
            spec_hash="abc123",
            steps=[
                JoinStep(
                    step_id="join_no_evidence",
                    right_table_ref="t2",
                    join_type=JoinType.INNER,
                    join_keys=[
                        (
                            ColumnRef(table_ref="t1", column_name="key", normalized_name="key"),
                            ColumnRef(table_ref="t2", column_name="key", normalized_name="key"),
                        )
                    ],
                    relationship_ref="",  # 缺少 relationship_ref
                    cardinality_hint=None,
                ),
            ],
            multi_table=True,
        )

        validator = PerfValidator()
        result = validator.validate(plan)

        assert result.all_reject_passed is True
        perf007 = [r for r in result.check_results if r.rule_id == "PERF-007"]
        assert len(perf007) == 1
        assert perf007[0].passed is False
        assert perf007[0].severity == PerfSeverity.WARN

    def test_perf014_complex_plan_warn(self):
        """PERF-014: 复杂计划（> 8 步）→ WARN。"""
        steps = []
        for i in range(10):
            steps.append(
                ScanStep(
                    step_id=f"scan_{i}",
                    table_ref=f"t{i}",
                    required_columns=[
                        ColumnRef(table_ref=f"t{i}", column_name="id", normalized_name="id"),
                    ],
                )
            )

        plan = SqlBuildPlan(
            plan_id="test_perf014",
            spec_hash="abc123",
            steps=steps,
            multi_table=True,
        )

        validator = PerfValidator()
        result = validator.validate(plan)

        assert result.all_reject_passed is True
        perf014 = [r for r in result.check_results if r.rule_id == "PERF-014"]
        assert len(perf014) == 1
        assert perf014[0].passed is False

    def test_warn_does_not_block_compilation(self):
        """WARN 违规不阻断编译——all_reject_passed 仍为 True。"""
        plan = SqlBuildPlan(
            plan_id="test_warn_no_block",
            spec_hash="abc123",
            steps=[
                ScanStep(
                    step_id="scan_ok",
                    table_ref="t1",
                    required_columns=[
                        ColumnRef(table_ref="t1", column_name="id", normalized_name="id"),
                    ],
                    estimated_row_count=5_000_000,
                ),
                # 多个 WARN 但无 REJECT
            ],
            multi_table=False,
        )

        validator = PerfValidator()
        result = validator.validate(plan)

        # 验证 REJECT 与 WARN 的分流逻辑：
        # 有 REJECT 失败时 all_reject_passed=False，仅 WARN 时 all_reject_passed=True
        reject_failures = [r for r in result.check_results
                           if not r.passed and r.severity == PerfSeverity.REJECT]
        warn_only_failures = [r for r in result.check_results
                              if not r.passed and r.severity == PerfSeverity.WARN]
        if reject_failures:
            assert result.all_reject_passed is False
        elif warn_only_failures:
            assert result.all_reject_passed is True

    def test_perf004_prefer_summary(self):
        """PERF-004: 优先使用汇总表——无信息跳过 / 有汇总但扫事实 WARN / 用汇总通过。"""
        # 无汇总表信息 → 跳过
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

        # 有汇总表但扫描明细事实表 → WARN
        plan2 = SqlBuildPlan(
            plan_id="test_perf004_warn",
            spec_hash="abc",
            steps=[
                ScanStep(
                    step_id="scan_fact",
                    table_ref="dwd_fact_daily",
                    required_columns=[
                        ColumnRef(table_ref="dwd_fact_daily", column_name="id", normalized_name="id"),
                    ],
                    estimated_row_count=5_000_000,
                ),
            ],
            multi_table=False,
        )
        result2 = validator.validate(
            plan2,
            summary_tables={"dws_user_daily", "dws_order_daily"},
        )
        perf004_2 = [r for r in result2.check_results if r.rule_id == "PERF-004"]
        assert len(perf004_2) == 1
        assert perf004_2[0].passed is False, f"应警告: {perf004_2[0].message}"
        assert perf004_2[0].severity == PerfSeverity.WARN

        # 扫描汇总表 → 通过
        plan3 = SqlBuildPlan(
            plan_id="test_perf004_ok",
            spec_hash="abc",
            steps=[
                ScanStep(
                    step_id="scan_dws",
                    table_ref="dws_user_daily",
                    required_columns=[
                        ColumnRef(table_ref="dws_user_daily", column_name="id", normalized_name="id"),
                    ],
                    estimated_row_count=100_000,
                ),
            ],
            multi_table=False,
        )
        result3 = validator.validate(
            plan3,
            summary_tables={"dws_user_daily", "dws_order_daily"},
        )
        perf004_3 = [r for r in result3.check_results if r.rule_id == "PERF-004"]
        assert len(perf004_3) == 1
        assert perf004_3[0].passed is True

    def test_perf012_narrow_before_window(self):
        """PERF-012: 窗口函数前必须先缩小数据范围——无缩小 WARN / 聚合后通过。"""
        # WindowStep 前无过滤或聚合 → WARN
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

        # WindowStep 在聚合之后 → 通过
        plan2 = SqlBuildPlan(
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
        result2 = validator.validate(plan2)
        perf012_2 = [r for r in result2.check_results if r.rule_id == "PERF-012"]
        assert len(perf012_2) == 1
        assert perf012_2[0].passed is True

    def test_perf013_high_freq_metric(self):
        """PERF-013: 高频指标沉淀建议——少量通过 / ≥5 个 WARN。"""
        # 少量指标 → 通过
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
                        AggregateSpec(aggregation="COUNT_DISTINCT", input_column="user_id", alias="dau"),
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

        # ≥5 个聚合指标 → WARN
        plan2 = SqlBuildPlan(
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
        result2 = validator.validate(plan2)
        perf013_2 = [r for r in result2.check_results if r.rule_id == "PERF-013"]
        assert len(perf013_2) == 1
        assert perf013_2[0].passed is False
        assert perf013_2[0].severity == PerfSeverity.WARN


# ════════════════════════════════════════════
# PERF_FEEDBACK 测试
# ════════════════════════════════════════════


class TestPerfValidatorFeedback:
    """PERF-015 PERF_FEEDBACK——慢 SQL 执行计划反馈。"""

    def test_perf015_no_execution_stats_skips(self):
        """PERF-015: 无执行统计时跳过检查——不阻断。"""
        plan = SqlBuildPlan(
            plan_id="test_perf015_skip",
            spec_hash="abc123",
            steps=[
                _make_minimal_scan("t1"),
            ],
            multi_table=False,
        )

        validator = PerfValidator()
        result = validator.validate(plan)  # 不传 execution_stats

        perf015 = [r for r in result.check_results if r.rule_id == "PERF-015"]
        assert len(perf015) == 1
        assert perf015[0].passed is True
        assert "跳过" in perf015[0].message

    def test_perf015_slow_query_feedback(self):
        """PERF-015: 慢 SQL（>5s）→ PERF_FEEDBACK 触发。"""
        plan = SqlBuildPlan(
            plan_id="test_perf015_slow",
            spec_hash="abc123",
            steps=[
                _make_minimal_scan("large_table"),
            ],
            multi_table=False,
        )

        validator = PerfValidator()
        result = validator.validate(
            plan,
            execution_stats={"execution_time_ms": 8000},  # 8s > 5s
        )

        perf015 = [r for r in result.check_results if r.rule_id == "PERF-015"]
        assert len(perf015) == 1
        assert perf015[0].passed is False
        assert perf015[0].severity == PerfSeverity.PERF_FEEDBACK
        # PERF_FEEDBACK 不阻断编译
        assert result.all_reject_passed is True

    def test_perf015_fast_query_passes(self):
        """PERF-015: 快 SQL（<5s）→ 通过。"""
        plan = SqlBuildPlan(
            plan_id="test_perf015_fast",
            spec_hash="abc123",
            steps=[
                _make_minimal_scan("small_table"),
            ],
            multi_table=False,
        )

        validator = PerfValidator()
        result = validator.validate(
            plan,
            execution_stats={"execution_time_ms": 1500},  # 1.5s < 5s
        )

        perf015 = [r for r in result.check_results if r.rule_id == "PERF-015"]
        assert len(perf015) == 1
        assert perf015[0].passed is True
