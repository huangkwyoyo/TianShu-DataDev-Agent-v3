"""测试 PerfValidator——8 条 PERF 规则验证。"""

import os

from tianshu_datadev.developer_spec.parser import DeveloperSpecParser
from tianshu_datadev.planning.models import ColumnRef, JoinType
from tianshu_datadev.planning.sql_build_plan import (
    JoinStep,
    ScanStep,
    SortStep,
    SqlBuildPlan,
)
from tianshu_datadev.sql.models import PerfRuleLevel
from tianshu_datadev.sql.perf_validator import PerfValidator

# ── 辅助 ──

def _read_fixture(path: str) -> str:
    abs_path = os.path.join(os.path.dirname(__file__), "..", path)
    with open(abs_path, "r", encoding="utf-8") as f:
        return f.read()


def _parse_spec(fixture_path: str):
    parser = DeveloperSpecParser()
    text = _read_fixture(fixture_path)
    return parser.parse(text)


# ════════════════════════════════════════════
# PERF 规则测试
# ════════════════════════════════════════════


class TestPerfValidatorReject:
    """PERF REJECT 规则——违反后阻断。"""

    def test_perf001_no_limit_full_scan_rejected(self):
        """PERF-001: 无 LIMIT 全量扫描且估算行数 > 10M → REJECT。"""
        plan = SqlBuildPlan(
            plan_id="test_perf001",
            spec_hash="abc123",
            steps=[
                ScanStep(
                    step_id="scan_big",
                    table_ref="huge_table",
                    required_columns=[
                        ColumnRef(table_ref="huge_table", column_name="id", normalized_name="id"),
                    ],
                    estimated_row_count=20_000_000,  # 20M > 10M
                ),
            ],
            multi_table=False,
        )

        validator = PerfValidator()
        all_passed, results = validator.validate(plan)

        assert all_passed is False
        perf001 = [r for r in results if r.rule_id == "PERF-001"]
        assert len(perf001) == 1
        assert perf001[0].passed is False

    def test_perf002_type_mismatch_rejected(self):
        """PERF-002: Join 键名暗示类型不一致 → REJECT。"""
        plan = SqlBuildPlan(
            plan_id="test_perf002",
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
        all_passed, results = validator.validate(plan)

        perf002 = [r for r in results if r.rule_id == "PERF-002"]
        assert len(perf002) == 1
        assert perf002[0].passed is False

    def test_perf003_noop(self):
        """PERF-003: 窗口函数规则注册但在 Phase 1C 中 no-op。"""
        plan = SqlBuildPlan(
            plan_id="test_perf003",
            spec_hash="abc123",
            steps=[
                ScanStep(
                    step_id="scan_1",
                    table_ref="t1",
                    required_columns=[
                        ColumnRef(table_ref="t1", column_name="id", normalized_name="id"),
                    ],
                ),
            ],
            multi_table=False,
        )

        validator = PerfValidator()
        all_passed, results = validator.validate(plan)

        perf003 = [r for r in results if r.rule_id == "PERF-003"]
        assert len(perf003) == 1
        assert perf003[0].passed is True  # no-op，始终通过
        assert "no-op" in perf003[0].message.lower()

    def test_perf004_no_partition_info_skips(self):
        """PERF-004: 无分区表信息时跳过检查（向后兼容）——始终通过。"""
        plan = SqlBuildPlan(
            plan_id="test_perf004_skip",
            spec_hash="abc123",
            steps=[
                ScanStep(
                    step_id="scan_1",
                    table_ref="t1",
                    required_columns=[
                        ColumnRef(table_ref="t1", column_name="id", normalized_name="id"),
                    ],
                ),
            ],
            multi_table=False,
        )

        validator = PerfValidator()
        all_passed, results = validator.validate(plan)  # 不传 partitioned_tables

        perf004 = [r for r in results if r.rule_id == "PERF-004"]
        assert len(perf004) == 1
        assert perf004[0].passed is True
        assert "跳过" in perf004[0].message

    def test_perf004_partitioned_table_no_filter_rejected(self):
        """PERF-004: 分区表缺少分区过滤 → REJECT。"""
        plan = SqlBuildPlan(
            plan_id="test_perf004_reject",
            spec_hash="abc123",
            steps=[
                ScanStep(
                    step_id="scan_partitioned",
                    table_ref="dwd_fact",
                    required_columns=[
                        ColumnRef(table_ref="dwd_fact", column_name="id", normalized_name="id"),
                    ],
                    # 无 partition_filters 且 predicates 中无分区字段
                ),
            ],
            multi_table=False,
        )

        validator = PerfValidator()
        all_passed, results = validator.validate(
            plan,
            partitioned_tables={"dwd_fact"},  # 声明为分区表
        )

        perf004 = [r for r in results if r.rule_id == "PERF-004"]
        assert len(perf004) == 1
        assert perf004[0].passed is False, f"应拒绝: {perf004[0].message}"
        assert "dwd_fact" in perf004[0].message

    def test_perf004_partitioned_table_with_filter_passes(self):
        """PERF-004: 分区表有分区过滤 → 通过。"""
        from tianshu_datadev.planning.models import Predicate, PredicateOperator, SqlLiteral

        plan = SqlBuildPlan(
            plan_id="test_perf004_pass",
            spec_hash="abc123",
            steps=[
                ScanStep(
                    step_id="scan_partitioned_ok",
                    table_ref="dwd_fact",
                    required_columns=[
                        ColumnRef(table_ref="dwd_fact", column_name="id", normalized_name="id"),
                    ],
                    predicates=[
                        Predicate(
                            left=ColumnRef(
                                table_ref="dwd_fact",
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
        all_passed, results = validator.validate(
            plan,
            partitioned_tables={"dwd_fact"},
        )

        perf004 = [r for r in results if r.rule_id == "PERF-004"]
        assert len(perf004) == 1
        assert perf004[0].passed is True, f"应通过: {perf004[0].message}"


class TestPerfValidatorWarn:
    """PERF WARN 规则——违反后记录但不阻断。"""

    def test_perf005_sort_no_limit_warn(self):
        """PERF-005: 无 LIMIT 排序 + 大输入 → WARN 但不阻断。"""
        plan = SqlBuildPlan(
            plan_id="test_perf005",
            spec_hash="abc123",
            steps=[
                ScanStep(
                    step_id="scan_big",
                    table_ref="big_table",
                    required_columns=[
                        ColumnRef(table_ref="big_table", column_name="id", normalized_name="id"),
                    ],
                    estimated_row_count=5_000_000,
                ),
                SortStep(
                    step_id="sort_big",
                    order_by=[],
                    requires_full_sort=True,
                    estimated_input_rows=5_000_000,  # 5M > 1M
                ),
            ],
            multi_table=False,
        )

        validator = PerfValidator()
        all_passed, results = validator.validate(plan)

        # PERF-005 是 WARN，不应阻断
        assert all_passed is True
        perf005 = [r for r in results if r.rule_id == "PERF-005"]
        assert len(perf005) == 1
        assert perf005[0].passed is False  # 规则未通过
        assert perf005[0].level == PerfRuleLevel.WARN

    def test_perf007_select_star_warn(self):
        """PERF-007: SELECT *（required_columns 为空）→ WARN。"""
        plan = SqlBuildPlan(
            plan_id="test_perf007",
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
        all_passed, results = validator.validate(plan)

        assert all_passed is True
        perf007 = [r for r in results if r.rule_id == "PERF-007"]
        assert len(perf007) == 1
        assert perf007[0].passed is False
        assert perf007[0].level == PerfRuleLevel.WARN

    def test_perf008_preagg_not_allowed_warn(self):
        """PERF-008: 大表 Join 未启用预聚合 → WARN。"""
        plan = SqlBuildPlan(
            plan_id="test_perf008",
            spec_hash="abc123",
            steps=[
                ScanStep(
                    step_id="scan_big",
                    table_ref="big_table",
                    required_columns=[
                        ColumnRef(table_ref="big_table", column_name="id", normalized_name="id"),
                    ],
                    estimated_row_count=5_000_000,  # 5M
                ),
                JoinStep(
                    step_id="join_no_preagg",
                    right_table_ref="small_table",
                    join_type=JoinType.INNER,
                    join_keys=[
                        (
                            ColumnRef(table_ref="big_table", column_name="key", normalized_name="key"),
                            ColumnRef(table_ref="small_table", column_name="key", normalized_name="key"),
                        )
                    ],
                    relationship_ref="jc_01",
                    pre_aggregation_allowed=False,  # 未启用预聚合
                ),
            ],
            multi_table=True,
        )

        validator = PerfValidator()
        all_passed, results = validator.validate(plan)

        assert all_passed is True
        perf008 = [r for r in results if r.rule_id == "PERF-008"]
        assert len(perf008) == 1
        assert perf008[0].passed is False
        assert perf008[0].level == PerfRuleLevel.WARN
