"""PlanComparator 测试——Scan + Join 步骤等价性对比。

从 test_plan_comparator.py 拆分（Phase 6.2 Comparator 文件拆分）。
公共构建器见 tests/spark/plan_comparator_fixtures.py。
"""

from __future__ import annotations

from tests.spark.plan_comparator_fixtures import (
    _make_spark_plan,
    _make_spark_read_step,
    _make_sql_plan,
    _make_sql_scan_step,
)
from tianshu_datadev.planning.models import (
    ColumnRef,
)
from tianshu_datadev.planning.sql_build_plan import (
    ScanStep,
)
from tianshu_datadev.spark.models import (
    SparkReadStep,
    SparkStepType,
)
from tianshu_datadev.spark.plan_comparator import (
    ComparisonStatus,
    PlanComparator,
)
from tianshu_datadev.spark.plan_equivalence import EquivalenceVerdict


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
            r.step_type == "scan" and r.verdict == EquivalenceVerdict.EQUIVALENT for r in report.step_results
        )

    def test_scan_read_not_equivalent_different_alias(self):
        """不同表别名 → LOGIC_MISMATCH。"""
        sql_plan = _make_sql_plan([_make_sql_scan_step()])
        spark_plan = _make_spark_plan(
            [
                _make_spark_read_step(alias="different_alias"),
            ]
        )

        comparator = PlanComparator()
        report = comparator.compare(sql_plan, spark_plan)

        assert report.status == ComparisonStatus.LOGIC_MISMATCH

    def test_scan_columns_mismatch_detected_by_alias(self):
        """同 alias 下列集合不一致 → NOT_EQUIVALENT。"""
        # SQL 侧：读 3 列
        sql_plan = _make_sql_plan([
            ScanStep(
                step_type="scan",
                step_id="step_scan_001",
                table_ref="od",
                required_columns=[
                    ColumnRef(table_ref="od", column_name="order_id", normalized_name="order_id"),
                    ColumnRef(table_ref="od", column_name="amount", normalized_name="amount"),
                    ColumnRef(table_ref="od", column_name="status", normalized_name="status"),
                ],
            ),
        ])
        # Spark 侧：只读 2 列（缺少 status）
        spark_plan = _make_spark_plan([
            SparkReadStep(
                step_type=SparkStepType.READ,
                alias="od",
                source_name="order_info",
                input_key="order_info_key",
                required_columns=["order_id", "amount"],
            ),
        ])

        comparator = PlanComparator()
        report = comparator.compare(sql_plan, spark_plan)

        # 列集合不同 → NOT_EQUIVALENT
        scan_results = [r for r in report.step_results if r.step_type == "scan"]
        assert len(scan_results) == 1
        assert scan_results[0].verdict == EquivalenceVerdict.NOT_EQUIVALENT
        assert "status" in scan_results[0].detail

class TestPlanComparatorJoinEquivalence:
    """Join ↔ Join 逻辑等价性对比（Phase 7B 收口）。"""

    def test_join_not_equivalent_different_aliases(self):
        """不同别名 → LOGIC_MISMATCH（SQL 侧用 table_ref，Spark 侧用 alias）。"""
        from tianshu_datadev.planning.sql_build_plan import JoinStep
        from tianshu_datadev.spark.models import SparkJoinStep, SparkJoinType

        sql_plan = _make_sql_plan(
            [
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
            ]
        )
        spark_plan = _make_spark_plan(
            [
                _make_spark_read_step(),
                SparkJoinStep(
                    step_type=SparkStepType.JOIN,
                    left_alias="od",
                    right_alias="up",
                    left_key="user_id",
                    right_key="user_id",
                    join_type=SparkJoinType.INNER,
                ),
            ]
        )

        comparator = PlanComparator()
        report = comparator.compare(sql_plan, spark_plan)

        # SQL left_table_ref="order_info" ≠ Spark left_alias="od" → LOGIC_MISMATCH
        assert report.status == ComparisonStatus.LOGIC_MISMATCH
        assert "join" in [r.step_type for r in report.step_results]

    def test_join_equivalent_same_keys(self):
        """相同 join 键和类型 → LOGIC_EQUIVALENT。"""
        from tianshu_datadev.planning.sql_build_plan import JoinStep
        from tianshu_datadev.spark.models import SparkJoinStep, SparkJoinType

        sql_plan = _make_sql_plan(
            [
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
            ]
        )
        spark_plan = _make_spark_plan(
            [
                _make_spark_read_step(),
                SparkJoinStep(
                    step_type=SparkStepType.JOIN,
                    left_alias="od",
                    right_alias="od",
                    left_key="user_id",
                    right_key="user_id",
                    join_type=SparkJoinType.INNER,
                ),
            ]
        )

        comparator = PlanComparator()
        report = comparator.compare(sql_plan, spark_plan)

        assert report.status == ComparisonStatus.LOGIC_EQUIVALENT


# ════════════════════════════════════════════
# PlanComparator——Aggregate 逻辑对比测试
# ════════════════════════════════════════════
