"""PlanComparator 测试——Aggregate + Window 步骤等价性对比。

从 test_plan_comparator.py 拆分（Phase 6.2 Comparator 文件拆分）。
公共构建器见 tests/spark/plan_comparator_fixtures.py。
"""

from __future__ import annotations

from tests.spark.plan_comparator_fixtures import (
    _make_spark_plan,
    _make_spark_read_step,
    _make_spark_window_step,
    _make_sql_plan,
    _make_sql_scan_step,
)
from tianshu_datadev.planning.models import (
    AggregateSpec,
    AggregationType,
    ColumnRef,
    SortDirection,
    SortSpec,
)
from tianshu_datadev.spark.models import (
    SparkStepType,
)
from tianshu_datadev.spark.plan_comparator import (
    ComparisonStatus,
    PlanComparator,
)
from tianshu_datadev.spark.plan_equivalence import EquivalenceVerdict


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

        sql_plan = _make_sql_plan(
            [
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
            ]
        )
        spark_plan = _make_spark_plan(
            [
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
            ]
        )

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

        sql_plan = _make_sql_plan(
            [
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
            ]
        )
        spark_plan = _make_spark_plan(
            [
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
            ]
        )

        comparator = PlanComparator()
        report = comparator.compare(sql_plan, spark_plan)

        assert report.status == ComparisonStatus.LOGIC_EQUIVALENT

    def test_aggregate_multi_step_not_crash(self):
        """多 aggregate step 通过 PlanComparator.compare 时不崩溃。

        SQL 侧两个不同粒度的 aggregate（dept_id, region），Spark 侧仅一个（dept_id）。
        compare_aggregate_steps 发现数量不一致（2 vs 1），返回 NOT_EQUIVALENT，
        不会触发行 376 的断言（该断言仅当 sql_count == spark_count != 1 时触发）。
        """
        from tianshu_datadev.planning.sql_build_plan import AggregateStep
        from tianshu_datadev.spark.models import (
            SparkAggFunction,
            SparkAggregateSpec,
            SparkAggregateStep,
        )

        # SQL 侧：两个不同粒度的 aggregate step
        sql_agg_1 = AggregateStep(
            step_type="aggregate", step_id="agg_001",
            group_keys=[
                ColumnRef(
                    table_ref="od", column_name="dept_id",
                    normalized_name="dept_id",
                ),
            ],
            metrics=[
                AggregateSpec(
                    aggregation=AggregationType.COUNT,
                    input_column="id",
                    alias="cnt",
                ),
            ],
        )
        sql_agg_2 = AggregateStep(
            step_type="aggregate", step_id="agg_002",
            group_keys=[
                ColumnRef(
                    table_ref="od", column_name="region",
                    normalized_name="region",
                ),
            ],
            metrics=[
                AggregateSpec(
                    aggregation=AggregationType.SUM,
                    input_column="amount",
                    alias="total",
                ),
            ],
        )
        sql_plan = _make_sql_plan([_make_sql_scan_step(), sql_agg_1, sql_agg_2])

        # Spark 侧：单 aggregate
        spark_agg = SparkAggregateStep(
            step_type=SparkStepType.AGGREGATE,
            input_alias="od",
            group_keys=["dept_id"],
            metrics=[
                SparkAggregateSpec(
                    function=SparkAggFunction.COUNT,
                    input_column="id",
                    alias="cnt",
                ),
            ],
        )
        spark_plan = _make_spark_plan([_make_spark_read_step(), spark_agg])

        comparator = PlanComparator()
        report = comparator.compare(sql_plan, spark_plan)

        # 不得崩溃，应有结构化结果
        agg_results = [r for r in report.step_results if r.step_type == "aggregate"]
        assert len(agg_results) > 0
        assert agg_results[0].verdict in (
            EquivalenceVerdict.NOT_EQUIVALENT,
            EquivalenceVerdict.UNSUPPORTED_COMPARISON,
        ), f"多 aggregate 不得崩溃或误判 EQUIVALENT, 实际={agg_results[0].verdict}"


# ════════════════════════════════════════════
# PlanComparator——CaseWhen 逻辑对比测试
# ════════════════════════════════════════════

class TestPlanComparatorWindowEquivalence:
    """Window 逻辑等价性对比——全管道集成测试（真实模型）。

    覆盖 frame 合并/input_column 扁平化/order_by 空值处理。
    """

    def test_window_frame_equivalent(self):
        """SQL WindowFrame dict + Spark frame_type/frame_start/frame_end → frame 合并后等价。"""
        from tianshu_datadev.planning.models import (
            ColumnRef,
            FrameBoundary,
            FrameBoundaryKind,
            WindowExpr,
            WindowFrame,
            WindowFrameType,
            WindowFunction,
        )
        from tianshu_datadev.planning.sql_build_plan import WindowStep
        from tianshu_datadev.spark.models import (
            SparkWindowExpr,
            SparkWindowFunction,
        )

        # SQL 侧：使用 WindowFrame dict
        sql_window = WindowStep(
            step_type="window", step_id="step_win_001",
            window_exprs=[
                WindowExpr(
                    function=WindowFunction.SUM_OVER,
                    alias="total",
                    input=ColumnRef(
                        table_ref="od", column_name="amount", normalized_name="amount",
                    ),
                    partition_by=[
                        ColumnRef(
                            table_ref="od", column_name="dept_id", normalized_name="dept_id",
                        ),
                    ],
                    order_by=[SortSpec(column="salary", direction=SortDirection.ASC)],
                    frame=WindowFrame(
                        frame_type=WindowFrameType.ROWS,
                        start=FrameBoundary(kind=FrameBoundaryKind.UNBOUNDED_PRECEDING),
                        end=FrameBoundary(kind=FrameBoundaryKind.CURRENT_ROW),
                    ),
                ),
            ],
        )
        sql_plan = _make_sql_plan([_make_sql_scan_step(), sql_window])

        # Spark 侧：使用分离 frame 字符串字段
        spark_window = _make_spark_window_step(
            expressions=[
                SparkWindowExpr(
                    function=SparkWindowFunction.SUM_OVER,
                    alias="total",
                    input_column="amount",
                    partition_by=["dept_id"],
                    order_by=["salary ASC LAST"],
                    frame_type="ROWS",
                    frame_start="unbounded_preceding",
                    frame_end="current_row",
                ),
            ],
        )
        spark_plan = _make_spark_plan([_make_spark_read_step(), spark_window])

        comparator = PlanComparator()
        report = comparator.compare(sql_plan, spark_plan)

        assert report.status == ComparisonStatus.LOGIC_EQUIVALENT, (
            f"相同 frame 应等价，实际 status={report.status}, "
            f"results={[(r.step_type, r.verdict.value) for r in report.step_results]}"
        )

    def test_window_frame_diff_not_equivalent(self):
        """SQL ROWS vs Spark RANGE → LOGIC_MISMATCH。"""
        from tianshu_datadev.planning.models import (
            ColumnRef,
            FrameBoundary,
            FrameBoundaryKind,
            WindowExpr,
            WindowFrame,
            WindowFrameType,
            WindowFunction,
        )
        from tianshu_datadev.planning.sql_build_plan import WindowStep
        from tianshu_datadev.spark.models import (
            SparkWindowExpr,
            SparkWindowFunction,
        )

        sql_window = WindowStep(
            step_type="window", step_id="step_win_001",
            window_exprs=[
                WindowExpr(
                    function=WindowFunction.SUM_OVER,
                    alias="total",
                    input=ColumnRef(
                        table_ref="od", column_name="amount", normalized_name="amount",
                    ),
                    partition_by=[
                        ColumnRef(
                            table_ref="od", column_name="dept_id", normalized_name="dept_id",
                        ),
                    ],
                    order_by=[SortSpec(column="salary", direction=SortDirection.ASC)],
                    frame=WindowFrame(
                        frame_type=WindowFrameType.ROWS,
                        start=FrameBoundary(kind=FrameBoundaryKind.UNBOUNDED_PRECEDING),
                        end=FrameBoundary(kind=FrameBoundaryKind.CURRENT_ROW),
                    ),
                ),
            ],
        )
        sql_plan = _make_sql_plan([_make_sql_scan_step(), sql_window])

        # Spark 侧：使用 RANGE（与 SQL 的 ROWS 不同）
        spark_window = _make_spark_window_step(
            expressions=[
                SparkWindowExpr(
                    function=SparkWindowFunction.SUM_OVER,
                    alias="total",
                    input_column="amount",
                    partition_by=["dept_id"],
                    order_by=["salary ASC LAST"],
                    frame_type="RANGE",
                    frame_start="unbounded_preceding",
                    frame_end="current_row",
                ),
            ],
        )
        spark_plan = _make_spark_plan([_make_spark_read_step(), spark_window])

        comparator = PlanComparator()
        report = comparator.compare(sql_plan, spark_plan)

        assert report.status == ComparisonStatus.LOGIC_MISMATCH, (
            f"ROWS vs RANGE 应不等价，实际 status={report.status}"
        )

    def test_window_order_reversed_not_equivalent(self):
        """ORDER BY salary DESC, name ASC vs name ASC, salary DESC → LOGIC_MISMATCH。"""
        from tianshu_datadev.planning.models import (
            ColumnRef,
            FrameBoundary,
            FrameBoundaryKind,
            WindowExpr,
            WindowFrame,
            WindowFrameType,
            WindowFunction,
        )
        from tianshu_datadev.planning.sql_build_plan import WindowStep
        from tianshu_datadev.spark.models import (
            SparkWindowExpr,
            SparkWindowFunction,
        )

        sql_window = WindowStep(
            step_type="window", step_id="step_win_002",
            window_exprs=[
                WindowExpr(
                    function=WindowFunction.SUM_OVER,
                    alias="total",
                    input=ColumnRef(
                        table_ref="od", column_name="amount", normalized_name="amount",
                    ),
                    partition_by=[
                        ColumnRef(
                            table_ref="od", column_name="dept_id", normalized_name="dept_id",
                        ),
                    ],
                    order_by=[
                        SortSpec(column="salary", direction=SortDirection.DESC),
                        SortSpec(column="name", direction=SortDirection.ASC),
                    ],
                    frame=WindowFrame(
                        frame_type=WindowFrameType.ROWS,
                        start=FrameBoundary(kind=FrameBoundaryKind.UNBOUNDED_PRECEDING),
                        end=FrameBoundary(kind=FrameBoundaryKind.CURRENT_ROW),
                    ),
                ),
            ],
        )
        sql_plan = _make_sql_plan([_make_sql_scan_step(), sql_window])

        # Spark 侧：顺序相反
        spark_window = _make_spark_window_step(
            expressions=[
                SparkWindowExpr(
                    function=SparkWindowFunction.SUM_OVER,
                    alias="total",
                    input_column="amount",
                    partition_by=["dept_id"],
                    order_by=["name ASC LAST", "salary DESC LAST"],
                    frame_type="ROWS",
                    frame_start="unbounded_preceding",
                    frame_end="current_row",
                ),
            ],
        )
        spark_plan = _make_spark_plan([_make_spark_read_step(), spark_window])

        comparator = PlanComparator()
        report = comparator.compare(sql_plan, spark_plan)

        assert report.status == ComparisonStatus.LOGIC_MISMATCH, (
            f"ORDER BY 顺序不同应不等价，实际 status={report.status}"
        )

    def test_window_input_column_diff_not_equivalent(self):
        """SUM(amount) vs SUM(discount) → LOGIC_MISMATCH。"""
        from tianshu_datadev.planning.models import (
            ColumnRef,
            FrameBoundary,
            FrameBoundaryKind,
            WindowExpr,
            WindowFrame,
            WindowFrameType,
            WindowFunction,
        )
        from tianshu_datadev.planning.sql_build_plan import WindowStep
        from tianshu_datadev.spark.models import (
            SparkWindowExpr,
            SparkWindowFunction,
        )

        sql_window = WindowStep(
            step_type="window", step_id="step_win_003",
            window_exprs=[
                WindowExpr(
                    function=WindowFunction.SUM_OVER,
                    alias="total",
                    input=ColumnRef(
                        table_ref="od", column_name="amount", normalized_name="amount",
                    ),
                    partition_by=[
                        ColumnRef(
                            table_ref="od", column_name="dept_id", normalized_name="dept_id",
                        ),
                    ],
                    order_by=[SortSpec(column="salary", direction=SortDirection.ASC)],
                    frame=WindowFrame(
                        frame_type=WindowFrameType.ROWS,
                        start=FrameBoundary(kind=FrameBoundaryKind.UNBOUNDED_PRECEDING),
                        end=FrameBoundary(kind=FrameBoundaryKind.CURRENT_ROW),
                    ),
                ),
            ],
        )
        sql_plan = _make_sql_plan([_make_sql_scan_step(), sql_window])

        # Spark 侧：input_column 为 "discount"（与 SQL 的 "amount" 不同）
        spark_window = _make_spark_window_step(
            expressions=[
                SparkWindowExpr(
                    function=SparkWindowFunction.SUM_OVER,
                    alias="total",
                    input_column="discount",
                    partition_by=["dept_id"],
                    order_by=["salary ASC LAST"],
                    frame_type="ROWS",
                    frame_start="unbounded_preceding",
                    frame_end="current_row",
                ),
            ],
        )
        spark_plan = _make_spark_plan([_make_spark_read_step(), spark_window])

        comparator = PlanComparator()
        report = comparator.compare(sql_plan, spark_plan)

        assert report.status == ComparisonStatus.LOGIC_MISMATCH, (
            f"不同 input_column 应不等价，实际 status={report.status}"
        )

    def test_window_full_equivalent(self):
        """完整 window（partition + order + frame + input）→ LOGIC_EQUIVALENT。"""
        from tianshu_datadev.planning.models import (
            ColumnRef,
            FrameBoundary,
            FrameBoundaryKind,
            WindowExpr,
            WindowFrame,
            WindowFrameType,
            WindowFunction,
        )
        from tianshu_datadev.planning.sql_build_plan import WindowStep
        from tianshu_datadev.spark.models import (
            SparkWindowExpr,
            SparkWindowFunction,
        )

        # 完整的三个窗口表达式
        sql_window = WindowStep(
            step_type="window", step_id="step_win_full",
            window_exprs=[
                WindowExpr(
                    function=WindowFunction.SUM_OVER,
                    alias="total_amt",
                    input=ColumnRef(
                        table_ref="od", column_name="amount", normalized_name="amount",
                    ),
                    partition_by=[
                        ColumnRef(
                            table_ref="od", column_name="dept_id", normalized_name="dept_id",
                        ),
                    ],
                    order_by=[SortSpec(column="salary", direction=SortDirection.ASC)],
                    frame=WindowFrame(
                        frame_type=WindowFrameType.ROWS,
                        start=FrameBoundary(kind=FrameBoundaryKind.UNBOUNDED_PRECEDING),
                        end=FrameBoundary(kind=FrameBoundaryKind.CURRENT_ROW),
                    ),
                ),
                WindowExpr(
                    function=WindowFunction.RANK,
                    alias="rnk",
                    partition_by=[
                        ColumnRef(
                            table_ref="od", column_name="dept_id", normalized_name="dept_id",
                        ),
                    ],
                    order_by=[SortSpec(column="salary", direction=SortDirection.DESC)],
                    frame=WindowFrame(
                        frame_type=WindowFrameType.ROWS,
                        start=FrameBoundary(kind=FrameBoundaryKind.UNBOUNDED_PRECEDING),
                        end=FrameBoundary(kind=FrameBoundaryKind.CURRENT_ROW),
                    ),
                ),
            ],
        )
        sql_plan = _make_sql_plan([_make_sql_scan_step(), sql_window])

        spark_window = _make_spark_window_step(
            expressions=[
                SparkWindowExpr(
                    function=SparkWindowFunction.SUM_OVER,
                    alias="total_amt",
                    input_column="amount",
                    partition_by=["dept_id"],
                    order_by=["salary ASC LAST"],
                    frame_type="ROWS",
                    frame_start="unbounded_preceding",
                    frame_end="current_row",
                ),
                SparkWindowExpr(
                    function=SparkWindowFunction.RANK,
                    alias="rnk",
                    partition_by=["dept_id"],
                    order_by=["salary DESC LAST"],
                ),
            ],
        )
        spark_plan = _make_spark_plan([_make_spark_read_step(), spark_window])

        comparator = PlanComparator()
        report = comparator.compare(sql_plan, spark_plan)

        assert report.status == ComparisonStatus.LOGIC_EQUIVALENT, (
            f"完整 window 应等价，实际 status={report.status}, "
            f"results={[(r.step_type, r.verdict.value) for r in report.step_results]}"
        )

    def test_window_no_frame_no_partition(self):
        """无 partition 的 ROW_NUMBER 使用默认 frame → 不崩溃，等价。"""
        from tianshu_datadev.planning.models import (
            FrameBoundary,
            FrameBoundaryKind,
            WindowExpr,
            WindowFrame,
            WindowFrameType,
            WindowFunction,
        )
        from tianshu_datadev.planning.sql_build_plan import WindowStep
        from tianshu_datadev.spark.models import (
            SparkWindowExpr,
            SparkWindowFunction,
        )

        # SQL 侧：ROW_NUMBER 无 partition，使用默认 frame
        sql_window = WindowStep(
            step_type="window", step_id="step_win_no",
            window_exprs=[
                WindowExpr(
                    function=WindowFunction.ROW_NUMBER,
                    alias="rn",
                    frame=WindowFrame(
                        frame_type=WindowFrameType.ROWS,
                        start=FrameBoundary(kind=FrameBoundaryKind.UNBOUNDED_PRECEDING),
                        end=FrameBoundary(kind=FrameBoundaryKind.CURRENT_ROW),
                    ),
                ),
            ],
        )
        sql_plan = _make_sql_plan([_make_sql_scan_step(), sql_window])

        # Spark 侧：使用 frame 默认值
        spark_window = _make_spark_window_step(
            expressions=[
                SparkWindowExpr(
                    function=SparkWindowFunction.ROW_NUMBER,
                    alias="rn",
                    frame_type="ROWS",
                    frame_start="unbounded_preceding",
                    frame_end="current_row",
                ),
            ],
        )
        spark_plan = _make_spark_plan([_make_spark_read_step(), spark_window])

        comparator = PlanComparator()
        report = comparator.compare(sql_plan, spark_plan)

        assert report.status == ComparisonStatus.LOGIC_EQUIVALENT, (
            f"ROW_NUMBER 默认 frame 应等价，实际 status={report.status}"
        )


# ════════════════════════════════════════════
# Task 4（B 类）— Filter 右侧谓词 tree 修复辅助测试
# ════════════════════════════════════════════
