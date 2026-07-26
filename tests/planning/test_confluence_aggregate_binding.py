"""合流聚合的临时表绑定回归测试。"""

from tianshu_datadev.planning.models import AggregateSpec, ColumnRef
from tianshu_datadev.planning.sql_build_plan import (
    AggregateStep,
    SqlBuildPlanBuilder,
)
from tianshu_datadev.planning.temp_table import make_temp_name


def test_confluence_aggregate_binds_metric_and_passthrough_columns():
    """合流指标绑定实际 temp，透传的上游聚合列保持原粒度。"""
    chain_id = "c9bba4f6"
    crash_source = "zone_crash_stats"
    trip_source = "__bridge_td_tz_zone_risk_assessment"
    aggregate = AggregateStep(
        step_id="aggregate_final",
        group_keys=[
            ColumnRef(
                table_ref=make_temp_name(chain_id, crash_source),
                column_name="borough",
                normalized_name="borough",
            ),
        ],
        metrics=[
            AggregateSpec(
                aggregation="COUNT",
                input_column="trip_id",
                alias="total_trips",
                source_table="td",
            ),
        ],
    )
    step_outputs = {
        crash_source: [
            ColumnRef(
                table_ref="",
                column_name="borough",
                normalized_name="borough",
            ),
            ColumnRef(
                table_ref="",
                column_name="crash_count",
                normalized_name="crash_count",
            ),
            ColumnRef(
                table_ref="",
                column_name="severity_score",
                normalized_name="severity_score",
            ),
        ],
        trip_source: [
            ColumnRef(
                table_ref="",
                column_name="borough",
                normalized_name="borough",
            ),
            ColumnRef(
                table_ref="",
                column_name="trip_id",
                normalized_name="trip_id",
            ),
        ],
    }

    SqlBuildPlanBuilder()._bind_confluence_aggregate(
        aggregate,
        sources=[crash_source, trip_source],
        step_outputs=step_outputs,
        chain_id=chain_id,
        output_columns=[
            "borough",
            "crash_count",
            "severity_score",
            "total_trips",
        ],
    )

    assert aggregate.metrics[0].source_table == make_temp_name(
        chain_id, trip_source
    )
    grouped = {
        (str(group.table_ref), str(group.column_name))
        for group in aggregate.group_keys
    }
    crash_temp = make_temp_name(chain_id, crash_source)
    assert (crash_temp, "crash_count") in grouped
    assert (crash_temp, "severity_score") in grouped
