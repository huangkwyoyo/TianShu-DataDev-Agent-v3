"""MetricFilterDecl filter 全链路传播测试——Mapper + Compiler。"""

from tianshu_datadev.artifacts.models import ContractAggregation
from tianshu_datadev.developer_spec.models import MetricFilterDecl
from tianshu_datadev.spark.compiler import SparkCompiler
from tianshu_datadev.spark.mapper import _map_aggregations
from tianshu_datadev.spark.models import (
    SparkAggFunction,
    SparkAggregateSpec,
    SparkAggregateStep,
    SparkPlan,
    SparkReadStep,
)


class TestMapAggregationsWithFilter:
    """Mapper——Contract -> Spark filter 映射。"""

    def test_filter_propagated_to_spark_aggregate_spec(self):
        """带 filter 的 ContractAggregation -> SparkAggregateSpec.filter 正确映射。"""
        result = _map_aggregations(
            aggregations=[
                ContractAggregation(
                    function="COUNT",
                    alias="anomaly_trip_count",
                    filter=MetricFilterDecl(
                        column="is_time_anomaly",
                        operator="eq",
                        value="true",
                    ),
                ),
            ],
            grouping_keys=["borough"],
            time_transforms=[],
        )
        step = result[0]
        assert len(step.metrics) == 1
        assert step.metrics[0].filter is not None
        assert step.metrics[0].filter.column == "is_time_anomaly"
        assert step.metrics[0].filter.operator == "eq"

    def test_filter_none_when_not_set(self):
        """无 filter 的 ContractAggregation -> SparkAggregateSpec.filter=None。"""
        result = _map_aggregations(
            aggregations=[
                ContractAggregation(function="COUNT", alias="trip_count"),
            ],
            grouping_keys=["borough"],
            time_transforms=[],
        )
        step = result[0]
        assert step.metrics[0].filter is None


class TestCompileAggregateWithFilter:
    """Compiler——Spark filter 代码生成。"""

    def test_compile_eq_filter_generates_when(self):
        """eq filter -> F.when(F.col("col") == F.lit("val"), ...)"""
        plan = SparkPlan(
            plan_id="test_filter",
            source_contract_hash="abc",
            steps=[
                SparkReadStep(source_name="ft", alias="ft", input_key="ft"),
                SparkAggregateStep(
                    input_alias="ft",
                    group_keys=["borough"],
                    metrics=[
                        SparkAggregateSpec(
                            function=SparkAggFunction.COUNT,
                            input_column=None,
                            alias="anomaly_trip_count",
                            filter=MetricFilterDecl(
                                column="is_time_anomaly",
                                operator="eq",
                                value="true",
                            ),
                        ),
                    ],
                ),
            ],
        )
        compiler = SparkCompiler()
        result = compiler.compile(plan)
        code = result.raw_pyspark
        # 应包含 F.when(condition, F.lit(1)) 条件聚合
        assert "F.when(" in code
        assert 'F.col("is_time_anomaly")' in code
        assert "anomaly_trip_count" in code

    def test_compile_no_filter_no_when(self):
        """无 filter 的聚合不生成 F.when。"""
        plan = SparkPlan(
            plan_id="test_no_filter",
            source_contract_hash="abc",
            steps=[
                SparkReadStep(source_name="ft", alias="ft", input_key="ft"),
                SparkAggregateStep(
                    input_alias="ft",
                    group_keys=["borough"],
                    metrics=[
                        SparkAggregateSpec(
                            function=SparkAggFunction.COUNT,
                            input_column=None,
                            alias="trip_count",
                        ),
                    ],
                ),
            ],
        )
        compiler = SparkCompiler()
        result = compiler.compile(plan)
        code = result.raw_pyspark
        # 无 filter 时不生成 F.when
        assert "F.when(" not in code
        assert "trip_count" in code
