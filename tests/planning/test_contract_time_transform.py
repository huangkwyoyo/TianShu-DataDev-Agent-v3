"""Task 3 — TimeTransform 模型测试（Contract 侧 + Spark 侧）+ Task 6 Contract 提取器测试。"""

from tianshu_datadev.artifacts.models import (
    ContractTimeTransform,
    DataTransformContractLite,
    DataTransformContractV1,
)
from tianshu_datadev.spark.models import (
    SparkAggregateSpec,
    SparkAggregateStep,
    SparkTimeTransformExpr,
)


class TestContractTimeTransform:
    """Contract 侧时间变换模型测试。"""

    def test_valid_contract_time_transform(self):
        tt = ContractTimeTransform(
            source_column="pickup_at",
            source_table="ft",
            time_function="HOUR",
            alias="pickup_hour",
        )
        assert tt.alias == "pickup_hour"
        assert tt.type == "time_transform"

    def test_data_transform_contract_lite_has_time_transforms(self):
        """DataTransformContractLite 应有 time_transforms 字段且默认为空。"""
        lite = DataTransformContractLite(
            contract_id="test",
            source_sqlbuildplan_hash="abc",
            grouping_keys=["pickup_hour"],
        )
        assert lite.time_transforms == []

    def test_data_transform_contract_v1_has_time_transforms(self):
        """DataTransformContractV1 应有 time_transforms 字段且默认为空。"""
        v1 = DataTransformContractV1(
            contract_id="test",
            source_sqlprogram_hash="abc",
            grouping_keys=["pickup_hour"],
        )
        assert v1.time_transforms == []


class TestSparkTimeTransformExpr:
    """Spark 侧时间变换模型测试。"""

    def test_valid_spark_time_transform(self):
        tt = SparkTimeTransformExpr(
            source_column="pickup_at",
            source_table="ft",
            time_function="hour",
            alias="pickup_hour",
        )
        assert tt.time_function == "hour"


class TestSparkAggregateStepWithTimeTransforms:
    """SparkAggregateStep + time_transforms 测试。"""

    def test_default_time_transforms_empty(self):
        """默认 time_transforms 为空列表——向后兼容。"""
        step = SparkAggregateStep(
            input_alias="t1",
            group_keys=["borough"],
            metrics=[
                SparkAggregateSpec(
                    function="COUNT",
                    input_column="trip_id",
                    alias="trip_count",
                ),
            ],
        )
        assert step.time_transforms == []

    def test_with_time_transforms(self):
        """time_transforms 非空时应正确存储。"""
        step = SparkAggregateStep(
            input_alias="t1",
            group_keys=["borough"],
            metrics=[],
            time_transforms=[
                SparkTimeTransformExpr(
                    source_column="pickup_at",
                    source_table="ft",
                    time_function="hour",
                    alias="pickup_hour",
                ),
            ],
        )
        assert len(step.time_transforms) == 1
        assert step.time_transforms[0].alias == "pickup_hour"


# ════════════════════════════════════════════
# Task 6: Contract 提取器测试
# ════════════════════════════════════════════

from tianshu_datadev.artifacts.contract_extractor import DataTransformContractExtractor
from tianshu_datadev.developer_spec.models import AggregationType
from tianshu_datadev.planning.models import (
    AggregateSpec,
    ColumnRef,
    DatePartExpression,
    DerivedGroupKey,
    TimeTransformExpr,
)
from tianshu_datadev.planning.sql_build_plan import AggregateStep


class TestExtractAggregateWithDerivedGroupKey:
    """_extract_aggregate 处理 DerivedGroupKey。"""

    def test_derived_key_produces_time_transform(self):
        """DerivedGroupKey → ContractTimeTransform + grouping_key alias。"""
        agg = AggregateStep(
            step_id="agg_1",
            group_keys=[
                DerivedGroupKey(
                    alias="pickup_hour",
                    expr=TimeTransformExpr(
                        source_column="pickup_at",
                        source_table="ft",
                        time_function="HOUR",
                    ),
                ),
            ],
            metrics=[
                AggregateSpec(
                    aggregation=AggregationType.COUNT,
                    input_column=None,
                    alias="trip_count",
                ),
            ],
        )
        aggs, groups, biz_keys, time_transforms, derived_columns = (
            DataTransformContractExtractor._extract_aggregate(agg)
        )
        assert "pickup_hour" in groups
        assert len(time_transforms) == 1
        assert time_transforms[0].alias == "pickup_hour"
        assert time_transforms[0].time_function == "HOUR"


class TestRenderOperandWithTimeTransform:
    """_render_operand 处理 TimeTransformExpr。"""

    def test_render_time_transform_operand(self):
        """TimeTransformExpr → 'HOUR(ft.pickup_at)'。"""
        expr = TimeTransformExpr(
            source_column="pickup_at",
            source_table="ft",
            time_function="HOUR",
        )
        result = DataTransformContractExtractor._render_operand(expr)
        assert result == "HOUR(ft.pickup_at)"


# ════════════════════════════════════════════
# Task 7: Spark 链路测试
# ════════════════════════════════════════════

from tianshu_datadev.artifacts.models import (
    ContractAggregation,
)
from tianshu_datadev.spark.compiler import SparkCompiler
from tianshu_datadev.spark.contract_adapter import adapt_lite_to_v1
from tianshu_datadev.spark.mapper import _map_aggregations


class TestMapAggregationsWithTimeTransforms:
    """Mapper——Contract 不变量应用。"""

    def test_mapper_replaces_group_key_with_transform(self):
        """同名 alias → 从 group_keys 移除，加入 time_transforms。"""
        result = _map_aggregations(
            aggregations=[
                ContractAggregation(function="COUNT", alias="trip_count"),
            ],
            grouping_keys=["pickup_hour", "borough"],
            time_transforms=[
                ContractTimeTransform(
                    source_column="pickup_at",
                    source_table="ft",
                    time_function="HOUR",
                    alias="pickup_hour",
                ),
            ],
        )
        step = result[0]
        # pickup_hour 从 group_keys 移除
        assert "pickup_hour" not in step.group_keys
        assert "borough" in step.group_keys
        # 加入 time_transforms
        assert len(step.time_transforms) == 1
        assert step.time_transforms[0].alias == "pickup_hour"

    def test_mapper_empty_time_transforms(self):
        """空 time_transforms——group_keys 保持不变。"""
        result = _map_aggregations(
            aggregations=[
                ContractAggregation(function="COUNT", alias="trip_count"),
            ],
            grouping_keys=["borough"],
            time_transforms=[],
        )
        step = result[0]
        assert step.group_keys == ["borough"]
        assert step.time_transforms == []


class TestAdaptLiteToV1TimeTransforms:
    """lite→v1 adapter 透传 time_transforms。"""

    def test_adapt_preserves_time_transforms(self):
        """time_transforms 应从 Lite 透传到 V1。"""
        from tianshu_datadev.artifacts.models import DataTransformContractLite
        lite = DataTransformContractLite(
            contract_id="test",
            source_sqlbuildplan_hash="abc",
            grouping_keys=["pickup_hour"],
            time_transforms=[
                ContractTimeTransform(
                    source_column="pickup_at",
                    source_table="ft",
                    time_function="HOUR",
                    alias="pickup_hour",
                ),
            ],
        )
        v1 = adapt_lite_to_v1(lite)
        assert len(v1.time_transforms) == 1
        assert v1.time_transforms[0].alias == "pickup_hour"


class TestCompileAggregateWithTimeTransforms:
    """_compile_aggregate 渲染 time_transforms——集成验证。"""

    def test_compile_generates_hour_in_groupby(self):
        """编译产物应包含 F.hour(...).alias(...) 在 groupBy 中。"""
        from tianshu_datadev.spark.models import (
            SparkAggFunction,
            SparkPlan,
            SparkReadStep,
            SparkTimeTransformExpr,
        )
        plan = SparkPlan(
            plan_id="test",
            source_contract_hash="abc",
            steps=[
                SparkReadStep(
                    source_name="ft",
                    alias="ft",
                    input_key="ft",
                ),
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
                    time_transforms=[
                        SparkTimeTransformExpr(
                            source_column="pickup_at",
                            source_table="ft",
                            time_function="hour",
                            alias="pickup_hour",
                        ),
                    ],
                ),
            ],
        )
        compiler = SparkCompiler()
        result = compiler.compile(plan)
        code = result.raw_pyspark
        # groupBy 中应有 F.hour(...).alias("pickup_hour")
        assert 'F.hour(F.col("ft.pickup_at")).alias("pickup_hour")' in code
        # select 中应有 F.col("pickup_hour")——禁止 F.hour 再次出现
        assert 'F.col("pickup_hour")' in code


class TestExtractAggregateDedupDatePartAndDerivedGroupKey:
    """_extract_aggregate 去重：DatePartExpression + DerivedGroupKey 共存时
    不应产生重复的 time_transform 和 group 条目。"""

    def test_dedup_prevents_duplicate_time_transforms(self):
        """同一 alias 的 DatePartExpression + DerivedGroupKey →
        groups 和 time_transforms 各仅一条。"""
        agg = AggregateStep(
            step_id="agg_dedup",
            group_keys=[
                # 场景：dimensions 中有 date_part="HOUR" 的 DatePartExpression
                DatePartExpression(
                    part="HOUR",
                    column=ColumnRef(
                        table_ref="ft",
                        column_name="pickup_at",
                        normalized_name="pickup_at",
                    ),
                    alias="pickup_hour",
                ),
                # derived_dimensions 中有同名的 DerivedGroupKey
                DerivedGroupKey(
                    alias="pickup_hour",
                    expr=TimeTransformExpr(
                        source_column="pickup_at",
                        source_table="ft",
                        time_function="HOUR",
                    ),
                ),
            ],
            metrics=[
                AggregateSpec(
                    aggregation=AggregationType.COUNT,
                    input_column=None,
                    alias="trip_count",
                ),
            ],
        )
        aggs, groups, biz_keys, time_transforms, derived_columns = (
            DataTransformContractExtractor._extract_aggregate(agg)
        )
        # groups 中 "pickup_hour" 仅出现一次
        assert groups.count("pickup_hour") == 1, (
            f"groups 中 pickup_hour 应仅一次，实际={groups}"
        )
        # time_transforms 中 pickup_hour 仅出现一次
        pickup_tt = [tt for tt in time_transforms if tt.alias == "pickup_hour"]
        assert len(pickup_tt) == 1, (
            f"time_transforms 中 pickup_hour 应仅一条，实际={len(pickup_tt)}"
        )


class TestExtractAggregateWithMetricFilter:
    """_extract_aggregate 透传 MetricFilterDecl filter。"""

    def test_metric_filter_propagated_to_contract_aggregation(self):
        """带 filter 的 AggregateSpec -> ContractAggregation.filter 非空。"""
        from tianshu_datadev.developer_spec.models import MetricFilterDecl

        agg = AggregateStep(
            step_id="agg_filter",
            group_keys=[
                ColumnRef(
                    table_ref="ft",
                    column_name="borough",
                    normalized_name="borough",
                ),
            ],
            metrics=[
                AggregateSpec(
                    aggregation=AggregationType.COUNT,
                    input_column=None,
                    alias="anomaly_trip_count",
                    filter=MetricFilterDecl(
                        column="is_time_anomaly",
                        operator="eq",
                        value="true",
                    ),
                ),
            ],
        )
        aggs, groups, biz_keys, time_transforms, derived_columns = (
            DataTransformContractExtractor._extract_aggregate(agg)
        )
        assert len(aggs) == 1
        assert aggs[0].filter is not None
        assert aggs[0].filter.column == "is_time_anomaly"
        assert aggs[0].filter.operator == "eq"
        assert aggs[0].filter.value == "true"

    def test_metric_filter_none_when_not_set(self):
        """无 filter 的 AggregateSpec -> ContractAggregation.filter 为 None。"""
        agg = AggregateStep(
            step_id="agg_no_filter",
            group_keys=[
                ColumnRef(
                    table_ref="ft",
                    column_name="borough",
                    normalized_name="borough",
                ),
            ],
            metrics=[
                AggregateSpec(
                    aggregation=AggregationType.COUNT,
                    input_column=None,
                    alias="trip_count",
                ),
            ],
        )
        aggs, groups, biz_keys, time_transforms, derived_columns = (
            DataTransformContractExtractor._extract_aggregate(agg)
        )
        assert len(aggs) == 1
        assert aggs[0].filter is None
