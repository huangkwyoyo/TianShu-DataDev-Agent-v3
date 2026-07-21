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
        aggs, groups, biz_keys, time_transforms = (
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
