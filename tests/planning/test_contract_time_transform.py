"""Task 3 — TimeTransform 模型测试（Contract 侧 + Spark 侧）。"""
import pytest
from pydantic import ValidationError

from tianshu_datadev.artifacts.models import (
    ContractTimeTransform,
    DataTransformContractLite,
    DataTransformContractV1,
)
from tianshu_datadev.spark.models import (
    SparkTimeTransformExpr,
    SparkAggregateStep,
    SparkAggregateSpec,
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
