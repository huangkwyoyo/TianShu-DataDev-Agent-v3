"""PlanComparator 测试共享构建器——最小合法 SQL/Spark step 和 plan 工厂函数。

拆分自 test_plan_comparator.py（Phase 6.2 Comparator 文件拆分）。
所有子文件通过 `from tests.spark.plan_comparator_fixtures import *` 使用。
"""

from __future__ import annotations

from tianshu_datadev.planning.models import (
    AliasExpr,
    ColumnRef,
    Predicate,
    PredicateOperator,
    SortDirection,
    SortSpec,
)
from tianshu_datadev.planning.sql_build_plan import (
    FilterStep,
    LimitStep,
    ProjectStep,
    ScanStep,
    SortStep,
    SqlBuildPlan,
)
from tianshu_datadev.spark.models import (
    SparkFilterStep,
    SparkLimitStep,
    SparkPlan,
    SparkProjectColumn,
    SparkProjectStep,
    SparkReadStep,
    SparkSortDirection,
    SparkSortSpec,
    SparkSortStep,
    SparkStepType,
    SparkWindowStep,
)

# ════════════════════════════════════════════
# SQL 侧 step 构建器
# ════════════════════════════════════════════


def _make_sql_scan_step(
    step_id: str = "step_scan_001",
    table_ref: str = "od",
) -> ScanStep:
    """构造最小合法 SQL ScanStep——table_ref 默认与 Spark alias 一致。"""
    return ScanStep(
        step_type="scan",
        step_id=step_id,
        table_ref=table_ref,
        required_columns=[
            ColumnRef(
                table_ref=table_ref,
                column_name="order_id",
                normalized_name="order_id",
            ),
            ColumnRef(
                table_ref=table_ref,
                column_name="amount",
                normalized_name="amount",
            ),
        ],
    )


def _make_sql_filter_step(step_id: str = "step_filter_001") -> FilterStep:
    """构造最小合法 SQL FilterStep。"""
    return FilterStep(
        step_type="filter",
        step_id=step_id,
        predicate=Predicate(
            left=ColumnRef(
                table_ref="order_info",
                column_name="amount",
                normalized_name="amount",
            ),
            operator=PredicateOperator.GT,
            right=ColumnRef(
                table_ref="order_info",
                column_name="threshold",
                normalized_name="threshold",
            ),
        ),
    )


def _make_sql_project_step(step_id: str = "step_project_001") -> ProjectStep:
    """构造最小合法 SQL ProjectStep。"""
    return ProjectStep(
        step_type="project",
        step_id=step_id,
        columns=[
            AliasExpr(
                expression=ColumnRef(
                    table_ref="order_info",
                    column_name="order_id",
                    normalized_name="order_id",
                ),
                alias="order_id",
            ),
            AliasExpr(
                expression=ColumnRef(
                    table_ref="order_info",
                    column_name="amount",
                    normalized_name="amount",
                ),
                alias="amount",
            ),
        ],
    )


def _make_sql_sort_step(step_id: str = "step_sort_001") -> SortStep:
    """构造最小合法 SQL SortStep。"""
    return SortStep(
        step_type="sort",
        step_id=step_id,
        order_by=[
            SortSpec(
                column="amount",
                direction=SortDirection.DESC,
            ),
        ],
    )


def _make_sql_limit_step(step_id: str = "step_limit_001") -> LimitStep:
    """构造最小合法 SQL LimitStep。"""
    return LimitStep(
        step_type="limit",
        step_id=step_id,
        limit=100,
    )


def _make_sql_plan(steps: list) -> SqlBuildPlan:
    """构造最小合法 SqlBuildPlan。"""
    spec_hash = "test_spec_hash_abc123"
    plan = SqlBuildPlan(
        plan_id=SqlBuildPlan.generate_plan_id(spec_hash),
        spec_hash=spec_hash,
        steps=steps,
    )
    return plan


# ════════════════════════════════════════════
# Spark 侧 step 构建器
# ════════════════════════════════════════════


def _make_spark_read_step(alias: str = "od") -> SparkReadStep:
    """构造最小合法 SparkReadStep。"""
    return SparkReadStep(
        step_type=SparkStepType.READ,
        alias=alias,
        source_name="order_info",
        input_key="order_info_key",
        required_columns=["order_id", "amount"],
    )


def _make_spark_filter_step(input_alias: str = "od") -> SparkFilterStep:
    """构造最小合法 SparkFilterStep。"""
    return SparkFilterStep(
        step_type=SparkStepType.FILTER,
        input_alias=input_alias,
        operator="GT",
        left="amount",
        right="threshold",
    )


def _make_spark_project_step(input_alias: str = "od") -> SparkProjectStep:
    """构造最小合法 SparkProjectStep。"""
    return SparkProjectStep(
        step_type=SparkStepType.PROJECT,
        input_alias=input_alias,
        columns=[
            SparkProjectColumn(column_name="order_id", alias="order_id"),
            SparkProjectColumn(column_name="amount", alias="amount"),
        ],
    )


def _make_spark_sort_step(input_alias: str = "od") -> SparkSortStep:
    """构造最小合法 SparkSortStep。"""
    return SparkSortStep(
        step_type=SparkStepType.SORT,
        input_alias=input_alias,
        order_by=[
            SparkSortSpec(column="amount", direction=SparkSortDirection.DESC),
        ],
    )


def _make_spark_limit_step(input_alias: str = "od") -> SparkLimitStep:
    """构造最小合法 SparkLimitStep。"""
    return SparkLimitStep(
        step_type=SparkStepType.LIMIT,
        input_alias=input_alias,
        limit=100,
    )


def _make_spark_plan(steps: list) -> SparkPlan:
    """构造最小合法 SparkPlan。"""
    contract_hash = "test_contract_hash_abc123"
    plan = SparkPlan(
        plan_id=SparkPlan.generate_plan_id(contract_hash),
        version="v1",
        source_phase="phase-5",
        source_contract_hash=contract_hash,
        source_contract_version="v1",
        steps=steps,
    )
    return plan


def _make_spark_window_step(expressions: list) -> SparkWindowStep:
    """构造最小 SparkWindowStep。"""
    return SparkWindowStep(
        step_type=SparkStepType.WINDOW,
        input_alias="od",
        expressions=expressions,
    )
