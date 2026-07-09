"""数据流式步骤别名生成器测试。

覆盖所有步骤类型的别名规则、唯一性冲突、截断、特殊字符清理。
"""


from tianshu_datadev.spark._alias_generator import _sanitize, _truncate, generate_step_alias
from tianshu_datadev.spark.models import (
    SparkAggFunction,
    SparkAggregateSpec,
    SparkAggregateStep,
    SparkCaseWhenStep,
    SparkFilterStep,
    SparkJoinStep,
    SparkLimitStep,
    SparkProjectColumn,
    SparkProjectStep,
    SparkReadStep,
    SparkSortDirection,
    SparkSortSpec,
    SparkSortStep,
    SparkWindowExpr,
    SparkWindowFunction,
    SparkWindowStep,
)

# ════════════════════════════════════════════
# ReadStep
# ════════════════════════════════════════════

def test_read_alias_is_table_ref():
    """ReadStep 别名直接使用 table_ref。"""
    step = SparkReadStep(
        alias="fact_orders", source_name="fact_orders", input_key="fact_orders",
    )
    result = generate_step_alias(step)
    assert result == "fact_orders"


def test_read_alias_short_form():
    """短别名如 od/ft 保持不变。"""
    step = SparkReadStep(alias="od", source_name="orders", input_key="orders")
    result = generate_step_alias(step)
    assert result == "od"


# ════════════════════════════════════════════
# FilterStep
# ════════════════════════════════════════════

def test_filter_alias_is_input_filtered():
    """FilterStep 别名 = {input}_filtered。"""
    step = SparkFilterStep(input_alias="ft", operator="GT", left="ft.amount", right="100")
    result = generate_step_alias(step, input_alias="ft")
    assert result == "ft_filtered"


# ════════════════════════════════════════════
# JoinStep
# ════════════════════════════════════════════

def test_join_alias_is_left_with_right():
    """JoinStep 别名 = {left}_with_{right}。"""
    step = SparkJoinStep(
        left_alias="od", right_alias="ft",
        left_key="order_id", right_key="order_id", join_type="inner",
    )
    result = generate_step_alias(step, left_alias="od", right_alias="ft")
    assert result == "od_with_ft"


# ════════════════════════════════════════════
# AggregateStep
# ════════════════════════════════════════════

def test_agg_with_metric_and_grain():
    """有 metric + group_keys → {metric}_by_{grain}。"""
    step = SparkAggregateStep(
        input_alias="od",
        group_keys=["day"],
        metrics=[SparkAggregateSpec(
            function=SparkAggFunction.SUM, input_column="amount", alias="revenue",
        )],
    )
    result = generate_step_alias(step, input_alias="od")
    assert result == "revenue_by_day"


def test_agg_with_multiple_grains_truncates_to_two():
    """多 grain 维度时只取前 2 个。"""
    step = SparkAggregateStep(
        input_alias="od",
        group_keys=["region", "day", "month"],
        metrics=[SparkAggregateSpec(
            function=SparkAggFunction.COUNT, input_column=None, alias="cnt",
        )],
    )
    result = generate_step_alias(step, input_alias="od")
    assert result == "cnt_by_region_day"


def test_agg_without_grain_fallback():
    """无 group_keys → {input}_aggregated。"""
    step = SparkAggregateStep(
        input_alias="ft",
        group_keys=[],
        metrics=[SparkAggregateSpec(
            function=SparkAggFunction.COUNT, input_column=None, alias="total",
        )],
    )
    result = generate_step_alias(step, input_alias="ft")
    assert result == "ft_aggregated"


def test_agg_without_metrics_fallback():
    """无 metrics → {input}_aggregated。"""
    step = SparkAggregateStep(
        input_alias="ft",
        group_keys=["day"],
        metrics=[],
    )
    result = generate_step_alias(step, input_alias="ft")
    assert result == "ft_aggregated"


def test_agg_metric_uses_function_value_when_no_alias():
    """metric 无 alias 时使用 function.value。"""
    step = SparkAggregateStep(
        input_alias="od",
        group_keys=["day"],
        metrics=[SparkAggregateSpec(
            function=SparkAggFunction.AVG, input_column="amount", alias="",
        )],
    )
    result = generate_step_alias(step, input_alias="od")
    assert result == "AVG_by_day"


# ════════════════════════════════════════════
# ProjectStep
# ════════════════════════════════════════════

def test_project_last_is_output():
    """is_last_project=True → {input}_output。"""
    step = SparkProjectStep(
        input_alias="od_with_ft",
        columns=[SparkProjectColumn(column_name="day", alias="day")],
    )
    result = generate_step_alias(step, input_alias="od_with_ft", is_last_project=True)
    assert result == "od_with_ft_output"


def test_project_middle_is_selected():
    """is_last_project=False → {input}_selected。"""
    step = SparkProjectStep(
        input_alias="od_with_ft",
        columns=[SparkProjectColumn(column_name="day", alias="day")],
    )
    result = generate_step_alias(step, input_alias="od_with_ft", is_last_project=False)
    assert result == "od_with_ft_selected"


# ════════════════════════════════════════════
# WindowStep
# ════════════════════════════════════════════

def test_window_alias_is_windowed():
    """WindowStep 别名 = {input}_windowed。"""
    step = SparkWindowStep(
        input_alias="ft_sorted",
        expressions=[SparkWindowExpr(
            function=SparkWindowFunction.ROW_NUMBER, alias="rn",
        )],
    )
    result = generate_step_alias(step, input_alias="ft_sorted")
    assert result == "ft_sorted_windowed"


# ════════════════════════════════════════════
# CaseWhenStep
# ════════════════════════════════════════════

def test_case_when_alias():
    """CaseWhenStep 别名 = {input}_with_{output_alias}。"""
    step = SparkCaseWhenStep(
        input_alias="ft", output_alias="trip_category",
        branches=[],
    )
    result = generate_step_alias(step, input_alias="ft")
    assert result == "ft_with_trip_category"


# ════════════════════════════════════════════
# SortStep
# ════════════════════════════════════════════

def test_sort_alias():
    """SortStep 别名 = {input}_sorted。"""
    step = SparkSortStep(
        input_alias="ft_filtered",
        order_by=[SparkSortSpec(column="pickup_at", direction=SparkSortDirection.ASC)],
    )
    result = generate_step_alias(step, input_alias="ft_filtered")
    assert result == "ft_filtered_sorted"


# ════════════════════════════════════════════
# LimitStep
# ════════════════════════════════════════════

def test_limit_alias():
    """LimitStep 别名 = {input}_top_{n}。"""
    step = SparkLimitStep(input_alias="ft_filtered_sorted", limit=100)
    result = generate_step_alias(step, input_alias="ft_filtered_sorted")
    assert result == "ft_filtered_sorted_top_100"


# ════════════════════════════════════════════
# 唯一性冲突
# ════════════════════════════════════════════

def test_conflict_appends_suffix():
    """同名别名追加 _2、_3 后缀。"""
    step = SparkFilterStep(input_alias="ft", operator="GT", left="ft.amount", right="0")
    used = {"ft_filtered"}
    result = generate_step_alias(step, input_alias="ft", used_aliases=used)
    assert result == "ft_filtered_2"
    assert "ft_filtered_2" in used


def test_conflict_increments_to_3():
    """已有 _2 时递增到 _3。"""
    step = SparkFilterStep(input_alias="ft", operator="GT", left="ft.amount", right="0")
    used = {"ft_filtered", "ft_filtered_2"}
    result = generate_step_alias(step, input_alias="ft", used_aliases=used)
    assert result == "ft_filtered_3"


# ════════════════════════════════════════════
# 截断
# ════════════════════════════════════════════

def test_truncate_long_alias():
    """超长别名截断为 head__tail 格式。"""
    long_input = "revenue_by_day_and_region_for_active_customers"
    step = SparkProjectStep(
        input_alias=long_input,
        columns=[SparkProjectColumn(column_name="day", alias="day")],
    )
    result = generate_step_alias(step, input_alias=long_input, is_last_project=True)
    assert len(result) <= 48
    assert "__" in result
    # 头尾应分别包含原始内容的前后缀
    assert result.startswith("revenue_by_day_and_")
    assert result.endswith("_customers_output")


# ════════════════════════════════════════════
# 标识符清理
# ════════════════════════════════════════════

def test_sanitize_special_chars():
    """特殊字符替换为下划线。"""
    result = _sanitize("my-table@name")
    assert result == "my_table_name"


def test_sanitize_leading_digit():
    """首字符为数字时加下划线前缀。"""
    result = _sanitize("123abc")
    assert result == "_123abc"


def test_sanitize_empty_string():
    """空字符串返回默认占位符。"""
    result = _sanitize("")
    assert result == "step_output"


# ════════════════════════════════════════════
# 截断函数
# ════════════════════════════════════════════

def test_truncate_short_enough():
    """短字符串不截断。"""
    result = _truncate("hello_world", 48)
    assert result == "hello_world"


def test_truncate_exact_boundary():
    """恰好 max_len 长度不截断。"""
    s = "a" * 48
    result = _truncate(s, 48)
    assert result == s
    assert len(result) == 48
