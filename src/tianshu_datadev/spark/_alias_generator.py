"""步骤别名生成器——数据流式命名。

compiler.py 和 mapper.py 共享此模块，确保别名规则一致。
生成的别名表达"这个 DataFrame 是什么状态"而非"这个步骤是什么类型"，
使 PySpark 代码接近人手写的风格。
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tianshu_datadev.spark.models import (
        SparkAggregateStep,
    )

# 别名最大长度——超长时保留头尾，中间用 __ 连接
_MAX_ALIAS_LEN = 48


def generate_step_alias(
    step,
    input_alias: str = "",
    left_alias: str = "",
    right_alias: str = "",
    used_aliases: set[str] | None = None,
    is_last_project: bool = False,
) -> str:
    """为步骤生成数据流式输出别名。

    Args:
        step: SparkPlan 步骤实例
        input_alias: 已解析的单输入别名（Filter/Project/Sort/Limit/Aggregate/Window/CaseWhen）
        left_alias: 已解析的左输入别名（Join）
        right_alias: 已解析的右输入别名（Join）
        used_aliases: 已用别名集合——用于冲突检测和递增后缀；会被原地修改
        is_last_project: 仅对 ProjectStep 有效——是否为管线中最后一个投影步骤

    Returns:
        符合 Python 标识符规则的别名
    """
    from tianshu_datadev.spark.models import (
        SparkAggregateStep,
        SparkCaseWhenStep,
        SparkFilterStep,
        SparkJoinStep,
        SparkLimitStep,
        SparkProjectStep,
        SparkReadStep,
        SparkSortStep,
        SparkWindowStep,
    )

    used = used_aliases if used_aliases is not None else set()

    if isinstance(step, SparkReadStep):
        base = step.alias
    elif isinstance(step, SparkFilterStep):
        base = f"{input_alias}_filtered"
    elif isinstance(step, SparkSortStep):
        base = f"{input_alias}_sorted"
    elif isinstance(step, SparkLimitStep):
        base = f"{input_alias}_top_{step.limit}"
    elif isinstance(step, SparkJoinStep):
        base = f"{left_alias}_with_{right_alias}"
    elif isinstance(step, SparkAggregateStep):
        base = _agg_alias(step, input_alias)
    elif isinstance(step, SparkProjectStep):
        suffix = "_output" if is_last_project else "_selected"
        base = f"{input_alias}{suffix}"
    elif isinstance(step, SparkWindowStep):
        base = f"{input_alias}_windowed"
    elif isinstance(step, SparkCaseWhenStep):
        base = f"{input_alias}_with_{step.output_alias}"
    else:
        base = "step_output"

    base = _sanitize(base)
    base = _truncate(base, _MAX_ALIAS_LEN)

    # 唯一性处理——同名追加 _2/_3 后缀
    alias = base
    n = 2
    while alias in used:
        alias = f"{base}_{n}"
        n += 1

    used.add(alias)
    return alias


def _agg_alias(step: "SparkAggregateStep", input_alias: str) -> str:
    """聚合步骤别名：{metric}_by_{grain} 或回退 {input}_aggregated。"""
    if step.metrics and step.group_keys:
        first = step.metrics[0]
        metric = first.alias or first.function.value
        # 最多取 2 个 grain 维度，避免别名过长
        grain = "_".join(step.group_keys[:2])
        return f"{metric}_by_{grain}"
    return f"{input_alias}_aggregated"


def _sanitize(name: str) -> str:
    """清理为合法 Python 标识符——仅保留字母数字下划线。"""
    result = "".join(c if c.isalnum() or c == "_" else "_" for c in name)
    # 首字符不能是数字
    if result and result[0].isdigit():
        result = "_" + result
    return result or "step_output"


def _truncate(name: str, max_len: int) -> str:
    """过长别名保留头尾，中间用 __ 连接。

    例如 revenue_by_day_and_region_for_active_customers
    → revenue_by_day_and__region__customers（48 字符内）
    """
    if len(name) <= max_len:
        return name
    # 头尾各约一半，中间 __ 占 2 字符
    head_len = max_len // 2 + max_len % 2
    tail_len = max_len - head_len - 2
    head = name[:head_len]
    tail = name[-tail_len:] if tail_len > 0 else ""
    return f"{head}__{tail}"
