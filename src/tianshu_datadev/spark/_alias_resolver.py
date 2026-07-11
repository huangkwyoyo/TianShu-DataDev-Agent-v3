"""单一别名解析层——SparkPlan → ResolvedPlan。

消除 mapper/compiler/test 三套别名状态机冗余：
- Read 节点按 input_key 字典序分配 t1、t2...
- 非 Read 节点按执行顺序分配 f1、f2...
- input_vars 通过追踪 latest 映射解析——仅此一份逻辑

严格校验：
- 非 Read 步骤的 input_alias 必须能在 latest 中解析
- Join 的左右别名必须都能解析
- 空 Plan 拒绝
- 重复 alias / input_key 拒绝
"""

from __future__ import annotations

from dataclasses import dataclass

from tianshu_datadev.spark.models import (
    SparkJoinStep,
    SparkReadStep,
    SparkStep,
)


class AliasResolutionError(ValueError):
    """别名解析失败——依赖缺失、重复别名或空 Plan。"""


@dataclass(frozen=True)
class ResolvedStep:
    """单个已解析步骤——input_vars 和 output_var 已确定。

    Attributes:
        step: 原始 SparkStep 引用
        input_vars: 输入 DataFrame 变量名——按 input_node 顺序
        output_var: 输出 DataFrame 变量名（tN 或 fN）
    """

    step: SparkStep
    input_vars: tuple[str, ...]
    output_var: str


@dataclass(frozen=True)
class ResolvedPlan:
    """已解析的 SparkPlan——所有变量名已确定。

    Attributes:
        steps: 已解析步骤列表——顺序与原始 Plan.steps 一致
        output_var: 最终 return 的变量名
    """

    steps: tuple[ResolvedStep, ...]
    output_var: str


def assign_source_aliases(steps: list[SparkStep]) -> dict[str, str]:
    """为 Read 节点分配 t1、t2...——按 input_key 字典序。

    Args:
        steps: SparkPlan.steps 列表

    Returns:
        {alias: tN} 映射——key 为 SparkReadStep.alias

    Raises:
        AliasResolutionError: 重复 input_key 或重复 alias
    """
    # 收集所有 Read 步骤的 (alias, input_key)
    read_entries: list[tuple[str, str]] = []
    for s in steps:
        if isinstance(s, SparkReadStep):
            read_entries.append((s.alias, s.input_key))

    # 按 input_key 字典序排序——保证确定性
    read_entries.sort(key=lambda x: x[1])

    # 校验无重复 input_key
    seen_keys: set[str] = set()
    for alias, key in read_entries:
        if key in seen_keys:
            raise AliasResolutionError(
                f"重复的 input_key: {key!r}——"
                f"同一数据源被多个 ReadStep 引用时请使用不同的 input_key"
            )
        seen_keys.add(key)

    # 校验无重复 alias——防止静默覆盖
    seen_aliases: set[str] = set()
    for alias, _ in read_entries:
        if alias in seen_aliases:
            raise AliasResolutionError(
                f"重复的 Read alias: {alias!r}——"
                f"多个 ReadStep 使用了相同的 alias，将导致映射覆盖"
            )
        seen_aliases.add(alias)

    return {alias: f"t{i + 1}" for i, (alias, _) in enumerate(read_entries)}


def resolve_codegen_aliases(plan) -> ResolvedPlan:
    """解析 SparkPlan 的所有代码生成变量名——单一入口。

    规则：
    - Read 按 input_key 字典序 → t1、t2...
    - 非 Read 按执行顺序 → f1、f2...
    - 通过 latest 映射追踪每个 alias 的最新输出变量（仅此一份逻辑）

    严格校验：
    - 非 Read 步骤的依赖必须在 latest 中已注册
    - Join 的左右别名都必须存在
    - 空 Plan 拒绝

    Args:
        plan: SparkPlan 实例（含 steps 列表）

    Returns:
        ResolvedPlan——含所有步骤的已解析变量名

    Raises:
        AliasResolutionError: 依赖缺失、空 Plan、重复 key
    """
    from tianshu_datadev.spark.models import SparkPlan

    if isinstance(plan, SparkPlan):
        steps = list(plan.steps)
    else:
        steps = list(plan)

    if not steps:
        raise AliasResolutionError("无法解析空 Plan——steps 列表为空")

    t_aliases = assign_source_aliases(steps)

    # 追踪每个原始 alias → 最新输出变量名
    latest: dict[str, str] = {}
    f_counter = 0
    prev_output: str | None = None
    resolved_steps: list[ResolvedStep] = []

    for i, step in enumerate(steps):
        if isinstance(step, SparkReadStep):
            out_var = t_aliases[step.alias]
            latest[step.alias] = out_var
            prev_output = out_var
            resolved_steps.append(ResolvedStep(
                step=step,
                input_vars=(),
                output_var=out_var,
            ))

        elif isinstance(step, SparkJoinStep):
            # 双输入——左右别名必须已在 latest 中注册
            left_var = latest.get(step.left_alias)
            if left_var is None:
                raise AliasResolutionError(
                    f"步骤 {i} Join 的左表别名 {step.left_alias!r} 未解析——"
                    f"请确认该别名对应的 Read 或上游步骤已执行。"
                    f"当前已解析别名: {list(latest.keys())}"
                )
            right_var = latest.get(step.right_alias)
            if right_var is None:
                raise AliasResolutionError(
                    f"步骤 {i} Join 的右表别名 {step.right_alias!r} 未解析——"
                    f"请确认该别名对应的 Read 或上游步骤已执行。"
                    f"当前已解析别名: {list(latest.keys())}"
                )
            f_counter += 1
            out_var = f"f{f_counter}"
            # Join 的输出覆盖左表的 latest 条目（与 compiler 原有逻辑一致）
            latest[step.left_alias] = out_var
            prev_output = out_var
            resolved_steps.append(ResolvedStep(
                step=step,
                input_vars=(left_var, right_var),
                output_var=out_var,
            ))

        else:
            # 单输入步骤——Filter/Project/Sort/Limit/Aggregate/CaseWhen/Window
            input_key = getattr(step, "input_alias", "")
            if input_key:
                # 依赖必须在 latest 中已注册
                input_var = latest.get(input_key)
                if input_var is None:
                    raise AliasResolutionError(
                        f"步骤 {i} {type(step).__name__} 的 input_alias={input_key!r} 未解析——"
                        f"请确认该别名对应的 Read 或上游步骤已执行。"
                        f"当前已解析别名: {list(latest.keys())}"
                    )
            elif prev_output is not None:
                # input_alias 为空但存在前序步骤——Mapper 应已补全此字段
                input_var = prev_output
            else:
                raise AliasResolutionError(
                    f"步骤 {i} {type(step).__name__} 的 input_alias 为空且无前序步骤——"
                    f"Plan 的首个步骤必须是 ReadStep"
                )

            f_counter += 1
            out_var = f"f{f_counter}"
            if input_key:
                latest[input_key] = out_var
            prev_output = out_var
            resolved_steps.append(ResolvedStep(
                step=step,
                input_vars=(input_var,) if input_var else (),
                output_var=out_var,
            ))

    return ResolvedPlan(
        steps=tuple(resolved_steps),
        output_var=prev_output or "",
    )
