"""SparkPlan 全部步骤的确定性 Step ID 生成。"""

from __future__ import annotations

from collections.abc import Iterator

from tianshu_datadev.spark.models import SparkPlan


def iter_plan_steps(
    plan: SparkPlan,
) -> Iterator[tuple[str, str, int, object]]:
    """按编译顺序遍历 branches 和主阶段，返回统一 Step ID。"""
    for stage_index, branch_steps in enumerate(
        plan.branches.values(),
        start=1,
    ):
        stage_label = f"s{stage_index}"
        for step_index, step in enumerate(branch_steps):
            step_id = f"{type(step).__name__}_{stage_label}_{step_index}"
            yield step_id, stage_label, step_index, step

    main_stage_label = f"s{len(plan.branches) + 1}" if plan.branches else "main"
    for step_index, step in enumerate(plan.steps):
        step_id = f"{type(step).__name__}_{step_index}"
        yield step_id, main_stage_label, step_index, step
