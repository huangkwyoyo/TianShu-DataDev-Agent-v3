"""SqlProgram 工厂——从 SqlBuildPlan 列表构建多语句 SqlProgram。

职责：将 Build 阶段的产物（单个 SqlBuildPlan 或多个 Plan 组成的链/DAG）
转换为 SqlProgram（多语句编排容器），由 Compiler 执行。

这些函数从 Pipeline._build_sql_program* 静态方法提取而来，
作为独立的工厂函数供 Pipeline、Phase 8 LangGraph 编排器等调用。
"""

from __future__ import annotations

from tianshu_datadev.developer_spec.models import ParsedDeveloperSpec
from tianshu_datadev.planning.sql_build_plan import SqlBuildPlan
from tianshu_datadev.planning.sql_program import (
    SqlProgram,
    SqlProgramBuilder,
    SqlStatement,
    StatementKind,
)
from tianshu_datadev.planning.temp_table import make_temp_name


def build_sql_program(plan: SqlBuildPlan, spec_hash: str) -> SqlProgram:
    """从单个 SqlBuildPlan 构建最小 SqlProgram（单语句 STANDALONE）。

    这是自动化多语句构建的基础——
    当前将单 plan 包装为单语句 SqlProgram，
    未来多语句拆分逻辑在此扩展（如按 _temp 依赖拆分）。

    Args:
        plan: SqlBuildPlan 实例
        spec_hash: 对应 DeveloperSpec 的 SHA-256

    Returns:
        含单个 STANDALONE 语句的 SqlProgram
    """
    stmt = SqlStatement(
        statement_id=plan.plan_id,
        plan=plan,
        kind=StatementKind.STANDALONE,
    )
    builder = SqlProgramBuilder()
    return builder.build_from_statements(
        statements=[stmt],
        spec_hash=spec_hash,
        final_output=plan.plan_id,
    )


def build_sql_program_from_chain(
    plans: list[SqlBuildPlan], spec_hash: str, chain_id: str
) -> SqlProgram:
    """从多 Plan 链构建 SqlProgram——每步 PRODUCER/FINAL，通过 _temp 串联。

    中间 Plan 标记为 PRODUCER，产生 _temp 中间表供下游消费。
    最终 Plan 标记为 FINAL，产生最终输出。
    """
    statements: list[SqlStatement] = []
    for idx, plan in enumerate(plans):
        is_final = (idx == len(plans) - 1)
        produces = None if is_final else make_temp_name(chain_id, str(idx))
        depends_on = [plans[idx - 1].plan_id] if idx > 0 else []

        stmt = SqlStatement(
            statement_id=plan.plan_id,
            plan=plan,
            kind=StatementKind.FINAL if is_final else StatementKind.PRODUCER,
            depends_on=depends_on,
            produces=produces,
        )
        statements.append(stmt)

    builder = SqlProgramBuilder()
    return builder.build_from_statements(
        statements=statements,
        spec_hash=spec_hash,
        final_output=plans[-1].plan_id,
    )


def build_sql_program_from_compute_steps(
    plans: list[SqlBuildPlan],
    spec: ParsedDeveloperSpec,
    chain_id: str,
) -> SqlProgram:
    """从 ComputeSteps Plan 链构建 SqlProgram——使用 step_name 命名 _temp 表。

    与 build_sql_program_from_chain 的区别：
    - 使用 spec.compute_steps 的 step_name 命名 _temp 表（而非 idx）
    - 确保 produces 与 Builder 中 ScanStep 的 table_ref 一致
    - 支持 DAG 依赖——合流步骤 depends_on 包含所有上游 plan_id
    """
    steps = spec.compute_steps or []
    # 构建 step_name → plan 映射（含 plan_id）
    step_plan_map: dict[str, SqlBuildPlan] = {}
    for cs, plan in zip(steps, plans):
        step_plan_map[cs.step_name] = plan

    statements: list[SqlStatement] = []
    # 跟踪哪些步骤被其他步骤依赖——不被依赖的为 FINAL
    consumed: set[str] = set()
    for cs in steps:
        src_list = cs.source if isinstance(cs.source, list) else [cs.source]
        for src in src_list:
            if src != "input" and src in step_plan_map:
                consumed.add(src)

    for cs, plan in zip(steps, plans):
        is_final = cs.step_name not in consumed
        src_list = cs.source if isinstance(cs.source, list) else [cs.source]

        # 计算依赖——所有非 input 的上游 step
        depends_on: list[str] = []
        for src in src_list:
            if src != "input" and src in step_plan_map:
                depends_on.append(step_plan_map[src].plan_id)

        # 使用 step_name 命名 _temp 表——与 Builder 中 ScanStep.table_ref 一致
        produces = (
            None
            if is_final
            else make_temp_name(chain_id, cs.step_name)
        )

        stmt = SqlStatement(
            statement_id=plan.plan_id,
            plan=plan,
            kind=StatementKind.FINAL if is_final else StatementKind.PRODUCER,
            depends_on=depends_on,
            produces=produces,
        )
        statements.append(stmt)

    # final_output 为所有 FINAL 语句的最后一个
    final_plans = [
        s for s in statements if s.kind == StatementKind.FINAL
    ]
    final_output = final_plans[-1].statement_id if final_plans else statements[-1].statement_id

    builder = SqlProgramBuilder()
    return builder.build_from_statements(
        statements=statements,
        spec_hash=spec.spec_hash,
        final_output=final_output,
    )
