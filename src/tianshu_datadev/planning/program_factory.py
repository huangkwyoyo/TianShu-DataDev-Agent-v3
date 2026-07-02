"""SqlProgram 工厂——薄委托层，向后兼容。

所有构建逻辑已迁移至 SqlProgramBuilder 的三个高级构建方法：
  - build_sql_program()          → SqlProgramBuilder.build_single()
  - build_sql_program_from_chain()       → SqlProgramBuilder.build_chain()
  - build_sql_program_from_compute_steps() → SqlProgramBuilder.build_from_compute_steps()

新代码应直接使用 SqlProgramBuilder 而非本文件的工厂函数。
本文件保留仅用于向后兼容——Pipeline 和测试的现有调用点无需立即迁移。
"""

from __future__ import annotations

from tianshu_datadev.developer_spec.models import ParsedDeveloperSpec
from tianshu_datadev.planning.sql_build_plan import SqlBuildPlan
from tianshu_datadev.planning.sql_program import (
    SqlProgram,
    SqlProgramBuilder,
)


def build_sql_program(plan: SqlBuildPlan, spec_hash: str) -> SqlProgram:
    """从单个 SqlBuildPlan 构建最小 SqlProgram——委托至 SqlProgramBuilder.build_single()。"""
    builder = SqlProgramBuilder()
    return builder.build_single(plan, spec_hash)


def build_sql_program_from_chain(
    plans: list[SqlBuildPlan], spec_hash: str, chain_id: str
) -> SqlProgram:
    """从多 Plan 线性链构建 SqlProgram——委托至 SqlProgramBuilder.build_chain()。"""
    builder = SqlProgramBuilder()
    return builder.build_chain(plans, spec_hash, chain_id)


def build_sql_program_from_compute_steps(
    plans: list[SqlBuildPlan],
    spec: ParsedDeveloperSpec,
    chain_id: str,
) -> SqlProgram:
    """从 ComputeSteps Plan 链构建 SqlProgram——委托至 SqlProgramBuilder.build_from_compute_steps()。"""
    builder = SqlProgramBuilder()
    return builder.build_from_compute_steps(plans, spec, chain_id)
