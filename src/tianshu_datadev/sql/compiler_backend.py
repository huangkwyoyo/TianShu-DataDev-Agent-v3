"""CompilerBackend——SQL 编译器后端抽象接口。

Phase 3C 占位定义——提供多 SQL 方言编译的统一入口抽象。
当前仅实现 DuckDBBackend（封装现有 DuckDbSqlCompiler），
Spark SQL 后端在 Phase 5 实现。

设计原则：
- 接口只定义 compile() 和 dialect() 两个抽象方法
- 所有后端返回统一的 CompilerOutput 类型
- 后端实现不可变——相同输入 + 相同 compiler_version → 相同输出
"""

from __future__ import annotations

from abc import ABC, abstractmethod

from tianshu_datadev.planning.sql_build_plan import SqlBuildPlan
from tianshu_datadev.planning.sql_program import SqlProgram

from .models import CompiledSql, SqlProgramArtifact


class CompilerBackend(ABC):
    """SQL 编译器后端抽象接口——Phase 3C 占位。

    Phase 5+ 实现 Spark SQL 后端（SparkSqlBackend），
    共享同一 compile() 接口，分别输出 DuckDB / Spark SQL 方言。
    """

    @abstractmethod
    def compile(self, plan: SqlBuildPlan | SqlProgram) -> CompiledSql | SqlProgramArtifact:
        """将 SqlBuildPlan / SqlProgram 编译为目标 SQL 方言。

        Args:
            plan: SqlBuildPlan（单语句）或 SqlProgram（多语句）

        Returns:
            CompiledSql（单语句）或 SqlProgramArtifact（多语句）
        """
        ...

    @abstractmethod
    def dialect(self) -> str:
        """返回 SQL 方言标识。

        Returns:
            'duckdb' | 'spark_sql'
        """
        ...


class DuckDBBackend(CompilerBackend):
    """DuckDB SQL 编译器后端——封装现有 DuckDbSqlCompiler。

    Phase 3C 重构：将现有 Compiler 逻辑对接到 CompilerBackend 接口。
    编译行为与重构前完全一致——相同输入产生相同 SQL + 相同 hash。
    """

    def __init__(self, table_mapping: dict[str, str] | None = None):
        """初始化 DuckDB 后端。

        Args:
            table_mapping: table_ref（别名）→ source_table（物理表名）的映射
        """
        from tianshu_datadev.sql.compiler import DuckDbSqlCompiler

        self._compiler = DuckDbSqlCompiler(table_mapping=table_mapping)

    def dialect(self) -> str:
        """DuckDB SQL 方言。"""
        return "duckdb"

    def compile(
        self, plan: SqlBuildPlan | SqlProgram
    ) -> CompiledSql | SqlProgramArtifact:
        """编译为 DuckDB SQL。

        Args:
            plan: SqlBuildPlan 或 SqlProgram

        Returns:
            CompiledSql 或 SqlProgramArtifact

        Raises:
            TypeError: plan 类型不是 SqlBuildPlan 或 SqlProgram
        """
        if isinstance(plan, SqlBuildPlan):
            return self._compiler.compile(plan)
        elif isinstance(plan, SqlProgram):
            return self._compiler.compile_program(plan)
        else:
            raise TypeError(
                f"DuckDBBackend.compile() 仅接受 SqlBuildPlan 或 SqlProgram，"
                f"收到 {type(plan).__name__}"
            )
