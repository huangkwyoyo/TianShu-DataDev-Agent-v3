"""SqlProgram 端到端集成测试——compile_program() + execute_program()。

覆盖：
- 多语句编译产物结构正确（ProgramCompiledSql + cleanup SQL）
- 多语句全部成功执行 + cleanup
- 中间语句失败阻断后续 + cleanup 仍然执行
- _temp 表生命周期：CREATE → READ → DROP
"""

import os

import pytest

from tianshu_datadev.planning.models import AggregateSpec, ColumnRef
from tianshu_datadev.planning.sql_build_plan import (
    AggregateStep,
    ScanStep,
    SqlBuildPlan,
)
from tianshu_datadev.planning.sql_program import (
    SqlProgramBuilder,
    SqlStatement,
    StatementKind,
)
from tianshu_datadev.planning.temp_table import TempTableSpec
from tianshu_datadev.sql.compiler import DuckDbSqlCompiler
from tianshu_datadev.sql.executor import DuckDBExecutor
from tianshu_datadev.sql.models import ExecutionStatus

# ── 辅助函数 ──


def _csv_path(filename: str) -> str:
    """获取 CSV fixture 的绝对路径。"""
    return os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "fixtures", "sql", filename)
    )


def _make_temp_table(
    temp_id: str,
    produced_by: str,
    consumed_by: list[str],
) -> TempTableSpec:
    """创建 TempTableSpec 辅助函数。"""
    return TempTableSpec(
        temp_id=temp_id,
        produced_by=produced_by,
        consumed_by=consumed_by,
        column_defs=[],
    )


def _build_producer_plan(statement_id: str, produces: str) -> SqlStatement:
    """构建 PRODUCER——读取 test_fact CSV 表，聚合后写入 _temp 表。

    对应 SQL：
      SELECT stat_date, COUNT(id) AS cnt
      FROM test_fact AS tf
      GROUP BY stat_date
    """
    plan = SqlBuildPlan(
        plan_id=statement_id,
        spec_hash="e2e_test",
        steps=[
            ScanStep(
                step_id=f"{statement_id}_scan",
                table_ref="tf",
                required_columns=[
                    ColumnRef(
                        table_ref="tf", column_name="id", normalized_name="id"
                    ),
                    ColumnRef(
                        table_ref="tf",
                        column_name="stat_date",
                        normalized_name="stat_date",
                    ),
                ],
            ),
            AggregateStep(
                step_id=f"{statement_id}_agg",
                group_keys=[
                    ColumnRef(
                        table_ref="tf",
                        column_name="stat_date",
                        normalized_name="stat_date",
                    )
                ],
                metrics=[
                    AggregateSpec(
                        aggregation="COUNT", input_column="id", alias="cnt"
                    )
                ],
            ),
        ],
    )
    return SqlStatement(
        statement_id=statement_id,
        plan=plan,
        kind=StatementKind.PRODUCER,
        produces=produces,
    )


def _build_consumer_plan(
    statement_id: str,
    temp_ref: str,
    depends_on: list[str],
    kind: StatementKind = StatementKind.CONSUMER,
) -> SqlStatement:
    """构建 _temp 表消费者——从指定 _temp 表读取数据。

    对应 SQL：
      SELECT {temp_ref}.stat_date, {temp_ref}.cnt
      FROM {temp_ref} AS {temp_ref}
    """
    plan = SqlBuildPlan(
        plan_id=statement_id,
        spec_hash="e2e_test",
        steps=[
            ScanStep(
                step_id=f"{statement_id}_scan",
                table_ref=temp_ref,
                required_columns=[
                    ColumnRef(
                        table_ref=temp_ref,
                        column_name="stat_date",
                        normalized_name="stat_date",
                    ),
                    ColumnRef(
                        table_ref=temp_ref,
                        column_name="cnt",
                        normalized_name="cnt",
                    ),
                ],
            ),
        ],
    )
    return SqlStatement(
        statement_id=statement_id,
        plan=plan,
        kind=kind,
        depends_on=depends_on,
    )


def _build_failing_plan(statement_id: str, depends_on: list[str]) -> SqlStatement:
    """构建会执行失败的语句——引用不存在的表。

    对应 SQL：
      SELECT nonexistent.col FROM nonexistent AS nonexistent
    DuckDB 将抛出 "table not found" 错误。
    """
    plan = SqlBuildPlan(
        plan_id=statement_id,
        spec_hash="e2e_test",
        steps=[
            ScanStep(
                step_id=f"{statement_id}_scan",
                table_ref="nonexistent_table",
                required_columns=[
                    ColumnRef(
                        table_ref="nonexistent_table",
                        column_name="col",
                        normalized_name="col",
                    ),
                ],
            ),
        ],
    )
    return SqlStatement(
        statement_id=statement_id,
        plan=plan,
        kind=StatementKind.CONSUMER,
        depends_on=depends_on,
    )


# ════════════════════════════════════════════
# 集成测试
# ════════════════════════════════════════════


class TestProgramCompileAndExecute:
    """compile_program() + execute_program() 集成测试。"""

    def test_two_step_program_all_success(self):
        """两步聚合：PRODUCER (CSV→_temp_agg) → FINAL (SELECT FROM _temp_agg)。

        验证：编译产物正确、全部语句成功、cleanup 执行。
        """
        stmt_agg = _build_producer_plan("stmt_agg", "_temp_agg")
        stmt_output = _build_consumer_plan(
            "stmt_output", "_temp_agg", ["stmt_agg"], StatementKind.FINAL
        )
        temp_tables = [_make_temp_table("_temp_agg", "stmt_agg", ["stmt_output"])]

        builder = SqlProgramBuilder()
        program = builder.build_from_statements(
            statements=[stmt_agg, stmt_output],
            temp_tables=temp_tables,
            spec_hash="e2e_success",
            final_output="stmt_output",
        )

        # 编译
        compiler = DuckDbSqlCompiler(table_mapping={"tf": "test_fact"})
        artifact = compiler.compile_program(program)
        compiled = artifact.compiled

        # 验证编译产物
        assert compiled.program_id == program.program_id
        assert len(compiled.statements) == 2
        assert compiled.statement_order == ["stmt_agg", "stmt_output"]
        assert "CREATE TEMP TABLE _temp_agg" in compiled.statements[0].sql
        assert any(
            "DROP TABLE IF EXISTS _temp_agg" in s for s in compiled.cleanup_sql
        ), f"cleanup_sql 应包含 DROP，实际: {compiled.cleanup_sql}"

        # 执行
        executor = DuckDBExecutor(
            table_paths={"test_fact": _csv_path("test_fact.csv")}
        )
        result = executor.execute_program(compiled)

        # 验证执行结果
        assert result.completed_count == 2, (
            f"应全部成功，实际 completed={result.completed_count}，"
            f"failed_at={result.failed_at}"
        )
        assert result.failed_at is None
        assert result.cleanup_status == "success"

        for r in result.results:
            assert r.trace.status == ExecutionStatus.RUNTIME_PASS, (
                f"{r.statement_id}: {r.trace.error_message}"
            )

        # FINAL 应有结果行
        assert result.results[1].summary.row_count > 0

    def test_failure_stops_subsequent_and_cleanup_runs(self):
        """中间语句失败 → 阻断后续 → cleanup 仍然执行。

        验证：stmt_agg 成功，stmt_bad 失败，stmt_final 不执行，cleanup 成功。
        """
        stmt_agg = _build_producer_plan("stmt_agg", "_temp_agg")
        stmt_bad = _build_failing_plan("stmt_bad", ["stmt_agg"])
        stmt_final = _build_consumer_plan(
            "stmt_final", "_temp_agg", ["stmt_bad"], StatementKind.FINAL
        )
        temp_tables = [
            _make_temp_table("_temp_agg", "stmt_agg", ["stmt_final"])
        ]

        builder = SqlProgramBuilder()
        program = builder.build_from_statements(
            statements=[stmt_agg, stmt_bad, stmt_final],
            temp_tables=temp_tables,
            spec_hash="e2e_fail",
            final_output="stmt_final",
        )

        compiler = DuckDbSqlCompiler(table_mapping={"tf": "test_fact"})
        artifact = compiler.compile_program(program)

        executor = DuckDBExecutor(
            table_paths={"test_fact": _csv_path("test_fact.csv")}
        )
        result = executor.execute_program(artifact.compiled)

        # 验证：仅 stmt_agg 成功
        assert result.completed_count == 1, (
            f"仅第一个语句应成功，实际 completed={result.completed_count}"
        )
        assert result.failed_at == "stmt_bad", (
            f"失败语句应为 stmt_bad，实际={result.failed_at}"
        )

        # stmt_final 不应执行
        executed_ids = {r.statement_id for r in result.results}
        assert "stmt_final" not in executed_ids, (
            "stmt_final 不应被执行（被失败阻断）"
        )

        # cleanup 仍然执行
        assert result.cleanup_status == "success", (
            f"cleanup 应在失败后仍执行，实际={result.cleanup_status}，"
            f"错误={result.cleanup_error}"
        )

    def test_temp_table_lifecycle(self):
        """_temp 表完整生命周期：CREATE → READ → DROP。

        验证 cleanup_sql 包含 DROP，执行后所有结果成功。
        """
        stmt_agg = _build_producer_plan("stmt_agg", "_temp_lifecycle")
        stmt_output = _build_consumer_plan(
            "stmt_output", "_temp_lifecycle", ["stmt_agg"], StatementKind.FINAL
        )
        temp_tables = [
            _make_temp_table("_temp_lifecycle", "stmt_agg", ["stmt_output"])
        ]

        builder = SqlProgramBuilder()
        program = builder.build_from_statements(
            statements=[stmt_agg, stmt_output],
            temp_tables=temp_tables,
            spec_hash="lifecycle",
            final_output="stmt_output",
        )

        compiler = DuckDbSqlCompiler(table_mapping={"tf": "test_fact"})
        artifact = compiler.compile_program(program)

        # 验证 DROP 在 cleanup_sql 中
        assert any(
            "DROP TABLE IF EXISTS _temp_lifecycle" in s
            for s in artifact.compiled.cleanup_sql
        ), f"cleanup_sql 应包含 DROP，实际: {artifact.compiled.cleanup_sql}"

        executor = DuckDBExecutor(
            table_paths={"test_fact": _csv_path("test_fact.csv")}
        )
        result = executor.execute_program(artifact.compiled)

        assert result.completed_count == 2
        assert result.cleanup_status == "success"

    def test_compile_program_rejects_invalid_dag(self):
        """compile_program() 应在编译前校验 DAG——非法 DAG 应抛出 ValueError。"""
        plan = SqlBuildPlan(
            plan_id="stmt_A",
            spec_hash="invalid",
            steps=[
                ScanStep(
                    step_id="scan_1",
                    table_ref="tf",
                    required_columns=[
                        ColumnRef(
                            table_ref="tf",
                            column_name="id",
                            normalized_name="id",
                        ),
                    ],
                ),
            ],
        )
        stmt_a = SqlStatement(
            statement_id="stmt_A",
            plan=plan,
            kind=StatementKind.STANDALONE,
            depends_on=["stmt_nonexistent"],  # 引用不存在的依赖
        )

        builder = SqlProgramBuilder()
        program = builder.build_from_statements(
            statements=[stmt_a],
            temp_tables=[],
            spec_hash="invalid",
        )

        compiler = DuckDbSqlCompiler()
        with pytest.raises(ValueError, match="DAG 校验失败"):
            compiler.compile_program(program)
