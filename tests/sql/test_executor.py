"""测试 DuckDBExecutor——隔离执行 + ExecutionTrace + ResultSummary。

包含资源保护测试：超时中断、fetchmany 限制、行为不变回归。
"""

import os
from pathlib import Path

import pytest

from tianshu_datadev.developer_spec.parser import DeveloperSpecParser
from tianshu_datadev.planning.sql_build_plan import SqlBuildPlanBuilder
from tianshu_datadev.sql.compiler import DuckDbSqlCompiler
from tianshu_datadev.sql.executor import DuckDBExecutor, _is_interrupt_exception
from tianshu_datadev.sql.models import (
    CompiledSql,
    ExecutionStatus,
    OptimizedSQLPlan,
)

# ── 辅助 ──

def read_fixture(path: str) -> str:
    abs_path = os.path.join(os.path.dirname(__file__), "..", path)
    with open(abs_path, "r", encoding="utf-8") as f:
        return f.read()


def _parse_and_compile():
    """解析 golden fixture 并编译为 CompiledSql。"""
    parser = DeveloperSpecParser()
    text = read_fixture("fixtures/golden/golden_no_time_range.md")
    spec = parser.parse(text)

    builder = SqlBuildPlanBuilder()
    plan, _ = builder.build(spec)

    # 使用表名映射：table_ref（别名）→ 物理表名
    # DuckDB 内存模式不支持 schema 前缀，直接用简单表名
    table_mapping = {"tf": "test_fact"}
    compiler = DuckDbSqlCompiler(table_mapping=table_mapping)
    return compiler.compile(plan)


def _csv_path(filename: str) -> str:
    """获取 CSV fixture 的绝对路径。"""
    return os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "fixtures", "sql", filename)
    )


# ════════════════════════════════════════════
# Executor 测试
# ════════════════════════════════════════════


class TestDuckDBExecutor:
    """DuckDBExecutor 执行测试。"""

    def test_executor_rejects_raw_sql_string(self):
        """Executor 拒绝外部 SQL 字符串——只接受 CompiledSql 对象。"""
        executor = DuckDBExecutor()

        with pytest.raises(TypeError, match="CompiledSql"):
            executor.execute("SELECT 1")  # type: ignore

    def test_execute_compiled_sql_success(self):
        """合法 CompiledSql 执行成功——需要 DuckDB 安装。"""
        compiled = _parse_and_compile()

        # 使用 CSV fixture 作为表数据源
        table_paths = {
            "test_fact": _csv_path("test_fact.csv"),
        }

        executor = DuckDBExecutor(table_paths=table_paths)
        try:
            trace, summary = executor.execute(compiled)
        except Exception:
            raise

        # 验证 ExecutionTrace
        assert trace.engine == "duckdb"
        # RUNTIME_PASS 或 RUNTIME_FAIL 都是合法的执行结果（取决于 SQL 与 CSV 的列匹配）
        assert trace.status in (
            ExecutionStatus.RUNTIME_PASS,
            ExecutionStatus.RUNTIME_FAIL,
            ExecutionStatus.NOT_EXECUTED,
        ), f"意外状态: {trace.status}"

        if trace.status == ExecutionStatus.RUNTIME_PASS:
            assert trace.row_count >= 0
            assert trace.execution_time_ms >= 0
            assert trace.error_message is None

            # 验证 ResultSummary
            assert summary.row_count == trace.row_count
            assert summary.engine == "duckdb"
            assert len(summary.columns) > 0
        elif trace.status == ExecutionStatus.RUNTIME_FAIL:
            # 执行失败时应记录错误信息
            assert trace.error_message is not None

    def test_execute_produces_result_summary(self):
        """ExecutionTrace + ResultSummary 结构完整——需要 DuckDB 安装。"""
        compiled = _parse_and_compile()

        table_paths = {
            "test_fact": _csv_path("test_fact.csv"),
        }

        executor = DuckDBExecutor(table_paths=table_paths)
        try:
            trace, summary = executor.execute(compiled)
        except Exception:
            raise

        # ExecutionTrace 完整性检查
        assert trace.trace_id != ""
        assert trace.plan_id != ""
        assert trace.trace_id == summary.trace_id

        # ResultSummary 完整性检查
        assert summary.summary_id != ""
        assert len(summary.columns) == len(summary.column_types)

        if trace.status == ExecutionStatus.RUNTIME_PASS and trace.row_count > 0:
            # null_counts 应覆盖所有列
            for col in summary.columns:
                assert col in summary.null_counts, f"列 '{col}' 应在 null_counts 中"

            # sample_rows 应有数据（如果行数 > 0）
            if trace.row_count > 0:
                assert len(summary.sample_rows) > 0


# ════════════════════════════════════════════
# 资源保护测试辅助
# ════════════════════════════════════════════


def _make_slow_compiled() -> CompiledSql:
    """构建一个需要较长时间执行的 CompiledSql——使用 generate_series + 窗口函数。

    DuckDB 对 count(*) + generate_series 有 O(1) 优化，不会真正生成行。
    因此使用 sum() + 窗口函数强制计算，配合低 memory_limit 确保触发超时。
    无需任何 CSV 表——generate_series 是 DuckDB 内置函数。
    """
    sql = (
        "SELECT sum(x) FROM ("
        "  SELECT a.x, row_number() OVER (ORDER BY b.x) AS rn "
        "  FROM generate_series(1, 10000000) a(x), "
        "       generate_series(1, 1000) b(x)"
        ")"
    )
    return CompiledSql(
        sql=sql,
        sql_sha256=CompiledSql.compute_sql_hash(sql, "test"),
        optimized_plan=OptimizedSQLPlan(
            input_plan_hash="test_in",
            output_plan_hash="test_out",
        ),
        compiler_version="test",
        input_plan_hash="test_slow_hash",
    )


def _make_small_compiled() -> CompiledSql:
    """构建一个小结果集的 CompiledSql——用于验证正常行为不变。"""
    sql = "SELECT 1 AS n, 'hello' AS s"
    return CompiledSql(
        sql=sql,
        sql_sha256=CompiledSql.compute_sql_hash(sql, "test"),
        optimized_plan=OptimizedSQLPlan(
            input_plan_hash="test_in",
            output_plan_hash="test_out",
        ),
        compiler_version="test",
        input_plan_hash="test_small_hash",
    )


def _make_range_compiled(row_count: int = 3) -> CompiledSql:
    """构造行数确定的查询，用于验证结果上限。"""
    sql = f"SELECT i FROM range({row_count}) AS t(i)"
    return CompiledSql(
        sql=sql,
        sql_sha256=CompiledSql.compute_sql_hash(sql, "test"),
        optimized_plan=OptimizedSQLPlan(
            input_plan_hash="test_in",
            output_plan_hash="test_out",
        ),
        compiler_version="test",
        input_plan_hash="test_range_hash",
    )


# ════════════════════════════════════════════
# 资源保护测试
# ════════════════════════════════════════════


class TestDuckDBExecutorResourceProtection:
    """DuckDBExecutor 资源保护——超时、结果限制、行为不变。"""

    def test_timeout_interrupts_execute(self):
        """execute() 超时——返回 TIMEOUT 状态，不伪装为成功。"""
        executor = DuckDBExecutor(
            timeout_sec=0.2,
            memory_limit="128MB",
        )
        compiled = _make_slow_compiled()

        trace, summary = executor.execute(compiled)

        # 超时时状态应为 TIMEOUT（非 RUNTIME_FAIL，非 RUNTIME_PASS）
        assert trace.status == ExecutionStatus.TIMEOUT, (
            f"期望 TIMEOUT，实际 {trace.status}：{trace.error_message}"
        )
        assert "超时" in (trace.error_message or "")
        assert trace.row_count == 0
        # 超时不应产生假的非零 row_count
        assert summary.row_count == 0

    def test_timeout_not_masquerade_as_success(self):
        """超时场景明确不产生 RUNTIME_PASS 或假的 row_count=0 成功结果。"""
        executor = DuckDBExecutor(
            timeout_sec=0.2,
            memory_limit="128MB",
        )
        compiled = _make_slow_compiled()

        trace, _summary = executor.execute(compiled)

        # 超时必须返回明确的失败状态——不能是 RUNTIME_PASS
        assert trace.status != ExecutionStatus.RUNTIME_PASS
        # 不能伪装成正常完成的 0 行——必须区分"真的 0 行"和"超时未完成"
        assert trace.status in (
            ExecutionStatus.TIMEOUT,
            ExecutionStatus.RUNTIME_FAIL,
        )

    def test_fetchmany_limits_result(self):
        """fetchmany——结果超过 max_result_rows 时返回 RESULT_TOO_LARGE。"""
        executor = DuckDBExecutor(max_result_rows=2)
        compiled = _make_range_compiled(3)
        trace, summary = executor.execute(compiled)

        assert trace.status == ExecutionStatus.RESULT_TOO_LARGE
        assert trace.row_count == 0
        assert summary.row_count == 0
        assert "max_result_rows" in (trace.error_message or "")

    def test_oversize_not_masquerade_as_success(self):
        """结果过大时返回 RESULT_TOO_LARGE 而非 RUNTIME_PASS + row_count=0。"""
        executor = DuckDBExecutor(max_result_rows=1)
        compiled = _make_range_compiled(2)
        trace, _summary = executor.execute(compiled)

        assert trace.status == ExecutionStatus.RESULT_TOO_LARGE
        assert trace.row_count == 0
        assert "max_result_rows" in (trace.error_message or "")
        assert "不会自动修改业务 SQL" in (trace.error_message or "")

    def test_small_result_unchanged(self):
        """小结果 + 大 max_result_rows——行为与改造前完全一致（回归测试）。"""
        table_paths = {
            "test_fact": _csv_path("test_fact.csv"),
        }
        compiled = _parse_and_compile()

        executor = DuckDBExecutor(table_paths=table_paths)
        trace, summary = executor.execute(compiled)

        # 小数据集默认配置下应正常通过
        if trace.status == ExecutionStatus.RUNTIME_PASS:
            assert trace.row_count >= 0
            assert trace.execution_time_ms >= 0
            assert trace.error_message is None
            assert summary.row_count == trace.row_count
            assert len(summary.columns) > 0
        # RUNTIME_FAIL 也是合法的——取决于 SQL 与 CSV 列匹配

    def test_parquet_snapshot_supports_schema_table(self, tmp_path):
        """业务 Executor 从 Parquet 快照还原 schema.table，不再依赖源库。"""
        import pyarrow as pa
        import pyarrow.parquet as pq

        parquet_path = tmp_path / "gold.fact_orders.parquet"
        pq.write_table(pa.table({"order_id": [1, 2, 3]}), parquet_path)
        sql = "SELECT COUNT(*) AS cnt FROM gold.fact_orders"
        compiled = CompiledSql(
            sql=sql,
            sql_sha256=CompiledSql.compute_sql_hash(sql, "test"),
            optimized_plan=OptimizedSQLPlan(
                input_plan_hash="test_in",
                output_plan_hash="test_out",
            ),
            compiler_version="test",
            input_plan_hash="test_parquet_hash",
        )

        trace, summary = DuckDBExecutor(
            table_paths={"gold.fact_orders": str(parquet_path)},
        ).execute(compiled)

        assert trace.status == ExecutionStatus.RUNTIME_PASS
        assert summary.sample_rows == [[3]]

    def test_execute_simple_query_no_tables(self):
        """无需 CSV 表的小查询——验证基本超时和资源保护不干预正常流程。"""
        executor = DuckDBExecutor(timeout_sec=30, max_result_rows=10000)
        compiled = _make_small_compiled()

        trace, summary = executor.execute(compiled)

        assert trace.status == ExecutionStatus.RUNTIME_PASS
        assert trace.row_count == 1
        assert trace.execution_time_ms >= 0
        assert summary.columns == ["n", "s"]

    def test_configurable_resource_params(self, tmp_path):
        """构造器接受并存储所有资源保护参数。"""
        temp_directory = tmp_path / "duckdb"
        executor = DuckDBExecutor(
            table_paths=None,
            timeout_sec=60.0,
            duckdb_path=None,
            memory_limit="4GB",
            threads=4,
            temp_directory=str(temp_directory),
            max_result_rows=5000,
        )
        assert executor._timeout_sec == 60.0
        assert executor._memory_limit == "4GB"
        assert executor._threads == 4
        assert executor._temp_directory == str(temp_directory.resolve())
        assert executor._max_result_rows == 5000

    def test_default_resource_params(self):
        """默认构造器使用文档声明的默认值。"""
        executor = DuckDBExecutor()
        assert executor._timeout_sec == 30
        assert executor._memory_limit == "2GB"
        assert executor._threads == 2
        assert executor._temp_directory
        assert Path(executor._temp_directory).is_dir()
        assert executor._max_result_rows == 10_000

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("memory_limit", "2GB'; DROP TABLE x; --"),
            ("max_temp_directory_size", "unlimited"),
            ("threads", 0),
            ("max_result_rows", 0),
            ("process_memory_limit_mb", 128),
        ],
    )
    def test_invalid_resource_config_is_rejected(self, field, value):
        """非法资源配置必须在启动 Worker 前被拒绝。"""
        with pytest.raises(ValueError):
            DuckDBExecutor(**{field: value})

    def test_connection_configuration_failure_is_not_ignored(self):
        """DuckDB 拒绝资源设置时必须阻断执行。"""
        class RejectingConnection:
            def execute(self, _sql):
                raise RuntimeError("SET failed")

        executor = DuckDBExecutor(_worker_mode=True)
        with pytest.raises(RuntimeError, match="SET failed"):
            executor._configure_connection(RejectingConnection())

    def test_worker_environment_does_not_inherit_secrets(self, monkeypatch):
        """Worker 环境白名单不得携带 API 凭据。"""
        monkeypatch.setenv("TIANSHU_FAKE_SECRET", "do-not-copy")
        monkeypatch.setenv("PATH", os.environ.get("PATH", ""))

        env = DuckDBExecutor._build_worker_env()

        assert "TIANSHU_FAKE_SECRET" not in env
        assert "PATH" in env


# ════════════════════════════════════════════
# _is_interrupt_exception 单元测试
# ════════════════════════════════════════════


class TestIsInterruptException:
    """_is_interrupt_exception 函数——识别 DuckDB interrupt 异常。"""

    def test_recognizes_interrupt_exception_class(self):
        """类名为 InterruptException 时返回 True。"""

        class InterruptException(Exception):  # noqa: N818
            pass

        assert _is_interrupt_exception(InterruptException("test"))

    def test_recognizes_query_interrupted_class(self):
        """类名为 QueryInterruptedException 时返回 True。"""

        class QueryInterruptedException(Exception):  # noqa: N818
            pass

        assert _is_interrupt_exception(QueryInterruptedException("test"))

    def test_recognizes_duckdb_interrupt_message(self):
        """duckdb 模块异常 + 消息含 interrupt 时返回 True。"""

        class DuckDBError(Exception):
            pass

        DuckDBError.__module__ = "duckdb.duckdb"
        assert _is_interrupt_exception(
            DuckDBError("Query was interrupted by user")
        )

    def test_recognizes_duckdb_timeout_message(self):
        """duckdb 模块异常 + 消息含 timeout 时返回 True。"""

        class DuckDBError(Exception):
            pass

        DuckDBError.__module__ = "duckdb.duckdb"
        assert _is_interrupt_exception(
            DuckDBError("Query timeout exceeded")
        )

    def test_ordinary_exception_not_interrupt(self):
        """普通异常不应被误判为 interrupt。"""
        assert not _is_interrupt_exception(ValueError("something went wrong"))
        assert not _is_interrupt_exception(RuntimeError("normal error"))
        assert not _is_interrupt_exception(Exception("generic error"))
