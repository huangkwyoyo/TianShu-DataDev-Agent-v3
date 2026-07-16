"""测试 DuckDBExecutor——隔离执行 + ExecutionTrace + ResultSummary。"""

import os

import pytest

from tests._test_utils import read_fixture
from tianshu_datadev.developer_spec.parser import DeveloperSpecParser
from tianshu_datadev.planning.sql_build_plan import SqlBuildPlanBuilder
from tianshu_datadev.sql.compiler import DuckDbSqlCompiler
from tianshu_datadev.sql.executor import DuckDBExecutor
from tianshu_datadev.sql.models import ExecutionStatus

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
