"""DuckDB CDP builder 交叉验证——与 Python oracle 逐场景对比。"""
from __future__ import annotations

import pytest

from tests.spark.test_cdp_golden_vectors import G1_FULL_DIGEST_HEX, G2_FULL_DIGEST_HEX


class TestDuckDBFullDigest:
    """DuckDB 完整 CDP digest——与 Python oracle 交叉验证。"""

    @pytest.fixture
    def oracle(self):
        from tianshu_datadev.spark.cdp_serializer import CdpCanonicalSerializer

        return CdpCanonicalSerializer()

    @pytest.fixture(autouse=True)
    def _check_duckdb(self):
        """跳过测试（若 duckdb 未安装）。"""
        pytest.importorskip("duckdb")
        import duckdb

        # 为整个测试类共享一个内存连接
        self._con = duckdb.connect(":memory:")

    @staticmethod
    def _int64_spec():
        """G1-G3 共用的 INT64 spec。"""
        from tianshu_datadev.spark.cdp_spec import CreDigestSpec, TypeFamily

        return CreDigestSpec(
            output_columns=["id"],
            type_families=[TypeFamily.INT64],
            timezone="UTC",
            decimal_precision=[None],
            decimal_scale=[None],
            float_precision=[None],
        )

    def _run_duckdb_digest(self, source_sql: str, spec) -> str:
        """在共享 DuckDB 连接中执行 CDP 查询并返回 full_digest_hex。"""
        from tianshu_datadev.spark.cdp_duckdb_builder import DuckdbCdpBuilder
        from tianshu_datadev.spark.cdp_spec import compute_digest_spec_hash

        builder = DuckdbCdpBuilder()
        spec_hash_hex = compute_digest_spec_hash(spec).hex()
        cdp_query = builder.build_query(source_sql, spec, spec_hash_hex=spec_hash_hex)
        return self._con.execute(cdp_query).fetchone()[0]

    # ── G1: 空结果集 ──

    def test_g1_empty_result_vs_golden(self, oracle):
        """DuckDB 空结果集 → full_digest == Python oracle == G1 黄金向量。"""
        spec = self._int64_spec()
        duckdb_digest = self._run_duckdb_digest(
            "SELECT CAST(42 AS BIGINT) AS id WHERE 1=0", spec
        )
        oracle_digest = oracle.compute_full_digest([], spec)

        assert duckdb_digest == oracle_digest, (
            f"DuckDB {duckdb_digest} ≠ oracle {oracle_digest}"
        )
        assert duckdb_digest == G1_FULL_DIGEST_HEX, (
            f"DuckDB {duckdb_digest} ≠ G1 {G1_FULL_DIGEST_HEX}"
        )

    # ── G2: 单行 INT64 ──

    def test_g2_single_row_vs_golden(self, oracle):
        """DuckDB 单行 → full_digest == Python oracle == G2 黄金向量。"""
        spec = self._int64_spec()

        self._con.execute("CREATE OR REPLACE TABLE _t_g2 (id BIGINT)")
        self._con.execute("INSERT INTO _t_g2 VALUES (42)")

        duckdb_digest = self._run_duckdb_digest("SELECT id FROM _t_g2", spec)
        oracle_digest = oracle.compute_full_digest([{"id": 42}], spec)

        assert duckdb_digest == oracle_digest, (
            f"DuckDB {duckdb_digest} ≠ oracle {oracle_digest}"
        )
        assert duckdb_digest == G2_FULL_DIGEST_HEX, (
            f"DuckDB {duckdb_digest} ≠ G2 {G2_FULL_DIGEST_HEX}"
        )

    # ── G6: 两行相同 INT64 42（多重集）──

    def test_g6_duplicate_rows_vs_oracle(self, oracle):
        """DuckDB 两行相同 → full_digest == Python oracle == G6。"""
        from tianshu_datadev.spark.cdp_spec import CreDigestSpec, TypeFamily

        spec = CreDigestSpec(
            output_columns=["id"],
            type_families=[TypeFamily.INT64],
            timezone="UTC",
            decimal_precision=[None],
            decimal_scale=[None],
            float_precision=[None],
        )

        self._con.execute("CREATE OR REPLACE TABLE _t_g6 (id BIGINT)")
        self._con.execute("INSERT INTO _t_g6 VALUES (42), (42)")

        duckdb_digest = self._run_duckdb_digest("SELECT id FROM _t_g6", spec)
        oracle_digest = oracle.compute_full_digest(
            [{"id": 42}, {"id": 42}], spec
        )

        assert duckdb_digest == oracle_digest, (
            f"DuckDB {duckdb_digest} ≠ oracle {oracle_digest}"
        )
        # 多重集语义：两行相同 ≠ 单行
        assert duckdb_digest != G2_FULL_DIGEST_HEX

    # ── 多行多类型：50 行 INT64 + VARCHAR + FLOAT64 ──

    def test_50_rows_vs_oracle(self, oracle):
        """DuckDB 50 行 INT64+VARCHAR+FLOAT64 → full_digest == Python oracle。"""
        from tianshu_datadev.spark.cdp_spec import CreDigestSpec, TypeFamily

        spec = CreDigestSpec(
            output_columns=["id", "name", "val"],
            type_families=[
                TypeFamily.INT64,
                TypeFamily.VARCHAR,
                TypeFamily.FLOAT64,
            ],
            timezone="UTC",
            decimal_precision=[None, None, None],
            decimal_scale=[None, None, None],
            float_precision=[None, None, 2],
        )

        rows = [
            {"id": i, "name": f"row_{i}", "val": round(i * 1.5, 2)}
            for i in range(50)
        ]
        oracle_digest = oracle.compute_full_digest(rows, spec)

        self._con.execute(
            "CREATE OR REPLACE TABLE _t_multi (id BIGINT, name VARCHAR, val DOUBLE)"
        )
        for r in rows:
            self._con.execute(
                "INSERT INTO _t_multi VALUES (?, ?, ?)",
                (r["id"], r["name"], r["val"]),
            )

        duckdb_digest = self._run_duckdb_digest(
            "SELECT id, name, val FROM _t_multi", spec
        )

        assert duckdb_digest == oracle_digest, (
            f"DuckDB {duckdb_digest} ≠ oracle {oracle_digest}"
        )

    # ── NULL 值 ──

    def test_null_values_vs_oracle(self, oracle):
        """DuckDB NULL 值 → full_digest == Python oracle。"""
        from tianshu_datadev.spark.cdp_spec import CreDigestSpec, TypeFamily

        spec = CreDigestSpec(
            output_columns=["id", "name"],
            type_families=[TypeFamily.INT64, TypeFamily.VARCHAR],
            timezone="UTC",
            decimal_precision=[None, None],
            decimal_scale=[None, None],
            float_precision=[None, None],
        )

        rows = [
            {"id": 1, "name": "alice"},
            {"id": None, "name": "bob"},
            {"id": 3, "name": None},
            {"id": None, "name": None},
        ]
        oracle_digest = oracle.compute_full_digest(rows, spec)

        self._con.execute(
            "CREATE OR REPLACE TABLE _t_null (id BIGINT, name VARCHAR)"
        )
        for r in rows:
            self._con.execute(
                "INSERT INTO _t_null VALUES (?, ?)", (r["id"], r["name"])
            )

        duckdb_digest = self._run_duckdb_digest(
            "SELECT id, name FROM _t_null", spec
        )

        assert duckdb_digest == oracle_digest, (
            f"DuckDB {duckdb_digest} ≠ oracle {oracle_digest}"
        )

    # ── 确定性验证 ──

    def test_deterministic_same_data(self, oracle):
        """同一数据集两次计算 → 相同 full_digest。"""
        from tianshu_datadev.spark.cdp_spec import CreDigestSpec, TypeFamily

        spec = CreDigestSpec(
            output_columns=["id"],
            type_families=[TypeFamily.INT64],
            timezone="UTC",
            decimal_precision=[None],
            decimal_scale=[None],
            float_precision=[None],
        )

        self._con.execute("CREATE OR REPLACE TABLE _t_det (id BIGINT)")
        for i in range(10):
            self._con.execute("INSERT INTO _t_det VALUES (?)", (i,))

        d1 = self._run_duckdb_digest("SELECT id FROM _t_det ORDER BY id", spec)
        d2 = self._run_duckdb_digest(
            "SELECT id FROM _t_det ORDER BY id DESC", spec
        )

        # 顺序无关性：ORDER BY 不应影响 hash（CDP 内置排序）
        assert d1 == d2


class TestExecuteWithCDP:
    """DuckDBExecutor.execute_with_cdp() 集成测试。"""

    @pytest.fixture(autouse=True)
    def _check_duckdb(self):
        """跳过测试（若 duckdb 未安装）。"""
        pytest.importorskip("duckdb")

    def test_execute_with_cdp_success(self):
        """execute_with_cdp 正确返回 DigestExecutionEnvelope——自包含查询。"""
        from tianshu_datadev.sql.executor import DuckDBExecutor
        from tianshu_datadev.sql.models import CompiledSql, OptimizedSQLPlan
        from tianshu_datadev.spark.cdp_spec import (
            CreDigestSpec,
            DigestExecutionEnvelope,
            TypeFamily,
        )

        # 自包含 SQL——不依赖外部表，仅验证 Envelope 结构正确
        compiled = CompiledSql(
            sql="SELECT 1 AS id, 'hello' AS name",
            sql_sha256="test",
            optimized_plan=OptimizedSQLPlan(
                input_plan_hash="test", output_plan_hash="test"
            ),
            compiler_version="test",
            input_plan_hash="test",
        )

        spec = CreDigestSpec(
            output_columns=["id", "name"],
            type_families=[TypeFamily.INT64, TypeFamily.VARCHAR],
            timezone="UTC",
            decimal_precision=[None, None],
            decimal_scale=[None, None],
            float_precision=[None, None],
        )

        executor = DuckDBExecutor()
        result = executor.execute_with_cdp(
            compiled=compiled, spec=spec, snapshot_id="test-snap-001"
        )

        assert isinstance(result, DigestExecutionEnvelope)
        assert result.execution_status == "SUCCESS"
        assert result.snapshot_id == "test-snap-001"
        assert result.summary is not None
        assert result.summary.row_count == 1  # 1 行自包含数据
        assert len(result.summary.full_digest) == 64  # SHA256 hex
        assert result.engine_version == "duckdb"
        assert result.protocol_version == "cdp-v1"

    def test_execute_with_cdp_failed(self):
        """执行失败时正确返回 FAILED 状态。"""
        from tianshu_datadev.sql.executor import DuckDBExecutor
        from tianshu_datadev.sql.models import CompiledSql, OptimizedSQLPlan
        from tianshu_datadev.spark.cdp_spec import (
            CreDigestSpec,
            DigestExecutionEnvelope,
            TypeFamily,
        )

        # 使用不存在的表——执行应失败
        compiled = CompiledSql(
            sql="SELECT * FROM nonexistent_table",
            sql_sha256="test",
            optimized_plan=OptimizedSQLPlan(
                input_plan_hash="test", output_plan_hash="test"
            ),
            compiler_version="test",
            input_plan_hash="test",
        )

        spec = CreDigestSpec(
            output_columns=["id"],
            type_families=[TypeFamily.INT64],
            timezone="UTC",
            decimal_precision=[None],
            decimal_scale=[None],
            float_precision=[None],
        )

        executor = DuckDBExecutor()
        result = executor.execute_with_cdp(
            compiled=compiled, spec=spec, snapshot_id="test-snap-002"
        )

        assert isinstance(result, DigestExecutionEnvelope)
        assert result.execution_status == "FAILED"
        assert result.summary is None
        assert result.error is not None
        assert "nonexistent_table" in result.error


# ═══════════════════════════════════════════════════════════════
# Spark CDP 探针——验证内置表达式可行性
# ═══════════════════════════════════════════════════════════════


class TestSparkBuiltinProbes:
    """Spark 内置表达式探针——验证 F.unhex(F.format_string(...)) 能产出精确 4B/8B BE。

    关键验证：
    1. 整数 2 的 4B BE → b'\\x00\\x00\\x00\\x02'
    2. 整数 256 的 8B BE → b'\\x00\\x00\\x00\\x00\\x00\\x00\\x01\\x00'
    3. CHR(0) 在字符串拼接中的截断风险（文档化限制）
    """

    @pytest.fixture(autouse=True)
    def _check_pyspark(self):
        """跳过测试（若 PySpark 不可用）。"""
        pytest.importorskip("pyspark")
        # Windows 上 Spark UDF Worker 需要 python 而非 python3
        import os

        if os.name == "nt":
            os.environ.setdefault("PYSPARK_PYTHON", "python")
        try:
            from pyspark.sql import SparkSession

            SparkSession.builder.master("local[1]").appName("cdp-probe").getOrCreate()
        except Exception:
            pytest.skip("PySpark 无法启动 SparkSession")

    def test_probe_spark_builtin_4b_be(self):
        """Spark 内置表达式能产出正确的 4B BE BLOB。

        0x00000002 的 4B BE = 00 00 00 02。
        F.unhex(F.format_string("%08X", 2)) 理论上应返回 b'\\x00\\x00\\x00\\x02'。
        """
        from pyspark.sql import SparkSession, functions as F

        spark = SparkSession.builder.master("local[1]").appName("cdp-probe").getOrCreate()
        result = (
            spark.range(1)
            .select(F.unhex(F.format_string("%08X", F.lit(2).cast("int"))).alias("be4"))
            .collect()[0][0]
        )
        assert result == b"\x00\x00\x00\x02", (
            f"Spark 4B BE (2): {result!r}"
        )
        assert isinstance(result, bytes)
        assert len(result) == 4

    def test_probe_spark_builtin_8b_be(self):
        """Spark 内置表达式能产出正确的 8B BE BLOB。

        256 的 8B BE = 00 00 00 00 00 00 01 00。
        """
        from pyspark.sql import SparkSession, functions as F

        spark = SparkSession.builder.master("local[1]").appName("cdp-probe").getOrCreate()
        result = (
            spark.range(1)
            .select(
                F.unhex(F.format_string("%016X", F.lit(256).cast("bigint"))).alias("be8")
            )
            .collect()[0][0]
        )
        assert result == b"\x00\x00\x00\x00\x00\x00\x01\x00", (
            f"Spark 8B BE (256): {result!r}"
        )
        assert isinstance(result, bytes)
        assert len(result) == 8

    def test_probe_spark_builtin_be_zero(self):
        """Spark 内置表达式能处理整数 0（无 CHR(0) 截断风险）。

        0 的 4B BE = 00 00 00 00。
        F.unhex("00000000") 应正确返回 4 字节全零，不会因 CHR(0) 被截断。
        """
        from pyspark.sql import SparkSession, functions as F

        spark = SparkSession.builder.master("local[1]").appName("cdp-probe").getOrCreate()
        result = (
            spark.range(1)
            .select(F.unhex(F.format_string("%08X", F.lit(0).cast("int"))).alias("be0"))
            .collect()[0][0]
        )
        # CHR(0) 方案会被截断为 b''，但 hex 方案正确返回 b'\\x00\\x00\\x00\\x00'
        assert result == b"\x00\x00\x00\x00", (
            f"Spark 4B BE (0): {result!r} —— CHR(0) 会被截断，hex 方案不应"
        )
        assert len(result) == 4

    def test_probe_spark_builtin_be_max(self):
        """Spark 内置表达式能处理 4B BE 全 1（用于 NULL 编码的 0xFFFFFFFF）。"""
        from pyspark.sql import SparkSession, functions as F

        spark = SparkSession.builder.master("local[1]").appName("cdp-probe").getOrCreate()
        result = (
            spark.range(1)
            .select(
                F.unhex(F.format_string("%08X", F.lit(0xFFFFFFFF).cast("int"))).alias(
                    "bemax"
                )
            )
            .collect()[0][0]
        )
        assert result == b"\xff\xff\xff\xff", (
            f"Spark 4B BE (0xFFFFFFFF): {result!r}"
        )
        assert len(result) == 4


# ═══════════════════════════════════════════════════════════════
# Spark 完整 CDP digest——与 Python oracle 交叉验证
# ═══════════════════════════════════════════════════════════════


class TestSparkFullDigest:
    """Spark 完整 CDP digest——通过子进程隔离执行，与 Python oracle 交叉验证。

    每个测试：
    1. 使用 pyarrow 写入测试 parquet 数据
    2. 通过 LocalSparkExecutor 在子进程中执行 CDP 计算
    3. 断言 Spark digest == Python oracle digest
    """

    @pytest.fixture
    def oracle(self):
        from tianshu_datadev.spark.cdp_serializer import CdpCanonicalSerializer

        return CdpCanonicalSerializer()

    @pytest.fixture(autouse=True)
    def _check_pyspark(self):
        """跳过测试（若 PySpark 不可用）。"""
        pytest.importorskip("pyspark")
        # Windows 上 Spark UDF Worker 需要 python 而非 python3
        import os

        if os.name == "nt":
            os.environ.setdefault("PYSPARK_PYTHON", "python")

    @staticmethod
    def _int64_spec():
        """G1-G3 共用的 INT64 spec。"""
        from tianshu_datadev.spark.cdp_spec import CreDigestSpec, TypeFamily

        return CreDigestSpec(
            output_columns=["id"],
            type_families=[TypeFamily.INT64],
            timezone="UTC",
            decimal_precision=[None],
            decimal_scale=[None],
            float_precision=[None],
        )

    def _write_parquet(self, tmp_path, rows, columns):
        """使用 pyarrow 写入测试 parquet 数据和索引文件。

        columns 可以是 str（列名）或 pa.Field（显式类型定义）。
        空 rows 时必须使用 pa.Field 指定 schema 以避免 null 类型。

        Args:
            tmp_path: 临时目录路径
            rows: list[dict] —— 每行的列值字典
            columns: list[str | pa.Field] —— 列名或类型定义
        """
        import json

        import pyarrow as pa
        import pyarrow.parquet as pq

        # 提取列名和类型
        col_names: list[str] = []
        col_fields: list[pa.Field] = []
        for c in columns:
            if isinstance(c, pa.Field):
                col_names.append(c.name)
                col_fields.append(c)
            else:
                col_names.append(str(c))
                col_fields.append(None)  # 后述推断

        if rows:
            # 非空行——从 dict 列表自动推断 schema
            data = {col: [r.get(col) for r in rows] for col in col_names}
            table = pa.table(data)
        else:
            # 空行——必须使用显式 pa.Field schema
            fields = [f for f in col_fields if f is not None]
            if not fields:
                # 回退：使用 null 类型（极少数情况）
                fields = [pa.field(c, pa.null()) for c in col_names]
            schema = pa.schema(fields)
            arrays = [pa.array([], type=f.type) for f in fields]
            table = pa.Table.from_arrays(arrays, schema=schema)

        data_path = str(tmp_path / "result.parquet")
        pq.write_table(table, data_path)

        # 写入索引——key 必须为 "result"（与 builder 代码中的 inputs['result'] 对齐）
        (tmp_path / "_inputs_index.json").write_text(
            json.dumps({"result": "result.parquet"}), encoding="utf-8"
        )

    def _run_spark_digest(self, data_dir, spec):
        """通过 LocalSparkExecutor 在子进程中执行 CDP 计算。

        Args:
            data_dir: 包含 parquet 文件和 _inputs_index.json 的目录

        Returns:
            full_digest hex 字符串
        """
        from tianshu_datadev.spark.executor import LocalSparkExecutor

        executor = LocalSparkExecutor(timeout_seconds=180)
        result = executor.execute_with_cdp(
            spec=spec,
            snapshot_id="test",
            data_dir=str(data_dir),
        )

        assert result.execution_status == "SUCCESS", (
            f"Spark CDP 失败: {result.error}"
        )
        assert result.summary is not None
        return result.summary.full_digest, result.summary.row_count

    # ── G1: 空结果集 ──

    def test_g1_empty_result_vs_golden(self, tmp_path, oracle):
        """Spark 空结果集 → full_digest == Python oracle == G1 黄金向量。"""
        import pyarrow as pa

        from tests.spark.test_cdp_golden_vectors import G1_FULL_DIGEST_HEX

        spec = self._int64_spec()
        # 写入空的 parquet——含 schema 但无数据行
        self._write_parquet(
            tmp_path,
            [],
            [pa.field("id", pa.int64())],
        )

        spark_digest, spark_count = self._run_spark_digest(tmp_path, spec)
        oracle_digest = oracle.compute_full_digest([], spec)

        assert spark_count == 0
        assert spark_digest == oracle_digest, (
            f"Spark {spark_digest} ≠ oracle {oracle_digest}"
        )
        assert spark_digest == G1_FULL_DIGEST_HEX, (
            f"Spark {spark_digest} ≠ G1 {G1_FULL_DIGEST_HEX}"
        )

    # ── G2: 单行 INT64 42 ──

    def test_g2_single_row_vs_golden(self, tmp_path, oracle):
        """Spark 单行 → full_digest == Python oracle == G2 黄金向量。"""
        from tests.spark.test_cdp_golden_vectors import G2_FULL_DIGEST_HEX

        spec = self._int64_spec()
        self._write_parquet(
            tmp_path,
            [{"id": 42}],
            ["id"],
        )

        spark_digest, spark_count = self._run_spark_digest(tmp_path, spec)
        oracle_digest = oracle.compute_full_digest([{"id": 42}], spec)

        assert spark_count == 1
        assert spark_digest == oracle_digest, (
            f"Spark {spark_digest} ≠ oracle {oracle_digest}"
        )
        assert spark_digest == G2_FULL_DIGEST_HEX, (
            f"Spark {spark_digest} ≠ G2 {G2_FULL_DIGEST_HEX}"
        )

    # ── G3: NULL INT64 ──

    def test_g3_null_value_vs_oracle(self, tmp_path, oracle):
        """Spark NULL → full_digest == Python oracle。

        NULL 编码：tag || 0xFFFFFFFF（无 value_bytes）。
        """
        spec = self._int64_spec()
        self._write_parquet(
            tmp_path,
            [{"id": None}],
            ["id"],
        )

        spark_digest, spark_count = self._run_spark_digest(tmp_path, spec)
        oracle_digest = oracle.compute_full_digest([{"id": None}], spec)

        assert spark_count == 1
        assert spark_digest == oracle_digest, (
            f"Spark {spark_digest} ≠ oracle {oracle_digest}"
        )

    # ── G6: 两行相同 INT64 42（多重集）──

    def test_g6_duplicate_rows_vs_oracle(self, tmp_path, oracle):
        """Spark 两行相同 → full_digest == Python oracle ≠ G2。"""
        from tests.spark.test_cdp_golden_vectors import G2_FULL_DIGEST_HEX

        spec = self._int64_spec()
        self._write_parquet(
            tmp_path,
            [{"id": 42}, {"id": 42}],
            ["id"],
        )

        spark_digest, spark_count = self._run_spark_digest(tmp_path, spec)
        oracle_digest = oracle.compute_full_digest(
            [{"id": 42}, {"id": 42}], spec
        )

        assert spark_count == 2
        assert spark_digest == oracle_digest, (
            f"Spark {spark_digest} ≠ oracle {oracle_digest}"
        )
        # 多重集语义：两行相同 ≠ 单行
        assert spark_digest != G2_FULL_DIGEST_HEX

    # ── 多行多类型：50 行 INT64 + VARCHAR + FLOAT64 ──

    def test_50_rows_vs_oracle(self, tmp_path, oracle):
        """Spark 50 行多类型 → full_digest == Python oracle。"""
        from tianshu_datadev.spark.cdp_spec import CreDigestSpec, TypeFamily

        spec = CreDigestSpec(
            output_columns=["id", "name", "val"],
            type_families=[
                TypeFamily.INT64,
                TypeFamily.VARCHAR,
                TypeFamily.FLOAT64,
            ],
            timezone="UTC",
            decimal_precision=[None, None, None],
            decimal_scale=[None, None, None],
            float_precision=[None, None, 2],
        )

        rows = [
            {"id": i, "name": f"row_{i}", "val": round(i * 1.5, 2)}
            for i in range(50)
        ]

        self._write_parquet(
            tmp_path,
            rows,
            ["id", "name", "val"],
        )

        spark_digest, spark_count = self._run_spark_digest(tmp_path, spec)
        oracle_digest = oracle.compute_full_digest(rows, spec)

        assert spark_count == 50
        assert spark_digest == oracle_digest, (
            f"Spark {spark_digest} ≠ oracle {oracle_digest}"
        )

    # ── NULL 值 ──

    def test_null_values_vs_oracle(self, tmp_path, oracle):
        """Spark 含 NULL 值的多列 → full_digest == Python oracle。"""
        from tianshu_datadev.spark.cdp_spec import CreDigestSpec, TypeFamily

        spec = CreDigestSpec(
            output_columns=["id", "name"],
            type_families=[TypeFamily.INT64, TypeFamily.VARCHAR],
            timezone="UTC",
            decimal_precision=[None, None],
            decimal_scale=[None, None],
            float_precision=[None, None],
        )

        rows = [
            {"id": 1, "name": "alice"},
            {"id": None, "name": "bob"},
            {"id": 3, "name": None},
            {"id": None, "name": None},
        ]

        self._write_parquet(
            tmp_path,
            rows,
            ["id", "name"],
        )

        spark_digest, spark_count = self._run_spark_digest(tmp_path, spec)
        oracle_digest = oracle.compute_full_digest(rows, spec)

        assert spark_count == 4
        assert spark_digest == oracle_digest, (
            f"Spark {spark_digest} ≠ oracle {oracle_digest}"
        )

    # ── 确定性验证 ──

    def test_deterministic_same_data(self, tmp_path, oracle):
        """同一数据集两次计算 → 相同 full_digest。"""
        spec = self._int64_spec()
        rows = [{"id": i} for i in range(10)]

        self._write_parquet(
            tmp_path,
            rows,
            ["id"],
        )

        d1, _ = self._run_spark_digest(tmp_path, spec)
        d2, _ = self._run_spark_digest(tmp_path, spec)

        assert d1 == d2

    # ── BOOLEAN 类型 ──

    def test_boolean_values(self, tmp_path, oracle):
        """Spark BOOLEAN 类型 → full_digest == Python oracle。

        BOOLEAN 编码：tag=0x01, "true"(4B) / "false"(5B)。
        """
        import pyarrow as pa

        from tianshu_datadev.spark.cdp_spec import CreDigestSpec, TypeFamily

        spec = CreDigestSpec(
            output_columns=["flag"],
            type_families=[TypeFamily.BOOLEAN],
            timezone="UTC",
            decimal_precision=[None],
            decimal_scale=[None],
            float_precision=[None],
        )

        rows = [{"flag": True}, {"flag": False}, {"flag": None}]
        self._write_parquet(
            tmp_path,
            rows,
            [pa.field("flag", pa.bool_())],
        )

        spark_digest, spark_count = self._run_spark_digest(tmp_path, spec)
        oracle_digest = oracle.compute_full_digest(rows, spec)

        assert spark_count == 3
        assert spark_digest == oracle_digest, (
            f"Spark {spark_digest} ≠ oracle {oracle_digest}"
        )

    # ── FLOAT64 特殊值 ──

    def test_float_special_values(self, tmp_path, oracle):
        """Spark FLOAT64 特殊值（NaN/Inf/±0.0）→ full_digest == Python oracle。"""
        import math

        import pyarrow as pa

        from tianshu_datadev.spark.cdp_spec import CreDigestSpec, TypeFamily

        spec = CreDigestSpec(
            output_columns=["val"],
            type_families=[TypeFamily.FLOAT64],
            timezone="UTC",
            decimal_precision=[None],
            decimal_scale=[None],
            float_precision=[None],
        )

        rows = [
            {"val": float("nan")},
            {"val": float("inf")},
            {"val": float("-inf")},
            {"val": -0.0},
            {"val": 0.0},
            {"val": 3.14},
        ]
        self._write_parquet(
            tmp_path,
            rows,
            [pa.field("val", pa.float64())],
        )

        spark_digest, spark_count = self._run_spark_digest(tmp_path, spec)
        oracle_digest = oracle.compute_full_digest(rows, spec)

        assert spark_count == 6
        assert spark_digest == oracle_digest, (
            f"Spark {spark_digest} ≠ oracle {oracle_digest}"
        )

    # ── DuckDB 交叉验证（若 DuckDB 可用） ──

    def test_spark_equals_duckdb(self, tmp_path, oracle):
        """Spark digest == DuckDB digest（若 DuckDB 可用）。

        三引擎一致性：Spark == DuckDB == Python oracle。
        """
        import math

        import pyarrow as pa

        from tianshu_datadev.spark.cdp_spec import CreDigestSpec, TypeFamily

        pytest.importorskip("duckdb")
        import duckdb

        spec = CreDigestSpec(
            output_columns=["id", "name", "val"],
            type_families=[
                TypeFamily.INT64,
                TypeFamily.VARCHAR,
                TypeFamily.FLOAT64,
            ],
            timezone="UTC",
            decimal_precision=[None, None, None],
            decimal_scale=[None, None, None],
            float_precision=[None, None, 3],
        )

        rows = [
            {"id": 1, "name": "alice", "val": 1.2345},
            {"id": 2, "name": "bob", "val": float("nan")},
            {"id": 3, "name": None, "val": float("inf")},
            {"id": None, "name": "dave", "val": -0.0},
        ]

        # Python oracle
        oracle_digest = oracle.compute_full_digest(rows, spec)

        # DuckDB
        from tianshu_datadev.spark.cdp_duckdb_builder import DuckdbCdpBuilder
        from tianshu_datadev.spark.cdp_spec import compute_digest_spec_hash

        spec_hash_hex = compute_digest_spec_hash(spec).hex()
        con = duckdb.connect(":memory:")
        con.execute(
            "CREATE OR REPLACE TABLE _t(id BIGINT, name VARCHAR, val DOUBLE)"
        )
        for r in rows:
            con.execute(
                "INSERT INTO _t VALUES (?, ?, ?)",
                (r["id"], r["name"], r["val"]),
            )
        builder = DuckdbCdpBuilder()
        cdp_query = builder.build_query(
            "SELECT id, name, val FROM _t", spec, spec_hash_hex=spec_hash_hex
        )
        duckdb_digest = con.execute(cdp_query).fetchone()[0]
        assert duckdb_digest == oracle_digest, (
            f"DuckDB {duckdb_digest} ≠ oracle {oracle_digest}"
        )

        # Spark
        self._write_parquet(
            tmp_path,
            rows,
            ["id", "name", "val"],
        )
        spark_digest, spark_count = self._run_spark_digest(tmp_path, spec)
        assert spark_count == 4
        assert spark_digest == oracle_digest, (
            f"Spark {spark_digest} ≠ oracle {oracle_digest}"
        )
        assert spark_digest == duckdb_digest, (
            f"Spark {spark_digest} ≠ DuckDB {duckdb_digest}"
        )
