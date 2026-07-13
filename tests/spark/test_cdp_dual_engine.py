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
