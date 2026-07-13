"""CRE shadow 管线级集成测试——验证 Pipeline → PhysicalVerifier → CRE 参数传递。

覆盖要求 5：
- EnvironmentManifest timezone 和特殊值策略真实传入
- NaN/±Inf/singleton/无键多行覆盖
- 严格 Schema（CreShadowReport extra="forbid"）拒绝额外字段
- Audit artifact 写入验证

测试分为两组：
- 直接调用 _shadow_cre_diagnose：测试 NaN/Inf/EnvManifest/singleton 逻辑
- 调用 verify()（真实 DuckDB + mock Spark）：测试完整管线参数传递
"""

from __future__ import annotations

import os
import tempfile

import pytest

from tianshu_datadev.cre_models import (
    CreHarnessAggregation,
    CreShadowReport,
    CreShadowStatus,
    DecimalStrategy,
    EnvironmentManifest,
    NullStrategy,
    SpecialFloatStrategy,
)
from tianshu_datadev.spark.executor import (
    SparkExecutionResult,
    SparkExecutionStatus,
)
from tianshu_datadev.spark.physical_verifier import (
    NormalizationColumn,
    NormalizationConfig,
    PhysicalVerificationStatus,
    PhysicalVerifier,
)

# ════════════════════════════════════════════
# Mock Spark 执行器
# ════════════════════════════════════════════


class _MockSparkExecutor:
    """可配置的 Mock Spark 执行器——返回预设行数据。"""

    def __init__(self, rows: list[dict] | None = None, success: bool = True):
        self._rows = rows or []
        self._success = success

    def execute(self, pyspark_code: str, data_dir: str, output_var: str = "result_df"):
        if not self._success:
            return SparkExecutionResult(
                status=SparkExecutionStatus.RUNTIME_ERROR,
                error_message="Mock Spark 执行失败",
                execution_time_ms=10,
            )
        return SparkExecutionResult(
            status=SparkExecutionStatus.SUCCESS,
            output_rows=self._rows,
            execution_time_ms=10,
        )


# ════════════════════════════════════════════
# 辅助：创建自定义 Parquet 文件
# ════════════════════════════════════════════


def _make_parquet_dir(columns: dict, filename: str = "test_data.parquet") -> str:
    """用给定列创建含单个 Parquet 文件的临时目录。"""
    import pyarrow as pa
    import pyarrow.parquet as pq

    tmpdir = tempfile.mkdtemp(prefix="tianshu_pipeline_")
    table = pa.table(columns)
    pq.write_table(table, os.path.join(tmpdir, filename))
    return tmpdir


# ════════════════════════════════════════════
# 组 A：直接调用 _shadow_cre_diagnose（无 DuckDB）
# ════════════════════════════════════════════


class TestCreShadowDiagnoseDirect:
    """直接测试 _shadow_cre_diagnose 参数传递和逻辑——不依赖 DuckDB。"""

    _SAMPLE_COLS = [
        NormalizationColumn(column_name="id", data_type="bigint"),
        NormalizationColumn(column_name="val", data_type="double"),
    ]

    def test_env_manifest_nan_equal_passed(self):
        """EnvironmentManifest NaN=EQUAL 传入——双侧 NaN 判定一致。"""
        config = NormalizationConfig(
            output_columns=self._SAMPLE_COLS, primary_keys=["id"],
        )
        env = EnvironmentManifest(
            nan_handling=SpecialFloatStrategy.EQUAL,
            pos_inf_handling=SpecialFloatStrategy.MISMATCH,
            neg_inf_handling=SpecialFloatStrategy.HUMAN_REVIEW,
        )
        report = PhysicalVerifier._shadow_cre_diagnose(
            duckdb_raw=[{"id": 1, "val": float("nan")}],
            spark_raw=[{"id": 1, "val": float("nan")}],
            norm_config=config,
            legacy_status="RESULT_CONSISTENT",
            contract_hash="test",
            snapshot_id="snap",
            primary_keys=["id"],
            environment_manifest=env,
        )
        assert report.diagnostic_available is True
        assert report.cre_status in ("CONSISTENT", "CONSISTENT_WITH_WARN")

    def test_env_manifest_nan_default_human_review(self):
        """不传 EnvironmentManifest 时 NaN vs 非 NaN 差异→HUMAN_REVIEW。"""
        config = NormalizationConfig(
            output_columns=self._SAMPLE_COLS, primary_keys=["id"],
        )
        # 一侧 NaN、一侧非 NaN——差异触发 HUMAN_REVIEW
        report = PhysicalVerifier._shadow_cre_diagnose(
            duckdb_raw=[{"id": 1, "val": float("nan")}],
            spark_raw=[{"id": 1, "val": 10.5}],
            norm_config=config,
            legacy_status="RESULT_CONSISTENT",
            contract_hash="test",
            snapshot_id="snap",
            primary_keys=["id"],
            # 不传 environment_manifest——NaN vs 非 NaN 差异触发 HUMAN_REVIEW
        )
        assert report.diagnostic_available is True
        # NaN vs 非 NaN 差异 → HUMAN_REVIEW（默认保守策略）
        assert report.cre_status in ("HUMAN_REVIEW", "MISMATCH"), (
            f"NaN vs 非 NaN 应为 HUMAN_REVIEW/MISMATCH，实际 {report.cre_status}"
        )

    def test_env_manifest_inf_handling(self):
        """±Inf 策略传入验证。"""
        config = NormalizationConfig(
            output_columns=self._SAMPLE_COLS, primary_keys=["id"],
        )
        env = EnvironmentManifest(
            nan_handling=SpecialFloatStrategy.HUMAN_REVIEW,
            pos_inf_handling=SpecialFloatStrategy.EQUAL,
            neg_inf_handling=SpecialFloatStrategy.MISMATCH,
        )
        report = PhysicalVerifier._shadow_cre_diagnose(
            duckdb_raw=[{"id": 1, "val": float("inf")}],
            spark_raw=[{"id": 1, "val": float("inf")}],
            norm_config=config,
            legacy_status="RESULT_CONSISTENT",
            contract_hash="test",
            snapshot_id="snap",
            primary_keys=["id"],
            environment_manifest=env,
        )
        assert report.diagnostic_available is True

    def test_env_manifest_timezone_passed(self):
        """timezone 参数传入——contract_hash/snapshot_id 审计追溯。"""
        config = NormalizationConfig(
            output_columns=self._SAMPLE_COLS, primary_keys=["id"],
        )
        env = EnvironmentManifest(timezone="Asia/Shanghai")
        report = PhysicalVerifier._shadow_cre_diagnose(
            duckdb_raw=[{"id": 1, "val": 10.5}],
            spark_raw=[{"id": 1, "val": 10.5}],
            norm_config=config,
            legacy_status="RESULT_CONSISTENT",
            contract_hash="test_tz_hash",
            snapshot_id="snap_tz_001",
            primary_keys=["id"],
            timezone="Asia/Shanghai",
            environment_manifest=env,
        )
        assert report.diagnostic_available is True
        assert report.contract_hash == "test_tz_hash"
        assert report.snapshot_id == "snap_tz_001"

    def test_singleton_no_pk_allowed(self):
        """无主键 + 双侧 1 行→singleton 对齐允许。"""
        config = NormalizationConfig(
            output_columns=self._SAMPLE_COLS, primary_keys=[],
        )
        report = PhysicalVerifier._shadow_cre_diagnose(
            duckdb_raw=[{"id": 1, "val": 10.5}],
            spark_raw=[{"id": 1, "val": 10.5}],
            norm_config=config,
            legacy_status="RESULT_CONSISTENT",
            contract_hash="test",
            snapshot_id="snap",
            primary_keys=None,
        )
        assert report.diagnostic_available is True
        assert report.cre_status in ("CONSISTENT", "CONSISTENT_WITH_WARN")

    def test_no_pk_multi_row_not_executed(self):
        """无主键 + 多行→NOT_EXECUTED + HUMAN_REVIEW 建议。"""
        config = NormalizationConfig(
            output_columns=self._SAMPLE_COLS, primary_keys=[],
        )
        report = PhysicalVerifier._shadow_cre_diagnose(
            duckdb_raw=[{"id": 1, "val": 10.5}, {"id": 2, "val": 20.3}],
            spark_raw=[{"id": 1, "val": 10.5}, {"id": 2, "val": 20.3}],
            norm_config=config,
            legacy_status="RESULT_CONSISTENT",
            contract_hash="test",
            snapshot_id="snap",
            primary_keys=None,
        )
        assert report.cre_status == "NOT_EXECUTED"
        assert report.diagnostic_available is False
        assert report.human_review_recommended is True

    def test_no_pk_asymmetric_rows_not_executed(self):
        """DuckDB 多行 + Spark 1 行→NOT_EXECUTED（不满足 singleton）。"""
        config = NormalizationConfig(
            output_columns=self._SAMPLE_COLS, primary_keys=[],
        )
        report = PhysicalVerifier._shadow_cre_diagnose(
            duckdb_raw=[{"id": 1, "val": 10.5}, {"id": 2, "val": 20.3}],
            spark_raw=[{"id": 1, "val": 10.5}],
            norm_config=config,
            legacy_status="RESULT_CONSISTENT",
            contract_hash="test",
            snapshot_id="snap",
            primary_keys=None,
        )
        assert report.cre_status == "NOT_EXECUTED"
        assert report.diagnostic_available is False

    # ── 严格 Schema 测试 ──

    def test_strict_schema_valid(self):
        """CreShadowReport 合法构造。"""
        report = CreShadowReport(
            diagnostic_available=True,
            contract_hash="test",
            snapshot_id="snap",
            cre_status="CONSISTENT",
            mapped_status="RESULT_CONSISTENT",
            legacy_status="RESULT_CONSISTENT",
            status_consistent=True,
            human_review_recommended=False,
        )
        assert report.cre_status == "CONSISTENT"

    def test_strict_schema_rejects_extra(self):
        """CreShadowReport extra="forbid"——拒绝未定义字段。"""
        with pytest.raises(Exception):
            CreShadowReport(
                diagnostic_available=True,
                contract_hash="test",
                snapshot_id="snap",
                cre_status="CONSISTENT",
                mapped_status="RESULT_CONSISTENT",
                legacy_status="RESULT_CONSISTENT",
                status_consistent=True,
                human_review_recommended=False,
                extra_unknown_field="should_reject",
            )

    def test_environment_manifest_defaults(self):
        """EnvironmentManifest 默认值——UNKNOWN（强制调用方显式声明）。"""
        manifest = EnvironmentManifest()
        # Req 1: 所有策略默认值为 UNKNOWN——禁止依赖默认猜测
        assert manifest.nan_handling == SpecialFloatStrategy.UNKNOWN
        assert manifest.pos_inf_handling == SpecialFloatStrategy.UNKNOWN
        assert manifest.neg_inf_handling == SpecialFloatStrategy.UNKNOWN
        assert manifest.decimal_strategy == DecimalStrategy.UNKNOWN
        assert manifest.null_strategy == NullStrategy.UNKNOWN


# ════════════════════════════════════════════
# 组 B：完整 verify() 调用（DuckDB + mock Spark）
# ════════════════════════════════════════════


class TestCreShadowVerifyPipeline:
    """通过 verify() 测试完整 Pipeline → CRE 参数传递链路。"""

    def test_verify_passes_env_manifest_to_cre(self):
        """EnvironmentManifest 经 verify() 传入 CRE shadow。"""
        tmpdir = _make_parquet_dir({
            "order_id": ["1", "2"],
            "amount": [100.0, 200.0],
            "region": ["east", "west"],
        }, filename="order_info.parquet")

        try:
            rows = [
                {"order_id": "1", "amount": 100.0, "region": "east"},
                {"order_id": "2", "amount": 200.0, "region": "west"},
            ]
            mock_spark = _MockSparkExecutor(rows=rows)
            config = NormalizationConfig(
                output_columns=[
                    NormalizationColumn(column_name="order_id", data_type="varchar"),
                    NormalizationColumn(column_name="amount", data_type="double"),
                    NormalizationColumn(column_name="region", data_type="varchar"),
                ],
                primary_keys=["order_id"],
            )
            verifier = PhysicalVerifier(
                spark_executor=mock_spark,
                normalization_config=config,
            )
            env_manifest = EnvironmentManifest(
                timezone="Asia/Shanghai",
                nan_handling=SpecialFloatStrategy.EQUAL,
            )
            report = verifier.verify(
                sql_query='SELECT * FROM "order_info" ORDER BY "order_id"',
                pyspark_code='result_df = input_df.orderBy("order_id")',
                snapshot_dir=tmpdir,
                contract_hash="test_env_verify",
                snapshot_id="snap_env_001",
                order_keys=["order_id"],
                cre_primary_keys=["order_id"],
                cre_timezone="Asia/Shanghai",
                cre_environment_manifest=env_manifest,
            )
            assert report.cre_shadow_report is not None
            assert report.cre_shadow_report.diagnostic_available is True
            assert report.cre_shadow_report.contract_hash == "test_env_verify"
            assert report.cre_shadow_report.snapshot_id == "snap_env_001"
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_verify_singleton_alignment(self):
        """无主键 + 双侧 1 行→verify() 中 singleton 对齐。"""
        tmpdir = _make_parquet_dir({
            "order_id": ["1"],
            "amount": [100.0],
            "region": ["east"],
        }, filename="order_info.parquet")

        try:
            rows = [{"order_id": "1", "amount": 100.0, "region": "east"}]
            mock_spark = _MockSparkExecutor(rows=rows)
            config = NormalizationConfig(
                output_columns=[
                    NormalizationColumn(column_name="order_id", data_type="varchar"),
                    NormalizationColumn(column_name="amount", data_type="double"),
                    NormalizationColumn(column_name="region", data_type="varchar"),
                ],
                primary_keys=[],  # 无主键
            )
            verifier = PhysicalVerifier(
                spark_executor=mock_spark,
                normalization_config=config,
            )
            report = verifier.verify(
                sql_query='SELECT * FROM "order_info"',
                pyspark_code="result_df = input_df",
                snapshot_dir=tmpdir,
                contract_hash="test_single_verify",
                snapshot_id="snap_single_001",
                cre_timezone="",
            )
            assert report.cre_shadow_report is not None
            assert report.cre_shadow_report.diagnostic_available is True
            assert report.cre_shadow_report.cre_status in (
                "CONSISTENT", "CONSISTENT_WITH_WARN",
            )
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_verify_no_key_multi_row_not_executed(self):
        """无主键 + 多行→verify() 中 NOT_EXECUTED。"""
        tmpdir = _make_parquet_dir({
            "order_id": ["1", "2"],
            "amount": [100.0, 200.0],
            "region": ["east", "west"],
        }, filename="order_info.parquet")

        try:
            rows = [
                {"order_id": "1", "amount": 100.0, "region": "east"},
                {"order_id": "2", "amount": 200.0, "region": "west"},
            ]
            mock_spark = _MockSparkExecutor(rows=rows)
            config = NormalizationConfig(
                output_columns=[
                    NormalizationColumn(column_name="order_id", data_type="varchar"),
                    NormalizationColumn(column_name="amount", data_type="double"),
                    NormalizationColumn(column_name="region", data_type="varchar"),
                ],
                primary_keys=[],
            )
            verifier = PhysicalVerifier(
                spark_executor=mock_spark,
                normalization_config=config,
            )
            report = verifier.verify(
                sql_query='SELECT * FROM "order_info" ORDER BY "order_id"',
                pyspark_code='result_df = input_df.orderBy("order_id")',
                snapshot_dir=tmpdir,
                contract_hash="test_nokey_verify",
                snapshot_id="snap_nokey_001",
                order_keys=["order_id"],
                cre_timezone="",
            )
            assert report.cre_shadow_report is not None
            assert report.cre_shadow_report.cre_status == "NOT_EXECUTED"
            assert report.cre_shadow_report.diagnostic_available is False
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_verify_audit_artifact_written(self):
        """验证 CRE shadow report 已附加到 PhysicalVerificationReport。

        Req 3: audit artifact 写入已移至 Pipeline 层（_write_cre_to_package_dir），
        verify() 不再直接写入快照目录。此测试验证报告对象本身包含正确数据。
        """
        tmpdir = _make_parquet_dir({
            "order_id": ["1"],
            "amount": [100.0],
            "region": ["east"],
        }, filename="order_info.parquet")

        try:
            rows = [{"order_id": "1", "amount": 100.0, "region": "east"}]
            mock_spark = _MockSparkExecutor(rows=rows)
            config = NormalizationConfig(
                output_columns=[
                    NormalizationColumn(column_name="order_id", data_type="varchar"),
                    NormalizationColumn(column_name="amount", data_type="double"),
                    NormalizationColumn(column_name="region", data_type="varchar"),
                ],
                primary_keys=["order_id"],
            )
            verifier = PhysicalVerifier(
                spark_executor=mock_spark,
                normalization_config=config,
            )
            report = verifier.verify(
                sql_query='SELECT * FROM "order_info" ORDER BY "order_id"',
                pyspark_code='result_df = input_df.orderBy("order_id")',
                snapshot_dir=tmpdir,
                contract_hash="test_audit_verify",
                snapshot_id="snap_audit_001",
                order_keys=["order_id"],
                cre_primary_keys=["order_id"],
            )
            # CRE shadow report 存在且数据完整
            assert report.cre_shadow_report is not None
            assert report.cre_shadow_report.diagnostic_available is True
            assert report.cre_shadow_report.contract_hash == "test_audit_verify"
            assert report.cre_shadow_report.snapshot_id == "snap_audit_001"
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_verify_legacy_status_unchanged(self):
        """CRE shadow 存在时 legacy status 完全不变。"""
        tmpdir = _make_parquet_dir({
            "order_id": ["1", "2"],
            "amount": [100.0, 200.0],
            "region": ["east", "west"],
        }, filename="order_info.parquet")

        try:
            rows = [
                {"order_id": "1", "amount": 100.0, "region": "east"},
                {"order_id": "2", "amount": 200.0, "region": "west"},
            ]
            mock_spark = _MockSparkExecutor(rows=rows)
            config = NormalizationConfig(
                output_columns=[
                    NormalizationColumn(column_name="order_id", data_type="varchar"),
                    NormalizationColumn(column_name="amount", data_type="double"),
                    NormalizationColumn(column_name="region", data_type="varchar"),
                ],
                primary_keys=["order_id"],
            )
            verifier = PhysicalVerifier(
                spark_executor=mock_spark,
                normalization_config=config,
            )
            env_manifest = EnvironmentManifest(
                nan_handling=SpecialFloatStrategy.EQUAL,
            )
            report = verifier.verify(
                sql_query='SELECT * FROM "order_info" ORDER BY "order_id"',
                pyspark_code='result_df = input_df.orderBy("order_id")',
                snapshot_dir=tmpdir,
                contract_hash="test_legacy_verify",
                snapshot_id="snap_legacy_001",
                order_keys=["order_id"],
                cre_primary_keys=["order_id"],
                cre_environment_manifest=env_manifest,
            )
            # legacy status 按自身逻辑判定——数据一致
            assert report.status == PhysicalVerificationStatus.RESULT_CONSISTENT
            # CRE shadow 存在且不改变 legacy
            assert report.cre_shadow_report is not None
            assert report.cre_shadow_report.legacy_status == "RESULT_CONSISTENT"
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)


# ════════════════════════════════════════════
# 组 C：Req 7 新增测试——UNKNOWN 策略 + singleton 统一判定
# ════════════════════════════════════════════


class TestCreShadowUnknownManifest:
    """EnvironmentManifest 全部 UNKNOWN 策略行为测试（Req 1）。"""

    _SAMPLE_COLS = [
        NormalizationColumn(column_name="id", data_type="bigint"),
        NormalizationColumn(column_name="val", data_type="double"),
    ]

    def test_all_unknown_nan_triggers_human_review(self):
        """全部策略 UNKNOWN——NaN 差异触发 HUMAN_REVIEW（保守回退）。"""
        config = NormalizationConfig(
            output_columns=self._SAMPLE_COLS, primary_keys=["id"],
        )
        manifest = EnvironmentManifest(
            nan_handling=SpecialFloatStrategy.UNKNOWN,
            pos_inf_handling=SpecialFloatStrategy.UNKNOWN,
            neg_inf_handling=SpecialFloatStrategy.UNKNOWN,
        )
        report = PhysicalVerifier._shadow_cre_diagnose(
            duckdb_raw=[{"id": 1, "val": float("nan")}],
            spark_raw=[{"id": 1, "val": 10.5}],
            norm_config=config,
            legacy_status="RESULT_CONSISTENT",
            contract_hash="test",
            snapshot_id="snap",
            primary_keys=["id"],
            environment_manifest=manifest,
        )
        assert report.diagnostic_available is True
        # UNKNOWN 策略 → NaN vs 非 NaN → HUMAN_REVIEW
        assert report.cre_status in (
            CreShadowStatus.HUMAN_REVIEW, CreShadowStatus.MISMATCH,
        ), f"UNKNOWN NaN 策略应为 HUMAN_REVIEW/MISMATCH，实际 {report.cre_status}"

    def test_all_unknown_inf_triggers_human_review(self):
        """全部策略 UNKNOWN——±Inf 差异触发 HUMAN_REVIEW。"""
        config = NormalizationConfig(
            output_columns=self._SAMPLE_COLS, primary_keys=["id"],
        )
        manifest = EnvironmentManifest(
            nan_handling=SpecialFloatStrategy.UNKNOWN,
            pos_inf_handling=SpecialFloatStrategy.UNKNOWN,
            neg_inf_handling=SpecialFloatStrategy.UNKNOWN,
        )
        report = PhysicalVerifier._shadow_cre_diagnose(
            duckdb_raw=[{"id": 1, "val": float("inf")}],
            spark_raw=[{"id": 1, "val": 10.5}],
            norm_config=config,
            legacy_status="RESULT_CONSISTENT",
            contract_hash="test",
            snapshot_id="snap",
            primary_keys=["id"],
            environment_manifest=manifest,
        )
        assert report.diagnostic_available is True
        # UNKNOWN +Inf 策略 → HUMAN_REVIEW
        assert report.cre_status in (
            CreShadowStatus.HUMAN_REVIEW, CreShadowStatus.MISMATCH,
        ), f"UNKNOWN Inf 策略应为 HUMAN_REVIEW/MISMATCH，实际 {report.cre_status}"

    def test_no_manifest_default_unknown(self):
        """不传 manifest——EnvironmentManifest 默认全 UNKNOWN——NaN→HUMAN_REVIEW。"""
        config = NormalizationConfig(
            output_columns=self._SAMPLE_COLS, primary_keys=["id"],
        )
        # 不传 environment_manifest——默认全 UNKNOWN 策略
        report = PhysicalVerifier._shadow_cre_diagnose(
            duckdb_raw=[{"id": 1, "val": float("nan")}],
            spark_raw=[{"id": 1, "val": 10.5}],
            norm_config=config,
            legacy_status="RESULT_CONSISTENT",
            contract_hash="test",
            snapshot_id="snap",
            primary_keys=["id"],
        )
        assert report.diagnostic_available is True
        # NaN vs 非 NaN → HUMAN_REVIEW（默认 UNKNOWN → 保守回退）
        assert report.cre_status in (
            CreShadowStatus.HUMAN_REVIEW, CreShadowStatus.MISMATCH,
        )


class TestCreShadowSingletonDecision:
    """Singleton 对齐统一走 DecisionEngine 测试（Req 6）。"""

    _SAMPLE_COLS = [
        NormalizationColumn(column_name="id", data_type="bigint"),
        NormalizationColumn(column_name="val", data_type="double"),
    ]

    def test_singleton_exact_via_decision_engine(self):
        """Singleton 1 行 exact match——经 DecisionEngine → CONSISTENT。"""
        config = NormalizationConfig(
            output_columns=self._SAMPLE_COLS, primary_keys=[],
        )
        report = PhysicalVerifier._shadow_cre_diagnose(
            duckdb_raw=[{"id": 1, "val": 10.5}],
            spark_raw=[{"id": 1, "val": 10.5}],
            norm_config=config,
            legacy_status="RESULT_CONSISTENT",
            contract_hash="test",
            snapshot_id="snap",
            primary_keys=None,
        )
        assert report.diagnostic_available is True
        assert report.cre_status in (CreShadowStatus.CONSISTENT, CreShadowStatus.CONSISTENT_WITH_WARN)
        # DecisionEngine 的 decision_reason 被保留（非手写）
        assert len(report.decision_reason) > 0

    def test_singleton_mismatch_via_decision_engine(self):
        """Singleton 1 行值不同——经 DecisionEngine → MISMATCH。"""
        config = NormalizationConfig(
            output_columns=self._SAMPLE_COLS, primary_keys=[],
        )
        report = PhysicalVerifier._shadow_cre_diagnose(
            duckdb_raw=[{"id": 1, "val": 10.5}],
            spark_raw=[{"id": 1, "val": 99.9}],
            norm_config=config,
            legacy_status="RESULT_CONSISTENT",
            contract_hash="test",
            snapshot_id="snap",
            primary_keys=None,
        )
        assert report.diagnostic_available is True
        assert report.cre_status in (
            CreShadowStatus.MISMATCH, CreShadowStatus.HUMAN_REVIEW,
        )

    def test_singleton_no_pk_not_executed_multi_row(self):
        """无主键多行→NOT_EXECUTED——不满足 singleton 条件。"""
        config = NormalizationConfig(
            output_columns=self._SAMPLE_COLS, primary_keys=[],
        )
        report = PhysicalVerifier._shadow_cre_diagnose(
            duckdb_raw=[{"id": 1, "val": 10.5}, {"id": 2, "val": 20.3}],
            spark_raw=[{"id": 1, "val": 10.5}, {"id": 2, "val": 20.3}],
            norm_config=config,
            legacy_status="RESULT_CONSISTENT",
            contract_hash="test",
            snapshot_id="snap",
            primary_keys=None,
        )
        assert report.cre_status == CreShadowStatus.NOT_EXECUTED
        assert report.diagnostic_available is False


# ════════════════════════════════════════════
# CRE Harness 聚合器——跨请求指标聚合测试（Point 4）
# ════════════════════════════════════════════


class TestCreHarnessAggregation:
    """CRE Harness 跨请求聚合器——golden 标签驱动、零容忍准入标准验证。

    准入标准（全部必须满足）：
    - 至少一个已知差异 golden 样本（total_known_differences > 0）
    - 可执行样本状态一致率 = 100%（CRE ↔ legacy 一致）
    - 零假阴性率 = 0%（golden MISMATCH 全部由 CRE 检出）
    - CRE/legacy 冲突 = 0（mapped=CONSISTENT 但 legacy=MISMATCH）
    - NOT_EXECUTED 单独统计，不稀释一致率
    - WARN 率仅诊断，不作为门槛
    - 所有指标由 aggregate() 内部计算，禁止外部手工赋值
    """

    @staticmethod
    def _make_ahs(
        contract_hash: str = "hash_001",
        scenario_id: str = "scenario_a",
        cre_status: CreShadowStatus = CreShadowStatus.CONSISTENT,
        legacy_status: str = "RESULT_CONSISTENT",
        status_consistent: bool = True,
        diagnostic_available: bool = True,
        total_rows: int = 100,
        has_warnings: bool = False,
        decision_reason: str = "Exact match",
        *,
        is_golden: bool = False,
        golden_label: CreShadowStatus | None = None,
    ):
        """辅助构造 CreAhsMetrics 样本。"""
        from tianshu_datadev.cre_models import CreAhsMetrics
        return CreAhsMetrics(
            contract_hash=contract_hash,
            scenario_id=scenario_id,
            cre_status=cre_status,
            legacy_status=legacy_status,
            status_consistent=status_consistent,
            diagnostic_available=diagnostic_available,
            total_rows=total_rows,
            exact_match_rows=total_rows if cre_status == CreShadowStatus.CONSISTENT else 0,
            has_warnings=has_warnings,
            decision_reason=decision_reason,
            is_golden=is_golden,
            golden_label=golden_label,
        )

    def test_all_consistent_passes_admission(self):
        """全部样本一致 + golden MISMATCH 正确检出 → 通过准入。"""

        samples = [
            self._make_ahs("h1", "s1", CreShadowStatus.CONSISTENT),
            self._make_ahs("h2", "s2", CreShadowStatus.CONSISTENT),
            self._make_ahs("h3", "s3", CreShadowStatus.CONSISTENT_WITH_WARN, has_warnings=True),
            # 至少一个 golden 已知差异样本供 Harness 验证判别能力
            self._make_ahs("h4", "s4", CreShadowStatus.MISMATCH,
                           legacy_status="RESULT_MISMATCH",
                           is_golden=True, golden_label=CreShadowStatus.MISMATCH),
        ]
        agg = CreHarnessAggregation()
        agg.aggregate(samples)

        assert agg.total_samples == 4
        assert agg.executable_total == 4
        assert agg.not_executed_count == 0
        assert agg.executable_consistency_rate == 1.0
        assert agg.warn_count == 1
        assert agg.warn_rate == 1.0 / 4.0
        assert agg.total_known_differences == 1  # 1 个 golden MISMATCH
        assert agg.false_negative_count == 0      # 已正确检出
        assert agg.passes_admission is True

    def test_one_mismatch_fails_consistency(self):
        """一个 MISMATCH → 一致率 < 100% → 准入失败。"""

        samples = [
            self._make_ahs("h1", "s1", CreShadowStatus.CONSISTENT),
            self._make_ahs("h2", "s2", CreShadowStatus.MISMATCH,
                           legacy_status="RESULT_CONSISTENT", status_consistent=False),
            self._make_ahs("h3", "s3", CreShadowStatus.CONSISTENT),
            # golden 已知差异样本
            self._make_ahs("h4", "s4", CreShadowStatus.MISMATCH,
                           legacy_status="RESULT_MISMATCH",
                           is_golden=True, golden_label=CreShadowStatus.MISMATCH),
        ]
        agg = CreHarnessAggregation()
        agg.aggregate(samples)

        assert agg.executable_total == 4
        assert agg.executable_consistent_count == 3
        assert agg.executable_consistency_rate == 3.0 / 4.0
        assert agg.passes_admission is False  # 一致率 < 100%

    def test_false_negative_blocks_admission(self):
        """golden MISMATCH 被 CRE 判 CONSISTENT（假阴性）→ 准入失败。"""

        samples = [
            self._make_ahs("h1", "s1", CreShadowStatus.CONSISTENT),
            # golden 已知差异——但 CRE 误判为 CONSISTENT（假阴性！）
            self._make_ahs("h2", "s2", CreShadowStatus.CONSISTENT,
                           legacy_status="RESULT_CONSISTENT",
                           is_golden=True, golden_label=CreShadowStatus.MISMATCH),
        ]
        agg = CreHarnessAggregation()
        agg.aggregate(samples)

        assert agg.total_known_differences == 1     # 有 1 个已知差异 golden 样本
        assert agg.false_negative_count == 1          # CRE 判了 CONSISTENT（假阴性）
        assert agg.false_negative_rate == 1.0         # 100% 假阴性
        assert agg.passes_admission is False

    def test_zero_false_negative_passes(self):
        """零假阴性（golden MISMATCH 正确检出）→ 准入通过。"""

        samples = [
            self._make_ahs("h1", "s1", CreShadowStatus.CONSISTENT,
                           legacy_status="RESULT_CONSISTENT"),
            # golden 已知差异——CRE 正确检出 MISMATCH
            self._make_ahs("h2", "s2", CreShadowStatus.MISMATCH,
                           legacy_status="RESULT_MISMATCH",
                           is_golden=True, golden_label=CreShadowStatus.MISMATCH),
        ]
        agg = CreHarnessAggregation()
        agg.aggregate(samples)

        assert agg.total_known_differences == 1      # 1 个 golden MISMATCH
        assert agg.false_negative_count == 0          # 无假阴性
        assert agg.false_negative_rate == 0.0
        assert agg.executable_consistency_rate == 1.0  # 两个样本 CRE↔legacy 都一致
        assert agg.passes_admission is True

    def test_cre_legacy_conflict_blocks(self):
        """mapped=CONSISTENT 但 legacy=MISMATCH（冲突）→ 准入失败。"""

        samples = [
            # CRE=CONSISTENT 映射为 RESULT_CONSISTENT，但 legacy=RESULT_MISMATCH → 冲突！
            self._make_ahs("h1", "s1", CreShadowStatus.CONSISTENT,
                           legacy_status="RESULT_MISMATCH", status_consistent=False),
            # golden 已知差异样本（正确检出）
            self._make_ahs("h2", "s2", CreShadowStatus.MISMATCH,
                           legacy_status="RESULT_MISMATCH",
                           is_golden=True, golden_label=CreShadowStatus.MISMATCH),
        ]
        agg = CreHarnessAggregation()
        agg.aggregate(samples)

        assert agg.cre_legacy_conflict_count == 1
        assert agg.passes_admission is False  # 冲突 + 一致率 < 100%

    def test_cre_legacy_conflict_computed_internally(self):
        """CRE/legacy 冲突完全由 aggregate() 内部计算——外部无法赋值绕过。"""

        # 构造有冲突的样本
        samples = [
            self._make_ahs("h1", "s1", CreShadowStatus.CONSISTENT,
                           legacy_status="RESULT_MISMATCH", status_consistent=False),
            self._make_ahs("h2", "s2", CreShadowStatus.MISMATCH,
                           legacy_status="RESULT_MISMATCH",
                           is_golden=True, golden_label=CreShadowStatus.MISMATCH),
        ]
        agg = CreHarnessAggregation()
        # 即使外部企图设置 cre_legacy_conflict_count=0，aggregate() 也会重算
        agg.cre_legacy_conflict_count = 0
        agg.aggregate(samples)

        # aggregate() 重算后——冲突数来自实际样本数据，不受外部赋值影响
        assert agg.cre_legacy_conflict_count == 1
        assert agg.passes_admission is False

    def test_not_executed_separate_counting(self):
        """NOT_EXECUTED 单独统计——不稀释可执行样本一致率。"""

        samples = [
            self._make_ahs("h1", "s1", CreShadowStatus.CONSISTENT),
            self._make_ahs("h2", "s2", diagnostic_available=False),  # NOT_EXECUTED
            self._make_ahs("h3", "s3", CreShadowStatus.CONSISTENT),
            self._make_ahs("h4", "s4", diagnostic_available=False),  # NOT_EXECUTED
            # golden 已知差异样本
            self._make_ahs("h5", "s5", CreShadowStatus.MISMATCH,
                           legacy_status="RESULT_MISMATCH",
                           is_golden=True, golden_label=CreShadowStatus.MISMATCH),
        ]
        agg = CreHarnessAggregation()
        agg.aggregate(samples)

        assert agg.total_samples == 5
        assert agg.executable_total == 3        # 只有 3 个可执行
        assert agg.not_executed_count == 2       # 2 个 NOT_EXECUTED
        assert agg.not_executed_ratio == 2.0 / 5.0
        assert agg.executable_consistency_rate == 1.0  # 可执行的 3 个全部一致
        assert agg.passes_admission is True

    def test_all_not_executed(self):
        """全部 NOT_EXECUTED → 无可执行样本 → 准入失败。"""

        samples = [
            self._make_ahs("h1", "s1", diagnostic_available=False),
            self._make_ahs("h2", "s2", diagnostic_available=False),
        ]
        agg = CreHarnessAggregation()
        agg.aggregate(samples)

        assert agg.executable_total == 0
        assert agg.passes_admission is False  # 无可执行样本

    def test_warn_only_diagnostic(self):
        """WARN 率不进入准入判定——仅诊断。"""

        samples = [
            self._make_ahs("h1", "s1", CreShadowStatus.CONSISTENT_WITH_WARN, has_warnings=True),
            self._make_ahs("h2", "s2", CreShadowStatus.CONSISTENT_WITH_WARN, has_warnings=True),
            self._make_ahs("h3", "s3", CreShadowStatus.CONSISTENT),
            # golden 已知差异样本
            self._make_ahs("h4", "s4", CreShadowStatus.MISMATCH,
                           legacy_status="RESULT_MISMATCH",
                           is_golden=True, golden_label=CreShadowStatus.MISMATCH),
        ]
        agg = CreHarnessAggregation()
        agg.aggregate(samples)

        # 高 WARN 率——但仍通过准入（WARN 仅诊断）
        assert agg.warn_rate == 2.0 / 4.0
        assert agg.executable_consistency_rate == 1.0
        assert agg.passes_admission is True

    def test_status_distribution_counts(self):
        """状态分布计数正确——CONSISTENT/MISMATCH/HUMAN_REVIEW/ERROR。"""

        samples = [
            self._make_ahs("h1", "s1", CreShadowStatus.CONSISTENT),
            self._make_ahs("h2", "s2", CreShadowStatus.CONSISTENT_WITH_WARN, has_warnings=True),
            self._make_ahs("h3", "s3", CreShadowStatus.MISMATCH,
                           legacy_status="RESULT_MISMATCH", status_consistent=False),
            self._make_ahs("h4", "s4", CreShadowStatus.HUMAN_REVIEW,
                           legacy_status="RESULT_MISMATCH", status_consistent=False),
            self._make_ahs("h5", "s5", CreShadowStatus.ERROR,
                           diagnostic_available=True, status_consistent=False),
        ]
        agg = CreHarnessAggregation()
        agg.aggregate(samples)

        assert agg.cre_consistent_count == 1
        assert agg.cre_consistent_warn_count == 1
        assert agg.cre_mismatch_count == 1
        assert agg.cre_human_review_count == 1
        assert agg.error_count == 1
        # 零 golden 已知差异 → 无法验证 Harness 判别能力 → 准入失败
        assert agg.total_known_differences == 0
        assert agg.passes_admission is False

    def test_empty_samples(self):
        """零样本边界——aggregate 不崩溃，passes_admission = False。"""

        agg = CreHarnessAggregation()
        agg.aggregate([])

        assert agg.total_samples == 0
        assert agg.executable_total == 0
        assert agg.passes_admission is False

    def test_idempotent_aggregate(self):
        """聚合幂等——连续两次 aggregate 返回相同指标。"""

        samples = [
            self._make_ahs("h1", "s1", CreShadowStatus.CONSISTENT),
            self._make_ahs("h2", "s2", CreShadowStatus.MISMATCH,
                           legacy_status="RESULT_MISMATCH",
                           is_golden=True, golden_label=CreShadowStatus.MISMATCH),
        ]
        agg = CreHarnessAggregation()
        agg.aggregate(samples)
        first = {
            "total": agg.total_samples,
            "executable": agg.executable_total,
            "consistency": agg.executable_consistency_rate,
            "known_diffs": agg.total_known_differences,
            "false_neg": agg.false_negative_count,
            "conflicts": agg.cre_legacy_conflict_count,
            "passes": agg.passes_admission,
        }

        # 第二次 aggregate——即使在上次结果上叠加也不影响（幂等重算）
        agg.aggregate(samples)
        second = {
            "total": agg.total_samples,
            "executable": agg.executable_total,
            "consistency": agg.executable_consistency_rate,
            "known_diffs": agg.total_known_differences,
            "false_neg": agg.false_negative_count,
            "conflicts": agg.cre_legacy_conflict_count,
            "passes": agg.passes_admission,
        }

        assert first == second  # 幂等：两次结果完全一致

    def test_zero_known_diffs_no_admission(self):
        """零已知差异 golden 样本 → 准入失败（无法验证 Harness 判别能力）。"""

        samples = [
            self._make_ahs("h1", "s1", CreShadowStatus.CONSISTENT),
            self._make_ahs("h2", "s2", CreShadowStatus.CONSISTENT),
            # 全都是 golden CONSISTENT 样本——没有 golden MISMATCH 用于验证
            self._make_ahs("h3", "s3", CreShadowStatus.CONSISTENT,
                           is_golden=True, golden_label=CreShadowStatus.CONSISTENT),
        ]
        agg = CreHarnessAggregation()
        agg.aggregate(samples)

        assert agg.executable_total == 3
        assert agg.executable_consistency_rate == 1.0  # 100% 一致
        assert agg.total_known_differences == 0         # 但无 golden MISMATCH
        assert agg.false_negative_count == 0
        assert agg.passes_admission is False             # 无法验证判别能力

    def test_golden_stats_computed(self):
        """Golden 样本统计由 aggregate 内部计算。"""

        samples = [
            self._make_ahs("h1", "s1", CreShadowStatus.CONSISTENT,
                           is_golden=True, golden_label=CreShadowStatus.CONSISTENT),
            self._make_ahs("h2", "s2", CreShadowStatus.MISMATCH,
                           legacy_status="RESULT_MISMATCH",
                           is_golden=True, golden_label=CreShadowStatus.MISMATCH),
            self._make_ahs("h3", "s3", CreShadowStatus.CONSISTENT,
                           is_golden=True, golden_label=CreShadowStatus.MISMATCH),
        ]
        agg = CreHarnessAggregation()
        agg.aggregate(samples)

        assert agg.golden_total == 3
        assert agg.golden_consistent_count == 3  # h1+h2+h3 的 CRE↔legacy 都一致
        assert agg.total_known_differences == 2  # h2 + h3 的 golden_label=MISMATCH
        assert agg.false_negative_count == 1      # h3 是假阴性（golden=MISMATCH 但 CRE=CONSISTENT）
