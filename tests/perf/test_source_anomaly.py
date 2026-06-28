"""SOURCE_ANOMALY 模型与审查包测试。

验证：SourceAnomaly 的构造、序列化，以及进入审查包的路径。
Phase 4B 规定统一使用 SOURCE_ANOMALY——禁止 CATALOG_ANOMALY。
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from tianshu_datadev.sql.models import SourceAnomaly, SourceAnomalyType


class TestSourceAnomalyModel:
    """SourceAnomaly 模型基本测试。"""

    def test_source_missing_column_anomaly(self):
        """SOURCE_MISSING_COLUMN 异常——列在 SourceManifest 中声明但快照中不存在。"""
        anomaly = SourceAnomaly(
            anomaly_id=SourceAnomaly.generate_anomaly_id(
                "dwd_fact_daily", "SOURCE_MISSING_COLUMN"
            ),
            anomaly_type=SourceAnomalyType.SOURCE_MISSING_COLUMN,
            table_ref="dwd_fact_daily",
            column_name="new_col",
            description="SourceManifest 声明了列 'new_col'，但快照中不存在",
            detected_at="2026-06-28T10:30:00",
            snapshot_ref="snapshot_20260628",
            expected_value="BIGINT",
            actual_value="(不存在)",
        )

        assert anomaly.anomaly_type == SourceAnomalyType.SOURCE_MISSING_COLUMN
        assert anomaly.table_ref == "dwd_fact_daily"
        assert anomaly.column_name == "new_col"

    def test_source_type_mismatch_anomaly(self):
        """SOURCE_TYPE_MISMATCH 异常——列类型与声明不一致。"""
        anomaly = SourceAnomaly(
            anomaly_id=SourceAnomaly.generate_anomaly_id(
                "dim_user", "SOURCE_TYPE_MISMATCH"
            ),
            anomaly_type=SourceAnomalyType.SOURCE_TYPE_MISMATCH,
            table_ref="dim_user",
            column_name="user_id",
            description="列 'user_id' 声明为 BIGINT，实际为 VARCHAR",
            detected_at="2026-06-28T10:30:00",
            snapshot_ref="snapshot_20260628",
            expected_value="BIGINT",
            actual_value="VARCHAR",
        )

        assert anomaly.anomaly_type == SourceAnomalyType.SOURCE_TYPE_MISMATCH
        assert anomaly.expected_value == "BIGINT"
        assert anomaly.actual_value == "VARCHAR"

    def test_source_null_surprise_anomaly(self):
        """SOURCE_NULL_SURPRISE 异常——声明的非空列发现大量 NULL。"""
        anomaly = SourceAnomaly(
            anomaly_id=SourceAnomaly.generate_anomaly_id(
                "dwd_transaction", "SOURCE_NULL_SURPRISE"
            ),
            anomaly_type=SourceAnomalyType.SOURCE_NULL_SURPRISE,
            table_ref="dwd_transaction",
            column_name="amount",
            description="列 'amount' 声明为 NOT NULL，但发现 15.3% 的 NULL 值",
            detected_at="2026-06-28T10:30:00",
            snapshot_ref="snapshot_20260628",
            expected_value="DECIMAL(18,2) NOT NULL",
            actual_value="14.7% NULL",
        )

        assert anomaly.anomaly_type == SourceAnomalyType.SOURCE_NULL_SURPRISE

    def test_source_partition_gap_anomaly(self):
        """SOURCE_PARTITION_GAP 异常——快照分区不连续。"""
        anomaly = SourceAnomaly(
            anomaly_id=SourceAnomaly.generate_anomaly_id(
                "dwd_fact_daily", "SOURCE_PARTITION_GAP"
            ),
            anomaly_type=SourceAnomalyType.SOURCE_PARTITION_GAP,
            table_ref="dwd_fact_daily",
            column_name=None,  # 表级异常
            description="快照分区不连续——缺少 dt=2026-06-15 的数据",
            detected_at="2026-06-28T10:30:00",
            snapshot_ref="snapshot_20260628",
            expected_value="2026-06-01 ~ 2026-06-28 连续",
            actual_value="缺少 2026-06-15",
        )

        assert anomaly.anomaly_type == SourceAnomalyType.SOURCE_PARTITION_GAP
        assert anomaly.column_name is None  # 表级异常

    def test_source_anomaly_generate_id_deterministic(self):
        """generate_anomaly_id 是确定性的——相同输入 → 相同输出。"""
        id1 = SourceAnomaly.generate_anomaly_id("dim_user", "SOURCE_TYPE_MISMATCH")
        id2 = SourceAnomaly.generate_anomaly_id("dim_user", "SOURCE_TYPE_MISMATCH")
        assert id1 == id2
        assert id1.startswith("anomaly_")

    def test_source_anomaly_different_inputs_different_ids(self):
        """不同输入产生不同 anomaly_id。"""
        id1 = SourceAnomaly.generate_anomaly_id("t1", "SOURCE_MISSING_COLUMN")
        id2 = SourceAnomaly.generate_anomaly_id("t2", "SOURCE_MISSING_COLUMN")
        id3 = SourceAnomaly.generate_anomaly_id("t1", "SOURCE_TYPE_MISMATCH")
        assert id1 != id2
        assert id1 != id3

    def test_source_anomaly_serializable(self):
        """SourceAnomaly 可序列化——model_dump 包含全部字段。"""
        anomaly = SourceAnomaly(
            anomaly_id="anomaly_test001",
            anomaly_type=SourceAnomalyType.SOURCE_ROW_COUNT_DRIFT,
            table_ref="dwd_fact_daily",
            description="实际行数 12M，SourceManifest 预估 5M——偏差 140%",
            detected_at="2026-06-28T10:30:00",
            expected_value="5,000,000",
            actual_value="12,000,000",
        )
        data = anomaly.model_dump()
        assert data["anomaly_id"] == "anomaly_test001"
        assert data["anomaly_type"] == "SOURCE_ROW_COUNT_DRIFT"
        assert data["table_ref"] == "dwd_fact_daily"

    def test_source_anomaly_rejects_extra_fields(self):
        """SourceAnomaly 拒绝额外字段（StrictModel + extra=forbid）。"""
        with pytest.raises(ValidationError):
            SourceAnomaly(
                anomaly_id="test",
                anomaly_type=SourceAnomalyType.SOURCE_MISSING_COLUMN,
                table_ref="t1",
                description="test",
                catalog_anomaly=True,  # 禁止 CATALOG_ANOMALY 标记
            )

    def test_all_anomaly_types_have_source_prefix(self):
        """所有 SourceAnomalyType 值都以 SOURCE_ 开头（符合 Phase 4B 规范）。"""
        for anomaly_type in SourceAnomalyType:
            assert anomaly_type.value.startswith("SOURCE_"), (
                f"{anomaly_type.value} 不以 SOURCE_ 开头——"
                f"Phase 4B 规定统一使用 SOURCE_ANOMALY，禁止 CATALOG_ANOMALY"
            )
