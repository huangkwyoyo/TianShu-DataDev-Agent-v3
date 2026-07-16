"""CDP v1 Python Oracle vs 手工黄金向量——oracle 编码必须精确匹配硬编码期望值。"""
from __future__ import annotations

import pytest

from tests.spark.test_cdp_golden_vectors import (
    G1_FULL_DIGEST_HEX,
    G2_FIELD_BYTES_HEX,
    G2_FULL_DIGEST_HEX,
    G3_FIELD_BYTES_HEX,
    G4_FALSE_FIELD_BYTES_HEX,
    G4_TRUE_FIELD_BYTES_HEX,
    G5_INF_FIELD_BYTES_HEX,
    G5_NAN_FIELD_BYTES_HEX,
    G5_NEG_INF_FIELD_BYTES_HEX,
    G5_NEG_ZERO_FIELD_BYTES_HEX,
    G5_POS_ZERO_FIELD_BYTES_HEX,
    G6_FULL_DIGEST_HEX,
)
from tianshu_datadev.spark.cdp_serializer import CdpCanonicalSerializer, CdpEncodingError
from tianshu_datadev.spark.cdp_spec import CreDigestSpec, TypeFamily


class TestOracleVsGoldenVectors:
    """Python oracle 必须精确匹配所有手工黄金向量——硬编码常量断言。"""

    @pytest.fixture
    def oracle(self):
        return CdpCanonicalSerializer()

    @staticmethod
    def _int64_spec():
        """G1-G3 共用的 INT64 spec。"""
        return CreDigestSpec(
            output_columns=["id"], type_families=[TypeFamily.INT64],
            timezone="UTC",
            decimal_precision=[None], decimal_scale=[None], float_precision=[None],
        )

    def test_g1_empty_result(self, oracle):
        """oracle 产出必须 == 硬编码 G1_FULL_DIGEST_HEX。"""
        spec = self._int64_spec()
        result = oracle.compute_full_digest([], spec)
        # 与预计算常量比较——禁止运行时重算 expected
        assert result == G1_FULL_DIGEST_HEX, (
            f"oracle {result} ≠ golden {G1_FULL_DIGEST_HEX}"
        )

    def test_g2_single_int64(self, oracle):
        """oracle field_bytes 必须 == 硬编码 G2_FIELD_BYTES_HEX。"""
        spec = self._int64_spec()
        field = oracle.encode_field(42, TypeFamily.INT64, spec, 0)
        assert field == bytes.fromhex(G2_FIELD_BYTES_HEX)

    def test_g3_null_field(self, oracle):
        """oracle NULL 编码必须 == 硬编码 G3_FIELD_BYTES_HEX。"""
        spec = self._int64_spec()
        field = oracle.encode_field(None, TypeFamily.INT64, spec, 0)
        assert field == bytes.fromhex(G3_FIELD_BYTES_HEX)

    def test_g4_boolean(self, oracle):
        """oracle BOOLEAN 编码必须 == 硬编码常量。"""
        spec = CreDigestSpec(
            output_columns=["f"], type_families=[TypeFamily.BOOLEAN],
            timezone="UTC",
            decimal_precision=[None], decimal_scale=[None], float_precision=[None],
        )
        assert oracle.encode_field(True, TypeFamily.BOOLEAN, spec, 0) == bytes.fromhex(G4_TRUE_FIELD_BYTES_HEX)
        assert oracle.encode_field(False, TypeFamily.BOOLEAN, spec, 0) == bytes.fromhex(G4_FALSE_FIELD_BYTES_HEX)

    def test_g5_float_specials(self, oracle):
        """oracle Float 特殊值编码必须 == 硬编码常量。"""
        spec = CreDigestSpec(
            output_columns=["v"], type_families=[TypeFamily.FLOAT64],
            timezone="UTC",
            decimal_precision=[None], decimal_scale=[None], float_precision=[None],
        )
        assert oracle.encode_field(float("nan"), TypeFamily.FLOAT64, spec, 0) == bytes.fromhex(G5_NAN_FIELD_BYTES_HEX)
        assert oracle.encode_field(float("inf"), TypeFamily.FLOAT64, spec, 0) == bytes.fromhex(G5_INF_FIELD_BYTES_HEX)
        assert oracle.encode_field(float("-inf"), TypeFamily.FLOAT64, spec, 0) == bytes.fromhex(G5_NEG_INF_FIELD_BYTES_HEX)
        assert oracle.encode_field(-0.0, TypeFamily.FLOAT64, spec, 0) == bytes.fromhex(G5_NEG_ZERO_FIELD_BYTES_HEX)
        assert oracle.encode_field(0.0, TypeFamily.FLOAT64, spec, 0) == bytes.fromhex(G5_POS_ZERO_FIELD_BYTES_HEX)

    def test_g2_full_digest(self, oracle):
        """oracle full_digest 必须 == 硬编码 G2_FULL_DIGEST_HEX。"""
        spec = self._int64_spec()
        result = oracle.compute_full_digest([{"id": 42}], spec)
        assert result == G2_FULL_DIGEST_HEX, (
            f"oracle {result} ≠ golden {G2_FULL_DIGEST_HEX}"
        )

    def test_g6_duplicate_rows(self, oracle):
        """G6：两行相同值 → 多重集 full_digest ≠ 单行，且必须 == 硬编码 G6_FULL_DIGEST_HEX。"""
        spec = self._int64_spec()
        rows = [{"id": 42}, {"id": 42}]  # 两行相同
        result = oracle.compute_full_digest(rows, spec)

        # 必须匹配 G6 硬编码常量
        assert result == G6_FULL_DIGEST_HEX, (
            f"oracle {result} ≠ golden G6 {G6_FULL_DIGEST_HEX}"
        )
        # 多重集语义：两行相同 ≠ 单行
        assert result != G2_FULL_DIGEST_HEX, (
            "两行相同 digest 不应 == 单行 digest G2"
        )


class TestOracleRejectsNaiveTimestamp:
    """Python oracle 必须拒绝 naive timestamp——与设计文档一致。"""

    @pytest.fixture
    def oracle(self):
        return CdpCanonicalSerializer()

    def test_naive_datetime_raises(self, oracle):
        """无时区的 datetime → CdpEncodingError，不是继续编码。"""
        from datetime import datetime

        spec = CreDigestSpec(
            output_columns=["ts"], type_families=[TypeFamily.TIMESTAMP],
            timezone="Asia/Shanghai",
            decimal_precision=[None], decimal_scale=[None], float_precision=[None],
        )
        naive = datetime(2026, 1, 15, 12, 0, 0)  # 无 tzinfo
        with pytest.raises(CdpEncodingError, match="naive.*timestamp|时区"):
            oracle.encode_field(naive, TypeFamily.TIMESTAMP, spec, 0)


class TestOracleDeterminism:
    """oracle 的确定性和顺序无关性验证。"""

    @pytest.fixture
    def oracle(self):
        return CdpCanonicalSerializer()

    def test_repeated_full_digest_stable(self, oracle):
        """同一数据集 50 次重复计算 full_digest 不变。"""
        spec = CreDigestSpec(
            output_columns=["id", "name"],
            type_families=[TypeFamily.INT64, TypeFamily.VARCHAR],
            timezone="UTC",
            decimal_precision=[None, None], decimal_scale=[None, None],
            float_precision=[None, None],
        )
        rows = [{"id": i, "name": f"row_{i}"} for i in range(100)]
        digests = [oracle.compute_full_digest(rows, spec) for _ in range(50)]
        assert len(set(digests)) == 1

    def test_order_independence(self, oracle):
        """行输入顺序不影响 full_digest。"""
        spec = CreDigestSpec(
            output_columns=["id"], type_families=[TypeFamily.INT64],
            timezone="UTC",
            decimal_precision=[None], decimal_scale=[None], float_precision=[None],
        )
        forward = [{"id": i} for i in range(100)]
        backward = [{"id": i} for i in range(99, -1, -1)]
        assert oracle.compute_full_digest(forward, spec) == oracle.compute_full_digest(backward, spec)
