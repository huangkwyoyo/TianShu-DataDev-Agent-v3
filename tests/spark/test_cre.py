"""CRE v2 原型测试——Canonical Row Encoding + 分桶 + 容差比较 + 判定矩阵。

测试范围（要求 9）：
1. 单行编码正确性（CREEncoder 所有类型）
2. 相同数据 → 相同 digest（引擎无关性）
3. Float 容差边界：1e-12（within）/ 2e-9（out of tolerance）
4. Decimal 量化比较：尾随零等价 / 真实差异
5. KeyBasedRowAligner：对齐 / 缺失键 / 重复键
6. BucketHasher：全匹配 / 单桶不一致精确定位
7. DecisionMatrix：全量 exact match、全量容差内 WARN、单条实际错误→MISMATCH
8. 100% 行 1e-15 尾差 → CONSISTENT_WITH_WARN
9. 95% 一致 + 1 条真实错误 → MISMATCH
10. 缺少 Contract/primary_keys → HUMAN_REVIEW

不接入生产 verify()，不修改 executor.py/pipeline.py/现有状态枚举。
"""

from __future__ import annotations

import hashlib
import math
from decimal import Decimal

import pytest

from tianshu_datadev.spark.cre import (
    BucketHasher,
    BucketResult,
    CreConfig,
    CREEncoder,
    DecisionEngine,
    KeyBasedRowAligner,
    NotToleratedReason,
    ToleranceComparator,
    _normalize_name,
    _type_family,
)
from tianshu_datadev.spark.physical_verifier import NormalizationColumn

# ════════════════════════════════════════════
# 夹具：通用测试数据
# ════════════════════════════════════════════


@pytest.fixture
def contract_cols():
    """通用 Contract output_columns 定义。"""
    return [
        NormalizationColumn(column_name="order_id", data_type="bigint"),
        NormalizationColumn(column_name="amount", data_type="decimal(18,2)"),
        NormalizationColumn(column_name="status", data_type="varchar"),
        NormalizationColumn(column_name="score", data_type="double"),
        NormalizationColumn(column_name="is_active", data_type="boolean"),
    ]


@pytest.fixture
def cre_config(contract_cols):
    """通用 CreConfig——带权威主键。"""
    return CreConfig(
        output_columns=contract_cols,
        primary_keys=["order_id"],
    )


@pytest.fixture
def encoder(cre_config):
    return CREEncoder(cre_config)


@pytest.fixture
def sample_rows():
    """3 行标准测试数据。"""
    return [
        {
            "order_id": 1, "amount": Decimal("100.50"),
            "status": "completed", "score": 95.5, "is_active": True,
        },
        {
            "order_id": 2, "amount": Decimal("200.00"),
            "status": "pending", "score": 87.0, "is_active": False,
        },
        {
            "order_id": 3, "amount": Decimal("150.75"),
            "status": "completed", "score": 92.3, "is_active": True,
        },
    ]


# ════════════════════════════════════════════
# 第 1 节：CREEncoder 编码正确性
# ════════════════════════════════════════════


class TestCREEncoder:
    """CREEncoder 所有类型编码 + 引擎无关性验证。"""

    def test_magic_and_header(self, encoder, sample_rows):
        """编码结果以 CRE2 magic header + column_count 开头。"""
        encoded = encoder.encode_row(sample_rows[0])
        assert encoded[:4] == b"CRE2"
        col_count = int.from_bytes(encoded[4:6], "big")
        assert col_count == 5  # 5 columns in fixture

    def test_total_length(self, encoder, sample_rows):
        """total_length 字段值正确。"""
        encoded = encoder.encode_row(sample_rows[0])
        total_len = int.from_bytes(encoded[6:10], "big")
        assert total_len == len(encoded)

    def test_same_data_same_bytes(self, encoder):
        """相同数据 → 完全相同字节序列（引擎无关性基础）。"""
        row = {"order_id": 42, "amount": Decimal("99.99"), "status": "ok", "score": 88.8, "is_active": True}
        b1 = encoder.encode_row(row)
        b2 = encoder.encode_row(row)
        assert b1 == b2
        assert hashlib.sha256(b1).hexdigest() == hashlib.sha256(b2).hexdigest()

    def test_null_encoding(self, encoder):
        """NULL 值编码为 0x00 type_tag + 0 值字节。"""
        row = {"order_id": 1, "amount": None, "status": None, "score": None, "is_active": None}
        encoded = encoder.encode_row(row)

        # 第一列 order_id=1 → INT64 编码
        assert encoded[10] == 0x05  # first non-header byte
        # 第二列 amount=NULL → 0x00
        order_id_end = 10 + 1 + 8  # type_tag + 8 bytes int64
        assert encoded[order_id_end] == 0x00  # NULL tag for amount

    def test_varchar_empty_string(self, encoder):
        """空字符串与 NULL 区分编码。"""
        row_null = {
            "order_id": 1, "amount": Decimal("10.00"),
            "status": None, "score": 1.0, "is_active": True,
        }
        row_empty = {
            "order_id": 1, "amount": Decimal("10.00"),
            "status": "", "score": 1.0, "is_active": True,
        }
        null_enc = encoder.encode_row(row_null)
        empty_enc = encoder.encode_row(row_empty)
        assert null_enc != empty_enc  # 必须区分

    def test_bool_encoding(self, encoder):
        """布尔值编码。"""
        row_t = {"order_id": 1, "amount": Decimal("1.00"), "status": "x", "score": 1.0, "is_active": True}
        row_f = {"order_id": 1, "amount": Decimal("1.00"), "status": "x", "score": 1.0, "is_active": False}
        enc_t = encoder.encode_row(row_t)
        enc_f = encoder.encode_row(row_f)
        assert enc_t != enc_f

    def test_int64_encoding(self, encoder):
        """INT64 编码稳定。"""
        row = {"order_id": 2**62, "amount": Decimal("1.00"), "status": "x", "score": 1.0, "is_active": True}
        encoded = encoder.encode_row(row)
        # order_id 编码：type_tag(0x05) + 8 bytes
        # 2**62 的高位字节应为 0x40
        assert encoded[10:13] == b"\x05\x40\x00"

    def test_float_encoding_raw_ieee(self, encoder):
        """float/double 保持原生 IEEE 754 编码（不 round 不转网格）。"""
        config_f = CreConfig(
            output_columns=[NormalizationColumn(column_name="val", data_type="double")],
            primary_keys=["val"],
        )
        enc = CREEncoder(config_f)

        row1 = {"val": 1.0 / 3.0}
        row2 = {"val": 1.0 / 3.0}
        # 相同浮点值 → 相同编码
        assert enc.encode_row(row1) == enc.encode_row(row2)

    def test_float_nan_normalization(self, encoder):
        """NaN 的编码统一性。"""
        import struct
        config_f = CreConfig(
            output_columns=[NormalizationColumn(column_name="val", data_type="double")],
            primary_keys=["val"],
        )
        enc = CREEncoder(config_f)

        row = {"val": float("nan")}
        encoded = enc.encode_row(row)
        # 第 5 字节起：type_tag(0x07) + 8 字节 IEEE 754
        nan_bytes = encoded[10:]
        assert len(nan_bytes) == 9  # tag + 8 bytes
        assert nan_bytes[0] == 0x07
        # IEEE 754 double NaN = 0x7FF8000000000000
        assert nan_bytes[1:] == struct.pack(">d", float("nan"))

    def test_order_keys_secondary_sort(self, encoder, sample_rows):
        """pk_digest 基于主键列 value bytes。"""
        digests = [encoder.pk_digest(r) for r in sample_rows]
        assert len(set(digests)) == 3  # 3 个不同的 order_id
        # pk_digest 是确定性的
        assert encoder.pk_digest(sample_rows[0]) == encoder.pk_digest(sample_rows[0])


# ════════════════════════════════════════════
# 第 2 节：Float 容差边界
# ════════════════════════════════════════════


class TestFloatTolerance:
    """float/double 容差边界测试（要求 6/7）。"""

    def test_within_abs_tolerance(self):
        """1.0 vs 1.000000000001 → abs diff≈1e-12 ≤ abs_tol=1e-12 → WITHIN_TOLERANCE。
        注意：1.000000000001 - 1.0 在 float64 中不完全等于 1e-12（表示误差），
        但 math.isclose 仍认为在容差内。"""
        config = CreConfig(
            output_columns=[NormalizationColumn(column_name="val", data_type="double")],
            primary_keys=["val"],
        )
        d_row = {"val": 1.0}
        s_row = {"val": 1.000000000001}
        result = ToleranceComparator.compare_row(d_row, s_row, config, CREEncoder(config))
        assert not result.exact_match
        assert len(result.cell_results) == 1
        assert result.cell_results[0].reason == NotToleratedReason.WITHIN_TOLERANCE
        assert result.cell_results[0].abs_error is not None

    def test_out_of_tolerance(self):
        """1.000000002 vs 1.0: abs diff=2e-9 → isclose 不满足 → OUT_OF_TOLERANCE。"""
        config = CreConfig(
            output_columns=[NormalizationColumn(column_name="val", data_type="double")],
            primary_keys=["val"],
        )
        d_row = {"val": 1.0}
        s_row = {"val": 1.000000002}  # diff=2e-9
        result = ToleranceComparator.compare_row(d_row, s_row, config, CREEncoder(config))
        assert not result.exact_match
        assert len(result.cell_results) == 1
        assert result.cell_results[0].reason == NotToleratedReason.OUT_OF_TOLERANCE

    def test_float_exact_match_skips_tolerance(self):
        """两值相等时 exact_match=True，不进入容差比较。"""
        config = CreConfig(
            output_columns=[NormalizationColumn(column_name="val", data_type="double")],
            primary_keys=["val"],
        )
        d_row = {"val": 3.141592653589793}
        s_row = {"val": 3.141592653589793}
        result = ToleranceComparator.compare_row(d_row, s_row, config, CREEncoder(config))
        assert result.exact_match
        assert len(result.cell_results) == 0

    def test_float_very_small_values(self):
        """极小浮点值在容差内。"""
        config = CreConfig(
            output_columns=[NormalizationColumn(column_name="val", data_type="double")],
            primary_keys=["val"],
        )
        d_row = {"val": 1e-15}
        s_row = {"val": 1.5e-15}  # diff=0.5e-15 < abs_tol=1e-12 → within
        result = ToleranceComparator.compare_row(d_row, s_row, config, CREEncoder(config))
        assert not result.exact_match
        assert result.cell_results[0].reason == NotToleratedReason.WITHIN_TOLERANCE

    def test_float_large_values_rel_tolerance(self):
        """大值用 rel_tol 判定。"""
        config = CreConfig(
            output_columns=[NormalizationColumn(column_name="val", data_type="double")],
            primary_keys=["val"],
        )
        # val=1e12, diff=1e3 → rel diff=1e-9 == rel_tol → within
        d_row = {"val": 1e12}
        s_row = {"val": 1e12 + 1000.0}
        result = ToleranceComparator.compare_row(d_row, s_row, config, CREEncoder(config))
        assert not result.exact_match
        # rel diff = 1000/1e12 = 1e-9 → exactly at boundary, isclose returns True
        assert result.cell_results[0].reason == NotToleratedReason.WITHIN_TOLERANCE


# ════════════════════════════════════════════
# 第 3 节：Decimal 量化比较
# ════════════════════════════════════════════


class TestDecimalComparison:
    """Decimal 按 Contract precision/scale 量化后精确比较。"""

    def test_trailing_zero_equivalent(self):
        """Decimal(18,2)：1.20 vs 1.2 → 编码层量化后等价的 exact_match。
        （CRE 编码器已将 1.2 量化为 1.20 scale=2，相同编码→exact_match）"""
        config = CreConfig(
            output_columns=[NormalizationColumn(column_name="val", data_type="decimal(18,2)")],
            primary_keys=["val"],
        )
        d_row = {"val": Decimal("1.20")}
        s_row = {"val": Decimal("1.2")}
        result = ToleranceComparator.compare_row(d_row, s_row, config, CREEncoder(config))
        # 编码层已量化到 scale=2 → 相同字节序列 → exact_match
        assert result.exact_match, "尾随零在编码层已消除，应 exact_match"

    def test_true_difference(self):
        """Decimal(18,2)：1.20 vs 1.23 → scale=2 量化后不等 → OUT_OF_TOLERANCE。"""
        config = CreConfig(
            output_columns=[NormalizationColumn(column_name="val", data_type="decimal(18,2)")],
            primary_keys=["val"],
        )
        d_row = {"val": Decimal("1.20")}
        s_row = {"val": Decimal("1.23")}
        result = ToleranceComparator.compare_row(d_row, s_row, config, CREEncoder(config))
        assert not result.exact_match
        assert result.cell_results[0].reason == NotToleratedReason.OUT_OF_TOLERANCE

    def test_integer_decimal_equivalent(self):
        """Decimal(18,0)：5 vs 5.0 → scale=0 编码层等价 → exact_match。"""
        config = CreConfig(
            output_columns=[NormalizationColumn(column_name="val", data_type="decimal(18,0)")],
            primary_keys=["val"],
        )
        d_row = {"val": Decimal("5")}
        s_row = {"val": Decimal("5.0")}
        result = ToleranceComparator.compare_row(d_row, s_row, config, CREEncoder(config))
        # 编码层已量化到 scale=0 → 5 vs 5 → 相同编码
        assert result.exact_match, "整数 Decimal 在编码层应 exact_match"

    def test_different_scale_respected(self):
        """scale=2 只比较两位，scale=4 比较四位。
        CRE 编码层按 Contract scale 量化，不同 scale 产生不同编码。"""
        config_d2 = CreConfig(
            output_columns=[NormalizationColumn(column_name="val", data_type="decimal(18,2)")],
            primary_keys=["val"],
        )
        config_d4 = CreConfig(
            output_columns=[NormalizationColumn(column_name="val", data_type="decimal(18,4)")],
            primary_keys=["val"],
        )

        d_row = {"val": Decimal("1.2340")}
        s_row = {"val": Decimal("1.23")}
        # scale=2 → 1.23 vs 1.23 → 编码层已等价
        r2 = ToleranceComparator.compare_row(d_row, s_row, config_d2, CREEncoder(config_d2))
        assert r2.exact_match
        # scale=4 → 1.2340 vs 1.2300 → 编码层不等
        r4 = ToleranceComparator.compare_row(d_row, s_row, config_d4, CREEncoder(config_d4))
        assert not r4.exact_match
        assert any(c.reason == NotToleratedReason.OUT_OF_TOLERANCE for c in r4.cell_results)

    def test_no_float_conversion(self):
        """Decimal 转 string 后量化——不使用 float(value) 避免精度损失。"""
        config = CreConfig(
            output_columns=[NormalizationColumn(column_name="val", data_type="decimal(38,10)")],
            primary_keys=["val"],
        )
        # 大 Decimal 值，float 会丢失精度
        big_dec = Decimal("999999999999.1234567890")
        d_row = {"val": big_dec}
        s_row = {"val": big_dec}
        result = ToleranceComparator.compare_row(d_row, s_row, config, CREEncoder(config))
        assert result.exact_match  # 精确匹配，不是容差匹配


# ════════════════════════════════════════════
# 第 4 节：KeyBasedRowAligner 对齐
# ════════════════════════════════════════════


class TestKeyBasedRowAligner:
    """按权威主键对齐双引擎行。"""

    def test_aligned_success(self, cre_config, encoder, sample_rows):
        """双方行数一致 + 主键相同 → 完全对齐。"""
        result = KeyBasedRowAligner.align(sample_rows, sample_rows, cre_config, encoder)
        assert len(result.aligned_pairs) == 3
        assert len(result.duckdb_only) == 0
        assert len(result.spark_only) == 0
        assert not result.duplicate_keys
        assert not result.error_message

    def test_missing_rows(self, cre_config, encoder, sample_rows):
        """Spark 缺失 1 行 → duckdb_only 含该行。"""
        spark_rows = sample_rows[:2]  # 缺第 3 行
        result = KeyBasedRowAligner.align(sample_rows, spark_rows, cre_config, encoder)
        assert len(result.aligned_pairs) == 2
        assert len(result.duckdb_only) == 1
        assert len(result.spark_only) == 0
        assert result.duckdb_only[0]["order_id"] == 3

    def test_duplicate_keys(self, cre_config, encoder):
        """重复主键 → duplicate_keys=True。"""
        rows = [
            {"order_id": 1, "amount": Decimal("10.00"), "status": "a", "score": 1.0, "is_active": True},
            {"order_id": 1, "amount": Decimal("20.00"), "status": "b", "score": 2.0, "is_active": False},
        ]
        result = KeyBasedRowAligner.align(rows, rows, cre_config, encoder)
        assert result.duplicate_keys
        assert "重复键" in result.error_message

    def test_no_primary_keys(self, encoder, sample_rows):
        """缺少 primary_keys → error_message。"""
        config_no_pk = CreConfig(
            output_columns=[
                NormalizationColumn(column_name="order_id", data_type="bigint"),
            ],
            primary_keys=[],
        )
        result = KeyBasedRowAligner.align(sample_rows, sample_rows, config_no_pk, encoder)
        assert result.error_message
        assert "primary_keys" in result.error_message

    def test_missing_pk_column(self, cre_config, encoder, sample_rows):
        """主键列不在 output_columns 中 → error_message。"""
        bad_config = CreConfig(
            output_columns=[NormalizationColumn(column_name="name", data_type="varchar")],
            primary_keys=["missing_col"],
        )
        bad_enc = CREEncoder(bad_config)
        result = KeyBasedRowAligner.align(sample_rows, sample_rows, bad_config, bad_enc)
        assert result.error_message


# ════════════════════════════════════════════
# 第 5 节：BucketHasher 分桶
# ════════════════════════════════════════════


class TestBucketHasher:
    """基于主键 digest 的分桶与桶内 digest 验证。"""

    def test_all_buckets_match(self, cre_config, encoder, sample_rows):
        """相同数据 → 全桶 digest 匹配。"""
        aligned = [(r, r) for r in sample_rows]
        result = BucketHasher.compute_bucket_digests(aligned, encoder, cre_config)
        assert result.duckdb_bucket_digests == result.spark_bucket_digests
        assert len(result.mismatched_buckets) == 0

    def test_single_bucket_mismatch(self, cre_config, encoder, sample_rows):
        """修改 1 行后 → 仅该行所在桶 digest 不同。"""
        # 构造对齐行对
        spark_rows = [
            {
                "order_id": 1, "amount": Decimal("100.50"),
                "status": "completed", "score": 95.5, "is_active": True,
            },
            {  # 修改行
                "order_id": 2, "amount": Decimal("999.99"),
                "status": "CHANGED", "score": 87.0, "is_active": False,
            },
            {
                "order_id": 3, "amount": Decimal("150.75"),
                "status": "completed", "score": 92.3, "is_active": True,
            },
        ]
        aligned = list(zip(sample_rows, spark_rows))
        result = BucketHasher.compute_bucket_digests(aligned, encoder, cre_config)
        # 至少一个桶不匹配
        assert len(result.mismatched_buckets) >= 1


# ════════════════════════════════════════════
# 第 6 节：DecisionMatrix 判定矩阵
# ════════════════════════════════════════════


class TestDecisionEngine:
    """判定矩阵全覆盖测试（要求 7/9）。"""

    def test_exact_match_consistent(self, cre_config, encoder, sample_rows):
        """全量 exact match → CONSISTENT。"""
        alignment = KeyBasedRowAligner.align(sample_rows, sample_rows, cre_config, encoder)
        comps = [
            (d, s, ToleranceComparator.compare_row(d, s, cre_config, encoder))
            for d, s in alignment.aligned_pairs
        ]
        buckets = BucketHasher.compute_bucket_digests(alignment.aligned_pairs, encoder, cre_config)
        result = DecisionEngine.decide(alignment, comps, buckets, cre_config)
        assert result.status == "CONSISTENT", (
            f"期望 CONSISTENT，实际 {result.status}: {result.decision_reason}"
        )

    def test_all_rows_1e15_tail_consistent_with_warn(self):
        """100% 行均有 1e-15 尾差 → CONSISTENT_WITH_WARN（要求 7）。"""
        config = CreConfig(
            output_columns=[
                NormalizationColumn(column_name="id", data_type="bigint"),
                NormalizationColumn(column_name="val", data_type="double"),
            ],
            primary_keys=["id"],
        )
        enc = CREEncoder(config)
        duckdb_rows = [{"id": i, "val": 1.0 + i * 1e-15} for i in range(10)]
        spark_rows = [{"id": i, "val": 1.0 + i * 1.1e-15} for i in range(10)]

        alignment = KeyBasedRowAligner.align(duckdb_rows, spark_rows, config, enc)
        comps = [
            (d, s, ToleranceComparator.compare_row(d, s, config, enc))
            for d, s in alignment.aligned_pairs
        ]
        buckets = BucketHasher.compute_bucket_digests(alignment.aligned_pairs, enc, config)
        result = DecisionEngine.decide(alignment, comps, buckets, config)
        assert result.status == "CONSISTENT_WITH_WARN", (
            f"期望 CONSISTENT_WITH_WARN，实际 {result.status}: {result.decision_reason}"
        )
        # 必须有 WARN
        assert len(result.warnings) > 0

    def test_95_percent_consistent_one_real_error_mismatch(self):
        """95% 一致但存在 1 条真实整数字段错误 → MISMATCH（要求 7/9）。"""
        config = CreConfig(
            output_columns=[
                NormalizationColumn(column_name="id", data_type="bigint"),
                NormalizationColumn(column_name="val", data_type="double"),
                NormalizationColumn(column_name="label", data_type="varchar"),
            ],
            primary_keys=["id"],
        )
        enc = CREEncoder(config)

        duckdb_rows = []
        spark_rows = []
        for i in range(20):
            duckdb_rows.append({"id": i, "val": 1.0, "label": "ok"})
            if i == 19:
                # 第 20 行：真实字符串错误
                spark_rows.append({"id": i, "val": 1.0, "label": "NOT_OK"})
            else:
                spark_rows.append({"id": i, "val": 1.0 + i * 1e-15, "label": "ok"})

        alignment = KeyBasedRowAligner.align(duckdb_rows, spark_rows, config, enc)
        comps = [
            (d, s, ToleranceComparator.compare_row(d, s, config, enc))
            for d, s in alignment.aligned_pairs
        ]
        buckets = BucketHasher.compute_bucket_digests(alignment.aligned_pairs, enc, config)
        result = DecisionEngine.decide(alignment, comps, buckets, config)
        assert result.status == "MISMATCH", (
            f"期望 MISMATCH，实际 {result.status}: {result.decision_reason}"
        )

    def test_missing_contract_human_review(self, encoder, sample_rows):
        """缺少 output_columns → HUMAN_REVIEW。"""
        config = CreConfig(output_columns=[], primary_keys=["id"])
        alignment = KeyBasedRowAligner.align(sample_rows, sample_rows, config, encoder)
        buckets = BucketResult()
        result = DecisionEngine.decide(alignment, [], buckets, config)
        assert result.status == "HUMAN_REVIEW"

    def test_missing_primary_keys_human_review(self, cre_config, encoder, sample_rows):
        """缺少 primary_keys → HUMAN_REVIEW。"""
        config = CreConfig(
            output_columns=cre_config.output_columns,
            primary_keys=[],
        )
        alignment = KeyBasedRowAligner.align(sample_rows, sample_rows, config, encoder)
        buckets = BucketResult()
        result = DecisionEngine.decide(alignment, [], buckets, config)
        assert result.status == "HUMAN_REVIEW"

    def test_row_count_mismatch_mismatch(self, cre_config, encoder, sample_rows):
        """行数不匹配 → MISMATCH。"""
        # DuckDB 有第 4 行，Spark 没有
        extra_row = {
            "order_id": 4, "amount": Decimal("300.00"),
            "status": "new", "score": 70.0, "is_active": False,
        }
        duckdb_rows = sample_rows + [extra_row]
        spark_rows = sample_rows
        alignment = KeyBasedRowAligner.align(duckdb_rows, spark_rows, cre_config, encoder)
        comps = [
            (d, s, ToleranceComparator.compare_row(d, s, cre_config, encoder))
            for d, s in alignment.aligned_pairs
        ]
        buckets = BucketHasher.compute_bucket_digests(alignment.aligned_pairs, encoder, cre_config)
        result = DecisionEngine.decide(alignment, comps, buckets, cre_config)
        assert result.status == "MISMATCH"

    def test_integer_mismatch_mismatch(self):
        """整数字段差异 → MISMATCH。"""
        config = CreConfig(
            output_columns=[
                NormalizationColumn(column_name="id", data_type="bigint"),
                NormalizationColumn(column_name="count", data_type="int"),
            ],
            primary_keys=["id"],
        )
        enc = CREEncoder(config)
        d_rows = [{"id": 1, "count": 100}]
        s_rows = [{"id": 1, "count": 101}]
        alignment = KeyBasedRowAligner.align(d_rows, s_rows, config, enc)
        comps = [
            (d, s, ToleranceComparator.compare_row(d, s, config, enc))
            for d, s in alignment.aligned_pairs
        ]
        buckets = BucketHasher.compute_bucket_digests(alignment.aligned_pairs, enc, config)
        result = DecisionEngine.decide(alignment, comps, buckets, config)
        assert result.status == "MISMATCH"

    def test_duplicate_keys_human_review(self, cre_config, encoder):
        """重复主键 → HUMAN_REVIEW。"""
        rows = [
            {"order_id": 1, "amount": Decimal("10.00"), "status": "a", "score": 1.0, "is_active": True},
            {"order_id": 1, "amount": Decimal("20.00"), "status": "b", "score": 2.0, "is_active": False},
        ]
        alignment = KeyBasedRowAligner.align(rows, rows, cre_config, encoder)
        buckets = BucketResult()
        result = DecisionEngine.decide(alignment, [], buckets, cre_config)
        assert result.status == "HUMAN_REVIEW"

    def test_missing_type_info_human_review(self):
        """缺少 data_type → HUMAN_REVIEW。"""
        config = CreConfig(
            output_columns=[
                NormalizationColumn(column_name="id", data_type=None),  # 无 type
            ],
            primary_keys=["id"],
        )
        enc = CREEncoder(config)
        d_rows = [{"id": 1}]
        s_rows = [{"id": 2}]
        alignment = KeyBasedRowAligner.align(d_rows, s_rows, config, enc)
        comps = [
            (d, s, ToleranceComparator.compare_row(d, s, config, enc))
            for d, s in alignment.aligned_pairs
        ]
        buckets = BucketHasher.compute_bucket_digests(alignment.aligned_pairs, enc, config)
        result = DecisionEngine.decide(alignment, comps, buckets, config)
        # 主键匹配但 data_type=None → 比较时 NO_TYPE_INFO → HUMAN_REVIEW
        assert result.status == "HUMAN_REVIEW"

    def test_consistent_warn_ratio_high_still_consistent(self):
        """tolerated_ratio>5% 不改变判定（要求 3）。"""
        config = CreConfig(
            output_columns=[
                NormalizationColumn(column_name="id", data_type="bigint"),
                NormalizationColumn(column_name="val", data_type="double"),
            ],
            primary_keys=["id"],
            float_abs_tolerance=1e-12,
            float_rel_tolerance=1e-9,
        )
        enc = CREEncoder(config)
        # 50 行，全部在容差内
        d_rows = [{"id": i, "val": 1.0 + i * 1e-15} for i in range(50)]
        s_rows = [{"id": i, "val": 1.0 + i * 1.5e-15} for i in range(50)]

        alignment = KeyBasedRowAligner.align(d_rows, s_rows, config, enc)
        comps = [
            (d, s, ToleranceComparator.compare_row(d, s, config, enc))
            for d, s in alignment.aligned_pairs
        ]
        buckets = BucketHasher.compute_bucket_digests(alignment.aligned_pairs, enc, config)
        result = DecisionEngine.decide(alignment, comps, buckets, config)
        # 即使 ratio=100% > 5%，仍为 CONSISTENT_WITH_WARN（不是 HUMAN_REVIEW）
        assert result.status == "CONSISTENT_WITH_WARN"


# ════════════════════════════════════════════
# 第 7 节：ToleranceComparator 逐列比较
# ════════════════════════════════════════════


class TestToleranceComparator:
    """逐列容差比较。"""

    def test_exact_match(self, cre_config, encoder):
        """完全相同行 → exact_match=True。"""
        row = {"order_id": 1, "amount": Decimal("100.00"), "status": "ok", "score": 1.0, "is_active": True}
        result = ToleranceComparator.compare_row(row, row, cre_config, encoder)
        assert result.exact_match

    def test_within_tolerance_float(self, cre_config, encoder):
        """float 在容差内 → WITHIN_TOLERANCE。"""
        d_row = {
            "order_id": 1, "amount": Decimal("100.00"),
            "status": "ok", "score": 1.000000000001, "is_active": True,
        }
        s_row = {
            "order_id": 1, "amount": Decimal("100.00"),
            "status": "ok", "score": 1.0, "is_active": True,
        }
        result = ToleranceComparator.compare_row(d_row, s_row, cre_config, encoder)
        assert not result.exact_match
        cells = result.cell_results
        # 只有 score 列有差异（在容差内）
        assert any(c.reason == NotToleratedReason.WITHIN_TOLERANCE for c in cells)

    def test_out_of_tolerance_float(self, cre_config, encoder):
        """float 超容差 → OUT_OF_TOLERANCE。"""
        d_row = {
            "order_id": 1, "amount": Decimal("100.00"),
            "status": "ok", "score": 1.0, "is_active": True,
        }
        s_row = {
            "order_id": 1, "amount": Decimal("100.00"),
            "status": "ok", "score": 2.0, "is_active": True,
        }
        result = ToleranceComparator.compare_row(d_row, s_row, cre_config, encoder)
        assert not result.exact_match
        cells = result.cell_results
        assert any(c.reason == NotToleratedReason.OUT_OF_TOLERANCE for c in cells)

    def test_string_mismatch(self, cre_config, encoder):
        """字符串差异 → STRING_MISMATCH。"""
        d_row = {
            "order_id": 1, "amount": Decimal("100.00"),
            "status": "ok", "score": 1.0, "is_active": True,
        }
        s_row = {
            "order_id": 1, "amount": Decimal("100.00"),
            "status": "NOT_OK", "score": 1.0, "is_active": True,
        }
        result = ToleranceComparator.compare_row(d_row, s_row, cre_config, encoder)
        cells = result.cell_results
        assert any(c.reason == NotToleratedReason.STRING_MISMATCH for c in cells)

    def test_null_vs_value(self, cre_config, encoder):
        """NULL vs 有值 → UNKNOWN_CAUSE。"""
        d_row = {"order_id": 1, "amount": None, "status": "ok", "score": 1.0, "is_active": True}
        s_row = {"order_id": 1, "amount": Decimal("100.00"), "status": "ok", "score": 1.0, "is_active": True}
        result = ToleranceComparator.compare_row(d_row, s_row, cre_config, encoder)
        cells = result.cell_results
        assert any(c.reason == NotToleratedReason.UNKNOWN_CAUSE for c in cells)

    def test_bool_mismatch(self, cre_config, encoder):
        """布尔值差异 → BOOL_MISMATCH。"""
        d_row = {"order_id": 1, "amount": Decimal("100.00"), "status": "ok", "score": 1.0, "is_active": True}
        s_row = {"order_id": 1, "amount": Decimal("100.00"), "status": "ok", "score": 1.0, "is_active": False}
        result = ToleranceComparator.compare_row(d_row, s_row, cre_config, encoder)
        cells = result.cell_results
        assert any(c.reason == NotToleratedReason.BOOL_MISMATCH for c in cells)


# ════════════════════════════════════════════
# 第 8 节：整合——引擎跨执行一致性
# ════════════════════════════════════════════


class TestCREIntegration:
    """整合——模拟双引擎对同一 fixture 产生一致 digest。"""

    def test_full_pipeline_self_consistent(self, cre_config, encoder, sample_rows):
        """全流程：同一数据双引擎 → 全量 digest 一致 → CONSISTENT。"""
        # 模拟 DuckDB 和 Spark 返回相同数据
        duckdb_rows = sample_rows
        spark_rows = sample_rows

        alignment = KeyBasedRowAligner.align(duckdb_rows, spark_rows, cre_config, encoder)
        assert len(alignment.aligned_pairs) == 3

        comps = [
            (d, s, ToleranceComparator.compare_row(d, s, cre_config, encoder))
            for d, s in alignment.aligned_pairs
        ]
        buckets = BucketHasher.compute_bucket_digests(
            alignment.aligned_pairs, encoder, cre_config,
        )

        result = DecisionEngine.decide(alignment, comps, buckets, cre_config)
        assert result.status == "CONSISTENT"
        assert result.exact_match_rows == 3

    def test_full_pipeline_float_tail_diffs(self, cre_config, encoder):
        """全流程：一致的 key 但每行都有微小平浮点差 → CONSISTENT_WITH_WARN。
        使用 ULP 级差异（约 2.2e-16），确保 IEEE 754 编码可区分但 isclose 通过。"""
        ulp = math.ulp(1.0)
        rows = [
            {
                "order_id": i,
                "amount": Decimal("100.00"),
                "status": "ok",
                "score": 1.0 + i * ulp,
                "is_active": True,
            }
            for i in range(5)
        ]
        spark_rows = [
            {
                "order_id": i,
                "amount": Decimal("100.00"),
                "status": "ok",
                "score": 1.0 + i * ulp * 2,  # diff = i * ulp
                "is_active": True,
            }
            for i in range(5)
        ]

        alignment = KeyBasedRowAligner.align(rows, spark_rows, cre_config, encoder)
        comps = [
            (d, s, ToleranceComparator.compare_row(d, s, cre_config, encoder))
            for d, s in alignment.aligned_pairs
        ]
        buckets = BucketHasher.compute_bucket_digests(
            alignment.aligned_pairs, encoder, cre_config,
        )
        result = DecisionEngine.decide(alignment, comps, buckets, cre_config)
        assert result.status == "CONSISTENT_WITH_WARN"
        assert result.warnings

    def test_key_aligned_before_tolerance(self, cre_config, encoder):
        """同一 key 对齐后执行容差比较（要求 2）。"""
        # DuckDB 和 Spark 对同一 order_id 有不同的 amount
        d_rows = [
            {"order_id": 1, "amount": Decimal("100.00"), "status": "ok", "score": 1.0, "is_active": True},
        ]
        s_rows = [
            {"order_id": 1, "amount": Decimal("100.01"), "status": "ok", "score": 1.0, "is_active": True},
        ]

        # 先对齐（基于 order_id）
        alignment = KeyBasedRowAligner.align(d_rows, s_rows, cre_config, encoder)
        assert len(alignment.aligned_pairs) == 1
        d_aligned, s_aligned = alignment.aligned_pairs[0]
        assert d_aligned["order_id"] == s_aligned["order_id"] == 1

        # 对齐后逐列比较
        result = ToleranceComparator.compare_row(d_aligned, s_aligned, cre_config, encoder)
        # amount 列差异必须在容差规则内？Decimal 差异 0.01 → scale=2 量化后不等
        assert not result.exact_match
        # amount diff=0.01 > 0 → OUT_OF_TOLERANCE（Decimal 无 isclose）
        # 但 Decimal 的 OUT_OF_TOLERANCE 是量化后不等
        assert any(
            c.reason == NotToleratedReason.OUT_OF_TOLERANCE
            for c in result.cell_results
        )


# ════════════════════════════════════════════
# 第 9 节：边界和异常用例
# ════════════════════════════════════════════


class TestEdgeCases:
    """边界和异常用例。"""

    def test_empty_output_columns(self):
        """output_columns 为空 → ValueError。"""
        config = CreConfig(output_columns=[], primary_keys=["id"])
        with pytest.raises(ValueError, match="output_columns 为空"):
            CREEncoder(config).encode_row({"id": 1})

    def test_encoder_rejects_unknown_type(self):
        """未知 data_type → ValueError。"""
        config = CreConfig(
            output_columns=[NormalizationColumn(column_name="val", data_type="custom_type")],
            primary_keys=["val"],
        )
        enc = CREEncoder(config)
        with pytest.raises(ValueError, match="不支持的 data_type"):
            enc.encode_value(42, "custom_type")

    def test_encoder_rejects_complex_type(self):
        """COMPLEX 类型 → ValueError。"""
        config = CreConfig(
            output_columns=[NormalizationColumn(column_name="val", data_type="array<int>")],
            primary_keys=["val"],
        )
        enc = CREEncoder(config)
        with pytest.raises(ValueError, match="不支持的数据类型族 COMPLEX"):
            enc.encode_value([1, 2, 3], "array<int>")

    def test_no_rules_unknown_type_mismatch(self):
        """未知类型 → encode_row 拒绝，抛出 ValueError。
        （设计原则：禁止未知类型通过 CRE 编码，原始差异和规则缺失在设计层面捕获）"""
        config = CreConfig(
            output_columns=[NormalizationColumn(column_name="val", data_type="macaddr")],
            primary_keys=["val"],
        )
        with pytest.raises(ValueError, match="不支持的 data_type"):
            enc = CREEncoder(config)
            enc.encode_row({"val": "00:11:22:33:44:55"})

    def test_comparator_rejects_no_type(self):
        """data_type=None → NO_TYPE_INFO。"""
        config = CreConfig(
            output_columns=[NormalizationColumn(column_name="val", data_type=None)],
            primary_keys=["val"],
        )
        enc = CREEncoder(config)
        # 无 data_type 时 encode_value 按 str(value) 编码...
        # 实际上 _type_family(None) 返回 "UNKNOWN" → use encode_value with data_type=None
        # Which goes through the else branch → raises ValueError
        d_row = {"val": "hello"}
        s_row = {"val": "world"}
        with pytest.raises(ValueError, match="不支持的 data_type"):
            ToleranceComparator.compare_row(d_row, s_row, config, enc)


# ════════════════════════════════════════════
# 第 10 节：辅助函数测试
# ════════════════════════════════════════════


class TestHelpers:
    """辅助函数 _normalize_name / _type_family。"""

    def test_normalize_name(self):
        assert _normalize_name("OrderId") == "orderid"
        assert _normalize_name("table.column") == "column"
        assert _normalize_name("  spaced  ") == "spaced"
        assert _normalize_name("gold.fact_trips.passenger_count") == "passenger_count"

    def test_type_family(self):
        assert _type_family("bigint") == "INT64"
        assert _type_family("int") == "INT32"
        assert _type_family("decimal(18,2)") == "DECIMAL"
        assert _type_family("numeric(10,0)") == "DECIMAL"
        assert _type_family("varchar") == "VARCHAR"
        assert _type_family("boolean") == "BOOLEAN"
        assert _type_family("date") == "DATE"
        assert _type_family("timestamp") == "TIMESTAMP"
        assert _type_family("float") == "FLOAT"
        assert _type_family("double") == "DOUBLE"
        assert _type_family("array<struct<...>>") == "COMPLEX"
        assert _type_family("unknown_type") == "UNKNOWN"
        assert _type_family("") == "UNKNOWN"

    def test_cre2_magic_preserved(self, encoder, sample_rows):
        """所有编码结果以 CRE2 magic 开头。"""
        for row in sample_rows:
            encoded = encoder.encode_row(row)
            assert encoded[:4] == b"CRE2"


# ════════════════════════════════════════════
# 第 11 节：反向测试——NULL 主键防御
# ════════════════════════════════════════════


class TestReverseNullPrimaryKey:
    """NULL/空字符串主键 → error_message（禁止按 NULL digest 对齐）。"""

    def test_null_pk_in_duckdb(self, contract_cols):
        """DuckDB 侧主键为 NULL → error_message。"""
        config = CreConfig(
            output_columns=contract_cols,
            primary_keys=["order_id"],
        )
        enc = CREEncoder(config)
        d_rows = [{"order_id": None, "amount": Decimal("100.00"),
                    "status": "ok", "score": 1.0, "is_active": True}]
        s_rows = [{"order_id": 1, "amount": Decimal("100.00"),
                    "status": "ok", "score": 1.0, "is_active": True}]
        result = KeyBasedRowAligner.align(d_rows, s_rows, config, enc)
        assert result.error_message, "NULL 主键应产生 error_message"
        assert "NULL" in result.error_message

    def test_null_pk_in_spark(self, contract_cols):
        """Spark 侧主键为 NULL → error_message。"""
        config = CreConfig(
            output_columns=contract_cols,
            primary_keys=["order_id"],
        )
        enc = CREEncoder(config)
        d_rows = [{"order_id": 1, "amount": Decimal("100.00"),
                    "status": "ok", "score": 1.0, "is_active": True}]
        s_rows = [{"order_id": None, "amount": Decimal("100.00"),
                    "status": "ok", "score": 1.0, "is_active": True}]
        result = KeyBasedRowAligner.align(d_rows, s_rows, config, enc)
        assert result.error_message, "NULL 主键应产生 error_message"

    def test_empty_string_pk(self, contract_cols):
        """空字符串主键 → error_message。"""
        config = CreConfig(
            output_columns=contract_cols + [
                NormalizationColumn(column_name="name", data_type="varchar"),
            ],
            primary_keys=["name"],
        )
        enc = CREEncoder(config)
        d_rows = [{"order_id": 1, "amount": Decimal("100.00"),
                    "status": "ok", "score": 1.0, "is_active": True, "name": ""}]
        s_rows = [{"order_id": 1, "amount": Decimal("100.00"),
                    "status": "ok", "score": 1.0, "is_active": True, "name": "alice"}]
        result = KeyBasedRowAligner.align(d_rows, s_rows, config, enc)
        assert result.error_message, "空字符串主键应产生 error_message"

    def test_null_pk_decision_engine_human_review(self, contract_cols):
        """NULL 主键 → alignment error → Gate 2 → HUMAN_REVIEW。"""
        config = CreConfig(
            output_columns=contract_cols,
            primary_keys=["order_id"],
        )
        enc = CREEncoder(config)
        d_rows = [{"order_id": None, "amount": Decimal("100.00"),
                    "status": "ok", "score": 1.0, "is_active": True}]
        s_rows = [{"order_id": 1, "amount": Decimal("100.00"),
                    "status": "ok", "score": 1.0, "is_active": True}]
        alignment = KeyBasedRowAligner.align(d_rows, s_rows, config, enc)
        buckets = BucketResult()
        result = DecisionEngine.decide(alignment, [], buckets, config)
        assert result.status == "HUMAN_REVIEW", (
            f"NULL 主键应触发 HUMAN_REVIEW，实际 {result.status}"
        )
        assert "对齐失败" in result.decision_reason


# ════════════════════════════════════════════
# 第 12 节：反向测试——Decimal 边界碰撞与编码无歧义
# ════════════════════════════════════════════


class TestReverseDecimalBoundary:
    """Decimal 编码无歧义、边界碰撞不重复、正负数边界。"""

    def test_positive_negative_distinct(self):
        """正负 Decimal 编码应不同。"""
        config = CreConfig(
            output_columns=[NormalizationColumn(column_name="val", data_type="decimal(10,2)")],
            primary_keys=["val"],
        )
        enc = CREEncoder(config)
        pos_enc = enc.encode_value(Decimal("1.00"), "decimal(10,2)")
        neg_enc = enc.encode_value(Decimal("-1.00"), "decimal(10,2)")
        assert pos_enc != neg_enc, "正负数编码必须不同"
        # 验证 type_tag=0x08
        assert pos_enc[0] == 0x08
        assert neg_enc[0] == 0x08

    def test_negative_zero_normalized(self):
        """+0 和 -0 应归一化为相同编码。"""
        config = CreConfig(
            output_columns=[NormalizationColumn(column_name="val", data_type="decimal(10,2)")],
            primary_keys=["val"],
        )
        enc = CREEncoder(config)
        pos0 = enc.encode_value(Decimal("0.00"), "decimal(10,2)")
        neg0 = enc.encode_value(Decimal("-0.00"), "decimal(10,2)")
        assert pos0 == neg0, "±0 应归一化为相同编码"

    def test_no_collision_different_precision(self):
        """不同 precision/scale 产生不同编码（即使数值等价）。"""
        enc_p2 = CREEncoder(CreConfig(
            output_columns=[NormalizationColumn(column_name="val", data_type="decimal(5,2)")],
            primary_keys=["val"],
        ))
        enc_p4 = CREEncoder(CreConfig(
            output_columns=[NormalizationColumn(column_name="val", data_type="decimal(7,4)")],
            primary_keys=["val"],
        ))
        # 同数值 1.00，但 precision/scale 不同
        b1 = enc_p2.encode_value(Decimal("1.00"), "decimal(5,2)")
        b2 = enc_p4.encode_value(Decimal("1.00"), "decimal(7,4)")
        assert b1 != b2, "不同 precision/scale 的编码必须有区别"

    def test_unscaled_length_prefix_unambiguous(self):
        """length prefix 使编码无歧义——能被正确解码。"""
        config = CreConfig(
            output_columns=[NormalizationColumn(column_name="val", data_type="decimal(18,2)")],
            primary_keys=["val"],
        )
        enc = CREEncoder(config)
        encoded = enc.encode_value(Decimal("1234567890.12"), "decimal(18,2)")
        # type_tag(1) + precision(1) + scale(1) + length(1) + value(N)
        assert encoded[0] == 0x08  # type_tag
        assert encoded[1] == 18    # precision
        assert encoded[2] == 2     # scale
        length_byte = encoded[3]
        assert length_byte > 0, "unscaled_value 长度应 > 0"
        assert len(encoded) == 4 + length_byte, "总长度 = header(4) + value"


class TestReverseDecimalPrecisionOverflow:
    """Decimal precision 溢出校验。"""

    def test_precision_too_high_raises(self):
        """precision=39 超出最大范围 38 → ValueError。"""
        config = CreConfig(
            output_columns=[NormalizationColumn(column_name="val", data_type="decimal(39,2)")],
            primary_keys=["val"],
        )
        enc = CREEncoder(config)
        with pytest.raises(ValueError, match="precision"):
            enc.encode_value(Decimal("1.00"), "decimal(39,2)")

    def test_precision_below_min_raises(self):
        """precision=0 低于最小范围 1 → ValueError。"""
        config = CreConfig(
            output_columns=[NormalizationColumn(column_name="val", data_type="decimal(0,0)")],
            primary_keys=["val"],
        )
        enc = CREEncoder(config)
        with pytest.raises(ValueError, match="precision"):
            enc.encode_value(Decimal("0"), "decimal(0,0)")

    def test_scale_greater_than_precision_raises(self):
        """scale > precision → ValueError。"""
        config = CreConfig(
            output_columns=[NormalizationColumn(column_name="val", data_type="decimal(10,15)")],
            primary_keys=["val"],
        )
        enc = CREEncoder(config)
        with pytest.raises(ValueError, match="scale"):
            enc.encode_value(Decimal("1.00"), "decimal(10,15)")

    def test_value_exceeds_precision_raises(self):
        """Decimal 值超出 precision 约束 → ValueError。"""
        config = CreConfig(
            output_columns=[NormalizationColumn(column_name="val", data_type="decimal(3,0)")],
            primary_keys=["val"],
        )
        enc = CREEncoder(config)
        # precision=3, max=999, 但 1000 > 999
        with pytest.raises(ValueError, match="超出"):
            enc.encode_value(Decimal("1000"), "decimal(3,0)")

    def test_value_just_at_precision_boundary_passes(self):
        """Decimal 值刚好在 precision 边界上 → 能编码。"""
        config = CreConfig(
            output_columns=[NormalizationColumn(column_name="val", data_type="decimal(3,0)")],
            primary_keys=["val"],
        )
        enc = CREEncoder(config)
        # precision=3, max=999
        encoded = enc.encode_value(Decimal("999"), "decimal(3,0)")
        assert encoded[0] == 0x08


# ════════════════════════════════════════════
# 第 13 节：反向测试——Timestamp 时区处理
# ════════════════════════════════════════════


class TestReverseTimestampTimezone:
    """Timestamp 时区转换——非 UTC 时区、naive datetime 禁止默认 UTC。"""

    def test_non_utc_timezone_encoding(self):
        """非 UTC 时区（Europe/Berlin）编码后与同一时刻的 UTC 值相同。"""
        config = CreConfig(
            output_columns=[NormalizationColumn(column_name="ts", data_type="timestamp")],
            primary_keys=["ts"],
            timezone="Europe/Berlin",
        )
        enc = CREEncoder(config)

        import datetime as _dt
        # Berlin 的 2024-01-01 00:00:00 = UTC 2023-12-31 23:00:00
        berlin_naive = _dt.datetime(2024, 1, 1, 0, 0, 0)
        # 手动计算 UTC 等价时间（Berlin 冬季 = UTC+1）
        utc_aware = _dt.datetime(2023, 12, 31, 23, 0, 0, tzinfo=_dt.timezone.utc)

        berlin_enc = enc.encode_value(berlin_naive, "timestamp")
        utc_enc = enc.encode_value(utc_aware, "timestamp")
        assert berlin_enc == utc_enc, (
            f"Berlin 2024-01-01 00:00:00 应等于 UTC 2023-12-31 23:00:00。"
            f"Berlin编码={berlin_enc.hex()}, UTC编码={utc_enc.hex()}"
        )

    def test_naive_datetime_without_timezone_rejected(self):
        """无 timezone 配置且遇到 naive datetime → ValueError。"""
        config = CreConfig(
            output_columns=[NormalizationColumn(column_name="ts", data_type="timestamp")],
            primary_keys=["ts"],
            timezone="",  # 无时区
        )
        with pytest.raises(ValueError, match="timezone"):
            CREEncoder(config)

    def test_naive_string_without_timezone_rejected(self):
        """配置了 timezone 但字符串为 naive → 按配置时区 localize。"""
        config = CreConfig(
            output_columns=[NormalizationColumn(column_name="ts", data_type="timestamp")],
            primary_keys=["ts"],
            timezone="Asia/Shanghai",
        )
        enc = CREEncoder(config)
        # Shanghai 2024-01-01 00:00:00 = UTC 2023-12-31 16:00:00
        encoded = enc.encode_value("2024-01-01T00:00:00", "timestamp")
        assert len(encoded) == 9  # type_tag(1) + micros(8)
        # 验证是 UTC 2023-12-31 16:00:00
        import datetime as _dt
        epoch = _dt.datetime(1970, 1, 1, tzinfo=_dt.timezone.utc)
        sh_naive = _dt.datetime(2024, 1, 1, 0, 0, 0)
        sh_tz = _dt.timezone(_dt.timedelta(hours=8))
        sh_aware = sh_naive.replace(tzinfo=sh_tz)
        expected_micros = int((sh_aware - epoch).total_seconds() * 1_000_000)
        decoded_micros = int.from_bytes(encoded[1:], "big", signed=True)
        assert decoded_micros == expected_micros, (
            f"Asia/Shanghai naive datetime 编码错误："
            f"期望 {expected_micros}，实际 {decoded_micros}"
        )

    def test_aware_datetime_converted(self):
        """有时区信息的 datetime 正确转换到 UTC。"""
        config = CreConfig(
            output_columns=[NormalizationColumn(column_name="ts", data_type="timestamp")],
            primary_keys=["ts"],
            timezone="America/New_York",
        )
        enc = CREEncoder(config)

        import datetime as _dt
        # NY 2024-06-01 12:00:00 EDT = UTC 16:00:00（EDT=UTC-4）
        ny_aware = _dt.datetime(
            2024, 6, 1, 12, 0, 0,
            tzinfo=_dt.timezone(_dt.timedelta(hours=-4)),
        )
        encoded = enc.encode_value(ny_aware, "timestamp")
        epoch = _dt.datetime(1970, 1, 1, tzinfo=_dt.timezone.utc)
        expected_micros = int(
            (ny_aware.astimezone(_dt.timezone.utc) - epoch).total_seconds() * 1_000_000
        )
        decoded_micros = int.from_bytes(encoded[1:], "big", signed=True)
        assert decoded_micros == expected_micros

    def test_timezone_comparison_consistent(self):
        """相同时刻但不同时区表示的 datetime → _compare_cell 判定为等价。"""
        config = CreConfig(
            output_columns=[
                NormalizationColumn(column_name="id", data_type="bigint"),
                NormalizationColumn(column_name="ts", data_type="timestamp"),
            ],
            primary_keys=["id"],
            timezone="Europe/London",
        )
        enc = CREEncoder(config)

        import datetime as _dt
        # London winter (UTC+0): 2024-01-01 12:00:00 naive
        d_row = {"id": 1, "ts": _dt.datetime(2024, 1, 1, 12, 0, 0)}
        # Berlin winter (UTC+1): 同一时刻
        s_row = {"id": 1, "ts": _dt.datetime(
            2024, 1, 1, 13, 0, 0, tzinfo=_dt.timezone(_dt.timedelta(hours=1)),
        )}

        result = ToleranceComparator.compare_row(d_row, s_row, config, enc)
        # 两值编码应不相等（London naive→London tz→UTC vs Berlin aware→UTC）
        # 但都转换到 UTC 应为同一值
        assert result.exact_match, "相同时刻不同时区表示应 exact_match"

    # ── DST 歧义检测 ──

    def test_dst_spring_forward_gap_raises(self):
        """春令时跳转（spring-forward）中的不存在时间 → _encode_timestamp 拒绝。

        US/Eastern 2024-03-10 02:30:00 不存在（02:00 跳到 03:00）。
        """
        config = CreConfig(
            output_columns=[NormalizationColumn(column_name="ts", data_type="timestamp")],
            primary_keys=["ts"],
            timezone="America/New_York",
        )
        enc = CREEncoder(config)
        import datetime as _dt
        gap_dt = _dt.datetime(2024, 3, 10, 2, 30, 0)
        with pytest.raises(ValueError, match="DST 歧义"):
            enc.encode_value(gap_dt, "timestamp")

    def test_dst_fall_back_overlap_raises(self):
        """冬令时回退（fall-back）中的重叠时间 → _encode_timestamp 拒绝。

        US/Eastern 2024-11-03 01:30:00 出现两次（EDT 01:30→EST 01:30）。
        """
        config = CreConfig(
            output_columns=[NormalizationColumn(column_name="ts", data_type="timestamp")],
            primary_keys=["ts"],
            timezone="America/New_York",
        )
        enc = CREEncoder(config)
        import datetime as _dt
        overlap_dt = _dt.datetime(2024, 11, 3, 1, 30, 0)
        with pytest.raises(ValueError, match="DST 歧义"):
            enc.encode_value(overlap_dt, "timestamp")

    def test_dst_normal_time_passes(self):
        """DST 过渡期以外的时间正常编码。"""
        config = CreConfig(
            output_columns=[NormalizationColumn(column_name="ts", data_type="timestamp")],
            primary_keys=["ts"],
            timezone="America/New_York",
        )
        enc = CREEncoder(config)
        import datetime as _dt
        # 冬季正常时间（非 DST 过渡期）
        winter_dt = _dt.datetime(2024, 1, 15, 12, 0, 0)
        encoded = enc.encode_value(winter_dt, "timestamp")
        assert len(encoded) == 9  # type_tag(1) + micros(8)

        # 夏季正常时间（非 DST 过渡期）
        summer_dt = _dt.datetime(2024, 7, 15, 12, 0, 0)
        encoded2 = enc.encode_value(summer_dt, "timestamp")
        assert len(encoded2) == 9

    def test_dst_string_spring_gap_raises(self):
        """字符串形式的 spring-forward 不存在时间也被拒绝。"""
        config = CreConfig(
            output_columns=[NormalizationColumn(column_name="ts", data_type="timestamp")],
            primary_keys=["ts"],
            timezone="America/New_York",
        )
        enc = CREEncoder(config)
        with pytest.raises(ValueError, match="DST 歧义"):
            enc.encode_value("2024-03-10T02:30:00", "timestamp")

    def test_dst_comparator_detects_ambiguity(self):
        """比较器在遇到 DST 歧义值时标记 TIMESTAMP_MISMATCH。

        注：encoder.encode_row 已先拒绝 DST 歧义值，因此本测试直接调用
        _compare_cell 验证比较路径的 DST 检测。
        """
        config = CreConfig(
            output_columns=[NormalizationColumn(column_name="ts", data_type="timestamp")],
            primary_keys=["ts"],
            timezone="America/New_York",
        )
        import datetime as _dt
        # 两个引擎都返回了 DST 歧义值——都无法唯—确定
        d_val = _dt.datetime(2024, 11, 3, 1, 30, 0)
        s_val = _dt.datetime(2024, 11, 3, 1, 30, 0)
        result = ToleranceComparator._compare_cell(d_val, s_val, "timestamp", config)
        assert result is not None
        assert result.reason == NotToleratedReason.TIMESTAMP_MISMATCH, (
            f"预期 TIMESTAMP_MISMATCH，实际 {result.reason}"
        )


# ════════════════════════════════════════════
# 第 14 节：反向测试——字符串 "false" 拒绝
# ════════════════════════════════════════════


class TestReverseBoolStringFalse:
    """布尔值拒绝字符串 "false" 被隐式解析为 True。"""

    def test_encode_bool_string_false_rejected(self):
        """字符串 'false' 被 _encode_bool 接受为 False（不是 True）。"""
        config = CreConfig(
            output_columns=[NormalizationColumn(column_name="flag", data_type="boolean")],
            primary_keys=["flag"],
        )
        enc = CREEncoder(config)
        encoded = enc.encode_value("false", "boolean")
        # False → tag=0x01 + value 0x00
        assert encoded == b"\x01\x00", f"字符串 false 应编码为 False，实际 {encoded.hex()}"

    def test_encode_bool_wrong_string_rejected(self):
        """不支持的布尔字符串 → ValueError。"""
        config = CreConfig(
            output_columns=[NormalizationColumn(column_name="flag", data_type="boolean")],
            primary_keys=["flag"],
        )
        enc = CREEncoder(config)
        with pytest.raises(ValueError, match="不支持的布尔"):
            enc.encode_value("maybe", "boolean")

    def test_encode_bool_non_bool_type_rejected(self):
        """不支持的非 bool/int/str 类型 → ValueError。"""
        config = CreConfig(
            output_columns=[NormalizationColumn(column_name="flag", data_type="boolean")],
            primary_keys=["flag"],
        )
        enc = CREEncoder(config)
        with pytest.raises(ValueError, match="不支持的布尔"):
            enc.encode_value([1, 2, 3], "boolean")

    def test_compare_cell_string_false_detected(self):
        """禁止 bool("false")==True——字符串 false 被解析为有效的 False。"""
        config = CreConfig(
            output_columns=[NormalizationColumn(column_name="flag", data_type="boolean")],
            primary_keys=["flag"],
        )
        enc = CREEncoder(config)
        # 双方都是字符串 "false"（通过 _compare_cell 比较）
        # 应被解析为 false==false → 等价
        row = {"flag": "false"}
        # encode_row 走 encode_value → _encode_bool，这里只看比较
        result = ToleranceComparator.compare_row(row, row, config, enc)
        assert result.exact_match, "双方字符串 false 应 exact_match"

    def test_compare_cell_string_true_vs_false(self):
        """'true' vs 'false' → BOOL_MISMATCH（不是意外等价）。"""
        config = CreConfig(
            output_columns=[NormalizationColumn(column_name="flag", data_type="boolean")],
            primary_keys=["flag"],
        )
        enc = CREEncoder(config)
        d_row = {"flag": "true"}
        s_row = {"flag": "false"}
        result = ToleranceComparator.compare_row(d_row, s_row, config, enc)
        assert not result.exact_match
        assert any(c.reason == NotToleratedReason.BOOL_MISMATCH for c in result.cell_results)

    def test_encode_bool_int_5_rejected(self):
        """int(5) 被 _encode_bool 拒绝——仅接受 0/1。"""
        config = CreConfig(
            output_columns=[NormalizationColumn(column_name="flag", data_type="boolean")],
            primary_keys=["flag"],
        )
        enc = CREEncoder(config)
        with pytest.raises(ValueError, match="不支持的布尔整数"):
            enc.encode_value(5, "boolean")

    def test_encode_bool_int_2_rejected(self):
        """int(2) 被 _encode_bool 拒绝——仅接受 0/1。"""
        config = CreConfig(
            output_columns=[NormalizationColumn(column_name="flag", data_type="boolean")],
            primary_keys=["flag"],
        )
        enc = CREEncoder(config)
        with pytest.raises(ValueError, match="不支持的布尔整数"):
            enc.encode_value(2, "boolean")


# ════════════════════════════════════════════
# 第 15 节：反向测试——ToleranceComparator 严格布尔比较
# ════════════════════════════════════════════


class TestReverseComparatorBool:
    """ToleranceComparator 的布尔比较不信任隐式 bool()。"""

    def test_bool_vs_string_false_detected(self):
        """bool(True) vs str('false') → 被检测到差异。"""
        config = CreConfig(
            output_columns=[NormalizationColumn(column_name="flag", data_type="boolean")],
            primary_keys=["flag"],
        )
        enc = CREEncoder(config)
        d_row = {"flag": True}
        s_row = {"flag": "false"}
        result = ToleranceComparator.compare_row(d_row, s_row, config, enc)
        assert not result.exact_match
        # True vs False → BOOL_MISMATCH
        assert any(c.reason == NotToleratedReason.BOOL_MISMATCH for c in result.cell_results)

    def test_bool_vs_int_consistent(self):
        """bool(True) vs int(1) → 等价。"""
        config = CreConfig(
            output_columns=[NormalizationColumn(column_name="flag", data_type="boolean")],
            primary_keys=["flag"],
        )
        enc = CREEncoder(config)
        d_row = {"flag": True}
        s_row = {"flag": 1}
        result = ToleranceComparator.compare_row(d_row, s_row, config, enc)
        assert result.exact_match or all(
            c.reason == NotToleratedReason.WITHIN_TOLERANCE
            for c in result.cell_results
        ), "True 和 1 应等价"

    def test_bool_vs_int_zero_consistent(self):
        """bool(False) vs int(0) → 等价。"""
        config = CreConfig(
            output_columns=[NormalizationColumn(column_name="flag", data_type="boolean")],
            primary_keys=["flag"],
        )
        enc = CREEncoder(config)
        d_row = {"flag": False}
        s_row = {"flag": 0}
        result = ToleranceComparator.compare_row(d_row, s_row, config, enc)
        assert result.exact_match, "False 和 0 编码后应 exact_match"

    def test_bool_int_5_rejected_in_comparator(self):
        """int(5) 在 _compare_cell 中返回 UNKNOWN_CAUSE——非 0/1 整数视为非法。"""
        config = CreConfig(
            output_columns=[NormalizationColumn(column_name="flag", data_type="boolean")],
            primary_keys=["flag"],
        )
        # 直接测试 _compare_cell（compare_row 先走 encode_row，int 5 在编码层就被拒）
        result = ToleranceComparator._compare_cell(5, 5, "boolean", config)
        assert result is not None
        assert result.reason == NotToleratedReason.UNKNOWN_CAUSE

    def test_bool_int_5_vs_1_mismatch(self):
        """int(5) vs int(1) 在 _compare_cell 中返回 UNKNOWN_CAUSE（5 非法）。"""
        config = CreConfig(
            output_columns=[NormalizationColumn(column_name="flag", data_type="boolean")],
            primary_keys=["flag"],
        )
        result = ToleranceComparator._compare_cell(5, 1, "boolean", config)
        assert result is not None
        assert result.reason == NotToleratedReason.UNKNOWN_CAUSE
