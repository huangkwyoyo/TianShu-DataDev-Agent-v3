"""CDP v1 数据模型严格校验测试。"""
import pytest

from tianshu_datadev.spark.cdp_spec import (
    CreDigestSpec,
    CreSampleSpec,
    DigestExecutionEnvelope,
    EngineDigestSummary,
    TypeFamily,
    compute_digest_spec_hash,
)


class TestTypeFamily:
    def test_null_not_in_enum(self):
        """NULL 不是 TypeFamily 成员。"""
        assert not hasattr(TypeFamily, "NULL")
        with pytest.raises(ValueError):
            TypeFamily(0x00)

    def test_values_unique(self):
        vals = [t.value for t in TypeFamily]
        assert len(vals) == len(set(vals)) == 11


class TestCreDigestSpec:
    def test_rejects_mismatched_list_lengths(self):
        """所有列表必须与 output_columns 等长。"""
        with pytest.raises(Exception):  # ValidationError
            CreDigestSpec(
                output_columns=["id", "name"],
                type_families=[TypeFamily.INT64],  # 只有 1 个
                timezone="Asia/Shanghai",
                decimal_precision=[None, None],
                decimal_scale=[None, None],
                float_precision=[None, None],
            )

    def test_rejects_decimal_precision_on_non_decimal(self):
        """decimal_precision 仅 DECIMAL 列可非 None。"""
        with pytest.raises(Exception):
            CreDigestSpec(
                output_columns=["id"],
                type_families=[TypeFamily.INT64],  # 非 DECIMAL
                timezone="UTC",
                decimal_precision=[12],  # 应为 None
                decimal_scale=[None],
                float_precision=[None],
            )

    def test_rejects_float_precision_on_non_float(self):
        """float_precision 仅 FLOAT32/FLOAT64 列可非 None。"""
        with pytest.raises(Exception):
            CreDigestSpec(
                output_columns=["name"],
                type_families=[TypeFamily.VARCHAR],
                timezone="UTC",
                decimal_precision=[None],
                decimal_scale=[None],
                float_precision=[2],  # 应为 None
            )

    def test_accepts_valid_spec(self):
        """合法 spec 通过所有校验。"""
        spec = CreDigestSpec(
            output_columns=["id", "amount", "rate"],
            type_families=[TypeFamily.INT64, TypeFamily.DECIMAL, TypeFamily.FLOAT64],
            timezone="Asia/Shanghai",
            decimal_precision=[None, 12, None],
            decimal_scale=[None, 2, None],
            float_precision=[None, None, 4],
        )
        assert spec.protocol_version == "cdp-v1"


class TestCreSampleSpec:
    def test_default_factory_isolates_instances(self):
        """每个实例的 primary_keys 是独立列表——防止 mutable default 共享。"""
        s1 = CreSampleSpec()
        s2 = CreSampleSpec()
        s1.primary_keys.append("id")
        assert s2.primary_keys == []  # 不受 s1 影响

    def test_sample_size_clamped(self):
        """sample_size 必须在 1-100 范围内。"""
        with pytest.raises(Exception):
            CreSampleSpec(sample_size=0)
        with pytest.raises(Exception):
            CreSampleSpec(sample_size=101)


class TestDigestExecutionEnvelope:
    def test_success_requires_summary(self):
        """status==SUCCESS → summary 必须非 None。"""
        with pytest.raises(Exception):
            DigestExecutionEnvelope(
                execution_status="SUCCESS",
                snapshot_id="s1",
                digest_spec_hash="a" * 64,
                protocol_version="cdp-v1",
                engine_version="1.0",
                summary=None,  # 非法
            )

    @pytest.mark.parametrize("status", ["FAILED", "TIMEOUT", "UNSUPPORTED", "REQUIRES_REVIEW"])
    def test_non_success_forbids_summary(self, status):
        """status!=SUCCESS → summary 必须为 None。"""
        with pytest.raises(Exception):
            DigestExecutionEnvelope(
                execution_status=status,
                snapshot_id="s1",
                digest_spec_hash="a" * 64,
                protocol_version="cdp-v1",
                engine_version="1.0",
                error="test error",
                summary=EngineDigestSummary(row_count=0, full_digest="x"*64, samples=[]),  # 非法
            )

    def test_success_accepts_summary(self):
        """status=SUCCESS + summary 非 None → 通过。"""
        env = DigestExecutionEnvelope(
            execution_status="SUCCESS",
            snapshot_id="s1",
            digest_spec_hash="a" * 64,
            protocol_version="cdp-v1",
            engine_version="1.0",
            summary=EngineDigestSummary(row_count=0, full_digest="0"*64, samples=[]),
        )
        assert env.execution_status == "SUCCESS"


class TestDigestSpecHash:
    def test_deterministic(self):
        spec = CreDigestSpec(
            output_columns=["id"],
            type_families=[TypeFamily.INT64],
            timezone="UTC",
            decimal_precision=[None], decimal_scale=[None], float_precision=[None],
        )
        assert compute_digest_spec_hash(spec) == compute_digest_spec_hash(spec)

    def test_returns_32_bytes(self):
        spec = CreDigestSpec(
            output_columns=["id"],
            type_families=[TypeFamily.INT64],
            timezone="UTC",
            decimal_precision=[None], decimal_scale=[None], float_precision=[None],
        )
        assert len(compute_digest_spec_hash(spec)) == 32  # 原始字节

    def test_timezone_affects_hash(self):
        s1 = CreDigestSpec(
            output_columns=["id"], type_families=[TypeFamily.INT64],
            timezone="UTC",
            decimal_precision=[None], decimal_scale=[None], float_precision=[None],
        )
        s2 = CreDigestSpec(
            output_columns=["id"], type_families=[TypeFamily.INT64],
            timezone="Asia/Shanghai",
            decimal_precision=[None], decimal_scale=[None], float_precision=[None],
        )
        assert compute_digest_spec_hash(s1) != compute_digest_spec_hash(s2)
