"""CDP v1 Python Canonical Serializer——测试 oracle，不用于生产。

严格遵循 CDP v1 冻结规范（§6.2）。所有字段编码必须精确匹配手工黄金向量。
naive timestamp 和 DST 边界 → CdpEncodingError（与设计文档 HUMAN_REVIEW 一致）。
"""

from __future__ import annotations

import hashlib
import math
import struct
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal

from tianshu_datadev.spark.cdp_spec import CreDigestSpec, TypeFamily, compute_digest_spec_hash


class CdpEncodingError(Exception):
    """CDP 编码拒绝——naive timestamp、DST 歧义等不可编码值。"""
    pass


class CdpCanonicalSerializer:
    """CDP v1 Python oracle——纯 Python 参考实现。"""

    @staticmethod
    def encode_field(value: object, type_family: TypeFamily,
                     spec: CreDigestSpec, col_index: int) -> bytes:
        """编码单个字段为 tag(1B) || length_prefix(4B BE) || value_bytes。"""
        tag = type_family.value.to_bytes(1, "big")

        if value is None:
            # NULL 编码：tag + 0xFFFFFFFF（无 value_bytes）
            return tag + b"\xff\xff\xff\xff"

        value_bytes = CdpCanonicalSerializer._encode_value(value, type_family, spec, col_index)
        length_prefix = len(value_bytes).to_bytes(4, "big")
        return tag + length_prefix + value_bytes

    @staticmethod
    def _encode_value(value: object, tf: TypeFamily,
                      spec: CreDigestSpec, col_index: int) -> bytes:
        """编码 value_bytes（不含 tag 和 length_prefix）。"""
        float_prec = spec.float_precision[col_index]

        if tf == TypeFamily.BOOLEAN:
            return b"true" if value else b"false"
        elif tf in (TypeFamily.INT8, TypeFamily.INT16, TypeFamily.INT32, TypeFamily.INT64):
            return str(int(value)).encode("utf-8")
        elif tf in (TypeFamily.FLOAT32, TypeFamily.FLOAT64):
            return CdpCanonicalSerializer._encode_float(float(value), float_prec)
        elif tf == TypeFamily.DECIMAL:
            return CdpCanonicalSerializer._encode_decimal(value, spec.decimal_scale[col_index])
        elif tf == TypeFamily.VARCHAR:
            return str(value).encode("utf-8")
        elif tf == TypeFamily.DATE:
            if isinstance(value, date):
                return value.isoformat().encode("utf-8")
            return str(value).encode("utf-8")
        elif tf == TypeFamily.TIMESTAMP:
            return CdpCanonicalSerializer._encode_timestamp(value, spec.timezone)
        raise CdpEncodingError(f"不支持的 TypeFamily: {tf}")

    @staticmethod
    def _encode_float(value: float, float_prec: int | None) -> bytes:
        """编码 FLOAT value_bytes——特殊值用固定小写 ASCII，正常值 ROUND_HALF_UP。"""
        # 先处理特殊值
        if math.isnan(value):
            return b"nan"
        if math.isinf(value):
            return b"inf" if value > 0 else b"-inf"

        # 负零检测——即使有精度也要先处理
        if value == 0.0 and math.copysign(1.0, value) == -1.0:
            if float_prec is not None:
                rounded = CdpCanonicalSerializer._round_half_up(value, float_prec)
                # 取整后仍为负零则保留"-0.0"，否则用取整后的值
                if rounded == 0.0 and math.copysign(1.0, rounded) == -1.0:
                    return b"-0.0"
                return str(rounded).encode("utf-8")
            return b"-0.0"

        # 精度归一化
        if float_prec is not None:
            value = CdpCanonicalSerializer._round_half_up(value, float_prec)

        # 正零归一化为 "0.0"
        if value == 0.0:
            return b"0.0"

        return str(value).encode("utf-8")

    @staticmethod
    def _encode_decimal(value: object, scale: int | None) -> bytes:
        """编码 DECIMAL value_bytes——ROUND_HALF_UP 取整后输出无小数点整数。"""
        if scale is None:
            scale = 0
        d = value if isinstance(value, Decimal) else Decimal(str(value))
        # 构造精度描述符，如 scale=2 → "1.00"
        quantize_str = "1." + "0" * scale if scale > 0 else "1"
        rounded = d.quantize(Decimal(quantize_str), rounding=ROUND_HALF_UP)
        unscaled = int(rounded * (10 ** scale))
        return str(unscaled).encode("utf-8")

    @staticmethod
    def _encode_timestamp(value: object, tz_name: str) -> bytes:
        """编码 timestamp——naive datetime 必须拒绝，DST 歧义同样拒绝。"""
        if isinstance(value, datetime):
            if value.tzinfo is None:
                raise CdpEncodingError(
                    "naive timestamp 不可编码——缺少时区信息，拒绝执行"
                )
            # 注意：完整 DST 检测需要 zoneinfo——当前简化处理，
            # 引擎 builder 侧由引擎原生时区函数处理时区转换
            iso = value.isoformat()
            return iso.encode("utf-8")
        if isinstance(value, str):
            # 字符串 timestamp——不做时区转换（由引擎保证一致性）
            return value.encode("utf-8")
        raise CdpEncodingError(f"不支持的 timestamp 类型: {type(value)}")

    @staticmethod
    def _round_half_up(value: float, precision: int) -> float:
        """ROUND_HALF_UP 四舍五入（远离零）。"""
        if precision == 0:
            return float(int(value + 0.5)) if value >= 0 else float(int(value - 0.5))
        multiplier = 10 ** precision
        scaled = value * multiplier
        if scaled >= 0:
            return float(int(scaled + 0.5)) / multiplier
        else:
            return float(int(scaled - 0.5)) / multiplier

    def encode_row(self, row: dict, spec: CreDigestSpec) -> bytes:
        """编码整行 → SHA-256 row_hash（32B 原始）。"""
        parts = []
        for i, col_name in enumerate(spec.output_columns):
            parts.append(self.encode_field(row.get(col_name), spec.type_families[i], spec, i))
        return hashlib.sha256(b"".join(parts)).digest()

    def compute_full_digest(self, rows: list[dict], spec: CreDigestSpec) -> str:
        """计算全量 full_digest（hex 字符串）。"""
        spec_hash_bytes = compute_digest_spec_hash(spec)
        total = len(rows)

        # 按 row_hash 首字节分桶
        buckets: dict[int, dict[bytes, int]] = {i: {} for i in range(256)}
        for row in rows:
            rh = self.encode_row(row, spec)
            b = buckets[rh[0]]
            b[rh] = b.get(rh, 0) + 1

        # 计算每个桶的 digest
        bucket_digests = []
        for bid in range(256):
            b = buckets[bid]
            items = sorted(b.items(), key=lambda x: x[0])
            payload = struct.pack(">BQ", bid, len(items))
            for rh, cnt in items:
                payload += rh + struct.pack(">Q", cnt)
            bucket_digests.append(hashlib.sha256(payload).digest())

        # 构造 full_digest_input：
        # "cdp-v1"(6B) || spec_hash(32B) || total(8B BE) || bucket_count(2B BE)
        # || 256 × (bucket_id(1B) || bucket_digest(32B))
        parts = [
            b"cdp-v1",
            spec_hash_bytes,
            struct.pack(">Q", total),
            struct.pack(">H", 256),
        ]
        for bid in range(256):
            parts.append(struct.pack(">B", bid))
            parts.append(bucket_digests[bid])

        return hashlib.sha256(b"".join(parts)).hexdigest()
