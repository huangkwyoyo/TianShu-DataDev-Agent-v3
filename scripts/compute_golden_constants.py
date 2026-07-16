"""
CDP v1 黄金常量预计算脚本——一次性运行，人工审定输出后粘贴到测试文件。

用法：python scripts/compute_golden_constants.py
输出：所有 G1-G6 的 hex 常量，供人工审定后硬编码到 test_cdp_golden_vectors.py。
"""
import hashlib
import json
import struct

# ═══════════════════════════════════════════════════════════════
# 通用：spec_hash（所有 G1-G6 使用同一 spec）
# ═══════════════════════════════════════════════════════════════
spec_json = json.dumps({
    "bucket_count": 256,
    "columns": [{"decimal_precision": None, "decimal_scale": None,
                 "float_precision": None, "name": "id", "type": "INT64"}],
    "protocol_version": "cdp-v1", "timezone": "UTC",
}, sort_keys=True, separators=(",", ":"))
spec_hash = hashlib.sha256(spec_json.encode()).digest()
spec_hash_hex = spec_hash.hex()
print(f"SPEC_HASH_HEX = '{spec_hash_hex}'")
print(f"  (len={len(spec_hash_hex)} hex chars, {len(spec_hash)} raw bytes)")
print()


# ═══════════════════════════════════════════════════════════════
# 辅助函数
# ═══════════════════════════════════════════════════════════════
def empty_bucket_payload(bucket_id: int) -> bytes:
    """空桶 payload = bucket_id || unique_hash_count=0(8B BE)"""
    return struct.pack(">BQ", bucket_id, 0)


def non_empty_bucket_payload(bucket_id: int, row_hash: bytes, occurrence: int) -> bytes:
    """非空桶 payload = bucket_id || unique_count=1(8B) || row_hash(32B) || occurrence(8B)"""
    return struct.pack(">BQ", bucket_id, 1) + row_hash + struct.pack(">Q", occurrence)


def build_full_digest_input(total_rows: int, bucket_payloads: list[bytes]) -> bytes:
    """构造 full_digest_input。

    格式： "cdp-v1" || spec_hash(32B) || total_rows(8B BE) || bucket_count=256(2B BE)
           || 256 × (bucket_id(1B) || bucket_digest(32B))
    """
    parts = [b"cdp-v1", spec_hash, struct.pack(">Q", total_rows), struct.pack(">H", 256)]
    for bid in range(256):
        parts.append(struct.pack(">B", bid))
        parts.append(hashlib.sha256(bucket_payloads[bid]).digest())
    return b"".join(parts)


# ═══════════════════════════════════════════════════════════════
# G1：空结果集（0 行）
# ═══════════════════════════════════════════════════════════════
print("=" * 60)
print("G1: 空结果集（0 行）")
print("=" * 60)

g1_bucket_payloads = [empty_bucket_payload(bid) for bid in range(256)]
g1_empty_bucket_0_digest = hashlib.sha256(g1_bucket_payloads[0]).digest()
print(f"EMPTY_BUCKET_0_PAYLOAD_HEX = '{g1_bucket_payloads[0].hex()}'")
print("  (手工已知，无需替换)")
print(f"EMPTY_BUCKET_0_DIGEST_HEX = '{g1_empty_bucket_0_digest.hex()}'")

g1_full_input = build_full_digest_input(0, g1_bucket_payloads)
assert len(g1_full_input) == 8496, f"预期 8496，实际 {len(g1_full_input)}"
g1_full_digest = hashlib.sha256(g1_full_input).hexdigest()
print(f"G1_FULL_DIGEST_INPUT_LENGTH = {len(g1_full_input)}")
print("  (手工已知 8496，只需校验)")
print(f"G1_FULL_DIGEST_HEX = '{g1_full_digest}'")
print()


# ═══════════════════════════════════════════════════════════════
# G2：单行 INT64 42
# ═══════════════════════════════════════════════════════════════
print("=" * 60)
print("G2: 单行 INT64 42")
print("=" * 60)

g2_field = bytes([0x05, 0x00, 0x00, 0x00, 0x02, 0x34, 0x32])
g2_row_hash = hashlib.sha256(g2_field).digest()
g2_bid = g2_row_hash[0]
print(f"G2_FIELD_BYTES_HEX = '{g2_field.hex()}'")
print("  (手工已知，无需替换)")
print(f"G2_ROW_HASH_HEX = '{g2_row_hash.hex()}'")
print(f"G2_BUCKET_ID = 0x{g2_bid:02X}")

g2_bucket_payload = non_empty_bucket_payload(g2_bid, g2_row_hash, 1)
print(f"G2_BUCKET_PAYLOAD_HEX = '{g2_bucket_payload.hex()}'")

g2_bucket_digest = hashlib.sha256(g2_bucket_payload).hexdigest()
print(f"G2_BUCKET_DIGEST_HEX = '{g2_bucket_digest}'")

# 构造 256 个桶的 payload 列表
g2_all_payloads = [empty_bucket_payload(bid) for bid in range(256)]
g2_all_payloads[g2_bid] = g2_bucket_payload
g2_full_input = build_full_digest_input(1, g2_all_payloads)
g2_full_digest = hashlib.sha256(g2_full_input).hexdigest()
print(f"G2_FULL_DIGEST_HEX = '{g2_full_digest}'")
print()


# ═══════════════════════════════════════════════════════════════
# G3：单行 INT64 NULL
# ═══════════════════════════════════════════════════════════════
print("=" * 60)
print("G3: 单行 INT64 NULL")
print("=" * 60)

g3_field = bytes([0x05, 0xFF, 0xFF, 0xFF, 0xFF])
g3_row_hash = hashlib.sha256(g3_field).digest()
g3_bid = g3_row_hash[0]
print(f"G3_FIELD_BYTES_HEX = '{g3_field.hex()}'")
print("  (手工已知，无需替换)")
print(f"G3_ROW_HASH_HEX = '{g3_row_hash.hex()}'")
print(f"  G3_BUCKET_ID = 0x{g3_bid:02X}")

g3_bucket_payload = non_empty_bucket_payload(g3_bid, g3_row_hash, 1)
g3_bucket_digest = hashlib.sha256(g3_bucket_payload).hexdigest()

g3_all_payloads = [empty_bucket_payload(bid) for bid in range(256)]
g3_all_payloads[g3_bid] = g3_bucket_payload
g3_full_input = build_full_digest_input(1, g3_all_payloads)
g3_full_digest = hashlib.sha256(g3_full_input).hexdigest()
print(f"G3_FULL_DIGEST_HEX = '{g3_full_digest}'")
print()


# ═══════════════════════════════════════════════════════════════
# G4：BOOLEAN true / false
# ═══════════════════════════════════════════════════════════════
print("=" * 60)
print("G4: BOOLEAN true / false")
print("=" * 60)

g4_true_field = bytes([0x01, 0x00, 0x00, 0x00, 0x04]) + b"true"
g4_false_field = bytes([0x01, 0x00, 0x00, 0x00, 0x05]) + b"false"
print(f"G4_TRUE_FIELD_BYTES_HEX = '{g4_true_field.hex()}'")
print("  (手工已知，无需替换)")
print(f"G4_FALSE_FIELD_BYTES_HEX = '{g4_false_field.hex()}'")
print("  (手工已知，无需替换)")

g4_true_hash = hashlib.sha256(g4_true_field).hexdigest()
g4_false_hash = hashlib.sha256(g4_false_field).hexdigest()
print(f"  [参考] G4_TRUE_ROW_HASH = '{g4_true_hash}'")
print(f"  [参考] G4_FALSE_ROW_HASH = '{g4_false_hash}'")
print()


# ═══════════════════════════════════════════════════════════════
# G5：FLOAT64 特殊值
# ═══════════════════════════════════════════════════════════════
print("=" * 60)
print("G5: FLOAT64 特殊值")
print("=" * 60)

g5_nan_field = bytes([0x07, 0x00, 0x00, 0x00, 0x03]) + b"nan"
g5_inf_field = bytes([0x07, 0x00, 0x00, 0x00, 0x03]) + b"inf"
g5_neg_inf_field = bytes([0x07, 0x00, 0x00, 0x00, 0x04]) + b"-inf"
g5_neg_zero_field = bytes([0x07, 0x00, 0x00, 0x00, 0x04]) + b"-0.0"
g5_pos_zero_field = bytes([0x07, 0x00, 0x00, 0x00, 0x03]) + b"0.0"

print(f"G5_NAN_FIELD_BYTES_HEX = '{g5_nan_field.hex()}'")
print("  (手工已知，无需替换)")
print(f"G5_INF_FIELD_BYTES_HEX = '{g5_inf_field.hex()}'")
print("  (手工已知，无需替换)")
print(f"G5_NEG_INF_FIELD_BYTES_HEX = '{g5_neg_inf_field.hex()}'")
print("  (手工已知，无需替换)")
print(f"G5_NEG_ZERO_FIELD_BYTES_HEX = '{g5_neg_zero_field.hex()}'")
print("  (手工已知，无需替换)")
print(f"G5_POS_ZERO_FIELD_BYTES_HEX = '{g5_pos_zero_field.hex()}'")
print("  (手工已知，无需替换)")

for name, field in [("NAN", g5_nan_field), ("INF", g5_inf_field),
                     ("NEG_INF", g5_neg_inf_field), ("NEG_ZERO", g5_neg_zero_field),
                     ("POS_ZERO", g5_pos_zero_field)]:
    h = hashlib.sha256(field).hexdigest()
    print(f"  [参考] G5_{name}_ROW_HASH = '{h}'")
print()


# ═══════════════════════════════════════════════════════════════
# G6：两行相同 INT64 42（多重集）
# ═══════════════════════════════════════════════════════════════
print("=" * 60)
print("G6: 两行相同 INT64 42（多重集）")
print("=" * 60)

# 与 G2 相同的 row_hash
g6_row_hash = g2_row_hash  # 相同字段编码 -> 相同 row_hash
g6_bid = g2_bid            # 相同桶

g6_bucket_payload = non_empty_bucket_payload(g6_bid, g6_row_hash, 2)
print(f"G6_BUCKET_PAYLOAD_HEX = '{g6_bucket_payload.hex()}'")

g6_bucket_digest = hashlib.sha256(g6_bucket_payload).hexdigest()
print(f"  G6_BUCKET_DIGEST = '{g6_bucket_digest}' (same bucket_id, different payload)")

g6_all_payloads = [empty_bucket_payload(bid) for bid in range(256)]
g6_all_payloads[g6_bid] = g6_bucket_payload
g6_full_input = build_full_digest_input(2, g6_all_payloads)
g6_full_digest = hashlib.sha256(g6_full_input).hexdigest()
print(f"G6_FULL_DIGEST_HEX = '{g6_full_digest}'")

# 验证 G6 != G2
assert g6_full_digest != g2_full_digest, "G6 full_digest 必须 ≠ G2 full_digest（多重集语义）"
print("  [OK] G6_FULL_DIGEST != G2_FULL_DIGEST（多重集语义验证通过）")
print()

# ═══════════════════════════════════════════════════════════════
# 摘要报告
# ═══════════════════════════════════════════════════════════════
print("=" * 60)
print("替换摘要（用此输出更新测试文件中的占位符）")
print("=" * 60)
print()
print(f"SPEC_HASH_HEX = '{spec_hash_hex}'")
print(f"EMPTY_BUCKET_0_DIGEST_HEX = '{g1_empty_bucket_0_digest.hex()}'")
print(f"G1_FULL_DIGEST_HEX = '{g1_full_digest}'")
print()
print(f"G2_ROW_HASH_HEX = '{g2_row_hash.hex()}'")
print(f"G2_BUCKET_ID = 0x{g2_bid:02X}")
print(f"G2_BUCKET_PAYLOAD_HEX = '{g2_bucket_payload.hex()}'")
print(f"G2_BUCKET_DIGEST_HEX = '{g2_bucket_digest}'")
print(f"G2_FULL_DIGEST_HEX = '{g2_full_digest}'")
print()
print(f"G3_ROW_HASH_HEX = '{g3_row_hash.hex()}'")
print(f"G3_BUCKET_ID = 0x{g3_bid:02X}")
print(f"G3_BUCKET_PAYLOAD_HEX = '{g3_bucket_payload.hex()}'")
print(f"G3_BUCKET_DIGEST_HEX = '{g3_bucket_digest}'")
print(f"G3_FULL_DIGEST_HEX = '{g3_full_digest}'")
print()
print(f"G6_BUCKET_PAYLOAD_HEX = '{g6_bucket_payload.hex()}'")
print(f"G6_BUCKET_DIGEST_HEX = '{g6_bucket_digest}'")
print(f"G6_FULL_DIGEST_HEX = '{g6_full_digest}'")
print()
print("=" * 60)
print("验证摘要")
print("=" * 60)
print(f"SPEC_HASH_HEX len: {len(spec_hash_hex)} (预期 64)")
print(f"G1_FULL_DIGEST input len: {len(g1_full_input)} (预期 8496)")
print(f"G1_FULL_DIGEST: {g1_full_digest}")
print(f"G2_FULL_DIGEST: {g2_full_digest}")
print(f"G3_FULL_DIGEST: {g3_full_digest}")
print(f"G6_FULL_DIGEST: {g6_full_digest}")
print(f"G6 != G2: {g6_full_digest != g2_full_digest}")
print(f"G3 != G2 (different content -> different digest): {g3_full_digest != g2_full_digest}")
print()
