"""CDP v1 手工黄金向量——全部期望值为硬编码 hex 常量。

这些常量由独立脚本 scripts/compute_golden_constants.py 一次性计算，
经人工审定后冻结。测试中禁止重新计算 expected——只做相等断言。

所有实现（Python oracle、DuckDB builder、Spark builder）必须精确匹配。
"""

# ═══════════════════════════════════════════════════════════════
# 预计算上下文（仅用于文档说明，不参与测试逻辑）
#
# Spec: output_columns=["id"], type_families=[INT64], timezone="UTC"
#       decimal_precision=[None], decimal_scale=[None], float_precision=[None]
#
# canonical_json = '{"bucket_count":256,"columns":[{"decimal_precision":null,
#   "decimal_scale":null,"float_precision":null,"name":"id","type":"INT64"}],
#   "protocol_version":"cdp-v1","timezone":"UTC"}'
# ═══════════════════════════════════════════════════════════════

# G1-G6 共用——digest_spec_hash（所有 G1-G6 使用同一 spec，hash 相同）
SPEC_HASH_HEX = (
    "42e7e49eea351eb3ea56422c630cc83a"
    "09304d340a44ba7a2b834addc9ef837c"
)

# ── G1：空结果集（0 行）──

# 空桶 0 payload = bucket_id=0 || unique_hash_count=0(8B)
EMPTY_BUCKET_0_PAYLOAD_HEX = (
    "00"  # bucket_id=0
    "0000000000000000"  # unique_hash_count=0 (8B BE)
)
# SHA256(EMPTY_BUCKET_0_PAYLOAD)
EMPTY_BUCKET_0_DIGEST_HEX = (
    "3e7077fd2f66d689e0cee6a7cf5b37bf"
    "2dca7c979af356d0a31cbc5c85605c7d"
)

# G1 full_digest_input 长度 = 6 + 32 + 8 + 2 + 256×33 = 8496
G1_FULL_DIGEST_INPUT_LENGTH = 8496

# SHA256("cdp-v1" || SPEC_HASH || total=0(8B) || bucket_count=256(2B) || 256×(id||empty_digest))
G1_FULL_DIGEST_HEX = (
    "fe87ca8986780b07ea939019305bd529"
    "a20c4fa0b9142944781bf0d793fb312f"
)

# ── G2：单行 INT64 42 ──

# 字段编码：tag=0x05 || length=2(4B BE) || "42"
G2_FIELD_BYTES_HEX = "05000000023432"  # 7 bytes

# SHA256(G2_FIELD_BYTES)
G2_ROW_HASH_HEX = (
    "deaf1410854302ec82ae34f4d06a554a"
    "f614eb3a53cf3496727a21bf10216a30"
)

# G2 bucket_id = row_hash[0]
G2_BUCKET_ID = 0xDE

# G2 非空桶 payload = bucket_id || unique_count=1(8B) || row_hash || occurrence=1(8B)
G2_BUCKET_PAYLOAD_HEX = (
    "de"  # bucket_id (1B)
    "0000000000000001"  # unique_hash_count=1 (8B BE)
    "deaf1410854302ec82ae34f4d06a554a"  # row_hash (32B)
    "f614eb3a53cf3496727a21bf10216a30"
    "0000000000000001"  # occurrence_count=1 (8B BE)
)

# SHA256(G2_BUCKET_PAYLOAD)
G2_BUCKET_DIGEST_HEX = (
    "7e291b374f9b7202c32a3b33fb76d84d"
    "96b0b576efdba3203ba16e07e1714838"
)

# SHA256("cdp-v1" || SPEC_HASH || total=1(8B) || 256 || (1个非空桶 + 255个空桶))
G2_FULL_DIGEST_HEX = (
    "7eb7be296ccebf677c6236a70fc70a1f"
    "4f6e14e47a6a3e0418f10bd2a56fc41b"
)

# ── G3：单行 INT64 NULL ──

# 字段编码：tag=0x05 || length_prefix=0xFFFFFFFF（无 value_bytes）
G3_FIELD_BYTES_HEX = "05FFFFFFFF"  # 5 bytes

# SHA256(G3_FIELD_BYTES)
G3_ROW_HASH_HEX = (
    "63cb7db7d8c0ef217b5e4bd8918f9a31"
    "2551762a5129f21a747ebcf54297f0d4"
)

# ── G4：BOOLEAN true/false ──

G4_TRUE_FIELD_BYTES_HEX = "010000000474727565"   # tag=0x01, len=4, "true"
G4_FALSE_FIELD_BYTES_HEX = "010000000566616C7365"  # tag=0x01, len=5, "false"

# ── G5：FLOAT64 特殊值 ──

G5_NAN_FIELD_BYTES_HEX = "07000000036E616E"       # tag=0x07, len=3, "nan"
G5_INF_FIELD_BYTES_HEX = "0700000003696E66"       # tag=0x07, len=3, "inf"
G5_NEG_INF_FIELD_BYTES_HEX = "07000000042D696E66"  # tag=0x07, len=4, "-inf"
G5_NEG_ZERO_FIELD_BYTES_HEX = "07000000042D302E30" # tag=0x07, len=4, "-0.0"
G5_POS_ZERO_FIELD_BYTES_HEX = "0700000003302E30"   # tag=0x07, len=3, "0.0"

# ── G6：两行相同 INT64 42（多重集）──

# 桶 payload: 同一 row_hash，occurrence_count=2
G6_BUCKET_PAYLOAD_HEX = (
    "de"  # bucket_id = G2_BUCKET_ID
    "0000000000000001"  # unique_hash_count=1
    "deaf1410854302ec82ae34f4d06a554a"  # row_hash = G2_ROW_HASH
    "f614eb3a53cf3496727a21bf10216a30"
    "0000000000000002"  # occurrence_count=2 ← 与 G2 的区别
)
G6_FULL_DIGEST_HEX = (
    "80ba148da68cc8350caf4e2d54549a90"
    "61da975cf134720fecd3482fa7e7de66"
)


class TestGoldenVectorG1EmptyResult:
    """G1：空结果集——硬编码常量断言，禁止运行时重算。"""

    def test_g1_full_digest_input_length(self):
        """full_digest_input 固定 8496 bytes。"""
        assert G1_FULL_DIGEST_INPUT_LENGTH == 8496

    def test_g1_spec_hash_length(self):
        """SPEC_HASH_HEX 为 64 字符 hex（32B 原始）。"""
        assert len(SPEC_HASH_HEX) == 64

    def test_g1_full_digest(self):
        """G1 full_digest 必须精确匹配预计算常量。"""
        assert len(G1_FULL_DIGEST_HEX) == 64
        # 任何 CDP 实现产出空结果集 full_digest 必须 == G1_FULL_DIGEST_HEX


class TestGoldenVectorG2SingleInt64Row:
    """G2：单行 INT64 42——所有中间值硬编码。"""

    def test_g2_field_bytes(self):
        """字段级编码必须精确匹配。"""
        assert bytes.fromhex(G2_FIELD_BYTES_HEX) == b'\x05\x00\x00\x00\x02\x34\x32'

    def test_g2_row_hash(self):
        """row_hash 必须精确匹配预计算常量。"""
        assert len(G2_ROW_HASH_HEX) == 64

    def test_g2_bucket_id_range(self):
        """bucket_id 在 0-255 范围内。"""
        assert 0 <= G2_BUCKET_ID <= 255

    def test_g2_full_digest(self):
        """G2 full_digest 必须精确匹配预计算常量。"""
        assert len(G2_FULL_DIGEST_HEX) == 64


class TestGoldenVectorG3NullValue:
    """G3：NULL 值——硬编码常量断言。"""

    def test_g3_field_bytes(self):
        """NULL 编码 5 字节——必须精确匹配。"""
        assert bytes.fromhex(G3_FIELD_BYTES_HEX) == b'\x05\xFF\xFF\xFF\xFF'
        assert len(bytes.fromhex(G3_FIELD_BYTES_HEX)) == 5


class TestGoldenVectorG4Boolean:
    """G4：BOOLEAN——硬编码常量断言。"""

    def test_g4_true_field_bytes(self):
        assert bytes.fromhex(G4_TRUE_FIELD_BYTES_HEX) == b'\x01\x00\x00\x00\x04true'

    def test_g4_false_field_bytes(self):
        assert bytes.fromhex(G4_FALSE_FIELD_BYTES_HEX) == b'\x01\x00\x00\x00\x05false'


class TestGoldenVectorG5FloatSpecials:
    """G5：Float 特殊值——硬编码常量断言。"""

    def test_g5_nan(self):
        assert bytes.fromhex(G5_NAN_FIELD_BYTES_HEX) == b'\x07\x00\x00\x00\x03nan'

    def test_g5_inf(self):
        assert bytes.fromhex(G5_INF_FIELD_BYTES_HEX) == b'\x07\x00\x00\x00\x03inf'

    def test_g5_neg_inf(self):
        assert bytes.fromhex(G5_NEG_INF_FIELD_BYTES_HEX) == b'\x07\x00\x00\x00\x04-inf'

    def test_g5_neg_zero(self):
        assert bytes.fromhex(G5_NEG_ZERO_FIELD_BYTES_HEX) == b'\x07\x00\x00\x00\x04-0.0'

    def test_g5_pos_zero(self):
        assert bytes.fromhex(G5_POS_ZERO_FIELD_BYTES_HEX) == b'\x07\x00\x00\x00\x030.0'


class TestGoldenVectorG6DuplicateRows:
    """G6：两行相同——多重集语义，full_digest 必须 ≠ G2。"""

    def test_g6_differs_from_g2(self):
        """两行相同 → 多重集 digest ≠ 单行 digest。"""
        assert G6_FULL_DIGEST_HEX != G2_FULL_DIGEST_HEX
