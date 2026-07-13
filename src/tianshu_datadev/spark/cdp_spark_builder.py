"""Spark CDP v1 builder——生成 PySpark 脚本在子进程中执行完整 digest 计算。

使用 struct.pack Python UDF 实现精确的 4B/8B BE 编码——经 Task 4 探针验证，
Spark 内置表达式无法可靠处理 CHR(0) 和 NULL 0xFFFFFFFF。

架构模式与 DuckDB builder（cdp_duckdb_builder.py）对称：
builder 生成代码/查询 → executor 执行并收集结果。
"""
from __future__ import annotations

from tianshu_datadev.spark.cdp_spec import CreDigestSpec, TypeFamily


class SparkCdpBuilder:
    """生成完整的 PySpark 脚本——在子进程中执行 CDP v1 digest 计算。

    脚本使用 struct.pack Python UDF 实现字段编码中的 4B BE 长度前缀
    和桶聚合中的 8B BE 计数。UDF 在子进程内定义和执行，与 Spark 内置
    表达式在同一 DataFrame 管道中协同工作。
    """

    # 脚本头部——标准库和 Spark 类型导入
    _HEADER = '''# ── CDP v1 摘要计算（由 SparkCdpBuilder 生成） ──
import struct, math, hashlib, json, sys as _sys
from pyspark.sql import functions as F
from pyspark.sql.types import BinaryType, IntegerType
from decimal import Decimal'''

    _MARKER_START = "===SPARK_EXECUTOR_OUTPUT_START==="
    _MARKER_END = "===SPARK_EXECUTOR_OUTPUT_END==="

    def build_digest_script(
        self,
        spec: CreDigestSpec,
        spec_hash_hex: str,
        snapshot_id: str,
    ) -> str:
        """生成完整的 PySpark CDP digest 计算代码。

        Args:
            spec: CDP 规范——包含列名、类型族、精度等信息
            spec_hash_hex: 64 字符 spec hash hex 字符串
            snapshot_id: 快照 ID（仅用于日志/溯源）

        Returns:
            Python 代码字符串——可在 PySpark 子进程中执行。
            代码预期 inputs['result'] 已由 executor prologue 加载好。
            输出使用标准标记包裹的 JSON 行。
        """
        parts = [
            self._HEADER,
            self._build_udfs(spec),
            self._build_pipeline(spec, spec_hash_hex),
        ]
        return "\n\n\n".join(parts)

    # ── UDF 生成 ──

    def _build_udfs(self, spec: CreDigestSpec) -> str:
        """为 spec 中每个字段生成编码 UDF。"""
        udf_defs: list[str] = []
        for i, tf in enumerate(spec.type_families):
            udf_defs.append(self._make_field_udf(i, tf, spec))
        return "\n\n".join(udf_defs)

    def _make_field_udf(self, idx: int, tf: TypeFamily, spec: CreDigestSpec) -> str:
        """生成单个字段的 CDP 编码 UDF 定义。

        每个 UDF：
        - 接收一个值（Python 原生类型——Spark UDF 自动转换）
        - NULL → tag + 0xFFFFFFFF（4B BE）
        - 非 NULL → tag(1B) + 4B BE len + value_bytes(UTF-8)
        """
        tag = tf.value
        tag_escaped = f"\\x{tag:02x}"

        if tf == TypeFamily.BOOLEAN:
            return (
                f"@F.udf(BinaryType())\n"
                f"def _enc_{idx}(v):\n"
                f"    tag = b'{tag_escaped}'\n"
                f"    if v is None:\n"
                f"        return tag + b'\\xff\\xff\\xff\\xff'\n"
                f"    vb = b'true' if v else b'false'\n"
                f"    return tag + struct.pack('>I', len(vb)) + vb"
            )
        elif tf in (
            TypeFamily.INT8, TypeFamily.INT16,
            TypeFamily.INT32, TypeFamily.INT64,
        ):
            return (
                f"@F.udf(BinaryType())\n"
                f"def _enc_{idx}(v):\n"
                f"    tag = b'{tag_escaped}'\n"
                f"    if v is None:\n"
                f"        return tag + b'\\xff\\xff\\xff\\xff'\n"
                f"    vb = str(v).encode('utf-8')\n"
                f"    return tag + struct.pack('>I', len(vb)) + vb"
            )
        elif tf in (TypeFamily.FLOAT32, TypeFamily.FLOAT64):
            return self._build_float_udf(idx, tag_escaped, spec.float_precision[idx])
        elif tf == TypeFamily.DECIMAL:
            return self._build_decimal_udf(idx, tag_escaped, spec.decimal_scale[idx] or 0)
        elif tf in (TypeFamily.VARCHAR, TypeFamily.DATE, TypeFamily.TIMESTAMP):
            return (
                f"@F.udf(BinaryType())\n"
                f"def _enc_{idx}(v):\n"
                f"    tag = b'{tag_escaped}'\n"
                f"    if v is None:\n"
                f"        return tag + b'\\xff\\xff\\xff\\xff'\n"
                f"    if hasattr(v, 'isoformat'):\n"
                f"        vb = v.isoformat().encode('utf-8')\n"
                f"    else:\n"
                f"        vb = str(v).encode('utf-8')\n"
                f"    return tag + struct.pack('>I', len(vb)) + vb"
            )
        else:
            raise ValueError(f"SparkCdpBuilder: 不支持的 TypeFamily {tf}")

    def _build_float_udf(self, idx: int, tag_escaped: str, fp: int | None) -> str:
        """生成 FLOAT32/FLOAT64 UDF——含特殊值处理和可选 ROUND_HALF_UP。"""
        lines = [
            f"@F.udf(BinaryType())",
            f"def _enc_{idx}(v):",
            f"    tag = b'{tag_escaped}'",
            f"    if v is None:",
            f"        return tag + b'\\xff\\xff\\xff\\xff'",
            f"    if math.isnan(v):",
            f"        return tag + struct.pack('>I', 3) + b'nan'",
            f"    if math.isinf(v):",
            f"        value = b'inf' if v > 0 else b'-inf'",
            f"        return tag + struct.pack('>I', len(value)) + value",
        ]
        if fp is not None:
            # ROUND_HALF_UP 精度归一化（在栈上修改 v）
            multiplier = 10**fp
            lines.extend([
                f"    # ROUND_HALF_UP 精度归一化",
                f"    _mul_fp = {multiplier}",
                f"    _scaled = v * _mul_fp",
                f"    if _scaled >= 0:",
                f"        v = float(int(_scaled + 0.5)) / _mul_fp",
                f"    else:",
                f"        v = float(int(_scaled - 0.5)) / _mul_fp",
            ])
        lines.extend([
            f"    if v == 0.0 and math.copysign(1.0, v) < 0:",
            f"        return tag + struct.pack('>I', 4) + b'-0.0'",
            f"    if v == 0.0:",
            f"        return tag + struct.pack('>I', 3) + b'0.0'",
            f"    vb = str(v).encode('utf-8')",
            f"    return tag + struct.pack('>I', len(vb)) + vb",
        ])
        return "\n".join(lines)

    def _build_decimal_udf(self, idx: int, tag_escaped: str, scale: int) -> str:
        """生成 DECIMAL UDF——ROUND_HALF_UP 取整后去小数点。"""
        return (
            f"@F.udf(BinaryType())\n"
            f"def _enc_{idx}(v):\n"
            f"    tag = b'{tag_escaped}'\n"
            f"    if v is None:\n"
            f"        return tag + b'\\xff\\xff\\xff\\xff'\n"
            f"    d = Decimal(str(v))\n"
            f"    _factor = Decimal(10 ** {scale})\n"
            f"    _scaled = d * _factor\n"
            f"    _half = Decimal('0.5')\n"
            f"    if _scaled >= 0:\n"
            f"        _unscaled = int(_scaled + _half)\n"
            f"    else:\n"
            f"        _unscaled = int(_scaled - _half)\n"
            f"    vb = str(_unscaled).encode('utf-8')\n"
            f"    return tag + struct.pack('>I', len(vb)) + vb"
        )

    # ── 摘要管道 ──

    def _build_pipeline(self, spec: CreDigestSpec, spec_hash_hex: str) -> str:
        """生成 CDP 摘要计算管道——字段编码 → row_hash → 分桶 → full_digest。"""
        n_cols = len(spec.output_columns)
        col_names = spec.output_columns

        # 字段选择表达式：每个字段调用对应 UDF 并重命名
        select_lines = [
            f"    _enc_{i}(F.col('{col}')).alias('_enc_{i}')"
            for i, col in enumerate(col_names)
        ]
        select_clause = ",\n".join(select_lines)

        # F.concat 的参数：拼接所有编码字段的二进制值
        concat_items = ",\n            ".join(
            f"F.col('_enc_{i}')" for i in range(n_cols)
        )

        marker_start = self._MARKER_START
        marker_end = self._MARKER_END
        return f'''# ── CDP v1 摘要管道 ──

# 步骤 1: 加载数据（inputs 由 executor prologue 注入）
df = inputs['result']

# 步骤 2: 字段编码——逐列应用 UDF
encoded = df.select(
{select_clause}
)

# 步骤 3: 拼接编码字段并计算 row_hash（SHA256）
row_hashes = encoded.withColumn(
    '_row_hash',
    F.sha2(F.concat(
        {concat_items}
    ), 256)
)

# 步骤 4: 提取 bucket_id（SHA256 首字节 hex → int）
@F.udf(IntegerType())
def _get_bucket_id(h):
    return int(h[:2], 16) if h else 0

row_hashes = row_hashes.withColumn('_bid', _get_bucket_id(F.col('_row_hash')))

# 步骤 5: 分组——(bucket_id, row_hash, occurrence_count)
buckets = row_hashes.groupBy('_bid', '_row_hash').agg(F.count('*').alias('_cnt'))
rows = buckets.orderBy('_bid', '_row_hash').collect()

# 步骤 6: Driver 端计算 full_digest
# （收集到 driver 后做 Python 侧聚合——数据集较小，避免复杂 UDAF）
total_count = sum(r['_cnt'] for r in rows)

# 按 bucket_id 分组
buckets_dict = {{}}
for r in rows:
    bid = r['_bid']
    buckets_dict.setdefault(bid, [])
    buckets_dict[bid].append((bytes.fromhex(r['_row_hash']), r['_cnt']))

# 计算每个桶的 digest：SHA256(bucket_id || unique_count || sorted(row_hash||count)...)
bucket_digests = {{}}
for bid in range(256):
    items = sorted(buckets_dict.get(bid, []), key=lambda x: x[0])
    payload = struct.pack('>BQ', bid, len(items))
    for rh, cnt in items:
        payload += rh + struct.pack('>Q', cnt)
    bucket_digests[bid] = hashlib.sha256(payload).digest()

# 构造 full_digest：SHA256("cdp-v1" || spec_hash || total(8B) || 256(2B) || 256×(id||digest))
spec_hash_parsed = bytes.fromhex('{spec_hash_hex}')
full_parts = [
    b'cdp-v1',
    spec_hash_parsed,
    struct.pack('>Q', total_count),
    struct.pack('>H', 256),
]
for bid in range(256):
    full_parts.append(struct.pack('>B', bid))
    full_parts.append(bucket_digests[bid])

full_digest = hashlib.sha256(b''.join(full_parts)).hexdigest()

# 输出结果——使用标准标记包裹 JSON 行
print("{marker_start}")
print(json.dumps({{'full_digest': full_digest, 'row_count': total_count}}))
print("{marker_end}")
_sys.stdout.flush()'''
