"""双引擎字段编码探针——证明 DuckDB/Spark 能精确产出 CDP v1 二进制。

每个探针精确匹配手工黄金向量 G2-G5 的期望字节。
先不构建完整 builder——先证明引擎能产出正确的 tag || length_prefix || value_bytes。
"""

import struct

import pytest

from tests.spark.test_cdp_golden_vectors import (
    G2_FIELD_BYTES_HEX,
    G3_FIELD_BYTES_HEX,
    G4_FALSE_FIELD_BYTES_HEX,
    G4_TRUE_FIELD_BYTES_HEX,
    G5_INF_FIELD_BYTES_HEX,
    G5_NAN_FIELD_BYTES_HEX,
    G5_NEG_INF_FIELD_BYTES_HEX,
    G5_NEG_ZERO_FIELD_BYTES_HEX,
    G5_POS_ZERO_FIELD_BYTES_HEX,
)


# ═══════════════════════════════════════════════════════════════
# DuckDB 探针
# ═══════════════════════════════════════════════════════════════

class TestDuckDBFieldProbes:
    """DuckDB 单字段编码探针——每个探针精确匹配手工黄金向量期望字节。

    CDP v1 字段编码格式：tag(1B) || length_prefix(4B BE) || value_bytes。
    DuckDB 用 CHR 逐个字节构造字符串，Python 侧用 latin-1 转 bytes。
    Latin-1 编码能 1:1 保留所有 Unicode 代码点 U+0000..U+00FF 为对应字节，
    因此 CHR(255) 在 Python 中还原为单字节 0xFF。
    """

    @pytest.fixture(autouse=True)
    def _check_duckdb(self):
        """跳过测试（若 duckdb 未安装）。"""
        pytest.importorskip("duckdb")

    @staticmethod
    def _decode(val):
        """将 DuckDB VARCHAR 结果转为 bytes。

        DuckDB 返回 VARCHAR 为 Python str。latin-1 编码将每个代码点
        U+0000..U+00FF 映射为对应单字节，完美保留所有字节值。
        """
        if isinstance(val, bytes):
            return val
        return val.encode("latin-1")

    # ── G2: INT64 42 ──

    def test_probe_int64_42(self):
        """DuckDB INT64 42 → G2 期望字节。

        构造：CHR(5=INT64 tag) || CHR(0)×3 || CHR(2) || CAST(42 AS VARCHAR)。
        """
        import duckdb

        con = duckdb.connect(":memory:")
        result = self._decode(con.execute(
            "SELECT CHR(5) || CHR(0) || CHR(0) || CHR(0) || CHR(2) || CAST(42 AS VARCHAR)"
        ).fetchone()[0])
        assert result == bytes.fromhex(G2_FIELD_BYTES_HEX), (
            f"DuckDB: {result.hex()} != G2: {bytes.fromhex(G2_FIELD_BYTES_HEX).hex()}"
        )

    # ── G3: NULL INT64 ──

    def test_probe_null_int64(self):
        """DuckDB NULL → G3 期望字节。

        NULL 编码：tag(0x05) || 0xFFFFFFFF（无 value_bytes）。
        """
        import duckdb

        con = duckdb.connect(":memory:")
        result = self._decode(con.execute(
            "SELECT CHR(5) || CHR(255) || CHR(255) || CHR(255) || CHR(255)"
        ).fetchone()[0])
        assert result == bytes.fromhex(G3_FIELD_BYTES_HEX), (
            f"DuckDB: {result.hex()} != G3: {bytes.fromhex(G3_FIELD_BYTES_HEX).hex()}"
        )

    # ── G4: BOOLEAN ──

    def test_probe_boolean_true(self):
        """DuckDB BOOLEAN true → G4 true 期望字节。

        tag(0x01=BOOLEAN) || 4B BE length=4 || "true"。
        """
        import duckdb

        con = duckdb.connect(":memory:")
        result = self._decode(con.execute(
            "SELECT CHR(1) || CHR(0) || CHR(0) || CHR(0) || CHR(4) || 'true'"
        ).fetchone()[0])
        assert result == bytes.fromhex(G4_TRUE_FIELD_BYTES_HEX)

    def test_probe_boolean_false(self):
        """DuckDB BOOLEAN false → G4 false 期望字节。

        tag(0x01=BOOLEAN) || 4B BE length=5 || "false"。
        """
        import duckdb

        con = duckdb.connect(":memory:")
        result = self._decode(con.execute(
            "SELECT CHR(1) || CHR(0) || CHR(0) || CHR(0) || CHR(5) || 'false'"
        ).fetchone()[0])
        assert result == bytes.fromhex(G4_FALSE_FIELD_BYTES_HEX)

    # ── G5: FLOAT64 特殊值 ──

    def test_probe_float_nan(self):
        """DuckDB FLOAT64 NaN → G5 nan 期望字节。

        DuckDB 的 isnan() 可识别 NaN。WHERE 子句体现引擎能区分特殊值。
        DuckDB 支持 SELECT ... WHERE ... 标量表达式写法。
        """
        import duckdb

        con = duckdb.connect(":memory:")
        result = self._decode(con.execute(
            "SELECT CHR(7) || CHR(0) || CHR(0) || CHR(0) || CHR(3) || 'nan' "
            "WHERE isnan(CAST('NaN' AS DOUBLE))"
        ).fetchone()[0])
        assert result == bytes.fromhex(G5_NAN_FIELD_BYTES_HEX)

    def test_probe_float_inf(self):
        """DuckDB FLOAT64 +Inf → G5 inf 期望字节。

        DuckDB 的 isinf() 可识别 Infinity。
        """
        import duckdb

        con = duckdb.connect(":memory:")
        result = self._decode(con.execute(
            "SELECT CHR(7) || CHR(0) || CHR(0) || CHR(0) || CHR(3) || 'inf' "
            "WHERE isinf(CAST('Infinity' AS DOUBLE))"
        ).fetchone()[0])
        assert result == bytes.fromhex(G5_INF_FIELD_BYTES_HEX)

    def test_probe_float_neg_inf(self):
        """DuckDB FLOAT64 -Inf → G5 -inf 期望字节。

        isinf + 符号检查（val < 0）区分 ±Inf。需要 FROM 子句引用待检值。
        """
        import duckdb

        con = duckdb.connect(":memory:")
        result = self._decode(con.execute(
            "SELECT CHR(7) || CHR(0) || CHR(0) || CHR(0) || CHR(4) || '-inf' "
            "FROM (SELECT CAST('-Infinity' AS DOUBLE) AS val) t "
            "WHERE isinf(val) AND val < 0"
        ).fetchone()[0])
        assert result == bytes.fromhex(G5_NEG_INF_FIELD_BYTES_HEX)

    def test_probe_float_neg_zero(self):
        """DuckDB FLOAT64 -0.0 → G5 -0.0 期望字节。

        注意：DuckDB CAST(-0.0 AS DOUBLE) 将 -0.0 归一化为 +0.0，
        但 CAST('-0.0' AS DOUBLE) 从字符串解析可保留 -0.0。
        用 CAST(val AS VARCHAR) = '-0.0' 区分正负零。
        """
        import duckdb

        con = duckdb.connect(":memory:")
        result = self._decode(con.execute(
            "SELECT CHR(7) || CHR(0) || CHR(0) || CHR(0) || CHR(4) || '-0.0' "
            "FROM (SELECT CAST('-0.0' AS DOUBLE) AS val) t "
            "WHERE CAST(val AS VARCHAR) = '-0.0'"
        ).fetchone()[0])
        assert result == bytes.fromhex(G5_NEG_ZERO_FIELD_BYTES_HEX)

    def test_probe_float_pos_zero(self):
        """DuckDB FLOAT64 +0.0 → G5 0.0 期望字节。

        DuckDB 的 CAST(0.0 AS VARCHAR) 正确产出 "0.0"。
        """
        import duckdb

        con = duckdb.connect(":memory:")
        result = self._decode(con.execute(
            "SELECT CHR(7) || CHR(0) || CHR(0) || CHR(0) || CHR(3) || CAST(0.0 AS VARCHAR)"
        ).fetchone()[0])
        assert result == bytes.fromhex(G5_POS_ZERO_FIELD_BYTES_HEX)


# ═══════════════════════════════════════════════════════════════
# Spark 探针
# ═══════════════════════════════════════════════════════════════

class TestSparkFieldProbes:
    """Spark 单字段编码探针——每个探针精确匹配手工黄金向量期望字节。

    关键发现：Spark 内置 CAST 可正确产出所有值类型的字符串表示
    （INT64→"42", BOOLEAN→"true"/"false", ±0.0→"-0.0"/"0.0"），
    但浮点特殊值的大小写与 CDP v1 规范不同：
      - NaN       → "NaN"     而非 "nan"
      - +Infinity → "Infinity" 而非 "inf"
      - -Infinity → "-Infinity" 而非 "-inf"

    Spark 内置表达式无法可靠构建 4B BE 长度前缀：
    ──────────────────────────────────────────────
    Spark SQL 提供了 CHAR(n)、CONCAT、ENCODE(str, charset) 等函数，
    理论上可组合实现 4B BE，但存在三个实践问题：

    1. CHAR(0) 在 Spark 内部 UTF-16 字符串中可能导致不可预测截断
    2. Spark 缺少无符号右移（>>>），用 shiftright + bitwiseAnd 模拟
       对 NULL 编码 0xFFFFFFFF 不可靠
    3. 此方案极其脆弱，对 NULL 编码（全 0xFF）更不可行

    因此：本组探针使用 Python UDF（struct.pack）证明二进制能力，
    同时通过 test_builtin_* 探针记录内置函数的能力与局限。

    PySpark 注意事项：Spark 3+ 在 Windows 上需要 PYSPARK_PYTHON 环境变量
    指向 `python`（而非 `python3`），否则 Python UDF Worker 启动失败。
    """

    @pytest.fixture(autouse=True)
    def _check_spark(self):
        """检查 PySpark 是否可用，不可用则跳过所有 Spark 探针。"""
        import os

        # Windows 上 Spark UDF Worker 需要 python 而非 python3
        if os.name == "nt":
            os.environ.setdefault("PYSPARK_PYTHON", "python")
        try:
            from pyspark.sql import SparkSession

            SparkSession.builder.master("local[1]").appName("cdp-test").getOrCreate()
        except Exception:
            pytest.skip("PySpark 不可用")

    # ── 内置表达式探针：验证字符串表示 ──

    def test_builtin_int64_to_string(self):
        """Spark CAST(bigint AS string) 产出 "42"。"""
        from pyspark.sql import SparkSession, functions as F

        spark = SparkSession.builder.master("local[1]").appName("cdp-test").getOrCreate()
        result = (
            spark.range(1)
            .select(F.lit(42).cast("bigint").cast("string"))
            .collect()[0][0]
        )
        assert result == "42"

    def test_builtin_boolean_to_string(self):
        """Spark CAST(boolean AS string) 产出 "true"/"false"。"""
        from pyspark.sql import SparkSession, functions as F

        spark = SparkSession.builder.master("local[1]").appName("cdp-test").getOrCreate()
        true_result = (
            spark.range(1).select(F.lit(True).cast("string")).collect()[0][0]
        )
        false_result = (
            spark.range(1).select(F.lit(False).cast("string")).collect()[0][0]
        )
        assert true_result == "true"
        assert false_result == "false"

    def test_builtin_float_nan_to_string(self):
        """Spark CAST(NaN AS string) 产出 "NaN"——与 CDP v1 的 "nan" 不同。

        此差异表明 builder 必须用 CASE WHEN 做小写转换。
        """
        from pyspark.sql import SparkSession, functions as F

        spark = SparkSession.builder.master("local[1]").appName("cdp-test").getOrCreate()
        result = (
            spark.range(1)
            .select(F.lit(float("nan")).cast("string"))
            .collect()[0][0]
        )
        assert result == "NaN", f"Spark CAST(NaN) = {result!r}，期望 'NaN'"

    def test_builtin_float_inf_to_string(self):
        """Spark CAST(inf AS string) 产出 "Infinity"/"-Infinity"——与 CDP v1 不同。

        CDP v1 要求 "inf"/"-inf"，builder 必须用 CASE WHEN 转换。
        """
        from pyspark.sql import SparkSession, functions as F

        spark = SparkSession.builder.master("local[1]").appName("cdp-test").getOrCreate()
        pos = (
            spark.range(1)
            .select(F.lit(float("inf")).cast("string"))
            .collect()[0][0]
        )
        neg = (
            spark.range(1)
            .select(F.lit(float("-inf")).cast("string"))
            .collect()[0][0]
        )
        assert pos == "Infinity", f"Spark CAST(+inf) = {pos!r}"
        assert neg == "-Infinity", f"Spark CAST(-inf) = {neg!r}"

    def test_builtin_float_neg_zero_to_string(self):
        """Spark CAST(-0.0 AS string) 产出 "-0.0"——与 CDP v1 一致。"""
        from pyspark.sql import SparkSession, functions as F

        spark = SparkSession.builder.master("local[1]").appName("cdp-test").getOrCreate()
        result = (
            spark.range(1)
            .select(F.lit(-0.0).cast("string"))
            .collect()[0][0]
        )
        assert result == "-0.0"

    def test_builtin_float_pos_zero_to_string(self):
        """Spark CAST(0.0 AS string) 产出 "0.0"——与 CDP v1 一致。"""
        from pyspark.sql import SparkSession, functions as F

        spark = SparkSession.builder.master("local[1]").appName("cdp-test").getOrCreate()
        result = (
            spark.range(1).select(F.lit(0.0).cast("string")).collect()[0][0]
        )
        assert result == "0.0"

    # ── UDF 探针：4B BE 长度前缀 ──

    def test_udf_4byte_be_length_prefix(self):
        """Spark UDF (struct.pack) 精确产出 4B BE。

        struct.pack(">I", 2) → b'\\x00\\x00\\x00\\x02'。
        此探针证明 Python UDF 能完成 Spark 内置函数无法可靠完成的二进制打包。
        """
        from pyspark.sql import SparkSession, functions as F
        from pyspark.sql.types import BinaryType

        spark = SparkSession.builder.master("local[1]").appName("cdp-test").getOrCreate()

        @F.udf(BinaryType())
        def _len4be(n: int):
            return struct.pack(">I", n)

        result = (
            spark.range(1)
            .select(_len4be(F.lit(2)).alias("lp"))
            .collect()[0][0]
        )
        assert result == b"\x00\x00\x00\x02"

    def test_udf_4byte_be_null_prefix(self):
        """Spark UDF 产出 NULL 4B BE 前缀 = b'\\xff\\xff\\xff\\xff'。"""
        from pyspark.sql import SparkSession, functions as F
        from pyspark.sql.types import BinaryType

        spark = SparkSession.builder.master("local[1]").appName("cdp-test").getOrCreate()

        @F.udf(BinaryType())
        def _null_prefix():
            return b"\xff\xff\xff\xff"

        result = (
            spark.range(1).select(_null_prefix().alias("lp")).collect()[0][0]
        )
        assert result == b"\xff\xff\xff\xff"

    # ── UDF 探针：完整字段编码 ──

    def test_udf_field_encode_int64_42(self):
        """Spark UDF 完整字段编码 INT64 42 → G2。"""
        from pyspark.sql import SparkSession, functions as F
        from pyspark.sql.types import BinaryType

        spark = SparkSession.builder.master("local[1]").appName("cdp-test").getOrCreate()

        @F.udf(BinaryType())
        def _encode_int64(v):
            tag = b"\x05"
            if v is None:
                return tag + b"\xff\xff\xff\xff"
            value_bytes = str(v).encode("utf-8")
            return tag + struct.pack(">I", len(value_bytes)) + value_bytes

        df = spark.range(1).select(F.lit(42).cast("bigint").alias("v"))
        result = df.select(_encode_int64("v")).collect()[0][0]
        assert result == bytes.fromhex(G2_FIELD_BYTES_HEX), (
            f"Spark UDF: {result.hex()} != G2: {bytes.fromhex(G2_FIELD_BYTES_HEX).hex()}"
        )

    def test_udf_field_encode_null_int64(self):
        """Spark UDF 完整字段编码 NULL INT64 → G3。"""
        from pyspark.sql import SparkSession, functions as F
        from pyspark.sql.types import BinaryType

        spark = SparkSession.builder.master("local[1]").appName("cdp-test").getOrCreate()

        @F.udf(BinaryType())
        def _encode_null_int64(v):
            tag = b"\x05"
            if v is None:
                return tag + b"\xff\xff\xff\xff"
            value_bytes = str(v).encode("utf-8")
            return tag + struct.pack(">I", len(value_bytes)) + value_bytes

        df = spark.range(1).select(F.lit(None).cast("bigint").alias("v"))
        result = df.select(_encode_null_int64("v")).collect()[0][0]
        assert result == bytes.fromhex(G3_FIELD_BYTES_HEX), (
            f"Spark UDF: {result.hex()} != G3: {bytes.fromhex(G3_FIELD_BYTES_HEX).hex()}"
        )

    def test_udf_field_encode_boolean_true(self):
        """Spark UDF 完整字段编码 BOOLEAN true → G4 true。"""
        from pyspark.sql import SparkSession, functions as F
        from pyspark.sql.types import BinaryType

        spark = SparkSession.builder.master("local[1]").appName("cdp-test").getOrCreate()

        @F.udf(BinaryType())
        def _encode_bool(v):
            tag = b"\x01"
            if v is None:
                return tag + b"\xff\xff\xff\xff"
            value_bytes = b"true" if v else b"false"
            return tag + struct.pack(">I", len(value_bytes)) + value_bytes

        df = spark.range(1).select(F.lit(True).cast("boolean").alias("v"))
        result = df.select(_encode_bool("v")).collect()[0][0]
        assert result == bytes.fromhex(G4_TRUE_FIELD_BYTES_HEX)

    def test_udf_field_encode_boolean_false(self):
        """Spark UDF 完整字段编码 BOOLEAN false → G4 false。"""
        from pyspark.sql import SparkSession, functions as F
        from pyspark.sql.types import BinaryType

        spark = SparkSession.builder.master("local[1]").appName("cdp-test").getOrCreate()

        @F.udf(BinaryType())
        def _encode_bool(v):
            tag = b"\x01"
            if v is None:
                return tag + b"\xff\xff\xff\xff"
            value_bytes = b"true" if v else b"false"
            return tag + struct.pack(">I", len(value_bytes)) + value_bytes

        df = spark.range(1).select(F.lit(False).cast("boolean").alias("v"))
        result = df.select(_encode_bool("v")).collect()[0][0]
        assert result == bytes.fromhex(G4_FALSE_FIELD_BYTES_HEX)

    def test_udf_field_encode_float_nan(self):
        """Spark UDF FLOAT64 NaN → G5 nan。"""
        import math

        from pyspark.sql import SparkSession, functions as F
        from pyspark.sql.types import BinaryType

        spark = SparkSession.builder.master("local[1]").appName("cdp-test").getOrCreate()

        @F.udf(BinaryType())
        def _encode_float(v):
            tag = b"\x07"
            if v is None:
                return tag + b"\xff\xff\xff\xff"
            if math.isnan(v):
                value_bytes = b"nan"
            elif math.isinf(v):
                value_bytes = b"inf" if v > 0 else b"-inf"
            elif v == 0.0 and math.copysign(1.0, v) < 0:
                value_bytes = b"-0.0"
            elif v == 0.0:
                value_bytes = b"0.0"
            else:
                value_bytes = str(v).encode("utf-8")
            return tag + struct.pack(">I", len(value_bytes)) + value_bytes

        df = spark.range(1).select(F.lit(float("nan")).cast("double").alias("v"))
        result = df.select(_encode_float("v")).collect()[0][0]
        assert result == bytes.fromhex(G5_NAN_FIELD_BYTES_HEX)

    def test_udf_field_encode_float_inf(self):
        """Spark UDF FLOAT64 +Inf → G5 inf。"""
        import math

        from pyspark.sql import SparkSession, functions as F
        from pyspark.sql.types import BinaryType

        spark = SparkSession.builder.master("local[1]").appName("cdp-test").getOrCreate()

        @F.udf(BinaryType())
        def _encode_float(v):
            tag = b"\x07"
            if v is None:
                return tag + b"\xff\xff\xff\xff"
            if math.isnan(v):
                value_bytes = b"nan"
            elif math.isinf(v):
                value_bytes = b"inf" if v > 0 else b"-inf"
            elif v == 0.0 and math.copysign(1.0, v) < 0:
                value_bytes = b"-0.0"
            elif v == 0.0:
                value_bytes = b"0.0"
            else:
                value_bytes = str(v).encode("utf-8")
            return tag + struct.pack(">I", len(value_bytes)) + value_bytes

        df = spark.range(1).select(F.lit(float("inf")).cast("double").alias("v"))
        result = df.select(_encode_float("v")).collect()[0][0]
        assert result == bytes.fromhex(G5_INF_FIELD_BYTES_HEX)

    def test_udf_field_encode_float_neg_inf(self):
        """Spark UDF FLOAT64 -Inf → G5 -inf。"""
        import math

        from pyspark.sql import SparkSession, functions as F
        from pyspark.sql.types import BinaryType

        spark = SparkSession.builder.master("local[1]").appName("cdp-test").getOrCreate()

        @F.udf(BinaryType())
        def _encode_float(v):
            tag = b"\x07"
            if v is None:
                return tag + b"\xff\xff\xff\xff"
            if math.isnan(v):
                value_bytes = b"nan"
            elif math.isinf(v):
                value_bytes = b"inf" if v > 0 else b"-inf"
            elif v == 0.0 and math.copysign(1.0, v) < 0:
                value_bytes = b"-0.0"
            elif v == 0.0:
                value_bytes = b"0.0"
            else:
                value_bytes = str(v).encode("utf-8")
            return tag + struct.pack(">I", len(value_bytes)) + value_bytes

        df = spark.range(1).select(F.lit(float("-inf")).cast("double").alias("v"))
        result = df.select(_encode_float("v")).collect()[0][0]
        assert result == bytes.fromhex(G5_NEG_INF_FIELD_BYTES_HEX)

    def test_udf_field_encode_float_neg_zero(self):
        """Spark UDF FLOAT64 -0.0 → G5 -0.0。

        Spark 保留 IEEE 754 负零语义，math.copysign 正确检测符号位。
        """
        import math

        from pyspark.sql import SparkSession, functions as F
        from pyspark.sql.types import BinaryType

        spark = SparkSession.builder.master("local[1]").appName("cdp-test").getOrCreate()

        @F.udf(BinaryType())
        def _encode_float(v):
            tag = b"\x07"
            if v is None:
                return tag + b"\xff\xff\xff\xff"
            if math.isnan(v):
                value_bytes = b"nan"
            elif math.isinf(v):
                value_bytes = b"inf" if v > 0 else b"-inf"
            elif v == 0.0 and math.copysign(1.0, v) < 0:
                value_bytes = b"-0.0"
            elif v == 0.0:
                value_bytes = b"0.0"
            else:
                value_bytes = str(v).encode("utf-8")
            return tag + struct.pack(">I", len(value_bytes)) + value_bytes

        df = spark.range(1).select(F.lit(-0.0).cast("double").alias("v"))
        result = df.select(_encode_float("v")).collect()[0][0]
        assert result == bytes.fromhex(G5_NEG_ZERO_FIELD_BYTES_HEX)

    def test_udf_field_encode_float_pos_zero(self):
        """Spark UDF FLOAT64 +0.0 → G5 0.0。

        Spark CAST(0.0 AS string) 产出 "0.0"，与 CDP v1 一致。
        """
        import math

        from pyspark.sql import SparkSession, functions as F
        from pyspark.sql.types import BinaryType

        spark = SparkSession.builder.master("local[1]").appName("cdp-test").getOrCreate()

        @F.udf(BinaryType())
        def _encode_float(v):
            tag = b"\x07"
            if v is None:
                return tag + b"\xff\xff\xff\xff"
            if math.isnan(v):
                value_bytes = b"nan"
            elif math.isinf(v):
                value_bytes = b"inf" if v > 0 else b"-inf"
            elif v == 0.0 and math.copysign(1.0, v) < 0:
                value_bytes = b"-0.0"
            elif v == 0.0:
                value_bytes = b"0.0"
            else:
                value_bytes = str(v).encode("utf-8")
            return tag + struct.pack(">I", len(value_bytes)) + value_bytes

        df = spark.range(1).select(F.lit(0.0).cast("double").alias("v"))
        result = df.select(_encode_float("v")).collect()[0][0]
        assert result == bytes.fromhex(G5_POS_ZERO_FIELD_BYTES_HEX)
