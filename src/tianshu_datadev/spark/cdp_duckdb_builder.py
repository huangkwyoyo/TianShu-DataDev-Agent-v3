"""DuckDB CDP v1 builder——生成完整 CDP 摘要查询。

所有中间值使用 hex 字符串，仅在 SHA256 前用 UNHEX 转为 BLOB。
这是唯一正确的方式——DuckDB 的 CHR+VARCHAR 到 BLOB 的转换对非 ASCII
字节会失败。
"""
from __future__ import annotations

from tianshu_datadev.spark.cdp_spec import CreDigestSpec, TypeFamily


class DuckdbCdpBuilder:
    """生成 DuckDB SQL 查询——在引擎内部完成全部 CDP 计算。

    策略：所有中间值使用 hex 字符串，UNHEX 仅用于 SHA256 输入。
    避免了 DuckDB VARCHAR→BLOB 转换对非 ASCII 字节的限制。
    """

    # "cdp-v1" 的 hex 编码（6 字节 → 12 hex 字符）
    _CDP_V1_HEX = "6364702d7631"

    # ── 整数到 hex 的位运算辅助 ──

    @staticmethod
    def _extract_bucket_id(hex_expr: str) -> str:
        """从 SHA256 hex 字符串提取首字节作为 bucket_id (0-255)。

        DuckDB 1.5.3 无 GET_BYTE 函数，改用 CAST('0x' || SUBSTR(hex, 1, 2) AS INTEGER)。
        此方法处理大小写混合 hex（SHA256 返回小写，LPAD(HEX(...)) 返回大写）。
        """
        return (
            f"CAST('0x' || SUBSTR({hex_expr}, 1, 2) AS INTEGER)"
        )

    @staticmethod
    def _hex_int1be(expr: str) -> str:
        """单字节(1B BE)整数 → 2 字符 hex。"""
        return f"LPAD(HEX(({expr}) & 255), 2, '0')"

    @staticmethod
    def _hex_int2be(expr: str) -> str:
        """双字节(2B BE)整数 → 4 字符 hex。"""
        return (
            f"LPAD(HEX((({expr}) >> 8) & 255), 2, '0') || "
            f"LPAD(HEX(({expr}) & 255), 2, '0')"
        )

    @staticmethod
    def _hex_int4be(expr: str) -> str:
        """四字节(4B BE)整数 → 8 字符 hex。"""
        return (
            f"LPAD(HEX((({expr}) >> 24) & 255), 2, '0') || "
            f"LPAD(HEX((({expr}) >> 16) & 255), 2, '0') || "
            f"LPAD(HEX((({expr}) >> 8) & 255), 2, '0') || "
            f"LPAD(HEX(({expr}) & 255), 2, '0')"
        )

    @staticmethod
    def _hex_int8be(expr: str) -> str:
        """八字节(8B BE)整数 → 16 字符 hex。"""
        return (
            f"LPAD(HEX((({expr}) >> 56) & 255), 2, '0') || "
            f"LPAD(HEX((({expr}) >> 48) & 255), 2, '0') || "
            f"LPAD(HEX((({expr}) >> 40) & 255), 2, '0') || "
            f"LPAD(HEX((({expr}) >> 32) & 255), 2, '0') || "
            f"LPAD(HEX((({expr}) >> 24) & 255), 2, '0') || "
            f"LPAD(HEX((({expr}) >> 16) & 255), 2, '0') || "
            f"LPAD(HEX((({expr}) >> 8) & 255), 2, '0') || "
            f"LPAD(HEX(({expr}) & 255), 2, '0')"
        )

    # ── 字段编码 ──

    def _build_field_hex_expr(
        self, col: str, tf: TypeFamily, spec: CreDigestSpec, idx: int
    ) -> str:
        """单字段 CDP 编码的 hex 表达式——tag(1B) || 4B BE len || value_bytes。

        结果为大写 hex 字符串（UNHEX 可正确处理），可能混合大小写。
        """
        tag_hex = self._hex_int1be(str(tf.value))

        # NULL 编码：tag_hex || "FFFFFFFF"（8 hex 字符 = 4 字节全 1）
        null_expr = f"{tag_hex} || 'FFFFFFFF'"

        # ── 值表达式（VARCHAR）──
        if tf == TypeFamily.BOOLEAN:
            val_varchar = f"CASE WHEN {col} THEN 'true' ELSE 'false' END"
        elif tf in (
            TypeFamily.INT8,
            TypeFamily.INT16,
            TypeFamily.INT32,
            TypeFamily.INT64,
        ):
            val_varchar = f"CAST({col} AS VARCHAR)"
        elif tf in (TypeFamily.FLOAT32, TypeFamily.FLOAT64):
            fp = spec.float_precision[idx]
            if fp is not None:
                rounded = f"ROUND({col}::DOUBLE, {fp})"
            else:
                rounded = f"{col}::DOUBLE"
            val_varchar = (
                f"CASE WHEN ISNAN({rounded}) THEN 'nan' "
                f"WHEN ISINF({rounded}) AND {rounded} > 0 THEN 'inf' "
                f"WHEN ISINF({rounded}) AND {rounded} < 0 THEN '-inf' "
                f"WHEN {rounded} = 0.0 AND SIGN({rounded}) = -1 THEN '-0.0' "
                f"WHEN {rounded} = 0.0 THEN '0.0' "
                f"ELSE CAST({rounded} AS VARCHAR) END"
            )
        elif tf == TypeFamily.DECIMAL:
            sc = spec.decimal_scale[idx] or 0
            unscaled = f"ROUND({col} * {10 ** sc}, 0)"
            val_varchar = (
                f"CAST(CAST({unscaled} AS BIGINT) AS VARCHAR)"
            )
        elif tf == TypeFamily.VARCHAR:
            val_varchar = f"CAST({col} AS VARCHAR)"
        elif tf == TypeFamily.DATE:
            val_varchar = f"STRFTIME({col}, '%Y-%m-%d')"
        elif tf == TypeFamily.TIMESTAMP:
            val_varchar = f"STRFTIME({col}, '%Y-%m-%dT%H:%M:%S.%f%z')"
        else:
            raise ValueError(f"不支持: {tf}")

        # 值 hex + 长度 hex
        val_hex = f"HEX({val_varchar})"
        # DuckDB 1.5.3: OCTET_LENGTH 只接受 BLOB，不接受 VARCHAR。
        # 先用 ENCODE 转 BLOB（UTF-8 编码），再求字节数，与 Python oracle 一致。
        len_hex = self._hex_int4be(
            f"OCTET_LENGTH(ENCODE({val_varchar}))"
        )

        return (
            f"CASE WHEN {col} IS NULL THEN {null_expr} "
            f"ELSE {tag_hex} || {len_hex} || {val_hex} END"
        )

    def build_row_hash_expr(self, spec: CreDigestSpec) -> str:
        """所有字段 hex 编码串接 → UNHEX → SHA256 → VARCHAR(64) hex。"""
        fields_hex = [
            self._build_field_hex_expr(c, tf, spec, i)
            for i, (c, tf) in enumerate(
                zip(spec.output_columns, spec.type_families)
            )
        ]
        concat_hex = " || ".join(fields_hex)
        # SHA256(UNHEX(hex_string)) = hashlib.sha256(bytes).hexdigest()
        return f"SHA256(UNHEX({concat_hex}))"

    # ── 完整查询生成 ──

    def build_query(
        self, source_sql: str, spec: CreDigestSpec, spec_hash_hex: str
    ) -> str:
        """生成完整 CDP digest 查询。

        Args:
            source_sql: 数据源 SQL（如 "SELECT * FROM t"）
            spec: CDP 规范
            spec_hash_hex: 64 字符 hex——由调用方注入（不在此处计算）

        Returns:
            DuckDB SQL 查询字符串——执行后返回 (full_digest_hex, row_count)
        """
        rh = self.build_row_hash_expr(spec)

        # 256 是 bucket 数量常量
        bucket_count = 256

        return f"""
WITH _rows AS (
    SELECT {rh} AS _row_hash_hex FROM ({source_sql}) _src
),
_buckets AS (
    SELECT
        {self._extract_bucket_id('_row_hash_hex')} AS _bid,
        _row_hash_hex,
        COUNT(*) AS _cnt
    FROM _rows
    GROUP BY _bid, _row_hash_hex
),
_bucket_agg AS (
    SELECT
        _bid,
        SHA256(UNHEX(
            {self._hex_int1be('_bid')} ||
            {self._hex_int8be('COUNT(*)')} ||
            STRING_AGG(
                _row_hash_hex || {self._hex_int8be('_cnt')},
                '' ORDER BY _row_hash_hex
            )
        )) AS _bucket_digest_hex
    FROM _buckets
    GROUP BY _bid
),
_all_buckets AS (
    SELECT _bid, _bucket_digest_hex FROM _bucket_agg
    UNION ALL
    SELECT
        bucket_id,
        SHA256(UNHEX(
            {self._hex_int1be('bucket_id')} ||
            {self._hex_int8be('0')}
        )) AS _empty_digest
    FROM (SELECT UNNEST(GENERATE_SERIES(0, {bucket_count - 1})) AS bucket_id) _all
    WHERE bucket_id NOT IN (SELECT _bid FROM _bucket_agg)
),
_ordered AS (
    SELECT {self._hex_int1be('_bid')} || _bucket_digest_hex AS _pair_hex
    FROM _all_buckets ORDER BY _bid
)
SELECT
    SHA256(UNHEX(
        '{self._CDP_V1_HEX}' ||
        '{spec_hash_hex}' ||
        {self._hex_int8be('(SELECT COUNT(*) FROM _rows)')} ||
        {self._hex_int2be(str(bucket_count))} ||
        (SELECT COALESCE(STRING_AGG(_pair_hex, '' ORDER BY _pair_hex), '') FROM _ordered)
    )) AS full_digest_hex,
    (SELECT COUNT(*) FROM _rows) AS row_count
"""
