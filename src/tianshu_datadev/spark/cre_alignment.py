"""CRE v2 原型——行对齐与分桶层。

模块职责：
- AlignmentResult / BucketResult 数据模型
- KeyBasedRowAligner：按权威主键对齐双引擎行
- BucketHasher：基于主键 digest 分桶 + 桶内 row_digest 聚合
"""

from __future__ import annotations

import hashlib
from typing import Any

from pydantic import Field

from tianshu_datadev.developer_spec.models import StrictModel
from tianshu_datadev.spark.cre_encoding import (
    CreConfig,
    CREEncoder,
    _normalize_name,
)

# ════════════════════════════════════════════
# AlignmentResult——行对齐结果
# ════════════════════════════════════════════


class AlignmentResult(StrictModel):
    """按权威主键对齐双引擎行的结果。

    aligned_pairs : [(duckdb_row, spark_row)]——按相同 pk_digest 配对的列表
    duckdb_only   : 仅在 DuckDB 中出现的行（Spark 缺失）
    spark_only    : 仅在 Spark 中出现的行（DuckDB 缺失）
    duplicate_keys: True 表示发现重复主键
    error_message : 对齐失败时的错误描述
    """
    aligned_pairs: list[tuple[dict[str, Any], dict[str, Any]]] = Field(default_factory=list)
    duckdb_only: list[dict[str, Any]] = Field(default_factory=list)
    spark_only: list[dict[str, Any]] = Field(default_factory=list)
    duplicate_keys: bool = False
    error_message: str = ""


# ════════════════════════════════════════════
# KeyBasedRowAligner——按权威主键对齐双引擎行
# ════════════════════════════════════════════


class KeyBasedRowAligner:
    """按 Contract 权威主键对齐双引擎的结果行。

    流程：
    1. 检查 NULL/缺失主键——禁止按 NULL digest 对齐
    2. 对双引擎各行计算 pk_digest（基于主键值）
    3. 检查重复键（同一 pk_digest 对应 >1 行）
    4. 按 pk_digest 配对对齐

    缺少权威主键或主键为 NULL/空值时返回 AlignmentResult(error_message=...)。
    """

    @staticmethod
    def _validate_schema(
        duckdb_rows: list[dict[str, Any]],
        spark_rows: list[dict[str, Any]],
        config: CreConfig,
    ) -> str:
        """校验双引擎数据列的 schema 一致性。

        检查项：
        1. 双方缺少 Contract 必需列 → MISMATCH（配置错误）
        2. 单方缺少 Contract 必需列 → MISMATCH（数据错误）
        3. 双引擎列集合不一致（一方有额外列）→ MISMATCH

        Returns:
            空字符串表示校验通过，否则返回错误描述
        """
        if not duckdb_rows and not spark_rows:
            return ""

        # 获取实际列名（归一化后）
        d_cols = set()
        for row in duckdb_rows:
            d_cols.update({_normalize_name(k) for k in row})
        s_cols = set()
        for row in spark_rows:
            s_cols.update({_normalize_name(k) for k in row})

        # 检查 Contract 必需列
        contract_norm = {
            _normalize_name(col.column_name)
            for col in config.output_columns
        }

        # 双方缺少 Contract 必需列
        missing_from_both = contract_norm - d_cols - s_cols
        if missing_from_both:
            return (
                f"双方引擎均缺少 Contract 必需列："
                f"{sorted(missing_from_both)}——配置错误"
            )

        # 单方缺少 Contract 必需列
        missing_from_d = contract_norm - d_cols
        missing_from_s = contract_norm - s_cols
        if missing_from_d:
            return (
                f"DuckDB 缺少 Contract 必需列：{sorted(missing_from_d)}"
            )
        if missing_from_s:
            return (
                f"Spark 缺少 Contract 必需列：{sorted(missing_from_s)}"
            )

        # 额外列：一方有另一方没有的列（超出 Contract 范围）
        extra_in_d = d_cols - s_cols
        extra_in_s = s_cols - d_cols
        if extra_in_d or extra_in_s:
            parts = []
            if extra_in_d:
                parts.append(f"DuckDB 额外列：{sorted(extra_in_d)}")
            if extra_in_s:
                parts.append(f"Spark 额外列：{sorted(extra_in_s)}")
            return "；".join(parts)

        return ""

    @staticmethod
    def align(
        duckdb_rows: list[dict[str, Any]],
        spark_rows: list[dict[str, Any]],
        config: CreConfig,
        encoder: CREEncoder,
    ) -> AlignmentResult:
        if not config.primary_keys:
            return AlignmentResult(
                error_message="缺少权威主键配置（primary_keys）——无法对齐"
            )

        # Schema 列校验——双方缺少/单方缺少/额外列
        schema_err = KeyBasedRowAligner._validate_schema(duckdb_rows, spark_rows, config)
        if schema_err:
            return AlignmentResult(
                error_message=schema_err,
            )

        # 检查 NULL/缺失主键——禁止按 NULL digest 对齐
        def _has_null_pk(row: dict[str, Any]) -> str:
            norm_row = {_normalize_name(k): v for k, v in row.items()}
            for pk_name in config.primary_keys:
                val = norm_row.get(_normalize_name(pk_name))
                if val is None:
                    return f"主键列 '{pk_name}' 值为 NULL——无法对齐，需人工介入"
                if isinstance(val, str) and val.strip() == "":
                    return f"主键列 '{pk_name}' 值为空字符串——无法对齐，需人工介入"
            return ""

        for label, rows_iter in [("DuckDB", duckdb_rows), ("Spark", spark_rows)]:
            for row in rows_iter:
                err = _has_null_pk(row)
                if err:
                    return AlignmentResult(
                        error_message=f"[{label}] {err}"
                    )

        # 计算 pk_digest
        duckdb_map: dict[str, list[dict]] = {}
        for row in duckdb_rows:
            try:
                pk = encoder.pk_digest(row)
            except (ValueError, KeyError) as e:
                return AlignmentResult(
                    error_message=f"计算 DuckDB 行主键 digest 失败：{e}"
                )
            duckdb_map.setdefault(pk, []).append(row)

        spark_map: dict[str, list[dict]] = {}
        for row in spark_rows:
            try:
                pk = encoder.pk_digest(row)
            except (ValueError, KeyError) as e:
                return AlignmentResult(
                    error_message=f"计算 Spark 行主键 digest 失败：{e}"
                )
            spark_map.setdefault(pk, []).append(row)

        # 检查重复键
        duckdb_dups = {k: v for k, v in duckdb_map.items() if len(v) > 1}
        spark_dups = {k: v for k, v in spark_map.items() if len(v) > 1}
        has_dups = bool(duckdb_dups or spark_dups)

        if has_dups:
            detail_parts = []
            if duckdb_dups:
                detail_parts.append(
                    f"DuckDB 重复键：{[k[:12] for k in duckdb_dups]}"
                )
            if spark_dups:
                detail_parts.append(
                    f"Spark 重复键：{[k[:12] for k in spark_dups]}"
                )
            return AlignmentResult(
                duplicate_keys=True,
                error_message="；".join(detail_parts),
            )

        # 对齐
        all_pks = set(duckdb_map.keys()) | set(spark_map.keys())
        aligned: list[tuple[dict, dict]] = []
        d_only: list[dict] = []
        s_only: list[dict] = []

        for pk in all_pks:
            d_row = duckdb_map.get(pk)
            s_row = spark_map.get(pk)
            if d_row and s_row:
                aligned.append((d_row[0], s_row[0]))
            elif d_row:
                d_only.append(d_row[0])
            else:
                s_only.append(s_row[0])

        return AlignmentResult(
            aligned_pairs=aligned,
            duckdb_only=d_only,
            spark_only=s_only,
        )


# ════════════════════════════════════════════
# BucketResult——分桶结果
# ════════════════════════════════════════════


class BucketResult(StrictModel):
    """分桶结果。

    duckdb_bucket_digests : 每桶的 DuckDB 侧聚合 digest
    spark_bucket_digests  : 每桶的 Spark 侧聚合 digest
    mismatched_buckets    : digest 不一致的桶号列表
    num_buckets           : 总桶数
    """
    duckdb_bucket_digests: list[str] = Field(default_factory=list)
    spark_bucket_digests: list[str] = Field(default_factory=list)
    mismatched_buckets: list[int] = Field(default_factory=list)
    num_buckets: int = 0


# ════════════════════════════════════════════
# BucketHasher——分桶与桶内 digest
# ════════════════════════════════════════════


class BucketHasher:
    """基于 pk_digest 分桶 + 桶内 row_digest 排序聚合。

    分桶基于主键 digest（不是 row digest），确保同一主键的行落入
    同一桶。桶内 row_digest 排序后计算桶级 aggregate digest，
    用于诊断不一致分布。

    重复行在桶内保留，因排序后位置一致。
    """

    @staticmethod
    def compute_bucket_digests(
        aligned_pairs: list[tuple[dict, dict]],
        encoder: CREEncoder,
        config: CreConfig,
    ) -> BucketResult:
        num = config.num_buckets
        duckdb_buckets: list[list[str]] = [[] for _ in range(num)]
        spark_buckets: list[list[str]] = [[] for _ in range(num)]

        for d_row, s_row in aligned_pairs:
            d_bytes = encoder.encode_row(d_row)
            s_bytes = encoder.encode_row(s_row)
            d_digest = hashlib.sha256(d_bytes).hexdigest()
            s_digest = hashlib.sha256(s_bytes).hexdigest()

            # 用主键 digest 前 4 字节确定分桶（非 row digest）
            pk_hash = encoder.pk_digest(d_row)
            bucket_id = int.from_bytes(
                hashlib.sha256(pk_hash.encode()).digest()[:4], "big"
            ) % num

            duckdb_buckets[bucket_id].append(d_digest)
            spark_buckets[bucket_id].append(s_digest)

        # 桶内排序后计算桶 digest
        d_bucket_digests: list[str] = []
        s_bucket_digests: list[str] = []
        for i in range(num):
            d_sorted = sorted(duckdb_buckets[i])
            s_sorted = sorted(spark_buckets[i])
            d_bucket_digests.append(
                hashlib.sha256("".join(d_sorted).encode()).hexdigest()
            )
            s_bucket_digests.append(
                hashlib.sha256("".join(s_sorted).encode()).hexdigest()
            )

        mismatched = [
            i for i in range(num)
            if d_bucket_digests[i] != s_bucket_digests[i]
        ]

        return BucketResult(
            duckdb_bucket_digests=d_bucket_digests,
            spark_bucket_digests=s_bucket_digests,
            mismatched_buckets=mismatched,
            num_buckets=num,
        )
