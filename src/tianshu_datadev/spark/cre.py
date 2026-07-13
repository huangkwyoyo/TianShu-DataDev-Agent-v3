"""CRE v2 原型——Canonical Row Encoding 双引擎确定性比较（兼容 re-export 入口）。

本模块为向后兼容 re-export 入口，所有实现已拆分到：
  cre_encoding.py   — 编码层（CREEncoder, CreConfig, 类型映射）
  cre_alignment.py  — 对齐与分桶（KeyBasedRowAligner, BucketHasher）
  cre_comparison.py — 容差比较（ToleranceComparator）
  cre_decision.py   — 判定矩阵（DecisionEngine）

保持原型不接入生产 PhysicalVerifier.verify()。
不修改 executor.py/pipeline.py，不改变现有状态枚举。

Usage（原型/测试用）：
    config = CreConfig(
        output_columns=[NormalizationColumn(column_name="id", data_type="bigint"), ...],
        primary_keys=["id"],
    )
    encoder = CREEncoder(config)
    alignment = KeyBasedRowAligner.align(duckdb_rows, spark_rows, config, encoder)
    # 逐行比较
    aligned_results = [...]
    # 分桶
    buckets = BucketHasher.compute_bucket_digests(alignment.aligned_pairs, encoder, config)
    # 判定
    result = DecisionEngine.decide(alignment, aligned_results, buckets, config)
"""

from __future__ import annotations

# ── 对齐与分桶 ──
from tianshu_datadev.spark.cre_alignment import (
    AlignmentResult,
    BucketHasher,
    BucketResult,
    KeyBasedRowAligner,
)

# ── 容差比较 ──
from tianshu_datadev.spark.cre_comparison import (
    CellComparisonResult,
    ComparisonRowResult,
    ToleranceComparator,
)

# ── 判定矩阵 ──
from tianshu_datadev.spark.cre_decision import (
    CanonicalComparisonResult,
    DecisionEngine,
)

# ── 编码层 ──
from tianshu_datadev.spark.cre_encoding import (
    _TOLERANCE_RULE_DESC,
    CreConfig,
    CREEncoder,
    CreShadowReport,
    CreShadowStatus,
    CreShadowWarning,
    DecimalStrategy,
    EnvironmentManifest,
    NotToleratedReason,
    NullStrategy,
    SpecialFloatStrategy,
    ToleratedDifferenceWarning,
    ToleratedFieldDetail,
    _normalize_name,
    _type_family,
)

__all__ = [
    # 编码层
    "CREEncoder",
    "CreConfig",
    "CreShadowReport",
    "CreShadowStatus",
    "CreShadowWarning",
    "DecimalStrategy",
    "EnvironmentManifest",
    "NotToleratedReason",
    "NullStrategy",
    "SpecialFloatStrategy",
    "ToleratedDifferenceWarning",
    "ToleratedFieldDetail",
    "_normalize_name",
    "_type_family",
    "_TOLERANCE_RULE_DESC",
    # 对齐与分桶
    "AlignmentResult",
    "BucketHasher",
    "BucketResult",
    "KeyBasedRowAligner",
    # 容差比较
    "CellComparisonResult",
    "ComparisonRowResult",
    "ToleranceComparator",
    # 判定矩阵
    "CanonicalComparisonResult",
    "DecisionEngine",
]
