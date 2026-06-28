"""Code Review Package 生成器——Phase 2。

从 SqlBuildPlan + CompiledSql + ExecutionTrace + SourceManifest 组装
完整 Code Review Package（含 DataTransformContract-lite、provenance.yml 和 review.md）。

DataTransformContract-lite 从 SqlBuildPlan 确定性抽取，不依赖 LLM。
所有 artifact 记录 SHA-256 用于可追溯性。
"""

# ── 数据模型 ──
# ── Contract 抽取器 ──
from .contract_extractor import DataTransformContractExtractor
from .models import (
    REVIEW_ROUTING_TABLE,
    VALID_REVIEW_TARGETS,
    ArtifactRef,
    ContractAggregation,
    ContractColumn,
    ContractInputTable,
    ContractJoin,
    ContractLimit,
    ContractOutputColumn,
    ContractPredicate,
    ContractSort,
    DataTransformContractLite,
    HumanReviewItem,
    PackageInputs,
    ReviewFeedback,
    ReviewPackageManifest,
    ReviewTarget,
    ValidationSummaryArtifact,
)

# ── Packager ──
from .packager import ReviewPackageBuilder

# ── Provenance 生成器 ──
from .provenance import compute_json_hash, generate_provenance

# ── Review.md 生成器 ──
from .review_md import generate_review_md

__all__ = [
    # 数据模型
    "ReviewTarget",
    "VALID_REVIEW_TARGETS",
    "REVIEW_ROUTING_TABLE",
    "ArtifactRef",
    "ContractAggregation",
    "ContractColumn",
    "ContractInputTable",
    "ContractJoin",
    "ContractLimit",
    "ContractOutputColumn",
    "ContractPredicate",
    "ContractSort",
    "DataTransformContractLite",
    "HumanReviewItem",
    "PackageInputs",
    "ReviewFeedback",
    "ReviewPackageManifest",
    "ValidationSummaryArtifact",
    # Contract 抽取器
    "DataTransformContractExtractor",
    # Packager
    "ReviewPackageBuilder",
    # 生成器
    "compute_json_hash",
    "generate_provenance",
    "generate_review_md",
]
