"""DeveloperSpec 解析与 SourceManifest 构建。

Phase 1A 提供确定性解析、模型校验和事实源追踪能力。
"""

from .field_normalizer import FieldNormalizer, NormalizationConfig
from .models import (
    AggregationType,
    ColumnDecl,
    ComputeStep,
    ConflictType,
    DimensionDecl,
    FieldSource,
    FilterDecl,
    ForeignKeyRef,
    HumanResolution,
    InputTableDecl,
    JoinDecl,
    JoinTypeEnum,
    ManifestColumn,
    ManifestTable,
    MetricDecl,
    OpenQuestion,
    OutputSpecDecl,
    ParsedDeveloperSpec,
    ParseWarning,
    SafeCsvPathLiteral,
    SafePhysicalTableName,
    SortDecl,
    SortDirection,
    SourceAnomaly,
    SourceConflict,
    SourceManifest,
    StrictModel,
    TimeRangeDecl,
    WarningSeverity,
    _render_sql_string_literal,
)
from .parser import DeveloperSpecParser, ParseError, ParseErrorCode
from .source_manifest import SchemaRegistry, SnapshotProfile, SourceManifestBuilder

__all__ = [
    # 模型
    "StrictModel",
    "SafeCsvPathLiteral",
    "SafePhysicalTableName",
    "_render_sql_string_literal",
    "ParsedDeveloperSpec",
    "OpenQuestion",
    "ParseWarning",
    "SourceConflict",
    "HumanResolution",
    "InputTableDecl",
    "ColumnDecl",
    "MetricDecl",
    "DimensionDecl",
    "JoinDecl",
    "TimeRangeDecl",
    "OutputSpecDecl",
    "SortDecl",
    "FilterDecl",
    "SourceManifest",
    "ManifestTable",
    "ManifestColumn",
    "FieldSource",
    "SourceAnomaly",
    "ForeignKeyRef",
    "ComputeStep",
    # 枚举
    "AggregationType",
    "ConflictType",
    "JoinTypeEnum",
    "SortDirection",
    "WarningSeverity",
    # Parser
    "DeveloperSpecParser",
    "ParseError",
    "ParseErrorCode",
    # 归一化
    "FieldNormalizer",
    "NormalizationConfig",
    # SourceManifest
    "SourceManifestBuilder",
    "SchemaRegistry",
    "SnapshotProfile",
]
