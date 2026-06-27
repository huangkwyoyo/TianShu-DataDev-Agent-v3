"""Phase 1A 严格 Pydantic 模型定义。

所有运行时模型使用 extra="forbid"，拒绝未知字段。
枚举类型使用 (str, Enum) 保证 JSON 序列化兼容。
"""

from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, ConfigDict

# ════════════════════════════════════════════
# 基础
# ════════════════════════════════════════════

class StrictModel(BaseModel):
    """所有运行时模型的基类——统一 extra="forbid"，拒绝未知字段。"""

    model_config = ConfigDict(extra="forbid", frozen=False)


# ════════════════════════════════════════════
# 枚举
# ════════════════════════════════════════════

class AggregationType(str, Enum):
    """聚合函数枚举——仅支持已注册的 6 种聚合。"""

    COUNT = "COUNT"
    SUM = "SUM"
    AVG = "AVG"
    MIN = "MIN"
    MAX = "MAX"
    COUNT_DISTINCT = "COUNT_DISTINCT"


class JoinTypeEnum(str, Enum):
    """Join 类型枚举。"""

    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    FULL = "FULL"


class SortDirection(str, Enum):
    """排序方向。"""

    ASC = "ASC"
    DESC = "DESC"


class FieldSource(str, Enum):
    """字段信息来源——用于 SourceManifest 来源追踪。"""

    DEVELOPER_SPEC = "developer_spec"
    SCHEMA_REGISTRY = "schema_registry"
    SNAPSHOT_PROFILE = "snapshot_profile"


class ConflictType(str, Enum):
    """SourceConflict 分类。"""

    TYPE_MISMATCH = "TYPE_MISMATCH"
    ENUM_MISMATCH = "ENUM_MISMATCH"
    UNIQUENESS_MISMATCH = "UNIQUENESS_MISMATCH"
    MISSING_IN_REGISTRY = "MISSING_IN_REGISTRY"


class WarningSeverity(str, Enum):
    """ParseWarning 严重等级。"""

    LOW = "LOW"
    MEDIUM = "MEDIUM"


# ════════════════════════════════════════════
# 字段级声明模型
# ════════════════════════════════════════════

class ColumnDecl(StrictModel):
    """程序员声明的字段定义。

    data_type、enum_values、nullable、unique 均可为 None——允许程序员不声明
    （属于 Parser 允许宽松第 1 项），由 SourceManifest 从 SchemaRegistry 补充。
    """

    column_name: str
    normalized_name: str  # 由 FieldNormalizer 归一化后填入
    data_type: str | None = None
    enum_values: list[str] | None = None
    nullable: bool | None = None
    unique: bool | None = None
    description: str | None = None


class FilterDecl(StrictModel):
    """表级预过滤声明——只支持确定性的操作符，不支持自由 SQL 片段。"""

    column_ref: str
    # 允许的操作符
    operator: str  # "=" | "!=" | ">" | "<" | ">=" | "<=" | "IN" | "BETWEEN" | "IS_NULL" | "IS_NOT_NULL"
    value: str | int | float | list | None = None


class MetricDecl(StrictModel):
    """程序员声明的指标。"""

    metric_name: str
    aggregation: AggregationType
    input_column: str | None = None  # COUNT(*) 时为 None
    alias: str


class DimensionDecl(StrictModel):
    """维度声明——列引用指向已声明的字段。"""

    dimension_name: str
    column_ref: str


class JoinDecl(StrictModel):
    """程序员显式声明的 Join 关系——可选，缺失时由 RelationshipHypothesis 推理。"""

    left_table: str
    right_table: str
    left_key: str
    right_key: str
    join_type: JoinTypeEnum


class TimeRangeDecl(StrictModel):
    """时间范围声明——可选，缺失时生成 W002 警告。"""

    column_ref: str
    start: str
    end: str
    inclusive: bool = True


class SortDecl(StrictModel):
    """输出排序声明——可选，缺失时不拒绝。"""

    column: str
    direction: SortDirection = SortDirection.ASC


# ════════════════════════════════════════════
# 表声明
# ════════════════════════════════════════════

class InputTableDecl(StrictModel):
    """源表声明——包含别名、物理表名、列、过滤、角色等全部声明信息。"""

    table_alias: str
    source_table: str
    row_count: int | None = None  # 规范化后的数字（中文量级已转换）
    raw_row_count: str | None = None  # 原始字符串（如 "~5000万"），用于追溯
    role: str | None = None  # "fact" | "dim" | None
    description: str | None = None
    columns: list[ColumnDecl] = []
    filters: list[FilterDecl] = []
    partition_field: str | None = None
    time_field: str | None = None
    key_columns: list[ColumnDecl] = []  # 从 YAML key_columns 解析
    business_columns: list[ColumnDecl] = []  # 从 YAML business_columns 解析


class OutputSpecDecl(StrictModel):
    """输出规格——声明输出列、粒度和可选排序/行限制。"""

    columns: list[str]  # 输出列名列表
    grain: list[str]  # 输出粒度
    sort: list[SortDecl] | None = None
    limit: int | None = None


# ════════════════════════════════════════════
# 开放问题与警告
# ════════════════════════════════════════════

class HumanResolution(StrictModel):
    """程序员对 OpenQuestion 的人工裁决。"""

    resolved_by: str  # 裁决人标识
    resolved_at: str  # ISO 时间戳
    answer: str  # 裁决内容
    confidence: str = "confirmed"  # "confirmed" | "estimated"


class OpenQuestion(StrictModel):
    """Parser 或 SourceManifest 无法确定的问题。

    blocking=True 时阻断后续流程（Phase 1A 中 SOURCE_CONFLICT 必须 blocking）。
    """

    question_id: str
    source: str  # "parser" | "source_manifest" | "relationship"
    field_ref: str | None = None
    description: str
    blocking: bool = False
    resolution: HumanResolution | None = None


class ParseWarning(StrictModel):
    """非阻断解析警告。"""

    warning_id: str
    field_ref: str | None = None
    message: str
    severity: WarningSeverity = WarningSeverity.LOW


# ════════════════════════════════════════════
# SourceManifest 模型
# ════════════════════════════════════════════

class SourceConflict(StrictModel):
    """DeveloperSpec 与 SchemaRegistry 之间的字段级冲突。"""

    field_ref: str
    table_ref: str
    developer_spec_value: str
    schema_registry_value: str
    conflict_type: ConflictType


class SourceAnomaly(StrictModel):
    """快照采样或注册表查询中发现的异常——非冲突，但值得记录。"""

    anomaly_id: str
    table_ref: str
    column_ref: str | None = None
    description: str
    # 取值: MISSING_IN_REGISTRY | TYPE_CONFLICT | UNEXPECTED_NULL | TABLE_NOT_FOUND
    anomaly_type: str


class ForeignKeyRef(StrictModel):
    """外键引用。"""

    column: str
    ref_table: str
    ref_column: str


class ManifestColumn(StrictModel):
    """SourceManifest 中的字段条目——含来源标记。"""

    column_name: str
    normalized_name: str
    data_type: str
    nullable: bool = False
    unique: bool | None = None
    enum_values: list[str] | None = None
    source: FieldSource = FieldSource.DEVELOPER_SPEC


class ManifestTable(StrictModel):
    """SourceManifest 中的表条目。"""

    table_ref: str
    source_table: str
    columns: list[ManifestColumn] = []
    primary_key: list[str] | None = None
    foreign_keys: list[ForeignKeyRef] | None = None
    estimated_row_count: int | None = None


# ════════════════════════════════════════════
# 顶层模型
# ════════════════════════════════════════════

class ParsedDeveloperSpec(StrictModel):
    """确定性解析后的 DeveloperSpec——Phase 1A 核心输出。

    open_questions 中 blocking=True 的条目阻断后续流程。
    parse_warnings 记录非阻断警告，供人工审查。
    """

    spec_id: str
    spec_hash: str  # normalized_spec_hash
    title: str
    description: str  # YAML summary + Markdown 正文
    input_tables: list[InputTableDecl]
    metrics: list[MetricDecl]
    dimensions: list[DimensionDecl]
    joins: list[JoinDecl] | None = None
    time_range: TimeRangeDecl | None = None
    output_spec: OutputSpecDecl
    open_questions: list[OpenQuestion] = []
    parse_warnings: list[ParseWarning] = []


class SourceManifest(StrictModel):
    """表字段事实源追踪——Phase 1A 核心输出。

    每个字段标记来源：developer_spec | schema_registry | snapshot_profile。
    conflicts 记录 developer_spec 与 SchemaRegistry 的冲突条目。
    """

    manifest_id: str
    spec_hash: str
    tables: list[ManifestTable] = []
    conflicts: list[SourceConflict] = []
    anomalies: list[SourceAnomaly] = []
