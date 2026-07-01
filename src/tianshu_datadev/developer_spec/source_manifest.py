"""SourceManifest 构建器——事实源追踪与冲突检测。

SourceManifest 是表字段事实的唯一追踪点。
构建流程：
  1. 从 ParsedDeveloperSpec 提取表字段 → 标记 source=developer_spec
  2. 可选 SchemaRegistry 补充物理表元数据 → 标记 source=schema_registry
  3. 冲突检测：同一字段在两个来源中不一致 → 输出 SOURCE_CONFLICT → OpenQuestion(blocking=true)
  4. 可选 SnapshotProfile 补充统计特征 → 标记 source=snapshot_profile

SchemaRegistry 只补充 developer_spec 中缺失的字段信息，不静默覆盖程序员已声明的值。
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Any, Protocol

from .field_normalizer import FieldNormalizer
from .models import (
    ConflictType,
    FieldSource,
    ForeignKeyRef,
    ManifestColumn,
    ManifestTable,
    OpenQuestion,
    ParsedDeveloperSpec,
    ManifestAnomaly,
    SourceConflict,
    SourceManifest,
)


class SchemaRegistry(Protocol):
    """可选的 SchemaRegistry 接口——用于获取物理表元数据。

    Phase 1A 只定义接口，不实现具体适配器。
    实现方式待定（git submodule / REST API / 本地文件）。
    """

    def get_table_metadata(self, table_name: str) -> dict[str, Any] | None:
        """返回物理表元数据。

        Returns:
            dict 包含：
            - columns: list[dict]  （字段名、类型、nullable、enum_values 等）
            - primary_key: list[str] | None
            - foreign_keys: list[dict] | None
            - estimated_row_count: int | None
            表不存在时返回 None。
        """
        ...

    def get_column_metadata(self, table_name: str, column_name: str) -> dict[str, Any] | None:
        """返回单个字段的元数据。字段不存在时返回 None。"""
        ...


@dataclass
class SnapshotProfile:
    """从冻结快照采样推断的字段统计特征。

    Phase 1A 只做占位结构，不实现具体采样逻辑。
    """

    table_name: str
    column_stats: dict[str, dict] = field(default_factory=dict)
    # column_name → {null_count, distinct_count, min, max, ...}
    snapshot_timestamp: str = ""
    row_count: int = 0


class SourceManifestBuilder:
    """从 ParsedDeveloperSpec + 可选 SchemaRegistry + 可选 SnapshotProfile 构建 SourceManifest。

    用法:
        builder = SourceManifestBuilder()
        manifest, open_questions = builder.build(spec, registry=my_registry)
    """

    def __init__(self, normalizer: FieldNormalizer | None = None):
        """初始化构建器。

        Args:
            normalizer: 字段名归一化器，用于比较不同来源的字段名
        """
        self._normalizer = normalizer or FieldNormalizer()

    def build(
        self,
        spec: ParsedDeveloperSpec,
        registry: SchemaRegistry | None = None,
        profile: SnapshotProfile | None = None,
    ) -> tuple[SourceManifest, list[OpenQuestion]]:
        """构建 SourceManifest。

        Args:
            spec: 已解析的 DeveloperSpec
            registry: 可选的 SchemaRegistry 适配器
            profile: 可选的 SnapshotProfile

        Returns:
            (SourceManifest, list[OpenQuestion]) —— OpenQuestion 中包含
            blocking=true 的 SOURCE_CONFLICT 条目。
        """
        # 1. 从 DeveloperSpec 提取表字段
        tables = self._build_manifest_tables(spec)

        all_conflicts: list[SourceConflict] = []
        all_anomalies: list[ManifestAnomaly] = []
        open_questions: list[OpenQuestion] = []

        # 2. 从 SchemaRegistry 补充
        if registry is not None:
            conflicts, anomalies = self._supplement_from_registry(tables, registry)
            all_conflicts.extend(conflicts)
            all_anomalies.extend(anomalies)

            # 冲突转为 blocking OpenQuestion
            for c in conflicts:
                open_questions.append(OpenQuestion(
                    question_id=f"Q-CONFLICT-{uuid.uuid4().hex[:8]}",
                    source="source_manifest",
                    field_ref=f"{c.table_ref}.{c.field_ref}",
                    description=(
                        f"SOURCE_CONFLICT [{c.conflict_type.value}]: "
                        f"DeveloperSpec 声明为 '{c.developer_spec_value}'，"
                        f"SchemaRegistry 记录为 '{c.schema_registry_value}'"
                    ),
                    blocking=True,
                ))

        # 3. 从 SnapshotProfile 补充
        if profile is not None:
            self._supplement_from_profile(tables, profile)

        # 4. 构建 manifest
        manifest_id = self._build_manifest_id(spec.spec_hash)
        manifest = SourceManifest(
            manifest_id=manifest_id,
            spec_hash=spec.spec_hash,
            tables=tables,
            conflicts=all_conflicts,
            anomalies=all_anomalies,
        )

        return manifest, open_questions

    # ── 内部方法 ──

    def _build_manifest_tables(self, spec: ParsedDeveloperSpec) -> list[ManifestTable]:
        """从 ParsedDeveloperSpec 提取 ManifestTable，标记 source=developer_spec。

        合并 key_columns 和 business_columns 到 columns 列表。
        去重——同一字段可能在 key_columns 和 business_columns 中重复出现。
        """
        tables: list[ManifestTable] = []
        for input_t in spec.input_tables:
            seen_cols: set[str] = set()
            columns: list[ManifestColumn] = []

            # key_columns 优先（它们通常是主键/业务键）
            for c in input_t.key_columns:
                if c.normalized_name not in seen_cols:
                    seen_cols.add(c.normalized_name)
                    columns.append(ManifestColumn(
                        column_name=c.column_name,
                        normalized_name=c.normalized_name,
                        data_type=c.data_type or "unknown",
                        nullable=c.nullable if c.nullable is not None else False,
                        unique=c.unique,
                        enum_values=c.enum_values,
                        source=FieldSource.DEVELOPER_SPEC,
                    ))

            # business_columns 次之
            for c in input_t.business_columns:
                if c.normalized_name not in seen_cols:
                    seen_cols.add(c.normalized_name)
                    columns.append(ManifestColumn(
                        column_name=c.column_name,
                        normalized_name=c.normalized_name,
                        data_type=c.data_type or "unknown",
                        nullable=c.nullable if c.nullable is not None else False,
                        unique=c.unique,
                        enum_values=c.enum_values,
                        source=FieldSource.DEVELOPER_SPEC,
                    ))

            # 最后是 columns（扁平列表，如果存在）
            for c in input_t.columns:
                if c.normalized_name not in seen_cols:
                    seen_cols.add(c.normalized_name)
                    columns.append(ManifestColumn(
                        column_name=c.column_name,
                        normalized_name=c.normalized_name,
                        data_type=c.data_type or "unknown",
                        nullable=c.nullable if c.nullable is not None else False,
                        unique=c.unique,
                        enum_values=c.enum_values,
                        source=FieldSource.DEVELOPER_SPEC,
                    ))

            # 提取主键（来自 key_columns 中标记 unique=True 的字段）
            primary_key = [c.column_name for c in input_t.key_columns if c.unique]

            tables.append(ManifestTable(
                table_ref=input_t.table_alias,
                source_table=input_t.source_table,
                columns=columns,
                primary_key=primary_key if primary_key else None,
                foreign_keys=None,  # DeveloperSpec 不声明外键，由 SchemaRegistry 补充
                estimated_row_count=input_t.row_count,
            ))

        return tables

    def _supplement_from_registry(
        self,
        tables: list[ManifestTable],
        registry: SchemaRegistry,
    ) -> tuple[list[SourceConflict], list[ManifestAnomaly]]:
        """从 SchemaRegistry 补充字段信息，并检测冲突。

        规则：
        - 字段在 developer_spec 中存在，registry 中不存在 → SOURCE_ANOMALY (MISSING_IN_REGISTRY)
        - 字段在两者中都存在且一致 → 保留 developer_spec 标记
        - 字段在两者中都存在但不一致 → SOURCE_CONFLICT
        - 字段只在 registry 中存在 → 追加到 columns，标记 schema_registry

        永远不会静默覆盖 developer_spec 已声明的值。
        """
        conflicts: list[SourceConflict] = []
        anomalies: list[ManifestAnomaly] = []

        for table in tables:
            registry_meta = registry.get_table_metadata(table.source_table)
            if registry_meta is None:
                anomalies.append(ManifestAnomaly(
                    anomaly_id=f"ANOMALY-{uuid.uuid4().hex[:8]}",
                    table_ref=table.table_ref,
                    description=f"表 '{table.source_table}' 在 SchemaRegistry 中不存在",
                    anomaly_type="TABLE_NOT_FOUND",
                ))
                continue

            # 构建 registry 字段索引
            reg_columns: dict[str, dict] = {}
            for col in registry_meta.get("columns", []) or []:
                col_name = col.get("name", "")
                normalized = self._normalizer.normalize(col_name)
                reg_columns[normalized] = col

            # 更新表的行数估算
            if table.estimated_row_count is None:
                table.estimated_row_count = registry_meta.get("estimated_row_count")

            # 更新外键
            reg_fks = registry_meta.get("foreign_keys")
            if reg_fks and table.foreign_keys is None:
                table.foreign_keys = [
                    ForeignKeyRef(
                        column=fk.get("column", ""),
                        ref_table=fk.get("ref_table", ""),
                        ref_column=fk.get("ref_column", ""),
                    )
                    for fk in reg_fks
                ]

            # 逐字段对比
            for col in table.columns:
                reg_col = reg_columns.get(col.normalized_name)
                if reg_col is None:
                    # 字段在 registry 中不存在
                    anomalies.append(ManifestAnomaly(
                        anomaly_id=f"ANOMALY-{uuid.uuid4().hex[:8]}",
                        table_ref=table.table_ref,
                        column_ref=col.column_name,
                        description=(
                            f"字段 '{table.source_table}.{col.column_name}' "
                            f"在 DeveloperSpec 中声明但 SchemaRegistry 中不存在"
                        ),
                        anomaly_type="MISSING_IN_REGISTRY",
                    ))
                    continue

                # 检测冲突
                conflict = self._detect_field_conflict(
                    table.table_ref, col, reg_col
                )
                if conflict:
                    conflicts.append(conflict)
                else:
                    # 无冲突——补充缺失字段
                    self._supplement_column(col, reg_col)

            # 补充 registry 中存在但 developer_spec 中不存在的字段
            existing_names = {c.normalized_name for c in table.columns}
            for norm_name, reg_col in reg_columns.items():
                if norm_name not in existing_names:
                    source_name = reg_col.get("name", norm_name)
                    table.columns.append(ManifestColumn(
                        column_name=source_name,
                        normalized_name=norm_name,
                        data_type=str(reg_col.get("type", "unknown")),
                        nullable=reg_col.get("nullable", False),
                        unique=reg_col.get("unique"),
                        enum_values=reg_col.get("enum_values"),
                        source=FieldSource.SCHEMA_REGISTRY,
                    ))

        return conflicts, anomalies

    def _detect_field_conflict(
        self,
        table_ref: str,
        spec_col: ManifestColumn,
        reg_col: dict,
    ) -> SourceConflict | None:
        """检测单个字段在 developer_spec 和 schema_registry 之间的冲突。

        比较维度：data_type、enum_values、unique。
        nullable 不作为冲突维度——程序员可能故意不声明 nullable。
        """
        reg_type = str(reg_col.get("type", "")).lower()
        spec_type = spec_col.data_type.lower()

        # 类型冲突（大小写不敏感）
        if spec_type != "unknown" and reg_type and spec_type != reg_type:
            # 部分类型兼容性（int↔bigint、varchar↔text 不视为冲突）
            if not self._types_compatible(spec_type, reg_type):
                return SourceConflict(
                    field_ref=spec_col.column_name,
                    table_ref=table_ref,
                    developer_spec_value=spec_col.data_type,
                    schema_registry_value=str(reg_col.get("type", "")),
                    conflict_type=ConflictType.TYPE_MISMATCH,
                )

        # 枚举值冲突
        reg_enum = reg_col.get("enum_values")
        if spec_col.enum_values and reg_enum:
            spec_set = set(spec_col.enum_values)
            reg_set = set(reg_enum)
            if spec_set != reg_set:
                return SourceConflict(
                    field_ref=spec_col.column_name,
                    table_ref=table_ref,
                    developer_spec_value=str(sorted(spec_set)),
                    schema_registry_value=str(sorted(reg_set)),
                    conflict_type=ConflictType.ENUM_MISMATCH,
                )

        # 唯一性冲突
        reg_unique = reg_col.get("unique")
        if spec_col.unique is not None and reg_unique is not None and spec_col.unique != reg_unique:
            return SourceConflict(
                field_ref=spec_col.column_name,
                table_ref=table_ref,
                developer_spec_value=str(spec_col.unique),
                schema_registry_value=str(reg_unique),
                conflict_type=ConflictType.UNIQUENESS_MISMATCH,
            )

        return None

    def _supplement_column(self, col: ManifestColumn, reg_col: dict) -> None:
        """用 SchemaRegistry 的值补充 ManifestColumn 中的缺失字段。

        在调用此方法前已确认无冲突——只补充 developer_spec 中为 None/默认值的字段。
        """
        # 补充 data_type（如果 developer_spec 未声明）
        if col.data_type == "unknown":
            reg_type = reg_col.get("type")
            if reg_type:
                # 使用 object.__setattr__ 绕过 frozen
                object.__setattr__(col, "data_type", str(reg_type))

        # 补充 nullable（如果 developer_spec 未声明，默认为 False，registry 可能更准确）
        if col.nullable is False and reg_col.get("nullable") is True:
            object.__setattr__(col, "nullable", True)

        # 补充 unique
        if col.unique is None:
            reg_unique = reg_col.get("unique")
            if reg_unique is not None:
                object.__setattr__(col, "unique", reg_unique)

        # 补充 enum_values
        if col.enum_values is None:
            reg_enum = reg_col.get("enum_values")
            if reg_enum:
                object.__setattr__(col, "enum_values", list(reg_enum))

    def _supplement_from_profile(
        self,
        tables: list[ManifestTable],
        profile: SnapshotProfile,
    ) -> None:
        """从 SnapshotProfile 补充字段统计信息。

        只补充附加元数据（如去重率、NULL 比例），不覆盖已有类型/约束信息。
        Phase 1A 中为占位实现——只更新 estimated_row_count。
        """
        for table in tables:
            if table.source_table == profile.table_name:
                if table.estimated_row_count is None and profile.row_count > 0:
                    object.__setattr__(table, "estimated_row_count", profile.row_count)
                # 后续 Phase 可将 column_stats 合并到 ManifestColumn

    def _types_compatible(self, spec_type: str, reg_type: str) -> bool:
        """判断两个 SQL 类型是否兼容——不视为冲突。

        int ↔ bigint、varchar ↔ text 视为兼容。
        int ↔ varchar 视为冲突。
        """
        # 兼容类型组——每个组内的类型互相兼容
        compatible_groups: list[set[str]] = [
            {"int", "integer", "bigint", "smallint", "tinyint"},
            {"varchar", "text", "string", "char"},
            {"float", "double", "real"},
            {"decimal", "numeric", "decimal(18,2)"},
            {"timestamp", "datetime"},
            {"date"},
            {"boolean", "bool"},
        ]

        spec_lower = spec_type.lower()
        reg_lower = reg_type.lower()

        if spec_lower == reg_lower:
            return True

        # 精确检查：两个类型是否属于同一兼容组
        for group in compatible_groups:
            spec_in_group = spec_lower in group
            reg_in_group = reg_lower in group
            if spec_in_group and reg_in_group:
                return True
            # 部分匹配：如 "decimal(18,2)" 属于 decimal 组
            if not spec_in_group:
                spec_in_group = any(g in spec_lower for g in group if g != spec_lower)
            if not reg_in_group:
                reg_in_group = any(g in reg_lower for g in group if g != reg_lower)
            if spec_in_group and reg_in_group:
                return True

        return False

    def _build_manifest_id(self, spec_hash: str) -> str:
        """生成 manifest_id——前缀 'manifest_' + spec_hash 前 8 位。"""
        return f"manifest_{spec_hash[:8]}"


def build_manifest_from_spec(spec: ParsedDeveloperSpec) -> SourceManifest:
    """从 ParsedDeveloperSpec 构建 SourceManifest——涵盖所有列引用。

    不仅包含 input_tables 中显式声明的列，还从 metrics、dimensions、
    output_spec 中提取被引用但未显式声明的列（以 "varchar" 类型补充）。

    这是一个轻量级构建器——不涉及 SchemaRegistry 或 SnapshotProfile。
    需要完整冲突检测时，使用 SourceManifestBuilder.build()。
    """
    tables: list[ManifestTable] = []
    for t in spec.input_tables:
        seen: set[str] = set()
        cols: list[ManifestColumn] = []

        def _add(col_name: str) -> None:
            """添加列（去重），从原始声明中查找类型信息。"""
            if col_name in seen:
                return
            seen.add(col_name)
            dtype = "varchar"
            for src_list in [t.columns, t.key_columns, t.business_columns]:
                for c in src_list:
                    if c.column_name == col_name:
                        dtype = c.data_type or "varchar"
                        break
            cols.append(
                ManifestColumn(
                    column_name=col_name,
                    normalized_name=col_name.lower(),
                    data_type=dtype,
                    nullable=True,
                    source=FieldSource.DEVELOPER_SPEC,
                )
            )

        # 从显式声明的列开始
        for c in t.columns + t.key_columns + t.business_columns:
            _add(c.column_name)

        # 从指标引用中提取
        for m in spec.metrics:
            if m.input_column:
                _add(m.input_column)

        # 从维度引用中提取
        for d in spec.dimensions:
            _add(d.column_ref)

        # 从输出列提取
        for col in spec.output_spec.columns:
            _add(col.name)

        # 从排序列提取
        if spec.output_spec.sort:
            for s in spec.output_spec.sort:
                _add(s.column)

        tables.append(
            ManifestTable(
                table_ref=t.table_alias,
                source_table=t.source_table,
                columns=cols,
                estimated_row_count=t.row_count,
            )
        )
    return SourceManifest(
        manifest_id=f"manifest_{spec.spec_hash[:12]}",
        spec_hash=spec.spec_hash,
        tables=tables,
    )
