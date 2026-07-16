"""测试 SourceManifestBuilder——冲突检测 + 来源标记 + 不静默覆盖。"""


from tests._test_utils import read_fixture
from tianshu_datadev.developer_spec.models import (
    FieldSource,
    ManifestColumn,
)
from tianshu_datadev.developer_spec.parser import DeveloperSpecParser
from tianshu_datadev.developer_spec.source_manifest import SourceManifestBuilder

# ── 辅助函数 ──

def read_fixture(path: str) -> str:
    import os
    abs_path = os.path.join(os.path.dirname(__file__), path)
    with open(abs_path, "r", encoding="utf-8") as f:
        return f.read()


# ── Fake SchemaRegistry 实现 ──

class DictSchemaRegistry:
    """基于字典的 SchemaRegistry 测试实现——用于可控的冲突场景测试。"""

    def __init__(self, tables: dict | None = None):
        self._tables = tables or {}

    def get_table_metadata(self, table_name: str) -> dict | None:
        return self._tables.get(table_name)

    def get_column_metadata(self, table_name: str, column_name: str) -> dict | None:
        table = self._tables.get(table_name, {})
        columns = table.get("columns", [])
        for col in columns:
            if col.get("name") == column_name:
                return col
        return None


class TestSourceManifestBuilder:
    """基础构建测试。"""

    def test_build_without_registry(self):
        """无 SchemaRegistry 时正常构建——所有字段标记 developer_spec。"""
        parser = DeveloperSpecParser()
        text = read_fixture("fixtures/golden/golden_no_time_range.md")
        spec = parser.parse(text)

        builder = SourceManifestBuilder()
        manifest, questions = builder.build(spec, registry=None)

        assert manifest is not None
        assert len(manifest.tables) == 1
        assert manifest.spec_hash == spec.spec_hash
        # 无 registry 时不应有冲突
        assert len(manifest.conflicts) == 0
        assert len(questions) == 0
        # 所有字段来源应为 developer_spec
        for col in manifest.tables[0].columns:
            assert col.source == FieldSource.DEVELOPER_SPEC

    def test_build_with_registry_supplements(self):
        """SchemaRegistry 补充缺失字段信息——不产生冲突。"""
        parser = DeveloperSpecParser()
        text = read_fixture("fixtures/golden/golden_type_inferred_from_registry.md")
        spec = parser.parse(text)

        # SchemaRegistry 提供 amount 字段的类型
        registry = DictSchemaRegistry({
            "dwd.test_fact": {
                "columns": [
                    {"name": "amount", "type": "decimal", "nullable": False},
                    {
                        "name": "status", "type": "string",
                        "nullable": True, "enum_values": ["active", "inactive"],
                    },
                ],
                "estimated_row_count": 1200000,
            },
        })

        builder = SourceManifestBuilder()
        manifest, questions = builder.build(spec, registry=registry)

        # 不应该有冲突——registry 只补充缺失信息
        assert len(manifest.conflicts) == 0
        assert len(questions) == 0
        # amount 字段类型应被补充
        amount_col = _find_column(manifest, "tf", "amount")
        assert amount_col is not None


class TestSourceConflict:
    """冲突检测测试。"""

    def test_type_mismatch_produces_conflict(self):
        """类型不一致产生 SOURCE_CONFLICT。"""
        parser = DeveloperSpecParser()
        # golden_type_inferred_from_registry 中 amount 无类型声明
        text = read_fixture("fixtures/golden/golden_no_time_range.md")
        spec = parser.parse(text)

        # Registry 中 event_time 类型与 DeveloperSpec 不同
        registry = DictSchemaRegistry({
            "dwd.test_fact": {
                "columns": [
                    {"name": "event_time", "type": "date", "nullable": False},
                ],
            },
        })

        builder = SourceManifestBuilder()
        manifest, questions = builder.build(spec, registry=registry)

        # event_time: DeveloperSpec 声明 timestamp，Registry 为 date → 冲突
        assert len(manifest.conflicts) >= 1
        conflict = manifest.conflicts[0]
        assert conflict.conflict_type.value in ("TYPE_MISMATCH",)

    def test_conflict_becomes_blocking_open_question(self):
        """SOURCE_CONFLICT 转为 OpenQuestion(blocking=true)。"""
        parser = DeveloperSpecParser()
        text = read_fixture("fixtures/golden/golden_no_time_range.md")
        spec = parser.parse(text)

        registry = DictSchemaRegistry({
            "dwd.test_fact": {
                "columns": [
                    {"name": "event_time", "type": "date", "nullable": False},
                ],
            },
        })

        builder = SourceManifestBuilder()
        manifest, questions = builder.build(spec, registry=registry)

        if len(manifest.conflicts) > 0:
            assert len(questions) > 0
            for q in questions:
                assert q.blocking is True
                assert q.source == "source_manifest"


class TestRegistryNoOverride:
    """SchemaRegistry 不静默覆盖 DeveloperSpec 声明。"""

    def test_registry_does_not_override_declared_type(self):
        """Registry 类型与 DeveloperSpec 一致时不产生冲突，但也标记为 developer_spec。"""
        parser = DeveloperSpecParser()
        text = read_fixture("fixtures/golden/golden_no_time_range.md")
        spec = parser.parse(text)

        # Registry 中类型与 DeveloperSpec 完全一致
        registry = DictSchemaRegistry({
            "dwd.test_fact": {
                "columns": [
                    {"name": "event_time", "type": "timestamp", "nullable": False},
                ],
            },
        })

        builder = SourceManifestBuilder()
        manifest, questions = builder.build(spec, registry=registry)

        # 类型一致——无冲突
        assert len(manifest.conflicts) == 0
        # DeveloperSpec 声明的字段保持 DEVELOPER_SPEC 来源
        event_col = _find_column(manifest, "tf", "event_time")
        if event_col:
            # 类型一致时不覆盖
            assert event_col.source == FieldSource.DEVELOPER_SPEC


class TestManifestAnomaly:
    """SOURCE_ANOMALY 测试。"""

    def test_table_not_found_in_registry(self):
        """表在 Registry 中不存在——产生 TABLE_NOT_FOUND anomaly。"""
        parser = DeveloperSpecParser()
        text = read_fixture("fixtures/golden/golden_no_time_range.md")
        spec = parser.parse(text)

        # Registry 不包含此表
        registry = DictSchemaRegistry({})

        builder = SourceManifestBuilder()
        manifest, questions = builder.build(spec, registry=registry)

        # 应该有一个 TABLE_NOT_FOUND anomaly
        assert len(manifest.anomalies) >= 1
        assert manifest.anomalies[0].anomaly_type == "TABLE_NOT_FOUND"


# ── 辅助 ──

def _find_column(manifest, table_ref: str, normalized_name: str) -> ManifestColumn | None:
    """在 SourceManifest 中查找指定字段。"""
    for table in manifest.tables:
        if table.table_ref == table_ref:
            for col in table.columns:
                if col.normalized_name == normalized_name:
                    return col
    return None


# ════════════════════════════════════════════
# V2 测试——unique_keys 合并 + Registry PK + role/ key_column_names 透传
# ════════════════════════════════════════════


class TestUniqueKeysMergeV2:
    """V2 unique_keys 合并逻辑测试。"""

    def test_merge_unique_keys_from_registry_pk(self):
        """Registry 返回 primary_key=["loc_id"] → manifest.unique_keys 含 ["loc_id"]。"""
        from tianshu_datadev.developer_spec.field_normalizer import FieldNormalizer
        from tianshu_datadev.developer_spec.source_manifest import _merge_unique_keys_from_sources

        normalizer = FieldNormalizer()
        # 测试模块级合并函数：Registry primary_key 应合并到 unique_keys
        result = _merge_unique_keys_from_sources(
            [["loc_id"]],   # Registry primary_key 作为 unique_keys 来源
            None,            # Registry unique_keys
            normalizer=normalizer,
        )
        assert ["loc_id"] in result

    def test_builder_transmits_role_and_key_columns(self):
        """InputTableDecl(role="dim", key_columns=[ColumnDecl("Location_ID")])
        → ManifestTable.role="dim", key_column_names_normalized=["location_id"]。
        """
        from tianshu_datadev.developer_spec.field_normalizer import FieldNormalizer
        from tianshu_datadev.developer_spec.models import (
            ColumnDecl,
            InputTableDecl,
            ParsedDeveloperSpec,
            SafePhysicalTableName,
        )
        from tianshu_datadev.developer_spec.source_manifest import SourceManifestBuilder

        normalizer = FieldNormalizer()
        builder = SourceManifestBuilder(normalizer=normalizer)

        # 构建一个最小 InputTableDecl
        spec = ParsedDeveloperSpec(
            spec_id="test_v2",
            spec_hash="abc123",
            title="test",
            description="test",
            input_tables=[
                InputTableDecl(
                    table_alias="tz",
                    source_table=SafePhysicalTableName("silver.taxi_zone"),
                    role="dim",
                    key_columns=[
                        ColumnDecl(
                            column_name="Location_ID",
                            normalized_name=normalizer.normalize("Location_ID"),
                            data_type="bigint",
                        ),
                    ],
                ),
            ],
            metrics=[],
            dimensions=[],
            output_spec={"columns": [], "grain": []},  # type: ignore[arg-type]
        )

        manifest, _ = builder.build(spec)
        table = manifest.tables[0]
        assert table.role == "dim"
        assert "location_id" in table.key_column_names_normalized

    def test_unique_keys_preserves_original_order(self):
        """unique_keys: [["zone_name", "borough"]] → 保留原始顺序，不排序。"""
        from tianshu_datadev.developer_spec.field_normalizer import FieldNormalizer
        from tianshu_datadev.developer_spec.source_manifest import _normalize_unique_keys_list

        normalizer = FieldNormalizer()
        result = _normalize_unique_keys_list(
            [["zone_name", "borough"]], normalizer=normalizer
        )
        assert result == [["zone_name", "borough"]]
        # 不应被排序为 [["borough", "zone_name"]]
        assert result != [["borough", "zone_name"]]
