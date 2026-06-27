"""测试 DeveloperSpec 模型的严格性——extra="forbid"、必填字段、枚举校验。"""

import pytest
from pydantic import ValidationError

from tianshu_datadev.developer_spec.models import (
    ColumnDecl,
    ConflictType,
    HumanResolution,
    InputTableDecl,
    MetricDecl,
    OpenQuestion,
    OutputSpecDecl,
    ParsedDeveloperSpec,
    ParseWarning,
    SortDecl,
    SourceConflict,
)


class TestSchemaStrictness:
    """验证所有模型 extra="forbid" 且字段类型严格。"""

    def test_extra_field_rejected_on_parsed_spec(self):
        """ParsedDeveloperSpec 拒绝额外字段。"""
        with pytest.raises(ValidationError):
            ParsedDeveloperSpec(
                spec_id="test",
                spec_hash="abc123",
                title="Test",
                description="",
                input_tables=[],
                metrics=[],
                dimensions=[],
                output_spec=OutputSpecDecl(columns=["a"], grain=["b"]),
                invalid_extra_field="should_not_exist",
            )

    def test_extra_field_rejected_on_input_table(self):
        """InputTableDecl 拒绝额外字段。"""
        with pytest.raises(ValidationError):
            InputTableDecl(
                table_alias="t",
                source_table="db.t",
                invalid_field=123,
            )

    def test_required_fields_missing(self):
        """必填字段缺失触发 ValidationError。"""
        with pytest.raises(ValidationError):
            # InputTableDecl 缺少 source_table
            InputTableDecl(table_alias="t")

    def test_enum_invalid_value_rejected(self):
        """枚举字段非法值被拒绝。"""
        with pytest.raises(ValidationError):
            MetricDecl(
                metric_name="test",
                aggregation="INVALID_AGG",
                input_column="col",
                alias="test",
            )

    def test_type_error_rejected(self):
        """类型错误的字段值被拒绝。"""
        with pytest.raises(ValidationError):
            SortDecl(
                column="col",
                direction=123,  # 应为 SortDirection 枚举
            )

    def test_extra_field_on_nested_model(self):
        """嵌套模型（ColumnDecl）同样拒绝额外字段。"""
        with pytest.raises(ValidationError):
            ColumnDecl(
                column_name="test",
                normalized_name="test",
                extra_nested_field="bad",
            )


class TestModelFactories:
    """验证模型可正常构建和序列化。"""

    def test_open_question_construction(self):
        """OpenQuestion 正确构建——blocking 默认 False。"""
        q = OpenQuestion(
            question_id="Q001",
            source="parser",
            description="测试问题",
        )
        assert q.blocking is False
        assert q.resolution is None

    def test_open_question_blocking(self):
        """OpenQuestion blocking=True 正确设置。"""
        q = OpenQuestion(
            question_id="Q002",
            source="source_manifest",
            field_ref="t.col",
            description="冲突",
            blocking=True,
        )
        assert q.blocking is True

    def test_source_conflict_construction(self):
        """SourceConflict 正确构建。"""
        c = SourceConflict(
            field_ref="amount",
            table_ref="tf",
            developer_spec_value="decimal",
            schema_registry_value="bigint",
            conflict_type=ConflictType.TYPE_MISMATCH,
        )
        assert c.conflict_type == ConflictType.TYPE_MISMATCH
        assert "amount" in c.field_ref

    def test_parse_warning_construction(self):
        """ParseWarning 正确构建——默认 severity=LOW。"""
        w = ParseWarning(
            warning_id="W001",
            message="测试警告",
        )
        assert w.severity == "LOW"


class TestHumanResolution:
    """HumanResolution 模型的构建和序列化。"""

    def test_resolution_construction(self):
        """HumanResolution 正确构建。"""
        r = HumanResolution(
            resolved_by="test_user",
            resolved_at="2025-01-01T00:00:00",
            answer="确认使用此字段类型",
        )
        assert r.confidence == "confirmed"

    def test_resolution_attached_to_question(self):
        """HumanResolution 可附加到 OpenQuestion。"""
        r = HumanResolution(
            resolved_by="dev",
            resolved_at="2025-01-01T00:00:00",
            answer="已确认",
        )
        q = OpenQuestion(
            question_id="Q003",
            source="parser",
            description="需要人工确认",
            blocking=True,
            resolution=r,
        )
        assert q.resolution is not None
        assert q.resolution.resolved_by == "dev"
