"""测试 DeveloperSpecParser——golden + rejection fixture + hash 确定性。"""

import pytest

from tianshu_datadev.developer_spec.parser import DeveloperSpecParser, ParseError, ParseErrorCode

# ── 辅助函数：读取 fixture 文件 ──

def _read_fixture(path: str) -> str:
    """读取 fixture 文件内容。"""
    import os
    abs_path = os.path.join(os.path.dirname(__file__), path)
    with open(abs_path, "r", encoding="utf-8") as f:
        return f.read()


# ════════════════════════════════════════════
# Golden 6 项允许宽松
# ════════════════════════════════════════════

class TestParserGolden:
    """6 项允许宽松场景——每项解析成功并产生对应警告。"""

    def test_golden_type_inferred_from_registry(self):
        """允许宽松 1：字段类型未声明——Parser 不阻断，生成 W001 warning。"""
        parser = DeveloperSpecParser()
        text = _read_fixture("fixtures/golden/golden_type_inferred_from_registry.md")
        spec = parser.parse(text)
        assert spec is not None
        # 应生成 W001 警告（amount 和 status 类型未声明）
        w001_warnings = [w for w in spec.parse_warnings if w.warning_id.startswith("W001")]
        assert len(w001_warnings) >= 1

    def test_golden_no_time_range(self):
        """允许宽松 2：时间范围未指定——生成 W002 warning。"""
        parser = DeveloperSpecParser()
        text = _read_fixture("fixtures/golden/golden_no_time_range.md")
        spec = parser.parse(text)
        assert spec is not None
        # 时间字段存在但未指定时间范围
        w002_warnings = [w for w in spec.parse_warnings if w.warning_id.startswith("W002")]
        assert len(w002_warnings) >= 1

    def test_golden_no_explicit_joins(self):
        """允许宽松 3：Join 未显式声明——Parser 不拒绝，joins=None。"""
        parser = DeveloperSpecParser()
        text = _read_fixture("fixtures/golden/golden_no_explicit_joins.md")
        spec = parser.parse(text)
        assert spec is not None
        # joins 为 None 或空列表均可（不显式声明 joins key 时为 None）
        assert spec.joins is None or spec.joins == []

    def test_golden_no_output_sort(self):
        """允许宽松 4：输出排序未声明——生成 W004 warning。"""
        parser = DeveloperSpecParser()
        text = _read_fixture("fixtures/golden/golden_no_output_sort.md")
        spec = parser.parse(text)
        assert spec is not None
        w004_warnings = [w for w in spec.parse_warnings if w.warning_id.startswith("W004")]
        assert len(w004_warnings) >= 1

    def test_golden_extra_markdown_text(self):
        """允许宽松 5：额外 Markdown 正文——保留在 description 中。"""
        parser = DeveloperSpecParser()
        text = _read_fixture("fixtures/golden/golden_extra_markdown_text.md")
        spec = parser.parse(text)
        assert spec is not None
        # description 应包含额外内容
        assert len(spec.description) > 50
        assert "注意事项" in spec.description

    def test_golden_chinese_column_comments(self):
        """允许宽松 6：中文列注释——归一化正确处理。"""
        parser = DeveloperSpecParser()
        text = _read_fixture("fixtures/golden/golden_chinese_column_comments.md")
        spec = parser.parse(text)
        assert spec is not None
        assert len(spec.input_tables) > 0


# ════════════════════════════════════════════
# Rejection 7 项禁止宽松
# ════════════════════════════════════════════

class TestParserRejection:
    """7 项禁止宽松场景——每项应抛出 ParseError。"""

    def test_reject_missing_metadata(self):
        """禁止宽松 1：无 fenced code block→ E001。"""
        parser = DeveloperSpecParser()
        text = _read_fixture("fixtures/reject/reject_missing_metadata.md")
        with pytest.raises(ParseError) as exc:
            parser.parse(text)
        assert exc.value.error_code == ParseErrorCode.E001_YAML_PARSE_FAILED

    def test_reject_empty_input_tables(self):
        """禁止宽松 2：source_tables 为空数组 → E002。"""
        parser = DeveloperSpecParser()
        text = _read_fixture("fixtures/reject/reject_empty_input_tables.md")
        with pytest.raises(ParseError) as exc:
            parser.parse(text)
        assert exc.value.error_code == ParseErrorCode.E002_MISSING_REQUIRED_FIELD

    def test_reject_metric_refs_missing_column(self):
        """禁止宽松 3：指标引用未声明字段 → E004。"""
        parser = DeveloperSpecParser()
        text = _read_fixture("fixtures/reject/reject_metric_refs_missing_column.md")
        with pytest.raises(ParseError) as exc:
            parser.parse(text)
        assert exc.value.error_code == ParseErrorCode.E004_UNDECLARED_FIELD_REF

    def test_reject_duplicate_table_alias(self):
        """禁止宽松 4：重复表别名 → E005。"""
        parser = DeveloperSpecParser()
        text = _read_fixture("fixtures/reject/reject_duplicate_table_alias.md")
        with pytest.raises(ParseError) as exc:
            parser.parse(text)
        assert exc.value.error_code == ParseErrorCode.E005_DUPLICATE_TABLE_ALIAS

    def test_reject_join_refs_missing_table(self):
        """禁止宽松 5：Join 引用不存在的表别名 → E005。"""
        parser = DeveloperSpecParser()
        text = _read_fixture("fixtures/reject/reject_join_refs_missing_table.md")
        with pytest.raises(ParseError) as exc:
            parser.parse(text)
        # Join 引用不存在表也使用 E005（别名相关错误）
        assert exc.value.error_code in (
            ParseErrorCode.E005_DUPLICATE_TABLE_ALIAS,
            ParseErrorCode.E004_UNDECLARED_FIELD_REF,
        )

    def test_reject_empty_output_columns(self):
        """禁止宽松 6：output_columns 为空 → E006。"""
        parser = DeveloperSpecParser()
        text = _read_fixture("fixtures/reject/reject_empty_output_columns.md")
        with pytest.raises(ParseError) as exc:
            parser.parse(text)
        assert exc.value.error_code == ParseErrorCode.E006_EMPTY_OUTPUT_COLUMNS

    def test_reject_free_sql_field(self):
        """禁止宽松 7：raw_sql 字段出现 → E007。"""
        parser = DeveloperSpecParser()
        text = _read_fixture("fixtures/reject/reject_free_sql_field.md")
        with pytest.raises(ParseError) as exc:
            parser.parse(text)
        assert exc.value.error_code == ParseErrorCode.E007_FREE_SQL_FIELD


# ════════════════════════════════════════════
# Hash 确定性
# ════════════════════════════════════════════

class TestParserHashDeterminism:
    """normalized_spec_hash 确定性验证。"""

    def test_same_input_same_hash(self):
        """同一输入两次解析产生相同的 normalized_spec_hash。"""
        parser = DeveloperSpecParser()
        text = _read_fixture("fixtures/golden/golden_no_time_range.md")

        spec1 = parser.parse(text)
        spec2 = parser.parse(text)

        assert spec1.spec_hash == spec2.spec_hash
        # hash 应为非空 16 位 hex 字符串
        assert len(spec1.spec_hash) == 16
        # 验证是十六进制
        int(spec1.spec_hash, 16)

    def test_different_input_different_hash(self):
        """不同输入产生不同的 spec_hash。"""
        parser = DeveloperSpecParser()
        text1 = _read_fixture("fixtures/golden/golden_no_time_range.md")
        text2 = _read_fixture("fixtures/golden/golden_no_explicit_joins.md")

        spec1 = parser.parse(text1)
        spec2 = parser.parse(text2)

        assert spec1.spec_hash != spec2.spec_hash
