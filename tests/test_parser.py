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

    @pytest.mark.parametrize(
        "unsafe_expression,desc",
        [
            ("1; DROP TABLE users; --", "分号+DROP注入"),
            ("1' OR '1'='1", "单引号布尔注入"),
            ("1--\nDELETE FROM users", "注释逃逸注入"),
            ("1/*comment*/", "块注释注入"),
        ],
    )
    def test_reject_unsafe_input_expression(self, unsafe_expression, desc):
        """禁止宽松 8：input_expression 含注入字符 → E008。"""
        parser = DeveloperSpecParser()
        # 构造含不安全 input_expression 的最小合法 DeveloperSpec
        text = f"""```markdown
---
spec:
  type: aggregate_table
  target_table: ads.test
  target_grain: [dt]
  summary: "安全测试"
  source_tables:
    - name: dwd.fact
      alias: f
      role: fact
      key_columns:
        - name: id
          type: bigint
          nullable: false
      business_columns:
        - name: val
          type: integer
          nullable: false
  metrics:
    - metric_name: unsafe_metric
      aggregation: SUM
      input_expression: "{unsafe_expression}"
      alias: unsafe_alias
  dimensions:
    - dimension_name: dt
      column_ref: dt
  output_columns:
    - name: dt
      type: date
    - name: unsafe_alias
      type: integer
---
# 测试 E008
```"""
        with pytest.raises(ParseError) as exc:
            parser.parse(text)
        assert exc.value.error_code == ParseErrorCode.E008_UNSAFE_EXPRESSION, (
            f"场景 '{desc}' 应抛出 E008，实际: {exc.value.error_code}"
        )

    def test_accept_safe_input_expression(self):
        """合法 input_expression——纯算术表达式通过校验。"""
        parser = DeveloperSpecParser()
        text = """```markdown
---
spec:
  type: aggregate_table
  target_table: ads.test
  target_grain: [dt]
  summary: "合法表达式测试"
  source_tables:
    - name: dwd.fact
      alias: f
      role: fact
      key_columns:
        - name: id
          type: bigint
          nullable: false
      business_columns:
        - name: quantity
          type: integer
          nullable: false
        - name: unit_price
          type: decimal
          nullable: false
  metrics:
    - metric_name: revenue
      aggregation: SUM
      input_expression: "quantity * unit_price"
      alias: revenue
  dimensions:
    - dimension_name: dt
      column_ref: dt
  output_columns:
    - name: dt
      type: date
    - name: revenue
      type: decimal
---
# 合法表达式
```
"""
        spec = parser.parse(text)
        assert spec is not None
        assert spec.metrics[0].input_expression == "quantity * unit_price"


# ════════════════════════════════════════════
# Hash 确定性
# ════════════════════════════════════════════

# ════════════════════════════════════════════
# ComputeSteps 解析——环检测与 DAG 校验
# ════════════════════════════════════════════

# 最小合法 Spec 模板——包含 compute_steps 的基础 YAML 结构
_MINIMAL_COMPUTE_STEPS_SPEC_TEMPLATE = """```markdown
---
spec:
  type: aggregate_table
  target_table: ads.test_cs
  target_grain: [id]
  summary: "ComputeSteps 解析测试"
  source_tables:
    - name: dwd.test_fact
      alias: t
      row_count: ~100万
      role: fact
      key_columns:
        - name: id
          type: bigint
          nullable: false
      business_columns:
        - name: val
          type: bigint
          nullable: true
{compute_steps_yaml}
  output_columns:
    - name: id
      type: bigint
    - name: result
      type: bigint
---
# 测试
```
"""

# 单个 ComputeStep 的最小合法声明
_VALID_STEP_TEMPLATE = """    - step_name: {name}
      source: {source}
      group_by: [id]
      metrics:
        - metric_name: cnt
          aggregation: COUNT
          input_column: id
          alias: cnt
      output_alias: {alias}"""


def _make_compute_steps_spec(steps_yaml: str) -> str:
    """构造含 compute_steps 声明的完整 Markdown 测试文本。"""
    return _MINIMAL_COMPUTE_STEPS_SPEC_TEMPLATE.format(
        compute_steps_yaml=steps_yaml,
    )


class TestComputeStepsParser:
    """compute_steps 解析——环检测、引用校验、合法 DAG 通过。"""

    # ── 禁止场景（应抛出 ParseError）──

    def test_self_loop_rejected(self):
        """自引用步骤 → ParseError（source 引用自己，被前向引用检查拦截）。

        注：自引用步骤的 source=自身，而自身尚未加入 step_names，
        因此前向引用检查（L854）先于自引用检查（L862）触发。
        错误码和消息均为前向引用路径。
        """
        steps = _VALID_STEP_TEMPLATE.format(
            name="step_a", source="step_a", alias="a_alias",
        )
        text = _make_compute_steps_spec(f"  compute_steps:\n{steps}\n")
        parser = DeveloperSpecParser()
        with pytest.raises(ParseError) as exc:
            parser.parse(text)
        assert exc.value.error_code == ParseErrorCode.E004_UNDECLARED_FIELD_REF
        assert "step_a" in exc.value.message
        assert "无效" in exc.value.message or "已声明的 step_name" in exc.value.message

    def test_forward_reference_rejected(self):
        """引用未声明的步骤 → ParseError（前向引用拦截）。"""
        steps = (
            _VALID_STEP_TEMPLATE.format(
                name="step_a", source="step_b", alias="a_alias",
            )
            + "\n"
            + _VALID_STEP_TEMPLATE.format(
                name="step_b", source="input", alias="b_alias",
            )
        )
        text = _make_compute_steps_spec(f"  compute_steps:\n{steps}\n")
        parser = DeveloperSpecParser()
        with pytest.raises(ParseError) as exc:
            parser.parse(text)
        assert exc.value.error_code == ParseErrorCode.E004_UNDECLARED_FIELD_REF
        assert "step_b" in exc.value.message

    def test_duplicate_step_name_rejected(self):
        """重复 step_name → ParseError。"""
        step = _VALID_STEP_TEMPLATE.format(
            name="step_a", source="input", alias="a_alias",
        )
        steps = f"{step}\n{step}"  # 两个同名步骤
        text = _make_compute_steps_spec(f"  compute_steps:\n{steps}\n")
        parser = DeveloperSpecParser()
        with pytest.raises(ParseError) as exc:
            parser.parse(text)
        assert exc.value.error_code == ParseErrorCode.E005_DUPLICATE_TABLE_ALIAS
        assert "重复" in exc.value.message

    def test_list_source_duplicate_element_rejected(self):
        """source 列表中元素重复 → ParseError。"""
        steps = (
            _VALID_STEP_TEMPLATE.format(
                name="step_a", source="input", alias="a_alias",
            )
            + "\n"
            + "    - step_name: step_b\n"
            + "      source: [step_a, step_a]\n"
            + "      group_by: [id]\n"
            + "      metrics:\n"
            + "        - metric_name: cnt2\n"
            + "          aggregation: SUM\n"
            + "          input_column: val\n"
            + "          alias: cnt2\n"
            + "      output_alias: b_alias"
        )
        text = _make_compute_steps_spec(f"  compute_steps:\n{steps}\n")
        parser = DeveloperSpecParser()
        with pytest.raises(ParseError) as exc:
            parser.parse(text)
        assert exc.value.error_code == ParseErrorCode.E004_UNDECLARED_FIELD_REF
        assert "重复" in exc.value.message

    def test_missing_output_alias_rejected(self):
        """缺少 output_alias → ParseError。"""
        steps = (
            "    - step_name: step_a\n"
            "      source: input\n"
            "      group_by: [id]\n"
            "      metrics:\n"
            "        - metric_name: cnt\n"
            "          aggregation: COUNT\n"
            "          input_column: id\n"
            "          alias: cnt\n"
        )
        text = _make_compute_steps_spec(f"  compute_steps:\n{steps}\n")
        parser = DeveloperSpecParser()
        with pytest.raises(ParseError) as exc:
            parser.parse(text)
        assert exc.value.error_code == ParseErrorCode.E001_YAML_PARSE_FAILED
        assert "output_alias" in exc.value.message

    def test_forward_reference_in_list_source_rejected(self):
        """source 列表中含未声明步骤 → ParseError（前向引用拦截，列表形式）。"""
        steps = (
            _VALID_STEP_TEMPLATE.format(
                name="step_a", source="input", alias="a_alias",
            )
            + "\n"
            + "    - step_name: step_b\n"
            + "      source: [step_a, step_c]\n"
            + "      group_by: [id]\n"
            + "      metrics:\n"
            + "        - metric_name: cnt2\n"
            + "          aggregation: SUM\n"
            + "          input_column: val\n"
            + "          alias: cnt2\n"
            + "      output_alias: b_alias"
        )
        text = _make_compute_steps_spec(f"  compute_steps:\n{steps}\n")
        parser = DeveloperSpecParser()
        with pytest.raises(ParseError) as exc:
            parser.parse(text)
        assert exc.value.error_code == ParseErrorCode.E004_UNDECLARED_FIELD_REF
        assert "step_c" in exc.value.message

    def test_forward_reference_in_list_source_with_input_rejected(self):
        """source 列表含 'input' + 未声明步骤 → ParseError（前向引用拦截）。"""
        steps = (
            "    - step_name: step_a\n"
            "      source: [input, step_b]\n"
            "      group_by: [id]\n"
            "      metrics:\n"
            "        - metric_name: cnt\n"
            "          aggregation: COUNT\n"
            "          input_column: id\n"
            "          alias: cnt\n"
            "      output_alias: a_alias\n"
            + _VALID_STEP_TEMPLATE.format(
                name="step_b", source="input", alias="b_alias",
            )
        )
        text = _make_compute_steps_spec(f"  compute_steps:\n{steps}\n")
        parser = DeveloperSpecParser()
        with pytest.raises(ParseError) as exc:
            parser.parse(text)
        assert exc.value.error_code == ParseErrorCode.E004_UNDECLARED_FIELD_REF
        assert "step_b" in exc.value.message

    def test_empty_source_list_rejected(self):
        """source 为空列表 → ParseError。"""
        steps = (
            "    - step_name: step_a\n"
            "      source: []\n"
            "      group_by: [id]\n"
            "      metrics:\n"
            "        - metric_name: cnt\n"
            "          aggregation: COUNT\n"
            "          input_column: id\n"
            "          alias: cnt\n"
            "      output_alias: a_alias"
        )
        text = _make_compute_steps_spec(f"  compute_steps:\n{steps}\n")
        parser = DeveloperSpecParser()
        with pytest.raises(ParseError) as exc:
            parser.parse(text)
        assert exc.value.error_code == ParseErrorCode.E001_YAML_PARSE_FAILED
        assert "不能为空" in exc.value.message

    def test_undeclared_metric_field_in_input_source_rejected(self):
        """source=input 时指标引用未声明字段 → ParseError。"""
        steps = (
            "    - step_name: step_a\n"
            "      source: input\n"
            "      group_by: [id]\n"
            "      metrics:\n"
            "        - metric_name: bad_metric\n"
            "          aggregation: SUM\n"
            "          input_column: nonexistent_col\n"
            "          alias: bad\n"
            "      output_alias: a_alias"
        )
        text = _make_compute_steps_spec(f"  compute_steps:\n{steps}\n")
        parser = DeveloperSpecParser()
        with pytest.raises(ParseError) as exc:
            parser.parse(text)
        assert exc.value.error_code == ParseErrorCode.E004_UNDECLARED_FIELD_REF
        assert "nonexistent_col" in exc.value.message

    # ── 允许场景（应成功解析）──

    def test_linear_chain_accepted(self):
        """2 步线性链 A→B 通过解析，compute_steps 结构正确。"""
        steps = (
            _VALID_STEP_TEMPLATE.format(
                name="daily_agg", source="input", alias="daily_alias",
            )
            + "\n"
            + "    - step_name: monthly_avg\n"
            + "      source: daily_agg\n"
            + "      group_by: [id]\n"
            + "      metrics:\n"
            + "        - metric_name: avg_val\n"
            + "          aggregation: AVG\n"
            + "          input_column: cnt\n"
            + "          alias: avg_val\n"
            + "      output_alias: monthly_alias"
        )
        text = _make_compute_steps_spec(f"  compute_steps:\n{steps}\n")
        parser = DeveloperSpecParser()
        spec = parser.parse(text)
        assert spec is not None
        assert spec.compute_steps is not None
        assert len(spec.compute_steps) == 2
        assert spec.compute_steps[0].step_name == "daily_agg"
        assert spec.compute_steps[0].source == "input"
        assert spec.compute_steps[1].step_name == "monthly_avg"
        assert spec.compute_steps[1].source == "daily_agg"

    def test_diamond_dag_accepted(self):
        """4 步菱形 DAG A→B,A→C,B→D,C→D 通过解析。"""
        steps = (
            _VALID_STEP_TEMPLATE.format(
                name="root", source="input", alias="root_alias",
            )
            + "\n"
            + _VALID_STEP_TEMPLATE.format(
                name="branch_a", source="root", alias="a_alias",
            )
            + "\n"
            + _VALID_STEP_TEMPLATE.format(
                name="branch_b", source="root", alias="b_alias",
            )
            + "\n"
            + "    - step_name: merged\n"
            + "      source: [branch_a, branch_b]\n"
            + "      group_by: [id]\n"
            + "      metrics:\n"
            + "        - metric_name: total\n"
            + "          aggregation: SUM\n"
            + "          input_column: cnt\n"
            + "          alias: total\n"
            + "      output_alias: merged_alias"
        )
        text = _make_compute_steps_spec(f"  compute_steps:\n{steps}\n")
        parser = DeveloperSpecParser()
        spec = parser.parse(text)
        assert spec is not None
        assert spec.compute_steps is not None
        assert len(spec.compute_steps) == 4
        # 验证合流步骤的 source 为列表
        merged = spec.compute_steps[3]
        assert merged.step_name == "merged"
        assert isinstance(merged.source, list)
        assert set(merged.source) == {"branch_a", "branch_b"}

    def test_single_input_step_accepted(self):
        """单步 input source → 通过解析，compute_steps 含 1 个步骤。"""
        steps = _VALID_STEP_TEMPLATE.format(
            name="only_step", source="input", alias="only_alias",
        )
        text = _make_compute_steps_spec(f"  compute_steps:\n{steps}\n")
        parser = DeveloperSpecParser()
        spec = parser.parse(text)
        assert spec is not None
        assert spec.compute_steps is not None
        assert len(spec.compute_steps) == 1
        assert spec.compute_steps[0].source == "input"

    def test_no_compute_steps_is_none(self):
        """不声明 compute_steps → spec.compute_steps 为 None（向后兼容）。"""
        text = _make_compute_steps_spec("")
        parser = DeveloperSpecParser()
        spec = parser.parse(text)
        assert spec is not None
        assert spec.compute_steps is None

    def test_compute_steps_hash_determinism(self):
        """相同 compute_steps 两次解析产生相同 spec_hash。"""
        steps = (
            _VALID_STEP_TEMPLATE.format(
                name="step_a", source="input", alias="a_alias",
            )
            + "\n"
            + _VALID_STEP_TEMPLATE.format(
                name="step_b", source="step_a", alias="b_alias",
            )
        )
        text = _make_compute_steps_spec(f"  compute_steps:\n{steps}\n")
        parser = DeveloperSpecParser()
        spec1 = parser.parse(text)
        spec2 = parser.parse(text)
        assert spec1.spec_hash == spec2.spec_hash
        assert len(spec1.spec_hash) == 16


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
