"""UncertaintyEntry 路由 + 合并 + 冲突检测——单元测试。

全部使用确定性构造，不依赖 LLM 或数据库。
"""

import pytest
from pydantic import ValidationError

from tianshu_datadev.api.pipeline import (
    _apply_uncertainties_to_spec,
    _check_label_rule_conflicts,
    _get_output_kind,
    _merge_uncertainties,
)
from tianshu_datadev.developer_spec.models import (
    CaseWhenDecl,
    CaseWhenRule,
    OpenQuestion,
    ParsedDeveloperSpec,
    UncertaintyEntry,
)
from tianshu_datadev.planning.proposal_promotion import ProposalPromotion
from tianshu_datadev.developer_spec.models import RequirementProposal


def _make_minimal_spec(**overrides) -> ParsedDeveloperSpec:
    """构建最小 ParsedDeveloperSpec——减少样板代码。"""
    defaults = dict(
        spec_id="test",
        spec_hash="abc123",
        title="Test",
        description="test",
        input_tables=[],
        metrics=[],
        dimensions=[],
        output_spec={"columns": [], "grain": []},
    )
    defaults.update(overrides)
    return ParsedDeveloperSpec(**defaults)


# ═══ 模型默认值 ═══

def test_uncertainty_entry_defaults():
    """UncertaintyEntry 默认值——output_kind=UNKNOWN, output_column=None。"""
    u = UncertaintyEntry(field_ref="x", description="test")
    assert u.output_kind == "UNKNOWN"
    assert u.output_column is None


def test_uncertainty_entry_invalid_output_kind():
    """output_kind 枚举约束——非法值抛 ValidationError。"""
    with pytest.raises(ValidationError):
        UncertaintyEntry(
            field_ref="x", description="test", output_kind="INVALID",
        )


# ═══ _get_output_kind 路由 ═══

def test_get_output_kind_exact_match():
    """精确匹配 output_column 返回正确的 output_kind。"""
    u = UncertaintyEntry(
        field_ref="risk_label_ref",
        output_column="risk_label",
        output_kind="LABEL",
        description="需要 CASE WHEN",
    )
    assert _get_output_kind("risk_label", [u]) == "LABEL"


def test_get_output_kind_none_output_column_skipped():
    """output_column=None 时跳过，返回 UNKNOWN。"""
    u = UncertaintyEntry(
        field_ref="x", output_column=None, output_kind="LABEL",
        description="缺少路由键",
    )
    assert _get_output_kind("x", [u]) == "UNKNOWN"


def test_get_output_kind_no_field_ref_parsing():
    """不解析 field_ref 字符串——仅通过 output_column 匹配。"""
    u = UncertaintyEntry(
        field_ref="case_when.parse_error.x",
        output_column=None,
        output_kind="LABEL",
        description="field_ref 含 'x' 但 output_column 为 None",
    )
    # 查询 "x"——field_ref 不被解析，output_column=None → 跳过
    assert _get_output_kind("x", [u]) == "UNKNOWN"


# ═══ uncertainties 透传 ═══

def test_proposal_promotion_passthrough_uncertainties():
    """ProposalPromotion.promote() 透传 uncertainties 到 spec。"""
    entry = UncertaintyEntry(
        field_ref="test_field",
        output_column="col_a",
        output_kind="LABEL",
        description="test",
    )
    proposal = RequirementProposal(
        proposal_id="p1",
        spec_hash="abc123",
        uncertainties=[entry],
    )
    spec = _make_minimal_spec(spec_hash="abc123")
    promoter = ProposalPromotion()
    result = promoter.promote(proposal, spec)
    assert len(result.uncertainties) == 1
    assert result.uncertainties[0].output_column == "col_a"


# ═══ _merge_uncertainties ═══

def test_merge_uncertainties_same_key_overwrite():
    """同键覆盖——新值覆盖旧值，异键保留。"""
    existing = [
        UncertaintyEntry(
            field_ref="fr_a", output_column="a", output_kind="LABEL",
            description="old a",
        ),
        UncertaintyEntry(
            field_ref="fr_b", output_column="b", output_kind="METRIC",
            description="old b",
        ),
    ]
    incoming = [
        UncertaintyEntry(
            field_ref="fr_a", output_column="a", output_kind="LABEL",
            description="new a",
        ),
    ]
    merged = _merge_uncertainties(existing, incoming)
    assert len(merged) == 2  # 保留异键 b，覆盖同键 a
    # 找到 output_column="a" 的条目——应为新值
    a_entries = [u for u in merged if u.output_column == "a"]
    assert len(a_entries) == 1
    assert a_entries[0].description == "new a"
    # output_column="b" 应保留
    b_entries = [u for u in merged if u.output_column == "b"]
    assert len(b_entries) == 1


def test_apply_uncertainties_to_spec_empty():
    """空列表——返回原 spec 不触发 model_copy。"""
    spec = _make_minimal_spec()
    result = _apply_uncertainties_to_spec(spec, [])
    assert result is spec  # 空列表不触发 copy


# ═══ JSON Schema ═══

def test_json_schema_requires_output_column_and_output_kind():
    """JSON Schema 的 uncertainties required 含 output_column 和 output_kind。"""
    from tianshu_datadev.planning.requirement_planner import (
        _REQUIREMENT_PLANNER_JSON_SCHEMA,
    )
    items = _REQUIREMENT_PLANNER_JSON_SCHEMA["properties"]["uncertainties"]["items"]
    assert "output_column" in items["required"]
    assert "output_kind" in items["required"]


# ═══ CASE 解析失败仍阻断 ═══

def test_case_when_parse_error_uncertainty_blocks():
    """CASE WHEN 解析失败的 uncertainty 产出阻断 OpenQuestion。"""
    from tianshu_datadev.api.pipeline import _extract_case_when_parse_errors
    from tianshu_datadev.developer_spec.models import RequirementPlannerOutput

    ue = UncertaintyEntry(
        field_ref="case_when_rules.parse_error.peak_type",
        output_column="peak_type",
        output_kind="LABEL",
        description="CASE WHEN 规则 'peak_type' 解析失败",
    )
    output = RequirementPlannerOutput(uncertainties=[ue])
    questions = _extract_case_when_parse_errors(output)
    assert len(questions) == 1
    assert questions[0].blocking is True


# ═══ 同列冲突 → blocking ═══

def test_label_rule_conflict_blocking():
    """同 output_column 在 label_rules 和 case_when_rules 中 → blocking。"""
    spec = _make_minimal_spec(
        label_rules=[
            CaseWhenDecl(output_column="x", branches=[], else_value="a"),
        ],
        case_when_rules=[
            CaseWhenRule(output_column="x", branches=[], else_value="b"),
        ],
    )
    questions = _check_label_rule_conflicts(spec)
    assert len(questions) == 1
    assert questions[0].blocking is True
    assert "x" in questions[0].description
