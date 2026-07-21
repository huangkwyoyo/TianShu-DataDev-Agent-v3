"""TimeTransformExpr + DerivedGroupKey + Predicate.left 扩展——模型校验测试。"""

import pytest
from pydantic import ValidationError

from tianshu_datadev.planning.models import (
    TimeTransformExpr,
    DerivedGroupKey,
    SafeIdentifier,
    Predicate,
    PredicateOperator,
    ColumnRef,
    SqlLiteral,
)


class TestTimeTransformExpr:
    """TimeTransformExpr 模型校验测试。"""

    def test_valid_hour_expr(self):
        """合法 HOUR 表达式应通过校验。"""
        expr = TimeTransformExpr(
            source_column=SafeIdentifier("pickup_at"),
            source_table=SafeIdentifier("ft"),
            time_function="HOUR",
        )
        assert expr.time_function == "HOUR"
        assert str(expr.source_column) == "pickup_at"

    def test_rejects_invalid_time_function(self):
        """非法时间函数应被 Literal 拒绝。"""
        with pytest.raises(ValidationError):
            TimeTransformExpr(
                source_column=SafeIdentifier("pickup_at"),
                source_table=SafeIdentifier("ft"),
                time_function="DAY",  # MVP 仅 HOUR
            )


class TestDerivedGroupKey:
    """DerivedGroupKey 模型校验测试。"""

    def test_valid_derived_key(self):
        """合法 DerivedGroupKey 应通过校验。"""
        expr = TimeTransformExpr(
            source_column=SafeIdentifier("pickup_at"),
            source_table=SafeIdentifier("ft"),
            time_function="HOUR",
        )
        key = DerivedGroupKey(alias="pickup_hour", expr=expr)
        assert key.alias == "pickup_hour"
        assert key.expr.time_function == "HOUR"


class TestPredicateWithTimeTransform:
    """Predicate.left 扩展——允许 TimeTransformExpr。"""

    def test_predicate_left_with_time_transform(self):
        """Predicate.left 为 TimeTransformExpr 时应通过校验。"""
        expr = TimeTransformExpr(
            source_column=SafeIdentifier("pickup_at"),
            source_table=SafeIdentifier("ft"),
            time_function="HOUR",
        )
        pred = Predicate(
            left=expr,
            operator=PredicateOperator.IN,
            right=[
                SqlLiteral(value=7), SqlLiteral(value=8), SqlLiteral(value=9),
            ],
        )
        assert isinstance(pred.left, TimeTransformExpr)
        assert pred.operator == PredicateOperator.IN

    def test_predicate_left_still_accepts_column_ref(self):
        """Predicate.left 仍应接受 ColumnRef——向后兼容。"""
        col = ColumnRef(
            table_ref=SafeIdentifier("ft"),
            column_name=SafeIdentifier("borough"),
            normalized_name=SafeIdentifier("borough"),
        )
        pred = Predicate(
            left=col,
            operator=PredicateOperator.EQ,
            right=SqlLiteral(value="Manhattan"),
        )
        assert isinstance(pred.left, ColumnRef)


# ════════════════════════════════════════════
# Task 2: Developer Spec 模型测试
# ════════════════════════════════════════════

from tianshu_datadev.developer_spec.models import (
    DerivedDimensionDecl,
    CaseWhenBranch,
    CaseWhenRule,
    UncertaintyEntry,
    RequirementPlannerOutput,
    RequirementProposal,
    ParsedDeveloperSpec,
    DimensionDecl,
    MetricDecl,
    AggregationType,
)


class TestDerivedDimensionDecl:
    """派生维度声明模型测试。"""

    def test_valid_derived_dimension(self):
        dd = DerivedDimensionDecl(
            dimension_name="pickup_hour",
            source_column="pickup_at",
            source_table="ft",
            time_function="HOUR",
        )
        assert dd.dimension_name == "pickup_hour"

    def test_rejects_invalid_time_function(self):
        with pytest.raises(ValidationError):
            DerivedDimensionDecl(
                dimension_name="pickup_day",
                source_column="pickup_at",
                source_table="ft",
                time_function="DAY",
            )


class TestCaseWhenRule:
    """CASE WHEN 规则模型测试。"""

    def test_valid_case_when_rule(self):
        rule = CaseWhenRule(
            output_column="peak_type",
            branches=[
                CaseWhenBranch(
                    condition={"node_type": "COMPARE", "left": "pickup_hour",
                               "op": "IN", "right": {"node_type": "LITERAL",
                               "value": [7, 8, 9], "data_type": "number"}},
                    then_value="高峰",
                ),
            ],
            else_value="平峰",
        )
        assert rule.output_column == "peak_type"
        assert len(rule.branches) == 1

    def test_default_factory_empty_lists(self):
        """default_factory=list 确保默认空列表。"""
        rule = CaseWhenRule(output_column="test", else_value="unknown")
        assert rule.branches == []


class TestRequirementPlannerOutput:
    """LLM 输出模型测试。"""

    def test_empty_output_valid(self):
        output = RequirementPlannerOutput()
        assert output.dimensions == []
        assert output.derived_dimensions == []
        assert output.metrics == []
        assert output.case_when_rules == []
        assert output.uncertainties == []

    def test_rejects_unknown_fields(self):
        with pytest.raises(ValidationError):
            RequirementPlannerOutput(unknown_field="should_reject")


class TestRequirementProposal:
    """系统 Artifact 模型测试。"""

    def test_minimal_proposal(self):
        proposal = RequirementProposal(
            proposal_id="test-001",
            spec_hash="abc123",
        )
        assert proposal.proposal_id == "test-001"
        assert proposal.llm_model == ""
        assert proposal.inference_time_ms == 0
        assert proposal.total_inferred == 0
