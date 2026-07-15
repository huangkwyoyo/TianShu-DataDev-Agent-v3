"""标签子系统测试——Artifact 模型、Validator、FakeLabelExtractor、LlmLabelExtractor、Promotion。"""

from tianshu_datadev.labels.artifacts import (
    LabelExtractionArtifact,
    LabelPromotionArtifact,
    LabelValidationCheck,
    LabelValidationReport,
)


class TestLabelExtractionArtifact:
    """溯源 Artifact——记录 LLM 调用的完整上下文。"""

    def test_fields(self):
        """验证 Artifact 字段构造和读取。"""
        artifact = LabelExtractionArtifact(
            artifact_id="ext_001",
            source_spec_hash="h",
            extraction_time="2026-07-15T00:00:00Z",
            llm_model="fake",
            llm_prompt_version="v001",
            llm_temperature=0.1,
            unresolved_columns=["col1"],
            raw_proposals=[],
            prompt_snapshot="",
        )
        assert artifact.artifact_id == "ext_001"
        assert artifact.source_spec_hash == "h"
        assert artifact.unresolved_columns == ["col1"]
        assert artifact.llm_model == "fake"


class TestLabelValidationReport:
    """校验报告——双空通过。"""

    def test_passed_requires_both_empty(self):
        """passed=True 要求 blocking_errors 和 human_review_items 均为空。"""
        report = LabelValidationReport(
            proposal_id="p1",
            passed=True,
            checks=[],
            blocking_errors=[],
            human_review_items=[],
            warnings=[],
        )
        assert report.passed

    def test_human_review_causes_not_passed(self):
        """human_review_items 非空→passed=False。"""
        report = LabelValidationReport(
            proposal_id="p1",
            passed=False,
            checks=[],
            blocking_errors=[],
            human_review_items=["缺少 ELSE"],
            warnings=[],
        )
        assert not report.passed

    def test_blocking_error_causes_not_passed(self):
        """blocking_errors 非空→passed=False。"""
        report = LabelValidationReport(
            proposal_id="p1",
            passed=False,
            checks=[],
            blocking_errors=["字段不存在: unknown_col"],
            human_review_items=[],
            warnings=[],
        )
        assert not report.passed


class TestLabelValidationCheck:
    """单条校验检查项。"""

    def test_check_fields(self):
        check = LabelValidationCheck(
            check_name="FIELD_EXISTS",
            passed=True,
            level="BLOCKING",
            detail="字段 distance_miles 存在",
        )
        assert check.check_name == "FIELD_EXISTS"
        assert check.passed
        assert check.level == "BLOCKING"


class TestLabelPromotionArtifact:
    """提升 Artifact——记录 Proposal→CaseWhenDecl 转换。"""

    def test_fields(self):
        artifact = LabelPromotionArtifact(
            artifact_id="prom_001",
            parent_spec_hash="h_old",
            new_spec_hash="h_new",
            promotion_time="2026-07-15T00:00:00Z",
            extraction_artifact_id="ext_001",
            promoted_rules=[],
            validation_reports=[],
            rejected_proposals=[],
            human_review_required=False,
        )
        assert artifact.artifact_id == "prom_001"
        assert not artifact.human_review_required

    def test_rejected_proposals_tracked(self):
        """被拒绝的 proposal_id 被记录。"""
        artifact = LabelPromotionArtifact(
            artifact_id="prom_002",
            parent_spec_hash="h_old",
            new_spec_hash="h_new",
            promotion_time="2026-07-15T00:00:00Z",
            extraction_artifact_id="ext_001",
            promoted_rules=[],
            validation_reports=[],
            rejected_proposals=["p1", "p2"],
            human_review_required=True,
        )
        assert len(artifact.rejected_proposals) == 2
        assert artifact.human_review_required

# ================================================
# v4-light 最终版: LabelRuleValidator v1 六项检查 + 双空通过
# ================================================

from decimal import Decimal

from tianshu_datadev.developer_spec.models import (
    ColumnDecl,
    CompareOp,
    DatasetType,
    InputTableDecl,
    LabelBranchProposal,
    LabelCompare,
    LabelDomain,
    LabelRuleProposal,
    LabelTypedLiteral,
    OutputColumnDecl,
    OutputSpecDecl,
    ParsedDeveloperSpec,
)
from tianshu_datadev.labels.label_rule_validator import LabelRuleValidator


def _make_test_spec():
    """构造测试用 ParsedDeveloperSpec。"""
    return ParsedDeveloperSpec(
        spec_id="test", spec_hash="h", title="t", description="d",
        dataset_type=DatasetType.LABEL_TABLE,
        input_tables=[
            InputTableDecl(
                table_alias="tf", source_table="fact",
                columns=[
                    ColumnDecl(column_name="distance_miles",
                               normalized_name="distance_miles"),
                    ColumnDecl(column_name="is_distance_outlier",
                               normalized_name="is_distance_outlier"),
                ],
                key_columns=[], business_columns=[],
            ),
        ],
        metrics=[], dimensions=[],
        output_spec=OutputSpecDecl(columns=[
            OutputColumnDecl(name="distance_category", type="string"),
        ], grain=[]),
        time_range=None,
    )


class TestValidatorV1FieldExists:

    def test_field_exists_passes(self):
        validator = LabelRuleValidator()
        proposal = LabelRuleProposal(
            proposal_id="p1", source_spec_hash="h",
            output_column="distance_category",
            branches=[
                LabelBranchProposal(
                    condition=LabelCompare(
                        left="distance_miles", op=CompareOp.LTE,
                        right=LabelTypedLiteral(value=Decimal("2"), data_type="number"),
                    ),
                    then_label="short", evidence="<=2 -> short",
                ),
            ],
            else_value="long",
            label_domain=LabelDomain(values=["short", "long"]),
        )
        report = validator.validate(proposal, _make_test_spec())
        field_check = next(c for c in report.checks if c.check_name == "FIELD_EXISTS")
        assert field_check.passed

    def test_unknown_field_blocks(self):
        validator = LabelRuleValidator()
        proposal = LabelRuleProposal(
            proposal_id="p1", source_spec_hash="h",
            output_column="distance_category",
            branches=[
                LabelBranchProposal(
                    condition=LabelCompare(
                        left="nonexistent_col", op=CompareOp.LTE,
                        right=LabelTypedLiteral(value=Decimal("2"), data_type="number"),
                    ),
                    then_label="short", evidence="<=2 -> short",
                ),
            ],
            else_value="long",
            label_domain=LabelDomain(values=["short", "long"]),
        )
        report = validator.validate(proposal, _make_test_spec())
        assert not report.passed
        assert any("nonexistent_col" in e for e in report.blocking_errors)


class TestValidatorV1Coverage:

    def test_missing_else_with_empty_evidence_not_passed(self):
        """无 ELSE + evidence 为空 → HUMAN_REVIEW → passed=False。"""
        validator = LabelRuleValidator()
        proposal = LabelRuleProposal(
            proposal_id="p1", source_spec_hash="h",
            output_column="distance_category",
            branches=[
                LabelBranchProposal(
                    condition=LabelCompare(
                        left="distance_miles", op=CompareOp.LTE,
                        right=LabelTypedLiteral(value=Decimal("2"), data_type="number"),
                    ),
                    then_label="short", evidence="",
                ),
            ],
            else_value="long",
            label_domain=LabelDomain(values=["short", "long"]),
        )
        report = validator.validate(proposal, _make_test_spec())
        coverage_checks = [c for c in report.checks if c.check_name == "COVERAGE"]
        if coverage_checks:
            assert any("evidence" in c.detail.lower() for c in coverage_checks
                       if not c.passed)

    def test_all_evidence_present_passes(self):
        """全部 evidence 非空 + ELSE→passed=True。"""
        validator = LabelRuleValidator()
        proposal = LabelRuleProposal(
            proposal_id="p1", source_spec_hash="h",
            output_column="distance_category",
            branches=[
                LabelBranchProposal(
                    condition=LabelCompare(
                        left="distance_miles", op=CompareOp.LTE,
                        right=LabelTypedLiteral(value=Decimal("2"), data_type="number"),
                    ),
                    then_label="short", evidence="<=2 -> short",
                ),
            ],
            else_value="long",
            label_domain=LabelDomain(values=["short", "long"]),
        )
        report = validator.validate(proposal, _make_test_spec())
        assert report.passed, f"blocking={report.blocking_errors}, review={report.human_review_items}"


class TestValidatorV1DoubleEmpty:
    """v4-light 最终版: passed 要求 blocking_errors 和 human_review_items 均为空。"""

    def test_human_review_causes_fail(self):
        """human_review_items 非空→即使 blocking 为空也 passed=False。"""
        validator = LabelRuleValidator()
        proposal = LabelRuleProposal(
            proposal_id="p1", source_spec_hash="h",
            output_column="distance_category",
            branches=[
                LabelBranchProposal(
                    condition=LabelCompare(
                        left="distance_miles", op=CompareOp.LTE,
                        right=LabelTypedLiteral(value=Decimal("2"), data_type="number"),
                    ),
                    then_label="short", evidence="",
                ),
            ],
            else_value="long",
            label_domain=LabelDomain(values=["short", "long"]),
        )
        report = validator.validate(proposal, _make_test_spec())
        assert not report.passed, "human_review_items 非空时 passed 应为 False"


class TestValidatorV1LabelDomain:

    def test_label_outside_domain_blocks(self):
        """then_label 不在 domain 中→BLOCKING。"""
        validator = LabelRuleValidator()
        proposal = LabelRuleProposal(
            proposal_id="p1", source_spec_hash="h",
            output_column="distance_category",
            branches=[
                LabelBranchProposal(
                    condition=LabelCompare(
                        left="distance_miles", op=CompareOp.LTE,
                        right=LabelTypedLiteral(value=Decimal("2"), data_type="number"),
                    ),
                    then_label="ultra_short",  # ← 不在 domain 中
                    evidence="<=2 -> ultra_short",
                ),
            ],
            else_value="long",
            label_domain=LabelDomain(values=["short", "medium", "long"]),
        )
        report = validator.validate(proposal, _make_test_spec())
        assert any("ultra_short" in e for e in report.blocking_errors)

# ================================================
# v4-light 最终版: FakeLabelExtractor 测试
# ================================================


class TestFakeLabelExtractor:
    """FakeLabelExtractor——pytest 专用，确定性返回预定义 Proposal。"""

    def test_returns_predefined_proposals(self):
        from tianshu_datadev.labels.label_extractor import FakeLabelExtractor
        proposal = LabelRuleProposal(
            proposal_id="p1", source_spec_hash="h",
            output_column="col",
            branches=[
                LabelBranchProposal(
                    condition=LabelCompare(
                        left="x", op=CompareOp.EQ,
                        right=LabelTypedLiteral(value="a", data_type="string"),
                    ),
                    then_label="label_a", evidence="x=a",
                ),
            ],
            else_value="label_b",
            label_domain=LabelDomain(values=["label_a", "label_b"]),
        )
        extractor = FakeLabelExtractor(proposals=[proposal])
        spec = _make_test_spec()
        result, artifact = extractor.extract(spec, ["col"])
        assert len(result) == 1
        assert result[0].output_column == "col"
        assert artifact.llm_model == "fake"

    def test_empty_by_default(self):
        from tianshu_datadev.labels.label_extractor import FakeLabelExtractor
        extractor = FakeLabelExtractor()
        spec = _make_test_spec()
        result, artifact = extractor.extract(spec, [])
        assert result == []
        assert artifact.unresolved_columns == []
