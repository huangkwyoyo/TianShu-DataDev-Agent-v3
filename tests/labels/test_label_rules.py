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
