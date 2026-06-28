"""测试 Phase 2 artifacts 数据模型——Schema 验证、路由表。

覆盖：
- ReviewFeedback 拒绝 extra 字段
- ReviewFeedback 拒绝非法 target 值
- ReviewFeedback 路由表覆盖全部 5 个 target
"""

import pytest
from pydantic import ValidationError

from tianshu_datadev.artifacts.models import (
    VALID_REVIEW_TARGETS,
    REVIEW_ROUTING_TABLE,
    ArtifactRef,
    DataTransformContractLite,
    HumanReviewItem,
    ReviewFeedback,
    ReviewPackageManifest,
    ValidationSummaryArtifact,
)


class TestReviewFeedbackSchema:
    """ReviewFeedback schema 严格性测试。"""

    def test_review_feedback_rejects_extra_fields(self):
        """ReviewFeedback 必须拒绝 extra 字段（extra="forbid"）。"""
        with pytest.raises(ValidationError):
            ReviewFeedback(
                request_id="req_001",
                review_package_id="pkg_abc",
                developer_spec_hash="abc123",
                source_manifest_hash="def456",
                sql_build_plan_hash="ghi789",
                sql_artifact_hash="jkl012",
                target="SQL_PLAN",
                finding_type="wrong_join_key",
                comment="关联键错误",
                suggested_resolution="请修正关联键",
                extra_unknown_field="不应该存在",  # 非法额外字段
            )

    def test_review_feedback_rejects_invalid_target(self):
        """target 非法值应被拒绝——即便 Pydantic 不自动校验枚举，也应通过业务验证。"""
        # 创建合法的 ReviewFeedback（Pydantic 层面通过）
        feedback = ReviewFeedback(
            request_id="req_001",
            review_package_id="pkg_abc",
            developer_spec_hash="abc123",
            source_manifest_hash="def456",
            sql_build_plan_hash="ghi789",
            sql_artifact_hash="jkl012",
            target="INVALID_TARGET",
            finding_type="test",
            comment="测试",
            suggested_resolution="测试",
        )

        # 业务层验证：target 必须在 VALID_REVIEW_TARGETS 中
        assert ReviewFeedback.validate_target("INVALID_TARGET") is False
        assert ReviewFeedback.validate_target("SQL_PLAN") is True

        # 非法 target 应被检测
        is_valid = feedback.target in VALID_REVIEW_TARGETS
        assert is_valid is False

    def test_review_feedback_valid_targets_accepted(self):
        """所有 5 个合法 target 值应被接受。"""
        for target in VALID_REVIEW_TARGETS:
            feedback = ReviewFeedback(
                request_id="req_001",
                review_package_id="pkg_abc",
                developer_spec_hash="abc123",
                source_manifest_hash="def456",
                sql_build_plan_hash="ghi789",
                sql_artifact_hash="jkl012",
                target=target,
                finding_type="test",
                comment="测试",
                suggested_resolution="测试",
            )
            assert feedback.target == target
            assert ReviewFeedback.validate_target(target) is True

    def test_review_feedback_routing_table_complete(self):
        """路由表必须覆盖全部 5 个 target。"""
        for target in VALID_REVIEW_TARGETS:
            assert target in REVIEW_ROUTING_TABLE, (
                f"路由表缺少 target='{target}' 的入口"
            )
        # 验证路由表无多余条目
        assert set(REVIEW_ROUTING_TABLE.keys()) == set(VALID_REVIEW_TARGETS)

    def test_review_feedback_required_fields(self):
        """ReviewFeedback 缺少必填字段时应拒绝。"""
        with pytest.raises(ValidationError):
            ReviewFeedback(
                # 缺少 request_id
                review_package_id="pkg_abc",
                developer_spec_hash="abc123",
                source_manifest_hash="def456",
                sql_build_plan_hash="ghi789",
                sql_artifact_hash="jkl012",
                target="SQL_PLAN",
                finding_type="test",
                comment="test",
                suggested_resolution="test",
            )


class TestDataTransformContractLite:
    """DataTransformContract-lite 模型测试。"""

    def test_default_version_is_lite(self):
        """默认 version 必须是 "lite"。"""
        contract = DataTransformContractLite(
            contract_id="dtc_lite_test",
            source_sqlbuildplan_hash="abc123",
        )
        assert contract.version == "lite"
        assert contract.source_phase == "phase-2"

    def test_deterministic_contract_id(self):
        """相同 plan_hash → 相同 contract_id。"""
        id1 = DataTransformContractLite.generate_contract_id("abc123")
        id2 = DataTransformContractLite.generate_contract_id("abc123")
        assert id1 == id2

        # 不同 plan_hash → 不同 contract_id
        id3 = DataTransformContractLite.generate_contract_id("def456")
        assert id1 != id3

    def test_deterministic_contract_hash(self):
        """相同 contract 内容 → 相同 hash。"""
        c1 = DataTransformContractLite(
            contract_id="dtc_lite_abc",
            source_sqlbuildplan_hash="abc123",
            input_tables=[],
            input_columns=[],
            grouping_keys=["col_a"],
        )
        c2 = DataTransformContractLite(
            contract_id="dtc_lite_abc",
            source_sqlbuildplan_hash="abc123",
            input_tables=[],
            input_columns=[],
            grouping_keys=["col_a"],
        )
        h1 = DataTransformContractLite.compute_contract_hash(c1)
        h2 = DataTransformContractLite.compute_contract_hash(c2)
        assert h1 == h2

        # 不同内容 → 不同 hash
        c3 = DataTransformContractLite(
            contract_id="dtc_lite_abc",
            source_sqlbuildplan_hash="abc123",
            input_tables=[],
            input_columns=[],
            grouping_keys=["col_b"],  # 不同的 group key
        )
        h3 = DataTransformContractLite.compute_contract_hash(c3)
        assert h1 != h3

    def test_no_sql_code_fields(self):
        """Contract 模型不应包含 SQL 代码相关字段。"""
        contract = DataTransformContractLite(
            contract_id="dtc_lite_test",
            source_sqlbuildplan_hash="abc123",
        )
        data = contract.model_dump()
        # 确认不包含任何 SQL 代码字段
        assert "sql" not in data
        assert "raw_sql" not in data
        assert "sql_text" not in data
        assert "compiled_sql" not in data


class TestArtifactRef:
    """ArtifactRef 模型测试。"""

    def test_artifact_ref_creation(self):
        """ArtifactRef 正常创建。"""
        ref = ArtifactRef(path="sql/main.sql", sha256="abc123")
        assert ref.path == "sql/main.sql"
        assert ref.sha256 == "abc123"

    def test_artifact_ref_extra_fields_rejected(self):
        """ArtifactRef 应拒绝额外字段。"""
        with pytest.raises(ValidationError):
            ArtifactRef(
                path="sql/main.sql",
                sha256="abc123",
                unexpected_field=True,
            )


class TestReviewPackageManifest:
    """ReviewPackageManifest 模型测试。"""

    def test_deterministic_package_id(self):
        """相同 request_id → 相同 package_id。"""
        id1 = ReviewPackageManifest.generate_package_id("req_001")
        id2 = ReviewPackageManifest.generate_package_id("req_001")
        assert id1 == id2

    def test_empty_manifest(self):
        """空清单创建正常。"""
        manifest = ReviewPackageManifest(
            request_id="req_001",
            package_id="pkg_test",
            created_at="2026-01-01T00:00:00Z",
        )
        assert manifest.artifacts == []
        assert manifest.retry_count == 0


class TestHumanReviewItem:
    """HumanReviewItem 模型测试。"""

    def test_review_item_creation(self):
        """HumanReviewItem 正常创建。"""
        item = HumanReviewItem(
            item_id="hr_001",
            category="join_evidence",
            description="确认关联键正确性",
            severity="warning",
            related_artifact="planning/relationship_hypotheses.md",
        )
        assert item.category == "join_evidence"
        assert item.severity == "warning"


class TestValidationSummaryArtifact:
    """ValidationSummaryArtifact 模型测试。"""

    def test_validation_id_deterministic(self):
        """相同 plan_id → 相同 validation_id。"""
        id1 = ValidationSummaryArtifact.generate_validation_id("plan_abc")
        id2 = ValidationSummaryArtifact.generate_validation_id("plan_abc")
        assert id1 == id2

    def test_validation_summary_creation(self):
        """ValidationSummaryArtifact 正常创建。"""
        summary = ValidationSummaryArtifact(
            validation_id="val_test",
            plan_id="plan_abc",
            validator_passed=True,
            perf_all_passed=True,
            blocking_count=0,
            warning_count=0,
        )
        assert summary.validator_passed is True
