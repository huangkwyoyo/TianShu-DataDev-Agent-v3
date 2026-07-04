"""Phase 8 Review Package 测试——CrossReference + SparkProvenance + SparkReviewPackage。

覆盖：
- CrossReference 模型——sql_artifact_id/sql_step_id → spark_step_id 映射
- CrossReference 不含 SQL 文本
- SparkProvenance 完整 hash 链
- SparkReviewPackage 统一交付物模型
- SparkReviewBuilder 从 PipelineState 构建 ReviewPackage
"""

from __future__ import annotations

from tianshu_datadev.spark.orchestrator import (
    SparkPipelineState,
    SparkPipelineStatus,
)
from tianshu_datadev.spark.review_builder import SparkReviewBuilder
from tianshu_datadev.spark.review_package import (
    CrossReference,
    SparkProvenance,
    SparkReviewPackage,
)

# ════════════════════════════════════════════
# CrossReference 模型测试
# ════════════════════════════════════════════


class TestCrossReference:
    """CrossReference——SQL artifact ↔ Spark step 映射。"""

    def test_cross_reference_creation(self):
        """CrossReference 基本构造——sql_artifact_id + sql_step_id → spark_step_id。"""
        xref = CrossReference(
            sql_artifact_id="artifact_001",
            sql_step_id="step_filter_0",
            spark_step_id="SparkFilterStep_0",
        )
        assert xref.sql_artifact_id == "artifact_001"
        assert xref.sql_step_id == "step_filter_0"
        assert xref.spark_step_id == "SparkFilterStep_0"

    def test_cross_reference_no_sql_text(self):
        """CrossReference 不包含 SQL 文本字段——仅用 ID 引用。"""
        xref = CrossReference(
            sql_artifact_id="artifact_001",
            sql_step_id="step_scan_0",
            spark_step_id="SparkReadStep_0",
        )
        data = xref.model_dump()
        # 确保模型中没有 sql_text/sql_code 字段
        assert "sql_text" not in data
        assert "sql_code" not in data
        assert "SELECT" not in str(data)

    def test_cross_reference_multiple_mappings(self):
        """多个 CrossReference 可以映射不同的 step 对。"""
        xref1 = CrossReference(
            sql_artifact_id="a1", sql_step_id="s1", spark_step_id="p1",
        )
        xref2 = CrossReference(
            sql_artifact_id="a1", sql_step_id="s2", spark_step_id="p2",
        )
        assert xref1.spark_step_id != xref2.spark_step_id


# ════════════════════════════════════════════
# SparkProvenance 模型测试
# ════════════════════════════════════════════


class TestSparkProvenance:
    """SparkProvenance——完整 hash 溯源链。"""

    def test_spark_provenance_creation(self):
        """SparkProvenance 含完整 hash 链——contract→plan→annotation→code→snapshot→verification。"""
        prov = SparkProvenance(
            contract_hash="abc123",
            spark_plan_hash="def456",
            annotation_hash="ghi789",
            compiled_code_sha256="jkl012",
            snapshot_id="snap_mno345",
            verification_report_id="vrpt_pqr678",
        )
        assert prov.contract_hash == "abc123"
        assert prov.spark_plan_hash == "def456"
        assert prov.annotation_hash == "ghi789"
        assert prov.compiled_code_sha256 == "jkl012"
        assert prov.snapshot_id == "snap_mno345"
        assert prov.verification_report_id == "vrpt_pqr678"

    def test_spark_provenance_optional_fields(self):
        """annotation_hash 和 snapshot_id 可为空——Developer 和 Snapshot 阶段可选。"""
        prov = SparkProvenance(
            contract_hash="abc123",
            spark_plan_hash="def456",
            compiled_code_sha256="jkl012",
        )
        assert prov.annotation_hash == ""
        assert prov.snapshot_id == ""
        assert prov.verification_report_id == ""

    def test_spark_provenance_hash_chain_order(self):
        """Provenance hash 链确定性顺序——
        contract → plan → annotation → code → snapshot → verification。"""
        prov = SparkProvenance(
            contract_hash="c1",
            spark_plan_hash="p1",
            annotation_hash="a1",
            compiled_code_sha256="code1",
            snapshot_id="s1",
            verification_report_id="v1",
        )
        data = prov.model_dump()
        keys = list(data.keys())
        # contract 应该在 plan 前面，plan 在 annotation 前面……
        assert keys.index("contract_hash") < keys.index("spark_plan_hash")
        assert keys.index("spark_plan_hash") < keys.index("annotation_hash")
        assert keys.index("annotation_hash") < keys.index("compiled_code_sha256")
        assert keys.index("compiled_code_sha256") < keys.index("snapshot_id")
        assert keys.index("snapshot_id") < keys.index("verification_report_id")


# ════════════════════════════════════════════
# SparkReviewPackage 模型测试
# ════════════════════════════════════════════


class TestSparkReviewPackage:
    """SparkReviewPackage——统一交付物。"""

    def test_review_package_creation(self):
        """SparkReviewPackage 含 provenance + cross_references + 整体状态。"""
        prov = SparkProvenance(
            contract_hash="abc123",
            spark_plan_hash="def456",
            compiled_code_sha256="jkl012",
        )
        pkg = SparkReviewPackage(
            package_id="pkg_001",
            provenance=prov,
            overall_status="ALL_CONSISTENT",
        )
        assert pkg.package_id == "pkg_001"
        assert pkg.provenance.contract_hash == "abc123"
        assert pkg.overall_status == "ALL_CONSISTENT"

    def test_review_package_with_cross_references(self):
        """ReviewPackage 可包含 CrossReference 列表。"""
        prov = SparkProvenance(
            contract_hash="abc123",
            spark_plan_hash="def456",
            compiled_code_sha256="jkl012",
        )
        xrefs = [
            CrossReference(
                sql_artifact_id="a1", sql_step_id="s1", spark_step_id="p1",
            ),
            CrossReference(
                sql_artifact_id="a1", sql_step_id="s2", spark_step_id="p2",
            ),
        ]
        pkg = SparkReviewPackage(
            package_id="pkg_002",
            provenance=prov,
            cross_references=xrefs,
            overall_status="ALL_CONSISTENT",
        )
        assert len(pkg.cross_references) == 2

    def test_review_package_no_sql_text_in_model(self):
        """ReviewPackage 整体不含 SQL 文本字段。"""
        prov = SparkProvenance(
            contract_hash="abc123",
            spark_plan_hash="def456",
            compiled_code_sha256="jkl012",
        )
        pkg = SparkReviewPackage(
            package_id="pkg_003",
            provenance=prov,
            overall_status="ALL_CONSISTENT",
        )
        data = pkg.model_dump()
        assert "sql_text" not in data
        assert "sql_code" not in data
        assert "SELECT" not in str(data)


# ════════════════════════════════════════════
# SparkReviewBuilder 测试
# ════════════════════════════════════════════


class TestSparkReviewBuilder:
    """SparkReviewBuilder——从 PipelineState 构建 ReviewPackage。"""

    def test_builder_creation(self):
        """SparkReviewBuilder 可无参创建。"""
        builder = SparkReviewBuilder()
        assert builder is not None

    def test_build_from_pipeline_state(self):
        """从 PipelineState 构建 SparkReviewPackage。"""
        builder = SparkReviewBuilder()
        state = SparkPipelineState(
            contract_hash="test_hash",
            spark_plan_hash="plan_hash_123",
            annotation_hash="ann_hash_456",
            compiled_code_sha256="code_hash_789",
            snapshot_id="snap_abc",
            verification_report_id="vrpt_def",
        )
        pkg = builder.build(state)
        assert isinstance(pkg, SparkReviewPackage)
        assert pkg.provenance.contract_hash == "test_hash"
        assert pkg.provenance.spark_plan_hash == "plan_hash_123"
        assert pkg.provenance.compiled_code_sha256 == "code_hash_789"

    def test_build_package_id_is_deterministic(self):
        """同一 PipelineState 多次 build 产出的 package_id 一致。"""
        builder = SparkReviewBuilder()
        state = SparkPipelineState(
            contract_hash="test_hash",
            spark_plan_hash="plan_hash_123",
            compiled_code_sha256="code_hash_789",
        )
        pkg1 = builder.build(state)
        pkg2 = builder.build(state)
        assert pkg1.package_id == pkg2.package_id

    def test_build_reflects_pipeline_overall_status(self):
        """ReviewPackage 的 overall_status 反映 PipelineState 的全局状态。"""
        builder = SparkReviewBuilder()
        state = SparkPipelineState(
            contract_hash="test_hash",
            spark_plan_hash="plan_hash_123",
            compiled_code_sha256="code_hash_789",
            overall_status=SparkPipelineStatus.ALL_CONSISTENT,
        )
        pkg = builder.build(state)
        assert pkg.overall_status == "ALL_CONSISTENT"

    def test_build_with_repair_needed_status(self):
        """REPAIR_NEEDED 状态的 Pipeline → ReviewPackage 含 repair_info。"""
        builder = SparkReviewBuilder()
        state = SparkPipelineState(
            contract_hash="test_hash",
            spark_plan_hash="plan_hash_123",
            compiled_code_sha256="code_hash_789",
            overall_status=SparkPipelineStatus.REPAIR_NEEDED,
        )
        state.errors.append("[COMPILER] 编译错误——不支持的 step 类型")
        pkg = builder.build(state)
        assert pkg.overall_status == "REPAIR_NEEDED"
        assert len(pkg.repair_info) > 0
