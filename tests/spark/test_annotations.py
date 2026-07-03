"""Phase 6 标注模型 + AnnotationValidator 测试。"""

from __future__ import annotations

import pytest

from tianshu_datadev.spark.annotations import (
    AnnotatedSparkPlan,
    AnnotationValidator,
    AnnotationWarning,
    StepAnnotation,
    StepIntent,
    compute_annotation_hash,
)


class TestStepAnnotation:
    """StepAnnotation 模型测试。"""

    def test_annotation_creation(self):
        """基本创建。"""
        ann = StepAnnotation(
            step_id="SparkReadStep_0",
            step_index=0,
            step_type="read",
            intent=StepIntent.SOURCE,
            intent_detail="读取订单明细表",
            operation_summary="从 inputs 读取 dwd.order_detail",
        )
        assert ann.step_id == "SparkReadStep_0"
        assert ann.step_index == 0
        assert ann.intent == StepIntent.SOURCE
        assert ann.intent_detail == "读取订单明细表"

    def test_annotation_defaults(self):
        """默认值测试。"""
        ann = StepAnnotation(
            step_id="test_1",
            step_index=1,
            step_type="filter",
            intent=StepIntent.CLEAN,
        )
        assert ann.downstream_step_ids == []
        assert ann.review_flags == []
        assert ann.intent_detail == ""
        assert ann.operation_summary == ""

    def test_annotation_extra_field_rejected(self):
        """拒绝 extra 字段。"""
        with pytest.raises(Exception):
            StepAnnotation(
                step_id="test",
                step_index=0,
                step_type="read",
                intent=StepIntent.SOURCE,
                not_a_field="bad",  # type: ignore[call-arg]
            )


class TestAnnotationWarning:
    """AnnotationWarning 模型测试。"""

    def test_warning_creation(self):
        """基本创建。"""
        w = AnnotationWarning(
            warning_id="warn_001",
            step_id="step_0",
            severity="WARN",
            category="missing_filter",
            description="可能缺少日期过滤条件",
            suggestion="考虑添加 date 过滤",
        )
        assert w.warning_id == "warn_001"
        assert w.severity == "WARN"

    def test_warning_global(self):
        """全局疑点（无 step_id）。"""
        w = AnnotationWarning(
            warning_id="warn_global",
            severity="INFO",
            category="dataset_size",
            description="数据集可能较大",
        )
        assert w.step_id is None


class TestAnnotatedSparkPlan:
    """AnnotatedSparkPlan 模型测试。"""

    def test_annotated_plan_creation(self):
        """基本创建。"""
        annotations = [
            StepAnnotation(
                step_id="read_0", step_index=0, step_type="read",
                intent=StepIntent.SOURCE,
            ),
            StepAnnotation(
                step_id="filter_0", step_index=1, step_type="filter",
                intent=StepIntent.CLEAN,
            ),
        ]
        plan = AnnotatedSparkPlan(
            plan_id="spark_test",
            baseline_plan_hash="abc123",
            annotations=annotations,
            warnings=[],
        )
        assert plan.plan_id == "spark_test"
        assert len(plan.annotations) == 2
        assert plan.annotator_version == "v1"


class TestAnnotationValidator:
    """AnnotationValidator 校验规则测试。"""

    def test_valid_annotations_pass(self):
        """合法的标注通过校验。"""
        annotations = [
            StepAnnotation(
                step_id="step_0", step_index=0, step_type="read",
                intent=StepIntent.SOURCE,
            ),
            StepAnnotation(
                step_id="step_1", step_index=1, step_type="filter",
                intent=StepIntent.CLEAN,
            ),
        ]
        plan = AnnotatedSparkPlan(
            plan_id="test",
            baseline_plan_hash="abc",
            annotations=annotations,
            warnings=[],
        )
        validator = AnnotationValidator()
        result = validator.validate(
            plan,
            expected_step_count=2,
            valid_step_ids={"step_0", "step_1"},
        )
        assert result.is_valid is True
        assert len(result.errors) == 0

    def test_count_mismatch_blocked(self):
        """标注数量与 steps 数量不一致 → 阻断。"""
        annotations = [
            StepAnnotation(
                step_id="step_0", step_index=0, step_type="read",
                intent=StepIntent.SOURCE,
            ),
        ]
        plan = AnnotatedSparkPlan(
            plan_id="test",
            baseline_plan_hash="abc",
            annotations=annotations,
            warnings=[],
        )
        validator = AnnotationValidator()
        result = validator.validate(
            plan,
            expected_step_count=3,  # 不一致
            valid_step_ids={"step_0"},
        )
        assert result.is_valid is False
        assert "不一致" in result.errors[0]

    def test_unknown_step_id_blocked(self):
        """step_id 不在 baseline 中 → 阻断。"""
        annotations = [
            StepAnnotation(
                step_id="step_0", step_index=0, step_type="read",
                intent=StepIntent.SOURCE,
            ),
        ]
        plan = AnnotatedSparkPlan(
            plan_id="test",
            baseline_plan_hash="abc",
            annotations=annotations,
            warnings=[],
        )
        validator = AnnotationValidator()
        result = validator.validate(
            plan,
            expected_step_count=1,
            valid_step_ids={"step_1"},  # step_0 不在集合中
        )
        assert result.is_valid is False
        assert "step_0" in result.errors[0]

    def test_duplicate_step_id_blocked(self):
        """重复 step_id → 阻断。"""
        annotations = [
            StepAnnotation(
                step_id="step_0", step_index=0, step_type="read",
                intent=StepIntent.SOURCE,
            ),
            StepAnnotation(
                step_id="step_0", step_index=1, step_type="filter",  # 重复
                intent=StepIntent.CLEAN,
            ),
        ]
        plan = AnnotatedSparkPlan(
            plan_id="test",
            baseline_plan_hash="abc",
            annotations=annotations,
            warnings=[],
        )
        validator = AnnotationValidator()
        result = validator.validate(
            plan,
            expected_step_count=2,
            valid_step_ids={"step_0"},
        )
        assert result.is_valid is False
        assert "重复" in result.errors[0]

    def test_review_warning_triggers_flag(self):
        """REVIEW 级别 warning → HumanReviewSuggested。"""
        plan = AnnotatedSparkPlan(
            plan_id="test",
            baseline_plan_hash="abc",
            annotations=[
                StepAnnotation(
                    step_id="step_0", step_index=0, step_type="read",
                    intent=StepIntent.SOURCE,
                ),
            ],
            warnings=[
                AnnotationWarning(
                    warning_id="w1",
                    severity="REVIEW",
                    category="semantic_mismatch",
                    description="可疑语义",
                ),
            ],
        )
        validator = AnnotationValidator()
        result = validator.validate(
            plan,
            expected_step_count=1,
            valid_step_ids={"step_0"},
        )
        assert result.is_valid is True  # REVIEW 不阻断
        assert result.human_review_suggested is True

    def test_info_warning_no_flag(self):
        """INFO 级别 warning 不触发 HumanReviewSuggested。"""
        plan = AnnotatedSparkPlan(
            plan_id="test",
            baseline_plan_hash="abc",
            annotations=[
                StepAnnotation(
                    step_id="step_0", step_index=0, step_type="read",
                    intent=StepIntent.SOURCE,
                ),
            ],
            warnings=[
                AnnotationWarning(
                    warning_id="w1",
                    severity="INFO",
                    category="info_note",
                    description="仅供参考",
                ),
            ],
        )
        validator = AnnotationValidator()
        result = validator.validate(
            plan,
            expected_step_count=1,
            valid_step_ids={"step_0"},
        )
        assert result.is_valid is True
        assert result.human_review_suggested is False


class TestAnnotationHash:
    """annotation_hash 确定性测试。"""

    def test_hash_deterministic(self):
        """相同标注 → 相同 hash。"""
        annotations = [
            StepAnnotation(
                step_id="step_0", step_index=0, step_type="read",
                intent=StepIntent.SOURCE,
            ),
        ]
        plan1 = AnnotatedSparkPlan(
            plan_id="test",
            baseline_plan_hash="abc",
            annotations=annotations,
            warnings=[],
        )
        plan2 = AnnotatedSparkPlan(
            plan_id="test",
            baseline_plan_hash="abc",
            annotations=[StepAnnotation(
                step_id="step_0", step_index=0, step_type="read",
                intent=StepIntent.SOURCE,
            )],
            warnings=[],
        )
        h1 = compute_annotation_hash(plan1)
        h2 = compute_annotation_hash(plan2)
        assert h1 == h2
        assert len(h1) == 64  # SHA-256

    def test_hash_different_content(self):
        """不同标注 → 不同 hash。"""
        plan1 = AnnotatedSparkPlan(
            plan_id="test",
            baseline_plan_hash="abc",
            annotations=[
                StepAnnotation(
                    step_id="step_0", step_index=0, step_type="read",
                    intent=StepIntent.SOURCE,
                ),
            ],
            warnings=[],
        )
        plan2 = AnnotatedSparkPlan(
            plan_id="test",
            baseline_plan_hash="abc",
            annotations=[
                StepAnnotation(
                    step_id="step_0", step_index=0, step_type="read",
                    intent=StepIntent.CLEAN,  # 不同 intent
                ),
            ],
            warnings=[],
        )
        h1 = compute_annotation_hash(plan1)
        h2 = compute_annotation_hash(plan2)
        assert h1 != h2
