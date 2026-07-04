"""Phase 8 SparkDeveloperService 测试——LLM 语义标注 + prompt 回归。

覆盖：
- SparkDeveloperService 基本构造
- annotate() 产出 AnnotatedSparkPlan
- Prompt 不含 SQL 文本/DeveloperSpec 引用
- AnnotationValidator 校验产出
- Mock LLM 注入——确定性 fixture 验证 prompt 结构
"""

from __future__ import annotations

import pytest

from tianshu_datadev.spark.annotations import (
    AnnotatedSparkPlan,
    StepAnnotation,
    StepIntent,
)
from tianshu_datadev.spark.developer import SparkDeveloperService
from tianshu_datadev.spark.models import (
    SparkFilterStep,
    SparkPlan,
    SparkProjectColumn,
    SparkProjectStep,
    SparkReadStep,
    SparkWindowExpr,
    SparkWindowFunction,
    SparkWindowStep,
)

# ════════════════════════════════════════════
# 测试辅助——构造最小 SparkPlan
# ════════════════════════════════════════════


def _make_simple_plan() -> SparkPlan:
    """构造一个含 scan + filter + project 的最小 SparkPlan。"""
    return SparkPlan(
        plan_id="spark_test_001",
        version="v1",
        source_phase="phase-5",
        source_contract_hash="test_hash_123",
        steps=[
            SparkReadStep(
                alias="od",
                source_name="dwd.order_detail",
                input_key="order_detail",
            ),
            SparkFilterStep(
                input_alias="od",
                operator="EQ",
                left="od.order_status",
                right="'paid'",
            ),
            SparkProjectStep(
                input_alias="_f0",
                columns=[
                    SparkProjectColumn(column_name="order_id", alias="order_id"),
                    SparkProjectColumn(column_name="amount", alias="amount"),
                ],
            ),
        ],
    )


def _make_window_plan() -> SparkPlan:
    """构造一个含窗口函数的 SparkPlan——用于验证 prompt 中窗口函数描述。"""
    return SparkPlan(
        plan_id="spark_test_window",
        version="v1",
        source_phase="phase-5",
        source_contract_hash="test_hash_456",
        steps=[
            SparkReadStep(
                alias="od",
                source_name="dwd.order_detail",
                input_key="order_detail",
            ),
            SparkWindowStep(
                input_alias="od",
                expressions=[
                    SparkWindowExpr(
                        function=SparkWindowFunction.ROW_NUMBER,
                        alias="rn",
                        input_column="",
                        partition_by=["order_id"],
                        order_by=["amount"],
                    ),
                ],
            ),
        ],
    )


# ════════════════════════════════════════════
# Mock LLM——返回确定性 AnnotatedSparkPlan
# ════════════════════════════════════════════


def _mock_llm_annotate(spark_plan: SparkPlan) -> AnnotatedSparkPlan:
    """确定性 mock——不调用真实 LLM，直接基于 SparkPlan 字段构造标注。"""
    annotations: list[StepAnnotation] = []
    intent_map = {
        "SparkReadStep": StepIntent.SOURCE,
        "SparkFilterStep": StepIntent.CLEAN,
        "SparkProjectStep": StepIntent.SHAPE,
        "SparkSortStep": StepIntent.SHAPE,
        "SparkLimitStep": StepIntent.SHAPE,
        "SparkWindowStep": StepIntent.RANK,
    }

    for i, step in enumerate(spark_plan.steps):
        step_type = type(step).__name__
        step_id = f"{step_type}_{i}"
        intent = intent_map.get(step_type, StepIntent.SHAPE)
        annotations.append(
            StepAnnotation(
                step_id=step_id,
                step_index=i,
                step_type=step.step_type.value if hasattr(step.step_type, "value") else str(step.step_type),
                intent=intent,
                intent_detail=f"Mock 标注——{step_type} 第 {i} 步",
                operation_summary=f"执行 {step_type} 操作",
            )
        )

    return AnnotatedSparkPlan(
        plan_id=spark_plan.plan_id,
        baseline_plan_hash=SparkPlan.compute_plan_hash(spark_plan),
        annotations=annotations,
        warnings=[],
    )


# ════════════════════════════════════════════
# TestSparkDeveloperService——基本构造
# ════════════════════════════════════════════


class TestSparkDeveloperService:
    """SparkDeveloperService 基本构造与接口。"""

    def test_creation_with_mock_llm(self):
        """可用 mock LLM callable 创建。"""
        svc = SparkDeveloperService(llm_call=_mock_llm_annotate)
        assert svc is not None

    def test_creation_without_llm_raises(self):
        """未提供 llm_call 时抛出 ValueError——防止静默空实现。"""
        with pytest.raises(ValueError, match="llm_call"):
            SparkDeveloperService(llm_call=None)

    def test_annotate_returns_annotated_plan(self):
        """annotate() 返回 AnnotatedSparkPlan。"""
        svc = SparkDeveloperService(llm_call=_mock_llm_annotate)
        plan = _make_simple_plan()
        result = svc.annotate(plan)
        assert isinstance(result, AnnotatedSparkPlan)
        assert result.plan_id == plan.plan_id

    def test_annotate_preserves_baseline_hash(self):
        """标注后的 baseline_plan_hash 与原始 SparkPlan hash 一致。"""
        svc = SparkDeveloperService(llm_call=_mock_llm_annotate)
        plan = _make_simple_plan()
        expected_hash = SparkPlan.compute_plan_hash(plan)
        result = svc.annotate(plan)
        assert result.baseline_plan_hash == expected_hash

    def test_annotate_produces_one_annotation_per_step(self):
        """标注数量 == steps 数量（一一对应）。"""
        svc = SparkDeveloperService(llm_call=_mock_llm_annotate)
        plan = _make_simple_plan()
        result = svc.annotate(plan)
        assert len(result.annotations) == len(plan.steps)

    def test_annotate_validates_output(self):
        """annotate() 内部调用 AnnotationValidator 校验——无效产出时抛出异常。"""
        # 使用一个会返回错误数量标注的 mock
        def _bad_llm(spark_plan: SparkPlan) -> AnnotatedSparkPlan:
            return AnnotatedSparkPlan(
                plan_id=spark_plan.plan_id,
                baseline_plan_hash=SparkPlan.compute_plan_hash(spark_plan),
                annotations=[],  # 数量不对——应为 3 个
                warnings=[],
            )

        svc = SparkDeveloperService(llm_call=_bad_llm)
        plan = _make_simple_plan()
        with pytest.raises(ValueError, match="标注数量"):
            svc.annotate(plan)

    def test_annotate_validates_step_ids(self):
        """annotate() 校验 step_id 不重复——重复时抛出异常。"""
        def _duplicate_llm(spark_plan: SparkPlan) -> AnnotatedSparkPlan:
            ann = StepAnnotation(
                step_id="same_id",
                step_index=0,
                step_type="read",
                intent=StepIntent.SOURCE,
            )
            return AnnotatedSparkPlan(
                plan_id=spark_plan.plan_id,
                baseline_plan_hash=SparkPlan.compute_plan_hash(spark_plan),
                annotations=[ann, ann, ann],  # 重复 step_id
                warnings=[],
            )

        svc = SparkDeveloperService(llm_call=_duplicate_llm)
        plan = _make_simple_plan()
        with pytest.raises(ValueError, match="重复"):
            svc.annotate(plan)


# ════════════════════════════════════════════
# TestPromptSafety——Prompt 不含敏感内容
# ════════════════════════════════════════════


class TestPromptSafety:
    """Prompt 构造安全——不含 SQL 文本/DeveloperSpec 引用。"""

    def test_prompt_contains_no_sql_keywords(self):
        """Prompt 中不含 SELECT/FROM/WHERE/JOIN 等 SQL 关键字。"""
        svc = SparkDeveloperService(llm_call=_mock_llm_annotate)
        plan = _make_simple_plan()
        prompt = svc._build_prompt(plan)

        sql_keywords = ["SELECT", "FROM", "WHERE", "JOIN", "GROUP BY", "ORDER BY", "HAVING", "UNION"]
        prompt_upper = prompt.upper()
        for kw in sql_keywords:
            assert kw not in prompt_upper, f"Prompt 含 SQL 关键字: {kw}"

    def test_prompt_contains_no_developer_spec_references(self):
        """Prompt 中不含 DeveloperSpec / SqlBuildPlan 引用。"""
        svc = SparkDeveloperService(llm_call=_mock_llm_annotate)
        plan = _make_simple_plan()
        prompt = svc._build_prompt(plan)

        forbidden = ["DeveloperSpec", "SqlBuildPlan", "sql_text", "sql_code", "spec_hash"]
        for term in forbidden:
            assert term not in prompt, f"Prompt 含禁止术语: {term}"

    def test_prompt_contains_step_type_descriptions(self):
        """Prompt 中包含 step 类型的结构化描述（不是 SQL 文本）。"""
        svc = SparkDeveloperService(llm_call=_mock_llm_annotate)
        plan = _make_simple_plan()
        prompt = svc._build_prompt(plan)

        # Prompt 应提及 step 类型
        assert "read" in prompt.lower() or "ReadStep" in prompt
        assert "filter" in prompt.lower() or "FilterStep" in prompt
        assert "project" in prompt.lower() or "ProjectStep" in prompt

    def test_prompt_window_plan_mentions_window_function(self):
        """窗口函数 plan 的 prompt 中包含窗口函数说明。"""
        svc = SparkDeveloperService(llm_call=_mock_llm_annotate)
        plan = _make_window_plan()
        prompt = svc._build_prompt(plan)

        assert "window" in prompt.lower() or "Window" in prompt
        assert "row_number" in prompt.lower() or "ROW_NUMBER" in prompt

    def test_prompt_is_plain_text_no_markdown_code_blocks(self):
        """Prompt 不含 markdown 代码块——不是代码补全，而是语义标注请求。"""
        svc = SparkDeveloperService(llm_call=_mock_llm_annotate)
        plan = _make_simple_plan()
        prompt = svc._build_prompt(plan)

        assert "```" not in prompt
