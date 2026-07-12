"""Phase 8 SparkDeveloperService 测试——LLM 语义标注 + prompt 回归。

覆盖：
- SparkDeveloperService 基本构造
- annotate() 产出 AnnotatedSparkPlan
- Prompt 不含 SQL 文本/DeveloperSpec 引用
- AnnotationValidator 校验产出
- Mock LLM 注入——确定性 fixture 验证 prompt 结构
- ProviderAdapter 集成路径——复用既有 llm.adapters.base.ProviderAdapter + PromptManager
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
                input_alias="od",
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

    def test_prompt_from_template_safe(self):
        """使用 PromptManager 加载的模板渲染后也不含 SQL 关键字。"""
        # PromptManager 已可作为公共入口直接导入（循环导入已于 2026-07-04 修复）
        from tianshu_datadev.prompts.manager import PromptManager

        pm = PromptManager()
        plan = _make_simple_plan()
        prompt = SparkDeveloperService._build_prompt(plan, prompt_manager=pm)

        sql_keywords = ["SELECT", "FROM", "WHERE", "JOIN", "GROUP BY", "ORDER BY", "HAVING", "UNION"]
        prompt_upper = prompt.upper()
        for kw in sql_keywords:
            assert kw not in prompt_upper, f"模板渲染后 Prompt 含 SQL 关键字: {kw}"


# ════════════════════════════════════════════
# TestProviderAdapterIntegration——既有 ProviderAdapter → Developer 集成路径
# ════════════════════════════════════════════


class TestProviderAdapterIntegration:
    """既有 ProviderAdapter（llm.adapters.base）→ SparkDeveloperService 集成路径。

    所有测试使用 mock ProviderAdapter + PromptManager（不调真实 LLM），
    验证 from_provider_adapter() 工厂方法、重试逻辑、错误处理。
    """

    def test_from_provider_adapter_creates_service(self):
        """from_provider_adapter() 创建可用实例——使用既有 ProviderAdapter 接口。"""
        from tianshu_datadev.prompts.manager import PromptManager

        class TestAdapter:
            """最小 ProviderAdapter——仅用于验证实例创建。"""
            def invoke(self, system_message, user_message, json_schema, model, temperature):
                return {}
            def provider_name(self):
                return "test"

        adapter = TestAdapter()
        pm = PromptManager()
        svc = SparkDeveloperService.from_provider_adapter(adapter, pm)
        assert svc is not None
        assert isinstance(svc, SparkDeveloperService)

    def test_adapter_integration_with_mock_llm(self):
        """ProviderAdapter 注入后 annotate() 正常产出——模拟真实 LLM 路径。"""
        from tianshu_datadev.prompts.manager import PromptManager

        class MockAdapter:
            """模拟 ProviderAdapter.invoke()——返回确定性 AnnotatedSparkPlan 的 dict。"""

            def invoke(
                self, system_message: str, user_message: str,
                json_schema: dict, model: str, temperature: float,
            ) -> dict:
                plan = _make_simple_plan()
                return _mock_llm_annotate(plan).model_dump(mode="json")

            def provider_name(self) -> str:
                return "mock"

        adapter = MockAdapter()
        pm = PromptManager()
        svc = SparkDeveloperService.from_provider_adapter(adapter, pm)
        plan = _make_simple_plan()
        result = svc.annotate(plan)

        assert isinstance(result, AnnotatedSparkPlan)
        assert len(result.annotations) == len(plan.steps)
        assert result.plan_id == plan.plan_id

    def test_adapter_retry_on_failure(self):
        """LLM 调用首次失败后重试——重试成功则正常返回。"""
        from tianshu_datadev.llm.adapters.base import AdapterError
        from tianshu_datadev.prompts.manager import PromptManager

        call_count = [0]

        class RetryAdapter:
            def invoke(
                self, system_message: str, user_message: str,
                json_schema: dict, model: str, temperature: float,
            ) -> dict:
                call_count[0] += 1
                if call_count[0] < 2:
                    raise AdapterError("模拟临时故障", provider="test")
                plan = _make_simple_plan()
                return _mock_llm_annotate(plan).model_dump(mode="json")

            def provider_name(self) -> str:
                return "retry"

        adapter = RetryAdapter()
        pm = PromptManager()
        svc = SparkDeveloperService.from_provider_adapter(
            adapter, pm, max_llm_retries=1
        )
        plan = _make_simple_plan()
        result = svc.annotate(plan)

        # 首次失败 + 1 次重试成功
        assert call_count[0] == 2
        assert isinstance(result, AnnotatedSparkPlan)

    def test_adapter_exhausts_retries_raises(self):
        """重试耗尽后仍然失败——抛出异常。"""
        from tianshu_datadev.llm.adapters.base import AdapterError
        from tianshu_datadev.prompts.manager import PromptManager

        class AlwaysFailAdapter:
            def invoke(
                self, system_message: str, user_message: str,
                json_schema: dict, model: str, temperature: float,
            ) -> dict:
                raise AdapterError("模拟永久故障", provider="test")

            def provider_name(self) -> str:
                return "fail"

        adapter = AlwaysFailAdapter()
        pm = PromptManager()
        svc = SparkDeveloperService.from_provider_adapter(
            adapter, pm, max_llm_retries=1
        )
        plan = _make_simple_plan()

        with pytest.raises(AdapterError, match="模拟永久故障"):
            svc.annotate(plan)

    def test_adapter_non_retryable_error_raises_immediately(self):
        """非 AdapterError（如 ValidationError）立即抛出——不浪费重试次数。"""
        from tianshu_datadev.prompts.manager import PromptManager

        call_count = [0]

        class ValidationFailAdapter:
            def invoke(
                self, system_message: str, user_message: str,
                json_schema: dict, model: str, temperature: float,
            ) -> dict:
                call_count[0] += 1
                raise ValueError("Schema 校验失败——不可重试")

            def provider_name(self) -> str:
                return "validation_fail"

        adapter = ValidationFailAdapter()
        pm = PromptManager()
        svc = SparkDeveloperService.from_provider_adapter(
            adapter, pm, max_llm_retries=2
        )
        plan = _make_simple_plan()

        with pytest.raises(ValueError, match="Schema 校验失败"):
            svc.annotate(plan)

        # 非 AdapterError 只调用 1 次——不浪费重试次数
        assert call_count[0] == 1


# ════════════════════════════════════════════
# TestProvenanceOverride——plan_id 和 hash 由 Python 确定性覆盖
# ════════════════════════════════════════════


class TestProvenanceOverride:
    """Provenance 边界——plan_id / baseline_plan_hash 不由 LLM 决定。"""

    def test_llm_returns_empty_hash_overwritten(self):
        """LLM 返回空 baseline_plan_hash → 被确定性覆盖为正确值。"""
        plan = _make_simple_plan()
        expected_hash = SparkPlan.compute_plan_hash(plan)

        def _empty_hash_llm(spark_plan: SparkPlan) -> AnnotatedSparkPlan:
            ann = _mock_llm_annotate(spark_plan)
            return ann.model_copy(update={"baseline_plan_hash": ""})

        svc = SparkDeveloperService(llm_call=_empty_hash_llm)
        result = svc.annotate(plan)

        assert result.baseline_plan_hash == expected_hash
        assert result.baseline_plan_hash != ""

    def test_llm_returns_wrong_hash_overwritten(self):
        """LLM 返回错误 hash → 被确定性覆盖为正确值。"""
        plan = _make_simple_plan()
        expected_hash = SparkPlan.compute_plan_hash(plan)
        wrong_hash = "deadbeef" * 8

        def _wrong_hash_llm(spark_plan: SparkPlan) -> AnnotatedSparkPlan:
            ann = _mock_llm_annotate(spark_plan)
            return ann.model_copy(update={"baseline_plan_hash": wrong_hash})

        svc = SparkDeveloperService(llm_call=_wrong_hash_llm)
        result = svc.annotate(plan)

        assert result.baseline_plan_hash == expected_hash
        assert result.baseline_plan_hash != wrong_hash

    def test_llm_returns_wrong_plan_id_overwritten(self):
        """LLM 返回错误 plan_id → 被确定性覆盖为正确值。"""
        plan = _make_simple_plan()

        def _wrong_plan_id_llm(spark_plan: SparkPlan) -> AnnotatedSparkPlan:
            ann = _mock_llm_annotate(spark_plan)
            return ann.model_copy(update={"plan_id": "fake_plan_id"})

        svc = SparkDeveloperService(llm_call=_wrong_plan_id_llm)
        result = svc.annotate(plan)

        assert result.plan_id == plan.plan_id
        assert result.plan_id != "fake_plan_id"

    def test_injected_llm_call_and_adapter_path_both_override(self):
        """注入式 llm_call 和 adapter 包装路径——两种路径均确定性覆盖。"""
        from tianshu_datadev.prompts.manager import PromptManager

        plan = _make_simple_plan()
        expected_hash = SparkPlan.compute_plan_hash(plan)
        expected_plan_id = plan.plan_id

        # 路径 1：注入式 llm_call（mock 路径）
        svc_injected = SparkDeveloperService(llm_call=_mock_llm_annotate)
        result_injected = svc_injected.annotate(plan)
        assert result_injected.plan_id == expected_plan_id
        assert result_injected.baseline_plan_hash == expected_hash

        # 路径 2：adapter 包装路径——通过 from_provider_adapter 创建
        class PathAdapter:
            """适配器——返回错误 provenance 的 mock 数据。"""

            def invoke(
                self, system_message: str, user_message: str,
                json_schema: dict, model: str, temperature: float,
            ) -> dict:
                # 返回错误 provenance——验证会被代码覆盖
                data = _mock_llm_annotate(plan).model_dump(mode="json")
                data["plan_id"] = "adapter_wrong_id"
                data["baseline_plan_hash"] = ""
                return data

            def provider_name(self) -> str:
                return "path_test"

        adapter = PathAdapter()
        pm = PromptManager()
        svc_adapter = SparkDeveloperService.from_provider_adapter(adapter, pm)
        result_adapter = svc_adapter.annotate(plan)

        assert result_adapter.plan_id == expected_plan_id
        assert result_adapter.baseline_plan_hash == expected_hash


# ════════════════════════════════════════════
# TestWarningContract——category 封闭 + 确定性过滤
# ════════════════════════════════════════════


class TestWarningContract:
    """Warning contract——仅 4 种合法 category，其他被确定性过滤。"""

    def test_unknown_category_warnings_filtered(self):
        """LLM 返回未知 category → 被确定性过滤，不影响合法 warning。"""
        from tianshu_datadev.spark.annotations import AnnotationWarning

        plan = _make_simple_plan()

        def _unknown_category_llm(spark_plan: SparkPlan) -> AnnotatedSparkPlan:
            ann = _mock_llm_annotate(spark_plan)
            return ann.model_copy(update={
                "warnings": [
                    AnnotationWarning(
                        warning_id="w001",
                        severity="WARN",
                        category="step_count_anomaly",  # 合法
                        description="合法 warning——应保留",
                    ),
                    AnnotationWarning(
                        warning_id="w002",
                        severity="REVIEW",
                        category="missing_evidence_chain",  # 未知 category
                        description="非法 warning——应被过滤",
                    ),
                    AnnotationWarning(
                        warning_id="w003",
                        severity="INFO",
                        category="baseline_hash_missing",  # 未知 category
                        description="非法 warning——应被过滤",
                    ),
                ],
            })

        svc = SparkDeveloperService(llm_call=_unknown_category_llm)
        result = svc.annotate(plan)

        # 只有合法 category 的 warning 保留
        categories = [w.category for w in result.warnings]
        assert "step_count_anomaly" in categories
        assert "missing_evidence_chain" not in categories
        assert "baseline_hash_missing" not in categories
        assert len(result.warnings) == 1

    def test_baseline_hash_warning_filtered(self):
        """LLM 返回 baseline_plan_hash 缺失的 warning → 被过滤。"""
        from tianshu_datadev.spark.annotations import AnnotationWarning

        plan = _make_simple_plan()

        def _hash_warning_llm(spark_plan: SparkPlan) -> AnnotatedSparkPlan:
            ann = _mock_llm_annotate(spark_plan)
            return ann.model_copy(update={
                "warnings": [
                    AnnotationWarning(
                        warning_id="w_hash",
                        severity="INFO",
                        category="baseline_hash_missing",
                        description="输入中未提供 baseline_plan_hash",
                    ),
                ],
            })

        svc = SparkDeveloperService(llm_call=_hash_warning_llm)
        result = svc.annotate(plan)

        assert len(result.warnings) == 0

    def test_evidence_chain_warning_filtered(self):
        """LLM 返回 evidence_chain 为空的 warning → 被过滤。"""
        from tianshu_datadev.spark.annotations import AnnotationWarning

        plan = _make_simple_plan()

        def _evidence_warning_llm(spark_plan: SparkPlan) -> AnnotatedSparkPlan:
            ann = _mock_llm_annotate(spark_plan)
            return ann.model_copy(update={
                "warnings": [
                    AnnotationWarning(
                        warning_id="w_ev",
                        severity="REVIEW",
                        category="missing_evidence_chain",
                        description="join步骤缺少 evidence_chain 元数据",
                    ),
                ],
            })

        svc = SparkDeveloperService(llm_call=_evidence_warning_llm)
        result = svc.annotate(plan)

        assert len(result.warnings) == 0

    def test_unknown_review_does_not_trigger_human_review(self):
        """未知 REVIEW warning 被过滤 → 不触发 human_review_suggested。"""
        from tianshu_datadev.spark.annotations import AnnotationWarning

        plan = _make_simple_plan()

        def _unknown_review_llm(spark_plan: SparkPlan) -> AnnotatedSparkPlan:
            ann = _mock_llm_annotate(spark_plan)
            return ann.model_copy(update={
                "warnings": [
                    AnnotationWarning(
                        warning_id="w_review",
                        severity="REVIEW",
                        category="unknown_freetext_review",  # 未知
                        description="自由审查意见——应被过滤",
                    ),
                ],
            })

        svc = SparkDeveloperService(llm_call=_unknown_review_llm)
        result = svc.annotate(plan)

        # warning 被过滤，也不触发 human_review_suggested
        assert len(result.warnings) == 0

        # 通过 Validator 再次确认
        from tianshu_datadev.spark.annotations import AnnotationValidator
        validator = AnnotationValidator()
        valid_step_ids = {f"{type(s).__name__}_{i}" for i, s in enumerate(plan.steps)}
        validation = validator.validate(
            annotated=result,
            expected_step_count=len(plan.steps),
            valid_step_ids=valid_step_ids,
        )
        assert validation.is_valid
        assert not validation.human_review_suggested

    def test_valid_warnings_preserved(self):
        """已知 category 的合法 warning → 全部保留。"""
        from tianshu_datadev.spark.annotations import (
            ALLOWED_WARNING_CATEGORIES,
            AnnotationWarning,
        )

        plan = _make_simple_plan()
        # 为所有 4 种合法 category 各构造一个 warning
        expected_categories = sorted(ALLOWED_WARNING_CATEGORIES)

        def _all_valid_llm(spark_plan: SparkPlan) -> AnnotatedSparkPlan:
            ann = _mock_llm_annotate(spark_plan)
            return ann.model_copy(update={
                "warnings": [
                    AnnotationWarning(
                        warning_id=f"w_{cat}",
                        severity="WARN",
                        category=cat,
                        description=f"合法 warning: {cat}",
                    )
                    for cat in expected_categories
                ],
            })

        svc = SparkDeveloperService(llm_call=_all_valid_llm)
        result = svc.annotate(plan)

        result_categories = sorted([w.category for w in result.warnings])
        assert result_categories == expected_categories
        assert len(result.warnings) == len(expected_categories)

    def test_warning_with_invalid_step_id_rejected(self):
        """warning.step_id 不在 baseline 中 → 被确定性拒绝。"""
        from tianshu_datadev.spark.annotations import AnnotationWarning

        plan = _make_simple_plan()

        def _invalid_step_id_llm(spark_plan: SparkPlan) -> AnnotatedSparkPlan:
            ann = _mock_llm_annotate(spark_plan)
            return ann.model_copy(update={
                "warnings": [
                    AnnotationWarning(
                        warning_id="w_valid",
                        severity="WARN",
                        category="missing_cleaning_step",
                        step_id="SparkReadStep_0",  # 存在
                        description="合法 warning——step_id 有效",
                    ),
                    AnnotationWarning(
                        warning_id="w_invalid",
                        severity="WARN",
                        category="step_count_anomaly",
                        step_id="NotExistStep_99",  # 不存在
                        description="非法 warning——step_id 无效",
                    ),
                ],
            })

        svc = SparkDeveloperService(llm_call=_invalid_step_id_llm)
        result = svc.annotate(plan)

        # 只有 step_id 有效的 warning 保留
        assert len(result.warnings) == 1
        assert result.warnings[0].warning_id == "w_valid"


# ════════════════════════════════════════════
# TestEvidenceChainTransmission——证据链传递
# ════════════════════════════════════════════


class TestEvidenceChainTransmission:
    """evidence_chain 传递——有数据时完整传递，无数据时不伪造。"""

    def test_evidence_chain_transmitted_when_present(self):
        """evidence_chain 有数据时 → ContractJoin → mapper → SparkJoinStep 完整传递。"""
        from tianshu_datadev.artifacts.models import ContractJoin
        from tianshu_datadev.spark.mapper import _map_joins
        from tianshu_datadev.spark.models import SparkJoinStep, UnsupportedPattern

        # 构造含证据链的 ContractJoin
        evidence = {
            "evidence_id": "ev_001",
            "level": "STRONG",
            "action": "AUTO_ADOPT",
            "left_field": {"raw": "user_id", "normalized": "user_id"},
            "right_field": {"raw": "id", "normalized": "id"},
            "evidence_checks": ["field_name_match: MATCH", "data_type_match: MATCH"],
            "detail": "字段名和类型均匹配",
        }
        contract_join = ContractJoin(
            join_id="ev_001",
            left_table="od",
            right_table="ud",
            left_key="user_id",
            right_key="id",
            join_type="INNER",
            evidence_chain=evidence,
            level="STRONG",
        )

        unsupported: list[UnsupportedPattern] = []
        spark_joins = _map_joins([contract_join], unsupported)

        assert len(spark_joins) == 1
        js = spark_joins[0]
        assert isinstance(js, SparkJoinStep)
        assert js.evidence_chain["evidence_id"] == "ev_001"
        assert js.evidence_chain["level"] == "STRONG"
        assert js.evidence_chain["left_field"]["raw"] == "user_id"
        assert js.evidence_chain["right_field"]["normalized"] == "id"
        assert "field_name_match: MATCH" in js.evidence_chain["evidence_checks"]
        assert len(unsupported) == 0

    def test_evidence_chain_empty_when_absent(self):
        """无 evidence_chain 数据时 → 保持为空，不伪造。"""
        from tianshu_datadev.artifacts.models import ContractJoin
        from tianshu_datadev.spark.mapper import _map_joins
        from tianshu_datadev.spark.models import UnsupportedPattern

        contract_join = ContractJoin(
            join_id="ev_empty",
            left_table="od",
            right_table="ud",
            left_key="user_id",
            right_key="id",
            join_type="LEFT",
            evidence_chain={},  # 空
            level="MEDIUM",
        )

        unsupported: list[UnsupportedPattern] = []
        spark_joins = _map_joins([contract_join], unsupported)

        assert len(spark_joins) == 1
        # 空字典，不伪造数据
        assert spark_joins[0].evidence_chain == {}
