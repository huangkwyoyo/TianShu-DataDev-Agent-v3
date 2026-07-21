"""RequirementPlanner E2E 测试——使用 FakeLLMAdapter 模拟 LLM 调用。

覆盖路径：
1. 正常流程——FakeLLMAdapter 返回合法 JSON → 正确解析为 RequirementPlannerOutput
2. 无 Adapter——构造时 adapter=None，plan() 返回全空输出
3. LLM 调用失败——FakeLLMAdapter 抛出异常 → plan() 全空输出不阻断
"""

from __future__ import annotations

from tianshu_datadev.developer_spec.models import (
    ColumnDecl,
    DatasetType,
    InputTableDecl,
    ManifestColumn,
    ManifestTable,
    OutputColumnDecl,
    OutputSpecDecl,
    ParsedDeveloperSpec,
    SourceManifest,
)
from tianshu_datadev.llm.adapters.fake_adapter import FakeLLMAdapter
from tianshu_datadev.planning.requirement_planner import RequirementPlanner


class TestRequirementPlanner:
    """RequirementPlanner 核心测试——使用 FakeLLMAdapter 模拟 LLM。"""

    def _make_spec(self) -> ParsedDeveloperSpec:
        """构建最小可用的 aggregate_table Spec。"""
        return ParsedDeveloperSpec(
            spec_id="test_spec_001",
            spec_hash="test_planner_001",
            title="高峰时段出行分析",
            description="按小时和区域统计出行次数，区分高峰/平峰",
            dataset_type=DatasetType.AGGREGATE_TABLE,
            input_tables=[
                InputTableDecl(
                    table_alias="ft",
                    source_table="fact_table",
                    columns=[
                        ColumnDecl(
                            column_name="pickup_at",
                            normalized_name="pickup_at",
                            data_type="timestamp",
                        ),
                        ColumnDecl(
                            column_name="borough",
                            normalized_name="borough",
                            data_type="varchar",
                        ),
                    ],
                ),
            ],
            metrics=[],
            dimensions=[],
            output_spec=OutputSpecDecl(
                columns=[
                    OutputColumnDecl(name="pickup_hour"),
                    OutputColumnDecl(name="borough"),
                    OutputColumnDecl(name="trip_count"),
                    OutputColumnDecl(name="peak_type"),
                ],
                grain=["pickup_hour", "borough"],
            ),
        )

    def _make_manifest(self) -> SourceManifest:
        """构建对应的最小 SourceManifest。"""
        return SourceManifest(
            manifest_id="test_manifest_001",
            spec_hash="manifest_001",
            tables=[
                ManifestTable(
                    table_ref="ft",
                    source_table="fact_table",
                    columns=[
                        ManifestColumn(
                            column_name="pickup_at",
                            normalized_name="pickup_at",
                            data_type="timestamp",
                        ),
                        ManifestColumn(
                            column_name="borough",
                            normalized_name="borough",
                            data_type="varchar",
                        ),
                    ],
                ),
            ],
        )

    def _make_expected_response(self) -> dict:
        """构造 FakeLLMAdapter 的预设响应。"""
        return {
            "dimensions": [
                {
                    "dimension_name": "borough",
                    "column_ref": "borough",
                    "source_table": "ft",
                },
            ],
            "derived_dimensions": [
                {
                    "dimension_name": "pickup_hour",
                    "source_column": "pickup_at",
                    "source_table": "ft",
                    "time_function": "HOUR",
                },
            ],
            "metrics": [
                {
                    "metric_name": "出行次数",
                    "aggregation": "COUNT",
                    "alias": "trip_count",
                },
            ],
            "case_when_rules": [
                {
                    "output_column": "peak_type",
                    "branches": [
                        {
                            "condition": {
                                "node_type": "COMPARE",
                                "left": "pickup_hour",
                                "op": "IN",
                                "right": {
                                    "node_type": "LITERAL",
                                    "value": [7, 8, 9, 10, 17, 18, 19, 20],
                                    "data_type": "number",
                                },
                            },
                            "then_value": "高峰",
                        },
                    ],
                    "else_value": "平峰",
                },
            ],
            "uncertainties": [],
        }

    def test_planner_returns_valid_output_with_fake_adapter(self):
        """FakeLLMAdapter 应返回合法 RequirementPlannerOutput。"""
        fake = FakeLLMAdapter()
        fake.register_default_for_task(
            task="requirement_planner",
            output=self._make_expected_response(),
        )

        planner = RequirementPlanner(adapter=fake)
        spec = self._make_spec()
        manifest = self._make_manifest()
        output = planner.plan(spec, manifest)

        # 验证 dimensions
        assert len(output.dimensions) == 1
        assert output.dimensions[0].dimension_name == "borough"
        assert output.dimensions[0].column_ref == "borough"
        assert output.dimensions[0].source_table == "ft"

        # 验证 derived_dimensions
        assert len(output.derived_dimensions) == 1
        assert output.derived_dimensions[0].dimension_name == "pickup_hour"
        assert output.derived_dimensions[0].source_column == "pickup_at"
        assert output.derived_dimensions[0].time_function == "HOUR"

        # 验证 metrics
        assert len(output.metrics) == 1
        assert output.metrics[0].metric_name == "出行次数"
        assert output.metrics[0].aggregation.value == "COUNT"
        assert output.metrics[0].alias == "trip_count"

        # 验证 case_when_rules
        assert len(output.case_when_rules) == 1
        rule = output.case_when_rules[0]
        assert rule.output_column == "peak_type"
        assert len(rule.branches) == 1
        assert rule.branches[0].then_value == "高峰"
        assert rule.else_value == "平峰"
        # condition 为原始 dict（未校验结构——校验由后续 Validator 负责）
        assert isinstance(rule.branches[0].condition, dict)
        assert rule.branches[0].condition["node_type"] == "COMPARE"

        # 验证 uncertainties
        assert len(output.uncertainties) == 0

    def test_planner_without_adapter_returns_empty_output(self):
        """adapter=None 时 plan() 应返回全空 RequirementPlannerOutput。"""
        planner = RequirementPlanner(adapter=None)
        spec = self._make_spec()
        manifest = self._make_manifest()
        output = planner.plan(spec, manifest)

        assert len(output.dimensions) == 0
        assert len(output.derived_dimensions) == 0
        assert len(output.metrics) == 0
        assert len(output.case_when_rules) == 0
        assert len(output.uncertainties) == 0

    def test_planner_empty_on_adapter_error(self):
        """LLM 调用失败时 plan() 应返回全空输出，不阻断管线。"""
        fake = FakeLLMAdapter()
        # 不注册任何 fixture——FakeLLMAdapter 会抛出 AdapterError

        planner = RequirementPlanner(adapter=fake)
        spec = self._make_spec()
        manifest = self._make_manifest()
        output = planner.plan(spec, manifest)

        assert len(output.dimensions) == 0
        assert len(output.derived_dimensions) == 0
        assert len(output.metrics) == 0
        assert len(output.case_when_rules) == 0
        assert len(output.uncertainties) == 0

    def test_golden_chain_planner_to_builder(self):
        """Golden chain: FakeAdapter→Planner→Validator→Promotion→Builder 全链路。"""
        import uuid

        from tianshu_datadev.developer_spec.models import RequirementProposal
        from tianshu_datadev.planning.proposal_promotion import ProposalPromotion
        from tianshu_datadev.planning.proposal_validator import ProposalValidator
        from tianshu_datadev.planning.sql_build_plan import SqlBuildPlanBuilder

        fake = FakeLLMAdapter()
        fake.register_default_for_task(
            task="requirement_planner",
            output=self._make_expected_response(),
        )
        planner = RequirementPlanner(adapter=fake)
        spec = self._make_spec()
        manifest = self._make_manifest()

        # 1. Planner
        output = planner.plan(spec, manifest)
        assert len(output.derived_dimensions) == 1

        # 2. Proposal
        proposal = RequirementProposal(
            proposal_id=uuid.uuid4().hex[:12],
            spec_hash=spec.spec_hash,
            dimensions=output.dimensions,
            derived_dimensions=output.derived_dimensions,
            metrics=output.metrics,
            case_when_rules=output.case_when_rules,
            uncertainties=output.uncertainties,
            llm_model="fake",
            inference_time_ms=0,
            total_inferred=3,
        )

        # 3. Validator
        validator = ProposalValidator()
        valid, questions = validator.validate(proposal, spec, manifest)
        assert valid, f"Validator 应通过: {questions}"

        # 4. Promotion
        promotion = ProposalPromotion()
        spec = promotion.promote(proposal, spec)
        assert len(spec.derived_dimensions) == 1
        assert len(spec.case_when_rules) == 1

        # 5. Builder——验证全链路到 SqlBuildPlan
        builder = SqlBuildPlanBuilder()
        plan, plan_questions = builder.build(spec)

        # 验证 AggregateStep 含 DerivedGroupKey
        from tianshu_datadev.planning.models import TimeTransformExpr
        from tianshu_datadev.planning.sql_build_plan import AggregateStep, CaseWhenStep

        agg_steps = [s for s in plan.steps if isinstance(s, AggregateStep)]
        assert len(agg_steps) > 0
        agg = agg_steps[0]
        derived_keys = [k for k in agg.group_keys if hasattr(k, "alias") and hasattr(k, "expr")]
        assert len(derived_keys) >= 1
        assert derived_keys[0].alias == "pickup_hour"

        # 验证 CaseWhenStep 的 Predicate.left 为 TimeTransformExpr
        case_when_steps = [s for s in plan.steps if isinstance(s, CaseWhenStep)]
        assert len(case_when_steps) >= 1, "应生成至少一个 CaseWhenStep"
        case_when = case_when_steps[0]
        assert len(case_when.cases) >= 1
        condition_left = case_when.cases[0].condition.left
        assert isinstance(condition_left, TimeTransformExpr), (
            f"Predicate.left 应为 TimeTransformExpr，实际为 {type(condition_left).__name__}"
        )
        assert str(condition_left.source_column) == "pickup_at"
        assert str(condition_left.source_table) == "ft"
        assert condition_left.time_function == "HOUR"
