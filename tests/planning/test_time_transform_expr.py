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
    ColumnDecl,
    DatasetType,
    DerivedDimensionDecl,
    CaseWhenBranch,
    CaseWhenRule,
    InputTableDecl,
    OutputColumnDecl,
    OutputSpecDecl,
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


# ════════════════════════════════════════════
# Task 4: SQL Compiler 测试
# ════════════════════════════════════════════

from tianshu_datadev.planning.sql_build_plan import (
    AggregateStep,
    ScanStep,
    SqlBuildPlan,
)
from tianshu_datadev.planning.models import (
    AggregateSpec,
)
from tianshu_datadev.sql.compiler import DuckDbSqlCompiler


class TestRenderTimeTransform:
    """_render_time_transform 共享渲染器测试。"""

    def test_render_hour_expr(self):
        """渲染 HOUR(ft.pickup_at)。"""
        expr = TimeTransformExpr(
            source_column=SafeIdentifier("pickup_at"),
            source_table=SafeIdentifier("ft"),
            time_function="HOUR",
        )
        result = DuckDbSqlCompiler._render_time_transform(expr)
        assert result == "HOUR(ft.pickup_at)"


class TestRenderAggregateWithDerivedGroupKey:
    """_render_aggregate 处理 DerivedGroupKey。"""

    def test_select_with_derived_group_key(self):
        """SELECT 应含 'HOUR(ft.pickup_at) AS pickup_hour'。"""
        agg = AggregateStep(
            step_id="agg_1",
            group_keys=[
                DerivedGroupKey(
                    alias="pickup_hour",
                    expr=TimeTransformExpr(
                        source_column=SafeIdentifier("pickup_at"),
                        source_table=SafeIdentifier("ft"),
                        time_function="HOUR",
                    ),
                ),
                ColumnRef(
                    table_ref=SafeIdentifier("tz"),
                    column_name=SafeIdentifier("borough"),
                    normalized_name=SafeIdentifier("borough"),
                ),
            ],
            metrics=[
                AggregateSpec(
                    aggregation=AggregationType.COUNT,
                    input_column=None,
                    alias=SafeIdentifier("trip_count"),
                ),
            ],
        )
        compiler = DuckDbSqlCompiler()
        cols = compiler._render_aggregate(agg)
        assert "HOUR(ft.pickup_at) AS pickup_hour" in cols
        assert "tz.borough" in cols


class TestFlatSqlGroupByWithDerivedGroupKey:
    """_render_flat_sql GROUP BY 处理 DerivedGroupKey——集成验证。"""

    def test_group_by_uses_time_transform_without_alias(self):
        """GROUP BY 应使用 HOUR(ft.pickup_at) 不带 AS alias。"""
        plan = SqlBuildPlan(
            plan_id="test_plan",
            spec_hash="test_hash",
            steps=[
                ScanStep(
                    step_id="scan_1",
                    table_ref=SafeIdentifier("ft"),
                    required_columns=[
                        ColumnRef(
                            table_ref=SafeIdentifier("ft"),
                            column_name=SafeIdentifier("pickup_at"),
                            normalized_name=SafeIdentifier("pickup_at"),
                        ),
                    ],
                ),
                AggregateStep(
                    step_id="agg_1",
                    group_keys=[
                        DerivedGroupKey(
                            alias="pickup_hour",
                            expr=TimeTransformExpr(
                                source_column=SafeIdentifier("pickup_at"),
                                source_table=SafeIdentifier("ft"),
                                time_function="HOUR",
                            ),
                        ),
                    ],
                    metrics=[
                        AggregateSpec(
                            aggregation=AggregationType.COUNT,
                            input_column=None,
                            alias=SafeIdentifier("trip_count"),
                        ),
                    ],
                ),
            ],
        )
        compiler = DuckDbSqlCompiler()
        compiled = compiler.compile(plan)
        sql = compiled.sql
        # SELECT 含 AS alias
        assert "HOUR(ft.pickup_at) AS pickup_hour" in sql
        # GROUP BY 不含 AS alias
        assert "GROUP BY" in sql
        assert "HOUR(ft.pickup_at)" in sql


# ════════════════════════════════════════════
# Task 5: SqlBuildPlanBuilder 测试
# ════════════════════════════════════════════

from tianshu_datadev.planning.sql_build_plan import (
    CaseWhenStep,
    DerivedGroupKey,
    ScanStep,
    SqlBuildPlanBuilder,
)


class TestBuildAggregateStepWithDerivedDimensions:
    """_build_aggregate_step 生成 DerivedGroupKey。"""

    def test_derived_dimension_becomes_derived_group_key(self):
        """spec.derived_dimensions → DerivedGroupKey 在 group_keys 中。"""
        spec = ParsedDeveloperSpec(
            spec_id="test", spec_hash="test",
            title="测试",
            description="测试派生维度",
            dataset_type=DatasetType.AGGREGATE_TABLE,
            input_tables=[
                InputTableDecl(
                    table_alias="ft",
                    source_table="fact_table",
                    columns=[ColumnDecl(
                        column_name="pickup_at", data_type="timestamp",
                        normalized_name="pickup_at",
                    )],
                    key_columns=[],
                    business_columns=[],
                ),
            ],
            dimensions=[
                DimensionDecl(
                    dimension_name="borough",
                    column_ref="borough",
                    source_table="tz",
                ),
            ],
            derived_dimensions=[
                DerivedDimensionDecl(
                    dimension_name="pickup_hour",
                    source_column="pickup_at",
                    source_table="ft",
                    time_function="HOUR",
                ),
            ],
            metrics=[
                MetricDecl(
                    metric_name="trip_count",
                    aggregation=AggregationType.COUNT,
                    alias="trip_count",
                ),
            ],
            output_spec=OutputSpecDecl(
                columns=[
                    OutputColumnDecl(name="pickup_hour"),
                    OutputColumnDecl(name="borough"),
                    OutputColumnDecl(name="trip_count"),
                ],
                grain=[],
            ),
            time_range=None,
        )
        builder = SqlBuildPlanBuilder()
        agg = builder._build_aggregate_step(spec, primary_table="ft")
        derived_keys = [
            gk for gk in agg.group_keys if isinstance(gk, DerivedGroupKey)
        ]
        assert len(derived_keys) == 1
        assert derived_keys[0].alias == "pickup_hour"
        assert derived_keys[0].expr.time_function == "HOUR"

    def test_grain_dedup_with_derived_and_column_ref(self):
        """grain 去重兼容 ColumnRef 和 DerivedGroupKey。"""
        spec = ParsedDeveloperSpec(
            spec_id="test2", spec_hash="test2",
            title="测试",
            description="测试 grain 去重",
            dataset_type=DatasetType.AGGREGATE_TABLE,
            input_tables=[
                InputTableDecl(
                    table_alias="ft",
                    source_table="fact_table",
                    columns=[ColumnDecl(
                        column_name="pickup_at", data_type="timestamp",
                        normalized_name="pickup_at",
                    )],
                    key_columns=[],
                    business_columns=[],
                ),
            ],
            dimensions=[],
            derived_dimensions=[
                DerivedDimensionDecl(
                    dimension_name="pickup_hour",
                    source_column="pickup_at",
                    source_table="ft",
                    time_function="HOUR",
                ),
            ],
            metrics=[
                MetricDecl(
                    metric_name="trip_count",
                    aggregation=AggregationType.COUNT,
                    alias="trip_count",
                ),
            ],
            output_spec=OutputSpecDecl(
                columns=[OutputColumnDecl(name="pickup_hour")],
                grain=["pickup_hour"],  # grain 与派生维度同名——应去重
            ),
            time_range=None,
        )
        builder = SqlBuildPlanBuilder()
        agg = builder._build_aggregate_step(spec, primary_table="ft")
        derived_keys = [
            gk for gk in agg.group_keys if isinstance(gk, DerivedGroupKey)
        ]
        assert len(derived_keys) == 1, (
            f"grain 不应重复添加派生维度——实际 {len(derived_keys)} 个"
        )
        # 验证总 group_keys 数量——1 个 DerivedGroupKey
        assert len(agg.group_keys) == 1


class TestBuildCaseWhenStepsWithCaseWhenRules:
    """_build_case_when_steps 处理 case_when_rules。"""

    def test_case_when_rule_generates_step_with_dict_condition(self):
        """case_when_rules 中 dict 条件→Predicate。"""
        spec = ParsedDeveloperSpec(
            spec_id="test_cw", spec_hash="test_cw",
            title="测试",
            description="测试 case_when_rules",
            dataset_type=DatasetType.AGGREGATE_TABLE,
            input_tables=[
                InputTableDecl(
                    table_alias="ft",
                    source_table="fact_table",
                    columns=[ColumnDecl(
                        column_name="pickup_at", data_type="timestamp",
                        normalized_name="pickup_at",
                    )],
                    key_columns=[],
                    business_columns=[],
                ),
            ],
            dimensions=[],
            derived_dimensions=[
                DerivedDimensionDecl(
                    dimension_name="pickup_hour",
                    source_column="pickup_at",
                    source_table="ft",
                    time_function="HOUR",
                ),
            ],
            metrics=[],
            output_spec=OutputSpecDecl(
                columns=[OutputColumnDecl(name="peak_type")],
                grain=[],
            ),
            case_when_rules=[
                CaseWhenRule(
                    output_column="peak_type",
                    branches=[
                        CaseWhenBranch(
                            condition={
                                "node_type": "COMPARE",
                                "left": "pickup_hour",
                                "op": "IN",
                                "right": {
                                    "node_type": "LITERAL",
                                    "value": [7, 8, 9],
                                    "data_type": "number",
                                },
                            },
                            then_value="高峰",
                        ),
                    ],
                    else_value="平峰",
                ),
            ],
            time_range=None,
        )
        builder = SqlBuildPlanBuilder()
        steps = builder._build_case_when_steps(spec)
        assert len(steps) == 1
        step = steps[0]
        assert step.step_type == "case_when"
        assert step.alias == "peak_type"
        assert len(step.cases) == 1
        # 验证 Predicate.left 被解析为 TimeTransformExpr（非 ColumnRef）
        from tianshu_datadev.planning.models import TimeTransformExpr
        assert isinstance(step.cases[0].condition.left, TimeTransformExpr), (
            "派生维度别名应解析为 TimeTransformExpr"
        )
        assert step.else_value.value == "平峰"

    def test_case_when_rules_empty_returns_empty(self):
        """无 case_when_rules 时返回空列表——不影响现有 label_rules 行为。"""
        spec = ParsedDeveloperSpec(
            spec_id="test_empty", spec_hash="test_empty",
            title="测试",
            description="空 case_when_rules",
            dataset_type=DatasetType.AGGREGATE_TABLE,
            input_tables=[
                InputTableDecl(
                    table_alias="ft",
                    source_table="fact_table",
                    columns=[],
                    key_columns=[],
                    business_columns=[],
                ),
            ],
            dimensions=[],
            derived_dimensions=[],
            metrics=[],
            output_spec=OutputSpecDecl(
                columns=[OutputColumnDecl(name="col")],
                grain=[],
            ),
            case_when_rules=[],
            time_range=None,
        )
        builder = SqlBuildPlanBuilder()
        steps = builder._build_case_when_steps(spec)
        assert steps == []


class TestBuildSingleTableWithDerivedDimensions:
    """_build_single_table 整合派生维度和 case_when_rules。"""

    def test_scan_includes_derived_source_column(self):
        """派生维度的 source_column 应在 Scan 中。"""
        spec = ParsedDeveloperSpec(
            spec_id="test_scan", spec_hash="test_scan",
            title="测试",
            description="测试派生维度源列",
            dataset_type=DatasetType.AGGREGATE_TABLE,
            input_tables=[
                InputTableDecl(
                    table_alias="ft",
                    source_table="fact_table",
                    columns=[ColumnDecl(
                        column_name="pickup_at", data_type="timestamp",
                        normalized_name="pickup_at",
                    )],
                    key_columns=[],
                    business_columns=[],
                ),
            ],
            dimensions=[],
            derived_dimensions=[
                DerivedDimensionDecl(
                    dimension_name="pickup_hour",
                    source_column="pickup_at",
                    source_table="ft",
                    time_function="HOUR",
                ),
            ],
            metrics=[
                MetricDecl(
                    metric_name="trip_count",
                    aggregation=AggregationType.COUNT,
                    alias="trip_count",
                ),
            ],
            output_spec=OutputSpecDecl(
                columns=[
                    OutputColumnDecl(name="pickup_hour"),
                    OutputColumnDecl(name="trip_count"),
                ],
                grain=[],
            ),
            time_range=None,
        )
        builder = SqlBuildPlanBuilder()
        steps = builder._build_single_table(spec)
        scan = next(s for s in steps if isinstance(s, ScanStep))
        scan_col_names = [str(c.column_name) for c in scan.required_columns]
        assert "pickup_at" in scan_col_names, (
            f"派生维度源列 pickup_at 应在 Scan 中: {scan_col_names}"
        )
        # 验证 AggregateStep 含 DerivedGroupKey
        agg = next(s for s in steps if s.step_type == "aggregate")
        derived_keys = [
            gk for gk in agg.group_keys if isinstance(gk, DerivedGroupKey)
        ]
        assert len(derived_keys) == 1
        assert derived_keys[0].alias == "pickup_hour"
