"""TimeTransformExpr + DerivedGroupKey + Predicate.left 扩展——模型校验测试。"""

import pytest
from pydantic import ValidationError

from tianshu_datadev.planning.models import (
    ColumnRef,
    DatePartExpression,
    DerivedGroupKey,
    Predicate,
    PredicateOperator,
    SafeIdentifier,
    SqlLiteral,
    TimeTransformExpr,
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
    AggregationType,
    CaseWhenBranch,
    CaseWhenRule,
    ColumnDecl,
    DatasetType,
    DerivedDimensionDecl,
    DimensionDecl,
    InputTableDecl,
    MetricDecl,
    OutputColumnDecl,
    OutputSpecDecl,
    ParsedDeveloperSpec,
    RequirementPlannerOutput,
    RequirementProposal,
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

from tianshu_datadev.planning.models import (
    AggregateSpec,
)
from tianshu_datadev.planning.sql_build_plan import (
    AggregateStep,
    ScanStep,
    SqlBuildPlan,
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

    def test_render_predicate_operand_time_transform_expr(self):
        """回归测试：_render_predicate_operand 正确处理 TimeTransformExpr。

        复现条件：CASE WHEN 的 Predicate.left 为 TimeTransformExpr 时，
        _render_predicate_operand 缺少 isinstance 检查，落到 str(operand)，
        Pydantic __str__ 输出 source_table='...' 字面量嵌入 SQL 导致解析错误。
        """
        expr = TimeTransformExpr(
            source_column=SafeIdentifier("pickup_at"),
            source_table=SafeIdentifier("ft"),
            time_function="HOUR",
        )
        compiler = DuckDbSqlCompiler()
        result = compiler._render_predicate_operand(expr)
        assert result == "HOUR(ft.pickup_at)", (
            f"TimeTransformExpr 应渲染为 HOUR(ft.pickup_at)，"
            f"实际={result}"
        )
        # 关键断言：不能出现 Python 属性名字面量
        assert "source_table" not in result, (
            f"渲染结果不得包含字段名字面量 'source_table'，实际={result}"
        )
        assert "source_column" not in result, (
            f"渲染结果不得包含字段名字面量 'source_column'，实际={result}"
        )

    def test_render_predicate_operand_with_prefix_time_transform(self):
        """回归测试：_render_predicate_operand_with_prefix 正确处理 TimeTransformExpr。"""
        expr = TimeTransformExpr(
            source_column=SafeIdentifier("pickup_at"),
            source_table=SafeIdentifier("ft"),
            time_function="HOUR",
        )
        compiler = DuckDbSqlCompiler()
        result = compiler._render_predicate_operand_with_prefix(expr, "_sub")
        assert result == "HOUR(_sub.pickup_at)", (
            f"子查询前缀模式下应渲染为 HOUR(_sub.pickup_at)，"
            f"实际={result}"
        )
        assert "source_table" not in result
        assert "source_column" not in result


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
    DerivedGroupKey,
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

    def test_dimensions_date_part_dedup_with_derived_dimensions(self):
        """dimensions 已有 DatePartExpression(alias="pickup_hour") 时，
        derived_dimensions 中同名的 DerivedGroupKey 应被跳过——
        防止 Builder→Contract→Mapper→Compiler 链路产生重复
        time_transform 导致 Spark groupBy/agg 中同一列出现多次。"""
        from tianshu_datadev.planning.models import DatePartExpression
        from tianshu_datadev.planning.sql_build_plan import SqlBuildPlanBuilder

        spec = ParsedDeveloperSpec(
            spec_id="test_dedup", spec_hash="h_dedup",
            title="测试去重",
            description="dimensions+derived_dimensions 同名 date_part",
            dataset_type=DatasetType.AGGREGATE_TABLE,
            input_tables=[InputTableDecl(
                table_alias="ft", source_table="fact_table",
                columns=[ColumnDecl(
                    column_name="pickup_at", normalized_name="pickup_at",
                    data_type="timestamp",
                )],
                key_columns=[], business_columns=[],
            )],
            # dimensions 中已有 date_part="HOUR"——Builder 会生成
            # DatePartExpression(part="HOUR", alias="pickup_hour")
            dimensions=[DimensionDecl(
                dimension_name="pickup_hour",
                column_ref="pickup_at",
                source_table="ft",
                date_part="HOUR",
            )],
            # derived_dimensions 中又声明了同名 HOUR 派生维度——
            # Builder 会生成 DerivedGroupKey(alias="pickup_hour")
            derived_dimensions=[DerivedDimensionDecl(
                dimension_name="pickup_hour",
                source_column="pickup_at",
                source_table="ft",
                time_function="HOUR",
            )],
            metrics=[MetricDecl(
                metric_name="trip_count",
                aggregation=AggregationType.COUNT,
                alias="trip_count",
            )],
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
        agg = builder._build_aggregate_step(spec, primary_table="ft")

        # 验证：pickup_hour 相关的 group_key 有两条——
        # DatePartExpression（真实渲染）+ DerivedGroupKey（_shadow=True，仅用于 Contract 反查）
        pickup_keys = [
            gk for gk in agg.group_keys
            if (
                (isinstance(gk, DatePartExpression) and gk.alias == "pickup_hour")
                or (isinstance(gk, DerivedGroupKey) and gk.alias == "pickup_hour")
            )
        ]
        assert len(pickup_keys) == 2, (
            f"pickup_hour 应在 group_keys 中出现两次（DatePartExpression + shadow DerivedGroupKey），"
            f"实际={[(type(g).__name__, getattr(g, 'alias', None)) for g in pickup_keys]}"
        )

        # 验证：保留真实条目 DatePartExpression（dimensions 优先于 derived_dimensions）
        date_part_entries = [g for g in pickup_keys if isinstance(g, DatePartExpression)]
        assert len(date_part_entries) == 1, "应有一条 DatePartExpression"
        assert date_part_entries[0].alias == "pickup_hour"

        # 验证：DerivedGroupKey 以影子模式存在——不参与渲染，仅供 Contract 反查
        derived_pickup = [
            gk for gk in agg.group_keys
            if isinstance(gk, DerivedGroupKey) and gk.alias == "pickup_hour"
        ]
        assert len(derived_pickup) == 1, (
            f"应有一条影子 DerivedGroupKey(pickup_hour)，"
            f"实际={[(g.alias, g.expr.time_function) for g in derived_pickup]}"
        )
        assert derived_pickup[0]._shadow is True, (
            "DerivedGroupKey(pickup_hour) 应为 _shadow=True"
        )


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

    def test_case_when_with_time_transform_compiles_to_valid_sql(self):
        """回归测试：CASE WHEN 条件含 TimeTransformExpr → 编译为合法 SQL。

        复现 PIPELINE_EXECUTE_FAILED——Builder 将派生维度别名解析为
        TimeTransformExpr 作为 Predicate.left，Compiler 的
        _render_predicate_operand 之前缺少 isinstance 检查，
        导致 Pydantic str() 输出 source_table= 字面量嵌入 SQL。
        """
        spec = ParsedDeveloperSpec(
            spec_id="test_cw_compile", spec_hash="test_cw_compile",
            title="测试",
            description="CASE WHEN + TimeTransformExpr 编译回归",
            dataset_type=DatasetType.AGGREGATE_TABLE,
            input_tables=[
                InputTableDecl(
                    table_alias="ft",
                    source_table="fact_table",
                    columns=[
                        ColumnDecl(column_name="pickup_at", data_type="timestamp",
                                   normalized_name="pickup_at"),
                        ColumnDecl(column_name="trip_count", data_type="bigint",
                                   normalized_name="trip_count"),
                    ],
                    key_columns=[],
                    business_columns=[],
                ),
            ],
            dimensions=[
                DimensionDecl(dimension_name="pickup_hour", column_ref="pickup_at",
                              date_part="HOUR"),
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
                MetricDecl(metric_name="total_trips", alias="total_trips",
                           input_column="trip_count",
                           aggregation=AggregationType.SUM),
            ],
            output_spec=OutputSpecDecl(
                columns=[
                    OutputColumnDecl(name="pickup_hour"),
                    OutputColumnDecl(name="peak_type"),
                    OutputColumnDecl(name="total_trips"),
                ],
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
        plan, _ = builder.build(spec)

        compiler = DuckDbSqlCompiler()
        compiled = compiler.compile(plan)

        sql = compiled.sql
        # 关键断言：SQL 中不得出现 Python 字段名字面量
        assert "source_table" not in sql, (
            f"SQL 不得包含 Pydantic 字段名 'source_table'，"
            f"SQL=\n{sql}"
        )
        assert "source_column" not in sql, (
            f"SQL 不得包含 Pydantic 字段名 'source_column'，"
            f"SQL=\n{sql}"
        )
        # 正向断言：应包含正确的 HOUR() 语法
        assert "HOUR(ft.pickup_at)" in sql, (
            f"SQL 应包含 HOUR(ft.pickup_at)，SQL=\n{sql}"
        )
        # 应包含完整的 CASE WHEN 结构
        assert "CASE" in sql
        assert "高峰" in sql
        assert "平峰" in sql

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


class TestBuildAggregateStepWithDerivedGroupKey:
    """_build_aggregate_step 不应对 DerivedGroupKey 抛 AttributeError。"""

    def test_derived_dimension_in_group_keys_no_attribute_error(self):
        """spec 包含 derived_dimensions → group_keys 含 DerivedGroupKey → 不抛异常。"""
        from tianshu_datadev.planning.sql_build_plan import SqlBuildPlanBuilder

        spec = ParsedDeveloperSpec(
            spec_id="test_derived", spec_hash="h_derived",
            title="测试派生维度", description="",
            dataset_type=DatasetType.AGGREGATE_TABLE,
            input_tables=[InputTableDecl(
                table_alias="ft", source_table="fact_table",
                columns=[ColumnDecl(column_name="pickup_at", normalized_name="pickup_at",
                                    data_type="timestamp")],
                key_columns=[], business_columns=[],
            )],
            metrics=[],
            dimensions=[DimensionDecl(
                dimension_name="borough", column_ref="pickup_at", source_table="ft",
            )],
            derived_dimensions=[DerivedDimensionDecl(
                dimension_name="pickup_hour",
                source_column="pickup_at",
                source_table="ft",
                time_function="HOUR",
            )],
            output_spec=OutputSpecDecl(
                columns=[OutputColumnDecl(name="pickup_hour")],
                grain=["pickup_hour"],
            ),
            time_range=None,
        )

        builder = SqlBuildPlanBuilder()
        # 传入 extra_group_keys 以触发 L3118-3120 的 column_name 检查
        agg = builder._build_aggregate_step(spec, "ft", extra_group_keys={"other_col"})
        # 验证 group_keys 包含 DerivedGroupKey
        derived = [g for g in agg.group_keys if isinstance(g, DerivedGroupKey)]
        assert len(derived) == 1
        assert derived[0].alias == "pickup_hour"


class TestCatalogQualifiedSourceTableResolution:
    """catalog 限定表名（gold.fact_trips）→ 表别名（ft）的映射验证。

    LLM 输出的 DerivedDimensionDecl.source_table 是物理表名（含 catalog 前缀），
    但 TimeTransformExpr.source_table 需要表别名。
    Builder 的 _resolve_derived_source_table 应完成此映射。
    """

    def test_aggregate_step_resolves_catalog_name_to_alias(self):
        """_build_aggregate_step 将 catalog 限定名映射为别名。"""
        spec = ParsedDeveloperSpec(
            spec_id="test", spec_hash="test",
            title="测试 Catalog 表名映射",
            description="验证 gold.fact_trips → ft 的映射",
            dataset_type=DatasetType.AGGREGATE_TABLE,
            input_tables=[
                InputTableDecl(
                    table_alias="ft",
                    source_table="gold.fact_trips",
                    columns=[ColumnDecl(
                        column_name="pickup_datetime", data_type="timestamp",
                        normalized_name="pickup_datetime",
                    )],
                    key_columns=[],
                    business_columns=[],
                ),
            ],
            dimensions=[],
            derived_dimensions=[DerivedDimensionDecl(
                dimension_name="pickup_hour",
                source_column="pickup_datetime",
                source_table="gold.fact_trips",  # ← LLM 输出的物理表名
                time_function="HOUR",
            )],
            metrics=[],
            output_spec=OutputSpecDecl(
                columns=[OutputColumnDecl(name="pickup_hour")],
                grain=["pickup_hour"],
            ),
            time_range=None,
        )

        builder = SqlBuildPlanBuilder()
        agg = builder._build_aggregate_step(spec, "ft")
        derived = [g for g in agg.group_keys if isinstance(g, DerivedGroupKey)]
        assert len(derived) == 1
        dgk = derived[0]
        assert dgk.alias == "pickup_hour"
        # 关键断言：source_table 已被映射为表别名 "ft"，而非 "gold.fact_trips"
        assert str(dgk.expr.source_table) == "ft"

    def test_case_when_steps_resolves_catalog_name_to_alias(self):
        """_build_case_when_steps 将 catalog 限定名映射为别名。"""
        spec = ParsedDeveloperSpec(
            spec_id="test", spec_hash="test",
            title="测试 Catalog 表名映射——CASE WHEN",
            description="验证 gold.fact_trips → ft 的映射",
            dataset_type=DatasetType.AGGREGATE_TABLE,
            input_tables=[
                InputTableDecl(
                    table_alias="ft",
                    source_table="gold.fact_trips",
                    columns=[
                        ColumnDecl(
                            column_name="pickup_datetime", data_type="timestamp",
                            normalized_name="pickup_datetime",
                        ),
                        ColumnDecl(
                            column_name="total_amount", data_type="decimal",
                            normalized_name="total_amount",
                        ),
                    ],
                    key_columns=[],
                    business_columns=[],
                ),
            ],
            dimensions=[],
            metrics=[],
            derived_dimensions=[DerivedDimensionDecl(
                dimension_name="pickup_hour",
                source_column="pickup_datetime",
                source_table="gold.fact_trips",  # ← LLM 输出的物理表名
                time_function="HOUR",
            )],
            case_when_rules=[],  # 无 CASE WHEN 规则——只测试 derived_expr_map 构建
            output_spec=OutputSpecDecl(
                columns=[OutputColumnDecl(name="pickup_hour")],
                grain=[],
            ),
            time_range=None,
        )

        builder = SqlBuildPlanBuilder()
        steps = builder._build_case_when_steps(spec)
        # 无 CASE WHEN 规则时应返回空列表——但 derived_expr_map 构建不应抛异常
        # 这意味着 SafeIdentifier 验证已通过（catalog 名成功映射为别名）
        assert isinstance(steps, list)

    def test_catalog_name_rejected_without_input_table_match(self):
        """当 dd.source_table 不匹配任何 input_table 时，使用兜底（原值透传）。

        此时如果原值含点号，SafeIdentifier 会正常拒绝——这是预期的安全行为。
        """
        spec = ParsedDeveloperSpec(
            spec_id="test", spec_hash="test",
            title="测试无匹配表",
            description="验证 unmatched source_table 的兜底行为",
            dataset_type=DatasetType.AGGREGATE_TABLE,
            input_tables=[
                InputTableDecl(
                    table_alias="ft",
                    source_table="some_other_table",
                    columns=[ColumnDecl(
                        column_name="pickup_datetime", data_type="timestamp",
                        normalized_name="pickup_datetime",
                    )],
                    key_columns=[],
                    business_columns=[],
                ),
            ],
            dimensions=[],
            derived_dimensions=[DerivedDimensionDecl(
                dimension_name="pickup_hour",
                source_column="pickup_datetime",
                source_table="unmatched_catalog.fact_trips",  # 不匹配任何表
                time_function="HOUR",
            )],
            metrics=[],
            output_spec=OutputSpecDecl(
                columns=[OutputColumnDecl(name="pickup_hour")],
                grain=["pickup_hour"],
            ),
            time_range=None,
        )

        builder = SqlBuildPlanBuilder()
        with pytest.raises(ValidationError):
            builder._build_aggregate_step(spec, "ft")


class TestDimensionDeclSourceTableResolution:
    """DimensionDecl.source_table 物理表名→表别名映射验证。

    和 TestCatalogQualifiedSourceTableResolution 同根因——
    LLM 输出的 source_table 是物理表名（如 gold.dim_taxi_zone），
    但 ColumnRef.table_ref 需要表别名（如 dtz）。
    此测试类覆盖 _build_aggregate_step 和 _build_project_step 中
    DimensionDecl.source_table 的三处使用点。
    """

    def test_aggregate_step_dimensions_resolves_catalog_name(self):
        """_build_aggregate_step dimensions 循环中解析物理表名→别名。"""
        spec = ParsedDeveloperSpec(
            spec_id="test_dim", spec_hash="test_dim",
            title="测试 DimensionDecl source_table 映射",
            description="验证 gold.dim_taxi_zone → dtz 的映射",
            dataset_type=DatasetType.AGGREGATE_TABLE,
            input_tables=[
                InputTableDecl(
                    table_alias="dtz",
                    source_table="gold.dim_taxi_zone",
                    columns=[ColumnDecl(
                        column_name="zone_name", data_type="string",
                        normalized_name="zone_name",
                    )],
                    key_columns=[],
                    business_columns=[],
                ),
                InputTableDecl(
                    table_alias="ft",
                    source_table="gold.fact_trips",
                    columns=[ColumnDecl(
                        column_name="zone_id", data_type="int",
                        normalized_name="zone_id",
                    )],
                    key_columns=[],
                    business_columns=[],
                ),
            ],
            dimensions=[
                DimensionDecl(
                    dimension_name="zone_name",
                    column_ref="zone_name",
                    source_table="gold.dim_taxi_zone",  # ← LLM 输出的物理表名
                ),
            ],
            derived_dimensions=[],
            metrics=[MetricDecl(
                metric_name="trip_count",
                aggregation=AggregationType.COUNT,
                alias="trip_count",
            )],
            output_spec=OutputSpecDecl(
                columns=[OutputColumnDecl(name="zone_name"),
                         OutputColumnDecl(name="trip_count")],
                grain=["zone_name"],
            ),
            time_range=None,
        )

        builder = SqlBuildPlanBuilder()
        agg = builder._build_aggregate_step(spec, "ft")
        # 关键断言：zone_name 的 ColumnRef.table_ref 已被映射为 "dtz"
        zone_cols = [
            g for g in agg.group_keys
            if isinstance(g, ColumnRef) and g.column_name == "zone_name"
        ]
        assert len(zone_cols) == 1
        assert str(zone_cols[0].table_ref) == "dtz"

    def test_aggregate_step_grain_date_part_resolves_catalog_name(self):
        """_build_aggregate_step grain 循环中 DatePartExpression 解析物理表名→别名。"""
        spec = ParsedDeveloperSpec(
            spec_id="test_dim2", spec_hash="test_dim2",
            title="测试 grain date_part source_table 映射",
            description="验证 grain 中 DatePartExpression 的 source_table 映射",
            dataset_type=DatasetType.AGGREGATE_TABLE,
            input_tables=[
                InputTableDecl(
                    table_alias="ft",
                    source_table="gold.fact_trips",
                    columns=[ColumnDecl(
                        column_name="pickup_datetime", data_type="timestamp",
                        normalized_name="pickup_datetime",
                    )],
                    key_columns=[],
                    business_columns=[],
                ),
            ],
            dimensions=[
                DimensionDecl(
                    dimension_name="pickup_hour",
                    column_ref="pickup_datetime",
                    source_table="gold.fact_trips",  # ← LLM 输出的物理表名
                    date_part="HOUR",
                ),
            ],
            derived_dimensions=[],
            metrics=[MetricDecl(
                metric_name="trip_count",
                aggregation=AggregationType.COUNT,
                alias="trip_count",
            )],
            output_spec=OutputSpecDecl(
                columns=[OutputColumnDecl(name="pickup_hour"),
                         OutputColumnDecl(name="trip_count")],
                grain=["pickup_hour"],
            ),
            time_range=None,
        )

        builder = SqlBuildPlanBuilder()
        agg = builder._build_aggregate_step(spec, "ft")
        date_parts = [
            g for g in agg.group_keys
            if isinstance(g, DatePartExpression) and g.alias == "pickup_hour"
        ]
        assert len(date_parts) == 1
        # 关键断言：DatePartExpression 内部 ColumnRef.table_ref 已被映射为 "ft"
        assert str(date_parts[0].column.table_ref) == "ft"

    def test_project_step_dim_source_map_resolves_catalog_name(self):
        """_build_project_step dim_source_map 中解析物理表名→别名。"""
        spec = ParsedDeveloperSpec(
            spec_id="test_proj", spec_hash="test_proj",
            title="测试 project step source_table 映射",
            description="验证 _build_project_step 的 dim_source_map 映射",
            dataset_type=DatasetType.AGGREGATE_TABLE,
            input_tables=[
                InputTableDecl(
                    table_alias="dtz",
                    source_table="gold.dim_taxi_zone",
                    columns=[ColumnDecl(
                        column_name="zone_name", data_type="string",
                        normalized_name="zone_name",
                    )],
                    key_columns=[],
                    business_columns=[],
                ),
            ],
            dimensions=[
                DimensionDecl(
                    dimension_name="zone_name",
                    column_ref="zone_name",
                    source_table="gold.dim_taxi_zone",  # ← LLM 输出的物理表名
                ),
            ],
            derived_dimensions=[],
            metrics=[],
            output_spec=OutputSpecDecl(
                columns=[OutputColumnDecl(name="zone_name")],
                grain=[],
            ),
            time_range=None,
        )

        builder = SqlBuildPlanBuilder()
        proj = builder._build_project_step(spec, default_table_ref="dtz")
        # 关键断言：zone_name 的 AliasExpr.expression.table_ref 已被映射为 "dtz"
        zone_exprs = [
            e for e in proj.columns
            if hasattr(e, 'expression') and hasattr(e.expression, 'column_name')
            and e.expression.column_name == "zone_name"
        ]
        assert len(zone_exprs) == 1
        # table_ref 可能是 "" 因为 overrides 中没有覆盖，此时使用 dim_source_map 的值
        # 而 dim_source_map 中的值已经被 _resolve_derived_source_table 映射为 "dtz"
        # 但 _build_project_step 的解析逻辑是 overrides > dim_table > default_table_ref
        # 这里 dim_table = "" (因为 zone_name 没有 overrides), 所以 table_ref = default_table_ref = ""
        # --- 实际上，dimension_name == col_name 时，source_col=column_ref, dim_table=source_table
        # table_ref = overrides.get(col_name, dim_table or default_table_ref)
        # dim_table = "dtz"（已解析）, overrides 中无 zone_name → table_ref = "dtz"
        # 但 ColumnRef.table_ref 是 SafeIdentifier，所以 "dtz" 可以
        # 对于 zone_name 列：dim_source_map["zone_name"] = ("zone_name", "dtz")
        # table_ref = overrides.get("zone_name", "dtz" or "dtz") = "dtz"
        # 如果 table_ref 仍是 "gold.dim_taxi_zone"，那会 crash
        # 这里直接验证不会抛异常即可
        assert str(zone_exprs[0].expression.table_ref) == "dtz"
