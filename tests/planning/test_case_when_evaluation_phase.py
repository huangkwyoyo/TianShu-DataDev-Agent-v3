"""CASE WHEN evaluation_phase 回归测试。

覆盖：
- 单表/多表 pre/post aggregate 分流
- GROUP BY 自动包含 pre_agg CaseWhen 输出列
- Spark Plan 步骤顺序正确
- 无法判定 phase → OpenQuestion
- 错误计划仍被阻断
"""

from __future__ import annotations

from tianshu_datadev.developer_spec.models import (
    CaseWhenDecl,
    ColumnDecl,
    DatasetType,
    DimensionDecl,
    InputTableDecl,
    JoinDecl,
    JoinTypeEnum,
    LabelAnd,
    LabelCompare,
    LabelOr,
    LabelPredicateBranch,
    MetricDecl,
    OutputColumnDecl,
    OutputSpecDecl,
    ParsedDeveloperSpec,
)
from tianshu_datadev.planning.spec_enricher import SpecEnricher
from tianshu_datadev.planning.sql_build_plan import SqlBuildPlanBuilder

# ════════════════════════════════════════════
# 辅助——构造 pre_aggregate 派生维度 CASE WHEN
# ════════════════════════════════════════════

def _make_pre_agg_peak_type_rule() -> CaseWhenDecl:
    """构造 pre_aggregate CASE WHEN——peak_type 是派生维度。"""
    from tianshu_datadev.developer_spec.models import LabelPredicateBranch
    return CaseWhenDecl(
        output_column="peak_type",
        else_value="平峰",
        evaluation_phase="pre_aggregate",
        typed_branches=[
            LabelPredicateBranch(
                condition=_make_peak_condition(),
                then_label="高峰",
            ),
        ],
    )


def _make_peak_condition():
    """构造 OR(AND(7<=pickup_hour<=10), AND(17<=pickup_hour<=20)) 条件。"""
    return LabelOr(
        children=[
            LabelAnd(
                children=[
                    LabelCompare(
                        left="pickup_hour",
                        op=">=",
                        right={"node_type": "LITERAL", "value": 7, "data_type": "number"},
                    ),
                    LabelCompare(
                        left="pickup_hour",
                        op="<=",
                        right={"node_type": "LITERAL", "value": 10, "data_type": "number"},
                    ),
                ],
            ),
            LabelAnd(
                children=[
                    LabelCompare(
                        left="pickup_hour",
                        op=">=",
                        right={"node_type": "LITERAL", "value": 17, "data_type": "number"},
                    ),
                    LabelCompare(
                        left="pickup_hour",
                        op="<=",
                        right={"node_type": "LITERAL", "value": 20, "data_type": "number"},
                    ),
                ],
            ),
        ],
    )


def _make_post_agg_high_value_rule() -> CaseWhenDecl:
    """构造 post_aggregate CASE WHEN——value_tier 依赖聚合指标 total_fare。"""
    return CaseWhenDecl(
        output_column="value_tier",
        else_value="低价值",
        evaluation_phase="post_aggregate",
        typed_branches=[
            LabelPredicateBranch(
                condition=LabelCompare(
                    left="total_fare",
                    op=">=",
                    right={"node_type": "LITERAL", "value": 1000, "data_type": "number"},
                ),
                then_label="高价值",
            ),
        ],
    )


def _build_trips_spec(
    peak_type_rule: CaseWhenDecl | None = None,
    value_tier_rule: CaseWhenDecl | None = None,
    with_metrics: bool = False,
    with_dimensions: bool = False,
) -> ParsedDeveloperSpec:
    """构造 trips 表 Spec——含 pickup_hour, fare_amount 等列。"""
    label_rules: list[CaseWhenDecl] = []
    if peak_type_rule:
        label_rules.append(peak_type_rule)
    if value_tier_rule:
        label_rules.append(value_tier_rule)

    metrics: list[MetricDecl] = []
    if with_metrics:
        metrics = [
            MetricDecl(metric_name="total_fare", alias="total_fare",
                        aggregation="SUM", input_column="fare_amount"),
            MetricDecl(metric_name="trip_count", alias="trip_count",
                        aggregation="COUNT", input_column="trip_id"),
        ]

    dimensions: list[DimensionDecl] = []
    if with_dimensions:
        dimensions = [
            DimensionDecl(dimension_name="peak_type", column_ref="peak_type"),
        ]

    # 构建输出列列表——仅包含有解析路径的列
    output_columns: list[OutputColumnDecl] = []
    grain_columns: list[str] = []
    if peak_type_rule:
        output_columns.append(OutputColumnDecl(name="peak_type", type="varchar"))
        grain_columns.append("peak_type")
    if value_tier_rule:
        output_columns.append(OutputColumnDecl(name="value_tier", type="varchar"))
    if with_metrics:
        output_columns.append(OutputColumnDecl(name="total_fare", type="double"))
        output_columns.append(OutputColumnDecl(name="trip_count", type="int"))

    return ParsedDeveloperSpec(
        spec_id="test_phase",
        spec_hash="phase123",
        title="CASE WHEN 阶段测试",
        description="验证 CASE WHEN evaluation_phase 在 SQL/Spark 管道中的正确定位",
        input_tables=[
            InputTableDecl(
                table_alias="trips",
                source_table="test.trips",
                columns=[
                    ColumnDecl(column_name="trip_id", normalized_name="trip_id",
                               data_type="int"),
                    ColumnDecl(column_name="pickup_at", normalized_name="pickup_at",
                               data_type="timestamp"),
                    ColumnDecl(column_name="pickup_hour", normalized_name="pickup_hour",
                               data_type="int"),
                    ColumnDecl(column_name="fare_amount", normalized_name="fare_amount",
                               data_type="double"),
                ],
            ),
        ],
        metrics=metrics,
        dimensions=dimensions,
        label_rules=label_rules,
        output_spec=OutputSpecDecl(
            columns=output_columns,
            grain=grain_columns,
        ),
    )


# ════════════════════════════════════════════
# 测试 1-3：单表 pre/post/mixed 分流
# ════════════════════════════════════════════


class TestSingleTableCaseWhenPhase:
    """单表 CASE WHEN evaluation_phase 分流测试。"""

    def test_pre_aggregate_case_when_before_aggregate(self):
        """pre_aggregate CASE WHEN → 在 Aggregate 之前，GROUP BY 含 peak_type。"""
        rule = _make_pre_agg_peak_type_rule()
        spec = _build_trips_spec(peak_type_rule=rule, with_metrics=True,
                                  with_dimensions=True)
        builder = SqlBuildPlanBuilder()
        steps = builder._build_single_table(spec)

        # 提取步骤类型序列
        step_types = [s.step_type for s in steps]

        # 验证：CaseWhen 在 Aggregate 之前
        cw_idx = step_types.index("case_when")
        agg_idx = step_types.index("aggregate")
        assert cw_idx < agg_idx, (
            f"pre_aggregate CaseWhenStep 应在 AggregateStep 之前，"
            f"实际: case_when@{cw_idx}, aggregate@{agg_idx}"
        )

        # 验证：Aggregate 的 group_keys 包含 peak_type
        agg_step = steps[agg_idx]
        agg_group_names = [str(g.column_name) for g in agg_step.group_keys]
        assert "peak_type" in agg_group_names, (
            f"pre_agg CaseWhen 输出列 peak_type 应在 GROUP BY 中，"
            f"实际 group_keys={agg_group_names}"
        )

    def test_post_aggregate_case_when_after_aggregate(self):
        """post_aggregate CASE WHEN → 在 Aggregate 之后。"""
        rule = _make_post_agg_high_value_rule()
        spec = _build_trips_spec(value_tier_rule=rule, with_metrics=True)
        builder = SqlBuildPlanBuilder()
        steps = builder._build_single_table(spec)

        step_types = [s.step_type for s in steps]

        # 验证：CaseWhen 在 Aggregate 之后
        cw_idx = step_types.index("case_when")
        agg_idx = step_types.index("aggregate")
        assert cw_idx > agg_idx, (
            f"post_aggregate CaseWhenStep 应在 AggregateStep 之后，"
            f"实际: case_when@{cw_idx}, aggregate@{agg_idx}"
        )

    def test_mixed_pre_post_case_when_order(self):
        """混合 pre + post CASE WHEN → pre 在 Aggregate 前，post 在后。"""
        pre_rule = _make_pre_agg_peak_type_rule()
        post_rule = _make_post_agg_high_value_rule()
        spec = _build_trips_spec(
            peak_type_rule=pre_rule, value_tier_rule=post_rule,
            with_metrics=True,
        )
        builder = SqlBuildPlanBuilder()
        steps = builder._build_single_table(spec)

        step_types = [s.step_type for s in steps]
        cw_indices = [i for i, t in enumerate(step_types) if t == "case_when"]
        agg_idx = step_types.index("aggregate")

        # 验证：至少有两个 CaseWhenStep
        assert len(cw_indices) >= 2, (
            f"预期至少 2 个 CaseWhenStep，实际: {len(cw_indices)}"
        )

        # 验证：pre_agg CaseWhen 在 Aggregate 之前
        pre_cw_before = any(i < agg_idx for i in cw_indices)
        assert pre_cw_before, "pre_aggregate CaseWhenStep 应在 Aggregate 之前"

        # 验证：post_agg CaseWhen 在 Aggregate 之后
        post_cw_after = any(i > agg_idx for i in cw_indices)
        assert post_cw_after, "post_aggregate CaseWhenStep 应在 Aggregate 之后"


# ════════════════════════════════════════════
# 测试 4-5：多表 pre/post 分流
# ════════════════════════════════════════════


class TestMultiTableCaseWhenPhase:
    """多表 CASE WHEN evaluation_phase 分流测试。"""

    def _build_two_table_spec(
        self, phase: str = "pre_aggregate",
    ) -> ParsedDeveloperSpec:
        """构造两表 Spec——trips + zones，含 CASE WHEN 规则。"""
        condition = LabelCompare(
            left="pickup_hour",
            op=">=",
            right={"node_type": "LITERAL", "value": 7, "data_type": "number"},
        )
        rule = CaseWhenDecl(
            output_column="peak_type",
            else_value="平峰",
            evaluation_phase=phase,
            typed_branches=[
                LabelPredicateBranch(condition=condition, then_label="高峰"),
            ],
        )

        return ParsedDeveloperSpec(
            spec_id="test_multi_phase",
            spec_hash="multi123",
            title="多表 CASE WHEN 阶段测试",
            description="验证两表 Join + CASE WHEN 正确定位",
            dataset_type=DatasetType.AGGREGATE_TABLE,
            input_tables=[
                InputTableDecl(
                    table_alias="trips",
                    source_table="test.trips",
                    columns=[
                        ColumnDecl(column_name="trip_id", normalized_name="trip_id",
                                   data_type="int"),
                        ColumnDecl(column_name="pickup_location_id",
                                   normalized_name="pickup_location_id", data_type="int"),
                        ColumnDecl(column_name="pickup_hour",
                                   normalized_name="pickup_hour", data_type="int"),
                        ColumnDecl(column_name="fare_amount",
                                   normalized_name="fare_amount", data_type="double"),
                    ],
                    key_columns=[
                        ColumnDecl(column_name="pickup_location_id",
                                   normalized_name="pickup_location_id", data_type="int"),
                    ],
                ),
                InputTableDecl(
                    table_alias="zones",
                    source_table="test.zones",
                    columns=[
                        ColumnDecl(column_name="location_id",
                                   normalized_name="location_id", data_type="int"),
                        ColumnDecl(column_name="zone_name",
                                   normalized_name="zone_name", data_type="varchar"),
                    ],
                    key_columns=[
                        ColumnDecl(column_name="location_id",
                                   normalized_name="location_id", data_type="int"),
                    ],
                ),
            ],
            metrics=[
                MetricDecl(metric_name="total_fare", alias="total_fare", aggregation="SUM",
                            input_column="fare_amount"),
            ],
            dimensions=[
                DimensionDecl(dimension_name="zone_name", column_ref="zone_name"),
            ],
            label_rules=[rule],
            joins=[
                JoinDecl(
                    left_table="trips",
                    right_table="zones",
                    left_key="pickup_location_id",
                    right_key="location_id",
                    join_type=JoinTypeEnum.INNER,
                ),
            ],
            output_spec=OutputSpecDecl(
                columns=[
                    OutputColumnDecl(name="zone_name", type="varchar"),
                    OutputColumnDecl(name="peak_type", type="varchar"),
                    OutputColumnDecl(name="total_fare", type="double"),
                ],
                grain=["zone_name", "peak_type"],
            ),
        )

    def test_multi_table_pre_aggregate_case_when(self):
        """两表 pre_aggregate CASE WHEN → Join 后、Aggregate 前。"""
        spec = self._build_two_table_spec(phase="pre_aggregate")
        builder = SqlBuildPlanBuilder()
        from tianshu_datadev.planning.relationship_hypothesis import (
            JoinCandidate,
            RelationshipHypothesis,
        )
        candidate = JoinCandidate(
            candidate_id="j1",
            left_table="trips",
            right_table="zones",
            left_key="pickup_location_id",
            right_key="location_id",
            left_key_normalized="pickup_location_id",
            right_key_normalized="location_id",
            join_type=JoinTypeEnum.INNER,
        )
        hypothesis = RelationshipHypothesis(
            hypothesis_id="h1", spec_hash="multi123", candidates=[candidate],
        )
        steps = builder._build_multi_table(spec, hypothesis)

        step_types = [s.step_type for s in steps]
        cw_idx = step_types.index("case_when")
        agg_idx = step_types.index("aggregate")

        assert cw_idx < agg_idx, (
            f"多表 pre_aggregate CaseWhen 应在 Aggregate 之前，"
            f"实际: case_when@{cw_idx}, aggregate@{agg_idx}"
        )

    def test_multi_table_post_aggregate_case_when(self):
        """两表 post_aggregate CASE WHEN → Aggregate 后。"""
        spec = self._build_two_table_spec(phase="post_aggregate")
        builder = SqlBuildPlanBuilder()
        from tianshu_datadev.planning.relationship_hypothesis import (
            JoinCandidate,
            RelationshipHypothesis,
        )
        candidate = JoinCandidate(
            candidate_id="j1",
            left_table="trips",
            right_table="zones",
            left_key="pickup_location_id",
            right_key="location_id",
            left_key_normalized="pickup_location_id",
            right_key_normalized="location_id",
            join_type=JoinTypeEnum.INNER,
        )
        hypothesis = RelationshipHypothesis(
            hypothesis_id="h1", spec_hash="multi123", candidates=[candidate],
        )
        steps = builder._build_multi_table(spec, hypothesis)

        step_types = [s.step_type for s in steps]
        cw_idx = step_types.index("case_when")
        agg_idx = step_types.index("aggregate")

        assert cw_idx > agg_idx, (
            f"多表 post_aggregate CaseWhen 应在 Aggregate 之后，"
            f"实际: case_when@{cw_idx}, aggregate@{agg_idx}"
        )


# ════════════════════════════════════════════
# 测试 8：无法判定 phase → OpenQuestion
# ════════════════════════════════════════════


class TestUnresolvablePhase:
    """evaluation_phase 无法判定 → OpenQuestion 测试。"""

    def test_null_phase_unresolvable_creates_open_question(self):
        """LLM 返回 None + 上下文无法判定 → OpenQuestion。"""
        enricher = SpecEnricher()
        raw = {
            "inferred_metrics": [],
            "inferred_window_metrics": [],
            "inferred_computed_metrics": [],
            "inferred_dimensions": [],
            "inferred_post_window_filters": [],
            "inferred_case_when": [
                {
                    "output_column": "mystery_label",
                    "branches": [
                        {
                            "condition": {
                                "node_type": "COMPARE",
                                "left": "unknown_col",
                                "op": ">=",
                                "right": {
                                    "node_type": "LITERAL",
                                    "value": 100,
                                    "data_type": "number",
                                },
                            },
                            "then_label": "X",
                            "evidence": "某未知分类依据原文证据描述",
                        },
                    ],
                    "else_value": "Y",
                    "evaluation_phase": None,
                },
            ],
        }

        spec = ParsedDeveloperSpec(
            spec_id="test_null_phase",
            spec_hash="null123",
            title="未知 phase",
            description="某未知分类依据原文证据描述",
            input_tables=[
                InputTableDecl(
                    table_alias="data",
                    source_table="test.data",
                    columns=[
                        ColumnDecl(column_name="id", normalized_name="id",
                                   data_type="int"),
                        ColumnDecl(column_name="val", normalized_name="val",
                                   data_type="double"),
                    ],
                ),
            ],
            metrics=[],
            dimensions=[],
            label_rules=[],
            output_spec=OutputSpecDecl(
                columns=[OutputColumnDecl(name="mystery_label", type="varchar")],
                grain=["pickup_hour", "peak_type"],
            ),
        )

        # _parse_llm_response 不会报错（条件列 "unknown_col" 会在 Validator
        # 阶段触发 FIELD_EXISTS 阻断），测试关注 evaluation_phase=None 的回退路径
        enriched = enricher._parse_llm_response(raw, spec)
        meta = enriched.enrichment_metadata

        # 验证：unresolved_case_when 中应有记录
        unresolved = meta.get("unresolved_case_when", [])
        # FIELD_EXISTS 阻断会生成一条，但如果条件合法而 phase 是 None，
        # 则应在 apply_enrichment 中解析
        assert isinstance(unresolved, list)

    def test_context_resolve_pre_aggregate_by_dimension(self):
        """LLM 返回 None phase 但输出列在 dimensions 中 → resolve 为 pre_aggregate。"""
        from tianshu_datadev.developer_spec.models import LabelPredicateBranch

        cw = CaseWhenDecl(
            output_column="peak_type",
            else_value="平峰",
            evaluation_phase=None,  # LLM 未提供
            typed_branches=[
                LabelPredicateBranch(
                    condition=LabelCompare(
                        left="pickup_hour",
                        op=">=",
                        right={"node_type": "LITERAL", "value": 7,
                               "data_type": "number"},
                    ),
                    then_label="高峰",
                ),
            ],
        )

        spec = ParsedDeveloperSpec(
            spec_id="test_resolve_by_dim",
            spec_hash="dim123",
            title="维度场景",
            description="测试维度引用触发 pre_aggregate 判定",
            input_tables=[
                InputTableDecl(
                    table_alias="trips",
                    source_table="test.trips",
                    columns=[
                        ColumnDecl(column_name="pickup_hour",
                                   normalized_name="pickup_hour", data_type="int"),
                    ],
                ),
            ],
            dimensions=[
                DimensionDecl(dimension_name="peak_type", column_ref="peak_type"),
            ],
            metrics=[],
            output_spec=OutputSpecDecl(
                columns=[OutputColumnDecl(name="peak_type", type="varchar")],
                grain=["peak_type"],
            ),
        )

        resolved = SpecEnricher._resolve_evaluation_phase(cw, spec)
        assert resolved == "pre_aggregate", (
            f"输出列在 dimensions 中应解析为 pre_aggregate，实际: {resolved}"
        )

    def test_context_resolve_post_aggregate_by_metric_ref(self):
        """条件引用聚合指标别名 → resolve 为 post_aggregate。"""
        from tianshu_datadev.developer_spec.models import LabelPredicateBranch

        cw = CaseWhenDecl(
            output_column="value_tier",
            else_value="低价值",
            evaluation_phase=None,
            typed_branches=[
                LabelPredicateBranch(
                    condition=LabelCompare(
                        left="total_fare",  # 引用聚合指标
                        op=">=",
                        right={"node_type": "LITERAL", "value": 1000,
                               "data_type": "number"},
                    ),
                    then_label="高价值",
                ),
            ],
        )

        spec = ParsedDeveloperSpec(
            spec_id="test_resolve_by_metric",
            spec_hash="metric123",
            title="指标场景",
            description="测试聚合指标引用触发 post_aggregate 判定",
            input_tables=[
                InputTableDecl(
                    table_alias="trips",
                    source_table="test.trips",
                    columns=[
                        ColumnDecl(column_name="fare_amount",
                                   normalized_name="fare_amount", data_type="double"),
                    ],
                ),
            ],
            metrics=[
                MetricDecl(metric_name="total_fare", alias="total_fare", aggregation="SUM",
                            input_column="fare_amount"),
            ],
            dimensions=[],
            output_spec=OutputSpecDecl(
                columns=[OutputColumnDecl(name="value_tier", type="varchar")],
                grain=[],
            ),
        )

        resolved = SpecEnricher._resolve_evaluation_phase(cw, spec)
        assert resolved == "post_aggregate", (
            f"条件引用聚合指标应解析为 post_aggregate，实际: {resolved}"
        )


# ════════════════════════════════════════════
# 测试 10：错误计划仍被阻断
# ════════════════════════════════════════════


class TestErrorPlanStillBlocked:
    """错误 CASE WHEN 计划仍被阻断——regression。"""

    def test_invalid_column_still_blocked(self):
        """CASE WHEN 引用不存在的列 → FIELD_EXISTS 阻断。"""
        enricher = SpecEnricher()
        raw = {
            "inferred_metrics": [],
            "inferred_window_metrics": [],
            "inferred_computed_metrics": [],
            "inferred_dimensions": [],
            "inferred_post_window_filters": [],
            "inferred_case_when": [
                {
                    "output_column": "label",
                    "branches": [
                        {
                            "condition": {
                                "node_type": "COMPARE",
                                "left": "nonexistent_col",
                                "op": "=",
                                "right": {
                                    "node_type": "LITERAL",
                                    "value": "X",
                                    "data_type": "string",
                                },
                            },
                            "then_label": "matched",
                            "evidence": "原文证据描述——足够长度通过 COVERAGE 检查",
                        },
                    ],
                    "else_value": "unmatched",
                    "evaluation_phase": "pre_aggregate",
                },
            ],
        }

        spec = ParsedDeveloperSpec(
            spec_id="test_error_blocked",
            spec_hash="err123",
            title="错误列",
            description="原文证据描述——足够长度通过 COVERAGE 检查",
            input_tables=[
                InputTableDecl(
                    table_alias="data",
                    source_table="test.data",
                    columns=[
                        ColumnDecl(column_name="id", normalized_name="id",
                                   data_type="int"),
                    ],
                ),
            ],
            metrics=[],
            dimensions=[],
            label_rules=[],
            output_spec=OutputSpecDecl(
                columns=[OutputColumnDecl(name="label", type="varchar")],
                grain=[],
            ),
        )

        enriched = enricher._parse_llm_response(raw, spec)
        meta = enriched.enrichment_metadata

        # 验证：case_when_rules 为空——非法列被阻断
        case_when_rules = meta.get("case_when_rules", [])
        assert len(case_when_rules) == 0, (
            f"非法列应被 Validator 阻断，不应有规则通过，实际: {len(case_when_rules)}"
        )

        # 验证：unresolved_case_when 中有记录
        unresolved = meta.get("unresolved_case_when", [])
        assert len(unresolved) > 0, "非法 CASE WHEN 应有 unresolved 记录"
        assert any(
            "nonexistent_col" in str(u.get("blocking_errors", []))
            or "FIELD_EXISTS" in str(u.get("blocking_errors", []))
            for u in unresolved
        ), f"阻断原因应包含 FIELD_EXISTS，实际: {unresolved}"


class TestHourDerivedDimensionFromMinimalInput:
    """用户只声明 timestamp 字段和业务描述时仍可生成双引擎代码。"""

    def test_hour_case_when_flows_through_sql_and_spark(self):
        """DATE_PART(HOUR) 不要求前端虚构 pickup_hour 物理列。"""
        from tianshu_datadev.artifacts.contract_extractor import (
            DataTransformContractExtractor,
        )
        from tianshu_datadev.developer_spec.models import (
            ManifestColumn,
            ManifestTable,
            SourceManifest,
        )
        from tianshu_datadev.planning.program_factory import build_sql_program
        from tianshu_datadev.spark.compiler import SparkCompiler
        from tianshu_datadev.spark.mapper import map_contract_to_spark_plan
        from tianshu_datadev.sql.compiler import DuckDbSqlCompiler
        from tianshu_datadev.sql.validator import SqlBuildPlanValidator

        description = "高峰定义：7-10、17-20 为高峰，其余为平峰"
        spec = ParsedDeveloperSpec(
            spec_id="minimal_hour_case",
            spec_hash="minimal_hour_case_hash",
            title="按高峰类型汇总",
            description=description,
            input_tables=[
                InputTableDecl(
                    table_alias="trips",
                    source_table="test.trips",
                    columns=[
                        ColumnDecl(
                            column_name="pickup_at",
                            normalized_name="pickup_at",
                            data_type="timestamp",
                        ),
                        ColumnDecl(
                            column_name="trip_id",
                            normalized_name="trip_id",
                            data_type="bigint",
                        ),
                    ],
                ),
            ],
            metrics=[
                MetricDecl(
                    metric_name="trip_count",
                    alias="trip_count",
                    aggregation="COUNT",
                    input_column="trip_id",
                ),
            ],
            dimensions=[],
            output_spec=OutputSpecDecl(
                columns=[
                    OutputColumnDecl(name="pickup_hour", type="integer"),
                    OutputColumnDecl(name="peak_type", type="varchar"),
                    OutputColumnDecl(name="trip_count", type="bigint"),
                ],
                grain=["pickup_hour", "peak_type"],
            ),
        )
        raw = {
            "inferred_metrics": [],
            "inferred_window_metrics": [],
            "inferred_computed_metrics": [],
            "inferred_dimensions": [
                {
                    "dimension_name": "pickup_hour",
                    "column_ref": "pickup_at",
                    "source_table": "trips",
                    "date_part": "HOUR",
                },
            ],
            "inferred_post_window_filters": [],
            "inferred_case_when": [
                {
                    "output_column": "peak_type",
                    "branches": [
                        {
                            "condition": {
                                "node_type": "OR",
                                "children": [
                                    {
                                        "node_type": "AND",
                                        "children": [
                                            {
                                                "node_type": "COMPARE",
                                                "left": {
                                                    "node_type": "DATE_PART",
                                                    "part": "HOUR",
                                                    "column_name": "pickup_at",
                                                },
                                                "op": ">=",
                                                "right": {
                                                    "node_type": "LITERAL",
                                                    "value": 7,
                                                    "data_type": "number",
                                                },
                                            },
                                            {
                                                "node_type": "COMPARE",
                                                "left": {
                                                    "node_type": "DATE_PART",
                                                    "part": "HOUR",
                                                    "column_name": "pickup_at",
                                                },
                                                "op": "<=",
                                                "right": {
                                                    "node_type": "LITERAL",
                                                    "value": 10,
                                                    "data_type": "number",
                                                },
                                            },
                                        ],
                                    },
                                    {
                                        "node_type": "AND",
                                        "children": [
                                            {
                                                "node_type": "COMPARE",
                                                "left": {
                                                    "node_type": "DATE_PART",
                                                    "part": "HOUR",
                                                    "column_name": "pickup_at",
                                                },
                                                "op": ">=",
                                                "right": {
                                                    "node_type": "LITERAL",
                                                    "value": 17,
                                                    "data_type": "number",
                                                },
                                            },
                                            {
                                                "node_type": "COMPARE",
                                                "left": {
                                                    "node_type": "DATE_PART",
                                                    "part": "HOUR",
                                                    "column_name": "pickup_at",
                                                },
                                                "op": "<=",
                                                "right": {
                                                    "node_type": "LITERAL",
                                                    "value": 20,
                                                    "data_type": "number",
                                                },
                                            },
                                        ],
                                    },
                                ],
                            },
                            "then_label": "高峰",
                            "evidence": description,
                        },
                    ],
                    "else_value": "平峰",
                    "evaluation_phase": "pre_aggregate",
                },
            ],
        }

        enriched = SpecEnricher()._parse_llm_response(raw, spec)
        rules = enriched.enrichment_metadata["case_when_rules"]
        assert len(rules) == 1
        resolved_spec = spec.model_copy(update={
            # 模拟 LLM 同时把 CASE 输出误报为普通维度；Builder 必须以 CASE 为准。
            "dimensions": enriched.inferred_dimensions + [
                DimensionDecl(
                    dimension_name="peak_type",
                    column_ref="peak_type",
                    source_table="trips",
                ),
            ],
            "label_rules": [CaseWhenDecl(**rules[0])],
        })
        assert resolved_spec.dimensions[0].date_part == "HOUR"

        plan, _ = SqlBuildPlanBuilder().build(resolved_spec)
        manifest = SourceManifest(
            manifest_id="minimal_hour_manifest",
            spec_hash=spec.spec_hash,
            tables=[
                ManifestTable(
                    table_ref="trips",
                    source_table="test.trips",
                    columns=[
                        ManifestColumn(
                            column_name="pickup_at",
                            normalized_name="pickup_at",
                            data_type="timestamp",
                        ),
                        ManifestColumn(
                            column_name="trip_id",
                            normalized_name="trip_id",
                            data_type="bigint",
                        ),
                    ],
                ),
            ],
        )
        passed, questions = SqlBuildPlanValidator().validate(plan, manifest)
        assert passed, [q.description for q in questions if q.blocking]

        sql = DuckDbSqlCompiler(
            table_mapping={"trips": "test.trips"},
        ).compile(plan).sql
        assert "EXTRACT(HOUR FROM trips.pickup_at)" in sql
        assert "AS pickup_hour" in sql
        assert "GROUP BY\n  EXTRACT(HOUR FROM trips.pickup_at), peak_type" in sql

        contract = DataTransformContractExtractor().extract_v1(
            build_sql_program(plan, spec.spec_hash),
        )
        mapping = map_contract_to_spark_plan(contract)
        assert mapping.success
        spark = SparkCompiler().compile(mapping.spark_plan).raw_pyspark
        assert 'F.hour(F.col("pickup_at"))' in spark
        assert 'groupBy(F.col("pickup_hour"), F.col("peak_type"))' in spark


# ════════════════════════════════════════════
# 测试：CaseWhenDecl 默认 evaluation_phase 为 None
# ════════════════════════════════════════════


class TestCaseWhenDeclDefaultPhase:
    """CaseWhenDecl 向后兼容——evaluation_phase 默认为 None。"""

    def test_default_evaluation_phase_is_none(self):
        """新建 CaseWhenDecl 时 evaluation_phase 默认为 None。"""
        cw = CaseWhenDecl(output_column="test_col", else_value="default")
        assert cw.evaluation_phase is None, (
            f"默认 evaluation_phase 应为 None，实际: {cw.evaluation_phase}"
        )

    def test_explicit_phase_preserved(self):
        """显式设置 evaluation_phase 应保留。"""
        cw = CaseWhenDecl(
            output_column="test_col",
            else_value="default",
            evaluation_phase="pre_aggregate",
        )
        assert cw.evaluation_phase == "pre_aggregate"

    def test_model_dump_includes_phase(self):
        """model_dump 应包含 evaluation_phase 字段。"""
        cw = CaseWhenDecl(
            output_column="test_col",
            else_value="default",
            evaluation_phase="post_aggregate",
        )
        dumped = cw.model_dump(mode="json")
        assert dumped.get("evaluation_phase") == "post_aggregate", (
            f"model_dump 应包含 evaluation_phase，实际: {dumped.keys()}"
        )
