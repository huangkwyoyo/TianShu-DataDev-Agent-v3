"""管线测试——_find_unresolved_derived_columns()、_prepare_spec_for_planning()。"""

from decimal import Decimal

from tianshu_datadev.developer_spec.models import (
    CaseWhenDecl,
    ColumnDecl,
    CompareOp,
    DatasetType,
    InputTableDecl,
    LabelCompare,
    LabelPredicateBranch,
    LabelTypedLiteral,
    OutputColumnDecl,
    OutputSpecDecl,
    ParsedDeveloperSpec,
)
from tianshu_datadev.labels.resolver import _find_unresolved_derived_columns


def _make_spec(output_cols, source_cols=None):
    """构造最小 ParsedDeveloperSpec——用于 resolver 单元测试。"""
    cols = source_cols if source_cols is not None else output_cols
    return ParsedDeveloperSpec(
        spec_id="test", spec_hash="h", title="测试", description="",
        dataset_type=DatasetType.UNSPECIFIED,
        input_tables=[InputTableDecl(
            table_alias="t", source_table="test",
            columns=[ColumnDecl(column_name=c, normalized_name=c) for c in cols],
            key_columns=[], business_columns=[],
        )],
        metrics=[], dimensions=[],
        output_spec=OutputSpecDecl(
            columns=[OutputColumnDecl(name=c, type="string") for c in output_cols],
            grain=[],
        ),
        time_range=None,
    )


class TestFindUnresolvedDerivedColumns:

    def test_all_physical_returns_empty(self):
        spec = _make_spec(["col1", "col2"])
        assert _find_unresolved_derived_columns(spec) == []

    def test_derived_column_detected(self):
        spec = _make_spec(["col1", "derived_col"], source_cols=["col1"])
        unresolved = _find_unresolved_derived_columns(spec)
        assert "derived_col" in unresolved
        assert "col1" not in unresolved

    def test_all_derived(self):
        spec = _make_spec(["label_a", "label_b"], source_cols=["col1", "col2"])
        unresolved = _find_unresolved_derived_columns(spec)
        assert sorted(unresolved) == ["label_a", "label_b"]

    def test_label_rule_output_excluded(self):
        spec = _make_spec(["distance_category"], source_cols=["distance_miles"])
        spec.label_rules.append(CaseWhenDecl(
            output_column="distance_category",
            else_value="long",
            typed_branches=[
                LabelPredicateBranch(
                    condition=LabelCompare(
                        left="distance_miles", op=CompareOp.LTE,
                        right=LabelTypedLiteral(value=Decimal("2"), data_type="number"),
                    ),
                    then_label="short",
                ),
            ],
        ))
        unresolved = _find_unresolved_derived_columns(spec)
        assert "distance_category" not in unresolved


# ════════════════════════════════════════════
# Planner / SpecEnricher 分工 + unresolved 阻断
# ════════════════════════════════════════════


class TestPlannerSpecEnricherDivision:
    """验证 Planner 只覆盖部分输出列，其余留给 SpecEnricher，
    最终 unresolved 检查能阻断真实遗漏。"""

    def _make_spec_with_mixed_outputs(self):
        """构造 Spec——输出列混合了 Planner 覆盖项和 SpecEnricher 覆盖项。"""
        return ParsedDeveloperSpec(
            spec_id="test_division",
            spec_hash="hash_division",
            title="高峰时段与异常出行分析",
            description="按小时和区域统计出行次数，区分高峰/平峰，标记异常出行，窗口排名",
            dataset_type=DatasetType.AGGREGATE_TABLE,
            input_tables=[InputTableDecl(
                table_alias="ft",
                source_table="fact_table",
                columns=[
                    ColumnDecl(column_name="pickup_at", normalized_name="pickup_at",
                              data_type="timestamp"),
                    ColumnDecl(column_name="borough", normalized_name="borough",
                              data_type="varchar"),
                    ColumnDecl(column_name="trip_count", normalized_name="trip_count",
                              data_type="bigint"),
                    ColumnDecl(column_name="trip_duration", normalized_name="trip_duration",
                              data_type="integer"),
                ],
                key_columns=[],
                business_columns=[],
            )],
            metrics=[],
            dimensions=[],
            output_spec=OutputSpecDecl(
                columns=[
                    # Planner 产出
                    OutputColumnDecl(name="pickup_hour"),
                    OutputColumnDecl(name="borough"),
                    OutputColumnDecl(name="trip_count"),
                    OutputColumnDecl(name="peak_type"),
                    # SpecEnricher 产出
                    OutputColumnDecl(name="anomaly_trip_count"),
                    OutputColumnDecl(name="rank_by_trip_count"),
                ],
                grain=["pickup_hour", "borough"],
            ),
            time_range=None,
        )

    def test_planner_resolves_peak_type_and_pickup_hour(self):
        """Planner 产出 peak_type(CASE WHEN) + pickup_hour(derived_dimension)
        → _find_unresolved_derived_columns 不再报告它们。"""
        from tianshu_datadev.developer_spec.models import (
            AggregationType,
            CaseWhenBranch,
            CaseWhenRule,
            DerivedDimensionDecl,
            DimensionDecl,
            MetricDecl,
            RequirementProposal,
        )
        from tianshu_datadev.planning.proposal_promotion import ProposalPromotion

        spec = self._make_spec_with_mixed_outputs()

        # 模拟 Planner 产出：只覆盖 peak_type / pickup_hour / borough / trip_count
        proposal = RequirementProposal(
            proposal_id="test_001",
            spec_hash=spec.spec_hash,
            dimensions=[DimensionDecl(
                dimension_name="borough",
                column_ref="borough",
                source_table="ft",
            )],
            derived_dimensions=[DerivedDimensionDecl(
                dimension_name="pickup_hour",
                source_column="pickup_at",
                source_table="ft",
                time_function="HOUR",
            )],
            metrics=[MetricDecl(
                metric_name="行程数",
                aggregation=AggregationType.COUNT,
                alias="trip_count",
            )],
            case_when_rules=[CaseWhenRule(
                output_column="peak_type",
                branches=[CaseWhenBranch(
                    condition={
                        "node_type": "COMPARE",
                        "left": "pickup_hour",
                        "op": "IN",
                        "right": {
                            "node_type": "LITERAL",
                            "value": [7, 8, 9, 10, 17, 18, 19, 20],
                            "data_type": "number",
                        },
                    },
                    then_value="高峰",
                )],
                else_value="平峰",
            )],
            uncertainties=[],
            llm_model="test",
            inference_time_ms=0,
            total_inferred=4,
        )

        # Promotion 写入 spec
        promotion = ProposalPromotion()
        spec = promotion.promote(proposal, spec)

        unresolved = _find_unresolved_derived_columns(spec)
        # Planner 覆盖的列不应在 unresolved 中
        assert "pickup_hour" not in unresolved
        assert "borough" not in unresolved
        assert "trip_count" not in unresolved
        assert "peak_type" not in unresolved
        # SpecEnricher 覆盖的列应仍在 unresolved 中
        assert "anomaly_trip_count" in unresolved
        assert "rank_by_trip_count" in unresolved

    def test_real_omission_still_blocked_by_unresolved(self):
        """真实遗漏（所有输出列都未解析）→ unresolved 非空，管线硬阻断。"""
        spec = self._make_spec_with_mixed_outputs()
        # 无 Planner 产出，无 SpecEnricher —— 所有列应为 unresolved
        unresolved = _find_unresolved_derived_columns(spec)
        # borough 和 trip_count 在 input_tables 中已是物理列，不会被标记为 unresolved
        assert len(unresolved) == 4  # pickup_hour / peak_type / anomaly_trip_count / rank_by_trip_count
        assert "peak_type" in unresolved
        assert "anomaly_trip_count" in unresolved
        assert "rank_by_trip_count" in unresolved

    def test_window_metric_alias_resolves_column(self):
        """InferredWindowMetric 的 alias（而非 metric_name）应能使列被解析。"""
        from tianshu_datadev.developer_spec.models import InferredWindowMetric
        spec = self._make_spec_with_mixed_outputs()
        # 模拟 SpecEnricher 产出窗口指标——alias=输出列名, metric_name≠alias
        spec.inferred_window_metrics.append(InferredWindowMetric(
            metric_name="Trip Count Rank",
            window_function="RANK",
            input_column="trip_count",
            partition_by=["borough"],
            order_by=["trip_count DESC"],
            alias="rank_by_trip_count",
        ))
        unresolved = _find_unresolved_derived_columns(spec)
        # rank_by_trip_count 应已被 alias 匹配，不在 unresolved 中
        assert "rank_by_trip_count" not in unresolved
        # anomaly_trip_count 仍为 unresolved（未添加为指标/窗口指标）
        assert "anomaly_trip_count" in unresolved
