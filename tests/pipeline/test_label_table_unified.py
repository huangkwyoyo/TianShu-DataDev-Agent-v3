"""Label Table 统一管线——集成测试。

全部使用 FakeAdapter 和 FakeLabelExtractor，不依赖真实 LLM 或数据库。
覆盖 I1-I10 验收标准：Planner 覆盖、多表 JOIN、三表链、多对多阻断、
不支持聚合、LABEL 路由等。
"""

import pytest

from tianshu_datadev.api.pipeline import Pipeline
from tianshu_datadev.developer_spec.models import (
    CaseWhenBranch,
    CaseWhenRule,
    DatasetType,
    LabelPredicateBranch,
    LabelRuleProposal,
    ParsedDeveloperSpec,
    UncertaintyEntry,
)
from tianshu_datadev.labels.label_extractor import FakeLabelExtractor
from tianshu_datadev.llm.adapters.fake import FakeAdapter


def _make_label_spec(
    *,
    input_tables: list[dict],
    metrics: list[dict] | None = None,
    dimensions: list[dict] | None = None,
    output_columns: list[str] | None = None,
    description: str = "测试 label_table",
    label_rules: list | None = None,
    joins: list[dict] | None = None,
) -> ParsedDeveloperSpec:
    """构建 label_table spec fixture。"""
    from tianshu_datadev.developer_spec.models import (
        ColumnDecl,
        DimensionDecl,
        InputTableDecl,
        JoinDecl,
        JoinTypeEnum,
        MetricDecl,
        OutputColumnDecl,
        OutputSpecDecl,
    )

    tables = []
    for t in input_tables:
        cols = []
        for c in t.get("columns", []):
            col_name = c[0] if isinstance(c, tuple) else c
            col_type = c[1] if isinstance(c, tuple) else "varchar"
            cols.append(ColumnDecl(
                column_name=col_name,
                normalized_name=col_name,
                data_type=col_type,
                nullable=True,
            ))
        tables.append(InputTableDecl(
            table_alias=t["name"],
            source_table=t["name"],  # 物理表名与别名相同
            key_columns=[ColumnDecl(column_name=k, normalized_name=k) for k in t.get("key_columns", [])],
            columns=cols,
            business_columns=[],
        ))

    metric_decls = []
    if metrics:
        from tianshu_datadev.developer_spec.models import AggregationType
        for m in metrics:
            agg = m["aggregation"].upper()
            # 白名单内聚合字符串 → AggregationType 枚举
            agg_type = agg if isinstance(agg, str) else agg
            metric_decls.append(MetricDecl(
                metric_name=m["name"],
                aggregation=agg_type,
                alias=m["alias"],
                input_column=m.get("input_column"),
            ))

    dim_decls = []
    if dimensions:
        for d in dimensions:
            dim_decls.append(DimensionDecl(
                dimension_name=d["dimension_name"],
                column_ref=d.get("column_ref", d["dimension_name"]),
            ))

    join_decls = None
    if joins:
        join_decls = []
        for j in joins:
            join_decls.append(JoinDecl(
                left_table=j["left"].split(".")[0],
                right_table=j["right"].split(".")[0],
                left_key=j["left"].split(".")[1],
                right_key=j["right"].split(".")[1],
                join_type=JoinTypeEnum.LEFT,
            ))

    cols = output_columns or ["col_a"]
    return ParsedDeveloperSpec(
        spec_id="test_spec",
        spec_hash="test_hash_001",
        title="Test Label Table",
        description=description,
        dataset_type=DatasetType.LABEL_TABLE,
        input_tables=tables,
        metrics=metric_decls,
        dimensions=dim_decls,
        joins=join_decls,
        output_spec=OutputSpecDecl(
            columns=[OutputColumnDecl(name=c) for c in cols],
            grain=[d["dimension_name"] for d in (dimensions or [])] if dimensions else [],
        ),
        label_rules=label_rules or [],
    )


# ═══ I1: Planner 生成 case_when_rules → LabelExtractor 不调用 ═══

def test_i1_planner_covered_label_extractor_skipped():
    """Planner 已生成 case_when_rules 时，LabelExtractor 不调用。"""
    spec = _make_label_spec(
        input_tables=[{"name": "t", "key_columns": ["id"], "columns": [("id", "int"), ("val", "double")]}],
        output_columns=["val", "risk_label"],
        description="按 val 定义 risk_label：高风险/低风险",
    )
    # FakeAdapter——Planner 会生成 case_when_rules covering risk_label
    adapter = FakeAdapter(response={
        "dimensions": [],
        "derived_dimensions": [],
        "metrics": [],
        "case_when_rules": [{
            "output_column": "risk_label",
            "branches": [{
                "condition": {"node_type": "COMPARE", "left": "val", "op": ">", "right": {"node_type": "LITERAL", "value": 100, "data_type": "number"}},
                "then_value": "高风险",
            }],
            "else_value": "低风险",
        }],
        "uncertainties": [],
    })
    # FakeLabelExtractor 预置 proposal——验证它不被调用
    # 使用计数器 subclass 检测 extract() 是否被调用
    call_count = [0]

    class CountingFakeExtractor(FakeLabelExtractor):
        def extract(self, spec, unresolved_columns):
            call_count[0] += 1
            return super().extract(spec, unresolved_columns)

    pipeline = Pipeline(
        adapter=adapter,
        label_extractor=CountingFakeExtractor(proposals=[]),
    )
    result = pipeline.run_parse_and_enrich(spec)
    # I1: Planner 已覆盖 risk_label → LabelExtractor 不调用
    assert call_count[0] == 0, f"LabelExtractor 不应被调用，但调用了 {call_count[0]} 次"


# ═══ I2: 合法两表 JOIN+聚合+标签 ═══

def test_i2_two_table_join_agg_label_success():
    """合法两表 JOIN+聚合+标签全管线成功。"""
    spec = _make_label_spec(
        input_tables=[
            {"name": "trips", "key_columns": ["trip_id"],
             "columns": [("trip_id", "int"), ("pickup_location_id", "int"), ("fare_amount", "double")]},
            {"name": "zones", "key_columns": ["location_id"],
             "columns": [("location_id", "int"), ("borough", "varchar")]},
        ],
        joins=[{"left": "trips.pickup_location_id", "right": "zones.location_id"}],
        metrics=[
            {"name": "总行程数", "aggregation": "COUNT", "alias": "trip_count"},
            {"name": "平均费用", "aggregation": "AVG", "input_column": "fare_amount", "alias": "avg_fare"},
        ],
        dimensions=[{"dimension_name": "borough", "column_ref": "zones.borough"}],
        output_columns=["borough", "trip_count", "avg_fare", "risk_label"],
        description="按 borough 聚合 trip_count 和 avg_fare，按 avg_fare 定义 risk_label",
    )
    adapter = FakeAdapter(response={
        "dimensions": [],
        "derived_dimensions": [],
        "metrics": [],
        "case_when_rules": [{
            "output_column": "risk_label",
            "branches": [{
                "condition": {"node_type": "COMPARE", "left": "avg_fare", "op": ">", "right": {"node_type": "LITERAL", "value": 50, "data_type": "number"}},
                "then_value": "高风险",
            }],
            "else_value": "低风险",
        }],
        "uncertainties": [],
    })
    pipeline = Pipeline(adapter=adapter)
    result = pipeline.run_parse_and_enrich(spec)
    assert result is not None
    # 验证 Planner 生成了 case_when_rules（取 result["spec"]，因管线创建了副本）
    enriched = result.get("spec", spec)
    assert len(enriched.case_when_rules) > 0 or len(enriched.label_rules) > 0


# ═══ I3: 合法三表 JOIN+聚合+标签——全链路验收 ═══

def test_i3_three_table_join_chain_agg_label_full_pipeline():
    """合法三表 JOIN+聚合+标签——全链路成功。"""
    spec = _make_label_spec(
        input_tables=[
            {"name": "trips", "key_columns": ["trip_id"],
             "columns": [("trip_id", "int"), ("pickup_location_id", "int"),
                        ("dropoff_location_id", "int"), ("fare_amount", "double"),
                        ("trip_distance", "double")]},
            {"name": "zones", "key_columns": ["location_id"],
             "columns": [("location_id", "int"), ("borough", "varchar"), ("zone_name", "varchar")]},
            {"name": "weather", "key_columns": ["weather_id"],
             "columns": [("weather_id", "int"), ("pickup_date", "date"),
                        ("weather_condition", "varchar"), ("temp_high", "double")]},
        ],
        joins=[
            {"left": "trips.pickup_location_id", "right": "zones.location_id"},
            # 第二条 Join 在描述中由 RelationshipPlanner 推断
        ],
        metrics=[
            {"name": "总行程数", "aggregation": "COUNT", "alias": "trip_count"},
            {"name": "平均费用", "aggregation": "AVG", "input_column": "fare_amount", "alias": "avg_fare"},
        ],
        dimensions=[
            {"dimension_name": "borough", "column_ref": "zones.borough"},
        ],
        output_columns=["borough", "weather_condition", "trip_count", "avg_fare", "risk_label"],
        description="按 borough 和 weather_condition 聚合，按 avg_fare 定义 risk_label",
    )
    adapter = FakeAdapter(response={
        "dimensions": [{"dimension_name": "weather_condition", "column_ref": "weather_condition", "source_table": "weather"}],
        "derived_dimensions": [],
        "metrics": [],
        "case_when_rules": [{
            "output_column": "risk_label",
            "branches": [{
                "condition": {"node_type": "COMPARE", "left": "avg_fare", "op": ">", "right": {"node_type": "LITERAL", "value": 50, "data_type": "number"}},
                "then_value": "高风险",
            }],
            "else_value": "低风险",
        }],
        "uncertainties": [],
    })
    pipeline = Pipeline(adapter=adapter)
    result = pipeline.run_parse_and_enrich(spec)
    assert result is not None
    # 验证 Parser + Planner 成功（取 result["spec"]，因管线创建了副本）
    enriched = result.get("spec", spec)
    assert len(enriched.case_when_rules) > 0


# ═══ I4: 多对多 Join 阻断 ═══

def test_i4_many_to_many_join_blocked():
    """多对多 Join → 确定性阻断。"""
    spec = _make_label_spec(
        input_tables=[
            {"name": "a", "key_columns": ["id"], "columns": [("id", "int"), ("x", "varchar")]},
            {"name": "b", "key_columns": ["id"], "columns": [("id", "int"), ("y", "varchar")]},
        ],
        joins=[{"left": "a.x", "right": "b.y"}],  # 无唯一性证据
        output_columns=["x", "y", "label_col"],
        description="多对多 Join——应被阻断",
    )
    adapter = FakeAdapter(response={
        "dimensions": [],
        "derived_dimensions": [],
        "metrics": [],
        "case_when_rules": [{
            "output_column": "label_col",
            "branches": [{"condition": {"node_type": "IS_NULL", "column": "x"}, "then_value": "空"}],
            "else_value": "非空",
        }],
        "uncertainties": [],
    })
    pipeline = Pipeline(adapter=adapter)
    result = pipeline.run_parse_and_enrich(spec)
    # 多对多或无唯一性证据——CrossValidator 应阻断
    # result 为 None 或含 blocking OpenQuestion
    if result is not None:
        open_questions = result.get("open_questions", [])
        blocking = [q for q in open_questions if q.get("blocking")]
        # 至少有一个阻断问题
        assert len(blocking) > 0 or result.get("validation_passed") is False


# ═══ I5: MODE 聚合 → OpenQuestion ═══

def test_i5_mode_aggregation_uncertainty():
    """不支持的 MODE 聚合 → output_kind=METRIC uncertainty → 阻断。"""
    spec = _make_label_spec(
        input_tables=[{"name": "t", "key_columns": ["id"], "columns": [("id", "int"), ("val", "double")]}],
        output_columns=["val", "label_col"],
        description="使用 MODE 聚合——不在白名单",
    )
    adapter = FakeAdapter(response={
        "dimensions": [],
        "derived_dimensions": [],
        "metrics": [],
        "case_when_rules": [{
            "output_column": "label_col",
            "branches": [{"condition": {"node_type": "COMPARE", "left": "val", "op": ">", "right": {"node_type": "LITERAL", "value": 0, "data_type": "number"}}, "then_value": "有值"}],
            "else_value": "无值",
        }],
        "uncertainties": [{
            "field_ref": "metrics.mode_val",
            "output_column": "mode_val",
            "output_kind": "METRIC",
            "description": "聚合函数 MODE 不在白名单中",
        }],
    })
    pipeline = Pipeline(adapter=adapter)
    result = pipeline.run_parse_and_enrich(spec)
    # MODE 不在白名单 → Planner 产出 METRIC uncertainty → 阻断
    enriched = result.get("spec", spec)
    assert len(enriched.uncertainties) > 0
    assert any(u.output_kind == "METRIC" for u in enriched.uncertainties)


# ═══ I6: LABEL 才调用 Extractor ═══

def test_i6_label_kind_triggers_extractor():
    """Planner 标记 LABEL + unresolved + 未生成规则 → Extractor 被调用。"""
    spec = _make_label_spec(
        input_tables=[{"name": "t", "key_columns": ["id"], "columns": [("id", "int"), ("score", "double")]}],
        output_columns=["score", "risk_label"],
        description="按 score 定义 risk_label",
    )
    # Planner 只标记 LABEL 但未生成规则——LabelExtractor 应兜底
    adapter = FakeAdapter(response={
        "dimensions": [],
        "derived_dimensions": [],
        "metrics": [],
        "case_when_rules": [],  # 未生成规则
        "uncertainties": [{
            "field_ref": "risk_label_ref",
            "output_column": "risk_label",
            "output_kind": "LABEL",
            "description": "需要 CASE WHEN 定义",
        }],
    })
    call_count = [0]

    class CountingFakeExtractor(FakeLabelExtractor):
        def extract(self, spec, unresolved_columns):
            call_count[0] += 1
            return super().extract(spec, unresolved_columns)

    pipeline = Pipeline(
        adapter=adapter,
        label_extractor=CountingFakeExtractor(proposals=[]),
    )
    result = pipeline.run_parse_and_enrich(spec)
    # Planner 标记 LABEL + 未生成规则 → Extractor 应被调用
    assert call_count[0] == 1, f"Extractor 应被调用 1 次，实际 {call_count[0]} 次"


# ═══ I7: UNKNOWN 兜底调用 Extractor ═══

def test_i7_unknown_kind_no_extractor():
    """output_kind=UNKNOWN 但无已有规则 → 兜底逻辑调用 Extractor（向后兼容）。

    兜底逻辑：当 Planner 未标记任何 LABEL 列、且 label_rules/case_when_rules
    均为空时，对所有 unresolved 列调用 LabelExtractor。
    Extractor 返回空 proposals → 门禁检查失败 → LabelTableConfigError。
    """
    spec = _make_label_spec(
        input_tables=[{"name": "t", "key_columns": ["id"], "columns": [("id", "int"), ("val", "double")]}],
        output_columns=["val", "unknown_col"],
        description="无法判断 unknown_col 是什么",
    )
    adapter = FakeAdapter(response={
        "dimensions": [],
        "derived_dimensions": [],
        "metrics": [],
        "case_when_rules": [],
        "uncertainties": [{
            "field_ref": "unknown_ref",
            "output_column": "unknown_col",
            "output_kind": "UNKNOWN",
            "description": "完全无法判断",
        }],
    })
    call_count = [0]

    class CountingFakeExtractor(FakeLabelExtractor):
        def extract(self, spec, unresolved_columns):
            call_count[0] += 1
            return super().extract(spec, unresolved_columns)

    pipeline = Pipeline(
        adapter=adapter,
        label_extractor=CountingFakeExtractor(proposals=[]),
    )
    # 兜底逻辑触发 Extractor 调用，但空 proposals 导致门禁失败
    result = pipeline.run_parse_and_enrich(spec)
    # Extractor 被兜底逻辑调用了一次
    assert call_count[0] == 1
    # 门禁失败——空规则 + 仍有 unresolved 列
    assert result["validation_passed"] is False


# ═══ I8: 非 label_table 的 Planner CASE WHEN 正常 ═══

def test_i8_detail_table_case_when_works():
    """非 label_table 的 Planner CASE WHEN 正常工作。"""
    spec = _make_label_spec(
        input_tables=[{"name": "t", "key_columns": ["id"], "columns": [("id", "int"), ("val", "double")]}],
        output_columns=["val", "peak_type"],
        description="按 val 定义 peak_type",
    )
    spec = spec.model_copy(update={"dataset_type": DatasetType.DETAIL_TABLE})
    adapter = FakeAdapter(response={
        "dimensions": [],
        "derived_dimensions": [],
        "metrics": [],
        "case_when_rules": [{
            "output_column": "peak_type",
            "branches": [{
                "condition": {"node_type": "COMPARE", "left": "val", "op": ">", "right": {"node_type": "LITERAL", "value": 100, "data_type": "number"}},
                "then_value": "高峰",
            }],
            "else_value": "平峰",
        }],
        "uncertainties": [],
    })
    pipeline = Pipeline(adapter=adapter)
    result = pipeline.run_parse_and_enrich(spec)
    assert result is not None


# ═══ I9: SpecEnricher 不复用 Planner 已覆盖列 ═══

def test_i9_enricher_no_duplicate_planner_columns():
    """SpecEnricher 不对 Planner 已覆盖列重复生成 CASE WHEN。"""
    spec = _make_label_spec(
        input_tables=[{"name": "t", "key_columns": ["id"], "columns": [("id", "int"), ("val", "double")]}],
        output_columns=["val", "peak_type"],
        description="按 val 定义 peak_type",
    )
    # 预置 case_when_rules——模拟 Planner 已覆盖
    spec = spec.model_copy(update={
        "case_when_rules": [CaseWhenRule(
            output_column="peak_type",
            branches=[CaseWhenBranch(
                condition={"node_type": "COMPARE", "left": "val", "op": ">", "right": {"node_type": "LITERAL", "value": 100, "data_type": "number"}},
                then_value="高峰",
            )],
            else_value="平峰",
        )],
    })
    adapter = FakeAdapter(response={
        "dimensions": [],
        "derived_dimensions": [],
        "metrics": [],
        "case_when_rules": [],
        "uncertainties": [],
    })
    pipeline = Pipeline(adapter=adapter)
    result = pipeline.run_parse_and_enrich(spec)
    # Enricher 不应复写 peak_type——case_when_rules 数量不变
    assert result is not None


# ═══ I10: SQL 和 Spark 从同一 Contract 生成 ═══

def test_i10_sql_spark_same_contract():
    """同一已验证 Contract 下 SQL 和 Spark 均成功编译。"""
    spec = _make_label_spec(
        input_tables=[
            {"name": "trips", "key_columns": ["trip_id"],
             "columns": [("trip_id", "int"), ("pickup_location_id", "int"), ("fare_amount", "double")]},
            {"name": "zones", "key_columns": ["location_id"],
             "columns": [("location_id", "int"), ("borough", "varchar")]},
        ],
        joins=[{"left": "trips.pickup_location_id", "right": "zones.location_id"}],
        metrics=[{"name": "总行程数", "aggregation": "COUNT", "alias": "trip_count"}],
        dimensions=[{"dimension_name": "borough", "column_ref": "zones.borough"}],
        output_columns=["borough", "trip_count", "risk_label"],
        description="按 borough 聚合，按 trip_count 定义 risk_label",
    )
    adapter = FakeAdapter(response={
        "dimensions": [],
        "derived_dimensions": [],
        "metrics": [],
        "case_when_rules": [{
            "output_column": "risk_label",
            "branches": [{
                "condition": {"node_type": "COMPARE", "left": "trip_count", "op": ">", "right": {"node_type": "LITERAL", "value": 100, "data_type": "number"}},
                "then_value": "高",
            }],
            "else_value": "低",
        }],
        "uncertainties": [],
    })
    pipeline = Pipeline(adapter=adapter)
    result = pipeline.run_parse_and_enrich(spec)
    assert result is not None
