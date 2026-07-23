"""字段 + 业务描述驱动的 TopN SQL/Spark 回归测试。"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from tianshu_datadev.api.templates import TEMPLATES
from tianshu_datadev.artifacts.contract_extractor import DataTransformContractExtractor
from tianshu_datadev.developer_spec.parser import DeveloperSpecParser
from tianshu_datadev.developer_spec.source_manifest import build_manifest_from_spec
from tianshu_datadev.planning.models import AggregateSpec
from tianshu_datadev.planning.relationship_planner import RelationshipPlanner
from tianshu_datadev.planning.spec_enricher import SpecEnricher
from tianshu_datadev.planning.sql_build_plan import SqlBuildPlanBuilder
from tianshu_datadev.spark.compiler import SparkCompiler
from tianshu_datadev.spark.contract_adapter import adapt_lite_to_v1
from tianshu_datadev.spark.mapper import map_contract_to_spark_plan
from tianshu_datadev.sql.compiler import DuckDbSqlCompiler
from tianshu_datadev.sql.validator import SqlBuildPlanValidator

_MINIMAL_COUNT_SPEC = """```markdown
---
spec:
  type: aggregate_table
  target_table: ads.events_daily
  source_tables:
    - name: dwd.user_events
      alias: ue
      role: fact
      key_columns:
        - name: id
          type: varchar
      business_columns:
        - name: stat_date
          type: date
  output_columns:
    - name: stat_date
    - name: pv
---
# 每日事件量
按 stat_date 统计事件数量，输出 stat_date 和 pv。
```"""


def _build_topn_artifacts():
    markdown = next(
        item["markdown_template"]
        for item in TEMPLATES
        if item["template_id"] == "tpl_window_topn"
    )
    spec = DeveloperSpecParser().parse(markdown)
    manifest = build_manifest_from_spec(spec)
    spec = SpecEnricher().apply_enrichment(spec, manifest)
    hypothesis, questions = RelationshipPlanner().plan(spec, manifest)
    assert not [question for question in questions if question.blocking]
    plan, _ = SqlBuildPlanBuilder().build(spec, hypothesis)
    return spec, plan


def test_business_description_generates_topn_sql_without_template_hints():
    """原始模板只靠字段和正文，也应得到维度、窗口和外层 TopN 过滤。"""
    spec, plan = _build_topn_artifacts()

    assert [metric.alias for metric in spec.metrics] == ["trip_count", "total_fare"]
    assert "zone_name" in {dimension.dimension_name for dimension in spec.dimensions}
    assert [metric.alias for metric in spec.inferred_window_metrics] == [
        "rank_in_borough"
    ]
    assert [item.column for item in spec.inferred_post_window_filters] == [
        "rank_in_borough"
    ]
    assert [step.step_type for step in plan.steps][-4:] == [
        "aggregate", "window", "filter", "project",
    ]

    table_mapping = {
        table.table_alias: str(table.source_table)
        for table in spec.input_tables
    }
    sql = DuckDbSqlCompiler(table_mapping=table_mapping).compile(plan).sql
    assert "SUM(*)" not in sql
    assert "ROW_NUMBER() OVER" in sql
    assert "ORDER BY trip_count DESC" in sql
    assert "_sub.rank_in_borough <= 10" in sql


def test_contract_preserves_post_window_filter_for_spark():
    """Contract 必须保留过滤阶段和窗口方向，使 Spark 与 SQL 顺序一致。"""
    spec, plan = _build_topn_artifacts()
    lite = DataTransformContractExtractor().extract(
        plan, output_grain=spec.output_spec.grain,
    )
    contract = adapt_lite_to_v1(lite)

    assert [item.phase for item in contract.filters] == [
        "pre_transform", "pre_transform", "post_window",
    ]
    # grain tiebreaker 追加后：location_id 作为确定性排序键，确保跨引擎一致
    assert contract.window_specs[0].order_by == ["trip_count DESC", "location_id ASC"]

    mapping = map_contract_to_spark_plan(contract)
    assert mapping.success is True
    step_types = [step.step_type.value for step in mapping.spark_plan.steps]
    assert step_types[-4:] == ["aggregate", "window", "filter", "project"]

    code = SparkCompiler().compile(mapping.spark_plan).raw_pyspark
    compile(code, "<generated-spark>", "exec")
    # grain tiebreaker 追加后——location_id ASC 作为确定性排序键
    assert 'F.col("trip_count").desc()' in code
    assert 'F.col("location_id").asc()' in code
    assert 'filter(F.col("rank_in_borough") <= 10)' in code
    # 多表场景下 GROUP BY 键使用 table.column 格式消除 Spark AMBIGUOUS_REFERENCE
    assert 'F.col("tz.zone_name")' in code


def test_non_count_aggregate_without_input_is_rejected_before_compile():
    """无输入列的 SUM 必须在 IR 构造时失败，不能下沉为引擎 Binder Error。"""
    with pytest.raises(ValidationError, match="SUM 聚合必须声明"):
        AggregateSpec(aggregation="SUM", alias="zone_name")


def test_llm_structured_result_accepts_dimension_and_post_window_filter():
    """真实 Agent 可以用封闭结构表达普通维度和窗口后过滤。"""
    spec, _ = _build_topn_artifacts()
    raw = {
        "inferred_metrics": [],
        "inferred_window_metrics": [{
            "metric_name": "rank_in_borough",
            "window_function": "ROW_NUMBER",
            "input_column": None,
            "partition_by": ["borough"],
            "order_by": ["trip_count DESC"],
            "alias": "rank_in_borough",
            "confidence": "high",
            "reasoning": "业务描述明确要求分区排名",
        }],
        "inferred_computed_metrics": [],
        "inferred_dimensions": [{
            "dimension_name": "zone_name",
            "column_ref": "zone_name",
            "source_table": "tz",
        }],
        "inferred_post_window_filters": [{
            "column": "rank_in_borough",
            "operator": "<=",
            "value": 10,
        }],
    }

    enriched = SpecEnricher()._parse_llm_response(raw, spec)
    assert enriched.inferred_dimensions[0].column_ref == "zone_name"
    assert enriched.inferred_post_window_filters[0].value == 10


def test_create_app_injects_planning_agent_when_api_key_exists(monkeypatch):
    """生产 API 有模型配置时，SQL 规格增强器必须实际使用 Agent。"""
    from tianshu_datadev.api.app import create_app

    monkeypatch.setenv("DEEPSEEK_API_KEY", "sk-test")
    app = create_app()

    assert app.state.pipeline._spec_enricher._adapter is not None


def test_llm_omitted_exact_source_dimension_is_filled_deterministically():
    """Agent 漏掉同名源字段时，确定性层应补齐维度而非要求用户扩写模板。"""
    spec = DeveloperSpecParser().parse(_MINIMAL_COUNT_SPEC)
    raw = {
        "inferred_metrics": [{
            "metric_name": "pv",
            "aggregation": "COUNT",
            "input_column": "id",
            "alias": "pv",
        }],
        "inferred_window_metrics": [],
        "inferred_computed_metrics": [],
        "inferred_dimensions": [],
        "inferred_post_window_filters": [],
    }

    enriched = SpecEnricher()._parse_llm_response(raw, spec)

    assert [dimension.dimension_name for dimension in enriched.inferred_dimensions] == [
        "stat_date"
    ]


def test_validator_rejects_ungrouped_source_column_after_aggregate():
    """聚合后投影原始列但未分组时，Validator 必须确定性阻断。"""
    spec = DeveloperSpecParser().parse(_MINIMAL_COUNT_SPEC)
    raw = {
        "inferred_metrics": [{
            "metric_name": "pv",
            "aggregation": "COUNT",
            "input_column": "id",
            "alias": "pv",
        }],
        "inferred_window_metrics": [],
        "inferred_computed_metrics": [],
        "inferred_dimensions": [],
        "inferred_post_window_filters": [],
    }
    enriched = SpecEnricher()._parse_llm_response(raw, spec)
    spec = spec.model_copy(update={
        "metrics": enriched.inferred_metrics,
        "dimensions": enriched.inferred_dimensions,
    })
    manifest = build_manifest_from_spec(spec)
    plan, _ = SqlBuildPlanBuilder().build(spec, None)
    broken_steps = [
        step.model_copy(update={"group_keys": []})
        if step.step_type == "aggregate" else step
        for step in plan.steps
    ]
    broken_plan = plan.model_copy(update={"steps": broken_steps})

    passed, questions = SqlBuildPlanValidator().validate(
        broken_plan, manifest, spec=spec,
    )

    assert passed is False
    assert any(
        question.question_id.startswith("Q-VAL-AGG-PROJECT-")
        for question in questions
    )
