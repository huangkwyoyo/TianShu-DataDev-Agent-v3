"""窄范围 RatioExpr 的半结构化输入 Pipeline/API 回归测试。"""

from __future__ import annotations

from copy import deepcopy

from fastapi.testclient import TestClient

from tianshu_datadev.api.app import create_app
from tianshu_datadev.api.pipeline import Pipeline
from tianshu_datadev.artifacts.contract_extractor import (
    DataTransformContractExtractor,
)
from tianshu_datadev.developer_spec.models import RequirementPlannerOutput
from tianshu_datadev.llm.adapters.base import ProviderAdapter
from tianshu_datadev.planning.models import RatioExpr
from tianshu_datadev.planning.sql_build_plan import ProjectStep
from tianshu_datadev.spark.compiler import SparkCompiler
from tianshu_datadev.spark.contract_adapter import adapt_lite_to_v1
from tianshu_datadev.spark.mapper import map_contract_to_spark_plan
from tianshu_datadev.spark.plan_comparator import (
    ComparisonStatus,
    PlanComparator,
)
from tianshu_datadev.sql.compiler import DuckDbSqlCompiler


class _EmptyRequirementPlanner:
    """让本组测试只验证 SpecEnricher 对自然语言比率的职责。"""

    def plan(self, spec, manifest) -> RequirementPlannerOutput:  # noqa: ANN001, ARG002
        return RequirementPlannerOutput()


class _RatioInferenceAdapter(ProviderAdapter):
    """根据业务案例返回确定性的类型化指标与比率候选。"""

    def invoke(
        self,
        system_message: str,
        user_message: str,
        json_schema: dict,
        model: str,
        temperature: float,
    ) -> dict:
        del system_message, json_schema, model, temperature
        if "injury_rate" in user_message:
            return deepcopy(_CASE02_INFERENCE)
        if "county_share_pct" in user_message:
            return deepcopy(_CASE03_INFERENCE)
        raise AssertionError("测试输入未命中已声明案例")

    def provider_name(self) -> str:
        return "ratio-test"


_CASE02_INFERENCE = {
    "inferred_metrics": [
        {
            "metric_name": "受伤人数",
            "aggregation": "COUNT_DISTINCT",
            "input_column": "crash_person_id",
            "alias": "injured_person_count",
            "filter": {
                "column": "person_injury",
                "operator": "eq",
                "value": "Injured",
            },
            "confidence": "high",
        },
        {
            "metric_name": "涉及人数",
            "aggregation": "COUNT_DISTINCT",
            "input_column": "crash_person_id",
            "alias": "involved_person_count",
            "filter": None,
            "confidence": "high",
        },
    ],
    "inferred_window_metrics": [],
    "inferred_computed_metrics": [],
    "ratio_proposals": [
        {
            "output_alias": "injury_rate",
            "numerator_alias": "injured_person_count",
            "denominator_alias": "involved_person_count",
            "zero_division": "NULL",
            "multiplier": 1,
            "confidence": "high",
            "reasoning": "业务定义明确要求聚合后相除",
        },
    ],
    "inferred_dimensions": [
        {
            "dimension_name": "borough",
            "source_column": "borough",
        },
    ],
    "inferred_post_window_filters": [],
}


_CASE03_INFERENCE = {
    "inferred_metrics": [
        {
            "metric_name": "违章数",
            "aggregation": "COUNT_DISTINCT",
            "input_column": "violation_id",
            "alias": "violation_count",
            "filter": None,
            "confidence": "high",
        },
        {
            "metric_name": "有定价违章数",
            "aggregation": "COUNT",
            "input_column": "standard_fine_amount",
            "alias": "priced_violation_count",
            "filter": None,
            "confidence": "high",
        },
        {
            "metric_name": "罚款总额",
            "aggregation": "SUM",
            "input_column": "standard_fine_amount",
            "alias": "total_fine_amount",
            "filter": None,
            "confidence": "high",
        },
        {
            "metric_name": "外州违章数",
            "aggregation": "COUNT",
            "input_column": "violation_id",
            "alias": "out_of_state_count",
            "filter": {
                "column": "registration_state",
                "operator": "neq",
                "value": "NY",
            },
            "confidence": "high",
        },
        {
            "metric_name": "重复罚单数",
            "aggregation": "COUNT",
            "input_column": "violation_id",
            "alias": "duplicate_count",
            "filter": {
                "column": "is_duplicate_summons",
                "operator": "eq",
                "value": "true",
            },
            "confidence": "high",
        },
    ],
    "inferred_window_metrics": [
        {
            "metric_name": "辖区违章总量",
            "window_function": "SUM",
            "input_column": "violation_count",
            "partition_by": ["violation_county"],
            "order_by": [],
            "alias": "county_violation_count",
            "confidence": "high",
        },
    ],
    "inferred_computed_metrics": [],
    "ratio_proposals": [
        {
            "output_alias": "avg_fine_amount",
            "numerator_alias": "total_fine_amount",
            "denominator_alias": "priced_violation_count",
            "zero_division": "NULL",
            "multiplier": 1,
            "confidence": "high",
            "reasoning": "聚合后平均罚款",
        },
        {
            "output_alias": "out_of_state_ratio",
            "numerator_alias": "out_of_state_count",
            "denominator_alias": "violation_count",
            "zero_division": "NULL",
            "multiplier": 1,
            "confidence": "high",
            "reasoning": "聚合后外州占比",
        },
        {
            "output_alias": "duplicate_ratio",
            "numerator_alias": "duplicate_count",
            "denominator_alias": "violation_count",
            "zero_division": "NULL",
            "multiplier": 1,
            "confidence": "high",
            "reasoning": "聚合后重复罚单占比",
        },
        {
            "output_alias": "county_share_pct",
            "numerator_alias": "violation_count",
            "denominator_alias": "county_violation_count",
            "zero_division": "NULL",
            "multiplier": 100,
            "confidence": "high",
            "reasoning": "辖区窗口总量中的百分比",
        },
    ],
    "inferred_dimensions": [
        {
            "dimension_name": "violation_county",
            "source_column": "violation_county",
        },
        {
            "dimension_name": "violation_code",
            "source_column": "violation_code",
        },
    ],
    "inferred_post_window_filters": [],
}


_CASE02_SPEC = """```markdown
---
spec:
  type: aggregate_table
  target_grain: [borough]
  summary: "按 borough 统计事故人员并计算 injury_rate。"
  source_tables:
    - name: silver.crash_person
      alias: cp
      columns:
        - {name: borough, type: varchar}
        - {name: crash_person_id, type: bigint}
        - {name: person_injury, type: varchar}
  output_columns:
    - {name: borough, type: varchar}
    - {name: injured_person_count, type: bigint}
    - {name: involved_person_count, type: bigint}
    - {name: injury_rate, type: double}
---
# Case02 事故人员伤害率

按 borough 聚合。injured_person_count 是受伤人员去重数，
involved_person_count 是全部涉及人员去重数。
injury_rate 在聚合后计算 injured_person_count / involved_person_count；
分母为 0 时返回 NULL。
```"""


_CASE03_SPEC = """```markdown
---
spec:
  type: aggregate_table
  target_grain: [violation_county, violation_code]
  summary: "按辖区和违章代码分析执法效能，输出 county_share_pct 等四个比率。"
  source_tables:
    - name: gold.fact_parking_violations
      alias: fv
      columns:
        - {name: violation_id, type: bigint}
        - {name: violation_county, type: varchar}
        - {name: violation_code, type: integer}
        - {name: standard_fine_amount, type: double}
        - {name: registration_state, type: varchar}
        - {name: is_duplicate_summons, type: boolean}
  output_columns:
    - {name: violation_county, type: varchar}
    - {name: violation_code, type: integer}
    - {name: violation_count, type: bigint}
    - {name: priced_violation_count, type: bigint}
    - {name: total_fine_amount, type: double}
    - {name: out_of_state_count, type: bigint}
    - {name: duplicate_count, type: bigint}
    - {name: avg_fine_amount, type: double}
    - {name: out_of_state_ratio, type: double}
    - {name: duplicate_ratio, type: double}
    - {name: county_share_pct, type: double}
---
# Case03 停车违章执法效能

所有比率都在基础指标聚合完成后计算并做除零保护：
avg_fine_amount = total_fine_amount / priced_violation_count；
out_of_state_ratio = out_of_state_count / violation_count；
duplicate_ratio = duplicate_count / violation_count。
county_share_pct 先在 violation_county 内计算 violation_count 的窗口总和，
再计算 violation_count / county_violation_count * 100。
所有分母为 0 时均返回 NULL。
```"""


def _make_pipeline() -> Pipeline:
    return Pipeline(
        adapter=_RatioInferenceAdapter(),
        requirement_planner=_EmptyRequirementPlanner(),
    )


def _ratio_columns(pipeline: Pipeline, request_id: str) -> dict[str, RatioExpr]:
    plan = pipeline._results[request_id]["plan"]
    project = next(step for step in plan.steps if isinstance(step, ProjectStep))
    return {
        str(column.alias): column.expression
        for column in project.columns
        if isinstance(column.expression, RatioExpr)
    }


def test_case02_natural_language_injury_rate_passes_pipeline() -> None:
    """Case02 无需手写结构化比率即可通过 Pipeline。"""
    pipeline = _make_pipeline()

    result = pipeline.build_plan(_CASE02_SPEC)

    assert "pipeline_error" not in result
    assert result["validation_passed"] is True
    ratios = _ratio_columns(pipeline, result["request_id"])
    assert set(ratios) == {"injury_rate"}
    assert ratios["injury_rate"].denominator_alias == "involved_person_count"


def test_case03_four_natural_language_ratios_pass_api() -> None:
    """Case03 四个聚合后比率经 HTTP API 进入同一确定性构建链路。"""
    pipeline = _make_pipeline()
    client = TestClient(create_app(pipeline=pipeline))

    response = client.post("/api/plan", json={"markdown_text": _CASE03_SPEC})

    assert response.status_code == 200
    result = response.json()
    assert "pipeline_error" not in result
    assert result["validation_passed"] is True
    ratios = _ratio_columns(pipeline, result["request_id"])
    assert set(ratios) == {
        "avg_fine_amount",
        "out_of_state_ratio",
        "duplicate_ratio",
        "county_share_pct",
    }
    assert ratios["county_share_pct"].multiplier == 100

    # 执行生成 SQL，保护“聚合 → 窗口总量 → 比率”的固定三层结构。
    import duckdb

    connection = duckdb.connect(":memory:")
    connection.execute(
        """
        CREATE TABLE parking (
            violation_id BIGINT,
            violation_county VARCHAR,
            violation_code INTEGER,
            standard_fine_amount DOUBLE,
            registration_state VARCHAR,
            is_duplicate_summons BOOLEAN
        )
        """
    )
    connection.execute(
        """
        INSERT INTO parking VALUES
          (1, 'NY', 10, 50, 'NY', false),
          (2, 'NY', 10, 70, 'NJ', true),
          (3, 'NY', 20, NULL, 'NY', false)
        """
    )
    plan = pipeline._results[result["request_id"]]["plan"]
    sql = DuckDbSqlCompiler({"fv": "parking"}).compile(plan).sql
    rows = connection.execute(sql).fetchall()
    connection.close()

    assert len(rows) == 2
    assert round(sum(row[-1] for row in rows), 8) == 100

    contract = DataTransformContractExtractor().extract(
        plan,
        output_grain=["violation_county", "violation_code"],
    )
    mapping = map_contract_to_spark_plan(adapt_lite_to_v1(contract))
    assert mapping.success and mapping.spark_plan is not None

    spark_code = SparkCompiler().compile(mapping.spark_plan).raw_pyspark
    assert "Window.unboundedFollowing" in spark_code

    comparison = PlanComparator().compare(plan, mapping.spark_plan)
    assert comparison.status == ComparisonStatus.LOGIC_EQUIVALENT, [
        (step.step_type, step.verdict, step.detail)
        for step in comparison.step_results
    ]
