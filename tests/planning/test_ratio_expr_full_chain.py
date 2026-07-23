"""RatioExpr 窄范围全链路回归。"""

import pytest

from tianshu_datadev.artifacts.contract_extractor import (
    DataTransformContractExtractor,
)
from tianshu_datadev.developer_spec.models import (
    AggregationType,
    ColumnDecl,
    InputTableDecl,
    ManifestColumn,
    ManifestTable,
    MetricDecl,
    OutputColumnDecl,
    OutputSpecDecl,
    ParsedDeveloperSpec,
    RatioDecl,
    RatioProposal,
    SourceManifest,
)
from tianshu_datadev.planning.models import RatioExpr
from tianshu_datadev.planning.proposal_validator import RatioProposalValidator
from tianshu_datadev.planning.sql_build_plan import (
    ProjectStep,
    SqlBuildPlanBuilder,
)
from tianshu_datadev.spark.compiler import SparkCompiler
from tianshu_datadev.spark.contract_adapter import adapt_lite_to_v1
from tianshu_datadev.spark.mapper import map_contract_to_spark_plan
from tianshu_datadev.spark.plan_comparator import (
    ComparisonStatus,
    PlanComparator,
)
from tianshu_datadev.sql.compiler import DuckDbSqlCompiler
from tianshu_datadev.sql.validator import SqlBuildPlanValidator


def test_ratio_expr_sql_contract_spark_comparator_full_chain() -> None:
    """Case02 injury_rate 通过类型化链路，且 SQL/Spark 结构等价。"""
    table = InputTableDecl(
        table_alias="cp",
        source_table="silver_crash_person",
        columns=[
            ColumnDecl(
                column_name="borough",
                normalized_name="borough",
                data_type="varchar",
            ),
            ColumnDecl(
                column_name="crash_person_id",
                normalized_name="crash_person_id",
                data_type="bigint",
            ),
        ],
    )
    spec = ParsedDeveloperSpec(
        spec_id="spec_ratio_case02",
        spec_hash="a" * 64,
        title="Case02 事故伤害率",
        description="按 borough 计算受伤人数除以涉及人数，分母为零返回 NULL。",
        input_tables=[table],
        metrics=[
            MetricDecl(
                metric_name="受伤人数",
                aggregation=AggregationType.COUNT_DISTINCT,
                input_column="crash_person_id",
                alias="injured_person_count",
            ),
            MetricDecl(
                metric_name="涉及人数",
                aggregation=AggregationType.COUNT_DISTINCT,
                input_column="crash_person_id",
                alias="involved_person_count",
            ),
        ],
        dimensions=[],
        ratio_metrics=[
            RatioDecl(
                output_alias="injury_rate",
                numerator_alias="injured_person_count",
                denominator_alias="involved_person_count",
            ),
        ],
        output_spec=OutputSpecDecl(
            columns=[
                OutputColumnDecl(name="borough"),
                OutputColumnDecl(name="injured_person_count"),
                OutputColumnDecl(name="involved_person_count"),
                OutputColumnDecl(name="injury_rate"),
            ],
            grain=["borough"],
        ),
    )
    manifest = SourceManifest(
        manifest_id="manifest_ratio_case02",
        spec_hash=spec.spec_hash,
        tables=[
            ManifestTable(
                table_ref="cp",
                source_table="silver_crash_person",
                columns=[
                    ManifestColumn(
                        column_name="borough",
                        normalized_name="borough",
                        data_type="varchar",
                    ),
                    ManifestColumn(
                        column_name="crash_person_id",
                        normalized_name="crash_person_id",
                        data_type="bigint",
                    ),
                ],
            ),
        ],
    )

    plan, _ = SqlBuildPlanBuilder().build(spec)
    project = next(step for step in plan.steps if isinstance(step, ProjectStep))
    ratio_column = next(
        column for column in project.columns if column.alias == "injury_rate"
    )
    assert isinstance(ratio_column.expression, RatioExpr)

    passed, questions = SqlBuildPlanValidator().validate(plan, manifest)
    assert passed, [question.description for question in questions]

    sql = DuckDbSqlCompiler(
        table_mapping={"cp": "silver_crash_person"},
    ).compile(plan).sql
    assert "FROM (" in sql
    assert "_ratio_base.involved_person_count = 0" in sql
    assert "END AS injury_rate" in sql

    lite = DataTransformContractExtractor().extract(
        plan, output_grain=["borough"],
    )
    assert lite.ratio_specs["injury_rate"].numerator_alias == (
        "injured_person_count"
    )

    mapping = map_contract_to_spark_plan(adapt_lite_to_v1(lite))
    assert mapping.success and mapping.spark_plan is not None
    spark_code = SparkCompiler().compile(mapping.spark_plan).raw_pyspark
    assert 'F.col("involved_person_count").isNull()' in spark_code
    assert '.alias("injury_rate")' in spark_code

    comparison = PlanComparator().compare(plan, mapping.spark_plan)
    assert comparison.status == ComparisonStatus.LOGIC_EQUIVALENT, [
        (result.step_type, result.verdict, result.detail)
        for result in comparison.step_results
    ]


@pytest.mark.parametrize(
    ("denominator_alias", "expected_code"),
    [
        ("missing_metric", "RATIO-DEPENDENCY"),
        ("text_metric", "RATIO-DENOMINATOR_TYPE"),
    ],
)
def test_ratio_validator_rejects_unknown_or_non_numeric_denominator(
    denominator_alias: str,
    expected_code: str,
) -> None:
    """分母必须是已定义的聚合后数值输出。"""
    table = InputTableDecl(
        table_alias="t",
        source_table="source_table",
        columns=[
            ColumnDecl(
                column_name="row_id",
                normalized_name="row_id",
                data_type="bigint",
            ),
            ColumnDecl(
                column_name="label",
                normalized_name="label",
                data_type="varchar",
            ),
        ],
    )
    spec = ParsedDeveloperSpec(
        spec_id="spec_ratio_validator",
        spec_hash="b" * 64,
        title="比率校验",
        description="验证分母类型。",
        input_tables=[table],
        metrics=[
            MetricDecl(
                metric_name="行数",
                aggregation=AggregationType.COUNT,
                input_column="row_id",
                alias="row_count",
            ),
            MetricDecl(
                metric_name="文本最大值",
                aggregation=AggregationType.MAX,
                input_column="label",
                alias="text_metric",
            ),
        ],
        dimensions=[],
        output_spec=OutputSpecDecl(
            columns=[OutputColumnDecl(name="invalid_ratio")],
            grain=[],
        ),
    )
    manifest = SourceManifest(
        manifest_id="manifest_ratio_validator",
        spec_hash=spec.spec_hash,
        tables=[
            ManifestTable(
                table_ref="t",
                source_table="source_table",
                columns=[
                    ManifestColumn(
                        column_name="row_id",
                        normalized_name="row_id",
                        data_type="bigint",
                    ),
                    ManifestColumn(
                        column_name="label",
                        normalized_name="label",
                        data_type="varchar",
                    ),
                ],
            ),
        ],
    )
    proposal = RatioProposal(
        output_alias="invalid_ratio",
        numerator_alias="row_count",
        denominator_alias=denominator_alias,
        confidence="high",
    )

    valid, questions = RatioProposalValidator().validate(
        proposal,
        spec,
        manifest,
    )

    assert valid is False
    assert any(
        question.question_id.startswith(expected_code)
        for question in questions
    )
