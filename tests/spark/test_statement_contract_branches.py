"""逐语句 Contract 到 Spark branches 的回归测试。"""

import ast
import hashlib
from pathlib import Path

import pytest

from tianshu_datadev.artifacts.contract_extractor import (
    DataTransformContractExtractor,
)
from tianshu_datadev.artifacts.models import (
    ContractAggregation,
    ContractInputTable,
    ContractJoin,
    ContractOutputColumn,
    DataTransformContractV1,
    StatementTransformContract,
)
from tianshu_datadev.developer_spec.parser import DeveloperSpecParser
from tianshu_datadev.planning.program_factory import (
    build_sql_program_from_compute_steps,
)
from tianshu_datadev.planning.sql_build_plan import SqlBuildPlanBuilder
from tianshu_datadev.spark.annotations import StepAnnotation, StepIntent
from tianshu_datadev.spark.compiler import SparkCompiler
from tianshu_datadev.spark.mapper import map_contract_to_spark_plan
from tianshu_datadev.spark.models import (
    SparkJoinStep,
    SparkJoinType,
    SparkPlan,
    SparkProjectColumn,
    SparkProjectStep,
    SparkReadStep,
)
from tianshu_datadev.spark.step_ids import iter_plan_steps
from tianshu_datadev.spark.validator import SparkStaticValidator


def _branch_contract() -> DataTransformContractV1:
    left_temp = "_temp_left_stats"
    right_temp = "_temp_right_stats"
    left = StatementTransformContract(
        statement_id="left_stmt",
        produces=left_temp,
        input_tables=[
            ContractInputTable(table_ref="a", source_table="fact_a")
        ],
        aggregations=[
            ContractAggregation(
                function="COUNT",
                input_column="id",
                alias="left_count",
            )
        ],
        grouping_keys=["group_key"],
        output_columns=[
            ContractOutputColumn(
                column_name="group_key",
                alias="group_key",
            ),
            ContractOutputColumn(
                column_name="left_count",
                alias="left_count",
            ),
        ],
    )
    right = StatementTransformContract(
        statement_id="right_stmt",
        produces=right_temp,
        input_tables=[
            ContractInputTable(table_ref="b", source_table="fact_b")
        ],
        aggregations=[
            ContractAggregation(
                function="SUM",
                input_column="amount",
                alias="right_amount",
            )
        ],
        grouping_keys=["group_key"],
        output_columns=[
            ContractOutputColumn(
                column_name="group_key",
                alias="group_key",
            ),
            ContractOutputColumn(
                column_name="right_amount",
                alias="right_amount",
            ),
        ],
    )
    final = StatementTransformContract(
        statement_id="final_stmt",
        depends_on=["left_stmt", "right_stmt"],
        input_temp_tables=[left_temp, right_temp],
        join_relationships=[
            ContractJoin(
                join_id="join_branches",
                left_table=left_temp,
                right_table=right_temp,
                left_key="group_key",
                right_key="group_key",
                join_type="INNER",
                level="STRONG",
            )
        ],
        output_columns=[
            ContractOutputColumn(
                column_name="group_key",
                alias="group_key",
            ),
            ContractOutputColumn(
                column_name="left_count",
                alias="left_count",
            ),
            ContractOutputColumn(
                column_name="right_amount",
                alias="right_amount",
            ),
        ],
    )
    return DataTransformContractV1(
        contract_id="contract_branches",
        source_sqlprogram_hash="program_branches",
        input_tables=[
            ContractInputTable(table_ref="a", source_table="fact_a"),
            ContractInputTable(table_ref="b", source_table="fact_b"),
        ],
        output_columns=final.output_columns,
        step_dag={
            "left_stmt": [],
            "right_stmt": [],
            "final_stmt": ["left_stmt", "right_stmt"],
        },
        statement_contracts=[left, right, final],
    )


def test_two_statement_outputs_compile_as_independent_branches():
    mapping = map_contract_to_spark_plan(_branch_contract())

    assert mapping.success is True
    assert mapping.spark_plan is not None
    assert list(mapping.spark_plan.branches) == [
        "_temp_left_stats",
        "_temp_right_stats",
    ]

    result = SparkCompiler().compile(mapping.spark_plan)

    tree = ast.parse(result.raw_pyspark)
    functions = [
        node for node in tree.body if isinstance(node, ast.FunctionDef)
    ]
    assert [node.name for node in functions] == [
        "_transform_s1",
        "_transform_s2",
        "_transform_s3",
        "transform",
    ]
    for function in functions:
        assigned = {
            target.id
            for statement in function.body
            if isinstance(statement, ast.Assign)
            for target in statement.targets
            if isinstance(target, ast.Name)
        }
        expected_prefixes = {"s"} if function.name == "transform" else {"t", "f"}
        assert all(
            variable[0] in expected_prefixes and variable[1:].isdigit()
            for variable in assigned
        )
    assert "f1 = s1.join(s2" in result.raw_pyspark
    assert "s1 = _transform_s1(inputs, params=params)" in result.raw_pyspark
    assert "s2 = _transform_s2(inputs, params=params)" in result.raw_pyspark
    assert (
        "s3 = _transform_s3(inputs, s1, s2, params=params)"
        in result.raw_pyspark
    )
    assert 'inputs["fact_a"]' in result.raw_pyspark
    assert 'inputs["fact_b"]' in result.raw_pyspark
    assert "_br_" not in result.raw_pyspark
    assert "_temp_" not in result.raw_pyspark
    assert "# Step: SparkReadStep_s1_0（索引 1/3）" in result.annotated_pyspark
    assert "# 业务阶段 s1" in result.annotated_pyspark
    assert "# 按 group_key 聚合 left_count -> f1" in result.annotated_pyspark
    assert "left_alias=''" not in result.raw_pyspark
    assert SparkStaticValidator().validate(result.raw_pyspark).is_valid
    repeated = SparkCompiler().compile(mapping.spark_plan)
    assert repeated.raw_hash == result.raw_hash
    assert repeated.raw_pyspark == result.raw_pyspark


def test_pipeline_separates_pure_transform_from_runtime_runner():
    from tianshu_datadev.api.pipeline import Pipeline, SparkStageContext

    mapping = map_contract_to_spark_plan(_branch_contract())
    context = SparkStageContext(spark_plan=mapping.spark_plan)

    Pipeline()._do_spark_compile(context)

    runner = context.standalone_pyspark
    assert runner is not None
    assert "from transform import transform" in runner
    assert '"fact_a": "data/fact_a.csv"' in runner
    assert '"fact_b": "data/fact_b.csv"' in runner
    assert '.master("local[*]")' in runner
    assert ".count()" not in runner
    assert "result.limit(20).show(truncate=False)" in runner
    code = context.compile_result.annotated_pyspark
    assert "SparkSession" not in code
    assert "spark.read" not in code
    assert ".write." not in code
    assert ".count()" not in code
    assert ".show(" not in code


def test_multistage_compiler_uses_llm_business_descriptions_for_branches():
    mapping = map_contract_to_spark_plan(_branch_contract())
    assert mapping.spark_plan is not None
    annotations = [
        StepAnnotation(
            step_id=step_id,
            step_index=global_index,
            step_type=step.step_type.value,
            intent=StepIntent.SHAPE,
            intent_detail=f"业务描述：{stage_label} 阶段第 {step_index + 1} 步",
            operation_summary="测试",
        )
        for global_index, (step_id, stage_label, step_index, step) in enumerate(
            iter_plan_steps(mapping.spark_plan)
        )
    ]

    result = SparkCompiler().compile(
        mapping.spark_plan,
        annotations=annotations,
    )

    assert "# 业务描述：s1 阶段第 1 步" in result.annotated_pyspark
    assert "# 业务描述：s2 阶段第 1 步" in result.annotated_pyspark
    assert "# 业务描述：s3 阶段第 1 步" in result.annotated_pyspark


def test_sequential_branch_dependencies_use_stage_parameters():
    plan = SparkPlan(
        plan_id="sequential_branches",
        source_contract_hash="contract_hash",
        branches={
            "_temp_first": [
                SparkReadStep(
                    alias="a",
                    source_name="fact_a",
                    input_key="a",
                ),
                SparkProjectStep(
                    input_alias="a",
                    columns=[
                        SparkProjectColumn(
                            column_name="group_key",
                            alias="group_key",
                        ),
                    ],
                ),
            ],
            "_temp_second": [
                SparkReadStep(
                    alias="b",
                    source_name="fact_b",
                    input_key="b",
                ),
                SparkJoinStep(
                    left_alias="_temp_first",
                    right_alias="b",
                    left_key="group_key",
                    right_key="group_key",
                    join_type=SparkJoinType.INNER,
                ),
            ],
        },
        steps=[
            SparkProjectStep(
                input_alias="_temp_second",
                columns=[
                    SparkProjectColumn(
                        column_name="group_key",
                        alias="group_key",
                    ),
                ],
            ),
        ],
    )

    result = SparkCompiler().compile(plan)

    assert "def _transform_s2(" in result.raw_pyspark
    assert "    s1: DataFrame," in result.raw_pyspark
    assert "f1 = s1.join(t1" in result.raw_pyspark
    assert "s2 = _transform_s2(inputs, s1, params=params)" in result.raw_pyspark
    assert "s3 = _transform_s3(inputs, s2, params=params)" in result.raw_pyspark
    assert "_temp_" not in result.raw_pyspark


def test_extractor_preserves_each_sql_statement_boundary():
    fixture = (
        Path(__file__).resolve().parents[1]
        / "fixtures"
        / "case04_borough_crash_risk.md"
    )
    spec = DeveloperSpecParser().parse(fixture.read_text(encoding="utf-8"))
    plans = SqlBuildPlanBuilder().build_from_steps(spec)
    chain_id = hashlib.md5(
        "|".join(step.step_name for step in spec.compute_steps).encode()
    ).hexdigest()[:8]
    program = build_sql_program_from_compute_steps(
        plans,
        spec,
        chain_id,
    )

    contract = DataTransformContractExtractor().extract_v1(program)

    assert len(contract.statement_contracts) == 3
    assert contract.statement_contracts[0].produces is not None
    assert contract.statement_contracts[1].produces is not None
    assert contract.statement_contracts[2].produces is None
    assert len(contract.statement_contracts[2].input_temp_tables) == 2


def test_controlled_expression_parsers_reject_function_calls():
    extractor = DataTransformContractExtractor()

    arithmetic = extractor._parse_arithmetic_expression(
        "(total_injured + total_killed * 10.0) / crash_count"
    )
    guarded_ratio = extractor._parse_arithmetic_expression(
        "total_crashes / NULLIF(total_trip_count, 0)"
    )
    condition = extractor._parse_case_when_boolean_expression(
        "severity_score >= 2.0 AND total_trips >= 10000"
    )

    assert arithmetic.kind == "binary"
    assert arithmetic.operator == "DIVIDE"
    assert guarded_ratio.right is not None
    assert guarded_ratio.right.kind == "null_if_zero"
    assert condition.operator == "AND"
    with pytest.raises(ValueError):
        extractor._parse_arithmetic_expression("__import__('os')")
    with pytest.raises(ValueError):
        extractor._parse_case_when_boolean_expression("danger(x) > 0")
