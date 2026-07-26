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
from tianshu_datadev.spark.compiler import SparkCompiler
from tianshu_datadev.spark.mapper import map_contract_to_spark_plan


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

    ast.parse(result.raw_pyspark)
    assert "_temp_left_stats.join(_temp_right_stats" in result.raw_pyspark
    assert 'inputs["fact_a"]' in result.raw_pyspark
    assert 'inputs["fact_b"]' in result.raw_pyspark
    assert "left_alias=''" not in result.raw_pyspark


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
