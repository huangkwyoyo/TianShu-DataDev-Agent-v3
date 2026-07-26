"""Spark 本地运行入口确定性生成测试。"""

import ast

from tianshu_datadev.spark.models import SparkPlan, SparkProjectColumn, SparkProjectStep, SparkReadStep
from tianshu_datadev.spark.runner import collect_input_names, render_spark_job


def test_runner_is_deterministic_and_uses_contract_inputs():
    first = render_spark_job(["td", "tz", "td", "cd"])
    second = render_spark_job(["td", "tz", "td", "cd"])

    assert first == second
    assert '"td": "data/td.csv"' in first
    assert '"tz": "data/tz.csv"' in first
    assert '"cd": "data/cd.csv"' in first
    assert first.count('"td": "data/td.csv"') == 1
    assert "from transform import transform" in first
    assert 'if __name__ == "__main__":' in first
    ast.parse(first)


def test_runner_is_readable_local_preview_without_full_count():
    code = render_spark_job(["ft", "tz"])

    assert "# 1. 加载数据" in code
    assert "# 2. 执行转换" in code
    assert "# 3. 输出结果" in code
    assert '.master("local[*]")' in code
    assert '.config("spark.sql.shuffle.partitions", "4")' in code
    assert "spark.read.csv(path, header=True, inferSchema=True)" in code
    assert "result.printSchema()" in code
    assert "result.limit(20).show(truncate=False)" in code
    assert ".count()" not in code
    assert '"ft": "data/ft.csv"' in code
    assert '"tz": "data/tz.csv"' in code


def test_collect_input_names_includes_multistage_branches():
    plan = SparkPlan(
        plan_id="runner-branches",
        source_contract_hash="contract",
        branches={
            "_temp_s1": [
                SparkReadStep(alias="td", source_name="td", input_key="td"),
                SparkReadStep(alias="tz", source_name="tz", input_key="tz"),
            ],
        },
        steps=[
            SparkReadStep(alias="cd", source_name="cd", input_key="cd"),
            SparkProjectStep(
                input_alias="cd",
                columns=[SparkProjectColumn(column_name="id", alias="id")],
            ),
        ],
    )

    assert collect_input_names(plan) == ["td", "tz", "cd"]
