"""Spark 本地运行入口的确定性代码生成。

transform.py 保持纯转换边界；本模块仅生成便于个人开发调试的 spark_job.py。
"""

from __future__ import annotations

from tianshu_datadev.spark.models import SparkPlan, SparkReadStep


def collect_input_names(plan: SparkPlan) -> list[str]:
    """从主步骤和全部分支中按首次出现顺序收集 Contract 输入名。"""
    all_steps = [
        step
        for branch_steps in plan.branches.values()
        for step in branch_steps
    ]
    all_steps.extend(plan.steps)
    return list(dict.fromkeys(
        step.source_name
        for step in all_steps
        if isinstance(step, SparkReadStep)
    ))


def render_spark_job(input_names: list[str]) -> str:
    """生成结构清晰、可直接修改数据路径的本地 Spark 运行脚本。

    本地入口允许创建 SparkSession 和读取文件，但不进入 Validator 或物理验证路径。
    预览固定限制为 20 行，避免生成全量 count Action。
    """
    ordered_names = list(dict.fromkeys(input_names))
    input_lines = "\n".join(
        f'    "{name}": "data/{name}.csv",'
        for name in ordered_names
    )

    return f'''"""由 TianShu 确定性生成的 Spark 本地运行入口。

transform.py 必须与本文件位于同一目录。
运行前请根据实际情况修改 INPUT_PATHS。
"""

from pyspark.sql import SparkSession

from transform import transform


INPUT_PATHS = {{
{input_lines}
}}


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("tianshu_datadev")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

    try:
        # ======================
        # 1. 加载数据
        # ======================
        inputs = {{
            name: spark.read.csv(path, header=True, inferSchema=True)
            for name, path in INPUT_PATHS.items()
        }}

        # ======================
        # 2. 执行转换
        # ======================
        result = transform(inputs, params=None)

        # ======================
        # 3. 输出结果
        # ======================
        print("=== 结果概要 ===")
        result.printSchema()
        result.limit(20).show(truncate=False)
        print("=== 执行完毕 ===")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
'''
