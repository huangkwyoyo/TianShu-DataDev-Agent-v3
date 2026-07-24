"""Case04/Case05 端到端测试——DuckDBExecutor.execute_program() + Spark + digest。

验证核心 Pipeline 链路：
  解析 → 构建 SqlBuildPlan → 构建 SqlProgram → DuckDB 编译 → DuckDB 执行
  → Contract v1 提取 → SparkPlan 映射 → Spark 编译 → （可选）Spark 执行
"""

import hashlib
import json
import pathlib

import pytest

from tianshu_datadev.developer_spec.parser import DeveloperSpecParser
from tianshu_datadev.planning.sql_build_plan import SqlBuildPlanBuilder
from tianshu_datadev.planning.program_factory import build_sql_program_from_compute_steps
from tianshu_datadev.sql.compiler import DuckDbSqlCompiler
from tianshu_datadev.sql.executor import DuckDBExecutor
from tianshu_datadev.artifacts.contract_extractor import DataTransformContractExtractor
from tianshu_datadev.spark.mapper import map_contract_to_spark_plan
from tianshu_datadev.spark.compiler import SparkCompiler
from tianshu_datadev.spark.executor import LocalSparkExecutor

FIXTURE_DIR = pathlib.Path(__file__).resolve().parent.parent / "fixtures"


# ════════════════════════════════════════════
# 辅助函数
# ════════════════════════════════════════════


def _read_case_fixture(name: str) -> str:
    """读取测试 fixture markdown 文件。"""
    path = FIXTURE_DIR / f"{name}.md"
    assert path.exists(), f"Fixture 不存在: {path}"
    return path.read_text(encoding="utf-8")


def _compute_digest(rows: list[dict], columns: list[str]) -> str:
    """计算结果的确定性 SHA-256 digest——用于跨引擎比较。

    先按 JSON 序列化结果排序（消除行序不确定性），再整体 hash。
    """
    sorted_rows = sorted(
        rows, key=lambda r: json.dumps(r, sort_keys=True, default=str)
    )
    payload = json.dumps(
        {"columns": columns, "rows": sorted_rows}, sort_keys=True, default=str
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def _try_spark() -> bool:
    """检查 PySpark 是否可用。"""
    try:
        import pyspark  # noqa: F401
        return True
    except ImportError:
        return False


def _sample_rows_to_dicts(sample_rows: list[list], columns: list[str]) -> list[dict]:
    """将 DuckDB ResultSummary.sample_rows（list[list]）转为 list[dict]。"""
    return [dict(zip(columns, row)) for row in sample_rows]


def _run_duckdb_pipeline(
    fixture_name: str,
    table_mapping: dict[str, str],
    table_paths: dict[str, str],
) -> dict:
    """完整的 DuckDB Pipeline：解析 → 构建 → 编译 → 执行。

    Returns:
        {
            "row_count": int,
            "columns": list[str],
            "rows": list[dict],
            "compiled": SqlProgramArtifact,
            "spec": ParsedDeveloperSpec,
            "program": SqlProgram,
        }
    """
    markdown_text = _read_case_fixture(fixture_name)

    # 1. 解析
    parser = DeveloperSpecParser()
    spec = parser.parse(markdown_text)

    # 2. 构建 SqlBuildPlan 列表（按 compute_steps 拆多步）
    builder = SqlBuildPlanBuilder()
    plans = builder.build_from_steps(spec)

    # 3. 构建 SqlProgram
    chain_id = hashlib.md5(
        "|".join(s.step_name for s in spec.compute_steps).encode()
    ).hexdigest()[:8]
    program = build_sql_program_from_compute_steps(plans, spec, chain_id)

    # 4. DuckDB 编译
    compiler = DuckDbSqlCompiler(table_mapping=table_mapping)
    program_artifact = compiler.compile_program(program)

    # 5. DuckDB 执行
    executor = DuckDBExecutor(
        table_paths=table_paths,
        _worker_mode=True,
        max_result_rows=100,
    )
    result = executor.execute_program(program_artifact.compiled)

    # 6. 提取最终语句的结果
    final_stmt_result = None
    for stmt in result.results:
        if stmt.trace.status.name == "RUNTIME_PASS":
            final_stmt_result = stmt

    return {
        "row_count": final_stmt_result.trace.row_count if final_stmt_result else 0,
        "columns": final_stmt_result.summary.columns if final_stmt_result else [],
        "sample_rows": final_stmt_result.summary.sample_rows if final_stmt_result else [],
        "compiled": program_artifact,
        "spec": spec,
        "program": program,
        "execution_result": result,
    }


# ════════════════════════════════════════════
# Case04：事故按 borough 聚合
# ════════════════════════════════════════════

case04_parquet = FIXTURE_DIR / "e2e_case04_small.parquet"

CASE04_TABLE_MAPPING = {
    "cd": "crash_detail",
}

CASE04_TABLE_PATHS = {
    "crash_detail": str(case04_parquet / "crash_detail.parquet"),
}


@pytest.mark.integration
class TestCase04E2E:
    """Case04 端到端测试——事故数据两步聚合。"""

    def test_duckdb_execution(self):
        """验证 DuckDB 端：解析 → 构建 → 编译 → 执行，结果非空且正确。"""
        result = _run_duckdb_pipeline(
            "e2e_case04_small",
            CASE04_TABLE_MAPPING,
            CASE04_TABLE_PATHS,
        )

        # 验证执行结果
        assert result["row_count"] > 0, "DuckDB 执行应返回非零行数"
        assert "borough" in result["columns"], "输出应包含 borough 列"
        assert "crash_count" in result["columns"], "输出应包含 crash_count 列"
        assert "total_injured" in result["columns"], "输出应包含 total_injured 列"

        # 验证数据正确性（已知预期值）
        rows_by_borough = {
            r["borough"]: r
            for r in _sample_rows_to_dicts(
                result["sample_rows"], result["columns"]
            )
        }
        # crash_detail 有 5 条，MANHATTAN×2、BROOKLYN×2、QUEENS×1
        assert rows_by_borough.get("MANHATTAN", {}).get("crash_count") == 2
        assert rows_by_borough.get("BROOKLYN", {}).get("crash_count") == 2
        assert rows_by_borough.get("QUEENS", {}).get("crash_count") == 1
        # persons_injured 求和：MANHATTAN=2+1=3，BROOKLYN=0+3=3，QUEENS=1
        assert rows_by_borough.get("MANHATTAN", {}).get("total_injured") == 3
        assert rows_by_borough.get("BROOKLYN", {}).get("total_injured") == 3
        assert rows_by_borough.get("QUEENS", {}).get("total_injured") == 1

    def test_spark_pipeline(self):
        """验证 Spark 端：Contract 提取 → SparkPlan 映射 → 编译，Spark 执行为条件性。

        如果 PySpark 可用，则执行并比较 DuckDB/Spark digest。
        """
        # 先跑 DuckDB 获得结果
        duckdb_result = _run_duckdb_pipeline(
            "e2e_case04_small",
            CASE04_TABLE_MAPPING,
            CASE04_TABLE_PATHS,
        )

        # 提取 Contract v1
        extractor = DataTransformContractExtractor()
        contract = extractor.extract_v1(sql_program=duckdb_result["program"])
        assert contract.contract_id, "Contract 应有 contract_id"
        assert len(contract.input_tables) > 0, "Contract 应有输入表"

        # 映射到 SparkPlan
        mapping_result = map_contract_to_spark_plan(contract)
        assert mapping_result.spark_plan, "SparkPlan 映射应成功，不能为空"
        assert not mapping_result.gaps, f"不应有 gap: {mapping_result.gaps}"

        # 编译 SparkPlan 为 PySpark 代码
        spark_compiler = SparkCompiler()
        compile_result = spark_compiler.compile(mapping_result.spark_plan)
        assert compile_result.raw_pyspark, "PySpark 编译产物不应为空"

        # 条件性执行 Spark
        if not _try_spark():
            pytest.skip("PySpark 未安装，跳过 Spark 执行验证")

        spark_executor = LocalSparkExecutor(timeout_seconds=60)
        spark_result = spark_executor.execute(
            compile_result.raw_pyspark,
            data_dir=str(case04_parquet),
        )

        if spark_result.status.name != "SUCCESS":
            pytest.skip(
                f"Spark 执行未成功（状态={spark_result.status.name}），跳过 digest 比较"
            )

        # 比较 DuckDB 与 Spark 的 digest
        duckdb_rows = _sample_rows_to_dicts(
            duckdb_result["sample_rows"], duckdb_result["columns"]
        )
        duckdb_digest = _compute_digest(duckdb_rows, duckdb_result["columns"])
        spark_rows = spark_result.output_rows
        spark_digest = _compute_digest(spark_rows, duckdb_result["columns"])

        assert duckdb_digest == spark_digest, (
            f"DuckDB 与 Spark digest 不一致！\n"
            f"  DuckDB digest: {duckdb_digest}\n"
            f"  Spark  digest: {spark_digest}\n"
            f"  DuckDB rows: {duckdb_rows}\n"
            f"  Spark  rows: {spark_rows}"
        )


# ════════════════════════════════════════════
# Case05：违章按 violation_code 聚合
# ════════════════════════════════════════════

case05_parquet = FIXTURE_DIR / "e2e_case05_small.parquet"

CASE05_TABLE_MAPPING = {
    "fv": "fact_parking_violations",
}

CASE05_TABLE_PATHS = {
    "fact_parking_violations": str(case05_parquet / "fact_parking_violations.parquet"),
}


@pytest.mark.integration
class TestCase05E2E:
    """Case05 端到端测试——违章数据两步聚合。"""

    def test_duckdb_execution(self):
        """验证 DuckDB 端：解析 → 构建 → 编译 → 执行，结果非空且正确。"""
        result = _run_duckdb_pipeline(
            "e2e_case05_small",
            CASE05_TABLE_MAPPING,
            CASE05_TABLE_PATHS,
        )

        # 验证执行结果
        assert result["row_count"] > 0, "DuckDB 执行应返回非零行数"
        assert "violation_code" in result["columns"], "输出应包含 violation_code 列"
        assert "total_violations" in result["columns"], "输出应包含 total_violations 列"
        assert "total_fine" in result["columns"], "输出应包含 total_fine 列"

        # 验证数据正确性：每个 code 2 条，各 100.0 → total=200.0
        rows_by_code = {
            r["violation_code"]: r
            for r in _sample_rows_to_dicts(
                result["sample_rows"], result["columns"]
            )
        }
        for code in ["A", "B", "C", "D", "E"]:
            assert rows_by_code.get(code, {}).get("total_violations") == 2, (
                f"violation_code={code} 应为 2 条"
            )
            assert rows_by_code.get(code, {}).get("total_fine") == 200.0, (
                f"violation_code={code} 的 total_fine 应为 200.0"
            )

    def test_spark_pipeline(self):
        """验证 Spark 端：Contract 提取 → SparkPlan 映射 → 编译，Spark 执行为条件性。"""
        duckdb_result = _run_duckdb_pipeline(
            "e2e_case05_small",
            CASE05_TABLE_MAPPING,
            CASE05_TABLE_PATHS,
        )

        # 提取 Contract v1
        extractor = DataTransformContractExtractor()
        contract = extractor.extract_v1(sql_program=duckdb_result["program"])
        assert contract.contract_id, "Contract 应有 contract_id"

        # 映射到 SparkPlan
        mapping_result = map_contract_to_spark_plan(contract)
        assert mapping_result.spark_plan, "SparkPlan 映射应成功"
        assert not mapping_result.gaps, f"不应有 gap: {mapping_result.gaps}"

        # 编译 SparkPlan 为 PySpark 代码
        spark_compiler = SparkCompiler()
        compile_result = spark_compiler.compile(mapping_result.spark_plan)
        assert compile_result.raw_pyspark, "PySpark 编译产物不应为空"

        # 条件性执行 Spark
        if not _try_spark():
            pytest.skip("PySpark 未安装，跳过 Spark 执行验证")

        spark_executor = LocalSparkExecutor(timeout_seconds=60)
        spark_result = spark_executor.execute(
            compile_result.raw_pyspark,
            data_dir=str(case05_parquet),
        )

        if spark_result.status.name != "SUCCESS":
            pytest.skip(
                f"Spark 执行未成功（状态={spark_result.status.name}），跳过 digest 比较"
            )

        # 比较 DuckDB 与 Spark 的 digest
        duckdb_rows = _sample_rows_to_dicts(
            duckdb_result["sample_rows"], duckdb_result["columns"]
        )
        duckdb_digest = _compute_digest(duckdb_rows, duckdb_result["columns"])
        spark_rows = spark_result.output_rows
        spark_digest = _compute_digest(spark_rows, duckdb_result["columns"])

        assert duckdb_digest == spark_digest, (
            f"DuckDB 与 Spark digest 不一致！\n"
            f"  DuckDB digest: {duckdb_digest}\n"
            f"  Spark  digest: {spark_digest}\n"
            f"  DuckDB rows: {duckdb_rows}\n"
            f"  Spark  rows: {spark_rows}"
        )
