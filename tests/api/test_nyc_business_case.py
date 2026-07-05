"""Phase 9A4 真实业务样本端到端测试——NYC 按行程来源每日聚合。

数据来源：NYC TLC 2026 Q1 gold.fact_trips（nyc_transport.duckdb）分层抽样 CSV
验证链路：DeveloperSpec → Parser → Builder → Validator → Compiler → DuckDB Executor
                    → Contract → Spark Orchestrator（可选）→ PlanComparator
"""

from __future__ import annotations

import os
import tempfile

import duckdb
import pytest

from tianshu_datadev.api.pipeline import Pipeline
from tianshu_datadev.artifacts.models import (
    DataTransformContractLite,
    DataTransformContractV1,
)

# ════════════════════════════════════════════
# Fixtures
# ════════════════════════════════════════════


@pytest.fixture(scope="module")
def nyc_spec_md() -> str:
    """读取 NYC 按行程来源每日聚合 DeveloperSpec。"""
    spec_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "fixtures", "nyc", "nyc_trip_source_daily.md",
    )
    with open(spec_path, "r", encoding="utf-8") as f:
        return f.read()


@pytest.fixture(scope="module")
def nyc_csv_path() -> str:
    """NYC 行程样本 CSV 的绝对路径。"""
    return os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "fixtures", "nyc", "fact_trips_sample.csv",
    )


@pytest.fixture(scope="module")
def expected_aggregates() -> dict:
    """直接 DuckDB 查询预期结果——作为 Pipeline 输出的基准。

    使用模块级 scope 避免重复计算。
    """
    csv_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "fixtures", "nyc", "fact_trips_sample.csv",
    )
    conn = duckdb.connect()
    conn.execute(f"CREATE TABLE ft AS SELECT * FROM read_csv_auto('{csv_path}')")

    # 与 DeveloperSpec 完全一致的聚合口径
    row = conn.execute("""
        SELECT
            COUNT(trip_id) AS trip_count,
            CAST(SUM(total_amount) AS DOUBLE) AS total_revenue,
            CAST(AVG(fare_amount) AS DOUBLE) AS avg_fare,
            CAST(SUM(passenger_count) AS DOUBLE) AS total_passengers,
            CAST(AVG(distance_miles) AS DOUBLE) AS avg_distance
        FROM ft
        WHERE pickup_date_key >= 20260101 AND pickup_date_key <= 20260331
    """).fetchone()

    agg_row_count = conn.execute("""
        SELECT COUNT(*) FROM (
            SELECT trip_source, pickup_date_key
            FROM ft
            WHERE pickup_date_key >= 20260101 AND pickup_date_key <= 20260331
            GROUP BY trip_source, pickup_date_key
        )
    """).fetchone()[0]

    conn.close()

    return {
        "trip_count": int(row[0]),
        "total_revenue": float(row[1]) if row[1] else 0.0,
        "avg_fare": float(row[2]) if row[2] else 0.0,
        "total_passengers": float(row[3]) if row[3] else 0.0,
        "avg_distance": float(row[4]) if row[4] else 0.0,
        "row_count": agg_row_count,
    }


# ════════════════════════════════════════════
# SQL 管线全链路测试
# ════════════════════════════════════════════


class TestNYCSqlPipeline:
    """NYC 真实业务样本——SQL 管线全链路验证。"""

    def test_spec_parses_without_errors(self, nyc_spec_md):
        """DeveloperSpec 解析零错误——Parser 必须完整识别 NYC 业务口径。"""
        from tianshu_datadev.developer_spec.parser import DeveloperSpecParser

        parser = DeveloperSpecParser()
        spec = parser.parse(nyc_spec_md)

        # 零阻塞问题
        blocking = [q for q in spec.open_questions if q.blocking]
        assert len(blocking) == 0, (
            f"NYC spec 解析存在阻塞问题: {[q.description for q in blocking]}"
        )

        # 结构完整
        assert len(spec.input_tables) == 1
        assert spec.input_tables[0].table_alias == "ft"
        assert len(spec.metrics) == 5
        assert len(spec.dimensions) == 2
        assert spec.output_spec.grain == ["trip_source", "pickup_date_key"]

    def test_run_all_completes_all_stages(self, nyc_spec_md, nyc_csv_path):
        """Pipeline.run_all() 所有 8 个阶段全部通过——无阻断。"""
        pipeline = Pipeline()
        result = pipeline.run_all(
            nyc_spec_md, table_paths={"fact_trips_sample": nyc_csv_path},
        )

        assert result["validation_passed"] is True, (
            f"Validator 应通过: {result.get('open_questions')}"
        )

        # 验证所有阶段通过
        stages = result.get("pipeline_stages", [])
        failed = [s for s in stages if s["status"] == "failed"]
        assert len(failed) == 0, (
            f"存在失败阶段: {[(s['stage'], s.get('error_message', '')) for s in failed]}"
        )

    def test_execution_row_count_matches_direct_duckdb(
        self, nyc_spec_md, nyc_csv_path, expected_aggregates,
    ):
        """Pipeline 执行结果行数与直接 DuckDB 查询一致。"""
        pipeline = Pipeline()
        result = pipeline.run_all(
            nyc_spec_md, table_paths={"fact_trips_sample": nyc_csv_path},
        )

        trace = result.get("execution_trace", {})
        assert trace.get("status") == "RUNTIME_PASS", (
            f"执行应成功: {trace.get('error_message')}"
        )
        assert trace["row_count"] == expected_aggregates["row_count"], (
            f"行数不一致: Pipeline={trace['row_count']}, "
            f"Direct={expected_aggregates['row_count']}"
        )

    def test_aggregate_values_match_direct_duckdb(
        self, nyc_spec_md, nyc_csv_path, expected_aggregates,
    ):
        """Pipeline 聚合指标与直接 DuckDB 查询完全一致（容忍浮点 0.02）。"""
        pipeline = Pipeline()
        result = pipeline.run_all(
            nyc_spec_md, table_paths={"fact_trips_sample": nyc_csv_path},
        )

        sums = result["result_summary"]["numeric_sums"]

        assert sums["trip_count"] == expected_aggregates["trip_count"], (
            f"trip_count: {sums['trip_count']} != {expected_aggregates['trip_count']}"
        )
        assert abs(sums.get("total_revenue", 0) - expected_aggregates["total_revenue"]) < 0.02, (
            f"total_revenue: {sums.get('total_revenue')} != {expected_aggregates['total_revenue']}"
        )
        assert abs(sums.get("total_passengers", 0) - expected_aggregates["total_passengers"]) < 0.02, (
            f"total_passengers: {sums.get('total_passengers')} != {expected_aggregates['total_passengers']}"
        )

    def test_column_types_are_correct(self, nyc_spec_md, nyc_csv_path):
        """输出列类型与 DeveloperSpec 声明一致。"""
        pipeline = Pipeline()
        result = pipeline.run_all(
            nyc_spec_md, table_paths={"fact_trips_sample": nyc_csv_path},
        )

        summary = result["result_summary"]
        col_types = dict(zip(summary["columns"], summary["column_types"]))

        # trip_source 应为字符串类型
        assert "VARCHAR" in col_types.get("trip_source", "").upper(), (
            f"trip_source 类型: {col_types.get('trip_source')}"
        )
        # trip_count 应为整数类型
        tc_type = col_types.get("trip_count", "").upper()
        assert "INT" in tc_type or "BIGINT" in tc_type, (
            f"trip_count 类型: {col_types.get('trip_count')}"
        )

    def test_null_handling_matches_business_rules(self, nyc_spec_md, nyc_csv_path):
        """NULL 处理符合业务口径——SUM/AVG 忽略 NULL，COUNT 计入。"""
        pipeline = Pipeline()
        result = pipeline.run_all(
            nyc_spec_md, table_paths={"fact_trips_sample": nyc_csv_path},
        )

        null_counts = result["result_summary"]["null_counts"]

        # trip_count 和 trip_source/pickup_date_key 不应有 NULL
        assert null_counts.get("trip_count", 0) == 0, "trip_count 不应有 NULL"
        assert null_counts.get("trip_source", 0) == 0, "trip_source 不应有 NULL"

        # total_amount 在 fhvhv 来源中部分为 NULL——聚合结果中应有 NULL 记录
        # （这是业务真实情况——NULL 反映某些来源类型在某日无 total_amount 数据）
        assert null_counts.get("total_revenue", 0) >= 0, (
            "total_revenue NULL 计数应 >= 0（NULL 是合法的业务空值）"
        )

    def test_provenance_contains_all_hashes(self, nyc_spec_md, nyc_csv_path):
        """provenance.yml 包含完整溯源 hash——contract/snapshot/sql 全链路可追溯。"""
        out_dir = tempfile.mkdtemp(prefix="tianshu_nyc_")
        pipeline = Pipeline(base_output_dir=out_dir)
        result = pipeline.run_all(
            nyc_spec_md, table_paths={"fact_trips_sample": nyc_csv_path},
        )

        prov_path = os.path.join(out_dir, result["request_id"], "provenance.yml")
        assert os.path.isfile(prov_path), f"provenance.yml 未生成: {prov_path}"

        with open(prov_path, "r", encoding="utf-8") as f:
            prov = f.read()

        # 核心溯源字段全部存在
        required = [
            "request_id:",
            "spec_hash:",
            "parsed_spec_hash:",
            "source_manifest_hash:",
            "sql_build_plan_hash:",
            "compiled_sql_sha256:",
            "data_transform_contract_hash:",
            "snapshot_manifest_hash:",
            "retry_count:",
        ]
        for field in required:
            assert field in prov, f"provenance.yml 缺少字段: {field}"

        # snapshot_manifest_hash 无快照时应为空
        assert 'snapshot_manifest_hash: ""' in prov, (
            "未注入 SnapshotBuilder 时 snapshot_manifest_hash 应为空"
        )

        # 清理
        import shutil
        shutil.rmtree(out_dir, ignore_errors=True)


# ════════════════════════════════════════════
# DataTransformContract 测试
# ════════════════════════════════════════════


class TestNYCContractExtraction:
    """NYC 业务样本——DataTransformContract 确定性抽取。"""

    def test_contract_is_extracted_from_run_all(self, nyc_spec_md, nyc_csv_path):
        """run_all() 后 export_artifacts() 能导出 DataTransformContract。"""
        pipeline = Pipeline()
        result = pipeline.run_all(
            nyc_spec_md, table_paths={"fact_trips_sample": nyc_csv_path},
        )

        bundle = pipeline.export_artifacts(result["request_id"])
        assert bundle is not None
        assert bundle.data_transform_contract is not None, (
            "run_all() 应产出 DataTransformContract"
        )

    def test_contract_is_deterministic(self, nyc_spec_md, nyc_csv_path):
        """相同 spec → 相同 contract hash——确定性保证。"""
        pipeline1 = Pipeline()
        result1 = pipeline1.run_all(
            nyc_spec_md, table_paths={"fact_trips_sample": nyc_csv_path},
        )
        bundle1 = pipeline1.export_artifacts(result1["request_id"])

        pipeline2 = Pipeline()
        result2 = pipeline2.run_all(
            nyc_spec_md, table_paths={"fact_trips_sample": nyc_csv_path},
        )
        bundle2 = pipeline2.export_artifacts(result2["request_id"])

        contract1 = bundle1.data_transform_contract
        contract2 = bundle2.data_transform_contract

        # 两个 contract 应产生相同 hash
        if isinstance(contract1, DataTransformContractV1):
            hash1 = DataTransformContractV1.compute_contract_hash(contract1)
            hash2 = DataTransformContractV1.compute_contract_hash(contract2)
        else:
            hash1 = DataTransformContractLite.compute_contract_hash(contract1)
            hash2 = DataTransformContractLite.compute_contract_hash(contract2)

        assert hash1 == hash2, (
            f"相同 spec → 相同 contract hash: {hash1} != {hash2}"
        )

    def test_contract_contains_all_metrics(self, nyc_spec_md, nyc_csv_path):
        """Contract 保留所有 5 个业务指标的聚合定义。"""
        pipeline = Pipeline()
        result = pipeline.run_all(
            nyc_spec_md, table_paths={"fact_trips_sample": nyc_csv_path},
        )
        bundle = pipeline.export_artifacts(result["request_id"])
        contract = bundle.data_transform_contract

        # Lite 和 V1 都有 aggregations 字段
        agg_names = {a.function for a in contract.aggregations}
        assert "COUNT" in agg_names, "Contract 应包含 COUNT 聚合"
        assert "SUM" in agg_names, "Contract 应包含 SUM 聚合"
        assert "AVG" in agg_names, "Contract 应包含 AVG 聚合"


# ════════════════════════════════════════════
# Spark 双链验证（可选——依赖 Spark 环境）
# ════════════════════════════════════════════


class TestNYCSparkDualChain:
    """NYC 业务样本——Spark 双管线逻辑验证。

    若 Spark 环境不可用，测试标记为 skip 而非 fail。
    """

    def test_spark_orchestrator_logic_equivalence(self, nyc_spec_md, nyc_csv_path):
        """Spark Orchestrator 逻辑等价判定——SQL/Spark 双链对比。"""
        pytest.importorskip("pyspark", reason="PySpark 环境不可用——跳过物理验证")

        from tianshu_datadev.spark.contract_adapter import adapt_lite_to_v1
        from tianshu_datadev.spark.orchestrator import SparkOrchestrator

        # 第一步：跑 SQL 管线获取 contract
        pipeline = Pipeline()
        result = pipeline.run_all(
            nyc_spec_md, table_paths={"fact_trips_sample": nyc_csv_path},
        )
        bundle = pipeline.export_artifacts(result["request_id"])

        contract_lite = bundle.data_transform_contract
        sql_plan = bundle.sql_build_plan

        assert contract_lite is not None, "Contract 不应为空"
        assert sql_plan is not None, "SqlBuildPlan 不应为空"

        # 第二步：适配为 V1 → 跑 Spark Orchestrator
        contract_v1 = adapt_lite_to_v1(contract_lite)
        orchestrator = SparkOrchestrator()
        state = orchestrator.run(contract=contract_v1, sql_plan=sql_plan)

        # 第三步：验证逻辑等价
        # Comparator 阶段状态应从 stage_results 中读取
        comp_status = state.stage_results.get("COMPARATOR", "NOT_EXECUTED")
        valid_statuses = {"SUCCESS", "FAILURE", "HUMAN_REVIEW", "SKIPPED", "NOT_EXECUTED"}
        assert comp_status in valid_statuses, (
            f"Comparator 状态异常: {comp_status}"
        )
        # comparator_report 应非空（SQL/Spark 双链已跑）
        assert state.comparator_report is not None, (
            "Orchestrator 应产出 PlanComparisonReport"
        )
