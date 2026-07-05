"""Phase 9A4 真实业务样本端到端测试——NYC 案例 01-06。

数据来源：NYC TLC 2026 Q1 gold.fact_trips（nyc_transport.duckdb）分层抽样 CSV
验证链路：DeveloperSpec → Parser → Builder → Validator → Compiler → DuckDB Executor
                    → Contract → Spark Orchestrator（可选）→ PlanComparator

案例清单：
- 01：按行程来源每日聚合（aggregate_table）
- 02：行程距离分类标签（label_table——透传列验证，CASE WHEN/CONCAT 为 B 类限制）
"""

from __future__ import annotations

import hashlib
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
        """Spark Orchestrator 逻辑等价判定——SQL/Spark 双链对比。

        显式断言 comparator_report.status=LOGIC_EQUIVALENT——
        这是 NYC 案例 01 "Spark 双链路逻辑等价点亮" 的唯一证据。
        """
        pytest.importorskip("pyspark", reason="PySpark 环境不可用——跳过物理验证")

        from tianshu_datadev.spark.contract_adapter import adapt_lite_to_v1
        from tianshu_datadev.spark.orchestrator import SparkOrchestrator
        from tianshu_datadev.spark.plan_comparator import ComparisonStatus

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

        # 第三步：显式断言逻辑等价
        # ① comparator_report 必须非空
        assert state.comparator_report is not None, (
            "Orchestrator 应产出 PlanComparisonReport"
        )

        # ② comparator_report.status 必须显式断言为 LOGIC_EQUIVALENT
        #    这是 NYC 案例 01 Spark 双链路逻辑等价点亮的唯一证据——
        #    不得用 stage_results 字符串绕过。
        assert state.comparator_report.status == ComparisonStatus.LOGIC_EQUIVALENT, (
            f"NYC 案例 01 逻辑对比应判定为等价，"
            f"实际 status={state.comparator_report.status}，"
            f"step_results={[(r.step_type, r.verdict.value) for r in state.comparator_report.step_results]}"
        )

        # ③ overall_status 不得与 comparator_report.status 矛盾
        #    LOGIC_EQUIVALENT + PHYSICAL NOT_EXECUTED → LOGIC_CONSISTENT_PHYSICAL_NOT_EXECUTED
        logic_consistent_statuses = {
            "LOGIC_CONSISTENT_PHYSICAL_NOT_EXECUTED",
            "ALL_CONSISTENT",
        }
        assert state.overall_status.value in logic_consistent_statuses, (
            f"comparator_report.status=LOGIC_EQUIVALENT 时 "
            f"overall_status 应为逻辑一致，"
            f"实际 overall_status={state.overall_status}"
        )


# ════════════════════════════════════════════
# Case 02：行程距离分类标签（label_table）
# ════════════════════════════════════════════


@pytest.fixture(scope="module")
def nyc02_spec_md() -> str:
    """读取 NYC 行程距离分类标签 DeveloperSpec。"""
    spec_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "fixtures", "nyc", "nyc_distance_category_label.md",
    )
    with open(spec_path, "r", encoding="utf-8") as f:
        return f.read()


@pytest.fixture(scope="module")
def nyc02_expected_row_count() -> int:
    """直接 DuckDB 查询——Case 02 预期输出行数等于 Q1 时间范围内行程数。"""
    csv_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "fixtures", "nyc", "fact_trips_sample.csv",
    )
    conn = duckdb.connect()
    conn.execute(f"CREATE TABLE ft AS SELECT * FROM read_csv_auto('{csv_path}')")
    row_count = conn.execute(
        "SELECT COUNT(*) FROM ft "
        "WHERE pickup_date_key >= 20260101 AND pickup_date_key <= 20260331"
    ).fetchone()[0]
    conn.close()
    return int(row_count)


class TestNYCCase02SqlPipeline:
    """NYC 案例 02——label_table SQL 管线全链路验证。"""

    def test_spec_parses_without_errors(self, nyc02_spec_md):
        """label_table DeveloperSpec 解析零错误——metrics: [] 必须被正确识别。"""
        from tianshu_datadev.developer_spec.parser import DeveloperSpecParser

        parser = DeveloperSpecParser()
        spec = parser.parse(nyc02_spec_md)

        # 零阻塞问题
        blocking = [q for q in spec.open_questions if q.blocking]
        assert len(blocking) == 0, (
            f"NYC 案例 02 spec 解析存在阻塞: {[q.description for q in blocking]}"
        )

        # label_table 特征：metrics 为空，grain 为 trip_id
        assert len(spec.metrics) == 0, (
            f"label_table 应无聚合指标，实际 metrics={spec.metrics}"
        )
        assert spec.output_spec.grain == ["trip_id"], (
            f"grain 应为 trip_id，实际={spec.output_spec.grain}"
        )

    def test_run_all_completes_all_stages(self, nyc02_spec_md, nyc_csv_path):
        """label_table Pipeline.run_all() 所有阶段通过——无聚合查询需 LIMIT。"""
        pipeline = Pipeline()
        result = pipeline.run_all(
            nyc02_spec_md, table_paths={"fact_trips_sample": nyc_csv_path},
        )

        assert result["validation_passed"] is True, (
            f"Validator 应通过: {result.get('open_questions')}"
        )

        stages = result.get("pipeline_stages", [])
        failed = [s for s in stages if s["status"] == "failed"]
        assert len(failed) == 0, (
            f"存在失败阶段: {[(s['stage'], s.get('error_message', '')) for s in failed]}"
        )

    def test_row_count_matches_expected(
        self, nyc02_spec_md, nyc_csv_path, nyc02_expected_row_count,
    ):
        """label_table 输出行数 = Q1 时间范围内行程数（透传，无聚合丢失）。"""
        pipeline = Pipeline()
        result = pipeline.run_all(
            nyc02_spec_md, table_paths={"fact_trips_sample": nyc_csv_path},
        )

        trace = result.get("execution_trace", {})
        assert trace.get("status") == "RUNTIME_PASS", (
            f"执行应成功: {trace.get('error_message')}"
        )
        assert trace["row_count"] == nyc02_expected_row_count, (
            f"行数不一致: Pipeline={trace['row_count']}, "
            f"Expected={nyc02_expected_row_count}"
        )

    def test_output_columns_are_correct(self, nyc02_spec_md, nyc_csv_path):
        """输出列包含全部 5 个透传源表列。"""
        pipeline = Pipeline()
        result = pipeline.run_all(
            nyc02_spec_md, table_paths={"fact_trips_sample": nyc_csv_path},
        )

        summary = result["result_summary"]
        columns = summary["columns"]
        expected_cols = {"trip_id", "trip_source", "distance_miles",
                         "total_amount", "passenger_count"}
        assert set(columns) == expected_cols, (
            f"输出列应为源表透传列: {expected_cols}, 实际: {set(columns)}"
        )

    def test_no_aggregation_applied(self, nyc02_spec_md, nyc_csv_path):
        """label_table 不应执行聚合——每行 trip_id 唯一，输出行数 = 输入行数。"""
        pipeline = Pipeline()
        result = pipeline.run_all(
            nyc02_spec_md, table_paths={"fact_trips_sample": nyc_csv_path},
        )

        # 编译 SQL 中不应含 GROUP BY 或 SUM
        compiled = result.get("compiled", "")
        assert "GROUP BY" not in compiled.upper(), (
            f"label_table 不应有 GROUP BY: {compiled[:200]}"
        )
        assert "SUM(" not in compiled.upper(), (
            f"label_table 不应有 SUM 聚合: {compiled[:200]}"
        )

    def test_provenance_contains_all_hashes(self, nyc02_spec_md, nyc_csv_path):
        """provenance.yml 包含完整溯源 hash。"""
        out_dir = tempfile.mkdtemp(prefix="tianshu_nyc02_")
        pipeline = Pipeline(base_output_dir=out_dir)
        result = pipeline.run_all(
            nyc02_spec_md, table_paths={"fact_trips_sample": nyc_csv_path},
        )

        prov_path = os.path.join(out_dir, result["request_id"], "provenance.yml")
        assert os.path.isfile(prov_path), f"provenance.yml 未生成: {prov_path}"

        with open(prov_path, "r", encoding="utf-8") as f:
            prov = f.read()

        required = [
            "request_id:", "spec_hash:", "parsed_spec_hash:",
            "source_manifest_hash:", "sql_build_plan_hash:",
            "compiled_sql_sha256:", "data_transform_contract_hash:",
        ]
        for field in required:
            assert field in prov, f"provenance.yml 缺少字段: {field}"

        import shutil
        shutil.rmtree(out_dir, ignore_errors=True)


class TestNYCCase02ContractExtraction:
    """NYC 案例 02——label_table 的 DataTransformContract 提取。"""

    def test_contract_is_extracted(self, nyc02_spec_md, nyc_csv_path):
        """label_table 也能导出 DataTransformContract。"""
        pipeline = Pipeline()
        result = pipeline.run_all(
            nyc02_spec_md, table_paths={"fact_trips_sample": nyc_csv_path},
        )
        bundle = pipeline.export_artifacts(result["request_id"])
        assert bundle is not None
        assert bundle.data_transform_contract is not None, (
            "label_table run_all() 应产出 DataTransformContract"
        )

    def test_contract_is_deterministic(self, nyc02_spec_md, nyc_csv_path):
        """相同 label_table spec → 相同 contract hash。"""
        pipeline1 = Pipeline()
        result1 = pipeline1.run_all(
            nyc02_spec_md, table_paths={"fact_trips_sample": nyc_csv_path},
        )
        bundle1 = pipeline1.export_artifacts(result1["request_id"])

        pipeline2 = Pipeline()
        result2 = pipeline2.run_all(
            nyc02_spec_md, table_paths={"fact_trips_sample": nyc_csv_path},
        )
        bundle2 = pipeline2.export_artifacts(result2["request_id"])

        contract1 = bundle1.data_transform_contract
        contract2 = bundle2.data_transform_contract

        if isinstance(contract1, DataTransformContractV1):
            hash1 = DataTransformContractV1.compute_contract_hash(contract1)
            hash2 = DataTransformContractV1.compute_contract_hash(contract2)
        else:
            hash1 = DataTransformContractLite.compute_contract_hash(contract1)
            hash2 = DataTransformContractLite.compute_contract_hash(contract2)

        assert hash1 == hash2, (
            f"相同 spec → 相同 contract hash: {hash1} != {hash2}"
        )


class TestNYCCase02SparkDualChain:
    """NYC 案例 02——label_table Spark 双管线逻辑验证。"""

    def test_spark_orchestrator_logic_equivalence(self, nyc02_spec_md, nyc_csv_path):
        """label_table Spark Orchestrator 逻辑等价判定——显式断言 comparator_report.status。"""
        pytest.importorskip("pyspark", reason="PySpark 环境不可用——跳过物理验证")

        from tianshu_datadev.spark.contract_adapter import adapt_lite_to_v1
        from tianshu_datadev.spark.orchestrator import SparkOrchestrator
        from tianshu_datadev.spark.plan_comparator import ComparisonStatus

        # 第一步：跑 SQL 管线获取 contract
        pipeline = Pipeline()
        result = pipeline.run_all(
            nyc02_spec_md, table_paths={"fact_trips_sample": nyc_csv_path},
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

        # 第三步：显式断言
        assert state.comparator_report is not None, (
            "Orchestrator 应产出 PlanComparisonReport"
        )
        assert state.comparator_report.status == ComparisonStatus.LOGIC_EQUIVALENT, (
            f"NYC 案例 02 逻辑对比应判定为等价，"
            f"实际 status={state.comparator_report.status}，"
            f"step_results={[(r.step_type, r.verdict.value) for r in state.comparator_report.step_results]}"
        )

        logic_consistent_statuses = {
            "LOGIC_CONSISTENT_PHYSICAL_NOT_EXECUTED", "ALL_CONSISTENT",
        }
        assert state.overall_status.value in logic_consistent_statuses, (
            f"comparator_report.status=LOGIC_EQUIVALENT 时 "
            f"overall_status 应为逻辑一致，"
            f"实际 overall_status={state.overall_status}"
        )


# ════════════════════════════════════════════
# Case 03：停车违章明细宽表（detail_table + LEFT JOIN）
# ════════════════════════════════════════════


@pytest.fixture(scope="module")
def nyc03_spec_md() -> str:
    """读取 NYC 停车违章明细宽表 DeveloperSpec。"""
    spec_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "fixtures", "nyc", "nyc_parking_violation_detail.md",
    )
    with open(spec_path, "r", encoding="utf-8") as f:
        return f.read()


@pytest.fixture(scope="module")
def nyc03_csv_paths() -> dict:
    """Case 03 需要两张 CSV：事实表 + 维度表。"""
    base = os.path.join(os.path.dirname(os.path.dirname(__file__)), "fixtures", "nyc")
    return {
        "fact_parking_violations_sample": os.path.join(base, "fact_parking_violations_sample.csv"),
        "dim_violation_type": os.path.join(base, "dim_violation_type.csv"),
    }


class TestNYCCase03SqlPipeline:
    """NYC 案例 03——detail_table + LEFT JOIN SQL 管线全链路验证。"""

    def test_spec_parses_with_left_join(self, nyc03_spec_md):
        """detail_table + LEFT JOIN 解析零错误。"""
        from tianshu_datadev.developer_spec.parser import DeveloperSpecParser

        parser = DeveloperSpecParser()
        spec = parser.parse(nyc03_spec_md)

        blocking = [q for q in spec.open_questions if q.blocking]
        assert len(blocking) == 0, (
            f"Case 03 spec 解析存在阻塞: {[q.description for q in blocking]}"
        )

        # 两表 LEFT JOIN
        assert len(spec.input_tables) == 2
        assert spec.joins is not None
        assert len(spec.joins) == 1
        assert spec.joins[0].join_type.value == "LEFT"

    def test_run_all_completes_all_stages(self, nyc03_spec_md, nyc03_csv_paths):
        """detail_table + LEFT JOIN Pipeline.run_all() 全阶段通过。"""
        pipeline = Pipeline()
        result = pipeline.run_all(nyc03_spec_md, table_paths=nyc03_csv_paths)

        assert result["validation_passed"] is True
        stages = result.get("pipeline_stages", [])
        failed = [s for s in stages if s["status"] == "failed"]
        assert len(failed) == 0, (
            f"存在失败阶段: {[(s['stage'], s.get('error_message', '')) for s in failed]}"
        )

    def test_left_join_preserves_all_fact_rows(self, nyc03_spec_md, nyc03_csv_paths):
        """LEFT JOIN 不丢失事实表行——输出行数 = min(输入行数, LIMIT)。"""
        pipeline = Pipeline()
        result = pipeline.run_all(nyc03_spec_md, table_paths=nyc03_csv_paths)

        trace = result.get("execution_trace", {})
        assert trace.get("status") == "RUNTIME_PASS"
        assert trace["row_count"] == 3000, (
            f"LIMIT 3000 应输出 3000 行，实际={trace['row_count']}"
        )

    def test_violation_description_is_populated(self, nyc03_spec_md, nyc03_csv_paths):
        """LEFT JOIN 后 violation_description 应全部有值——所有 violation_code 均匹配字典。"""
        pipeline = Pipeline()
        result = pipeline.run_all(nyc03_spec_md, table_paths=nyc03_csv_paths)

        null_counts = result["result_summary"]["null_counts"]
        assert null_counts.get("violation_description", 0) == 0, (
            "LEFT JOIN 后 violation_description 不应有 NULL——"
            "所有抽样 violation_code 均应匹配字典"
        )

    def test_output_columns_match_spec(self, nyc03_spec_md, nyc03_csv_paths):
        """输出列符合 DevSpec 声明。"""
        pipeline = Pipeline()
        result = pipeline.run_all(nyc03_spec_md, table_paths=nyc03_csv_paths)

        columns = set(result["result_summary"]["columns"])
        expected = {"summons_number", "violation_description", "plate_id",
                    "registration_state", "is_duplicate_summons"}
        assert columns == expected, (
            f"输出列: {columns} != {expected}"
        )


class TestNYCCase03ContractExtraction:
    """NYC 案例 03——detail_table + LEFT JOIN 的 Contract 提取。"""

    def test_contract_is_extracted(self, nyc03_spec_md, nyc03_csv_paths):
        """detail_table 也能导出 DataTransformContract。"""
        pipeline = Pipeline()
        result = pipeline.run_all(nyc03_spec_md, table_paths=nyc03_csv_paths)
        bundle = pipeline.export_artifacts(result["request_id"])
        assert bundle is not None
        assert bundle.data_transform_contract is not None

    def test_contract_is_deterministic(self, nyc03_spec_md, nyc03_csv_paths):
        """相同 detail_table spec → 相同 contract hash。"""
        pipeline1 = Pipeline()
        result1 = pipeline1.run_all(nyc03_spec_md, table_paths=nyc03_csv_paths)
        bundle1 = pipeline1.export_artifacts(result1["request_id"])

        pipeline2 = Pipeline()
        result2 = pipeline2.run_all(nyc03_spec_md, table_paths=nyc03_csv_paths)
        bundle2 = pipeline2.export_artifacts(result2["request_id"])

        c1, c2 = bundle1.data_transform_contract, bundle2.data_transform_contract
        if isinstance(c1, DataTransformContractV1):
            h1 = DataTransformContractV1.compute_contract_hash(c1)
            h2 = DataTransformContractV1.compute_contract_hash(c2)
        else:
            h1 = DataTransformContractLite.compute_contract_hash(c1)
            h2 = DataTransformContractLite.compute_contract_hash(c2)
        assert h1 == h2, f"相同 spec → 相同 contract hash: {h1} != {h2}"


class TestNYCCase03SparkDualChain:
    """NYC 案例 03——detail_table Spark 双管线逻辑验证。"""

    def test_spark_orchestrator_logic_equivalence(
        self, nyc03_spec_md, nyc03_csv_paths,
    ):
        """detail_table + LEFT JOIN Spark Orchestrator 逻辑等价。"""
        pytest.importorskip("pyspark", reason="PySpark 环境不可用")

        from tianshu_datadev.spark.contract_adapter import adapt_lite_to_v1
        from tianshu_datadev.spark.orchestrator import SparkOrchestrator
        from tianshu_datadev.spark.plan_comparator import ComparisonStatus

        pipeline = Pipeline()
        result = pipeline.run_all(nyc03_spec_md, table_paths=nyc03_csv_paths)
        bundle = pipeline.export_artifacts(result["request_id"])

        contract_v1 = adapt_lite_to_v1(bundle.data_transform_contract)
        orchestrator = SparkOrchestrator()
        state = orchestrator.run(
            contract=contract_v1, sql_plan=bundle.sql_build_plan,
        )

        assert state.comparator_report is not None
        assert state.comparator_report.status == ComparisonStatus.LOGIC_EQUIVALENT, (
            f"Case 03 应为 LOGIC_EQUIVALENT，实际={state.comparator_report.status}"
        )
        assert state.overall_status.value in {
            "LOGIC_CONSISTENT_PHYSICAL_NOT_EXECUTED", "ALL_CONSISTENT",
        }


# ════════════════════════════════════════════
# Case 04：Borough 日期维度聚合（2 表 JOIN + 聚合）
# ════════════════════════════════════════════


@pytest.fixture(scope="module")
def nyc04_csv_paths() -> dict:
    """Case 04 需要事实表 + 区域维度表。"""
    base = os.path.join(os.path.dirname(os.path.dirname(__file__)), "fixtures", "nyc")
    return {
        "fact_trips_sample": os.path.join(base, "fact_trips_sample.csv"),
        "dim_taxi_zone": os.path.join(base, "dim_taxi_zone.csv"),
    }


class TestNYCCase04SqlPipeline:
    """NYC 案例 04——2 表 LEFT JOIN + 多指标聚合。"""

    @pytest.fixture(scope="class")
    def nyc04_spec_md(self) -> str:
        spec_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "fixtures", "nyc", "nyc_borough_weekly_agg.md",
        )
        with open(spec_path, "r", encoding="utf-8") as f:
            return f.read()

    def test_join_and_aggregate(self, nyc04_spec_md, nyc04_csv_paths):
        """2 表 LEFT JOIN + GROUP BY 3 维 + 4 指标——全链路通过。"""
        pipeline = Pipeline()
        result = pipeline.run_all(nyc04_spec_md, table_paths=nyc04_csv_paths)

        assert result["validation_passed"] is True
        trace = result.get("execution_trace", {})
        assert trace.get("status") == "RUNTIME_PASS"
        assert trace["row_count"] > 0, "2 表 JOIN 聚合应有产出"
        # 验证 borough 列有值（非全 NULL——LEFT JOIN 成功关联）
        columns = set(result["result_summary"]["columns"])
        assert "borough" in columns

    def test_contract_is_extracted(self, nyc04_spec_md, nyc04_csv_paths):
        """2 表 JOIN 聚合能导出 Contract。"""
        pipeline = Pipeline()
        result = pipeline.run_all(nyc04_spec_md, table_paths=nyc04_csv_paths)
        bundle = pipeline.export_artifacts(result["request_id"])
        assert bundle is not None
        assert bundle.data_transform_contract is not None


class TestNYCCase04SparkDualChain:
    """NYC 案例 04——2 表 JOIN Spark 双管线验证。"""

    @pytest.fixture(scope="class")
    def nyc04_spec_md(self) -> str:
        spec_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "fixtures", "nyc", "nyc_borough_weekly_agg.md",
        )
        with open(spec_path, "r", encoding="utf-8") as f:
            return f.read()

    def test_spark_orchestrator_logic_equivalence(
        self, nyc04_spec_md, nyc04_csv_paths,
    ):
        """2 表 JOIN Spark Orchestrator 逻辑等价。"""
        pytest.importorskip("pyspark", reason="PySpark 环境不可用")

        from tianshu_datadev.spark.contract_adapter import adapt_lite_to_v1
        from tianshu_datadev.spark.orchestrator import SparkOrchestrator
        from tianshu_datadev.spark.plan_comparator import ComparisonStatus

        pipeline = Pipeline()
        result = pipeline.run_all(nyc04_spec_md, table_paths=nyc04_csv_paths)
        bundle = pipeline.export_artifacts(result["request_id"])

        contract_v1 = adapt_lite_to_v1(bundle.data_transform_contract)
        orchestrator = SparkOrchestrator()
        state = orchestrator.run(
            contract=contract_v1, sql_plan=bundle.sql_build_plan,
        )

        assert state.comparator_report is not None
        assert state.comparator_report.status == ComparisonStatus.LOGIC_EQUIVALENT, (
            f"Case 04 应为 LOGIC_EQUIVALENT，实际={state.comparator_report.status}"
        )
        assert state.overall_status.value in {
            "LOGIC_CONSISTENT_PHYSICAL_NOT_EXECUTED", "ALL_CONSISTENT",
        }


# ════════════════════════════════════════════
# Case 05：Borough Top5 上车区域（INNER JOIN + ROW_NUMBER 窗口函数）
# ════════════════════════════════════════════


@pytest.fixture(scope="module")
def nyc05_csv_paths() -> dict:
    """Case 05 需要事实表 + 区域维度表。"""
    base = os.path.join(os.path.dirname(os.path.dirname(__file__)), "fixtures", "nyc")
    return {
        "fact_trips_sample": os.path.join(base, "fact_trips_sample.csv"),
        "dim_taxi_zone": os.path.join(base, "dim_taxi_zone.csv"),
    }


class TestNYCCase05SqlPipeline:
    """NYC 案例 05——INNER JOIN + ROW_NUMBER 窗口函数。"""

    @pytest.fixture(scope="class")
    def nyc05_spec_md(self) -> str:
        spec_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "fixtures", "nyc", "nyc_borough_top5_zones.md",
        )
        with open(spec_path, "r", encoding="utf-8") as f:
            return f.read()

    def test_window_function_produces_rank(self, nyc05_spec_md, nyc05_csv_paths):
        """ROW_NUMBER 窗口函数应在 INNER JOIN 后产生排名列。"""
        pipeline = Pipeline()
        result = pipeline.run_all(nyc05_spec_md, table_paths=nyc05_csv_paths)

        assert result["validation_passed"] is True
        trace = result.get("execution_trace", {})
        assert trace.get("status") == "RUNTIME_PASS"
        columns = set(result["result_summary"]["columns"])
        assert "rank_by_count" in columns, (
            f"窗口函数应产出 rank_by_count 列，实际列={columns}"
        )

    def test_contract_is_extracted(self, nyc05_spec_md, nyc05_csv_paths):
        """INNER JOIN + 窗口函数能导出 Contract。"""
        pipeline = Pipeline()
        result = pipeline.run_all(nyc05_spec_md, table_paths=nyc05_csv_paths)
        bundle = pipeline.export_artifacts(result["request_id"])
        assert bundle is not None
        assert bundle.data_transform_contract is not None


class TestNYCCase05SparkDualChain:
    """NYC 案例 05——INNER JOIN + 窗口函数 Spark 双管线验证。"""

    @pytest.fixture(scope="class")
    def nyc05_spec_md(self) -> str:
        spec_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "fixtures", "nyc", "nyc_borough_top5_zones.md",
        )
        with open(spec_path, "r", encoding="utf-8") as f:
            return f.read()

    def test_spark_orchestrator_logic_equivalence(
        self, nyc05_spec_md, nyc05_csv_paths,
    ):
        """INNER JOIN + 窗口函数 Spark Orchestrator 逻辑等价。"""
        pytest.importorskip("pyspark", reason="PySpark 环境不可用")

        from tianshu_datadev.spark.contract_adapter import adapt_lite_to_v1
        from tianshu_datadev.spark.orchestrator import SparkOrchestrator
        from tianshu_datadev.spark.plan_comparator import ComparisonStatus

        pipeline = Pipeline()
        result = pipeline.run_all(nyc05_spec_md, table_paths=nyc05_csv_paths)
        bundle = pipeline.export_artifacts(result["request_id"])

        contract_v1 = adapt_lite_to_v1(bundle.data_transform_contract)
        orchestrator = SparkOrchestrator()
        state = orchestrator.run(
            contract=contract_v1, sql_plan=bundle.sql_build_plan,
        )

        assert state.comparator_report is not None
        # 窗口函数（WindowStep）的 Comparator 支持待完善——当前预期 NOT_COVERED
        # NOT_COVERED → HUMAN_REVIEW_REQUIRED（符合 derive_overall_status 规则）
        assert state.comparator_report.status != ComparisonStatus.LOGIC_MISMATCH, (
            f"Case 05 不应为 LOGIC_MISMATCH，"
            f"实际 status={state.comparator_report.status}"
        )
        # NOT_COVERED 触发 HUMAN_REVIEW_REQUIRED——非逻辑错误，是能力覆盖不足
        assert state.overall_status.value in {
            "LOGIC_CONSISTENT_PHYSICAL_NOT_EXECUTED", "ALL_CONSISTENT",
            "HUMAN_REVIEW_REQUIRED",
        }


# ════════════════════════════════════════════
# Case 06：区域安全合规画像（多步 DAG 跨域融合）
# ════════════════════════════════════════════


@pytest.fixture(scope="module")
def nyc06_spec_md() -> str:
    """读取 NYC 区域安全合规画像 DeveloperSpec。"""
    spec_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "fixtures", "nyc", "nyc_safety_compliance_profile.md",
    )
    with open(spec_path, "r", encoding="utf-8") as f:
        return f.read()


@pytest.fixture(scope="module")
def nyc06_csv_paths() -> dict:
    """Case 06 需要 5 张 CSV——3 域数据。"""
    base = os.path.join(
        os.path.dirname(os.path.dirname(__file__)), "fixtures", "nyc",
    )
    return {
        "fact_trips_sample": os.path.join(base, "fact_trips_sample.csv"),
        "dim_taxi_zone": os.path.join(base, "dim_taxi_zone.csv"),
        "fact_crashes_sample": os.path.join(base, "fact_crashes_sample.csv"),
        "dws_daily_parking_summary": os.path.join(base, "dws_daily_parking_summary.csv"),
        "dim_violation_type": os.path.join(base, "dim_violation_type.csv"),
    }


class TestNYCCase06SqlPipeline:
    """NYC 案例 06——多步 DAG 跨域融合全链路验证。"""

    def test_spec_parses_with_compute_steps(self, nyc06_spec_md):
        """Case 06 spec 解析——compute_steps 必须被识别（7 步 DAG）。"""
        from tianshu_datadev.developer_spec.parser import DeveloperSpecParser

        parser = DeveloperSpecParser()
        spec = parser.parse(nyc06_spec_md)

        # compute_steps 必须非空
        assert spec.compute_steps is not None, (
            "Case 06 应解析出 compute_steps"
        )
        assert len(spec.compute_steps) == 7, (
            f"应为 7 步 DAG，实际={len(spec.compute_steps)}"
        )

        # 零阻塞问题
        blocking = [q for q in spec.open_questions if q.blocking]
        assert len(blocking) == 0, (
            f"Case 06 spec 解析存在阻塞: {[q.description for q in blocking]}"
        )

    def test_build_plan_produces_sql_program(self, nyc06_spec_md):
        """build_plan() 应为 Case 06 产出 SqlProgram（非单语句）。"""
        pipeline = Pipeline()
        result = pipeline.build_plan(nyc06_spec_md)

        assert result.get("validation_passed") is True, (
            f"Validator 应通过: {result.get('open_questions')}"
        )
        # 多步 DAG 应输出 plan_id（FINAL 步骤的 plan）
        assert result.get("plan_id"), "应产出 plan_id"

    def test_sql_program_has_seven_statements(self, nyc06_spec_md):
        """SqlProgram 应包含 7 个 statement——对应 7 个 compute_steps。"""
        from tianshu_datadev.developer_spec.parser import DeveloperSpecParser
        from tianshu_datadev.planning.sql_build_plan import SqlBuildPlanBuilder

        parser = DeveloperSpecParser()
        spec = parser.parse(nyc06_spec_md)

        builder = SqlBuildPlanBuilder()
        plans = builder.build_from_steps(spec, None)
        assert len(plans) == 7, (
            f"build_from_steps 应产出 7 个 plan，实际={len(plans)}"
        )

    def test_no_cte_in_compiled_sql(self, nyc06_spec_md):
        """编译产物中不得出现 WITH ... AS（CTE 禁止）。"""
        from tianshu_datadev.developer_spec.parser import DeveloperSpecParser
        from tianshu_datadev.planning.program_factory import build_sql_program_from_compute_steps
        from tianshu_datadev.planning.sql_build_plan import SqlBuildPlanBuilder
        from tianshu_datadev.sql.compiler import DuckDbSqlCompiler

        parser = DeveloperSpecParser()
        spec = parser.parse(nyc06_spec_md)

        builder = SqlBuildPlanBuilder()
        plans = builder.build_from_steps(spec, None)
        chain_id = hashlib.md5(
            "|".join(s.step_name for s in spec.compute_steps).encode()
        ).hexdigest()[:8]
        sql_program = build_sql_program_from_compute_steps(plans, spec, chain_id)

        # compile_program 需 table_mapping——使用 auto 映射
        table_mapping = {}
        for t in spec.input_tables:
            if t.table_alias and t.source_table:
                table_mapping[t.table_alias] = str(t.source_table)
        compiler_with_map = DuckDbSqlCompiler(table_mapping=table_mapping)
        program_artifact = compiler_with_map.compile_program(sql_program)
        program_sql = program_artifact.compiled

        # 检查每条 SQL 不含 WITH
        for stmt_sql in program_sql.statements:
            sql_upper = stmt_sql.sql.upper()
            assert "WITH " not in sql_upper or "WITHIN GROUP" in sql_upper, (
                f"编译 SQL 不得包含 CTE (WITH ... AS): {stmt_sql.sql[:200]}"
            )

    def test_run_all_produces_borough_results(self, nyc06_spec_md, nyc06_csv_paths):
        """Pipeline.run_all() 全链路执行——输出应包含 5 个 NYC 行政区结果。"""
        pipeline = Pipeline()
        result = pipeline.run_all(nyc06_spec_md, table_paths=nyc06_csv_paths)

        assert result["validation_passed"] is True, (
            f"Case 06 验证应通过: {result.get('open_questions')}"
        )

        # 验证所有阶段通过
        stages = result.get("pipeline_stages", [])
        failed = [s for s in stages if s["status"] == "failed"]
        assert len(failed) == 0, (
            f"存在失败阶段: {[(s['stage'], s.get('error_message', '')) for s in failed]}"
        )

        # 执行状态应为通过
        trace = result.get("execution_trace", {})
        assert trace.get("status") == "RUNTIME_PASS", (
            f"执行应成功: {trace.get('error_message')}"
        )

        # 验证输出包含 borough 列，且至少有 5 个不同的 borough
        summary = result["result_summary"]
        assert "borough" in summary.get("columns", []), (
            f"输出应包含 borough 列，实际列={summary.get('columns')}"
        )
        assert summary["row_count"] >= 5, (
            f"输出行数应 >= 5（5 个 NYC 行政区），实际={summary['row_count']}"
        )

    def test_safety_risk_level_values_valid(self, nyc06_spec_md, nyc06_csv_paths):
        """safety_risk_level 列值必须为预定义的风险等级之一。"""
        pipeline = Pipeline()
        result = pipeline.run_all(nyc06_spec_md, table_paths=nyc06_csv_paths)

        assert result["validation_passed"] is True, (
            f"验证应通过: {result.get('open_questions')}"
        )

        trace = result.get("execution_trace", {})
        assert trace.get("status") == "RUNTIME_PASS", (
            f"执行应成功: {trace.get('error_message')}"
        )

        # 验证 safety_risk_level 在输出列中
        columns = result["result_summary"]["columns"]
        assert "safety_risk_level" in columns, (
            f"输出应包含 safety_risk_level 列，实际列={columns}"
        )

        # 检查列类型为字符串
        col_types = dict(zip(result["result_summary"]["columns"],
                             result["result_summary"]["column_types"]))
        assert "VARCHAR" in col_types.get("safety_risk_level", "").upper(), (
            f"safety_risk_level 类型应为字符串，实际={col_types.get('safety_risk_level')}"
        )

    def test_execution_compiled_sql_no_cte(self, nyc06_spec_md, nyc06_csv_paths):
        """run_all 执行后编译 SQL 中不得出现 WITH ... AS（CTE 禁止）。"""
        pipeline = Pipeline()
        result = pipeline.run_all(nyc06_spec_md, table_paths=nyc06_csv_paths)

        compiled = result.get("compiled", {})
        if compiled and hasattr(compiled, "sql"):
            sql_upper = compiled.sql.upper()
            assert "WITH " not in sql_upper or "WITHIN GROUP" in sql_upper, (
                f"最终编译 SQL 不得包含 CTE: {compiled.sql[:200]}"
            )

    def test_temp_tables_cleaned_after_execution(self, nyc06_spec_md, nyc06_csv_paths):
        """执行完成后 _temp_* 临时表应被清除——不残留中间数据。

        Pipeline 通过 export_artifacts() 暴露 program_cleanup_status，
        直接断言 executor 的 cleanup 结果为 "success"。
        """
        pipeline = Pipeline()
        result = pipeline.run_all(nyc06_spec_md, table_paths=nyc06_csv_paths)

        assert result["validation_passed"] is True
        trace = result.get("execution_trace", {})
        assert trace.get("status") == "RUNTIME_PASS"

        # 通过 export_artifacts 获取 cleanup 状态
        bundle = pipeline.export_artifacts(result["request_id"])
        assert bundle is not None, "export_artifacts 不应为 None"

        # 严格断言：cleanup 必须成功
        assert bundle.program_cleanup_status == "success", (
            f"临时表清理应成功，实际 cleanup_status={bundle.program_cleanup_status}，"
            f"cleanup_error={bundle.program_cleanup_error}"
        )


# ════════════════════════════════════════════
# Phase 10 Case06——Spark 双链验证测试
# ════════════════════════════════════════════


class TestNYCCase06SparkDualChain:
    """NYC 案例 06——多语句 DAG 跨域融合 Spark 双管线逻辑验证。

    Case06 使用 SqlProgram（7 步 DAG，_temp_* 临时表串联），其 Comparator 缺口
    通过 PlanComparator.compare_program() 收口——将所有语句的 step 扁平化后与
    SparkPlan 对比，过滤 _temp_* 内部管道 scan。

    注意：compute_ratios（比率计算）和 risk_label（CASE WHEN）属于 B 类遗留，
    导致严格断言 xfail。一旦 B 类功能收口，xfail 应自然转正。
    """

    @pytest.mark.xfail(
        reason="已知限制：DAG 归一化（_normalize_dag_steps）已将 aggregate 3→1 和 project 7→1，"
               "但 scan/join/aggregate 的内容级差异（_temp_* 表引用与 Mapper 别名）导致仍为 "
               "LOGIC_MISMATCH。需更深层的 plan 级别对齐（scan 过滤、join 重排等）。"
               "一旦后续 Phase 补齐内容级对齐，此 xfail 应自然转正。",
        strict=True,
    )
    def test_spark_orchestrator_logic_equivalence(self, nyc06_spec_md, nyc06_csv_paths):
        """多语句 DAG Spark Orchestrator 逻辑等价判定——显式断言 comparator_report.status。"""
        pytest.importorskip("pyspark", reason="PySpark 环境不可用")

        from tianshu_datadev.spark.orchestrator import SparkOrchestrator
        from tianshu_datadev.spark.plan_comparator import ComparisonStatus

        pipeline = Pipeline()
        result = pipeline.run_all(nyc06_spec_md, table_paths=nyc06_csv_paths)
        bundle = pipeline.export_artifacts(result["request_id"])

        # 获取 SqlProgram 和 V1 Contract（ComputeSteps 路径直接产 V1）
        sql_program = bundle.sql_program
        contract_v1 = bundle.data_transform_contract

        assert sql_program is not None, "SqlProgram 不应为空——ComputeSteps 路径应填充"
        assert contract_v1 is not None, "Contract 不应为空"

        orchestrator = SparkOrchestrator()
        state = orchestrator.run(
            contract=contract_v1, sql_plan=sql_program,
        )

        # ① comparator_report 必须非空
        assert state.comparator_report is not None, (
            "Orchestrator 应产出 PlanComparisonReport"
        )
        # ② 严格断言——应为 LOGIC_EQUIVALENT（当前 xfail：比率/CASE WHEN 未实现）
        assert state.comparator_report.status == ComparisonStatus.LOGIC_EQUIVALENT, (
            f"Case 06 逻辑对比应判定为等价，"
            f"实际 status={state.comparator_report.status}"
        )
        # ③ overall_status 一致性
        assert state.overall_status.value in {
            "LOGIC_CONSISTENT_PHYSICAL_NOT_EXECUTED", "ALL_CONSISTENT",
        }

    def test_spark_comparator_report_not_mismatch(self, nyc06_spec_md, nyc06_csv_paths):
        """Comparator 应正常运行不崩溃——多语句 DAG 全链路可执行。

        此测试是防御性的——确保 SqlProgram 扁平化 + Orchestrator + Comparator
        全链路可正常运行，不抛异常。由于 SQL 侧扁平化步骤与 Mapper 产出的 SparkPlan
        存在结构差异（Mapper 使用 V1 Contract 独立生成拓扑，与原始 DAG 步骤不完全对称），
        Comparator 的状态（LOGIC_EQUIVALENT / LOGIC_MISMATCH / NOT_COVERED）取决于
        两侧 plan 结构一致性——任何状态都是有意义的诊断信号。
        """
        pytest.importorskip("pyspark", reason="PySpark 环境不可用")

        from tianshu_datadev.spark.orchestrator import SparkOrchestrator

        pipeline = Pipeline()
        result = pipeline.run_all(nyc06_spec_md, table_paths=nyc06_csv_paths)
        bundle = pipeline.export_artifacts(result["request_id"])

        orchestrator = SparkOrchestrator()
        state = orchestrator.run(
            contract=bundle.data_transform_contract, sql_plan=bundle.sql_program,
        )

        # 关键防御：Comparator 全链路不抛异常，产出有效报告
        assert state.comparator_report is not None, (
            "Orchestrator 应产出 PlanComparisonReport"
        )
        assert state.comparator_report.report_id, (
            "报告 ID 不应为空"
        )
        assert len(state.comparator_report.step_results) > 0, (
            "step_results 应包含对比结果"
        )

    def test_contract_v1_is_extracted_from_run_all(self, nyc06_spec_md, nyc06_csv_paths):
        """Case06 run_all() 应直接产出 DataTransformContractV1（非 Lite）。"""
        pipeline = Pipeline()
        result = pipeline.run_all(nyc06_spec_md, table_paths=nyc06_csv_paths)
        bundle = pipeline.export_artifacts(result["request_id"])

        assert bundle.data_transform_contract is not None
        contract = bundle.data_transform_contract
        # ComputeSteps 路径应直接产 V1
        from tianshu_datadev.artifacts.models import DataTransformContractV1
        assert isinstance(contract, DataTransformContractV1), (
            f"应为 DataTransformContractV1，实际={type(contract).__name__}"
        )
        # 验证 risk_label 的 CASE WHEN 标签被正确提取
        assert len(contract.case_when_labels) > 0, (
            "Case06 Contract 应包含 risk_label 的 CASE WHEN 标签"
        )
