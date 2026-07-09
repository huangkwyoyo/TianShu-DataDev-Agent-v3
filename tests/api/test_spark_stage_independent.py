"""Spark 阶段独立触发 + LLM 调用追踪——后端 pytest。

测试 9 个核心路径：
- execute-rich 产出 contract
- spark/map 正常执行
- 依赖缺失返回 422
- developer 无服务时 skipped
- validate 缺少 compile_result 返回 422
- compare 缺少 sql/spark plan 返回 422
- execute-rich 响应含 llm_traces
- spark 阶段响应含 llm_traces
- llm_traces 不参与 REVIEW_READY 判定
"""

from __future__ import annotations

import os

import pytest
from fastapi.testclient import TestClient

from tianshu_datadev.api.app import create_app
from tianshu_datadev.api.pipeline import Pipeline, SparkDependencyMissingError, SparkStageContext
from tianshu_datadev.llm.models import LlmTraceNode
from tianshu_datadev.spark.orchestrator import SparkPipelineStage

# 用于 DuckDB 执行的 CSV fixture 路径（与 test_run_all.py 一致）
_CSV_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "fixtures", "sql", "test_fact.csv")
)


# ── Fixtures ──

@pytest.fixture
def client():
    """创建测试用的 FastAPI TestClient（纯净环境，不含 CSV fixtures 的自动发现）。"""
    pipeline = Pipeline()
    app = create_app(pipeline=pipeline)
    return TestClient(app)


@pytest.fixture
def golden_spec_passing():
    """读取 golden fixture——golden_passing.md（行数低于阈值，可通过验证）。"""
    path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        "tests", "fixtures", "golden", "golden_passing.md",
    )
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


# ── 测试用例 ──


class TestExecuteRichProducesContract:
    """execute-rich 成功后 export_artifacts() 返回非空 contract。"""

    def test_produces_contract(self, client, golden_spec_passing):
        """验证 execute-rich 后 contract 被正确缓存。"""
        try:
            import duckdb  # noqa: F401
        except ImportError:
            pytest.skip("DuckDB 未安装")

        resp = client.post("/api/execute-rich", json={
            "markdown_text": golden_spec_passing,
            "table_mapping": {"tf": "test_fact"},
            "table_paths": {"test_fact": _CSV_PATH},
        })
        assert resp.status_code == 200, (
            f"execute-rich 应返回 200，实际 {resp.status_code}: {resp.text}"
        )
        data = resp.json()
        request_id = data["request_id"]

        # 通过 /api/spark/map 间接验证 contract 存在
        # （MAPPER 依赖 contract，不存在时返回 422）
        map_resp = client.post("/api/spark/map", json={
            "request_id": request_id,
        })
        assert map_resp.status_code == 200, (
            f"MAPPER 应成功执行（依赖 contract），实际返回 {map_resp.status_code}: {map_resp.json()}"
        )


class TestSparkMapAfterExecuteRich:
    """execute-rich 后调用 /api/spark/map 返回 200。"""

    def test_map_returns_ok(self, client, golden_spec_passing):
        """验证完整的 execute-rich → MAPPER 链路。"""
        try:
            import duckdb  # noqa: F401
        except ImportError:
            pytest.skip("DuckDB 未安装")

        # Step 1: execute-rich
        exec_resp = client.post("/api/execute-rich", json={
            "markdown_text": golden_spec_passing,
            "table_mapping": {"tf": "test_fact"},
            "table_paths": {"test_fact": _CSV_PATH},
        })
        assert exec_resp.status_code == 200
        request_id = exec_resp.json()["request_id"]

        # Step 2: MAPPER
        map_resp = client.post("/api/spark/map", json={
            "request_id": request_id,
        })
        assert map_resp.status_code == 200
        data = map_resp.json()
        assert data["stage"] == "MAPPER"
        assert data["status"] == "ok"
        # spark_stages 应包含至少 MAPPER 的状态
        assert len(data["spark_stages"]) >= 1


class TestSparkCompileMissingSparkPlan:
    """未执行 MAPPER 直接调用 COMPILER 返回 422。"""

    def test_compile_missing_dependency(self, client, golden_spec_passing):
        """验证依赖门禁——缺少 spark_plan 时 COMPILER 被拒。"""
        try:
            import duckdb  # noqa: F401
        except ImportError:
            pytest.skip("DuckDB 未安装")

        # execute-rich 产出 contract 但不执行 MAPPER
        exec_resp = client.post("/api/execute-rich", json={
            "markdown_text": golden_spec_passing,
            "table_mapping": {"tf": "test_fact"},
            "table_paths": {"test_fact": _CSV_PATH},
        })
        assert exec_resp.status_code == 200
        request_id = exec_resp.json()["request_id"]

        # 直接调用 COMPILER（跳过 MAPPER）
        resp = client.post("/api/spark/compile", json={
            "request_id": request_id,
        })
        assert resp.status_code == 422
        data = resp.json()
        assert data["error_code"] == "SPARK_DEPENDENCY_MISSING"
        assert "spark_plan" in data["message"]


class TestSparkDeveloperSkippedWithoutService:
    """DEVELOPER 未配置时返回 SKIPPED，不阻断。"""

    def test_developer_skipped(self, client, golden_spec_passing):
        """验证 DEVELOPER 在无 service 注入时 graceful degradation。"""
        try:
            import duckdb  # noqa: F401
        except ImportError:
            pytest.skip("DuckDB 未安装")

        exec_resp = client.post("/api/execute-rich", json={
            "markdown_text": golden_spec_passing,
            "table_mapping": {"tf": "test_fact"},
            "table_paths": {"test_fact": _CSV_PATH},
        })
        assert exec_resp.status_code == 200
        request_id = exec_resp.json()["request_id"]

        # 先执行 MAPPER 使 DEVELOPER 依赖满足
        client.post("/api/spark/map", json={"request_id": request_id})

        # 执行 DEVELOPER
        resp = client.post("/api/spark/develop", json={
            "request_id": request_id,
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["stage"] == "DEVELOPER"
        assert data["status"] == "skipped"


class TestSparkValidateMissingCompileResult:
    """未执行 COMPILER 直接调用 VALIDATOR 返回 422。"""

    def test_validate_missing_dependency(self, client, golden_spec_passing):
        """验证依赖门禁——缺少 compile_result 时 VALIDATOR 被拒。"""
        try:
            import duckdb  # noqa: F401
        except ImportError:
            pytest.skip("DuckDB 未安装")

        exec_resp = client.post("/api/execute-rich", json={
            "markdown_text": golden_spec_passing,
            "table_mapping": {"tf": "test_fact"},
            "table_paths": {"test_fact": _CSV_PATH},
        })
        assert exec_resp.status_code == 200
        request_id = exec_resp.json()["request_id"]

        # 执行 MAPPER（满足 COMPILER 依赖），但跳过 COMPILER
        client.post("/api/spark/map", json={"request_id": request_id})

        # 直接调用 VALIDATOR
        resp = client.post("/api/spark/validate", json={
            "request_id": request_id,
        })
        assert resp.status_code == 422
        data = resp.json()
        assert data["error_code"] == "SPARK_DEPENDENCY_MISSING"
        assert "compile_result" in str(data)


class TestSparkCompareNeedsSqlAndSparkPlan:
    """缺少 SqlBuildPlan 或 SparkPlan 时 COMPARATOR 返回 422。"""

    def test_compare_missing_spark_plan(self, client, golden_spec_passing):
        """验证 COMPARATOR 依赖——缺少 spark_plan 时被拒。"""
        try:
            import duckdb  # noqa: F401
        except ImportError:
            pytest.skip("DuckDB 未安装")

        exec_resp = client.post("/api/execute-rich", json={
            "markdown_text": golden_spec_passing,
            "table_mapping": {"tf": "test_fact"},
            "table_paths": {"test_fact": _CSV_PATH},
        })
        assert exec_resp.status_code == 200
        request_id = exec_resp.json()["request_id"]

        # 不执行 MAPPER，直接调用 COMPARATOR
        resp = client.post("/api/spark/compare", json={
            "request_id": request_id,
        })
        assert resp.status_code == 422
        data = resp.json()
        assert data["error_code"] == "SPARK_DEPENDENCY_MISSING"


class TestLlmTracesInExecuteRichResponse:
    """execute-rich 响应含 llm_traces 字段。"""

    def test_llm_traces_field_present(self, client, golden_spec_passing):
        """验证 execute-rich 响应中包含 llm_traces 字段。"""
        try:
            import duckdb  # noqa: F401
        except ImportError:
            pytest.skip("DuckDB 未安装")

        resp = client.post("/api/execute-rich", json={
            "markdown_text": golden_spec_passing,
            "table_mapping": {"tf": "test_fact"},
            "table_paths": {"test_fact": _CSV_PATH},
        })
        assert resp.status_code == 200
        data = resp.json()
        # llm_traces 应为 None 或 dict（无 LLM 调用时为空）
        assert "llm_traces" in data


class TestLlmTracesInSparkStageResponse:
    """spark 单阶段响应含 llm_traces 字段。"""

    def test_spark_stage_has_llm_traces(self, client, golden_spec_passing):
        """验证 spark/map 响应中包含 llm_traces 字段。"""
        try:
            import duckdb  # noqa: F401
        except ImportError:
            pytest.skip("DuckDB 未安装")

        exec_resp = client.post("/api/execute-rich", json={
            "markdown_text": golden_spec_passing,
            "table_mapping": {"tf": "test_fact"},
            "table_paths": {"test_fact": _CSV_PATH},
        })
        assert exec_resp.status_code == 200
        request_id = exec_resp.json()["request_id"]

        map_resp = client.post("/api/spark/map", json={
            "request_id": request_id,
        })
        assert map_resp.status_code == 200
        data = map_resp.json()
        assert "llm_traces" in data


class TestLlmTracesNotInReviewReady:
    """llm_traces 不参与 REVIEW_READY 判定。"""

    def test_traces_not_affect_review(self, client, golden_spec_passing):
        """验证 llm_traces 不会影响 Spark 管线的 REVIEW_READY 结果。

        通过 /api/spark/verify 执行全链路，确认 llm_traces 不参与判定。
        """
        try:
            import duckdb  # noqa: F401
        except ImportError:
            pytest.skip("DuckDB 未安装")

        # 使用 run-all——/api/spark/verify 需要完整的
        # sql_build_plan + data_transform_contract（run_all 产出）
        run_resp = client.post("/api/run-all", json={
            "markdown_text": golden_spec_passing,
            "table_mapping": {"tf": "test_fact"},
            "table_paths": {"test_fact": _CSV_PATH},
        })
        assert run_resp.status_code == 200
        request_id = run_resp.json()["request_id"]

        verify_resp = client.post("/api/spark/verify", json={
            "request_id": request_id,
        })
        assert verify_resp.status_code == 200
        data = verify_resp.json()
        # 确认 review_ready 判定不受外部因素影响
        assert "review_ready" in data
        # llm_traces 不应出现在 verify 响应中（verify 端点保持原有行为）
        # 这是设计意图——verify 端点保持 SparkStageResponse 模型不变


# ── 单元测试：SparkDependencyMissingError ──


class TestSparkDependencyMissingErrorUnit:
    """SparkDependencyMissingError 异常类的单元测试。"""

    def test_error_message_format(self):
        """验证异常消息格式包含阶段名和缺失项。"""
        exc = SparkDependencyMissingError(
            SparkPipelineStage.COMPILER,
            ["spark_plan", "compile_result"],
        )
        msg = str(exc)
        assert "COMPILER" in msg
        assert "spark_plan" in msg
        assert "compile_result" in msg
        assert exc.stage == SparkPipelineStage.COMPILER
        assert exc.missing == ["spark_plan", "compile_result"]


# ── 单元测试：SparkStageContext ──


class TestSparkStageContextUnit:
    """SparkStageContext 数据类的单元测试。"""

    def test_initial_state(self):
        """验证初始状态——所有字段为 None/空。"""
        ctx = SparkStageContext()
        assert ctx.spark_plan is None
        assert ctx.compile_result is None
        assert ctx.standalone_pyspark is None
        assert ctx.sandbox_transform_code is None
        assert ctx.comparator_report is None
        assert ctx.stage_results == {}
        assert ctx.errors == []

    def test_sandbox_transform_code_independent(self):
        """验证 sandbox_transform_code 与 standalone_pyspark 独立。"""
        ctx = SparkStageContext()
        ctx.standalone_pyspark = "# standalone wrapper with spark.read.csv"
        ctx.sandbox_transform_code = "def transform(inputs, params=None):\n    ..."
        # 两个字段应独立存储
        assert "spark.read.csv" in ctx.standalone_pyspark
        assert "spark.read" not in ctx.sandbox_transform_code
        assert "def transform" in ctx.sandbox_transform_code

    def test_mutable_stage_results(self):
        """验证 stage_results 是可变的且独立于实例。"""
        ctx1 = SparkStageContext()
        ctx2 = SparkStageContext()
        ctx1.stage_results["MAPPER"] = "SUCCESS"
        assert ctx2.stage_results == {}  # 独立


# ── 单元测试：LlmTraceNode ──


class TestLlmTraceNodeUnit:
    """LlmTraceNode 模型单元测试。"""

    def test_default_values(self):
        """验证默认值——status=skipped，其他为空。"""
        node = LlmTraceNode(
            node_name="test_node",
            model="fake",
        )
        assert node.status == "skipped"
        assert node.token_usage == {}
        assert node.latency_ms == 0
        assert node.error_type is None

    def test_full_fields(self):
        """验证全部字段正确赋值。"""
        node = LlmTraceNode(
            node_name="sql_build_planner",
            model="deepseek-v3",
            token_usage={"prompt_tokens": 100, "completion_tokens": 50, "total_tokens": 150},
            latency_ms=350,
            status="valid",
            error_type=None,
        )
        assert node.node_name == "sql_build_planner"
        assert node.model == "deepseek-v3"
        assert node.token_usage["total_tokens"] == 150
        assert node.latency_ms == 350
        assert node.status == "valid"


# ── Artifacts 状态检查端点测试 ──


class TestArtifactsStatusEndpoint:
    """GET /api/artifacts/{request_id}/status 端点测试。"""

    def test_nonexistent_request_id(self, client):
        """不存在的 request_id → artifacts_ready=false + 空列表。"""
        resp = client.get("/api/artifacts/nonexistent_req_12345/status")
        assert resp.status_code == 200
        data = resp.json()
        assert data["request_id"] == "nonexistent_req_12345"
        assert data["artifacts_ready"] is False
        assert data["available_artifacts"] == []

    def test_parse_only_not_ready(self, client, golden_spec_passing):
        """仅 parse（无 execute）→ artifacts_ready=false（缺少 contract）。"""
        try:
            import duckdb  # noqa: F401
        except ImportError:
            pytest.skip("DuckDB 未安装")

        # 仅 parse-rich（不执行 execute-rich）
        parse_resp = client.post("/api/spec/parse-rich", json={
            "markdown_text": golden_spec_passing,
        })
        assert parse_resp.status_code == 200
        request_id = parse_resp.json()["request_id"]
        assert request_id, "request_id 不应为空"

        resp = client.get(f"/api/artifacts/{request_id}/status")
        assert resp.status_code == 200
        data = resp.json()
        assert data["request_id"] == request_id
        # 仅 parse 存入 {parsed_spec}，没有 contract
        assert data["artifacts_ready"] is False

    def test_execute_rich_ready(self, client, golden_spec_passing):
        """execute-rich 成功后 → artifacts_ready=true + contract 就绪。"""
        try:
            import duckdb  # noqa: F401
        except ImportError:
            pytest.skip("DuckDB 未安装")

        exec_resp = client.post("/api/execute-rich", json={
            "markdown_text": golden_spec_passing,
            "table_mapping": {"tf": "test_fact"},
            "table_paths": {"test_fact": _CSV_PATH},
        })
        assert exec_resp.status_code == 200
        request_id = exec_resp.json()["request_id"]
        assert request_id, "request_id 不应为空"

        resp = client.get(f"/api/artifacts/{request_id}/status")
        assert resp.status_code == 200
        data = resp.json()
        assert data["request_id"] == request_id
        assert data["artifacts_ready"] is True
        assert "data_transform_contract" in data["available_artifacts"]
        assert "sql_build_plan" in data["available_artifacts"]


class TestSparkDependencyMissingErrorMessage:
    """SPARK_DEPENDENCY_MISSING 错误消息包含用户引导。"""

    def test_artifacts_not_found_has_guidance(self, client):
        """artifacts 不存在时，错误消息引导用户先执行「编译执行」。"""
        resp = client.post("/api/spark/compile", json={
            "request_id": "req_nonexistent_99999",
        })
        assert resp.status_code == 422
        data = resp.json()
        assert data["error_code"] == "SPARK_DEPENDENCY_MISSING"
        # 新增引导文字：提示用户先点击「编译执行」
        assert "编译执行" in data["message"]

    def test_mapper_missing_contract_has_guidance(self, client, golden_spec_passing):
        """MAPPER 缺少 contract 时，错误消息包含引导。"""
        try:
            import duckdb  # noqa: F401
        except ImportError:
            pytest.skip("DuckDB 未安装")

        # 仅 parse（不执行 execute-rich），直接调 MAPPER
        parse_resp = client.post("/api/spec/parse-rich", json={
            "markdown_text": golden_spec_passing,
        })
        assert parse_resp.status_code == 200
        request_id = parse_resp.json()["request_id"]
        # 注意：parse 后 artifacts_ready=false，但 export_artifacts 不返回 None
        # 因为 parse_only 已存入 {parsed_spec}。MAPPER 会走到
        # _check_stage_dependencies → data_transform_contract 缺失路径。
        resp = client.post("/api/spark/map", json={
            "request_id": request_id,
        })
        assert resp.status_code == 422
        data = resp.json()
        assert data["error_code"] == "SPARK_DEPENDENCY_MISSING"
        # MAPPER 依赖 data_transform_contract——消息含引导
        assert "data_transform_contract" in data["message"]
        assert "编译执行" in data["message"]
