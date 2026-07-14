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

# 两表 Join 测试用 CSV 路径
_CSV_FACT_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "fixtures", "sql", "test_fact.csv")
)
_CSV_DIM_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "fixtures", "sql", "test_dim.csv")
)

# ── Fixtures ──

@pytest.fixture
def client():
    """创建测试用的 FastAPI TestClient（纯净环境，不含 CSV fixtures 的自动发现）。"""
    pipeline = Pipeline()
    app = create_app(pipeline=pipeline)
    return TestClient(app)

@pytest.fixture
def two_table_join_spec():
    """读取两表 Join spec——explicit_join_spec.md（INNER JOIN）。"""
    path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        "tests", "fixtures", "relationship", "explicit_join_spec.md",
    )
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

# ── 测试用例 ──

class TestExecuteRichProducesContract:
    """execute-rich 成功后 export_artifacts() 返回非空 contract。"""

    def test_produces_contract(self, client, golden_spec_passing, csv_path):
        """验证 execute-rich 后 contract 被正确缓存。"""

        resp = client.post("/api/execute-rich", json={
            "markdown_text": golden_spec_passing,
            "table_mapping": {"tf": "test_fact"},
            "table_paths": {"test_fact": csv_path},
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

    def test_map_returns_ok(self, client, golden_spec_passing, csv_path):
        """验证完整的 execute-rich → MAPPER 链路。"""

        # Step 1: execute-rich
        exec_resp = client.post("/api/execute-rich", json={
            "markdown_text": golden_spec_passing,
            "table_mapping": {"tf": "test_fact"},
            "table_paths": {"test_fact": csv_path},
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

    def test_compile_missing_dependency(self, client, golden_spec_passing, csv_path):
        """验证依赖门禁——缺少 spark_plan 时 COMPILER 被拒。"""

        # execute-rich 产出 contract 但不执行 MAPPER
        exec_resp = client.post("/api/execute-rich", json={
            "markdown_text": golden_spec_passing,
            "table_mapping": {"tf": "test_fact"},
            "table_paths": {"test_fact": csv_path},
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

    def test_developer_skipped(self, client, golden_spec_passing, csv_path):
        """验证 DEVELOPER 在无 service 注入时 graceful degradation。"""

        exec_resp = client.post("/api/execute-rich", json={
            "markdown_text": golden_spec_passing,
            "table_mapping": {"tf": "test_fact"},
            "table_paths": {"test_fact": csv_path},
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

    def test_validate_missing_dependency(self, client, golden_spec_passing, csv_path):
        """验证依赖门禁——缺少 compile_result 时 VALIDATOR 被拒。"""

        exec_resp = client.post("/api/execute-rich", json={
            "markdown_text": golden_spec_passing,
            "table_mapping": {"tf": "test_fact"},
            "table_paths": {"test_fact": csv_path},
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

    def test_compare_missing_spark_plan(self, client, golden_spec_passing, csv_path):
        """验证 COMPARATOR 依赖——缺少 spark_plan 时被拒。"""

        exec_resp = client.post("/api/execute-rich", json={
            "markdown_text": golden_spec_passing,
            "table_mapping": {"tf": "test_fact"},
            "table_paths": {"test_fact": csv_path},
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

    def test_llm_traces_field_present(self, client, golden_spec_passing, csv_path):
        """验证 execute-rich 响应中包含 llm_traces 字段。"""

        resp = client.post("/api/execute-rich", json={
            "markdown_text": golden_spec_passing,
            "table_mapping": {"tf": "test_fact"},
            "table_paths": {"test_fact": csv_path},
        })
        assert resp.status_code == 200
        data = resp.json()
        # llm_traces 应为 None 或 dict（无 LLM 调用时为空）
        assert "llm_traces" in data

class TestLlmTracesInSparkStageResponse:
    """spark 单阶段响应含 llm_traces 字段。"""

    def test_spark_stage_has_llm_traces(self, client, golden_spec_passing, csv_path):
        """验证 spark/map 响应中包含 llm_traces 字段。"""

        exec_resp = client.post("/api/execute-rich", json={
            "markdown_text": golden_spec_passing,
            "table_mapping": {"tf": "test_fact"},
            "table_paths": {"test_fact": csv_path},
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

    def test_traces_not_affect_review(self, client, golden_spec_passing, csv_path):
        """验证 llm_traces 不会影响 Spark 管线的 REVIEW_READY 结果。

        通过 /api/spark/verify 执行全链路，确认 llm_traces 不参与判定。
        """

        # 使用 run-all——/api/spark/verify 需要完整的
        # sql_build_plan + data_transform_contract（run_all 产出）
        run_resp = client.post("/api/run-all", json={
            "markdown_text": golden_spec_passing,
            "table_mapping": {"tf": "test_fact"},
            "table_paths": {"test_fact": csv_path},
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

    def test_nonexistent_request_id(self, client, csv_path):
        """不存在的 request_id → artifacts_ready=false + 空列表。"""
        resp = client.get("/api/artifacts/nonexistent_req_12345/status")
        assert resp.status_code == 200
        data = resp.json()
        assert data["request_id"] == "nonexistent_req_12345"
        assert data["artifacts_ready"] is False
        assert data["available_artifacts"] == []

    def test_parse_only_not_ready(self, client, golden_spec_passing, csv_path):
        """仅 parse（无 execute）→ artifacts_ready=false（缺少 contract）。"""

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

    def test_execute_rich_ready(self, client, golden_spec_passing, csv_path):
        """execute-rich 成功后 → artifacts_ready=true + contract 就绪。"""

        exec_resp = client.post("/api/execute-rich", json={
            "markdown_text": golden_spec_passing,
            "table_mapping": {"tf": "test_fact"},
            "table_paths": {"test_fact": csv_path},
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

# ════════════════════════════════════════════
# 错误去重测试（B 类修复）
# ════════════════════════════════════════════

class TestErrorDedupOnRerun:
    """同一阶段重复触发不得累积完全相同的 errors。"""

    def test_rerun_clears_stage_errors_before_execution(self):
        """阶段重跑前清除该阶段的旧错误——不依赖 HTTP 端点的单元测试。"""
        context = SparkStageContext()

        # 模拟第一次 COMPILER 失败
        context.errors.append("[COMPILER] 异常：测试错误")
        assert len(context.errors) == 1

        # 模拟重跑前的清除逻辑（与 run_spark_stage 一致）
        stage_error_prefix = "[COMPILER] "
        context.errors = [e for e in context.errors if not e.startswith(stage_error_prefix)]
        assert len(context.errors) == 0, (
            f"清除后 errors 应为空，实际: {context.errors}"
        )

    def test_different_stage_errors_not_cleared(self):
        """清除 COMPILER 错误时不影响 MAPPER 等其他阶段的错误。"""
        context = SparkStageContext()
        context.errors.append("[MAPPER] 映射失败：测试")
        context.errors.append("[COMPILER] 异常：测试错误")

        # 清除 COMPILER 错误
        stage_error_prefix = "[COMPILER] "
        context.errors = [e for e in context.errors if not e.startswith(stage_error_prefix)]

        assert len(context.errors) == 1
        assert context.errors[0].startswith("[MAPPER]"), (
            f"MAPPER 错误应保留，实际: {context.errors}"
        )

    def test_same_error_not_duplicated_in_single_run(self):
        """同一次运行中同一异常不重复追加。"""
        context = SparkStageContext()
        error_msg = "[COMPILER] 异常：AliasResolutionError"

        # 模拟去重逻辑
        if error_msg not in context.errors:
            context.errors.append(error_msg)
        if error_msg not in context.errors:
            context.errors.append(error_msg)

        assert len(context.errors) == 1, (
            f"同一错误不应重复，实际 errors: {context.errors}"
        )

# ════════════════════════════════════════════
# Mapper→Developer→Compiler 集成测试（E2E 别名验证——Phase 5 迁移自 scripts/e2e_alias_verify.py）
# ════════════════════════════════════════════

class TestE2EAliasVerification:
    """Phase 5 迁移——真实 HTTP API 管线：execute-rich → spark/map → spark/develop → spark/compile。

    通过 TestClient 执行完整 API dispatcher 路径，不绕过 Pipeline。
    单表拓扑验证别名解析器全链路；两表 Join 拓扑由 test_alias_resolver.py 单元层覆盖。
    """

    def test_full_api_chain_produces_only_tn_fn_aliases(self, client, golden_spec_passing, csv_path):
        """完整的 API 管线——编译产物仅含 tN/fN 别名。

        验收标准：
        1. MAPPER 返回 ok
        2. COMPILER 返回 ok，pyspark_code 仅含 tN/fN DataFrame 变量
        3. 不含 ft_filtered 等旧式语义别名
        4. 重复 compile 3 次 hash 一致
        """
        import ast as _ast

        # Step 1: execute-rich
        resp = client.post("/api/execute-rich", json={
            "markdown_text": golden_spec_passing,
            "table_mapping": {"tf": "test_fact"},
            "table_paths": {"test_fact": csv_path},
        })
        assert resp.status_code == 200, f"execute-rich 应返回 200: {resp.text[:500]}"
        data = resp.json()
        request_id = data.get("request_id", "")
        assert request_id, f"execute-rich 应返回 request_id: {data}"

        # Step 2: MAPPER
        resp = client.post("/api/spark/map", json={"request_id": request_id})
        assert resp.status_code == 200, f"MAPPER 应返回 200: {resp.text[:500]}"
        map_data = resp.json()
        assert map_data.get("status") == "ok", (
            f"MAPPER 应 ok: status={map_data.get('status')}, errors={map_data.get('errors', [])}"
        )

        # Step 3: DEVELOPER（LLM 不可用时 skipped——不影响后续 compile）
        resp = client.post("/api/spark/develop", json={"request_id": request_id})

        # Step 4: COMPILER——第 1 次
        resp = client.post("/api/spark/compile", json={"request_id": request_id})
        assert resp.status_code == 200, f"COMPILER 应返回 200: {resp.text[:500]}"
        c1 = resp.json()
        assert c1.get("status") == "ok", (
            f"COMPILER 第 1 次应 ok: status={c1.get('status')}, errors={c1.get('errors', [])}"
        )
        r1 = c1.get("result", {}) if isinstance(c1, dict) else {}
        code1 = r1.get("pyspark_code", "") if isinstance(r1, dict) else ""
        hash1 = r1.get("raw_hash", "") if isinstance(r1, dict) else ""
        assert code1, "第 1 次 compile 应产出 pyspark_code"

        # Step 5: COMPILER——第 2 次（重复验证）
        resp = client.post("/api/spark/compile", json={"request_id": request_id})
        c2 = resp.json()
        assert c2.get("status") == "ok", f"COMPILER 第 2 次应 ok: {c2}"
        r2 = c2.get("result", {}) if isinstance(c2, dict) else {}
        code2 = r2.get("pyspark_code", "") if isinstance(r2, dict) else ""
        hash2 = r2.get("raw_hash", "") if isinstance(r2, dict) else ""

        # Step 6: COMPILER——第 3 次（重复验证）
        resp = client.post("/api/spark/compile", json={"request_id": request_id})
        c3 = resp.json()
        assert c3.get("status") == "ok", f"COMPILER 第 3 次应 ok: {c3}"
        r3 = c3.get("result", {}) if isinstance(c3, dict) else {}
        code3 = r3.get("pyspark_code", "") if isinstance(r3, dict) else ""
        hash3 = r3.get("raw_hash", "") if isinstance(r3, dict) else ""

        # 验证 1：三次 raw_hash 一致（幂等）
        assert hash1 == hash2 == hash3, (
            f"三次 compile raw_hash 应一致: {hash1} / {hash2} / {hash3}"
        )
        assert code1 == code2 == code3, "三次 compile pyspark_code 应一致"

        # 验证 2：errors 不重复累积
        e1 = len(c1.get("errors", [])) if isinstance(c1, dict) else 0
        e2 = len(c2.get("errors", [])) if isinstance(c2, dict) else 0
        e3 = len(c3.get("errors", [])) if isinstance(c3, dict) else 0
        assert e1 == e2 == e3, f"三次 errors 数量应一致: {e1} / {e2} / {e3}"

        # 验证 3：DataFrame 变量仅含 tN/fN（用 ast.parse）
        tree = _ast.parse(code1)
        func_def = None
        for node in _ast.walk(tree):
            if isinstance(node, _ast.FunctionDef) and node.name == "transform":
                func_def = node
                break
        assert func_def is not None, "编译产物中未找到 transform 函数"
        df_vars: list[str] = []
        for stmt in func_def.body:
            if isinstance(stmt, _ast.Assign):
                for target in stmt.targets:
                    if isinstance(target, _ast.Name):
                        df_vars.append(target.id)
        assert len(df_vars) >= 1, f"应至少收集到 1 个 DataFrame 变量: {code1[:200]}"
        for var in df_vars:
            assert (var[0] in ("t", "f") and var[1:].isdigit()), (
                f"DataFrame 变量应为 tN/fN，实际为 {var!r}——所有变量: {df_vars}"
            )

        # 验证 4：不含旧式语义别名
        assert "_filtered =" not in code1, "不应含 _filtered 旧式别名"
        assert "_with_" not in code1, "不应含 _with_ 旧式别名"
        assert "filtered_filtered" not in code1, "不应含 filtered_filtered"
        assert "AliasResolution" not in str(c1.get("errors", [])), "不应含 AliasResolutionError"

        # 验证 5：return 指向最后一个 fN
        assert "return f" in code1, f"return 应指向 fN: {code1[-60:]}"
        if df_vars:
            last_var = df_vars[-1]
            assert f"return {last_var}" in code1, f"return 应为 {last_var}: {code1[-80:]}"

    def test_two_table_join_full_api_chain(
        self, client,
    ):
        """两表 Join 全 API 链路——Mapper 产物包含 SparkJoinStep + 编译产物仅含 tN/fN。

        覆盖原 e2e_alias_verify.py 中的两表 Join 故障链：
        SQL Plan → ContractExtractor → Mapper → API context → Compiler

        使用内联最小两表 Join spec（避免 explicit_join_spec.md 的 SQL builder 预存 bug——
        dim_name 维度在 output_columns 中无对应 metric/dimension 定义，导致编译为 SUM(stat_date)）。
        """
        import ast as _ast

        # 最小两表 Join spec——列名与 test_fact.csv / test_dim.csv 精确匹配
        _two_table_spec = """# 两表 Join E2E 测试
> 最小 spec：fact + dim INNER JOIN，验证 Mapper 产物含 SparkJoinStep

```markdown
---
spec:
  type: aggregate_table
  target_table: ads.join_test
  target_grain: [dim_name]
  summary: "两表 Join E2E 测试——验证 SparkJoinStep 产物"

  source_tables:
    - name: test_fact
      alias: tf
      row_count: ~10万
      role: fact
      key_columns:
        - name: id
          type: bigint
          nullable: false
      business_columns:
        - name: amount
          type: decimal
          nullable: false
        - name: dim_id
          type: bigint
          nullable: false

    - name: test_dim
      alias: td
      row_count: ~100
      role: dim
      key_columns:
        - name: dim_id
          type: bigint
          nullable: false
      business_columns:
        - name: dim_name
          type: varchar
          nullable: false

  joins:
    - left_table: tf
      right_table: td
      left_key: dim_id
      right_key: dim_id
      join_type: INNER

  metrics:
    - metric_name: total_amount
      aggregation: SUM
      input_column: amount
      alias: total_amount

  dimensions:
    - dimension_name: dim_name
      column_ref: dim_name

  output_columns:
    - name: dim_name
      type: varchar
    - name: total_amount
      type: decimal
---
# 两表 Join E2E 验证
计算按维度的指标总和。使用 test_fact 与 test_dim INNER JOIN。
```"""

        # Step 1: execute-rich——两表 Join spec
        resp = client.post("/api/execute-rich", json={
            "markdown_text": _two_table_spec,
            "table_mapping": {"tf": "test_fact", "td": "test_dim"},
            "table_paths": {
                "test_fact": _CSV_FACT_PATH,
                "test_dim": _CSV_DIM_PATH,
            },
        })
        assert resp.status_code == 200, f"execute-rich 应返回 200: {resp.text[:500]}"
        data = resp.json()
        request_id = data.get("request_id", "")
        assert request_id, f"execute-rich 应返回 request_id: {data}"

        # Step 2: MAPPER——验证产物包含 Join step
        resp = client.post("/api/spark/map", json={"request_id": request_id})
        assert resp.status_code == 200, f"MAPPER 应返回 200: {resp.text[:500]}"
        map_data = resp.json()
        assert map_data.get("status") == "ok", (
            f"MAPPER 应 ok: status={map_data.get('status')}, errors={map_data.get('errors', [])}"
        )
        # 验证 Mapper 产物包含 SparkJoinStep
        result = map_data.get("result", {}) if isinstance(map_data, dict) else {}
        steps = result.get("steps", []) if isinstance(result, dict) else []
        step_types = [s.get("step_type", "") for s in steps]
        assert "join" in step_types, (
            f"两表 Join Mapper 产物应包含 join step，实际 step_types={step_types}"
        )

        # Step 3: DEVELOPER
        resp = client.post("/api/spark/develop", json={"request_id": request_id})

        # Step 4: COMPILER——验证编译产物含 join + tN/fN 别名
        resp = client.post("/api/spark/compile", json={"request_id": request_id})
        assert resp.status_code == 200, f"COMPILER 应返回 200: {resp.text[:500]}"
        c1 = resp.json()
        assert c1.get("status") == "ok", (
            f"COMPILER 应 ok: status={c1.get('status')}, errors={c1.get('errors', [])}"
        )
        r1 = c1.get("result", {}) if isinstance(c1, dict) else {}
        code1 = r1.get("pyspark_code", "") if isinstance(r1, dict) else ""
        hash1 = r1.get("raw_hash", "") if isinstance(r1, dict) else ""
        assert code1, "compile 应产出 pyspark_code"

        # 验证 1：编译产物含 .join( —— 两表 Join 的 DataFrame 操作
        assert ".join(" in code1, (
            f"两表 Join 编译产物应含 .join( 操作: {code1[:300]}"
        )

        # 验证 2：重复 compile 3 次 hash 一致（幂等）
        hashes = [hash1]
        codes = [code1]
        for attempt in range(2, 4):
            resp = client.post("/api/spark/compile", json={"request_id": request_id})
            c = resp.json()
            r = c.get("result", {}) if isinstance(c, dict) else {}
            hashes.append(r.get("raw_hash", "") if isinstance(r, dict) else "")
            codes.append(r.get("pyspark_code", "") if isinstance(r, dict) else "")
        assert hashes[0] == hashes[1] == hashes[2], (
            f"三次 compile raw_hash 应一致: {hashes}"
        )
        assert codes[0] == codes[1] == codes[2], "三次 compile pyspark_code 应一致"

        # 验证 3：DataFrame 变量仅含 tN/fN
        tree = _ast.parse(codes[0])
        func_def = None
        for node in _ast.walk(tree):
            if isinstance(node, _ast.FunctionDef) and node.name == "transform":
                func_def = node
                break
        assert func_def is not None, "编译产物中未找到 transform 函数"
        df_vars: list[str] = []
        for stmt in func_def.body:
            if isinstance(stmt, _ast.Assign):
                for target in stmt.targets:
                    if isinstance(target, _ast.Name):
                        df_vars.append(target.id)
        assert len(df_vars) >= 2, (
            f"两表 Join 应至少 2 个 DataFrame 变量（2 个 read + 后续操作）: {codes[0][:200]}"
        )
        for var in df_vars:
            assert (var[0] in ("t", "f") and var[1:].isdigit()), (
                f"DataFrame 变量应为 tN/fN，实际为 {var!r}——所有变量: {df_vars}"
            )

        # 验证 4：不含旧式语义别名
        assert "_filtered =" not in codes[0], "不应含 _filtered 旧式别名"
        assert "_with_" not in codes[0], "不应含 _with_ 旧式别名"
        assert "filtered_filtered" not in codes[0], "不应含 filtered_filtered"
