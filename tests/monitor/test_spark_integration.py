"""测试 Spark 管线与 monitor collector 集成——验证正确埋点行为。

Batch 5 — Spark 管线埋点（注入 collector.stage() 监控埋点）。
使用 mock collector 验证 orchestrator 和 run_spark_stage 在正确节点抛出 stage 事件。
"""

import uuid
from unittest.mock import MagicMock, patch

import pytest

from tianshu_datadev.spark.orchestrator import SparkOrchestrator, SparkPipelineStage

# ═══════════════════════════════════════════════════════════
# 辅助：MockCollector——轻量替代 RunLogCollector，避免文件 IO
# ═══════════════════════════════════════════════════════════


class MockCollector:
    """模拟 Collector——记录所有 stage 调用供断言。"""

    def __init__(self):
        self.stages: list[dict] = []

    def stage(
        self,
        node: str,
        artifact_request_id: str,
        parent_stage_run_id: str | None = None,
    ):
        return MockStageContext(
            collector=self,
            node=node,
            artifact_request_id=artifact_request_id,
            parent_stage_run_id=parent_stage_run_id,
        )

    def emit(self, event):
        pass

    def log_resource_sample(self, sample):
        pass

    def log_browser_event(self, payload):
        pass

    def flush(self, timeout=5.0):
        return True

    def close(self):
        pass


class MockStageContext:
    """模拟 StageContext——记录 start/completed/failed 事件供断言。"""

    def __init__(
        self,
        collector: MockCollector,
        node: str,
        artifact_request_id: str,
        parent_stage_run_id: str | None = None,
    ):
        self.collector = collector
        self.node = node
        self.artifact_request_id = artifact_request_id
        self.parent_stage_run_id = parent_stage_run_id
        self.stage_run_id = f"mock_{node}_{uuid.uuid4().hex[:8]}"
        self._result: dict = {}

    def set_result(self, **kwargs):
        self._result.update(kwargs)

    def __enter__(self):
        self.collector.stages.append({
            "node": self.node,
            "artifact_request_id": self.artifact_request_id,
            "parent_stage_run_id": self.parent_stage_run_id,
            "stage_run_id": self.stage_run_id,
            "status": "started",
            "result": {},
        })
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        status = "failed" if exc_type is not None else "completed"
        self.collector.stages.append({
            "node": self.node,
            "artifact_request_id": self.artifact_request_id,
            "parent_stage_run_id": self.parent_stage_run_id,
            "stage_run_id": self.stage_run_id,
            "status": status,
            "result": dict(self._result),
            "error_type": exc_type.__name__ if exc_type else None,
        })
        return False  # 不吞异常


# ═══════════════════════════════════════════════════════════
# Fixtures
# ═══════════════════════════════════════════════════════════


@pytest.fixture
def mock_collector():
    """返回 MockCollector 实例——直接注入 orchestrator，不 patch。"""
    return MockCollector()


@pytest.fixture
def orchestrator():
    """创建 SparkOrchestrator 实例。"""
    return SparkOrchestrator()


# ═══════════════════════════════════════════════════════════
# 辅助方法
# ═══════════════════════════════════════════════════════════


def _collect_nodes(events: list[dict]) -> list[str]:
    """从 stage 事件列表中提取 node 唯一序列（去重 same node 的 started/completed）。"""
    seen: set[str] = set()
    nodes: list[str] = []
    for ev in events:
        n = ev["node"]
        if n not in seen:
            seen.add(n)
            nodes.append(n)
    return nodes


# ═══════════════════════════════════════════════════════════
# 测试用例
# ═══════════════════════════════════════════════════════════


class TestSparkOrchestratorMonitoring:
    """验证 SparkOrchestrator.run() 在正确节点抛出 stage 事件。"""

    # ─── Test 1：全链路 6 子节点 + 父节点 ────────────────────

    def test_spark_verify_records_all_six_sub_stages(
        self, orchestrator, mock_collector,
    ):
        """全链路 verify 记录 6 个子节点 + 父节点 spark_verify。"""
        mock_collector.stages.clear()

        orchestrator.run(
            contract_hash="test-hash",
            stage_failures={"MAPPER": "模拟映射失败"},
            collector=mock_collector,
        )

        nodes = _collect_nodes(mock_collector.stages)
        expected = [
            "spark_verify",        # 父节点——包裹整个 Pipeline
            "spark_mapper",        # MAPPER（stage_failures 注入 → FAILURE）
            "spark_developer",     # DEVELOPER（无 service → SKIPPED）
            "spark_compiler",      # COMPILER（无 cached_plan → SKIPPED）
            "spark_validator",     # VALIDATOR（无 compile_result → SKIPPED）
            "spark_comparator",    # COMPARATOR（无 sql_plan/spark_plan → SKIPPED）
            "spark_physical_verifier",  # PHYSICAL_VERIFIER（始终 SKIPPED）
        ]
        for n in expected:
            assert n in nodes, f"缺少节点 {n}，实际节点：{nodes}"
        assert len(nodes) == len(expected), (
            f"应有 {len(expected)} 个节点，实际 {len(nodes)}：{nodes}"
        )

    # ─── Test 2：子节点 parent_stage_run_id ─────────────────

    def test_sub_stages_have_parent_stage_run_id(
        self, orchestrator, mock_collector,
    ):
        """子节点 parent_stage_run_id = spark_verify 的 stage_run_id。"""
        mock_collector.stages.clear()

        orchestrator.run(
            contract_hash="test-hash",
            stage_failures={"MAPPER": "模拟映射失败"},
            collector=mock_collector,
        )

        # 找到 spark_verify 的 stage_run_id
        verify_start = None
        for ev in mock_collector.stages:
            if ev["node"] == "spark_verify" and ev["status"] == "started":
                verify_start = ev
                break
        assert verify_start is not None, "缺少 spark_verify started 事件"
        verify_run_id = verify_start["stage_run_id"]

        # 检查所有子节点 started 事件的 parent_stage_run_id
        sub_nodes = {
            "spark_mapper", "spark_developer", "spark_compiler",
            "spark_validator", "spark_comparator", "spark_physical_verifier",
        }
        for ev in mock_collector.stages:
            if ev["node"] in sub_nodes and ev["status"] == "started":
                assert ev["parent_stage_run_id"] == verify_run_id, (
                    f"{ev['node']} 的 parent_stage_run_id 应为 "
                    f"{verify_run_id}，实际为 {ev['parent_stage_run_id']}"
                )

    # ─── Test 3：run_spark_stage 单阶段调用 ────────────────

    def test_run_spark_stage_records_single_stage(self, mock_collector):
        """单阶段调用记录独立节点（parent=null，独立调用不经过 run() 全链路包裹）。"""
        import tempfile

        from tianshu_datadev.api.pipeline import Pipeline

        tmpdir = tempfile.mkdtemp()
        try:
            p = Pipeline(base_output_dir=tmpdir)

            # Mock 内部方法——避免真实执行
            mock_ctx = MagicMock()
            mock_ctx.stage_results = {}
            mock_ctx.errors = []
            mock_ctx.spark_plan = None
            mock_ctx.compile_result = None
            mock_ctx.comparator_report = None
            mock_ctx.annotation_result = None
            mock_ctx.standalone_pyspark = None
            mock_ctx.sandbox_transform_code = None

            mock_artifacts = MagicMock()
            mock_artifacts.data_transform_contract = MagicMock()
            mock_artifacts.sql_build_plan = MagicMock()
            mock_artifacts.sql_program = None

            p.export_artifacts = MagicMock(return_value=mock_artifacts)
            p._get_or_create_spark_context = MagicMock(return_value=mock_ctx)
            p._check_stage_dependencies = MagicMock()
            p._do_spark_map = MagicMock()

            with patch(
                "tianshu_datadev.api.pipeline.get_collector",
                return_value=mock_collector,
            ):
                result = p.run_spark_stage("req_test", SparkPipelineStage.MAPPER)

            # 验证 event 记录
            mapper_starts = [
                ev for ev in mock_collector.stages
                if ev["node"] == "spark_mapper" and ev["status"] == "started"
            ]
            assert len(mapper_starts) >= 1, "缺少 spark_mapper started 事件"
            # parent=null——独立调用不经过 run() 全链路
            assert mapper_starts[0]["parent_stage_run_id"] is None, (
                "独立调用时 parent_stage_run_id 应为 None"
            )
            # 不应有 spark_verify 节点
            verify_nodes = [
                ev for ev in mock_collector.stages
                if ev["node"] == "spark_verify"
            ]
            assert len(verify_nodes) == 0, "独立调用不应有 spark_verify 节点"

            # 验证响应结构（至少应含 request_id 和 stage）
            assert result["request_id"] == "req_test"
            assert result["stage"] == "MAPPER"
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)

    # ─── Test 4：COMPILER artifact_path ─────────────────────

    def test_spark_compile_records_artifact_path(
        self, orchestrator, mock_collector,
    ):
        """COMPILER 完成后记录 artifact_path。"""
        mock_collector.stages.clear()

        # 补丁：让 MAPPER 成功（设置 cached_plan 和 spark_plan_hash）
        def _succeed_mapper(stage, state, contract):
            state.record_stage_result(stage, "SUCCESS")
            state.spark_plan_hash = "plan_abc123"
            orchestrator._cached_plan = MagicMock()  # COMPILER 依赖 cached_plan

        # 补丁：让 COMPILER 成功（设置 compiled_code_sha256）
        def _succeed_compiler(stage, state):
            state.record_stage_result(stage, "SUCCESS")
            state.compiled_code_sha256 = "compiled_def456"

        with patch.object(orchestrator, "_run_mapper", side_effect=_succeed_mapper):
            with patch.object(orchestrator, "_run_compiler", side_effect=_succeed_compiler):
                orchestrator.run(
                    contract="dummy",  # 非 None——让 MAPPER 不提前 SKIPPED
                    collector=mock_collector,
                )

        # 验证 COMPILER 的 completed 事件包含 artifact_path
        compiler_completed = [
            ev for ev in mock_collector.stages
            if ev["node"] == "spark_compiler" and ev["status"] == "completed"
        ]
        assert len(compiler_completed) >= 1, "缺少 spark_compiler completed 事件"
        assert compiler_completed[-1]["result"].get("artifact_path") == "compiled/compiled_def456", (
            f"artifact_path 应为 compiled/compiled_def456，"
            f"实际为 {compiler_completed[-1]['result'].get('artifact_path')}"
        )

    # ─── Test 5：PHYSICAL_VERIFIER row_count ──────────────

    def test_spark_physical_verify_records_row_count(
        self, orchestrator, mock_collector,
    ):
        """PHYSICAL_VERIFIER 完成后 row_count 正确（当前始终 SKIPPED）。"""
        mock_collector.stages.clear()

        orchestrator.run(
            contract_hash="test-hash",
            collector=mock_collector,
        )

        # PHYSICAL_VERIFIER 始终 SKIPPED，所以不应有 artifact_path/row_count
        pv_completed = [
            ev for ev in mock_collector.stages
            if ev["node"] == "spark_physical_verifier"
            and ev["status"] == "completed"
        ]
        assert len(pv_completed) >= 1, "缺少 spark_physical_verifier completed 事件"
        # SKIPPED 阶段没有 set_result 调用
        assert "artifact_path" not in pv_completed[-1]["result"], (
            "SKIPPED 阶段不应有 artifact_path"
        )
        assert "row_count" not in pv_completed[-1]["result"], (
            "SKIPPED 阶段不应有 row_count"
        )

    # ─── Test 6：节点失败记录 failed + error_type ──────────

    def test_spark_failure_records_stage_failed(
        self, orchestrator, mock_collector,
    ):
        """节点失败（stage_failures 注入）记录 failed + error_type。"""
        mock_collector.stages.clear()

        orchestrator.run(
            contract_hash="test-hash",
            stage_failures={"MAPPER": "模拟映射失败"},
            collector=mock_collector,
        )

        # 找到 MAPPER 的 completed/failed 事件
        mapper_events = [
            ev for ev in mock_collector.stages
            if ev["node"] == "spark_mapper"
        ]
        # 检查是否有 failed 事件
        mapper_failed = [
            ev for ev in mapper_events if ev["status"] == "failed"
        ]
        assert len(mapper_failed) >= 1, "MAPPER 应记录 failed 事件"
        # error_type 不应为 None
        assert mapper_failed[-1]["error_type"] is not None, (
            f"failed 事件应有 error_type，实际为 {mapper_failed[-1]}"
        )

    # ─── Test 7：retry 产生新 stage_run_id ─────────────────

    def test_spark_retry_is_new_stage_run_id(
        self, orchestrator, mock_collector,
    ):
        """retry_count>0 产生新的独立 stage_run_id。"""
        mock_collector.stages.clear()

        # 第一次执行（retry=0）
        orchestrator.run(
            contract_hash="test-hash",
            stage_failures={"MAPPER": "模拟映射失败"},
            collector=mock_collector,
        )
        first_verify_start = [
            ev for ev in mock_collector.stages
            if ev["node"] == "spark_verify" and ev["status"] == "started"
        ]
        assert len(first_verify_start) >= 1
        first_run_id = first_verify_start[0]["stage_run_id"]

        # 第二次执行（retry=1）——用新的 MockCollector
        mock_collector2 = MockCollector()
        orchestrator.run(
            contract_hash="test-hash",
            stage_failures={"MAPPER": "模拟映射失败"},
            retry_count=1,
            collector=mock_collector2,
        )
        second_verify_start = [
            ev for ev in mock_collector2.stages
            if ev["node"] == "spark_verify" and ev["status"] == "started"
        ]
        assert len(second_verify_start) >= 1
        second_run_id = second_verify_start[0]["stage_run_id"]

        # 两次 stage_run_id 应不同
        assert first_run_id != second_run_id, (
            f"retry=0 和 retry=1 的 stage_run_id 应不同：{first_run_id} vs {second_run_id}"
        )

    # ─── Test 8：NullCollector ─────────────────────────────

    def test_null_collector_spark_stage_noop(self, orchestrator):
        """NullCollector 模式下 Spark 行为不变（不崩溃、返回有效状态）。"""
        # 不传 collector → 默认为 NullCollector
        state = orchestrator.run(
            contract_hash="test-hash",
            stage_failures={"MAPPER": "模拟映射失败"},
        )

        # 行为不变——MAPPER 应为 FAILURE
        assert state.stage_results["MAPPER"] == "FAILURE", (
            f"NullCollector 模式下 MAPPER 应为 FAILURE，实际为 {state.stage_results['MAPPER']}"
        )
        # 应返回有效的 SparkPipelineState
        assert state.contract_hash == "test-hash"
        assert len(state.stage_results) == 6
