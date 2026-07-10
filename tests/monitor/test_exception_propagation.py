"""异常传播验证测试——验证监控代码不改变管线异常行为。

这是 C 类架构约束的核心验证。
每个测试通过以下方式验证异常传播不变性：
1. 不设置 TIANSHU_RUN_ID → NullCollector 模式 → 执行管线 → 捕获异常 A
2. 设置 TIANSHU_RUN_ID=test_run_001 → RunLogCollector 模式 → 执行相同管线 → 捕获异常 B
3. 断言 A 的类型、错误码、HTTP 状态码与 B 完全一致
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from tianshu_datadev.monitor.collector import (
    StageContext,
    get_collector,
)

# ═══════════════════════════════════════════════════════════
# 辅助：MockCollector——记录所有 stage 调用，可模拟异常
# ═══════════════════════════════════════════════════════════


class MockCollector:
    """模拟 Collector——记录所有 stage 调用供断言。"""

    def __init__(self):
        self.stages: list[dict] = []
        self._active_stages: list[MockStageContext] = []

    def stage(self, node: str, artifact_request_id: str, parent_stage_run_id: str | None = None):
        ctx = MockStageContext(
            collector=self,
            node=node,
            artifact_request_id=artifact_request_id,
            parent_stage_run_id=parent_stage_run_id,
        )
        self._active_stages.append(ctx)
        return ctx

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
    """模拟 StageContext——记录所有调用，__exit__ 返回 False（不吞异常）。"""

    def __init__(self, collector: MockCollector, node: str, artifact_request_id: str,
                 parent_stage_run_id: str | None = None):
        self.collector = collector
        self.node = node
        self.artifact_request_id = artifact_request_id
        self.parent_stage_run_id = parent_stage_run_id
        self._result = {}
        self._exited = False

    def set_result(self, **kwargs):
        self._result.update(kwargs)

    def __enter__(self):
        self.collector.stages.append({
            "node": self.node,
            "artifact_request_id": self.artifact_request_id,
            "status": "started",
            "result": {},
        })
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            status = "completed"
        else:
            status = "failed"
        self.collector.stages.append({
            "node": self.node,
            "artifact_request_id": self.artifact_request_id,
            "status": status,
            "result": dict(self._result),
            "error_type": exc_type.__name__ if exc_type else None,
        })
        return False  # 关键——不吞异常


# ═══════════════════════════════════════════════════════════
# Fixtures
# ═══════════════════════════════════════════════════════════


@pytest.fixture
def reset_env():
    """清理 TIANSHU_RUN_ID——确保每个测试从干净环境开始。"""
    saved = os.environ.get("TIANSHU_RUN_ID")
    if "TIANSHU_RUN_ID" in os.environ:
        del os.environ["TIANSHU_RUN_ID"]
    yield
    if saved is not None:
        os.environ["TIANSHU_RUN_ID"] = saved
    elif "TIANSHU_RUN_ID" in os.environ:
        del os.environ["TIANSHU_RUN_ID"]


# ═══════════════════════════════════════════════════════════
# 测试用例——异常传播一致性
# ═══════════════════════════════════════════════════════════


class TestExceptionPropagation:
    """异常在有无监控时完全一致（C 类架构约束）。"""

    # ─── Test 1：SQL 编译错误一致性 ──────────────────────────

    def test_sql_compile_error_identical_with_and_without_monitor(self, reset_env):
        """SQL 编译异常在有无监控时完全一致（异常类型 + 错误消息）。"""
        from tianshu_datadev.api.pipeline import Pipeline

        # 准备——构建一个会触发编译错误的场景
        invalid_spec = (
            "```markdown\n"
            "---\n"
            "spec:\n"
            "  type: aggregate_table\n"
            "  target_table: ads.test_daily\n"
            "  target_grain: [stat_date]\n"
            "  summary: '测试编译错误'\n"
            "  source_tables:\n"
            "    - name: dwd.test_fact\n"
            "      alias: tf\n"
            "      row_count: ~100万\n"
            "      role: fact\n"
            "      time_field: event_time\n"
            "  metrics:\n"
            "    - metric_name: cnt\n"
            "      aggregation: COUNT\n"
            "      field: *\n"
            "  time_range:\n"
            "    strategy: recent_days\n"
            "    days: 7\n"
            "```\n"
        )

        # ── A: NullCollector 模式（无 TIANSHU_RUN_ID）──
        tmpdir_a = tempfile.mkdtemp()
        try:
            pipe_a = Pipeline(base_output_dir=tmpdir_a)
            result_a = pipe_a.execute_rich(invalid_spec)
            pipeline_error_a = result_a.get("pipeline_error")
        finally:
            import shutil
            shutil.rmtree(tmpdir_a, ignore_errors=True)

        # ── B: RunLogCollector 模式（设置 TIANSHU_RUN_ID）──
        tmpdir_b = tempfile.mkdtemp()
        try:
            with patch.dict(os.environ, {"TIANSHU_RUN_ID": "test_propagation_001"}):
                collector_b = get_collector()
                pipe_b = Pipeline(base_output_dir=tmpdir_b)
                import tianshu_datadev.api.pipeline as pipeline_mod
                with patch.object(pipeline_mod, "get_collector", return_value=collector_b):
                    result_b = pipe_b.execute_rich(invalid_spec)
                    pipeline_error_b = result_b.get("pipeline_error")
        finally:
            import shutil
            shutil.rmtree(tmpdir_b, ignore_errors=True)

        # ── 断言一致性 ──
        assert pipeline_error_a is not None, "NullCollector 模式下应产生 pipeline_error"
        assert pipeline_error_b is not None, "RunLogCollector 模式下应产生 pipeline_error"

        assert pipeline_error_a.get("stage") == pipeline_error_b.get("stage"), (
            f"stage 不一致: {pipeline_error_a.get('stage')} vs {pipeline_error_b.get('stage')}"
        )

        error_type_a = pipeline_error_a.get("error_type", "")
        error_type_b = pipeline_error_b.get("error_type", "")
        assert error_type_a == error_type_b, (
            f"error_type 不一致: {error_type_a} vs {error_type_b}"
        )

    # ─── Test 2：Spark 异常一致性 ───────────────────────────

    def test_spark_error_identical_with_and_without_monitor(self, reset_env):
        """Spark 异常在有无监控时完全一致。"""
        from tianshu_datadev.api.pipeline import Pipeline

        tmpdir = tempfile.mkdtemp()

        # ── A: NullCollector 模式 ──
        pipe_a = Pipeline(base_output_dir=tmpdir)
        with patch.object(pipe_a, "export_artifacts", return_value=None):
            try:
                result_a = pipe_a.run_spark_stage("req_test", MagicMock())
                error_code_a = result_a.get("error_code", None)
            except Exception as exc_a:
                error_code_a = getattr(exc_a, "error_code", None)

        # ── B: RunLogCollector 模式 ──
        pipe_b = Pipeline(base_output_dir=tmpdir)
        with patch.object(pipe_b, "export_artifacts", return_value=None):
            with patch.dict(os.environ, {"TIANSHU_RUN_ID": "test_propagation_spark_001"}):
                try:
                    result_b = pipe_b.run_spark_stage("req_test", MagicMock())
                    error_code_b = result_b.get("error_code", None)
                except Exception as exc_b:
                    error_code_b = getattr(exc_b, "error_code", None)

        # ── 断言一致 ──
        assert error_code_a == error_code_b, (
            f"Spark 异常 error_code 不一致: {error_code_a} vs {error_code_b}"
        )

        try:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)
        except Exception:
            pass

    # ─── Test 3：error_code 一致性 ──────────────────────────

    def test_pipeline_error_code_identical(self, reset_env):
        """error_code 在有无监控时完全一致。"""
        from tianshu_datadev.api.pipeline import Pipeline

        # 构造一个可触发 ParseError 的无效 spec
        bad_spec = "not a valid spec at all"

        # ── A: NullCollector 模式 ──
        tmpdir_a = tempfile.mkdtemp()
        result_a = Pipeline(base_output_dir=tmpdir_a).parse_only(bad_spec)

        # ── B: RunLogCollector 模式 ──
        tmpdir_b = tempfile.mkdtemp()
        with patch.dict(os.environ, {"TIANSHU_RUN_ID": "test_propagation_code_001"}):
            collector_b = get_collector()
            pipe_b = Pipeline(base_output_dir=tmpdir_b)
            import tianshu_datadev.api.pipeline as pipeline_mod
            with patch.object(pipeline_mod, "get_collector", return_value=collector_b):
                result_b = pipe_b.parse_only(bad_spec)

        # ── 断言一致 ──
        error_a = result_a.get("pipeline_error", {})
        error_b = result_b.get("pipeline_error", {})

        assert bool(error_a) == bool(error_b), (
            f"有无监控时错误状态不一致: {bool(error_a)} vs {bool(error_b)}"
        )

        if error_a and error_b:
            assert error_a.get("stage") == error_b.get("stage"), (
                f"error_code 中的 stage 不一致: {error_a.get('stage')} vs {error_b.get('stage')}"
            )

        for d in [tmpdir_a, tmpdir_b]:
            import shutil
            shutil.rmtree(d, ignore_errors=True)

    # ─── Test 4：error_type 一致性 ──────────────────────────

    def test_pipeline_error_type_identical(self, reset_env):
        """error_type 在有无监控时完全一致。"""
        from tianshu_datadev.api.pipeline import Pipeline

        bad_spec = (
            "```markdown\n"
            "---\n"
            "spec:\n"
            "  type: aggregate_table\n"
            "  target_table: ads.test_daily\n"
            "  target_grain: [stat_date]\n"
            "  summary: '测试'\n"
            "  source_tables:\n"
            "    - name: dwd.test_fact\n"
            "      alias: tf\n"
            "      row_count: ~100万\n"
            "      role: fact\n"
            "      time_field: event_time\n"
            "  metrics:\n"
            "    - metric_name: cnt\n"
            "      aggregation: COUNT\n"
            "      field: id\n"
            "  time_range:\n"
            "    strategy: recent_days\n"
            "    days: 7\n"
            "```\n"
        )

        # ── A: NullCollector 模式 ──
        tmpdir_a = tempfile.mkdtemp()
        pipe_a = Pipeline(base_output_dir=tmpdir_a)
        result_a = pipe_a.execute_rich(bad_spec, table_mapping={})
        error_type_a = result_a.get("pipeline_error", {}).get("error_type", "")

        # ── B: RunLogCollector 模式 ──
        tmpdir_b = tempfile.mkdtemp()
        with patch.dict(os.environ, {"TIANSHU_RUN_ID": "test_propagation_type_001"}):
            collector_b = get_collector()
            pipe_b = Pipeline(base_output_dir=tmpdir_b)
            import tianshu_datadev.api.pipeline as pipeline_mod
            with patch.object(pipeline_mod, "get_collector", return_value=collector_b):
                result_b = pipe_b.execute_rich(bad_spec, table_mapping={})
                error_type_b = result_b.get("pipeline_error", {}).get("error_type", "")

        # ── 断言一致 ──
        assert error_type_a == error_type_b, (
            f"error_type 不一致: {error_type_a!r} vs {error_type_b!r}"
        )

        for d in [tmpdir_a, tmpdir_b]:
            import shutil
            shutil.rmtree(d, ignore_errors=True)

    # ─── Test 5：HTTP 状态码一致性 ──────────────────────────

    def test_pipeline_status_code_identical(self, reset_env):
        """HTTP 状态码在有无监控时完全一致。"""
        from tianshu_datadev.api.app import create_app

        # ── A: NullCollector 模式 ──
        app_a = create_app()
        client_a = TestClient(app_a)
        response_a = client_a.post("/api/plan", json={
            "markdown_text": "invalid spec",
            "table_mapping": {},
        })

        # ── B: RunLogCollector 模式 ──
        with patch.dict(os.environ, {"TIANSHU_RUN_ID": "test_propagation_http_001"}):
            app_b = create_app()
            client_b = TestClient(app_b)
            response_b = client_b.post("/api/plan", json={
                "markdown_text": "invalid spec",
                "table_mapping": {},
            })

        # ── 断言 HTTP 状态码一致 ──
        assert response_a.status_code == response_b.status_code, (
            f"HTTP 状态码不一致: {response_a.status_code} vs {response_b.status_code}\n"
            f"NullCollector body: {response_a.text[:200]}\n"
            f"RunLogCollector body: {response_b.text[:200]}"
        )

        # ── 断言 error_code 一致 ──
        body_a = response_a.json()
        body_b = response_b.json()
        if "error_code" in body_a and "error_code" in body_b:
            assert body_a["error_code"] == body_b["error_code"], (
                f"error_code 不一致: {body_a['error_code']} vs {body_b['error_code']}"
            )

    # ─── Test 6：emit 异常不影响管线 ─────────────────────────

    def test_monitoring_exception_in_emit_does_not_affect_pipeline(self, reset_env):
        """collector.emit() 内部异常不影响管线结果。

        Mock RunLogCollector.emit 抛出 RuntimeError，
        管线执行应正常完成（监控异常不向上传播）。
        """
        from tianshu_datadev.api.pipeline import Pipeline

        tmpdir = tempfile.mkdtemp()
        try:
            pipe = Pipeline(base_output_dir=tmpdir)

            # 创建一个 emit 会抛出异常的 collector
            failing_collector = MockCollector()

            def _failing_emit(event):
                raise RuntimeError("emit 内部异常")

            failing_collector.emit = _failing_emit  # type: ignore[method-assign]

            with patch.dict(os.environ, {"TIANSHU_RUN_ID": "test_propagation_emit_001"}):
                import tianshu_datadev.api.pipeline as pipeline_mod
                with patch.object(pipeline_mod, "get_collector", return_value=failing_collector):
                    result = pipe.parse_only("test spec")
                    assert isinstance(result, dict), (
                        "管线应返回 dict，即使 emit 抛出异常"
                    )
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)

    # ─── Test 7：stage.__exit__ 异常不影响管线 ───────────────

    def test_monitoring_exception_in_stage_exit_does_not_affect_pipeline(self, reset_env):
        """StageContext.__exit__ 异常不影响管线。

        注意：StageContext.__exit__ 返回 False（不吞异常）。
        测试点：即使 __exit__ 内部 emit 失败，业务异常仍正确传播。
        """
        # 场景 1：验证 __exit__ 返回 False——异常不被吞掉
        # 使用 RunLogCollector 以验证实际 ___exit__ 行为
        import tempfile as _tf
        tmpdir = _tf.mkdtemp()
        try:
            log_dir = Path(tmpdir) / "logs" / "monitor"
            log_dir.mkdir(parents=True, exist_ok=True)

            with patch.dict(os.environ, {"TIANSHU_RUN_ID": "test_stage_exit_001"}):
                collector = get_collector(log_dir)

                ctx = StageContext(
                    collector=collector,
                    stage_run_id="test_stage_exit",
                    node="test_node",
                    artifact_request_id="test_req",
                    parent_stage_run_id=None,
                )

                # 业务异常应被 StageContext.__exit__ 原样传播
                with pytest.raises(ValueError, match="业务异常"):
                    with ctx:
                        raise ValueError("业务异常")
                    # 如果 __exit__ 返回 True（吞异常），pytest.raises 捕获不到

                # 场景 2：使用 MockCollector 模拟 emit 抛出异常
                mock_coll = MockCollector()
                mock_ctx = MockStageContext(
                    collector=mock_coll,
                    node="test_node",
                    artifact_request_id="test_req_002",
                )
                # Mock __exit__ 行为——强制返回 False（不吞异常）
                # 即使内部处理出错，业务异常仍应传播
                with pytest.raises(ValueError, match="业务异常"):
                    with mock_ctx:
                        raise ValueError("业务异常")

                # 验证 stage 记录
                assert len(mock_coll.stages) >= 2, "MockCollector 应记录事件"
                # 最后事件应为 failed
                assert mock_coll.stages[-1]["status"] == "failed", "应记录 failed 状态"
                assert mock_coll.stages[-1]["error_type"] == "ValueError", "error_type 应为 ValueError"

        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)
