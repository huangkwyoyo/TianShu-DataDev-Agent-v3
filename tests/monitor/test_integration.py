"""端到端集成测试——需要实际启动前后端。

所有测试标记为 slow + integration，默认被 pytest 跳过。
使用 --run-slow 选项启用。
"""

from __future__ import annotations

import json
import os
import tempfile
from unittest.mock import patch

import pytest

from tianshu_datadev.monitor.collector import NullCollector, RunLogCollector, get_collector

# 所有测试标记为 slow + integration
pytestmark = [
    pytest.mark.slow,
    pytest.mark.integration,
]


# ═══════════════════════════════════════════════════════════
# Fixtures
# ═══════════════════════════════════════════════════════════


@pytest.fixture
def reset_env():
    """清理 TIANSHU_RUN_ID 环境变量。"""
    saved = os.environ.get("TIANSHU_RUN_ID")
    if "TIANSHU_RUN_ID" in os.environ:
        del os.environ["TIANSHU_RUN_ID"]
    yield
    if saved is not None:
        os.environ["TIANSHU_RUN_ID"] = saved
    elif "TIANSHU_RUN_ID" in os.environ:
        del os.environ["TIANSHU_RUN_ID"]


# ═══════════════════════════════════════════════════════════
# 测试用例
# ═══════════════════════════════════════════════════════════


class TestIntegration:
    """端到端集成测试——监控系统的完整运行验证。"""

    # ─── Test 1：完整运行产出所有日志文件 ───────────────────

    def test_full_run_generates_all_log_files(self, tmp_path):
        """一次完整运行产出 5 个日志文件：

        - _events.jsonl
        - _collector_status.json
        - summary.json

        使用 RunLogCollector 直接模拟完整生命周期，不启动真实子进程。
        """
        log_dir = tmp_path / "logs" / "monitor"
        log_dir.mkdir(parents=True, exist_ok=True)

        run_id = "integration_test_001"

        with patch.dict(os.environ, {"TIANSHU_RUN_ID": run_id}):
            collector = get_collector(log_dir)
            assert isinstance(collector, RunLogCollector), "应有 RunLogCollector 实例"

            from tianshu_datadev.monitor.models import StageEvent
            collector.emit(StageEvent(
                run_id=run_id,
                stage_run_id="stage_test_001",
                node="test_node",
                artifact_request_id="test_req_001",
                status="started",
            ))

            collector.close()

        # 验证产出文件
        events_files = list(log_dir.glob(f"*{run_id}_events.jsonl"))
        status_files = list(log_dir.glob(f"*{run_id}_collector_status.json"))

        assert len(events_files) >= 1, "缺少 _events.jsonl 文件"
        assert len(status_files) >= 1, "缺少 _collector_status.json 文件"

        # 验证 events.jsonl 内容
        events_text = events_files[0].read_text(encoding="utf-8")
        assert len(events_text.strip()) > 0, "events.jsonl 不应为空"

        lines = events_text.strip().split("\n")
        assert len(lines) >= 1, "events.jsonl 应有至少 1 行"

        parsed = json.loads(lines[0])
        assert parsed.get("event_type") == "stage"
        assert parsed.get("run_id") == run_id

        # 验证 collector_status.json 内容
        status_text = status_files[0].read_text(encoding="utf-8")
        status_data = json.loads(status_text)
        assert status_data.get("run_id") == run_id
        assert status_data.get("run_complete") is True

    # ─── Test 2：events.jsonl 格式正确 ──────────────────────

    def test_events_jsonl_has_correct_schema(self, tmp_path):
        """events.jsonl 每行可解析为合法 JSON，含 event_type 字段。"""
        log_dir = tmp_path / "logs" / "monitor"
        log_dir.mkdir(parents=True, exist_ok=True)

        run_id = "integration_test_schema"

        with patch.dict(os.environ, {"TIANSHU_RUN_ID": run_id}):
            from tianshu_datadev.monitor.models import (
                HttpEvent,
                ProcessMetrics,
                ResourceSample,
                StageEvent,
            )

            collector = get_collector(log_dir)
            assert isinstance(collector, RunLogCollector)

            # 写入多个事件类型
            collector.emit(StageEvent(
                run_id=run_id,
                stage_run_id="stage_001",
                node="sql_parser",
                artifact_request_id="req_001",
                status="started",
            ))
            collector.emit(StageEvent(
                run_id=run_id,
                stage_run_id="stage_001",
                node="sql_parser",
                artifact_request_id="req_001",
                status="completed",
                duration_ms=100,
            ))
            collector.emit(HttpEvent(
                run_id=run_id,
                http_request_id="hreq_001",
                method="POST",
                path="/api/execute",
                status_code=200,
                duration_ms=50,
            ))
            pm = ProcessMetrics(
                pid=1, name="python", cpu_percent=10.0,
                rss_mb=100.0, vms_mb=200.0, num_threads=5,
            )
            collector.emit(ResourceSample(
                run_id=run_id,
                processes=[pm],
            ))

            collector.close()

        # 验证每行都是合法 JSON
        events_files = list(log_dir.glob(f"*{run_id}_events.jsonl"))
        assert len(events_files) >= 1

        events_text = events_files[0].read_text(encoding="utf-8")
        lines = events_text.strip().split("\n")
        assert len(lines) == 4, f"应有 4 行事件，实际 {len(lines)}"

        event_types_found = set()
        for line in lines:
            data = json.loads(line)
            assert "event_type" in data, "每行应有 event_type 字段"
            assert "run_id" in data, "每行应有 run_id 字段"
            assert "timestamp" in data, "每行应有 timestamp 字段"
            event_types_found.add(data["event_type"])

        assert "stage" in event_types_found
        assert "http" in event_types_found
        assert "resource" in event_types_found

    # ─── Test 3：summary.json 必填字段 ─────────────────────

    def test_summary_json_has_required_fields(self, tmp_path):
        """summary.json 含 dropped_event_count, flush_completed, run_complete。"""
        log_dir = tmp_path / "logs" / "monitor"
        log_dir.mkdir(parents=True, exist_ok=True)

        run_id = "integration_test_summary"

        with patch.dict(os.environ, {"TIANSHU_RUN_ID": run_id}):
            collector = get_collector(log_dir)
            assert isinstance(collector, RunLogCollector)

            # 模拟写入 summary.json
            summary = {
                "run_id": run_id,
                "dropped_event_count": collector.dropped_event_count,
                "flush_completed": collector.flush_completed,
                "run_complete": False,
                "backend_exit_code": 0,
                "frontend_exit_code": 0,
                "monitor_version": "3.0",
            }
            summary_path = log_dir / f"tianshu_run_{run_id}_summary.json"
            summary_path.write_text(
                json.dumps(summary, ensure_ascii=False), encoding="utf-8",
            )

            collector.close()

        summary_files = list(log_dir.glob(f"*{run_id}_summary.json"))
        assert len(summary_files) >= 1

        summary_data = json.loads(summary_files[0].read_text(encoding="utf-8"))

        assert "run_id" in summary_data
        assert summary_data["run_id"] == run_id
        assert "dropped_event_count" in summary_data
        assert "flush_completed" in summary_data
        assert "run_complete" in summary_data
        assert "backend_exit_code" in summary_data
        assert "frontend_exit_code" in summary_data
        assert "monitor_version" in summary_data
        assert summary_data["monitor_version"] == "3.0"

    # ─── Test 4：轮转保留 50 组 ──────────────────────────────

    def test_rotation_keeps_recent_50_groups(self, tmp_path):
        """轮转后保留最近 50 组日志（超过 50 组时旧组被删除）。"""
        log_dir = tmp_path / "logs" / "monitor"
        log_dir.mkdir(parents=True, exist_ok=True)

        # 创建 56 组日志文件（55 组 + 1 组当前运行，当前运行组不应被删除）
        # 注意：run_id 不能包含下划线（与 rotation 正则中的 [._] 分隔符冲突）
        n_groups = 55
        for i in range(1, n_groups + 1):
            rid = f"run-{i:04d}"
            for fname in [
                f"tianshu_run_{rid}_events.jsonl",
                f"tianshu_run_{rid}_collector_status.json",
            ]:
                (log_dir / fname).write_text("{}", encoding="utf-8")

        # 当前运行组（不应被删除）
        current_run_id = "run-current"
        for fname in [
            f"tianshu_run_{current_run_id}_events.jsonl",
            f"tianshu_run_{current_run_id}_collector_status.json",
        ]:
            (log_dir / fname).write_text("{}", encoding="utf-8")

        from tianshu_datadev.monitor.rotation import cleanup
        deleted_count = cleanup(log_dir, current_run_id, keep_groups=50)

        # 总计 56 组（55 + 1 current），keep=50 → 应删除前 6 组
        assert deleted_count == 6, f"应删除 6 组旧日志，实际删除 {deleted_count}"

        # 验证剩余文件数量：112 - 12 = 100
        remaining_files = list(log_dir.iterdir())
        assert len(remaining_files) == 100, (
            f"应有 100 个剩余文件，实际 {len(remaining_files)}"
        )

        # 验证当前运行组仍存在
        current_files = list(log_dir.glob(f"*{current_run_id}*"))
        assert len(current_files) == 2, (
            f"当前运行组应保留 2 个文件，实际 {len(current_files)}"
        )

        # 验证最早 6 组被删除
        for i in range(1, 7):
            rid = f"run-{i:04d}"
            files = list(log_dir.glob(f"*{rid}*"))
            assert len(files) == 0, f"第 {i} 组日志应被删除"

        # 验证最新 49 组仍在（索引 7-55，共 49 组）
        for i in range(7, 56):
            rid = f"run-{i:04d}"
            files = list(log_dir.glob(f"*{rid}*"))
            assert len(files) == 2, f"第 {i} 组日志应保留 2 个文件"

    # ─── Test 5：浏览器事件端到端 ──────────────────────────

    def test_browser_event_end_to_end(self, tmp_path):
        """通过 get_collector 模拟前后端上报到写入 JSONL 的流程。"""
        log_dir = tmp_path / "logs" / "monitor"
        log_dir.mkdir(parents=True, exist_ok=True)

        run_id = "integration_test_browser"
        env_vars = {
            "TIANSHU_RUN_ID": run_id,
            "TIANSHU_MONITOR_TOKEN": "test_token_abcdef123456",
        }

        with patch.dict(os.environ, env_vars):
            collector = get_collector(log_dir)
            assert isinstance(collector, RunLogCollector)

            # 模拟前端上报浏览器事件
            collector.log_browser_event({
                "event_type": "browser",
                "run_id": run_id,
                "api_path": "/api/execute",
                "api_status": 200,
                "api_duration_ms": 150,
                "error_type": None,
                "error_message": None,
            })

            collector.close()

        events_files = list(log_dir.glob(f"*{run_id}_events.jsonl"))
        assert len(events_files) >= 1

        events_text = events_files[0].read_text(encoding="utf-8")
        lines = events_text.strip().split("\n")
        events = [json.loads(line) for line in lines if line]

        browser_events = [e for e in events if e.get("event_type") == "browser"]
        assert len(browser_events) >= 1, "应至少有一个 browser 事件"

        be = browser_events[0]
        assert be["api_path"] == "/api/execute"
        assert be["api_status"] == 200
        assert be["api_duration_ms"] == 150
        assert be["run_id"] == run_id

    # ─── Test 6：NullCollector 在 CI 环境 ──────────────────

    def test_null_collector_in_ci(self, reset_env):
        """CI 环境（无 TIANSHU_RUN_ID）不影响现有测试。"""
        if "TIANSHU_RUN_ID" in os.environ:
            del os.environ["TIANSHU_RUN_ID"]

        collector = get_collector()
        assert isinstance(collector, NullCollector), (
            "无 TIANSHU_RUN_ID 时应返回 NullCollector"
        )
        assert collector.enabled is False
        assert collector.run_id == ""

        # 验证管线正常执行
        from tianshu_datadev.api.pipeline import Pipeline

        tmpdir = tempfile.mkdtemp()
        try:
            pipeline = Pipeline(base_output_dir=tmpdir)
            result = pipeline.parse_only("test spec")
            assert isinstance(result, dict)
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)
