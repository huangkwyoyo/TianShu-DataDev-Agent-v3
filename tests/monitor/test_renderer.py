"""测试监控文本渲染器——LogRenderer.format_event() 纯函数行为。"""

import os
from unittest.mock import patch

import pytest

# 目标函数尚不存在——测试先行，预期 ImportError
from tianshu_datadev.monitor.renderer import LogRenderer  # noqa: E402


class TestFormatStageStarted:
    """StageEvent started 格式测试。"""

    def test_started_basic_format(self):
        """started 事件输出含时间戳、级别 INFO、节点名、状态 STARTED。"""
        event = {
            "event_type": "stage",
            "run_id": "20260710-120000",
            "timestamp": "2026-07-10T12:00:00.123000+00:00",
            "node": "sql_parser",
            "status": "started",
            "stage_run_id": "stage_sql_parser_abc123",
            "artifact_request_id": None,
            "duration_ms": None,
            "error_type": None,
            "error_code": None,
            "error_message": None,
            "stack_frames": None,
            "artifact_path": None,
            "artifact_sha256": None,
            "row_count": None,
            "http_request_id": None,
            "parent_stage_run_id": None,
        }
        result = LogRenderer.format_event(event)
        assert result is not None
        assert "[INFO" in result
        assert "sql_parser" in result
        assert "STARTED" in result
        # started 阶段没有耗时
        assert "ms" not in result

    def test_started_truncates_long_node_name(self):
        """超长节点名不破坏对齐。"""
        event = {
            "event_type": "stage",
            "run_id": "20260710-120000",
            "timestamp": "2026-07-10T12:00:00.000000+00:00",
            "node": "spark_physical_verifier_with_extra_suffix",
            "status": "started",
            "stage_run_id": "s1",
            "artifact_request_id": None,
            "duration_ms": None,
            "error_type": None,
            "error_code": None,
            "error_message": None,
            "stack_frames": None,
            "artifact_path": None,
            "artifact_sha256": None,
            "row_count": None,
            "http_request_id": None,
            "parent_stage_run_id": None,
        }
        result = LogRenderer.format_event(event)
        assert result is not None
        assert "STARTED" in result


class TestFormatStageCompleted:
    """StageEvent completed 格式测试。"""

    def test_completed_basic_format(self):
        """completed 含 DONE 状态和耗时。"""
        event = {
            "event_type": "stage",
            "run_id": "20260710-120000",
            "timestamp": "2026-07-10T12:00:01.456000+00:00",
            "node": "sql_parser",
            "status": "completed",
            "stage_run_id": "stage_sql_parser_abc123",
            "artifact_request_id": None,
            "duration_ms": 1234,
            "error_type": None,
            "error_code": None,
            "error_message": None,
            "stack_frames": None,
            "artifact_path": None,
            "artifact_sha256": None,
            "row_count": None,
            "http_request_id": None,
            "parent_stage_run_id": None,
        }
        result = LogRenderer.format_event(event)
        assert result is not None
        assert "DONE" in result
        assert "1234ms" in result

    def test_completed_with_debug_fields(self):
        """completed 含 artifact_path/row_count 时输出 DEBUG 行。"""
        event = {
            "event_type": "stage",
            "run_id": "20260710-120000",
            "timestamp": "2026-07-10T12:00:01.456000+00:00",
            "node": "sql_executor",
            "status": "completed",
            "stage_run_id": "s1",
            "artifact_request_id": "req1",
            "duration_ms": 567,
            "error_type": None,
            "error_code": None,
            "error_message": None,
            "stack_frames": None,
            "artifact_path": "compiled/abc123def456",
            "artifact_sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            "row_count": 265,
            "http_request_id": None,
            "parent_stage_run_id": None,
        }
        result = LogRenderer.format_event(event)
        assert result is not None
        assert "DONE" in result
        assert "artifact_path=" in result
        assert "row_count=" in result
        assert "265" in result


class TestFormatStageFailed:
    """StageEvent failed 格式测试。"""

    def test_failed_basic_format(self):
        """failed 含 FAILED 状态、耗时、错误消息。"""
        event = {
            "event_type": "stage",
            "run_id": "20260710-120000",
            "timestamp": "2026-07-10T12:00:00.789000+00:00",
            "node": "sql_parser",
            "status": "failed",
            "stage_run_id": "stage_sql_parser_c6885f93",
            "artifact_request_id": None,
            "duration_ms": 0,
            "error_type": "ParseError",
            "error_code": "E001",
            "error_message": "未找到 markdown fenced code block",
            "stack_frames": None,
            "artifact_path": None,
            "artifact_sha256": None,
            "row_count": None,
            "http_request_id": None,
            "parent_stage_run_id": None,
        }
        result = LogRenderer.format_event(event)
        assert result is not None
        assert "[ERROR" in result
        assert "FAILED" in result
        assert "E001" in result
        assert "未找到" in result

    def test_failed_with_stack_frames(self):
        """failed 含 stack_frames 时输出缩进的 stack 行。"""
        event = {
            "event_type": "stage",
            "run_id": "20260710-120000",
            "timestamp": "2026-07-10T12:00:00.000000+00:00",
            "node": "sql_builder",
            "status": "failed",
            "stage_run_id": "s1",
            "artifact_request_id": None,
            "duration_ms": 567,
            "error_type": "ValueError",
            "error_code": None,
            "error_message": "invalid input",
            "stack_frames": [
                {"file": "/path/to/parser.py", "function": "_extract_fenced_block", "lineno": 286},
                {"file": "/path/to/pipeline.py", "function": "_parse_and_enrich", "lineno": 436},
            ],
            "artifact_path": None,
            "artifact_sha256": None,
            "row_count": None,
            "http_request_id": None,
            "parent_stage_run_id": None,
        }
        result = LogRenderer.format_event(event)
        assert result is not None
        assert "FAILED" in result
        assert "parser.py" in result
        assert "_extract_fenced_block" in result
        assert "286" in result
        # 验证缩进标记存在——两条 stack frame 行
        lines = result.split("\n")
        stack_lines = [l for l in lines if "parser.py" in l or "pipeline.py" in l]
        assert len(stack_lines) == 2


class TestFormatStageSkipped:
    """StageEvent skipped 格式测试。"""

    def test_skipped_format(self):
        """skipped 含 WARN 级别和 SKIPPED 状态。"""
        event = {
            "event_type": "stage",
            "run_id": "20260710-120000",
            "timestamp": "2026-07-10T12:00:00.000000+00:00",
            "node": "snapshot_builder",
            "status": "skipped",
            "stage_run_id": "s1",
            "artifact_request_id": None,
            "duration_ms": None,
            "error_type": None,
            "error_code": None,
            "error_message": None,
            "stack_frames": None,
            "artifact_path": None,
            "artifact_sha256": None,
            "row_count": None,
            "http_request_id": None,
            "parent_stage_run_id": None,
        }
        result = LogRenderer.format_event(event)
        assert result is not None
        assert "[WARN" in result
        assert "SKIPPED" in result


class TestFormatHttp:
    """HttpEvent 格式测试。"""

    def test_http_success(self):
        """HTTP 200 含方法、路径、状态码、耗时、请求 ID。"""
        event = {
            "event_type": "http",
            "run_id": "20260710-120000",
            "timestamp": "2026-07-10T12:00:00.000000+00:00",
            "http_request_id": "hreq_b27fda23",
            "method": "POST",
            "path": "/api/spark/physical-verify",
            "status_code": 200,
            "duration_ms": 107053,
            "error_type": None,
            "error_message": None,
        }
        result = LogRenderer.format_event(event)
        assert result is not None
        assert "[INFO" in result
        assert "POST" in result
        assert "/api/spark/physical-verify" in result
        assert "200" in result
        assert "107053ms" in result
        assert "hreq_b27fda23" in result

    def test_http_error(self):
        """HTTP 500 含 ERROR 级别。"""
        event = {
            "event_type": "http",
            "run_id": "20260710-120000",
            "timestamp": "2026-07-10T12:00:00.000000+00:00",
            "http_request_id": "hreq_abc",
            "method": "GET",
            "path": "/api/pipeline/status",
            "status_code": 500,
            "duration_ms": 123,
            "error_type": "RuntimeError",
            "error_message": "internal error",
        }
        result = LogRenderer.format_event(event)
        assert result is not None
        assert "[ERROR" in result


class TestFormatBrowser:
    """BrowserEvent 格式测试。"""

    def test_browser_with_stack(self):
        """浏览器错误含 stack frames。"""
        event = {
            "event_type": "browser",
            "run_id": "20260710-120000",
            "timestamp": "2026-07-10T12:00:00.000000+00:00",
            "api_path": "/api/pipeline/execute",
            "api_status": 500,
            "api_duration_ms": None,
            "error_type": "TypeError",
            "error_message": "Cannot read property 'x'",
            "stack_frames": ["app.js:renderComponent:120", "app.js:handleClick:45"],
        }
        result = LogRenderer.format_event(event)
        assert result is not None
        assert "[ERROR" in result
        assert "BROWSER" in result
        assert "TypeError" in result
        assert "renderComponent" in result


class TestFormatResource:
    """ResourceSample 格式测试。"""

    def test_resource_default_off(self):
        """TIANSHU_LOG_RESOURCE 未设置时返回 None。"""
        event = {
            "event_type": "resource",
            "run_id": "20260710-120000",
            "timestamp": "2026-07-10T12:00:05.000000+00:00",
            "processes": [
                {"pid": 12345, "name": "python", "cpu_percent": 45.2, "rss_mb": 256.8, "vms_mb": 1024.0, "num_threads": 8},
            ],
        }
        with patch.dict(os.environ, {}, clear=True):
            result = LogRenderer.format_event(event)
        assert result is None

    def test_resource_enabled(self):
        """TIANSHU_LOG_RESOURCE=1 时输出资源信息。"""
        event = {
            "event_type": "resource",
            "run_id": "20260710-120000",
            "timestamp": "2026-07-10T12:00:05.000000+00:00",
            "processes": [
                {"pid": 12345, "name": "python", "cpu_percent": 45.2, "rss_mb": 256.8, "vms_mb": 1024.0, "num_threads": 8},
            ],
        }
        with patch.dict(os.environ, {"TIANSHU_LOG_RESOURCE": "1"}):
            result = LogRenderer.format_event(event)
        assert result is not None
        assert "[DEBUG" in result
        assert "python" in result
        assert "45.2" in result


class TestFormatUnknownEvent:
    """未知事件类型测试。"""

    def test_unknown_event_type_returns_none(self):
        """未知 event_type 返回 None（安全降级）。"""
        event = {
            "event_type": "unknown_xyz",
            "run_id": "test",
            "timestamp": "2026-07-10T12:00:00.000000+00:00",
        }
        result = LogRenderer.format_event(event)
        assert result is None
