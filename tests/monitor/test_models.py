"""测试 monitor 事件数据模型——字段正确性、约束合规。"""

import pytest
from pydantic import ValidationError

from tianshu_datadev.monitor.models import (
    BrowserEvent,
    HttpEvent,
    ProcessMetrics,
    ResourceSample,
    StageEvent,
)


class TestStageEvent:
    """StageEvent 序列化与字段测试。"""

    def test_stage_event_serialization(self):
        """StageEvent 序列化为 dict，含所有字段默认值。"""
        event = StageEvent(
            run_id="run-001",
            stage_run_id="stage-001",
            node="extract",
            status="started",
            artifact_path="/data/file.parquet",
            artifact_sha256="abc123",
            row_count=1000,
        )
        data = event.model_dump()
        assert data["event_type"] == "stage"
        assert data["run_id"] == "run-001"
        assert data["stage_run_id"] == "stage-001"
        assert data["node"] == "extract"
        assert data["status"] == "started"
        assert data["http_request_id"] is None
        assert data["artifact_request_id"] is None
        assert data["parent_stage_run_id"] is None
        assert data["duration_ms"] is None
        assert data["error_type"] is None
        assert data["error_code"] is None
        assert data["error_message"] is None
        assert data["stack_frames"] is None
        assert data["artifact_path"] == "/data/file.parquet"
        assert data["artifact_sha256"] == "abc123"
        assert data["row_count"] == 1000
        # 验证 timestamp 存在
        assert data["timestamp"] is not None


class TestHttpEvent:
    """HttpEvent 安全约束测试。"""

    def test_http_event_no_client_ip(self):
        """HttpEvent 没有 client_ip 字段引用。"""
        event = HttpEvent(
            run_id="run-001",
            http_request_id="req-001",
            method="GET",
            path="/api/health",
            status_code=200,
            duration_ms=42,
        )
        data = event.model_dump()
        assert "client_ip" not in data
        # 确保即使额外传 client_ip 也会被拒绝
        with pytest.raises(ValidationError):
            HttpEvent(
                run_id="run-001",
                http_request_id="req-001",
                method="GET",
                path="/api/health",
                status_code=200,
                duration_ms=42,
                client_ip="192.168.1.1",  # type: ignore
            )


class TestBrowserEvent:
    """BrowserEvent 安全约束测试。"""

    def test_browser_event_no_body_no_headers(self):
        """BrowserEvent 不含 request_body/response_body/headers 字段。"""
        event = BrowserEvent(run_id="run-001")
        data = event.model_dump()
        blacklist = {"request_body", "response_body", "headers"}
        for field in blacklist:
            assert field not in data, f"BrowserEvent 不应包含字段: {field}"
        # 验证即使尝试传入这些字段也会被拒绝
        with pytest.raises(ValidationError):
            BrowserEvent(
                run_id="run-001",
                request_body='{"key": "value"}',  # type: ignore
            )
        with pytest.raises(ValidationError):
            BrowserEvent(
                run_id="run-001",
                response_body="OK",  # type: ignore
            )
        with pytest.raises(ValidationError):
            BrowserEvent(
                run_id="run-001",
                headers={"Content-Type": "application/json"},  # type: ignore
            )


class TestResourceSample:
    """ResourceSample 约束测试。"""

    def test_resource_sample_has_no_active_stage_run_ids(self):
        """ResourceSample 不含 active_stage_run_ids（离线关联）。"""
        sample = ResourceSample(
            run_id="run-001",
            processes=[
                ProcessMetrics(
                    pid=1234,
                    name="python",
                    cpu_percent=10.5,
                    rss_mb=256.0,
                    vms_mb=1024.0,
                    num_threads=4,
                )
            ],
        )
        data = sample.model_dump()
        assert "active_stage_run_ids" not in data
        # 即使尝试传入 active_stage_run_ids 也会被拒绝
        with pytest.raises(ValidationError):
            ResourceSample(
                run_id="run-001",
                processes=[],
                active_stage_run_ids=["stage-001"],  # type: ignore
            )
