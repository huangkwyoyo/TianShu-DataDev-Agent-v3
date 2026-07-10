"""监控事件数据模型——所有事件使用 StrictModel 基类（extra="forbid"）。

事件类型：
- StageEvent：管线阶段事件
- HttpEvent：HTTP 请求事件（不含 client_ip）
- BrowserEvent：浏览器上报事件（不含请求/响应体）
- ResourceSample：资源采样事件（不含 active_stage_run_ids——离线关联）
"""

from datetime import datetime, timezone
from typing import Literal

from pydantic import Field

from tianshu_datadev.developer_spec.models import StrictModel


def _utc_now() -> datetime:
    """返回当前 UTC 时间（带时区）。"""
    return datetime.now(timezone.utc)


class MonitorEvent(StrictModel):
    """事件基类——所有监控事件的公共字段。"""

    event_type: str
    run_id: str
    timestamp: datetime = Field(default_factory=_utc_now)


class StageEvent(MonitorEvent):
    """管线阶段事件——单个节点的单次执行记录。"""

    event_type: Literal["stage"] = "stage"
    http_request_id: str | None = None
    artifact_request_id: str | None = None
    stage_run_id: str
    parent_stage_run_id: str | None = None
    node: str
    status: Literal["started", "completed", "failed", "skipped"]
    duration_ms: int | None = None
    error_type: str | None = None
    error_code: str | None = None
    error_message: str | None = None
    stack_frames: list[dict] | None = None  # [{file, function, lineno}]
    artifact_path: str | None = None
    artifact_sha256: str | None = None
    row_count: int | None = None


class HttpEvent(MonitorEvent):
    """HTTP 请求事件。注意：不含 client_ip。"""

    event_type: Literal["http"] = "http"
    http_request_id: str
    method: str
    path: str  # 不含 query string
    status_code: int
    duration_ms: int
    error_type: str | None = None
    error_message: str | None = None


class BrowserEvent(MonitorEvent):
    """浏览器上报事件。注意：不含 request_body/response_body/headers。"""

    event_type: Literal["browser"] = "browser"
    api_path: str | None = None
    api_status: int | None = None
    api_duration_ms: int | None = None
    error_type: str | None = None
    error_message: str | None = None
    stack_frames: list[str] | None = None  # ["file:function:lineno"]


class ProcessMetrics(StrictModel):
    """单个进程的资源指标。"""

    pid: int
    name: str
    cpu_percent: float
    rss_mb: float
    vms_mb: float
    num_threads: int


class ResourceSample(MonitorEvent):
    """资源采样事件。注意：不含 active_stage_run_ids（离线关联）。"""

    event_type: Literal["resource"] = "resource"
    processes: list[ProcessMetrics]
