"""流式进度基础设施——TeeCollector + 事件队列 + 错误清洗。

用于 /api/run-all-full/stream 端点：
- TeeCollector 包装现有 collector，将阶段事件同时推送到流式队列
- 后台线程执行全流程，通过 queue.Queue 向生成器传递 NDJSON 事件
- 错误信息经清洗后入队，不暴露完整 traceback
"""

from __future__ import annotations

import logging
import queue
import time
from typing import Literal

from tianshu_datadev.monitor.collector import StageContext

logger = logging.getLogger(__name__)


def _sanitize_stream_error(exc: Exception) -> str:
    """清洗异常信息用于流式传输——仅保留异常类型+消息，不暴露 traceback。

    Args:
        exc: 原始异常对象

    Returns:
        清洗后的错误字符串，上限 500 字符
    """
    msg = f"{type(exc).__name__}: {exc}"
    # 截断到 500 字符，避免超大 stderr 撑满事件流
    if len(msg) > 500:
        msg = msg[:497] + "..."
    return msg


def _sanitize_stage_message(raw: str, max_len: int = 300) -> str:
    """清洗阶段级别的错误消息——用于 stage 事件的 message 字段。

    Args:
        raw: 原始错误消息
        max_len: 最大长度（字符）

    Returns:
        清洗后的消息
    """
    if not raw:
        return ""
    # 截断
    if len(raw) > max_len:
        raw = raw[:max_len - 3] + "..."
    return raw


class _TeeStageContext:
    """包装 StageContext——退出时向流式队列发送 completed/failed 事件。

    不改变原有 StageContext 的行为——异常仍会传播。
    """

    def __init__(
        self,
        real_ctx: StageContext,
        event_queue: queue.Queue,
        pipeline: Literal["sql", "spark"],
        stage_name: str,
    ):
        self._real = real_ctx
        self._queue = event_queue
        self._pipeline = pipeline
        self._stage = stage_name
        self._started_at = time.time()

    def set_result(self, **kwargs) -> None:
        """透传——设置 artifact_path、row_count 等。"""
        self._real.set_result(**kwargs)

    def __enter__(self) -> "_TeeStageContext":
        self._real.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        duration_ms = int((time.time() - self._started_at) * 1000)
        if exc_type is None:
            self._queue.put({
                "event": "stage",
                "pipeline": self._pipeline,
                "stage": self._stage,
                "status": "completed",
                "duration_ms": duration_ms,
            })
        else:
            error_msg = _sanitize_stream_error(exc_val) if exc_val else ""
            self._queue.put({
                "event": "stage",
                "pipeline": self._pipeline,
                "stage": self._stage,
                "status": "failed",
                "duration_ms": duration_ms,
                "message": error_msg,
                "error_type": exc_type.__name__ if exc_type else None,
            })
        # 透传到真实 StageContext
        return self._real.__exit__(exc_type, exc_val, exc_tb)


class TeeCollector:
    """包装现有 collector，将阶段事件同时推送到流式队列。

    仅拦截 stage() 方法——emit() 等其他方法直接透传。
    不改变现有 collector 的任何行为。

    由于 NullCollector 的 stage() 返回一个空操作 StageContext，
    TeeCollector 包装后仍能正确工作——流式事件正常发送，
    但底层 collector 不受影响。
    """

    def __init__(self, real, event_queue: queue.Queue, pipeline: Literal["sql", "spark"]):
        self._real = real
        self._queue = event_queue
        self._pipeline = pipeline

    @property
    def enabled(self) -> bool:
        return getattr(self._real, "enabled", False)

    @property
    def run_id(self) -> str:
        return getattr(self._real, "run_id", "")

    def emit(self, event) -> None:
        """透传——不做拦截。"""
        self._real.emit(event)

    def stage(
        self,
        node: str,
        artifact_request_id: str,
        parent_stage_run_id: str | None = None,
    ):
        """拦截 stage()——先向流式队列发送 started 事件，再返回包装的上下文。

        包装的 _TeeStageContext 在退出时会自动发送 completed/failed 事件。
        """
        # 发送 started 事件
        self._queue.put({
            "event": "stage",
            "pipeline": self._pipeline,
            "stage": node,
            "status": "started",
        })
        # 获取真实 StageContext 并包装
        real_ctx = self._real.stage(node, artifact_request_id, parent_stage_run_id)
        return _TeeStageContext(real_ctx, self._queue, self._pipeline, node)

    def log_resource_sample(self, sample) -> None:
        """透传资源采样。"""
        self._real.log_resource_sample(sample)

    def log_browser_event(self, payload: dict) -> None:
        """透传浏览器事件。"""
        self._real.log_browser_event(payload)

    def flush(self, timeout: float = 5.0) -> bool:
        """透传 flush。"""
        return self._real.flush(timeout)

    def close(self) -> None:
        """透传 close。"""
        self._real.close()
