"""HTTP 监控中间件——safe_emit + bare raise。

唯一受控异常观察点：
- 成功路径：safe_emit HttpEvent → return response
- 异常路径：safe_emit HttpEvent(error) → bare raise（禁止转换/吞异常）
"""

import logging
import time
import uuid

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from tianshu_datadev.monitor.models import HttpEvent

logger = logging.getLogger(__name__)

# 排除路径——/api/health 和 /api/monitor/* 不经过监控
_SKIP_PATHS = {"/api/health", "/api/monitor/config", "/api/monitor/browser-event"}


class MonitorMiddleware(BaseHTTPMiddleware):
    """HTTP 监控中间件——safe_emit + bare raise。

    每个请求分配唯一 http_request_id（格式：hreq_xxxxxxxx），
    记录 method/path/status_code/duration_ms 到 HttpEvent。
    异常时 safe_emit 后 bare raise，不转换异常类型。
    """

    async def dispatch(self, request: Request, call_next):
        # 排除 /api/health 和 /api/monitor/*
        path = request.url.path
        if path in _SKIP_PATHS or path.startswith("/api/monitor/"):
            return await call_next(request)

        collector = getattr(request.app.state, "monitor_collector", None)
        if collector is None or not getattr(collector, "enabled", True):
            return await call_next(request)

        start_time = time.time()
        http_request_id = f"hreq_{uuid.uuid4().hex[:8]}"
        request.state.http_request_id = http_request_id

        try:
            response: Response = await call_next(request)
            duration_ms = int((time.time() - start_time) * 1000)
            self._safe_emit(collector, HttpEvent(
                run_id=collector.run_id,
                http_request_id=http_request_id,
                method=request.method,
                path=path,
                status_code=response.status_code,
                duration_ms=duration_ms,
            ))
            return response
        except Exception:
            duration_ms = int((time.time() - start_time) * 1000)
            self._safe_emit(collector, HttpEvent(
                run_id=collector.run_id,
                http_request_id=http_request_id,
                method=request.method,
                path=path,
                status_code=500,
                duration_ms=duration_ms,
            ))
            raise  # ← 原样传播——禁止转换，禁止吞异常

    @staticmethod
    def _safe_emit(collector, event: HttpEvent) -> None:
        """安全写入——失败仅 logging.warning，不抛异常。"""
        try:
            collector.emit(event)
        except Exception:
            logger.warning("监控事件写入失败", exc_info=True)
