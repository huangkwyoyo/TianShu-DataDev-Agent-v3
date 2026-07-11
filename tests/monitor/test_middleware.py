"""测试 MonitorMiddleware——HTTP 监控中间件。"""


import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from tianshu_datadev.monitor.middleware import MonitorMiddleware
from tianshu_datadev.monitor.models import HttpEvent


class _CollectingCollector:
    """测试用 collector——记录 emitted HttpEvent 到列表。"""

    enabled = True
    run_id = "test-run"

    def __init__(self):
        self.events: list[HttpEvent] = []

    def emit(self, event: HttpEvent) -> None:
        self.events.append(event)


class _NullCollector:
    """模拟 NullCollector——enabled=False。"""

    enabled = False
    run_id = ""

    def emit(self, event: HttpEvent) -> None:
        pass


@pytest.fixture
def app():
    """创建测试用 FastAPI 应用——含 MonitorMiddleware 和测试端点。"""
    _app = FastAPI()
    _app.add_middleware(MonitorMiddleware)

    @_app.get("/test")
    async def test_endpoint():
        return {"ok": True}

    @_app.get("/test-query")
    async def test_query(q: str = ""):
        return {"q": q}

    @_app.get("/api/health")
    async def health():
        return {"status": "ok"}

    @_app.get("/api/monitor/config")
    async def monitor_cfg():
        return {"enabled": True}

    return _app


@pytest.fixture
def collector():
    """返回测试用收集器实例。"""
    return _CollectingCollector()


class TestMonitorMiddleware:
    """MonitorMiddleware 核心行为测试。"""

    def test_middleware_records_http_request(self, app, collector):
        """正常请求记录 method/path/status_code/duration_ms。"""
        app.state.monitor_collector = collector
        with TestClient(app) as client:
            resp = client.get("/test")
            assert resp.status_code == 200
            assert len(collector.events) == 1
            event = collector.events[0]
            assert event.method == "GET"
            assert event.path == "/test"
            assert event.status_code == 200
            assert isinstance(event.duration_ms, int)
            assert event.duration_ms >= 0

    def test_middleware_assigns_http_request_id(self, app, collector):
        """每个请求分配唯一 hreq_* id。"""
        app.state.monitor_collector = collector
        with TestClient(app) as client:
            client.get("/test")
            client.get("/test")
            assert len(collector.events) == 2
            id1 = collector.events[0].http_request_id
            id2 = collector.events[1].http_request_id
            assert id1 != id2
            assert id1.startswith("hreq_")
            assert id2.startswith("hreq_")
            assert len(id1) == 13  # "hreq_" + 8 hex = 13
            assert len(id2) == 13

    def test_middleware_excludes_health_endpoint(self, app, collector):
        """GET /api/health 不记录。"""
        app.state.monitor_collector = collector
        with TestClient(app) as client:
            resp = client.get("/api/health")
            assert resp.status_code == 200
            assert len(collector.events) == 0

    def test_middleware_excludes_monitor_endpoints(self, app, collector):
        """GET /api/monitor/config 不记录。"""
        app.state.monitor_collector = collector
        with TestClient(app) as client:
            resp = client.get("/api/monitor/config")
            assert resp.status_code == 200
            assert len(collector.events) == 0

    def test_middleware_strips_query_string_from_path(self, app, collector):
        """path 不含 query 参数。"""
        app.state.monitor_collector = collector
        with TestClient(app) as client:
            resp = client.get("/test-query?q=hello")
            assert resp.status_code == 200
            assert len(collector.events) == 1
            event = collector.events[0]
            assert event.path == "/test-query"  # 不含 ?q=hello
            assert "?" not in event.path

    def test_middleware_does_not_record_client_ip(self, app, collector):
        """HttpEvent 不含 client_ip 字段。"""
        app.state.monitor_collector = collector
        with TestClient(app) as client:
            client.get("/test")
            assert len(collector.events) == 1
            event = collector.events[0]
            assert not hasattr(event, "client_ip")

    def test_middleware_bare_raise_on_exception(self, app, collector):
        """异常时 safe_emit 后 bare raise——事件仍被记录。"""
        # 注册一个会抛异常的端点（未注册异常处理器，让异常传播）
        @app.get("/boom")
        async def boom():
            raise RuntimeError("测试异常")

        # 确保不注册通用异常处理器
        app.state.monitor_collector = collector
        with TestClient(app) as client:
            with pytest.raises(RuntimeError, match="测试异常"):
                client.get("/boom")
            # 事件应已被记录（safe_emit 发生在 bare raise 之前）
            assert len(collector.events) == 1
            event = collector.events[0]
            assert event.status_code == 500
            assert event.method == "GET"
            assert event.path == "/boom"

    def test_middleware_exception_type_identical(self, app, collector):
        """异常时 bare raise 不转换异常类型。"""
        app.state.monitor_collector = collector

        @app.get("/value-error")
        async def value_err():
            raise ValueError("自定义错误")

        with TestClient(app) as client:
            with pytest.raises(ValueError, match="自定义错误"):
                client.get("/value-error")

        @app.get("/type-error")
        async def type_err():
            raise TypeError("类型错误")

        with TestClient(app) as client:
            with pytest.raises(TypeError, match="类型错误"):
                client.get("/type-error")

    def test_null_collector_middleware_noop(self, app):
        """NullCollector 时 middleware 透传——不记录事件。"""
        app.state.monitor_collector = _NullCollector()
        with TestClient(app) as client:
            resp = client.get("/test")
            assert resp.status_code == 200
            # NullCollector 无 events 属性可检查，只需验证请求正常通过
