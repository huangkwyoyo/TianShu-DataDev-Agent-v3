"""测试 /api/monitor/browser-event 安全校验链。"""

import os
from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from tianshu_datadev.api.routes import api_router, _rate_limit_state, _total_count_state
from tianshu_datadev.monitor.collector import RunLogCollector
from tianshu_datadev.monitor.middleware import MonitorMiddleware


class _CollectingBrowserCollector:
    """测试用 collector——记录 browser event payload。"""

    enabled = True
    run_id = "test-run-id-001"

    def __init__(self):
        self.browser_events: list[dict] = []

    def log_browser_event(self, payload: dict) -> None:
        self.browser_events.append(payload)

    def emit(self, event) -> None:
        pass


def _make_app() -> FastAPI:
    """创建含 monitor 端点的测试用 FastAPI 应用。"""
    app = FastAPI()
    app.include_router(api_router)
    collector = _CollectingBrowserCollector()
    app.state.monitor_collector = collector
    return app


# 环境变量基础值
_ENV_BASE = {
    "TIANSHU_RUN_ID": "test-run-id-001",
    "TIANSHU_MONITOR_TOKEN": "test-token-abc",
}


class TestBrowserEventSecurity:
    """browser-event 端点安全校验链测试。"""

    def _reset_rate_limit(self):
        """清空速率/总量限制状态。"""
        _rate_limit_state.clear()
        _total_count_state.clear()

    def test_browser_event_returns_404_when_monitoring_disabled(self):
        """监控未启用时返回 404。"""
        app = FastAPI()
        app.include_router(api_router)
        # 不设置 collector——app.state.monitor_collector 不存在
        with patch.dict(os.environ, _ENV_BASE):
            with TestClient(app) as client:
                resp = client.post(
                    "/api/monitor/browser-event",
                    json={"monitor_token": "test-token-abc", "run_id": "test-run-id-001"},
                )
                assert resp.status_code == 404

    def test_browser_event_rejects_wrong_origin(self):
        """错误 Origin 返回 403。"""
        with patch.dict(os.environ, _ENV_BASE):
            app = _make_app()
            with TestClient(app) as client:
                resp = client.post(
                    "/api/monitor/browser-event",
                    json={"monitor_token": "test-token-abc", "run_id": "test-run-id-001"},
                    headers={"Origin": "http://evil.com"},
                )
                assert resp.status_code == 403

    def test_browser_event_accepts_localhost_origin(self):
        """localhost:5173 origin 通过校验。"""
        self._reset_rate_limit()
        with patch.dict(os.environ, _ENV_BASE):
            app = _make_app()
            with TestClient(app) as client:
                resp = client.post(
                    "/api/monitor/browser-event",
                    json={"monitor_token": "test-token-abc", "run_id": "test-run-id-001"},
                    headers={"Origin": "http://localhost:5173"},
                )
                assert resp.status_code == 204

    def test_browser_event_accepts_127_origin(self):
        """127.0.0.1:5173 origin 通过校验。"""
        self._reset_rate_limit()
        with patch.dict(os.environ, _ENV_BASE):
            app = _make_app()
            with TestClient(app) as client:
                resp = client.post(
                    "/api/monitor/browser-event",
                    json={"monitor_token": "test-token-abc", "run_id": "test-run-id-001"},
                    headers={"Origin": "http://127.0.0.1:5173"},
                )
                assert resp.status_code == 204

    def test_browser_event_rejects_missing_monitor_token(self):
        """缺少 monitor_token 返回 403。"""
        with patch.dict(os.environ, _ENV_BASE):
            app = _make_app()
            with TestClient(app) as client:
                resp = client.post(
                    "/api/monitor/browser-event",
                    json={"run_id": "test-run-id-001"},
                    headers={"Origin": "http://localhost:5173"},
                )
                assert resp.status_code == 403

    def test_browser_event_rejects_wrong_monitor_token(self):
        """monitor_token 不匹配返回 403。"""
        with patch.dict(os.environ, _ENV_BASE):
            app = _make_app()
            with TestClient(app) as client:
                resp = client.post(
                    "/api/monitor/browser-event",
                    json={"monitor_token": "wrong-token", "run_id": "test-run-id-001"},
                    headers={"Origin": "http://localhost:5173"},
                )
                assert resp.status_code == 403

    def test_browser_event_rejects_wrong_run_id(self):
        """run_id 与 TIANSHU_RUN_ID 不匹配返回 403。"""
        with patch.dict(os.environ, _ENV_BASE):
            app = _make_app()
            with TestClient(app) as client:
                resp = client.post(
                    "/api/monitor/browser-event",
                    json={"monitor_token": "test-token-abc", "run_id": "wrong-run-id"},
                    headers={"Origin": "http://localhost:5173"},
                )
                assert resp.status_code == 403

    def test_browser_event_rate_limit(self):
        """速率限制——每分钟 20 条后返回 429。"""
        self._reset_rate_limit()
        with patch.dict(os.environ, _ENV_BASE):
            app = _make_app()
            with TestClient(app) as client:
                # 前 20 次应成功
                for i in range(20):
                    resp = client.post(
                        "/api/monitor/browser-event",
                        json={
                            "monitor_token": "test-token-abc",
                            "run_id": "test-run-id-001",
                        },
                        headers={"Origin": "http://localhost:5173"},
                    )
                    assert resp.status_code == 204, f"第 {i+1} 次请求应成功"

                # 第 21 次应被限流
                resp = client.post(
                    "/api/monitor/browser-event",
                    json={
                        "monitor_token": "test-token-abc",
                        "run_id": "test-run-id-001",
                    },
                    headers={"Origin": "http://localhost:5173"},
                )
                assert resp.status_code == 429

    def test_browser_event_total_limit(self):
        """总量限制——总共 200 条后返回 429。"""
        self._reset_rate_limit()
        # 跑满 200 条测试太慢，这里测试时将 max_total_events 模拟为 5
        # 用 mock 降低总量限制门槛
        with patch.dict(os.environ, _ENV_BASE):
            app = _make_app()
            collector = app.state.monitor_collector
            # 快速累积总量计数
            _total_count_state["test-run-id-001"] = 200
            with TestClient(app) as client:
                resp = client.post(
                    "/api/monitor/browser-event",
                    json={
                        "monitor_token": "test-token-abc",
                        "run_id": "test-run-id-001",
                    },
                    headers={"Origin": "http://localhost:5173"},
                )
                assert resp.status_code == 429

    def test_browser_event_rejects_oversized_body(self):
        """请求体超过 4KB 返回 413。"""
        self._reset_rate_limit()
        with patch.dict(os.environ, _ENV_BASE):
            app = _make_app()
            with TestClient(app) as client:
                # 构造超过 4KB 的 payload
                large_data = {
                    "monitor_token": "test-token-abc",
                    "run_id": "test-run-id-001",
                    "error_message": "x" * 5000,
                }
                resp = client.post(
                    "/api/monitor/browser-event",
                    json=large_data,
                    headers={"Origin": "http://localhost:5173"},
                )
                assert resp.status_code == 413

    def test_browser_event_rejects_request_body_in_payload(self):
        """payload 中包含 request_body 黑名单字段返回 400。"""
        self._reset_rate_limit()
        with patch.dict(os.environ, _ENV_BASE):
            app = _make_app()
            with TestClient(app) as client:
                resp = client.post(
                    "/api/monitor/browser-event",
                    json={
                        "monitor_token": "test-token-abc",
                        "run_id": "test-run-id-001",
                        "request_body": "sensitive data",
                    },
                    headers={"Origin": "http://localhost:5173"},
                )
                assert resp.status_code == 400
