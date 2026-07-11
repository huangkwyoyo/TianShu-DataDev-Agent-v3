"""测试 Sanitizer——traceback 脱敏、错误消息截断、URL 清洗、事件校验。"""

import pytest

from tianshu_datadev.monitor.models import (
    MonitorEvent,
    StageEvent,
)
from tianshu_datadev.monitor.sanitizer import Sanitizer


class TestSanitizeTraceback:
    """traceback 脱敏测试。"""

    def test_sanitize_traceback_strips_locals(self):
        """脱敏后不含 f_locals/f_globals。"""
        try:
            x = 42
            y = [1, 2, 3]
            raise ValueError("测试异常")
        except ValueError:
            import sys
            tb = sys.exc_info()[2]
            frames = Sanitizer.sanitize_traceback(tb)
            for frame in frames:
                assert "f_locals" not in frame
                assert "f_globals" not in frame
                assert "locals" not in frame
                assert "globals" not in frame

    def test_sanitize_traceback_keeps_file_function_lineno(self):
        """保留文件名/函数名/行号。"""
        try:
            raise ValueError("测试异常")
        except ValueError:
            import sys
            tb = sys.exc_info()[2]
            frames = Sanitizer.sanitize_traceback(tb)
            assert len(frames) >= 1
            for frame in frames:
                assert "file" in frame
                assert "function" in frame
                assert "lineno" in frame
                # 验证值不为空
                assert frame["file"]
                assert frame["function"]
                assert isinstance(frame["lineno"], int)


class TestSanitizeErrorMessage:
    """错误消息截断测试。"""

    def test_sanitize_error_message_truncates_500(self):
        """超长消息截断到 500 字符。"""
        long_msg = "x" * 1000
        result = Sanitizer.sanitize_error_message(long_msg)
        assert len(result) == 500
        assert result == "x" * 500

    def test_sanitize_error_message_short_unchanged(self):
        """短消息保持不变。"""
        msg = "简短错误"
        result = Sanitizer.sanitize_error_message(msg)
        assert result == msg


class TestSanitizeUrl:
    """URL 清洗测试。"""

    def test_sanitize_url_strips_query_string(self):
        """移除 URL query 参数。"""
        url = "/api/data?page=1&token=secret"
        result = Sanitizer.sanitize_url(url)
        assert result == "/api/data"
        assert "?" not in result

    def test_sanitize_url_no_query_unchanged(self):
        """无 query string 的 URL 保持不变。"""
        url = "/api/health"
        result = Sanitizer.sanitize_url(url)
        assert result == "/api/health"


class TestValidateEvent:
    """事件校验测试。"""

    def test_validate_event_rejects_request_body(self):
        """含 request_body 字段的事件被拒绝。"""

        # 创建一个包含 request_body 的恶意事件（模拟攻击）
        class BadEvent(MonitorEvent):
            event_type: str = "http"
            request_body: str  # 黑名单字段

        event = BadEvent(
            run_id="run-001",
            request_body='{"password": "secret"}',
        )
        with pytest.raises(ValueError, match="request_body"):
            Sanitizer.validate_event(event)

    def test_validate_event_rejects_authorization_header(self):
        """含 authorization 字段的事件被拒绝。"""

        class BadEvent(MonitorEvent):
            event_type: str = "http"
            authorization: str  # 黑名单字段

        event = BadEvent(
            run_id="run-001",
            authorization="Bearer secret-token",
        )
        with pytest.raises(ValueError, match="(?i)authorization"):
            Sanitizer.validate_event(event)

    def test_validate_event_valid_event_passes(self):
        """合规事件通过校验。"""
        event = StageEvent(
            run_id="run-001",
            stage_run_id="stage-001",
            node="extract",
            status="started",
        )
        result = Sanitizer.validate_event(event)
        assert result is event
