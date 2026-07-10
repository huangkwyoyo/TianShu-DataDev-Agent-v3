"""敏感字段白名单过滤器。

Sanitizer 提供四类静态方法：
1. sanitize_traceback — 遍历 traceback 对象，只保留 {file, function, lineno}
2. sanitize_error_message — 截断到 500 字符
3. sanitize_url — 移除 query string
4. validate_event — 检查事件不包含黑名单敏感字段
"""

import traceback
from typing import Any

from tianshu_datadev.monitor.models import MonitorEvent

# 事件字段黑名单——任何包含这些字段名的事件都将被拒绝
_BLACKLISTED_FIELDS: frozenset[str] = frozenset({
    "request_body",
    "response_body",
    "headers",
    "authorization",
    "cookie",
    "set_cookie",
})


class Sanitizer:
    """敏感字段白名单过滤器。"""

    @staticmethod
    def sanitize_traceback(tb) -> list[dict]:
        """遍历 traceback，只保留 {file, function, lineno}。移除 f_locals/f_globals。

        Args:
            tb: traceback 对象（sys.exc_info()[2]）。

        Returns:
            字典列表，每项包含 file、function、lineno 三个键。
        """
        frames: list[dict[str, Any]] = []
        for frame_summary in traceback.extract_tb(tb):
            frames.append({
                "file": frame_summary.filename,
                "function": frame_summary.name,
                "lineno": frame_summary.lineno,
            })
        return frames

    @staticmethod
    def sanitize_error_message(msg: str) -> str:
        """截断到 500 字符。

        Args:
            msg: 原始错误消息。

        Returns:
            截断后的消息（最多 500 字符）。
        """
        return msg[:500]

    @staticmethod
    def sanitize_url(url: str) -> str:
        """移除 query string（? 之后的内容）。

        Args:
            url: 原始 URL 路径。

        Returns:
            不含 query string 的 URL。
        """
        if "?" in url:
            return url[: url.index("?")]
        return url

    @staticmethod
    def validate_event(event: MonitorEvent) -> MonitorEvent:
        """检查事件不含黑名单字段（request_body、response_body、headers、authorization 等）。

        Args:
            event: 待校验的事件对象。

        Returns:
            原事件（校验通过）。

        Raises:
            ValueError: 事件包含黑名单字段时抛出。
        """
        for field in _BLACKLISTED_FIELDS:
            if hasattr(event, field):
                raise ValueError(f"事件包含黑名单敏感字段: {field}")
        return event
