"""监控日志文本渲染器——将事件 dict 格式化为人类可读的文本行。

纯函数，零外部依赖。不写文件——文件 I/O 由 collector 负责。
"""

import os
from datetime import datetime


class LogRenderer:
    """监控事件文本渲染器——纯函数集合，无状态。"""

    @staticmethod
    def format_event(event: dict) -> str | None:
        """将单个事件 dict 格式化为人类可读文本。

        返回 None 表示该事件不需要文本输出（如 ResourceSample 未启用时）。

        格式规格：
        - StageEvent: 时间 [级别] 节点名 状态 耗时 错误摘要
        - HttpEvent:  时间 [级别] 方法 路径 状态码 耗时 请求ID
        - BrowserEvent: 时间 [ERROR] BROWSER 路径 错误 + stack
        - ResourceSample: 默认 None，TIANSHU_LOG_RESOURCE=1 时输出
        """
        event_type = event.get("event_type")
        if event_type == "stage":
            return _format_stage(event)
        elif event_type == "http":
            return _format_http(event)
        elif event_type == "browser":
            return _format_browser(event)
        elif event_type == "resource":
            return _format_resource(event)
        return None


def _format_timestamp(ts_str: str) -> str:
    """将 ISO UTC 时间戳转为本地时间 HH:MM:SS.mmm 格式。

    兼容 'Z' 后缀和 '+00:00' 偏移两种格式。
    """
    ts_str = ts_str.replace("Z", "+00:00")
    dt = datetime.fromisoformat(ts_str)
    local_dt = dt.astimezone()
    return local_dt.strftime("%H:%M:%S.") + f"{local_dt.microsecond // 1000:03d}"


def _format_duration(ms: int | None) -> str:
    """格式化耗时——None 返回空字符串，否则返回 '{n}ms'。"""
    if ms is None:
        return ""
    return f"{ms}ms"


def _format_stage(event: dict) -> str:
    """格式化 StageEvent。支持 started/completed/failed/skipped 四种状态。"""
    ts = _format_timestamp(event["timestamp"])
    node = event.get("node", "?")
    status = event.get("status", "?")
    duration_ms = event.get("duration_ms")

    # 级别映射
    level_map = {
        "started": "INFO",
        "completed": "INFO",
        "failed": "ERROR",
        "skipped": "WARN",
    }
    level = level_map.get(status, "INFO")

    # 状态文本
    status_text_map = {
        "started": "STARTED",
        "completed": "DONE",
        "failed": "FAILED",
        "skipped": "SKIPPED",
    }
    status_text = status_text_map.get(status, status.upper())

    # 耗时
    duration_str = f" {_format_duration(duration_ms)}" if duration_ms is not None else ""

    # 错误摘要（仅 failed）
    error_str = ""
    if status == "failed":
        error_code = event.get("error_code")
        error_msg = event.get("error_message") or ""
        if error_code:
            error_str = f"  {error_code}: {error_msg}"
        elif error_msg:
            error_str = f"  {error_msg}"
        # 截断过长错误消息
        if len(error_str) > 120:
            error_str = error_str[:117] + "..."

    # 主行
    main_line = (
        f"{ts} [{level:5s}] {node:28s} {status_text:7s}{duration_str}{error_str}"
    )

    lines = [main_line]

    # DEBUG 字段（仅 completed 事件，非空可选字段追加到下行）
    if status == "completed":
        debug_fields = []
        for key in ("artifact_path", "artifact_sha256", "row_count", "error_code"):
            val = event.get(key)
            if val is not None and val != "":
                debug_fields.append(f"{key}={val}")
        indent = " " * (len(ts) + 8)
        for field in debug_fields:
            lines.append(f"{indent}{field}")

    # Stack frames（仅 failed 事件）
    if status == "failed":
        stack_frames = event.get("stack_frames")
        if stack_frames:
            indent = " " * (len(ts) + 8)
            for sf in stack_frames[:10]:
                file = sf.get("file", "?")
                func = sf.get("function", "?")
                lineno = sf.get("lineno", "?")
                short_file = os.path.basename(file) if file != "?" else "?"
                lines.append(f"{indent}  L {short_file}:{lineno} {func}")

    return "\n".join(lines)


def _format_http(event: dict) -> str:
    """格式化 HttpEvent。status < 400 → INFO，≥ 400 → ERROR。"""
    ts = _format_timestamp(event["timestamp"])
    method = event.get("method", "?")
    path = event.get("path", "?")
    status_code = event.get("status_code", 0)
    duration_ms = event.get("duration_ms", 0)
    hreq_id = event.get("http_request_id", "")

    level = "ERROR" if status_code >= 400 else "INFO"

    return (
        f"{ts} [{level:5s}] {method:6s} {path:40s} "
        f"{status_code}  {_format_duration(duration_ms):>8s}  {hreq_id}"
    )


def _format_browser(event: dict) -> str:
    """格式化 BrowserEvent。"""
    ts = _format_timestamp(event["timestamp"])
    api_path = event.get("api_path") or "-"
    error_type = event.get("error_type") or "Error"
    error_msg = event.get("error_message") or ""

    main_line = (
        f"{ts} [ERROR] BROWSER {api_path:40s} {error_type}: {error_msg}"
    )

    lines = [main_line]
    stack_frames = event.get("stack_frames")
    if stack_frames:
        indent = " " * (len(ts) + 8)
        for sf in stack_frames[:10]:
            if isinstance(sf, str):
                lines.append(f"{indent}  L {sf}")

    return "\n".join(lines)


def _format_resource(event: dict) -> str | None:
    """格式化 ResourceSample——仅 TIANSHU_LOG_RESOURCE=1 时输出。"""
    if not os.environ.get("TIANSHU_LOG_RESOURCE"):
        return None

    ts = _format_timestamp(event["timestamp"])
    processes = event.get("processes", [])

    lines = [f"{ts} [DEBUG] RESOURCE  {len(processes)} 进程"]
    indent = " " * (len(ts) + 8)
    for p in processes:
        pid = p.get("pid", 0)
        name = p.get("name", "?")
        cpu = p.get("cpu_percent", 0.0)
        rss = p.get("rss_mb", 0.0)
        lines.append(
            f"{indent}pid={pid:<6d} {name:20s} CPU={cpu:5.1f}% RSS={rss:6.1f}MB"
        )

    return "\n".join(lines)
