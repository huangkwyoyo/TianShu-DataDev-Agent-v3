# 监控人类可读日志——实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 JSONL 不变的前提下，每个事件同步输出一行/多行人类可读文本到 `tianshu_run_{run_id}_events.log`，支持 `tail -f` 实时查看。

**Architecture:** 新增 `monitor/renderer.py`（纯格式化函数），`RunLogCollector._writer_loop` 在 JSONL 写入后调用 renderer 追加 `.log`。NullCollector 完全不受影响。

**Tech Stack:** Python 标准库（`json`, `datetime`, `os`, `textwrap`），Pytest，无第三方依赖。

## Global Constraints

- JSONL schema 不动
- `collector.emit()` 签名不动
- Pipeline 调用代码不动
- NullCollector 行为不动（零开销模式）
- 不引入第三方依赖
- 不在 lifespan shutdown 做无条件 render_file 覆盖 .log
- Windows 兼容——全部使用 ASCII 字符，不用 Unicode 图标
- 所有注释使用中文
- TDD：测试先行，红灯→绿灯→重构

---

### Task 1: 编写 `test_renderer.py`——红灯阶段

**Files:**
- Create: `tests/monitor/test_renderer.py`

**Interfaces:**
- Produces: 测试依赖 `LogRenderer.format_event(event: dict) -> str | None`（函数尚不存在，测试先行）

- [ ] **Step 1: 创建测试文件，写入全部 10 个单元测试**

```python
"""测试监控文本渲染器——LogRenderer.format_event() 纯函数行为。"""

import json
import os
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

# 目标函数尚不存在——测试先行，预期 ImportError
from tianshu_datadev.monitor.renderer import LogRenderer


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
            "artifact_request_id": "",
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
            "artifact_request_id": "",
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
            "artifact_request_id": "",
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
            "artifact_request_id": "",
            "duration_ms": 0,
            "error_type": "ParseError",
            "error_code": "E001",
            "error_message": "未找到 ```markdown fenced code block",
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
        """failed 含 stack_frames 时输出缩进的 ↳ 行。"""
        event = {
            "event_type": "stage",
            "run_id": "20260710-120000",
            "timestamp": "2026-07-10T12:00:00.000000+00:00",
            "node": "sql_builder",
            "status": "failed",
            "stage_run_id": "s1",
            "artifact_request_id": "",
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
        # 验证缩进标记存在
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
            "artifact_request_id": "",
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
```

- [ ] **Step 2: 运行测试——确认全部因 ImportError 失败（红灯）**

```bash
cd "D:/Program Files/gitvscode/TianShu-DataDev-Agent-v3"
python -m pytest tests/monitor/test_renderer.py -v --tb=short 2>&1 | head -20
```

Expected: 全部 13 个测试 FAIL（`ModuleNotFoundError: No module named 'tianshu_datadev.monitor.renderer'`）

- [ ] **Step 3: 提交红灯测试**

```bash
git add tests/monitor/test_renderer.py
git commit -m "test: LogRenderer 纯函数红灯测试——13 个用例覆盖 stage/http/browser/resource/unknown"
```

---

### Task 2: 实现 `renderer.py`——绿灯阶段

**Files:**
- Create: `src/tianshu_datadev/monitor/renderer.py`

**Interfaces:**
- Consumes: 无（纯函数，零依赖）
- Produces: `LogRenderer.format_event(event: dict) -> str | None` + 私有辅助函数

- [ ] **Step 1: 创建 `renderer.py`**

```python
"""监控日志文本渲染器——将事件 dict 格式化为人类可读的文本行。

纯函数，零外部依赖。不写文件——文件 I/O 由 collector 负责。
"""

import os
from datetime import datetime, timezone


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
            if val is not None:
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
```

- [ ] **Step 2: 运行测试——确认全部通过（绿灯）**

```bash
python -m pytest tests/monitor/test_renderer.py -v --tb=short
```

Expected: 13 passed

- [ ] **Step 3: 提交**

```bash
git add src/tianshu_datadev/monitor/renderer.py
git commit -m "feat: LogRenderer 纯函数——stage/http/browser/resource 格式化"
```

---

### Task 3: 修改 `collector.py` + `__init__.py`——接入文本日志

**Files:**
- Modify: `src/tianshu_datadev/monitor/collector.py:79-98`（`__init__` 打开 .log 文件、`_writer_loop` 追加文本、`close` 关闭）
- Modify: `src/tianshu_datadev/monitor/__init__.py:9-14,26-39`（导入 + 导出 LogRenderer）

**Interfaces:**
- Consumes: `LogRenderer.format_event(event_dict) -> str | None`
- Produces: 无新增公开接口

- [ ] **Step 1: 修改 `collector.py`——三处改动**

**改动 A：模块顶部新增 import（第 18 行之后）**

```python
from tianshu_datadev.monitor.renderer import LogRenderer
```

**改动 B：`__init__` 中打开 `.log` 文件（第 81 行之后，`self._file` 打开之后）**

```python
# 同时打开文本日志文件供人类可读
log_path = self._log_dir / f"tianshu_run_{run_id}_events.log"
self._text_file: TextIO = open(log_path, "w", encoding="utf-8")
```

**改动 C：`_writer_loop` 中 JSONL 写入后追加文本（替换整个 `_writer_loop` 方法体）**

```python
def _writer_loop(self) -> None:
    """消费者线程主循环——从队列取事件，先写 JSONL 再写文本日志。"""
    while self._running or not self._queue.empty():
        try:
            event = self._queue.get(timeout=0.5)
            # JSONL 写入（不变）
            json_line = event.model_dump_json() + "\n"
            self._file.write(json_line)
            self._file.flush()
            # 文本日志写入（新增）
            event_dict = json.loads(event.model_dump_json())
            text_output = LogRenderer.format_event(event_dict)
            if text_output:
                self._text_file.write(text_output + "\n")
                self._text_file.flush()
        except queue.Empty:
            continue
        except Exception as exc:
            logging.warning("监控写入失败: %s", exc)
```

**改动 D：`close()` 中关闭 `.log` 文件（第 154 行 `self._file.close()` 之后）**

```python
self._text_file.close()
```

- [ ] **Step 2: 修改 `__init__.py`——新增 LogRenderer 导出**

**在第 9-14 行的 import block 后新增：**

```python
from tianshu_datadev.monitor.renderer import LogRenderer
```

**在 `__all__` 列表末尾（`"StageEvent",` 之后）新增：**

```python
"LogRenderer",
```

- [ ] **Step 3: 运行现有测试——确认 collector 行为不变**

```bash
python -m pytest tests/monitor/ -x --tb=short
```

Expected: 全部通过（现有 collector 测试 + 新增 renderer 测试）

- [ ] **Step 4: 提交**

```bash
git add src/tianshu_datadev/monitor/collector.py src/tianshu_datadev/monitor/__init__.py
git commit -m "feat: RunLogCollector writer 线程同步输出人类可读 .log"
```

---

### Task 4: 编写 collector 集成测试——变更验证

**Files:**
- Modify: `tests/monitor/test_collector.py`（追加 3 个集成测试）

- [ ] **Step 1: 在 `test_collector.py` 末尾追加集成测试**

```python
class TestRunLogCollectorTextLog:
    """RunLogCollector 人类可读文本日志集成测试。"""

    def test_writes_both_jsonl_and_log(self):
        """RunLogCollector 同时写出 .jsonl 和 .log 两个文件。"""
        with tempfile.TemporaryDirectory() as tmp:
            log_dir = Path(tmp)
            with patch.dict(os.environ, {"TIANSHU_RUN_ID": "test-log-001"}):
                collector = RunLogCollector(log_dir, "test-log-001")
                # emit 一个 stage started 事件
                event = StageEvent(
                    run_id="test-log-001",
                    stage_run_id="stage_test_abc",
                    node="test_node",
                    status="started",
                )
                collector.emit(event)
                collector.flush(timeout=2.0)
                collector.close()

            # 验证两个文件都存在且非空
            jsonl_path = log_dir / "tianshu_run_test-log-001_events.jsonl"
            log_path = log_dir / "tianshu_run_test-log-001_events.log"
            assert jsonl_path.exists()
            assert log_path.exists()
            # JSONL 含事件
            jsonl_content = jsonl_path.read_text(encoding="utf-8")
            assert "test_node" in jsonl_content
            # 文本日志含格式化行
            log_content = log_path.read_text(encoding="utf-8")
            assert "test_node" in log_content
            assert "STARTED" in log_content

    def test_null_collector_does_not_create_log(self):
        """NullCollector 不创建 .log 文件。"""
        with tempfile.TemporaryDirectory() as tmp:
            log_dir = Path(tmp)
            with patch.dict(os.environ, {}, clear=True):
                collector = get_collector(log_dir)
                assert isinstance(collector, NullCollector)
                # emit 不应创建任何文件
                event = StageEvent(
                    run_id="",
                    stage_run_id="stage_test",
                    node="test",
                    status="started",
                )
                collector.emit(event)
                collector.close()

            # 验证没有任何文件被创建
            log_files = list(log_dir.glob("*"))
            assert len(log_files) == 0

    def test_text_log_flushed_on_each_event(self):
        """每个事件写入后立即 flush——支持 tail -f 实时查看。"""
        with tempfile.TemporaryDirectory() as tmp:
            log_dir = Path(tmp)
            with patch.dict(os.environ, {"TIANSHU_RUN_ID": "test-flush-001"}):
                collector = RunLogCollector(log_dir, "test-flush-001")
                log_path = log_dir / "tianshu_run_test-flush-001_events.log"

                # 写入第一个事件
                collector.emit(StageEvent(
                    run_id="test-flush-001",
                    stage_run_id="stage_1",
                    node="node_1",
                    status="started",
                ))
                collector.flush(timeout=2.0)
                content_1 = log_path.read_text(encoding="utf-8")
                assert "node_1" in content_1

                # 写入第二个事件
                collector.emit(StageEvent(
                    run_id="test-flush-001",
                    stage_run_id="stage_2",
                    node="node_2",
                    status="completed",
                    duration_ms=100,
                ))
                collector.flush(timeout=2.0)
                content_2 = log_path.read_text(encoding="utf-8")
                assert "node_2" in content_2
                assert "DONE" in content_2

                collector.close()
```

- [ ] **Step 2: 运行集成测试**

```bash
python -m pytest tests/monitor/test_collector.py -v --tb=short -k "test_writes_both or test_null_collector_does_not or test_text_log_flushed"
```

Expected: 3 passed

- [ ] **Step 3: 提交**

```bash
git add tests/monitor/test_collector.py
git commit -m "test: 新增 RunLogCollector 文本日志集成测试——双写/jsonl不变/NullCollector 零开销"
```

---

### Task 5: 回归验证 + 清理

- [ ] **Step 1: 全量测试**

```bash
python -m pytest tests/monitor/ -x --tb=short -v
```

Expected: 全部通过（原有 + 13 renderer + 3 collector 集成）

- [ ] **Step 2: ruff 检查**

```bash
python -m ruff check .
```

Expected: 零告警

- [ ] **Step 3: git diff 检查**

```bash
git diff --check
```

Expected: 无空白告警

- [ ] **Step 4: 重启服务**

```bash
./dev-reload.sh
```

- [ ] **Step 5: 手动验证——前端 Run-All + `tail -f` 检查 `.log` 文件**

```bash
# 在前端执行 Run-All 后，另开终端
tail -f logs/monitor/tianshu_run_*_events.log
```

Expected: 能看到逐行实时输出的阶段事件（含 STARTED/DONE/FAILED、耗时、错误详情）

- [ ] **Step 6: 最终提交**

```bash
git add -A
git commit -m "chore: 监控人类可读日志——回归全绿，ruff 零告警"
```
