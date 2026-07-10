# 监控人类可读日志——设计文档

> **状态**：已确认，待实施计划

## 目标

在保持 JSONL 机器消费不变的前提下，额外生成一份程序员可读的文本日志（`logs/monitor/tianshu_run_{run_id}_events.log`），支持 `tail -f` 实时查看和事后排查。

## 正确行为

### 实时 `tail -f` 效果

```
19:49:42.003 [INFO ] ▶ sql_parser             STARTED
19:49:42.003 [ERROR] ✗ sql_parser             FAILED    0ms  E001: 未找到 markdown 代码块
                                                      ↳ parser.py:206 _extract_fenced_block
                                                      ↳ parser.py:286 parse
                                                      ↳ pipeline.py:436 _parse_and_enrich
19:49:42.055 [INFO ] → POST /api/pipeline/execute  200  52ms  hreq_b27fda23
19:50:37.003 [INFO ] ▶ spark_physical_verifier STARTED
19:50:37.016 [DEBUG]   artifact_path: spec/abc123def456
19:50:37.016 [DEBUG]   artifact_sha256: e3b0c44298fc1c149afbf4c8996fb924...
19:52:24.051 [INFO ] ✓ spark_physical_verifier DONE   107048ms
19:52:24.051 [DEBUG]   row_count: 265
19:52:24.055 [INFO ] → POST /api/spark/physical-verify 200  107053ms  hreq_b27fda23
```

### 格式规则

| 事件类型 | 级别 | 图标 | 格式 |
|---------|------|------|------|
| StageEvent started | INFO | `▶` | `时间 [INFO ] ▶ {node:28s} STARTED` |
| StageEvent completed | INFO | `✓` | `时间 [INFO ] ✓ {node:28s} DONE {duration}ms` |
| StageEvent failed | ERROR | `✗` | `时间 [ERROR] ✗ {node:28s} FAILED {duration}ms {error_msg}` + 缩进 stack |
| StageEvent skipped | WARN | `○` | `时间 [WARN ] ○ {node:28s} SKIPPED` |
| HttpEvent | INFO | `→` | `时间 [INFO ] → {method} {path:40s} {status} {duration}ms {hreq_id}` |
| BrowserEvent | ERROR | `🌐` | `时间 [ERROR] 🌐 BROWSER {api_path:40s} {error_msg}` + 缩进 stack |
| ResourceSample | DEBUG | `📊` | 默认不输出；`TIANSHU_LOG_RESOURCE=1` 时启用 |

**DEBUG 字段输出**：非空可选字段（`artifact_path`, `artifact_sha256`, `row_count`, `error_code` 等）在 `[DEBUG]` 级别下紧跟主行缩进打印。

## 不可触及的边界

| # | 边界 | 原因 |
|---|------|------|
| 1 | JSONL schema 不动 | 已有下游依赖（ResourceSampler 峰值聚合、rotation 清理策略） |
| 2 | `collector.emit()` 签名不动 | 保持非阻塞 `put_nowait` 语义——文本写入在消费者线程完成 |
| 3 | NullCollector 行为不动 | `TIANSHU_RUN_ID` 未设置时仍为零开销 |
| 4 | Pipeline 调用代码不动 | 不改 `pipeline.py` 中任何 `collector.stage()` 调用 |
| 5 | 不引入第三方依赖 | 纯标准库（`datetime`, `json`, `textwrap`） |
| 6 | 文本日志不做轮转管理 | 轮转仍由 `rotation.py` 负责，文本日志随 JSONL 一起清理 |
| 7 | 不改变 Sanitizer 规则 | 文本日志输出的字段和 JSONL 一样经过 sanitize |

## 架构

```
collector.emit(event)
  └── 消费者线程 _writer_loop
        ├── JSONL 写入（不变）
        └── LogRenderer.format_event() → 文本写入 .log（新增）

lifespan shutdown
  └── collector.close()
        └── LogRenderer.render_file(jsonl, log) → 最终补齐（新增）
```

`renderer.py` 是唯一知道文本长什么样的文件——`format_event()` 是纯函数，`collector.py` 只调用它，不自行拼字符串。

## 文件变更

| 文件 | 操作 | 内容 |
|------|------|------|
| `monitor/renderer.py` | **新建** | `LogRenderer` 类——`format_event()` + `render_file()` |
| `monitor/collector.py` | **修改** | 消费者线程新增文本写入（~15 行增量） |
| `monitor/lifespan.py` | **修改** | close() 后调用最终补齐（~3 行增量） |
| `monitor/__init__.py` | **修改** | 导出 `LogRenderer` |
| `tests/monitor/test_renderer.py` | **新建** | 10+ 单测 |

## 验收方式

| 层级 | 验收项 | 方法 |
|------|--------|------|
| 单元 | `format_event(stage_started)` 正确格式 | pytest——构造 event dict，断言输出字符串 |
| 单元 | `format_event(stage_failed)` 含缩进 stack | pytest——`error_type="ValueError"` + `stack_frames=[...]`，断言含 `↳` |
| 单元 | `format_event(stage_completed)` 含 DEBUG 字段 | pytest——`row_count=265` 等，断言 DEBUG 行 |
| 单元 | `format_event(http)` 格式正确 | pytest——method/path/status/duration 全部出现 |
| 单元 | `format_event(resource)` 默认返回 None | pytest——`TIANSHU_LOG_RESOURCE` 未设置时返回 None |
| 单元 | `render_file(jsonl_path)` 返回正确行数 | pytest——3 行 JSONL → 输出文件 3 行 |
| 单元 | NullCollector 不受影响 | pytest——确认无 `.log` 文件创建 |
| 集成 | `tail -f` 实时可见 | `./dev-reload.sh` → Run-All → `tail -f` 逐行输出 |
| 回归 | 现有监控测试全绿 | `pytest tests/monitor/ -x --tb=short` |
| 回归 | ruff 零告警 | `python -m ruff check .` |
| 回归 | JSONL 输出不变 | 新旧 JSONL 内容一致 |

## 实施任务（共 5 个）

1. **Task 1**：新建 `renderer.py`——格式引擎（纯函数，独立可测）
2. **Task 2**：修改 `collector.py`——消费者线程增加实时文本写入
3. **Task 3**：修改 `lifespan.py`——close() 后最终补齐
4. **Task 4**：新建 `test_renderer.py`——10 个单元测试
5. **Task 5**：回归测试 + ruff 检查 + `./dev-reload.sh`
