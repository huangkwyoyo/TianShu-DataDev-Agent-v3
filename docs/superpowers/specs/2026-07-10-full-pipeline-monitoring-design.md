# 全流程运行监控——设计规格书

> **状态**：Owner 已确认方案，待转入 writing-plans
> **CRCS 审查**：2×B 类 + 2×C 类 + 1×A 类已全部采纳推荐方案

**目标**：为 SQL 管线（6 节点）、Spark 管线（6 节点）、前端（浏览器运行时）和后端（HTTP 层）提供统一的运行监控，从项目启动到退出的全生命周期日志采集。

**原则**：监控失败不得改变管线结果，不得放宽安全边界，不得记录敏感数据。

---

## 1. 架构总览

```
┌─────────────────────────────────────────────────────┐
│                monitor_dev_run.py                    │
│  run_id="20260710_143022"                           │
│  ├─ 设置环境变量 TIANSHU_RUN_ID                       │
│  ├─ 创建 logs/monitor/ 目录                          │
│  ├─ 启动 ResourceSampler（后台线程，每 5s）             │
│  ├─ 启动 Backend（uvicorn, stdout→*_backend.log）     │
│  ├─ 启动 Frontend（vite, stdout→*_frontend.log）      │
│  ├─ 注册信号处理（SIGINT/SIGTERM→优雅退出+summary）    │
│  └─ 退出时触发 rotation.cleanup()                     │
└─────────────────────────────────────────────────────┘
                          │
          ┌───────────────┼───────────────┐
          ▼               ▼               ▼
   ┌──────────┐   ┌──────────────┐   ┌──────────┐
   │ Backend  │   │   Frontend   │   │ Sampler  │
   │ uvicorn  │   │    vite      │   │ 线程     │
   │ :8000    │   │    :5173     │   │ psutil   │
   └────┬─────┘   └──────┬───────┘   └────┬─────┘
        │                │                │
        ▼                ▼                ▼
   backend.log      frontend.log    resource.jsonl
        │                                │
        ▼                                ▼
   ┌─────────────────────────────────────────┐
   │        RunLogCollector（单写者）          │
   │  - 线程安全队列                           │
   │  - 唯一写入 tianshu_run_*_events.jsonl   │
   │  - 写入失败→告警，不抛异常                 │
   └─────────────────────────────────────────┘
        │
        ▼
   logs/monitor/
   ├── tianshu_run_20260710_143022_events.jsonl
   ├── tianshu_run_20260710_143022_backend.log
   ├── tianshu_run_20260710_143022_frontend.log
   ├── tianshu_run_20260710_143022_resource.jsonl
   └── tianshu_run_20260710_143022_summary.json
```

### 文件职责

| 文件 | 职责 |
|------|------|
| `scripts/monitor_dev_run.py` | 统一启动脚本：生成 run_id、启动前后端、启动采样器、信号处理、退出摘要、轮转 |
| `src/tianshu_datadev/monitor/collector.py` | `RunLogCollector`：线程安全单写者，`stage()` 上下文管理器，`emit()` 事件写入 |
| `src/tianshu_datadev/monitor/middleware.py` | `MonitorMiddleware`：FastAPI 中间件，捕获 HTTP 请求/状态/耗时/异常 |
| `src/tianshu_datadev/monitor/resource_sampler.py` | `ResourceSampler`：后台线程，psutil 采集进程树资源 |
| `src/tianshu_datadev/monitor/rotation.py` | 日志轮转：保留最近 50 组，启动+退出双重触发 |
| `src/tianshu_datadev/monitor/sanitizer.py` | `Sanitizer`：敏感字段白名单过滤 + traceback 脱敏 |
| `src/tianshu_datadev/monitor/models.py` | 事件数据模型（StrictModel）：`MonitorEvent`、`StageEvent`、`ResourceSample` |
| `src/tianshu_datadev/monitor/__init__.py` | 模块公开接口 |
| `frontend/src/monitor/client.ts` | 浏览器端 `monitorClient`：window.onerror、unhandledrejection、fetch 耗时上报 |
| `frontend/src/monitor/__init__.ts` | 前端监控模块入口 |

---

## 2. 关联 ID 模型

```text
run_id           一次 monitor_dev_run.py 生命周期，格式: YYYYMMDD_HHMMSS
                 通过环境变量 TIANSHU_RUN_ID 注入子进程，各模块只读不生成

request_id       一次 HTTP 请求，由 MonitorMiddleware 分配，格式: req_{uuid_short}
                 或复用已有的 request_id（Pipeline._gen_request_id）

stage_run_id     一次管线节点的单次执行，由 collector.stage() 上下文管理器分配
                 格式: stage_{node_name}_{uuid_short}

parent_stage_run_id  嵌套/返工关系的父节点 stage_run_id，顶级节点为 null
```

### 传播路径

```
monitor_dev_run.py
  run_id = "20260710_143022"
  os.environ["TIANSHU_RUN_ID"] = run_id
    │
    ├──> uvicorn 子进程 → collector 读 TIANSHU_RUN_ID
    │       │
    │       └──> MonitorMiddleware: 每个请求分配 request_id
    │               │
    │               └──> collector.stage(node, request_id):
    │                       分配 stage_run_id，记录 parent_stage_run_id
    │
    ├──> vite 子进程 → 仅 stdout 捕获，不参与 ID 传播
    │
    └──> ResourceSampler 线程 → 读 run_id，写 resource.jsonl
```

---

## 3. 前端监控（monitorClient）

### 设计原则

- **零依赖**：纯浏览器 API，不引入第三方 SDK
- **白名单字段**：仅发送 `event_type`、`timestamp`、`url`、`status`、`duration_ms`、`error_message`、`error_type`、`stack_frames`
- **禁止发送**：请求正文、响应正文、Authorization Header、Cookie、localStorage、sessionStorage、数据样本
- **静默失败**：上报失败仅 `console.warn`，不影响用户操作

### 采集点

```typescript
// frontend/src/monitor/client.ts

interface MonitorPayload {
  event_type: 'api_call' | 'js_error' | 'promise_rejection';
  timestamp: string;              // ISO 8601
  run_id: string;                 // 由后端 /api/monitor/config 返回
  url?: string;                   // window.location.href
  api_path?: string;              // 仅 API 路径（如 /api/execute-rich），不含 query
  api_status?: number;            // HTTP 状态码
  api_duration_ms?: number;       // fetch 耗时
  error_type?: string;            // 异常类型名
  error_message?: string;         // 异常消息（脱敏后）
  stack_frames?: string[];        // 栈帧 [文件:函数:行号]，不含 locals
}

// 1. fetch 包装——自动记录 API 调用耗时和状态
//    拦截 fetch() 原型，对 /api/* 请求计时
//    不记录 request body、response body、headers

// 2. window.onerror
//    捕获 JS 运行时异常，提取 message/type/stack（仅文件:行号）

// 3. window.onunhandledrejection
//    捕获未处理的 Promise rejection
```

### 后端上报端点

```
POST /api/monitor/browser-event
  Body: MonitorPayload（JSON）
  校验: 字段白名单 + 大小限制（max 4KB）+ run_id 匹配
  响应: 204 No Content（静默成功/失败）
```

端点**不经过 MonitorMiddleware**（避免无限循环），直接写入 `RunLogCollector` 队列。

---

## 4. RunLogCollector——单写者设计

### 线程安全

```python
class RunLogCollector:
    """结构化事件单写者——唯一写入 _events.jsonl 的组件。

    设计约束：
    - 所有事件写入通过队列（queue.Queue），单消费者线程写入文件
    - emit() 非阻塞——put_nowait + 队列满时丢弃并告警
    - 写入失败→logging.warning，不抛出
    - run_id 从环境变量 TIANSHU_RUN_ID 读取，不可自行生成
    """

    def __init__(self, log_dir: Path, max_queue: int = 10000):
        self._queue: queue.Queue[MonitorEvent] = queue.Queue(maxsize=max_queue)
        self._writer_thread: threading.Thread
        self._running: bool = False

    def emit(self, event: MonitorEvent) -> None:
        """非阻塞入队——队列满时丢弃并告警。"""

    def stage(
        self, node: str, request_id: str,
        parent_stage_run_id: str | None = None,
    ) -> "StageContext":
        """上下文管理器——进入时记录 started，退出时记录 completed/failed。

        用法:
            with collector.stage("sql_compiler", request_id) as ctx:
                compiled = compiler.compile(plan)
                ctx.set_result(row_count=...)
            # 退出时自动记录：status + duration_ms + error（如有）
        """

    def log_resource_sample(self, sample: ResourceSample) -> None:
        """写入资源采样事件。"""

    def log_browser_event(self, payload: MonitorPayload) -> None:
        """写入浏览器上报事件。"""
```

### StageContext 上下文管理器

```python
@dataclass
class StageContext:
    collector: RunLogCollector
    stage_run_id: str
    node: str
    request_id: str
    parent_stage_run_id: str | None
    started_at: float
    _result: dict | None = None

    def set_result(self, **kwargs) -> None:
        """设置阶段产出元信息（row_count, artifact_sha256 等）。"""

    def __enter__(self) -> "StageContext":
        self.collector.emit(StageEvent(
            stage_run_id=self.stage_run_id,
            status="started",
            node=self.node,
            ...
        ))
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.collector.emit(StageEvent(status="completed", ...))
        else:
            self.collector.emit(StageEvent(
                status="failed",
                error_type=exc_type.__name__,
                error_message=str(exc_val),
                stack_frames=_sanitize_traceback(exc_tb),  # 仅文件名:函数:行号
                ...
            ))
        # 关键：绝不吞异常——return False 让异常继续传播
        return False
```

---

## 5. 全入口覆盖矩阵

### SQL 管线（Pipeline 方法 → 节点）

| API 端点 | Pipeline 方法 | 监控节点 |
|----------|--------------|---------|
| `POST /api/spec/parse` | `parse_only()` | `sql_parser` |
| `POST /api/spec/parse-rich` | `parse_rich()` | `sql_parser`, `sql_enricher` |
| `POST /api/plan` | `build_plan()` | `sql_parser`, `sql_enricher`, `sql_builder`, `sql_validator` |
| `POST /api/plan-rich` | `build_plan_rich()` | 同上 |
| `POST /api/execute` | `execute()` | `sql_parser`, `sql_enricher`, `sql_builder`, `sql_validator`, `sql_compiler`, `sql_executor` |
| `POST /api/execute-rich` | `execute_rich()` | 同上 + `snapshot_builder` |
| `POST /api/run-all` | `run_all()` | 同上 + `contract_extractor`, `packager` |
| `POST /api/run-all-rich` | `run_all_rich()` | 同上 |

### Spark 管线（Orchestrator/Dispatcher）

| API 端点 | 触发方式 | 监控节点 |
|----------|---------|---------|
| `POST /api/spark/map` | `run_spark_stage(request_id, MAPPER)` | `spark_mapper` |
| `POST /api/spark/develop` | `run_spark_stage(request_id, DEVELOPER)` | `spark_developer` |
| `POST /api/spark/compile` | `run_spark_stage(request_id, COMPILER)` | `spark_compiler` |
| `POST /api/spark/validate` | `run_spark_stage(request_id, VALIDATOR)` | `spark_validator` |
| `POST /api/spark/compare` | `run_spark_stage(request_id, COMPARATOR)` | `spark_comparator` |
| `POST /api/spark/physical-verify` | `run_spark_stage(request_id, PHYSICAL_VERIFIER)` | `spark_physical_verifier` |
| `POST /api/spark/verify` | `SparkOrchestrator.run()` 全链路 | 上述 6 节点自动触发 |

### 埋点策略

- **不建立第二套状态机**：`collector.stage()` 只观察现有确定性状态（`_record_trace`、`stage_results`），不自推结论
- **复用现有追踪**：在 `_record_trace()` 调用处追加 `collector.emit()`，在 `stage_results` 写入处追加 `collector.emit()`
- **覆盖返工路径**：`run_spark_stage()` 的每个调用都是独立 `stage_run_id`，`parent_stage_run_id` 关联到原始请求

### HTTP 层（MonitorMiddleware）

- **before**：记录 `request_id`、`method`、`path`（不含 query）、`start_time`（注入 `request.state`）
- **after**：记录 `status_code`、`duration_ms`
- **exception**：记录 `error_type`、`error_message`、`stack_frames`（脱敏），**不捕获异常**（异常继续传播给 error_handlers）
- **白名单路径**：仅监控 `/api/*`，排除 `/api/health`、`/api/monitor/*`

---

## 6. 敏感数据白名单与脱敏

### 默认记录（白名单）

| 字段 | 来源 | 说明 |
|------|------|------|
| `run_id` | 环境变量 | 运行标识 |
| `request_id` | MonitorMiddleware | 请求标识 |
| `stage_run_id` | collector.stage() | 阶段标识 |
| `parent_stage_run_id` | collector.stage() | 父阶段标识 |
| `timestamp` | time.time() | ISO 8601 |
| `event_type` | 分类 | `stage`/`http`/`resource`/`browser` |
| `node` | 分类 | 管线节点名 |
| `status` | 枚举 | `started`/`completed`/`failed`/`skipped` |
| `duration_ms` | 计时 | 整数 |
| `error_type` | 异常类名 | 仅类型名 |
| `error_code` | ApiError | 业务错误码 |
| `error_message` | 脱敏后 | 最多 500 字符 |
| `stack_frames` | 脱敏后 | `[{file, function, lineno}]`，不含 locals |
| `status_code` | HTTP | 整数 |
| `cpu_percent` | psutil | 浮点 |
| `rss_mb` | psutil | 浮点 |
| `vms_mb` | psutil | 浮点 |
| `artifact_path` | Pipeline | 文件路径字符串 |
| `artifact_sha256` | Pipeline | 64 字符 hex |
| `row_count` | Executor | 整数 |

### 禁止记录（黑名单）

| 类别 | 示例 |
|------|------|
| 凭据 | Token、API Key、Password |
| 连接串 | `postgresql://user:pass@host/db` |
| 请求/响应体 | DeveloperSpec Markdown、SQL 文本、PySpark 代码、JSON body |
| Headers | Authorization、Cookie、Set-Cookie、X-API-Key |
| 客户端 IP | `request.client.host` |
| 数据样本 | CSV 行、Parquet 内容、DataFrame.show() 输出 |
| 局部变量 | traceback 中的 `locals()`、`f_locals` |
| URL Query | `?token=xxx&key=yyy` |
| 环境变量值 | `os.environ` 完整 dump |

### --debug-artifacts 模式

显式传入 `--debug-artifacts` 后：
- 记录 `generated_sql` 和 `raw_pyspark` 到 `_debug/` 子目录（不嵌入 JSONL）
- 对 SQL/PySpark 中的字符串字面量执行 `****` 掩码
- 每个 artifact 文件单独计算 SHA-256 并记录到 events.jsonl

### Sanitizer 实现

```python
class Sanitizer:
    """敏感字段白名单过滤器。

    - _sanitize_traceback(tb): 遍历栈帧，只保留 (filename, function_name, lineno)
    - _sanitize_error_message(msg): 截断到 500 字符，移除路径中的用户名
    - _sanitize_url(url): 移除 query string
    - _validate_event(event): 断言事件不含黑名单字段，失败时丢弃并告警
    """
```

---

## 7. 异常传播不变性

### 硬规则

```
监控代码的 try/except 只能出现在以下 3 个边界内：

1. RunLogCollector.emit()         —— 队列写入、文件写入失败
2. ResourceSampler._sample()      —— psutil 调用失败
3. Rotation.cleanup()             —— 文件删除失败

其他任何位置的监控调用（collector.stage()、middleware、browser-event）
不得捕获异常——异常必须原样传播给上层业务代码。
```

### 安全实现

```python
# ✅ 正确——emit 内部安全包裹
def emit(self, event):
    try:
        self._queue.put_nowait(event)
    except queue.Full:
        logging.warning("监控队列满，事件丢弃")

# ✅ 正确——stage() __exit__ 返回 False（不吞异常）
def __exit__(self, exc_type, exc_val, exc_tb):
    try:
        self._emit_stage_event(...)
    except Exception:
        logging.warning("监控事件写入失败")
    return False  # 关键——异常继续传播

# ❌ 错误——在业务调用处包裹 try/except
try:
    with collector.stage(...):
        compiler.compile(plan)
except Exception:  # 这会在监控失败时误吞业务异常
    pass
```

### 测试需求

必须实现测试证明：**有无监控代码时，同一业务异常的类型、error_code 和 Pipeline 响应完全一致**。

---

## 8. 资源采样器（ResourceSampler）

### 采集目标

- **Python 进程树**：uvicorn reloader + worker、multiprocessing.spawn 子进程
- **Node 进程树**：node.exe（vite）、npx、esbuild
- **Spark JVM 进程树**：java.exe（SparkSubmit）、python.exe（PySpark driver）
- **系统级**：总 CPU、总内存（可选）

### 行为

```python
class ResourceSampler:
    """后台线程，每 5 秒采集一次进程树资源指标。

    - 通过 psutil.Process.children(recursive=True) 遍历进程树
    - 采集: pid, name, cmdline_truncated, cpu_percent, rss_mb, vms_mb, num_threads
    - cmdline 截断到 200 字符，过滤掉含密码/Token 的参数（--password/--token/--key）
    - 采集失败→logging.warning，继续下一次采样
    - 写入 resource.jsonl（独立文件，不经过 RunLogCollector 队列——避免阻塞事件流）
    - 启动时即刻采集一次（冷启动基线），之后按间隔
    """

    def start(self) -> None: ...
    def stop(self) -> None: ...
    def _sample(self) -> ResourceSample: ...
```

---

## 9. 日志轮转（Rotation）

### 规则

- **保留单位**：一次完整的 `run_id`（一组 5 个文件 = 1 组）
- **保留数量**：最近 50 组
- **清理时机**：
  1. `monitor_dev_run.py` 启动时（清理上次崩溃残留）
  2. `monitor_dev_run.py` 正常退出时（清理超过 50 组的旧日志）
- **保护机制**：当前 `run_id` 的日志组**绝对不删**
- **原子性**：删除前先 `os.rename` 到临时名，确认后再 `os.unlink`
- **失败处理**：单个文件删除失败→`logging.warning`，继续处理下一个，不阻断

### 实现

```python
def cleanup(log_dir: Path, current_run_id: str, keep_groups: int = 50) -> int:
    """清理旧日志组，保留最近 keep_groups 组。

    按 run_id 分组（文件前缀 tianshu_run_{run_id}_），
    按 run_id 字符串排序（即时间顺序），保留最后 keep_groups 组。

    Returns:
        删除的组数
    """
```

---

## 10. 文件结构汇总

```
新增文件：
  scripts/monitor_dev_run.py
  src/tianshu_datadev/monitor/__init__.py
  src/tianshu_datadev/monitor/collector.py
  src/tianshu_datadev/monitor/middleware.py
  src/tianshu_datadev/monitor/resource_sampler.py
  src/tianshu_datadev/monitor/rotation.py
  src/tianshu_datadev/monitor/sanitizer.py
  src/tianshu_datadev/monitor/models.py
  frontend/src/monitor/client.ts
  frontend/src/monitor/__init__.ts
  tests/monitor/__init__.py
  tests/monitor/test_collector.py
  tests/monitor/test_middleware.py
  tests/monitor/test_resource_sampler.py
  tests/monitor/test_rotation.py
  tests/monitor/test_sanitizer.py
  tests/monitor/test_integration.py

修改文件：
  src/tianshu_datadev/api/app.py              # 添加 MonitorMiddleware
  src/tianshu_datadev/api/routes.py           # 添加 /api/monitor/browser-event + /api/monitor/config
  src/tianshu_datadev/api/pipeline.py         # 注入 collector，stage() 包裹节点
  src/tianshu_datadev/spark/orchestrator.py   # stage() 包裹 6 节点
  frontend/src/main.tsx                       # 初始化 monitorClient
  .gitignore                                  # 添加 logs/monitor/
```

---

## 11. 安全清单

- [x] `--debug-artifacts` 默认关闭
- [x] traceback 不含 `locals()` / `f_locals`
- [x] URL query 参数脱敏或丢弃
- [x] 禁止 `client_ip`
- [x] 监控端点限制请求大小（4KB）和字段白名单
- [x] `logs/monitor/` 在 `.gitignore` 中
- [x] 监控异常不改变 Pipeline 状态和异常类型
- [x] 默认不记录 SQL/PySpark/DeveloperSpec
- [x] `cmdline` 过滤 `--password`/`--token`/`--key` 参数
- [x] `browser-event` 端点不经过 MonitorMiddleware（避免无限循环）

---

## 12. 事件 JSONL 格式示例

```jsonl
{"event_type":"stage","run_id":"20260710_143022","request_id":"req_a1b2","stage_run_id":"stage_sql_compiler_c3d4","parent_stage_run_id":null,"node":"sql_compiler","status":"started","timestamp":"2026-07-10T14:30:22.123456","duration_ms":null,"error_type":null,"error_code":null,"error_message":null,"stack_frames":null,"artifact_sha256":null,"row_count":null}
{"event_type":"stage","run_id":"20260710_143022","request_id":"req_a1b2","stage_run_id":"stage_sql_compiler_c3d4","parent_stage_run_id":null,"node":"sql_compiler","status":"completed","timestamp":"2026-07-10T14:30:22.456789","duration_ms":333,"error_type":null,"error_code":null,"error_message":null,"stack_frames":null,"artifact_sha256":"abc123def456","row_count":804}
{"event_type":"http","run_id":"20260710_143022","request_id":"req_a1b2","method":"POST","path":"/api/execute-rich","status_code":200,"duration_ms":450}
{"event_type":"browser","run_id":"20260710_143022","api_path":"/api/execute-rich","api_status":200,"api_duration_ms":480,"error_type":null,"error_message":null,"stack_frames":null}
{"event_type":"resource","run_id":"20260710_143022","timestamp":"2026-07-10T14:30:25.000000","processes":[{"pid":33972,"name":"python.exe","cpu_percent":12.5,"rss_mb":156.3,"vms_mb":420.1,"num_threads":8}]}
```

---

## 13. summary.json 格式

```json
{
  "run_id": "20260710_143022",
  "started_at": "2026-07-10T14:30:22.000000",
  "ended_at": "2026-07-10T14:45:10.000000",
  "duration_seconds": 888,
  "exit_code": 0,
  "stages": {
    "sql_parser": {"total": 3, "completed": 3, "failed": 0},
    "sql_compiler": {"total": 3, "completed": 3, "failed": 0},
    "spark_physical_verifier": {"total": 1, "completed": 1, "failed": 0}
  },
  "errors": [
    {"node": "sql_executor", "error_type": "DuckDBError", "error_code": "RUNTIME_FAIL", "error_message": "Binder Error: Table 'xxx' not found", "timestamp": "2026-07-10T14:32:15.000"}
  ],
  "peak_resources": {
    "python_max_rss_mb": 512.3,
    "node_max_rss_mb": 320.1,
    "java_max_rss_mb": 1024.5,
    "system_max_cpu_percent": 45.2
  },
  "artifact_count": 12,
  "log_files": [
    "tianshu_run_20260710_143022_events.jsonl",
    "tianshu_run_20260710_143022_backend.log",
    "tianshu_run_20260710_143022_frontend.log",
    "tianshu_run_20260710_143022_resource.jsonl",
    "tianshu_run_20260710_143022_summary.json"
  ]
}
```
