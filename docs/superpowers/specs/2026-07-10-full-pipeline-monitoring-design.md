# 全流程运行监控——设计规格书 v2

> **状态**：Owner 已确认方案（含第二轮 7 项补丁），待转入 writing-plans
> **CRCS 审查**：2×B 类 + 2×C 类 + 1×A 类 + 第二轮 7 项全部采纳推荐方案

**目标**：为 SQL 管线（6 节点）、Spark 管线（6 节点）、前端（浏览器运行时）和后端（HTTP 层）提供统一的运行监控，从项目启动到退出的全生命周期日志采集。

**范围**：本期仅覆盖 `monitor_dev_run.py` 启动的 Web 开发流程。未设置 `TIANSHU_RUN_ID` 环境变量时，监控系统以 NullCollector 模式运行（零开销）。

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
| `scripts/monitor_dev_run.py` | 统一启动脚本：生成 run_id（碰撞时追加随机后缀）、monitor_token、启动前后端、启动采样器、信号处理、等待子进程退出后生成 summary、轮转 |
| `src/tianshu_datadev/monitor/collector.py` | `RunLogCollector` + `NullCollector`：线程安全单写者，`stage()` 上下文管理器，`emit()` 事件写入；无 TIANSHU_RUN_ID 时返回 NullCollector（零开销空操作） |
| `src/tianshu_datadev/monitor/middleware.py` | `MonitorMiddleware`：FastAPI 中间件，捕获 HTTP 请求/状态/耗时/异常；`/api/monitor/*` 排除在外 |
| `src/tianshu_datadev/monitor/resource_sampler.py` | `ResourceSampler`：后台线程，psutil 采集进程树资源，按阶段时间窗口关联，指标命名 `peak_observed_*` |
| `src/tianshu_datadev/monitor/rotation.py` | 日志轮转：保留最近 50 组，启动+退出双重触发 |
| `src/tianshu_datadev/monitor/sanitizer.py` | `Sanitizer`：敏感字段白名单过滤 + traceback 脱敏 |
| `src/tianshu_datadev/monitor/models.py` | 事件数据模型（StrictModel）：`MonitorEvent`、`StageEvent`、`ResourceSample`，所有时间戳为带时区 ISO 8601 |
| `src/tianshu_datadev/monitor/__init__.py` | 模块公开接口 + `get_collector()` 工厂——根据 TIANSHU_RUN_ID 返回 RunLogCollector 或 NullCollector |
| `src/tianshu_datadev/monitor/lifespan.py` | FastAPI lifespan 回调——startup 初始化 collector，shutdown 执行 flush/close |
| `frontend/src/monitor/client.ts` | 浏览器端 `monitorClient`：先 GET `/api/monitor/config` 确认 enabled，再采集 window.onerror、unhandledrejection、fetch 耗时；携带 monitor_token |
| `frontend/src/monitor/__init__.ts` | 前端监控模块入口 |

---

## 2. 关联 ID 模型

```text
run_id                一次 monitor_dev_run.py 生命周期，格式: YYYYMMDD_HHMMSS_{random4}
                      生成规则：优先精确到秒，若同名目录已存在则追加 4 位随机小写 hex
                      示例：20260710_143022、20260710_143022_a3f1
                      通过环境变量 TIANSHU_RUN_ID 注入子进程，各模块只读不生成

http_request_id       一次 HTTP 请求的唯一标识，由 MonitorMiddleware 分配
                      格式: hreq_{uuid_short}
                      与 artifact 存储无关——仅用于 HTTP 层事件关联

artifact_request_id   一次 Pipeline 产物存储的唯一标识，由 Pipeline._gen_request_id(spec) 生成
                      格式: 已有的 spec-hash 风格（不变）
                      用于 stage 事件与 artifacts 的关联

stage_run_id          一次管线节点的单次执行，由 collector.stage() 上下文管理器分配
                      格式: stage_{node_name}_{uuid_short}

parent_stage_run_id   仅表示真实阶段嵌套关系——例如 Spark 全链路 verify 包含 6 个子阶段，
                      此时子阶段的 parent_stage_run_id = verify 的 stage_run_id。
                      顶级节点为 null。返工/重试是独立 stage_run_id，不使用 parent。
```

### ID 关系图

```
HTTP 请求 (http_request_id = "hreq_a1b2")
  │
  ├── Pipeline.execute_rich()
  │     artifact_request_id = "abc123"  (由 _gen_request_id 生成)
  │     │
  │     ├── collector.stage("sql_parser", artifact_request_id)
  │     │     stage_run_id = "stage_sql_parser_c3d4", parent = null
  │     │
  │     ├── collector.stage("sql_compiler", artifact_request_id)
  │     │     stage_run_id = "stage_sql_compiler_e5f6", parent = null
  │     │
  │     └── collector.stage("sql_executor", artifact_request_id)
  │           stage_run_id = "stage_sql_executor_g7h8", parent = null
  │
  └── Spark verify (artifact_request_id = "abc123")
        │
        ├── collector.stage("spark_verify", artifact_request_id)
        │     stage_run_id = "stage_spark_verify_x1", parent = null
        │     │
        │     ├── collector.stage("spark_mapper", artifact_request_id,
        │     │                   parent_stage_run_id="stage_spark_verify_x1")
        │     │
        │     ├── collector.stage("spark_compiler", artifact_request_id,
        │     │                   parent_stage_run_id="stage_spark_verify_x1")
        │     │
        │     └── ... (其余子阶段同上)
```

### 传播路径

```
monitor_dev_run.py
  run_id = "20260710_143022"  (或 "20260710_143022_a3f1" 碰撞时)
  monitor_token = secrets.token_hex(16)
  os.environ["TIANSHU_RUN_ID"] = run_id
  os.environ["TIANSHU_MONITOR_TOKEN"] = monitor_token
    │
    ├──> uvicorn 子进程 → collector 读 TIANSHU_RUN_ID
    │       │
    │       ├──> lifespan startup: get_collector() → RunLogCollector (if TIANSHU_RUN_ID set)
    │       │
    │       ├──> MonitorMiddleware: 每个请求分配 http_request_id
    │       │
    │       ├──> Pipeline: 每个业务调用分配 artifact_request_id
    │       │     └──> collector.stage(node, artifact_request_id, parent=...)
    │       │
    │       └──> lifespan shutdown: collector.flush() + collector.close()
    │
    ├──> vite 子进程 → 仅 stdout 捕获，不参与 ID 传播
    │
    └──> ResourceSampler 线程 → 读 run_id，写 resource.jsonl
```

---

## 3. 前端监控（monitorClient）

### 启用检测

前端初始化时先 `GET /api/monitor/config`。响应 `{"enabled": false}` 时跳过所有监控逻辑，不注册任何事件监听器。

```typescript
// GET /api/monitor/config 响应
interface MonitorConfig {
  enabled: boolean;               // false = NullCollector 模式
  run_id?: string;                // 仅 enabled=true 时返回
  monitor_token?: string;         // 仅 enabled=true 时返回，浏览器携带此 token
  rate_limit_per_minute?: number; // 默认 20
  max_total_events?: number;      // 默认 200
}
```

当 `enabled=false` 时，`POST /api/monitor/browser-event` 返回 **404**，不暴露监控未启用的原因。

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
  timestamp: string;              // 带时区 ISO 8601，如 "2026-07-10T14:30:22.123+08:00"
  run_id: string;                 // 由 /api/monitor/config 返回
  monitor_token: string;          // 由 /api/monitor/config 返回
  url?: string;                   // window.location.href（不含 query/hash）
  api_path?: string;              // 仅 API 路径（如 /api/execute-rich），不含 query
  api_status?: number;            // HTTP 状态码
  api_duration_ms?: number;       // fetch 耗时（整数）
  error_type?: string;            // 异常类型名
  error_message?: string;         // 异常消息（脱敏后，截断 500 字符）
  stack_frames?: string[];        // 栈帧 ["file:function:lineno"]，不含 locals
}

// 1. fetch 包装——自动记录 API 调用耗时和状态
//    拦截 fetch() 原型，对 /api/* 请求计时
//    不记录 request body、response body、headers
//    不记录非 /api/* 请求

// 2. window.onerror
//    捕获 JS 运行时异常，提取 message/type/stack（仅文件:行号）

// 3. window.onunhandledrejection
//    捕获未处理的 Promise rejection
```

### 后端上报端点——安全边界

```
POST /api/monitor/browser-event
  Headers: Origin: http://127.0.0.1:5173 (必须)

  安全校验链（任一失败返回 403/404，不暴露具体原因）：
  1. 监控未启用 → 404
  2. Origin 不在白名单 → 403
     - http://127.0.0.1:5173
     - http://localhost:5173
  3. monitor_token 缺失或不匹配 → 403
  4. run_id 与 TIANSHU_RUN_ID 不匹配 → 403
  5. 速率限制 → 429（每 run_id 每分钟 20 条）
  6. 总量限制 → 429（每 run_id 总共 200 条）
  7. 请求体 > 4KB → 413

  Body: MonitorPayload（JSON——仅白名单字段通过校验）
  响应: 204 No Content（成功）/ 4xx（静默忽略）
```

端点**不经过 MonitorMiddleware**（避免无限循环），直接写入 `RunLogCollector` 队列。

---

## 4. NullCollector——零开销禁用模式

当环境变量 `TIANSHU_RUN_ID` 未设置时（普通 API 启动、E2E 测试、CI 等场景），`get_collector()` 返回 `NullCollector` 单例——所有方法为空操作，零性能开销。

```python
class NullCollector:
    """无 TIANSHU_RUN_ID 时的空操作采集器。

    所有方法返回有意义但无害的默认值：
    - stage() 返回 NullStageContext（with 语句正常通过）
    - emit() → no-op
    - flush() / close() → no-op
    - enabled = False
    """

    enabled: bool = False

    def stage(self, *args, **kwargs) -> "NullStageContext": ...
    def emit(self, event) -> None: ...
    def flush(self) -> None: ...
    def close(self) -> None: ...


def get_collector(log_dir: Path | None = None) -> RunLogCollector | NullCollector:
    """工厂——根据 TIANSHU_RUN_ID 环境变量返回对应采集器。

    - TIANSHU_RUN_ID 已设置 → RunLogCollector(run_id, log_dir)
    - 未设置 → NullCollector() 单例
    """
```

### /api/monitor/config 响应

```json
// 监控已启用（monitor_dev_run.py 启动）
{"enabled": true, "run_id": "20260710_143022", "monitor_token": "a1b2c3...",
 "rate_limit_per_minute": 20, "max_total_events": 200}

// 监控未启用（普通 uvicorn 启动）
{"enabled": false}
```

## 5. RunLogCollector——单写者 + FastAPI Lifespan

### 线程安全

```python
class RunLogCollector:
    """结构化事件单写者——唯一写入 _events.jsonl 的组件。

    设计约束：
    - 所有事件写入通过队列（queue.Queue），单消费者线程写入文件
    - emit() 非阻塞——put_nowait + 队列满时丢弃并告警（计入 dropped_event_count）
    - 写入失败→logging.warning，不抛出
    - run_id 从环境变量 TIANSHU_RUN_ID 读取，不可自行生成
    - flush() 等待队列排空（带超时），close() 关闭文件句柄
    """

    def __init__(self, log_dir: Path, run_id: str, max_queue: int = 10000):
        self._queue: queue.Queue[MonitorEvent] = queue.Queue(maxsize=max_queue)
        self._writer_thread: threading.Thread
        self._running: bool = False
        self.dropped_event_count: int = 0   # 队列满丢弃计数
        self.flush_completed: bool = False   # shutdown 时 flush 是否成功
        self.run_complete: bool = False      # 后端进程是否正常退出

    def emit(self, event: MonitorEvent) -> None:
        """非阻塞入队——队列满时丢弃并告警，dropped_event_count += 1。"""

    def stage(
        self, node: str, artifact_request_id: str,
        parent_stage_run_id: str | None = None,
    ) -> "StageContext":
        """上下文管理器——进入时记录 started，退出时记录 completed/failed。

        用法:
            with collector.stage("sql_compiler", artifact_request_id) as ctx:
                compiled = compiler.compile(plan)
                ctx.set_result(artifact_path="...", artifact_sha256="...")
            # 退出时自动记录：status + duration_ms + error（如有）
        """

    def log_resource_sample(self, sample: ResourceSample) -> None:
        """写入资源采样事件。"""

    def log_browser_event(self, payload: MonitorPayload) -> None:
        """写入浏览器上报事件。"""

    def flush(self, timeout: float = 5.0) -> bool:
        """等待队列排空——shutdown 时调用，设置 flush_completed。"""

    def close(self) -> None:
        """关闭文件句柄——flush 之后调用。"""
```

### FastAPI Lifespan 集成

```python
# src/tianshu_datadev/monitor/lifespan.py

from contextlib import asynccontextmanager
from fastapi import FastAPI

@asynccontextmanager
async def monitor_lifespan(app: FastAPI):
    """监控生命周期——绑定到 FastAPI app lifespan。

    startup:
      - 调用 get_collector() 初始化采集器
      - 注入到 app.state.monitor_collector

    shutdown:
      - collector.flush() 排空队列
      - collector.close() 关闭文件
      - 设置 flush_completed=True, run_complete=True
      - 异常时仅 logging.warning，不阻断 shutdown
    """
    # startup
    collector = get_collector(...)
    app.state.monitor_collector = collector
    yield
    # shutdown
    try:
        collector.flush_completed = collector.flush(timeout=10.0)
    except Exception:
        logging.warning("监控 flush 失败")
    finally:
        collector.run_complete = True
        collector.close()
```

### 启动脚本等待流程

```
monitor_dev_run.py
  │
  ├── 1. 生成 run_id + monitor_token
  ├── 2. 设置环境变量 TIANSHU_RUN_ID + TIANSHU_MONITOR_TOKEN
  ├── 3. 调用 rotation.cleanup()  ← 启动时轮转
  ├── 4. 启动 ResourceSampler
  ├── 5. 启动 Backend (uvicorn 子进程)
  ├── 6. 启动 Frontend (vite 子进程)
  ├── 7. 健康检查（轮询:8000 + :5173）
  ├── 8. 打印 "监控已就绪——run_id=20260710_143022"
  ├── 9. 等待 SIGINT/SIGTERM 或子进程退出
  │
  ├── 10. 终止 Frontend 子进程
  ├── 11. 终止 Backend 子进程
  ├── 12. 等待 Backend 子进程完全退出  ← 确保 lifespan shutdown 已执行 flush
  ├── 13. 停止 ResourceSampler
  ├── 14. 生成 summary.json（含 dropped_event_count、flush_completed、run_complete）
  └── 15. 调用 rotation.cleanup()  ← 退出时轮转
```

**关键顺序**：Backend 子进程**完全退出后**再生成 summary——确保 lifespan shutdown 的 `flush()` 已执行完毕，`dropped_event_count` 和 `flush_completed` 值准确。

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

## 6. 全入口覆盖矩阵

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

## 7. 敏感数据白名单与脱敏

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
| `artifact_path` | Pipeline | **项目相对路径**（相对于 `generated/`），不含盘符/绝对路径 |
| `artifact_sha256` | Pipeline | 64 字符 hex |
| `row_count` | Executor | 整数 |

### Artifact 记录规则

- **默认**：只记录 artifact 的**项目相对路径**和 **SHA-256**。不复制、不嵌入 SQL/PySpark/DeveloperSpec 内容到 JSONL
- **调试 artifact**（`--debug-artifacts` 模式）：SQL/PySpark 写入 `logs/monitor/_debug/` 子目录——该目录明确标记为**敏感文件**，不进入 Git、不嵌入 JSONL、不打包到 Review Package
- **路径规范化**：绝对路径转为项目相对路径（`generated/review_packages/xxx.sql`），消除环境差异
- **禁止**：JSONL 事件中禁止出现 `generated_sql`、`raw_pyspark`、`markdown_text`、`full_spec` 等 artifact 内容字段

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
- SQL/PySpark/DeveloperSpec 写入 `logs/monitor/_debug/{run_id}/` 子目录（不嵌入 JSONL）
- `_debug/` 目录标记为 `.gitignore`，不进入 Review Package
- 对 SQL/PySpark 中的字符串字面量执行 `****` 掩码（保留 SQL 结构、替换数据值）
- 每个 debug artifact 文件单独计算 SHA-256 并记录到 events.jsonl
- 仍禁止记录：凭据、连接串、Headers、数据样本

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

## 8. 异常传播不变性

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

## 9. 资源采样器（ResourceSampler）

### 采集目标

- **Python 进程树**：uvicorn reloader + worker、multiprocessing.spawn 子进程
- **Node 进程树**：node.exe（vite）、npx、esbuild
- **Spark JVM 进程树**：java.exe（SparkSubmit）、python.exe（PySpark driver）
- **系统级**：总 CPU、总内存（可选）

### 阶段时间窗口关联

资源采样按固定间隔（5s）运行，每条样本记录当前时间窗口内**正在执行的阶段列表**。事后通过 `active_stage_run_ids` 字段将资源指标关联到阶段，但**不声称精确归因**——同一进程可能同时服务多个阶段。

### 指标命名

所有峰值指标统一使用 `peak_observed_*` 前缀，强调"观测到的最大值"而非"精确贡献"：

- `peak_observed_python_rss_mb`：观测到的 Python 进程树最大 RSS
- `peak_observed_node_rss_mb`：观测到的 Node 进程树最大 RSS
- `peak_observed_java_rss_mb`：观测到的 Java 进程树最大 RSS
- `peak_observed_system_cpu_percent`：观测到的系统 CPU 使用率峰值

### 行为

```python
class ResourceSampler:
    """后台线程，每 5 秒采集一次进程树资源指标。

    - 通过 psutil.Process.children(recursive=True) 遍历进程树
    - 采集: pid, name, cmdline_truncated, cpu_percent, rss_mb, vms_mb, num_threads
    - 每条样本记录 active_stage_run_ids——当前时间窗口内活跃的阶段 ID 列表
      （从 RunLogCollector 查询当前 open 的 stage 上下文）
    - cmdline 截断到 200 字符，过滤掉含密码/Token 的参数（--password/--token/--key）
    - 采集失败→logging.warning，继续下一次采样
    - 写入 resource.jsonl（独立文件，不经过 RunLogCollector 队列——避免阻塞事件流）
    - 启动时即刻采集一次（冷启动基线），之后按间隔
    - 所有指标以 peak_observed_* 命名，summary 汇总时取整个 run 的峰值
    """

    def start(self) -> None: ...
    def stop(self) -> None: ...
    def _sample(self) -> ResourceSample: ...
    def set_active_stages(self, stage_run_ids: list[str]) -> None:
        """更新当前活跃阶段列表——供 collector.stage() 进入/退出时调用。"""
```

### 资源 JSONL 格式

```jsonl
{"event_type":"resource","run_id":"20260710_143022","timestamp":"2026-07-10T14:30:25.000+08:00","active_stage_run_ids":["stage_sql_compiler_c3d4"],"processes":[{"pid":33972,"name":"python.exe","cpu_percent":12.5,"rss_mb":156.3,"vms_mb":420.1,"num_threads":8}]}
```

---

## 10. 日志轮转（Rotation）

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

## 11. 文件结构汇总

```
新增文件：
  scripts/monitor_dev_run.py
  src/tianshu_datadev/monitor/__init__.py
  src/tianshu_datadev/monitor/collector.py      # RunLogCollector + NullCollector + StageContext
  src/tianshu_datadev/monitor/lifespan.py        # FastAPI lifespan——startup init / shutdown flush+close
  src/tianshu_datadev/monitor/middleware.py      # MonitorMiddleware——HTTP 事件采集
  src/tianshu_datadev/monitor/resource_sampler.py # ResourceSampler——psutil 进程树 + 阶段时间窗口关联
  src/tianshu_datadev/monitor/rotation.py        # 日志轮转——保留最近 50 组
  src/tianshu_datadev/monitor/sanitizer.py       # Sanitizer——白名单过滤 + traceback 脱敏
  src/tianshu_datadev/monitor/models.py          # StrictModel——MonitorEvent/StageEvent/ResourceSample
  frontend/src/monitor/client.ts                 # monitorClient——fetch 包装 + onerror + rejection
  frontend/src/monitor/__init__.ts               # 前端监控入口
  tests/monitor/__init__.py
  tests/monitor/test_collector.py                # Collector + NullCollector + StageContext
  tests/monitor/test_middleware.py               # MonitorMiddleware + 路径排除
  tests/monitor/test_resource_sampler.py         # ResourceSampler + peak_observed_* 命名
  tests/monitor/test_rotation.py                 # Rotation + 碰撞文件名
  tests/monitor/test_sanitizer.py                # Sanitizer + 白名单/黑名单
  tests/monitor/test_browser_event_security.py   # browser-event 安全边界——Origin/token/rate/404
  tests/monitor/test_exception_propagation.py    # 异常传播不变性——有无监控一致
  tests/monitor/test_integration.py              # 端到端——全流程监控

修改文件：
  src/tianshu_datadev/api/app.py                # 注册 lifespan + MonitorMiddleware + monitor router
  src/tianshu_datadev/api/routes.py             # /api/monitor/browser-event + /api/monitor/config
  src/tianshu_datadev/api/pipeline.py           # 注入 collector，stage() 包裹节点
  src/tianshu_datadev/spark/orchestrator.py     # stage() 包裹 6 节点
  frontend/src/main.tsx                         # 初始化 monitorClient（条件——enabled）
  .gitignore                                    # 添加 logs/monitor/ + logs/monitor/_debug/
```

---

## 12. 安全清单

- [x] `--debug-artifacts` 默认关闭
- [x] traceback 不含 `locals()` / `f_locals`
- [x] URL query 参数脱敏或丢弃
- [x] 禁止 `client_ip`
- [x] 监控端点限制请求大小（4KB）和字段白名单
- [x] `logs/monitor/` 和 `logs/monitor/_debug/` 在 `.gitignore` 中
- [x] 监控异常不改变 Pipeline 状态和异常类型
- [x] 默认只记录 artifact 项目相对路径和 SHA-256，不记录内容
- [x] `cmdline` 过滤 `--password`/`--token`/`--key` 参数
- [x] `browser-event` 端点不经过 MonitorMiddleware（避免无限循环）
- [x] 无 TIANSHU_RUN_ID → NullCollector，`/api/monitor/browser-event` → 404
- [x] browser-event Origin 白名单（仅 127.0.0.1:5173 + localhost:5173）
- [x] browser-event monitor_token + run_id 双重校验
- [x] browser-event 速率限制（20/min）+ 总量限制（200/run）
- [x] 资源指标命名 `peak_observed_*`，不声称精确归因
- [x] debug artifact 写入 `_debug/` 子目录（独立于 JSONL），明确标记为敏感文件
- [x] summary 含 `dropped_event_count`、`flush_completed`、`run_complete`——可审计监控健康度

---

## 13. 事件 JSONL 格式示例

```jsonl
{"event_type":"stage","run_id":"20260710_143022","http_request_id":"hreq_a1b2","artifact_request_id":"abc123","stage_run_id":"stage_sql_compiler_c3d4","parent_stage_run_id":null,"node":"sql_compiler","status":"started","timestamp":"2026-07-10T14:30:22.123+08:00","duration_ms":null,"error_type":null,"error_code":null,"error_message":null,"stack_frames":null,"artifact_path":null,"artifact_sha256":null}
{"event_type":"stage","run_id":"20260710_143022","http_request_id":"hreq_a1b2","artifact_request_id":"abc123","stage_run_id":"stage_sql_compiler_c3d4","parent_stage_run_id":null,"node":"sql_compiler","status":"completed","timestamp":"2026-07-10T14:30:22.456+08:00","duration_ms":333,"error_type":null,"error_code":null,"error_message":null,"stack_frames":null,"artifact_path":"generated/review_packages/abc123/query.sql","artifact_sha256":"abc123def456"}
{"event_type":"http","run_id":"20260710_143022","http_request_id":"hreq_a1b2","method":"POST","path":"/api/execute-rich","status_code":200,"duration_ms":450,"timestamp":"2026-07-10T14:30:22.456+08:00"}
{"event_type":"browser","run_id":"20260710_143022","api_path":"/api/execute-rich","api_status":200,"api_duration_ms":480,"error_type":null,"error_message":null,"stack_frames":null,"timestamp":"2026-07-10T14:30:22.500+08:00"}
{"event_type":"resource","run_id":"20260710_143022","timestamp":"2026-07-10T14:30:25.000+08:00","active_stage_run_ids":["stage_sql_compiler_c3d4"],"processes":[{"pid":33972,"name":"python.exe","cpu_percent":12.5,"rss_mb":156.3,"vms_mb":420.1,"num_threads":8}]}
```

---

## 14. summary.json 格式

```json
{
  "run_id": "20260710_143022",
  "monitor_version": "1.0.0",
  "started_at": "2026-07-10T14:30:22.000+08:00",
  "ended_at": "2026-07-10T14:45:10.000+08:00",
  "duration_seconds": 888,
  "exit_code": 0,
  "dropped_event_count": 0,
  "flush_completed": true,
  "run_complete": true,
  "stages": {
    "sql_parser": {"total": 3, "completed": 3, "failed": 0},
    "sql_compiler": {"total": 3, "completed": 3, "failed": 0},
    "spark_physical_verifier": {"total": 1, "completed": 1, "failed": 0}
  },
  "errors": [
    {"node": "sql_executor", "error_type": "DuckDBError", "error_code": "RUNTIME_FAIL", "error_message": "Binder Error: Table 'xxx' not found", "timestamp": "2026-07-10T14:32:15.000+08:00"}
  ],
  "peak_observed_resources": {
    "peak_observed_python_rss_mb": 512.3,
    "peak_observed_node_rss_mb": 320.1,
    "peak_observed_java_rss_mb": 1024.5,
    "peak_observed_system_cpu_percent": 45.2
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

### summary.json 字段说明

| 字段 | 来源 | 说明 |
|------|------|------|
| `dropped_event_count` | `RunLogCollector.dropped_event_count` | 队列满导致的丢弃事件数——>0 表示监控过载 |
| `flush_completed` | `RunLogCollector.flush()` 返回值 | False 表示 shutdown 时队列未排空（可能丢失末尾事件） |
| `run_complete` | lifespan shutdown 设置 | False 表示后端异常退出（lifespan shutdown 未执行） |
| `peak_observed_resources` | ResourceSampler 全量样本聚合 | 整个 run 生命周期内的观测峰值，非阶段精确归因 |
| `started_at` / `ended_at` | monitor_dev_run.py | **带时区** ISO 8601（`+08:00`） |
