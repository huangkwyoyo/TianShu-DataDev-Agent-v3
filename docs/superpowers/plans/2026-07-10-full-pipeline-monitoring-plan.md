# 全流程运行监控——分批实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 SQL 管线（6 节点）、Spark 管线（6 节点）、前端和后端提供统一的运行监控，从 monitor_dev_run.py 启动到退出的全生命周期日志采集。

**Architecture:** 8 批实施，每批独立可测。先建基础模块（Models/Sanitizer/Rotation/Collector），再集成 Middleware/Lifespan，然后接入管线（SQL→Spark），最后启动脚本 + 集成测试。NullCollector 保证未启用时零开销。

**Tech Stack:** Python 3.11+, FastAPI (lifespan/middleware), psutil, Pydantic (StrictModel), TypeScript (浏览器 monitorClient), pytest, ruff

## Global Constraints

- 无 `TIANSHU_RUN_ID` → NullCollector 零开销模式，`/api/monitor/browser-event` → 404
- 监控异常仅能在 Collector/Sampler/Rotation 边界捕获，业务异常必须原样传播
- 默认只记录 artifact 项目相对路径 + SHA-256，不记录 SQL/PySpark 内容
- 所有事件时间戳使用带时区 ISO 8601（`2026-07-10T14:30:22.123+08:00`）
- `run_id` 通过环境变量 `TIANSHU_RUN_ID` 传播，模块只读不生成
- browser-event 端点：Origin 白名单 + monitor_token + run_id 校验 + 速率限制
- 资源指标命名 `peak_observed_*`，不声称精确归因
- `logs/monitor/` 和 `logs/monitor/_debug/` 在 `.gitignore` 中
- 禁止记录：凭据、连接串、Headers、client_ip、locals、请求/响应体、数据样本
- 日志只保留最近 50 组，启动+退出双重轮转

---

## 退出条件（每批通用）

每批完成后必须通过以下检查，**不通过不得进入下一批**：

```bash
# 1. 单元测试全绿
pytest tests/monitor/ -x -v --tb=short

# 2. 无 lint 错误
ruff check src/tianshu_datadev/monitor/

# 3. 全量回归——确保现有功能不受影响
pytest tests/ -x --timeout=60 -q 2>&1 | tail -5

# 4. 无 B/C 类新问题
# 若发现业务逻辑异常、架构边界变更、安全策略放宽 → 立即停止，按 CRCS 输出分类报告
```

---

### Batch 1: 基础模块——Models、Sanitizer、Rotation

**范围**：纯函数模块，不依赖 FastAPI 或 Pipeline，最先实施

**允许修改文件**：
- **Create**: `src/tianshu_datadev/monitor/__init__.py`
- **Create**: `src/tianshu_datadev/monitor/models.py`
- **Create**: `src/tianshu_datadev/monitor/sanitizer.py`
- **Create**: `src/tianshu_datadev/monitor/rotation.py`
- **Create**: `tests/monitor/__init__.py`
- **Create**: `tests/monitor/test_models.py`
- **Create**: `tests/monitor/test_sanitizer.py`
- **Create**: `tests/monitor/test_rotation.py`
- **Modify**: `.gitignore`（添加 `logs/monitor/` 和 `logs/monitor/_debug/`）

**禁止修改**：`src/tianshu_datadev/api/`、`src/tianshu_datadev/spark/`、`frontend/`、`scripts/`

**接口产出**：

| 符号 | 签名 | 说明 |
|------|------|------|
| `MonitorEvent` | `StrictModel`，含 `event_type: Literal["stage","http","resource","browser"]`、`run_id: str`、`timestamp: datetime` | 事件基类 |
| `StageEvent` | `StrictModel`，含 `node: str`、`status: Literal["started","completed","failed","skipped"]`、`http_request_id`、`artifact_request_id`、`stage_run_id`、`parent_stage_run_id` 等 | 阶段事件 |
| `HttpEvent` | `StrictModel`，含 `http_request_id`、`method`、`path`、`status_code`、`duration_ms` | HTTP 事件 |
| `BrowserEvent` | `StrictModel`，含 `api_path`、`api_status`、`api_duration_ms`、`error_type`、`error_message`、`stack_frames` | 浏览器事件 |
| `ResourceSample` | `StrictModel`，含 `active_stage_run_ids: list[str]`、`processes: list[ProcessMetrics]`、`timestamp: datetime` | 资源样本 |
| `ProcessMetrics` | `StrictModel`，含 `pid`、`name`、`cpu_percent`、`rss_mb`、`vms_mb`、`num_threads` | 进程指标 |
| `Sanitizer.sanitize_traceback(tb)` | `list[dict[str,str]]` → `[{file, function, lineno}]` | traceback 脱敏 |
| `Sanitizer.sanitize_error_message(msg)` | `str → str`（截断 500 字符） | 错误消息脱敏 |
| `Sanitizer.sanitize_url(url)` | `str → str`（移除 query） | URL 脱敏 |
| `Sanitizer.validate_event(event)` | `MonitorEvent → MonitorEvent`（黑名单字段检查） | 事件白名单校验 |
| `cleanup(log_dir, current_run_id, keep_groups=50)` | `Path × str × int → int` | 轮转清理，返回删除组数 |

**测试清单**：

- `test_models.py`：
  - `test_stage_event_serialization`——StageEvent 序列化为符合 JSONL 格式的 dict
  - `test_resource_sample_active_stage_run_ids`——active_stage_run_ids 正确序列化
  - `test_http_event_no_client_ip`——HttpEvent 不包含 client_ip 字段
  - `test_browser_event_no_body_no_headers`——BrowserEvent 禁止包含 request_body/response_body/headers

- `test_sanitizer.py`：
  - `test_sanitize_traceback_strips_locals`——traceback 脱敏后不含 f_locals/f_globals
  - `test_sanitize_traceback_keeps_file_function_lineno`——保留文件名/函数名/行号
  - `test_sanitize_error_message_truncates_500`——超长消息截断到 500 字符
  - `test_sanitize_url_strips_query_string`——移除 URL query 参数
  - `test_validate_event_rejects_request_body`——含 request_body 字段的事件被拒绝
  - `test_validate_event_rejects_authorization_header`——含 authorization 字段的事件被拒绝

- `test_rotation.py`：
  - `test_cleanup_keeps_recent_50_groups`——保留最近 50 组
  - `test_cleanup_protects_current_run_id`——当前 run_id 的日志组不被删除
  - `test_cleanup_handles_empty_dir`——空目录不报错
  - `test_cleanup_handles_missing_dir`——目录不存在不报错
  - `test_cleanup_stable_with_collision_names`——碰撞文件名（后缀随机 hex）不影响清理

**验收命令**：

```bash
pytest tests/monitor/test_models.py tests/monitor/test_sanitizer.py tests/monitor/test_rotation.py -x -v --tb=short
ruff check src/tianshu_datadev/monitor/models.py src/tianshu_datadev/monitor/sanitizer.py src/tianshu_datadev/monitor/rotation.py
pytest tests/ -x --timeout=60 -q 2>&1 | tail -3  # 全量回归——确认现有测试不受影响
```

---

### Batch 2: Collector——RunLogCollector + NullCollector + StageContext

**范围**：核心采集器实现，线程安全单写者 + 工厂函数。不依赖 FastAPI。

**允许修改文件**：
- **Create**: `src/tianshu_datadev/monitor/collector.py`
- **Create**: `tests/monitor/test_collector.py`
- **Modify**: `src/tianshu_datadev/monitor/__init__.py`（导出 get_collector、RunLogCollector、NullCollector）

**禁止修改**：`src/tianshu_datadev/api/`、`src/tianshu_datadev/spark/`、`frontend/`、`scripts/`

**前置依赖**：Batch 1（models.py、sanitizer.py）

**接口产出**：

| 符号 | 签名 | 说明 |
|------|------|------|
| `get_collector(log_dir)` | `Path | None → RunLogCollector | NullCollector` | 工厂——读 TIANSHU_RUN_ID 决定返回类型 |
| `RunLogCollector.__init__(log_dir, run_id, max_queue)` | `Path × str × int → None` | 初始化队列 + writer 线程 |
| `RunLogCollector.emit(event)` | `MonitorEvent → None` | 非阻塞入队，队列满→dropped_event_count+=1 |
| `RunLogCollector.stage(node, artifact_request_id, parent_stage_run_id)` | `str × str × str|None → StageContext` | 上下文管理器 |
| `RunLogCollector.log_resource_sample(sample)` | `ResourceSample → None` | 写入资源事件 |
| `RunLogCollector.log_browser_event(payload)` | `dict → None` | 写入浏览器事件 |
| `RunLogCollector.flush(timeout)` | `float → bool` | 排空队列，返回是否完成 |
| `RunLogCollector.close()` | `→ None` | 关闭文件 + 停止线程 |
| `RunLogCollector.dropped_event_count` | `int` | 队列满丢弃计数 |
| `RunLogCollector.flush_completed` | `bool` | 最近 flush 是否排空 |
| `RunLogCollector.run_complete` | `bool` | 是否已调用 close |
| `NullCollector.enabled` | `False` | 标识禁用状态 |
| `StageContext.__enter__()` / `__exit__()` | 上下文管理器协议 | 进入写 started，退出写 completed/failed，返回 False |
| `StageContext.set_result(**kwargs)` | `→ None` | 设置 artifact_path、artifact_sha256 等 |

**测试清单**：

- `test_collector.py`：
  - `test_get_collector_returns_null_when_no_env_var`——无 TIANSHU_RUN_ID → NullCollector
  - `test_get_collector_returns_run_collector_when_env_var_set`——有 TIANSHU_RUN_ID → RunLogCollector
  - `test_null_collector_enabled_is_false`——NullCollector.enabled == False
  - `test_null_collector_stage_is_noop`——NullCollector.stage() 进去出来无副作用
  - `test_emit_writes_to_jsonl`——emit 后 JSONL 文件含正确事件行
  - `test_stage_context_writes_started_and_completed`——正常完成写两条事件
  - `test_stage_context_writes_failed_on_exception`——异常退出写 failed 状态 + error_type
  - `test_stage_context_does_not_swallow_exception`——异常原样传播（return False）
  - `test_stage_context_set_result`——set_result 的 artifact_path/SHA-256 出现在 completed 事件中
  - `test_queue_full_drops_and_counts`——队列满时 dropped_event_count 递增
  - `test_flush_returns_true_when_queue_empty`——flush 排空返回 True
  - `test_flush_timeout_returns_false`——队列未排空超时返回 False
  - `test_run_id_never_generated_internally`——run_id 永远从 env var 读取，不自生成

**验收命令**：

```bash
pytest tests/monitor/test_collector.py -x -v --tb=short
ruff check src/tianshu_datadev/monitor/collector.py src/tianshu_datadev/monitor/__init__.py
pytest tests/ -x --timeout=60 -q 2>&1 | tail -3
```

---

### Batch 3: Middleware + Lifespan + Monitor 路由

**范围**：FastAPI 集成——MonitorMiddleware、lifespan 回调、`/api/monitor/config`、`/api/monitor/browser-event`

**允许修改文件**：
- **Create**: `src/tianshu_datadev/monitor/middleware.py`
- **Create**: `src/tianshu_datadev/monitor/lifespan.py`
- **Create**: `tests/monitor/test_middleware.py`
- **Create**: `tests/monitor/test_browser_event_security.py`
- **Modify**: `src/tianshu_datadev/api/app.py`（注册 lifespan、middleware、monitor_router）
- **Modify**: `src/tianshu_datadev/api/routes.py`（添加 `/api/monitor/config` + `/api/monitor/browser-event`）

**禁止修改**：`src/tianshu_datadev/api/pipeline.py`、`src/tianshu_datadev/spark/`、`frontend/`、`scripts/`

**前置依赖**：Batch 2（collector.py）

**接口产出**：

| 符号 | 签名 | 说明 |
|------|------|------|
| `MonitorMiddleware` | ASGI middleware | before→start_time，after→emit HttpEvent，exception→emit failed |
| `monitor_lifespan(app)` | `FastAPI → AsyncContextManager` | startup→init collector，shutdown→flush+close |
| `GET /api/monitor/config` | `→ {"enabled": bool, "run_id"?, "monitor_token"?, ...}` | 前端查询监控状态 |
| `POST /api/monitor/browser-event` | `MonitorPayload → 204 \| 4xx` | 浏览器事件上报，含完整安全校验链 |

**测试清单**：

- `test_middleware.py`：
  - `test_middleware_records_http_request`——正常请求记录 method/path/status_code/duration_ms
  - `test_middleware_assigns_http_request_id`——每个请求分配唯一的 http_request_id
  - `test_middleware_excludes_health_endpoint`——GET /api/health 不记录
  - `test_middleware_excludes_monitor_endpoints`——/api/monitor/* 不记录（避免循环）
  - `test_middleware_strips_query_string_from_path`——path 不含 query 参数
  - `test_middleware_does_not_record_client_ip`——HttpEvent 不含 client_ip
  - `test_middleware_does_not_capture_exception`——异常不吞，继续传播到 error_handler
  - `test_null_collector_middleware_noop`——无 TIANSHU_RUN_ID 时 middleware 零开销

- `test_browser_event_security.py`：
  - `test_browser_event_returns_404_when_monitoring_disabled`——NullCollector → 404
  - `test_browser_event_rejects_wrong_origin`——Origin 不在白名单 → 403
  - `test_browser_event_accepts_localhost_origin`——Origin: localhost:5173 → 204
  - `test_browser_event_accepts_127_origin`——Origin: 127.0.0.1:5173 → 204
  - `test_browser_event_rejects_missing_monitor_token`——无 token → 403
  - `test_browser_event_rejects_wrong_monitor_token`——token 不匹配 → 403
  - `test_browser_event_rejects_wrong_run_id`——run_id 不匹配 → 403
  - `test_browser_event_rate_limit`——每分钟超过 20 条 → 429
  - `test_browser_event_total_limit`——超过 200 条总量 → 429
  - `test_browser_event_rejects_oversized_body`——请求体 > 4KB → 413
  - `test_browser_event_rejects_request_body_in_payload`——payload 含 request_body 字段 → 400

**验收命令**：

```bash
pytest tests/monitor/test_middleware.py tests/monitor/test_browser_event_security.py -x -v --tb=short
ruff check src/tianshu_datadev/monitor/middleware.py src/tianshu_datadev/monitor/lifespan.py
ruff check src/tianshu_datadev/api/app.py src/tianshu_datadev/api/routes.py
pytest tests/api/ -x --timeout=60 -q 2>&1 | tail -3  # API 层回归
pytest tests/ -x --timeout=60 -q 2>&1 | tail -3      # 全量回归
```

---

### Batch 4: SQL 管线埋点——Pipeline 集成

**范围**：在 `Pipeline.execute_rich()`、`execute()`、`build_plan()` 等 8 个入口方法中注入 `collector.stage()`

**允许修改文件**：
- **Modify**: `src/tianshu_datadev/api/pipeline.py`（注入 collector，stage() 包裹 SQL 节点）
- **Create**: `tests/monitor/test_pipeline_integration.py`

**禁止修改**：`src/tianshu_datadev/spark/orchestrator.py`、`frontend/`、`scripts/`

**前置依赖**：Batch 3（middleware + lifespan + routes）

**埋点位置**（`collector.stage()` 调用点）：

| 位置 | 节点名 | artifact_request_id 来源 |
|------|--------|------------------------|
| `parse_only()` ～ L850 | `sql_parser` | `request_id`（新生成） |
| `parse_rich()` | `sql_parser`, `sql_enricher` | `request_id`（新生成） |
| `build_plan()` | `sql_parser`, `sql_enricher`, `sql_builder`, `sql_validator` | `request_id`（新生成） |
| `execute()` ～ L886 | `sql_parser`, `sql_enricher`, `sql_builder`, `sql_validator`, `sql_compiler`, `sql_executor` | `self._gen_request_id(spec)` |
| `execute_rich()` ～ L2195 | 同上 + `snapshot_builder` | `self._gen_request_id(spec)` |
| `run_all()` | 同上 + `contract_extractor`, `packager` | `self._gen_request_id(spec)` |

**埋点规则**：
- 使用 `with collector.stage(node, artifact_request_id) as ctx:` 包裹每个节点
- 成功时调用 `ctx.set_result(artifact_path=..., artifact_sha256=..., row_count=...)`
- 失败时由 StageContext.__exit__ 自动记录（异常原样传播）
- 复用现有 `_record_trace()` 调用点——不建立新状态机
- `parent_stage_run_id` 仅用于真实阶段嵌套（当前 SQL 管线无嵌套，均为 null）

**测试清单**：

- `test_pipeline_integration.py`：
  - `test_execute_rich_records_all_sql_stages`——mock collector，验证 execute_rich 产生 6 个节点事件
  - `test_parse_only_records_parser_stage`——parse_only 记录 sql_parser
  - `test_build_plan_records_four_stages`——build_plan 记录 parser/enricher/builder/validator
  - `test_stage_event_has_artifact_request_id`——StageEvent 的 artifact_request_id 与 Pipeline._gen_request_id 一致
  - `test_stage_event_has_artifact_path_and_sha256`——completed 事件含 artifact 路径和 SHA-256
  - `test_compile_failure_records_stage_failed`——编译失败时记录 failed 状态 + error_type
  - `test_null_collector_pipeline_noop`——NullCollector 模式下管线行为不变
  - `test_monitoring_failure_does_not_change_pipeline_result`——监控异常不改变管线返回值

**验收命令**：

```bash
pytest tests/monitor/test_pipeline_integration.py -x -v --tb=short
pytest tests/api/ -x --timeout=60 -q 2>&1 | tail -3  # API 层回归
pytest tests/ -x --timeout=60 -q 2>&1 | tail -3      # 全量回归
```

---

### Batch 5: Spark 管线埋点——Orchestrator 集成

**范围**：在 `SparkOrchestrator.run()` 和 `run_spark_stage()` 中注入 `collector.stage()`，覆盖全部 6 个 Spark 节点

**允许修改文件**：
- **Modify**: `src/tianshu_datadev/spark/orchestrator.py`（stage() 包裹 6+1 节点）
- **Modify**: `src/tianshu_datadev/api/pipeline.py`（`run_spark_stage()` 传递 collector）
- **Create**: `tests/monitor/test_spark_integration.py`

**禁止修改**：`src/tianshu_datadev/spark/compiler.py`、`src/tianshu_datadev/spark/mapper.py`、`src/tianshu_datadev/spark/developer.py`、`scripts/`

**前置依赖**：Batch 4（Pipeline 集成——需要 collector 注入模式）

**埋点位置**：

| 位置 | 节点名 | parent_stage_run_id |
|------|--------|---------------------|
| `SparkOrchestrator.run()` → 整个全链路 | `spark_verify` | null |
| → `_run_mapper()` | `spark_mapper` | spark_verify 的 stage_run_id |
| → `_run_developer()` | `spark_developer` | spark_verify 的 stage_run_id |
| → `_run_compiler()` | `spark_compiler` | spark_verify 的 stage_run_id |
| → `_run_validator()` | `spark_validator` | spark_verify 的 stage_run_id |
| → `_run_comparator()` | `spark_comparator` | spark_verify 的 stage_run_id |
| → `_run_physical_verifier()` | `spark_physical_verifier` | spark_verify 的 stage_run_id |
| `run_spark_stage()` 单阶段 | 对应节点 | null（独立调用，非嵌套） |

**测试清单**：

- `test_spark_integration.py`：
  - `test_spark_verify_records_all_six_sub_stages`——全链路 verify 记录 6 个子节点 + 父节点
  - `test_sub_stages_have_parent_stage_run_id`——子节点的 parent_stage_run_id = verify 的 stage_run_id
  - `test_run_spark_stage_records_single_stage`——单阶段调用记录独立节点（parent=null）
  - `test_spark_compile_records_artifact_path`——COMPILER 完成后 artifact_path 记录 generated PySpark 路径
  - `test_spark_physical_verify_records_row_count`——PHYSICAL_VERIFIER 完成后 row_count 正确
  - `test_spark_failure_records_stage_failed`——节点失败记录 failed 状态 + error_type
  - `test_spark_retry_is_new_stage_run_id`——返工/重试产生新的独立 stage_run_id（非 parent）
  - `test_null_collector_spark_stage_noop`——NullCollector 模式下 Spark 行为不变

**验收命令**：

```bash
pytest tests/monitor/test_spark_integration.py -x -v --tb=short
pytest tests/spark/ -x --timeout=60 -q 2>&1 | tail -3  # Spark 层回归
pytest tests/ -x --timeout=60 -q 2>&1 | tail -3         # 全量回归
```

---

### Batch 6: 资源采样器——ResourceSampler

**范围**：psutil 后台线程，采集 Python/Node/Spark JVM 进程树资源，按阶段时间窗口关联

**允许修改文件**：
- **Create**: `src/tianshu_datadev/monitor/resource_sampler.py`
- **Create**: `tests/monitor/test_resource_sampler.py`

**禁止修改**：`src/tianshu_datadev/api/`、`src/tianshu_datadev/spark/`、`frontend/`、`scripts/`

**前置依赖**：Batch 2（collector.py——需要 RunLogCollector 提供活跃阶段列表）

**接口产出**：

| 符号 | 签名 | 说明 |
|------|------|------|
| `ResourceSampler.__init__(log_dir, run_id, collector, interval)` | `Path × str × RunLogCollector × float → None` | collector 用于查询活跃阶段列表 |
| `ResourceSampler.start()` | `→ None` | 启动后台线程 |
| `ResourceSampler.stop()` | `→ None` | 停止后台线程 |
| `ResourceSampler._sample()` | `→ ResourceSample` | 采集一次进程树指标 |
| `ResourceSampler.set_active_stages(stage_run_ids)` | `→ None` | 更新活跃阶段——供 collector.stage() 调用 |

**测试清单**：

- `test_resource_sampler.py`：
  - `test_sample_collects_python_processes`——识别 python.exe 进程
  - `test_sample_has_active_stage_run_ids`——样本含活跃阶段列表
  - `test_cmdline_truncated_to_200_chars`——命令行截断保护
  - `test_cmdline_filters_password_token_key`——过滤含 --password/--token/--key 的参数
  - `test_sample_failure_does_not_throw`——psutil 异常时仅 log warning
  - `test_metrics_named_peak_observed`——指标使用 peak_observed_* 命名
  - `test_peak_aggregation_across_run`——整个 run 的峰值聚合正确
  - `test_start_stop_lifecycle`——start/stop 线程管理正确

**验收命令**：

```bash
pytest tests/monitor/test_resource_sampler.py -x -v --tb=short
ruff check src/tianshu_datadev/monitor/resource_sampler.py
pytest tests/ -x --timeout=60 -q 2>&1 | tail -3
```

---

### Batch 7: 前端 monitorClient

**范围**：浏览器端轻量监控采集——fetch 包装 + window.onerror + unhandledrejection

**允许修改文件**：
- **Create**: `frontend/src/monitor/client.ts`
- **Create**: `frontend/src/monitor/__init__.ts`
- **Modify**: `frontend/src/main.tsx`（条件初始化 monitorClient）

**禁止修改**：`src/tianshu_datadev/`、`scripts/`

**前置依赖**：Batch 3（`/api/monitor/config` + `/api/monitor/browser-event` 端点已就绪）

**文件内容**：

`frontend/src/monitor/client.ts`：
```typescript
/**
 * 浏览器端监控客户端——轻量采集，零第三方依赖。
 *
 * 采集范围（白名单）：
 *   - fetch /api/* 耗时和 HTTP 状态
 *   - window.onerror 运行时异常
 *   - unhandledrejection Promise 异常
 *
 * 禁止记录：请求正文、响应正文、Authorization Header、数据样本
 */

interface MonitorConfig {
  enabled: boolean;
  run_id?: string;
  monitor_token?: string;
}

interface MonitorPayload {
  event_type: 'api_call' | 'js_error' | 'promise_rejection';
  timestamp: string;
  run_id: string;
  monitor_token: string;
  api_path?: string;
  api_status?: number;
  api_duration_ms?: number;
  error_type?: string;
  error_message?: string;
  stack_frames?: string[];
}

export async function initMonitor(): Promise<void> {
  // 1. GET /api/monitor/config
  // 2. enabled=false → return（不注册任何事件）
  // 3. enabled=true → 注册 fetch 包装 + onerror + rejection
  // 4. 所有上报带 monitor_token，静默失败
}
```

`frontend/src/main.tsx` 修改：
```typescript
import { initMonitor } from './monitor/client';

initMonitor().then(() => {
  ReactDOM.createRoot(document.getElementById('root')!).render(...)
});
```

**测试清单**：
- 前端测试通过 Vite 构建校验：`cd frontend && npx tsc --noEmit`
- 手动验证（集成测试阶段）：
  - 监控未启用时 monitorClient 不注册任何监听器
  - 监控启用时 fetch /api/* 请求自动上报
  - JS 运行时错误被捕获并上报
  - 上报失败不影响页面功能

**验收命令**：

```bash
cd frontend && npx tsc --noEmit
cd frontend && npx vite build --mode development 2>&1 | tail -5
```

---

### Batch 8: 启动脚本 + 集成测试 + 异常传播验证

**范围**：`monitor_dev_run.py` 统一启动脚本 + 端到端集成测试 + 异常传播不变性验证

**允许修改文件**：
- **Create**: `scripts/monitor_dev_run.py`
- **Create**: `tests/monitor/test_exception_propagation.py`
- **Create**: `tests/monitor/test_integration.py`

**禁止修改**：`src/tianshu_datadev/`（所有管线代码已完成）

**前置依赖**：Batch 1-7 全部完成

**`monitor_dev_run.py` 行为序列**：

```
1. 生成 run_id = YYYYMMDD_HHMMSS（碰撞时追加 _{random4}）
2. 生成 monitor_token = secrets.token_hex(16)
3. 创建 logs/monitor/ 目录
4. 设置环境变量 TIANSHU_RUN_ID + TIANSHU_MONITOR_TOKEN
5. 调用 rotation.cleanup()  ← 启动时轮转
6. 启动 ResourceSampler（后台线程）
7. 启动 Backend: python -m uvicorn ... （stdout→ *_backend.log）
8. 启动 Frontend: npx vite ... （stdout→ *_frontend.log）
9. 健康检查轮询（:8000 /api/health + :5173 /）
10. 打印 "监控就绪——run_id=20260710_143022，浏览器访问 http://127.0.0.1:5173"
11. 注册 SIGINT/SIGTERM 处理器
12. 等待信号或子进程退出
    ├─ 终止 Frontend 子进程
    ├─ 终止 Backend 子进程
    ├─ 等待 Backend 子进程完全退出（确保 lifespan shutdown flush 执行）
    ├─ 停止 ResourceSampler
    ├─ 生成 summary.json（含 dropped_event_count、flush_completed、run_complete）
    └─ 调用 rotation.cleanup()  ← 退出时轮转
```

**测试清单**：

- `test_exception_propagation.py`：
  - `test_sql_compile_error_identical_with_and_without_monitor`——SQL 编译异常在有无监控时完全一致
  - `test_spark_error_identical_with_and_without_monitor`——Spark 异常在有无监控时完全一致
  - `test_pipeline_error_code_identical`——error_code 在有无监控时完全一致
  - `test_pipeline_error_type_identical`——error_type 在有无监控时完全一致
  - `test_pipeline_status_code_identical`——HTTP 状态码在有无监控时完全一致
  - `test_monitoring_exception_in_emit_does_not_affect_pipeline`——emit 内部异常不影响管线结果
  - `test_monitoring_exception_in_stage_exit_does_not_affect_pipeline`——stage.__exit__ 异常不影响管线

- `test_integration.py`（端到端，需要实际启动前后端）：
  - `test_full_run_generates_all_log_files`——一次完整运行产出 5 个日志文件
  - `test_events_jsonl_has_correct_schema`——events.jsonl 每行符合 MonitorEvent 格式
  - `test_summary_json_has_required_fields`——summary.json 含 dropped_event_count/flush_completed/run_complete
  - `test_rotation_keeps_recent_50_groups`——轮转保留最近 50 组
  - `test_browser_event_end_to_end`——前端上报事件成功写入 JSONL
  - `test_null_collector_in_ci`——CI 环境（无 TIANSHU_RUN_ID）不影响现有测试

**验收命令**：

```bash
# 异常传播验证
pytest tests/monitor/test_exception_propagation.py -x -v --tb=short

# 全量回归
pytest tests/ -x --timeout=60 -q 2>&1 | tail -3

# Lint 全量
ruff check scripts/monitor_dev_run.py src/tianshu_datadev/monitor/
```

---

## 实施顺序与依赖图

```
Batch 1 (Models + Sanitizer + Rotation)
  ↓
Batch 2 (Collector + NullCollector + StageContext)
  ↓
Batch 3 (Middleware + Lifespan + Monitor Routes)
  ↓
Batch 4 (SQL Pipeline Integration) ←─┐
  ↓                                   │ 可并行
Batch 5 (Spark Pipeline Integration) ←┘
  ↓
Batch 6 (Resource Sampler)
  ↓
Batch 7 (Frontend monitorClient)      ← 可与 Batch 6 并行
  ↓
Batch 8 (Startup Script + Integration + Exception Propagation)
```

## 回归范围

| 回归层级 | 命令 | 覆盖范围 |
|---------|------|---------|
| Monitor 单层 | `pytest tests/monitor/ -x -v` | 新模块自身 |
| API 层 | `pytest tests/api/ -x --timeout=60 -q` | 路由、Pipeline、错误处理 |
| Spark 层 | `pytest tests/spark/ -x --timeout=60 -q` | Compiler、Orchestrator、Verifier |
| SQL 层 | `pytest tests/sql/ -x --timeout=60 -q` | SQL Executor、Compiler、Harness |
| 安全层 | `pytest tests/security/ -x --timeout=60 -q` | 安全检查 |
| 全量 | `pytest tests/ -x --timeout=60 -q` | 全部 601+ 测试 |

## CRCS 警示

**实施中发现以下任一情况 → 立即停止，按 CRCS 分类报告，不继续**：

- 监控代码导致现有测试失败（A/B/C 取决于根因）
- 需要放宽 `extra="forbid"` 或安全校验（C 类——绝对禁止）
- 需要修改 SQL/Spark 编译核心逻辑（B 类——需设计确认）
- 需要扩展 Pipeline 数据模型（B 类——需设计确认）
- 监控异常误吞业务异常（C 类——架构风险）
- 需要记录设计规格书禁止的字段（C 类——绝对禁止）
- 前一批退出条件不通过时强行进入下一批（C 类——破坏分批隔离）
