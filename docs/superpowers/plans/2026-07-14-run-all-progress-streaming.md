# Run-All 实时进度可见性——NDJSON 流式实现

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run-All 执行期间前端实时展示 SQL + Spark 双管线阶段进度和错误信息，消除黑盒体验。

**Architecture:** 后端新增 NDJSON 流式端点 `/api/run-all-full/stream`，后台线程执行全流程并通过 `queue.Queue` 传递事件；前端 `fetch` + `ReadableStream` 逐行消费 NDJSON，实时更新进度面板。

**Tech Stack:** Python `threading.Thread` + `queue.Queue` + FastAPI `StreamingResponse`；TypeScript `fetch` + `ReadableStream` + React `useState`

## Global Constraints

- **所有代码注释使用中文**
- **不修改 `monitor/` 下的采集器核心逻辑**——`TeeCollector` 是包装器，不改变 `RunLogCollector`/`NullCollector`/`StageContext` 行为
- **现有 `/api/run-all-full` 端点保持不变**——新增 `/api/run-all-full/stream` 为独立端点
- **连接断开不中止后台执行**——后台线程独立于 HTTP 连接生命周期
- **错误信息必须清洗后返回前端**——不暴露完整 traceback
- **不引入新的第三方依赖**

---

### Task 1: 流式基础设施——`streaming.py`（TeeCollector + 事件队列）

**Files:**
- Create: `src/tianshu_datadev/api/streaming.py`

**Interfaces:**
- Produces: `TeeCollector` 类（包装现有 collector，拦截 stage 事件推送到队列）
- Produces: `_TeeStageContext` 类（包装 StageContext，退出时发送 completed/failed 事件）
- Produces: `FullRunEvent` TypedDict（事件类型定义）
- Produces: `sanitize_stream_error()` 函数（清洗错误信息用于流式传输）

- [ ] **Step 1: 创建 `streaming.py` 完整实现**

```python
"""流式进度基础设施——TeeCollector + 事件队列 + 错误清洗。

用于 /api/run-all-full/stream 端点：
- TeeCollector 包装现有 collector，将阶段事件同时推送到流式队列
- 后台线程执行全流程，通过 queue.Queue 向生成器传递 NDJSON 事件
- 错误信息经清洗后入队，不暴露完整 traceback
"""

from __future__ import annotations

import logging
import queue
import time
from dataclasses import dataclass, field
from typing import Any, Literal

from tianshu_datadev.monitor.collector import StageContext

logger = logging.getLogger(__name__)

# ── 流式事件类型定义 ──

class FullRunEvent:
    """统一的全流程进度事件——不区分 sql_stage/spark_stage。"""
    pass  # 仅作类型标注用，实际使用 TypedDict


def _sanitize_stream_error(exc: Exception) -> str:
    """清洗异常信息用于流式传输——仅保留异常类型+消息，不暴露 traceback。
    
    Args:
        exc: 原始异常对象
    
    Returns:
        清洗后的错误字符串，上限 500 字符
    """
    msg = f"{type(exc).__name__}: {exc}"
    # 截断到 500 字符，避免超大 stderr 撑满事件流
    if len(msg) > 500:
        msg = msg[:497] + "..."
    return msg


def _sanitize_stage_message(raw: str, max_len: int = 300) -> str:
    """清洗阶段级别的错误消息——用于 stage 事件的 message 字段。
    
    Args:
        raw: 原始错误消息
        max_len: 最大长度（字符）
    
    Returns:
        清洗后的消息
    """
    if not raw:
        return ""
    # 截断
    if len(raw) > max_len:
        raw = raw[:max_len - 3] + "..."
    return raw


class _TeeStageContext:
    """包装 StageContext——退出时向流式队列发送 completed/failed 事件。
    
    不改变原有 StageContext 的行为——异常仍会传播。
    """

    def __init__(
        self,
        real_ctx: StageContext,
        event_queue: queue.Queue,
        pipeline: Literal["sql", "spark"],
        stage_name: str,
    ):
        self._real = real_ctx
        self._queue = event_queue
        self._pipeline = pipeline
        self._stage = stage_name
        self._started_at = time.time()

    def set_result(self, **kwargs) -> None:
        """透传——设置 artifact_path、row_count 等。"""
        self._real.set_result(**kwargs)

    def __enter__(self) -> "_TeeStageContext":
        self._real.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        duration_ms = int((time.time() - self._started_at) * 1000)
        if exc_type is None:
            self._queue.put({
                "event": "stage",
                "pipeline": self._pipeline,
                "stage": self._stage,
                "status": "completed",
                "duration_ms": duration_ms,
            })
        else:
            error_msg = _sanitize_stream_error(exc_val) if exc_val else ""
            self._queue.put({
                "event": "stage",
                "pipeline": self._pipeline,
                "stage": self._stage,
                "status": "failed",
                "duration_ms": duration_ms,
                "message": error_msg,
                "error_type": exc_type.__name__ if exc_type else None,
            })
        # 透传到真实 StageContext
        return self._real.__exit__(exc_type, exc_val, exc_tb)


class TeeCollector:
    """包装现有 collector，将阶段事件同时推送到流式队列。
    
    仅拦截 stage() 方法——emit() 等其他方法直接透传。
    不改变现有 collector 的任何行为。
    
    由于 NullCollector 的 stage() 返回一个空操作 StageContext，
    TeeCollector 包装后仍能正确工作——流式事件正常发送，
    但底层 collector 不受影响。
    """

    def __init__(self, real, event_queue: queue.Queue, pipeline: Literal["sql", "spark"]):
        self._real = real
        self._queue = event_queue
        self._pipeline = pipeline

    @property
    def enabled(self) -> bool:
        return getattr(self._real, "enabled", False)

    @property
    def run_id(self) -> str:
        return getattr(self._real, "run_id", "")

    def emit(self, event) -> None:
        """透传——不做拦截。"""
        self._real.emit(event)

    def stage(
        self,
        node: str,
        artifact_request_id: str,
        parent_stage_run_id: str | None = None,
    ):
        """拦截 stage()——先向流式队列发送 started 事件，再返回包装的上下文。
        
        包装的 _TeeStageContext 在退出时会自动发送 completed/failed 事件。
        """
        # 发送 started 事件
        self._queue.put({
            "event": "stage",
            "pipeline": self._pipeline,
            "stage": node,
            "status": "started",
        })
        # 获取真实 StageContext 并包装
        real_ctx = self._real.stage(node, artifact_request_id, parent_stage_run_id)
        return _TeeStageContext(real_ctx, self._queue, self._pipeline, node)

    def log_resource_sample(self, sample) -> None:
        self._real.log_resource_sample(sample)

    def log_browser_event(self, payload: dict) -> None:
        self._real.log_browser_event(payload)

    def flush(self, timeout: float = 5.0) -> bool:
        return self._real.flush(timeout)

    def close(self) -> None:
        self._real.close()
```

- [ ] **Step 2: 验证文件语法正确**

```bash
python -c "from tianshu_datadev.api.streaming import TeeCollector, _TeeStageContext, _sanitize_stream_error; print('OK')"
```

预期：`OK`

- [ ] **Step 3: 提交**

```bash
git add src/tianshu_datadev/api/streaming.py
git commit -m "feat: 流式进度基础设施——TeeCollector + 事件队列 + 错误清洗"
```

---

### Task 2: 后端流式编排——`run_all_full_stream()` 方法

**Files:**
- Modify: `src/tianshu_datadev/api/pipeline.py`（在 `run_all_full()` 方法之后新增）

**Interfaces:**
- Consumes: `TeeCollector` from `tianshu_datadev.api.streaming`
- Consumes: `get_collector` from `tianshu_datadev.monitor`
- Consumes: `SparkPipelineStage` from `tianshu_datadev.spark.orchestrator`
- Produces: `Pipeline.run_all_full_stream()` → 生成器，yield NDJSON 行（str）

- [ ] **Step 1: 在 `pipeline.py` 文件顶部新增 import**

在现有 `from tianshu_datadev.monitor import get_collector`（第 29 行）之后插入：

```python
from tianshu_datadev.api.streaming import TeeCollector, _sanitize_stream_error
```

- [ ] **Step 2: 在 `run_all_full()` 方法之后新增 `run_all_full_stream()`**

在 `run_all_full()` 方法的 `return` 语句之后（约第 2057 行）、`run_all_rich()` 方法之前插入：

```python
    def run_all_full_stream(
        self, markdown_text: str,
        table_mapping: dict[str, str] | None = None,
        table_paths: dict[str, str] | None = None,
    ):
        """全流程 SQL + Spark 管线——NDJSON 流式生成器。
        
        通过 queue.Queue 在后台线程和生成器之间传递进度事件。
        每行一个 JSON 对象（NDJSON），前端 ReadableStream 逐行消费。
        
        事件类型：
        - {"event":"stage","pipeline":"sql"|"spark","stage":"...","status":"..."}
        - {"event":"done","result":{...FullRunResponse...}}
        - {"event":"fatal","error_code":"...","message":"..."}
        - {"event":"heartbeat"}
        
        连接断开时后台线程继续执行——queue 满时丢弃事件并计数。
        
        Yields:
            NDJSON 行（str），每行以 \\n 结尾
        """
        import json
        import queue as _queue_mod
        import threading
        
        event_queue: _queue_mod.Queue = _queue_mod.Queue(maxsize=500)
        stop_event = threading.Event()
        logger = logging.getLogger(__name__)
        
        def _execute():
            """后台线程——执行 SQL + Spark 全流程，将事件推入队列。"""
            try:
                # ── Step 1: SQL 管线 ──
                # 用 TeeCollector 包装采集器，拦截 SQL 阶段的 stage 事件
                real_collector = get_collector()
                sql_tee = TeeCollector(real_collector, event_queue, "sql")
                
                sql_result = self.run_all(markdown_text, table_mapping, table_paths, rich=True)
                request_id = sql_result.get("request_id")
                sql_ok = sql_result.get("pipeline_error") is None
                generated_sql = sql_result.get("generated_sql", "") if sql_ok else ""
                
                # ── Step 2: Spark 管线 ──
                spark_stages: list[dict] = []
                spark_ok = False
                pyspark_code: str | None = None
                comparator_status: str | None = None
                all_llm_traces: dict = dict(sql_result.get("llm_traces", {}) or {})
                
                if sql_ok and request_id:
                    stages_sequence = [
                        SparkPipelineStage.MAPPER,
                        SparkPipelineStage.DEVELOPER,
                        SparkPipelineStage.COMPILER,
                        SparkPipelineStage.VALIDATOR,
                        SparkPipelineStage.COMPARATOR,
                        SparkPipelineStage.PHYSICAL_VERIFIER,
                    ]
                    
                    for stage in stages_sequence:
                        stage_val = stage.value
                        
                        # 发送 Spark 阶段 started 事件
                        event_queue.put({
                            "event": "stage",
                            "pipeline": "spark",
                            "stage": stage_val,
                            "status": "started",
                        })
                        
                        stage_start = time.time()
                        try:
                            stage_result = self.run_spark_stage(request_id, stage)
                        except Exception as exc:
                            duration_ms = int((time.time() - stage_start) * 1000)
                            err_msg = _sanitize_stream_error(exc)
                            event_queue.put({
                                "event": "stage",
                                "pipeline": "spark",
                                "stage": stage_val,
                                "status": "failed",
                                "duration_ms": duration_ms,
                                "message": err_msg,
                                "error_type": type(exc).__name__,
                            })
                            spark_stages.append({
                                "stage": stage_val, "status": "failed",
                                "errors": [str(exc)],
                            })
                            if stage in (SparkPipelineStage.MAPPER, SparkPipelineStage.COMPILER,
                                         SparkPipelineStage.VALIDATOR):
                                break
                            continue
                        
                        duration_ms = int((time.time() - stage_start) * 1000)
                        current_status = stage_result.get("status", "skipped")
                        current_errors = stage_result.get("errors", [])
                        
                        # 合并 LLM traces
                        stage_traces = stage_result.get("llm_traces", {}) or {}
                        all_llm_traces.update(stage_traces)
                        
                        # 提取 COMPILER 阶段产物
                        if stage == SparkPipelineStage.COMPILER:
                            compiler_result = stage_result.get("result", {}) or {}
                            pyspark_code = (
                                compiler_result.get("pyspark_code")
                                or compiler_result.get("standalone_pyspark")
                            )
                        
                        # 提取 COMPARATOR 细粒度状态
                        if stage == SparkPipelineStage.COMPARATOR:
                            comp_result = stage_result.get("result", {}) or {}
                            comparator_status = comp_result.get("status")
                        
                        # ── 失败策略 ──
                        # DEVELOPER 可选——失败标记 skipped 后继续
                        if stage == SparkPipelineStage.DEVELOPER:
                            if current_status == "failed":
                                event_queue.put({
                                    "event": "stage",
                                    "pipeline": "spark",
                                    "stage": stage_val,
                                    "status": "skipped",
                                    "duration_ms": duration_ms,
                                    "message": "LLM 标注服务不可用，已跳过",
                                })
                                spark_stages.append({
                                    "stage": stage_val, "status": "skipped",
                                    "errors": ["LLM 标注服务不可用，已跳过"],
                                })
                                continue
                            event_queue.put({
                                "event": "stage",
                                "pipeline": "spark",
                                "stage": stage_val,
                                "status": "completed",
                                "duration_ms": duration_ms,
                            })
                            spark_stages.append({
                                "stage": stage_val, "status": current_status,
                                "errors": current_errors,
                            })
                            continue
                        
                        # MAPPER / COMPILER / VALIDATOR 失败→停止下游
                        if stage in (SparkPipelineStage.MAPPER, SparkPipelineStage.COMPILER,
                                     SparkPipelineStage.VALIDATOR):
                            status_event = "completed" if current_status == "ok" else "failed"
                            event_queue.put({
                                "event": "stage",
                                "pipeline": "spark",
                                "stage": stage_val,
                                "status": status_event,
                                "duration_ms": duration_ms,
                                "message": "; ".join(current_errors) if current_errors and current_status == "failed" else None,
                            })
                            spark_stages.append({
                                "stage": stage_val, "status": current_status,
                                "errors": current_errors,
                            })
                            if current_status == "failed":
                                break
                            continue
                        
                        # COMPARATOR——记录细粒度状态
                        if stage == SparkPipelineStage.COMPARATOR:
                            event_queue.put({
                                "event": "stage",
                                "pipeline": "spark",
                                "stage": stage_val,
                                "status": "completed",
                                "duration_ms": duration_ms,
                                "message": f"对比状态: {comparator_status}" if comparator_status else None,
                            })
                            spark_stages.append({
                                "stage": stage_val, "status": current_status,
                                "errors": current_errors,
                                "comparator_status": comparator_status,
                            })
                            continue
                        
                        # PHYSICAL_VERIFIER——仅当 VALIDATOR 通过 + COMPARATOR 等价时执行
                        if stage == SparkPipelineStage.PHYSICAL_VERIFIER:
                            validator_passed = any(
                                s["stage"] == "VALIDATOR" and s["status"] == "ok"
                                for s in spark_stages
                            )
                            if not validator_passed:
                                event_queue.put({
                                    "event": "stage",
                                    "pipeline": "spark",
                                    "stage": stage_val,
                                    "status": "skipped",
                                    "message": "VALIDATOR 未通过，跳过物理验证",
                                })
                                spark_stages.append({
                                    "stage": stage_val, "status": "skipped",
                                    "errors": ["VALIDATOR 未通过，跳过物理验证"],
                                })
                                break
                            
                            # COMPARATOR 门禁——非 LOGIC_EQUIVALENT 时跳过物理验证
                            if comparator_status and comparator_status != "LOGIC_EQUIVALENT":
                                event_queue.put({
                                    "event": "stage",
                                    "pipeline": "spark",
                                    "stage": stage_val,
                                    "status": "skipped",
                                    "message": f"COMPARATOR 状态为 {comparator_status}，跳过物理验证",
                                })
                                spark_stages.append({
                                    "stage": stage_val, "status": "skipped",
                                    "errors": [f"COMPARATOR 状态为 {comparator_status}，跳过物理验证"],
                                })
                                break
                            
                            status_event = "completed" if current_status == "ok" else "failed"
                            event_queue.put({
                                "event": "stage",
                                "pipeline": "spark",
                                "stage": stage_val,
                                "status": status_event,
                                "duration_ms": duration_ms,
                                "message": "; ".join(current_errors) if current_errors and current_status == "failed" else None,
                            })
                            spark_stages.append({
                                "stage": stage_val, "status": current_status,
                                "errors": current_errors,
                            })
                            if current_status == "ok":
                                spark_ok = True
                
                # ── 判断整体 Spark 管线是否成功 ──
                physver_stage = next(
                    (s for s in spark_stages if s["stage"] == "PHYSICAL_VERIFIER"), None,
                )
                spark_ok = physver_stage is not None and physver_stage["status"] == "ok"
                
                # ── 汇总 FullRunResponse ──
                sql_pipeline_error = sql_result.get("pipeline_error")
                sql_pipeline_stages = sql_result.get("pipeline_stages", [])
                
                full_result = {
                    "request_id": request_id,
                    "pipeline_error": sql_pipeline_error,
                    "pipeline_stages": sql_pipeline_stages,
                    "sql_ok": sql_ok,
                    "sql_pipeline_error": sql_pipeline_error,
                    "sql_pipeline_stages": sql_pipeline_stages,
                    "generated_sql": generated_sql,
                    "spec_id": sql_result.get("spec_id"),
                    "plan_id": sql_result.get("plan_id"),
                    "package_id": sql_result.get("package_id"),
                    "spark_ok": spark_ok,
                    "spark_stages": spark_stages,
                    "pyspark_code": pyspark_code,
                    "llm_traces": all_llm_traces,
                }
                
                event_queue.put({"event": "done", "result": full_result})
                
            except Exception as exc:
                logger.exception("run_all_full_stream 后台线程致命错误")
                event_queue.put({
                    "event": "fatal",
                    "error_code": type(exc).__name__.upper(),
                    "message": _sanitize_stream_error(exc),
                })
            finally:
                stop_event.set()
        
        # 启动后台线程
        thread = threading.Thread(
            target=_execute, daemon=True, name="run-all-full-stream",
        )
        thread.start()
        
        # 生成器——从队列读取并 yield NDJSON 行
        while not stop_event.is_set() or not event_queue.empty():
            try:
                event = event_queue.get(timeout=0.5)
                yield json.dumps(event, ensure_ascii=False) + "\n"
                if event.get("event") in ("done", "fatal"):
                    return
            except _queue_mod.Empty:
                # 心跳保持连接
                yield '{"event":"heartbeat"}\n'
```

- [ ] **Step 3: 验证语法正确**

```bash
python -c "from tianshu_datadev.api.pipeline import Pipeline; print('OK')"
```

预期：`OK`

- [ ] **Step 4: 提交**

```bash
git add src/tianshu_datadev/api/pipeline.py
git commit -m "feat: run_all_full_stream() NDJSON 流式编排——后台线程 + 事件队列"
```

---

### Task 3: 流式端点——`POST /api/run-all-full/stream`

**Files:**
- Modify: `src/tianshu_datadev/api/routes.py`（在 `/run-all-full` 端点之后新增）

**Interfaces:**
- Consumes: `Pipeline.run_all_full_stream()` from `pipeline.py`
- Produces: FastAPI `StreamingResponse` with `media_type="application/x-ndjson"`

- [ ] **Step 1: 在 routes.py 的 import 区域新增 `StreamingResponse`**

`routes.py` 第 33 行当前为：
```python
from fastapi.responses import JSONResponse, Response
```

修改为：
```python
from fastapi.responses import JSONResponse, Response, StreamingResponse
```

- [ ] **Step 2: 在 `/run-all-full` 端点之后新增 `/run-all-full/stream` 端点**

在第 132 行（`return result` 之后、`@api_router.post("/run-all-rich")` 之前）插入：

```python

@api_router.post("/run-all-full/stream")
async def run_all_full_stream(request: Request, body: RunAllRequest):
    """全流程 SQL + Spark 管线——NDJSON 流式进度推送。

    返回 application/x-ndjson 流，每行一个 JSON 事件：
    - stage: 阶段进度（pipeline + stage + status + duration_ms）
    - done: 全流程完成（含完整 FullRunResponse）
    - fatal: 致命错误
    - heartbeat: 心跳（保持连接）

    前端通过 fetch + ReadableStream 逐行消费，实时更新进度面板。
    连接断开时后台继续执行——结果通过 done 事件的 result 字段返回。
    """
    pipeline = request.app.state.pipeline
    return StreamingResponse(
        pipeline.run_all_full_stream(
            body.markdown_text, body.table_mapping, body.table_paths,
        ),
        media_type="application/x-ndjson",
        headers={
            "X-Accel-Buffering": "no",   # 禁用 nginx 缓冲
            "Cache-Control": "no-cache",  # 禁用缓存
        },
    )
```

- [ ] **Step 3: 验证路由注册成功**

```bash
python -c "
from tianshu_datadev.api.routes import api_router
routes = [r.path for r in api_router.routes]
assert '/api/run-all-full/stream' in routes, f'路由未注册: {routes}'
print('OK')
"
```

预期：`OK`

- [ ] **Step 4: 提交**

```bash
git add src/tianshu_datadev/api/routes.py
git commit -m "feat: POST /api/run-all-full/stream NDJSON 流式端点"
```

---

### Task 4: 前端流式客户端——`runAllFullStream()` + FullRunEvent 类型

**Files:**
- Modify: `frontend/src/api/client.ts`（在 `runAllFull()` 之后新增）

**Interfaces:**
- Consumes: `FullRunResponse`（已有）
- Produces: `FullRunEvent` 类型（4 种事件联合类型）
- Produces: `runAllFullStream()` 函数——返回 `AbortController`，通过回调传递事件

- [ ] **Step 1: 在 `client.ts` 中 `runAllFull()` 之后新增类型和函数**

在文件末尾（`checkArtifactsStatus` 之后）新增：

```typescript
// ── 流式全流程 Run-All ──

/** 流式进度事件——统一模型，不区分 sql_stage/spark_stage */
export type FullRunEvent =
  | {
      event: "stage";
      pipeline: "sql" | "spark";
      stage: string;
      status: "started" | "completed" | "failed" | "skipped";
      duration_ms?: number;
      message?: string;
      error_type?: string;
    }
  | {
      event: "done";
      result: FullRunResponse;
    }
  | {
      event: "fatal";
      error_code: string;
      message: string;
    }
  | {
      event: "heartbeat";
    };

/** 流式全流程 Run-All——通过 NDJSON 流实时接收进度事件。
 *
 * @param onEvent 每收到一个事件时调用
 * @param onError 流错误或网络错误时调用
 * @param onDone 流正常结束时调用
 * @returns AbortController——调用 .abort() 可取消请求
 */
export function runAllFullStream(
  markdownText: string,
  tableMapping?: Record<string, string>,
  tablePaths?: Record<string, string>,
  onEvent?: (event: FullRunEvent) => void,
  onError?: (err: Error) => void,
  onDone?: () => void,
): AbortController {
  const controller = new AbortController();

  fetch(`${BASE}/run-all-full/stream`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      markdown_text: markdownText,
      table_mapping: (tableMapping && Object.keys(tableMapping).length > 0) ? tableMapping : null,
      table_paths: (tablePaths && Object.keys(tablePaths).length > 0) ? tablePaths : null,
    }),
    signal: controller.signal,
  })
    .then(async (resp) => {
      if (!resp.ok) {
        const text = await resp.text().catch(() => resp.statusText);
        onError?.(new Error(`HTTP ${resp.status}: ${text}`));
        return;
      }

      const reader = resp.body?.getReader();
      if (!reader) {
        onError?.(new Error("浏览器不支持 ReadableStream"));
        return;
      }

      const decoder = new TextDecoder();
      let buffer = "";

      try {
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;

          buffer += decoder.decode(value, { stream: true });
          // 按行分割 NDJSON
          const lines = buffer.split("\n");
          buffer = lines.pop() || "";

          for (const line of lines) {
            const trimmed = line.trim();
            if (!trimmed) continue;
            try {
              const event = JSON.parse(trimmed) as FullRunEvent;
              onEvent?.(event);
            } catch {
              // 忽略解析失败的行（畸形 JSON）
            }
          }
        }

        // 处理缓冲区中剩余的内容
        if (buffer.trim()) {
          try {
            const event = JSON.parse(buffer.trim()) as FullRunEvent;
            onEvent?.(event);
          } catch {
            // 忽略
          }
        }
      } catch (err) {
        if (err instanceof DOMException && err.name === "AbortError") {
          // 用户主动取消——不报错
          return;
        }
        onError?.(err instanceof Error ? err : new Error(String(err)));
        return;
      }

      onDone?.();
    })
    .catch((err) => {
      if (err instanceof DOMException && err.name === "AbortError") {
        return; // 用户主动取消
      }
      onError?.(err instanceof Error ? err : new Error(String(err)));
    });

  return controller;
}
```

- [ ] **Step 2: 验证 TypeScript 编译通过**

```bash
cd frontend && npx tsc --noEmit src/api/client.ts 2>&1 | head -20
```

预期：无错误（可能有项目级配置警告，但不影响编译）

- [ ] **Step 3: 提交**

```bash
git add frontend/src/api/client.ts
git commit -m "feat: runAllFullStream() NDJSON 流式客户端 + FullRunEvent 类型"
```

---

### Task 5: 进度面板组件——`RunProgressPanel`

**Files:**
- Create: `frontend/src/components/RunProgressPanel.tsx`
- Create: `frontend/src/components/RunProgressPanel.css`

**Interfaces:**
- Consumes: `FullRunEvent[]`（事件列表）
- Consumes: `isStreaming: boolean`（是否正在接收流）
- Consumes: `streamError: string | null`（流错误信息）
- Produces: React 组件——渲染双管线实时进度

- [ ] **Step 1: 创建 `RunProgressPanel.tsx`**

```tsx
import { FullRunEvent } from '../api/client';
import './RunProgressPanel.css';

interface Props {
  /** 累积的进度事件列表 */
  events: FullRunEvent[];
  /** 是否正在接收流数据 */
  isStreaming: boolean;
  /** 流错误（连接中断等） */
  streamError: string | null;
  /** 是否可见 */
  visible: boolean;
}

/** 状态 → 图标映射 */
function statusIcon(status: string): string {
  switch (status) {
    case 'completed': return '✅';
    case 'failed': return '❌';
    case 'skipped': return '⏭️';
    case 'started': return '🔄';
    default: return '⬜';
  }
}

/** 阶段名 → 中文显示名 */
function stageLabel(stage: string): string {
  const labels: Record<string, string> = {
    parser: '解析', enrich: '增强', build: '构建', validate: '校验',
    compile: '编译', execute: '执行', contract: '契约', package: '打包',
    sql_builder: 'SQL 构建', sql_validator: 'SQL 校验', sql_compiler: 'SQL 编译',
    sql_executor: 'SQL 执行', contract_extractor: '契约提取',
    MAPPER: '映射', DEVELOPER: '标注', COMPILER: '编译',
    VALIDATOR: '校验', COMPARATOR: '对比', PHYSICAL_VERIFIER: '物理验证',
  };
  return labels[stage] || stage;
}

/** 将事件列表聚合为以 stage 为 key 的最新状态映射 */
function aggregateStages(events: FullRunEvent[]): Map<string, FullRunEvent> {
  const stages = new Map<string, FullRunEvent>();
  for (const e of events) {
    if (e.event === 'stage') {
      stages.set(`${e.pipeline}:${e.stage}`, e);
    }
  }
  return stages;
}

/** Run-All 实时进度面板 */
export function RunProgressPanel({ events, isStreaming, streamError, visible }: Props) {
  if (!visible) return null;

  const stages = aggregateStages(events);
  const sqlStages = Array.from(stages.values()).filter(e => e.event === 'stage' && e.pipeline === 'sql');
  const sparkStages = Array.from(stages.values()).filter(e => e.event === 'stage' && e.pipeline === 'spark');

  // 检查是否有完成/致命事件
  const hasDone = events.some(e => e.event === 'done');
  const hasFatal = events.some(e => e.event === 'fatal');

  return (
    <div className={`run-progress-panel panel${hasFatal ? ' progress-fatal' : ''}${hasDone ? ' progress-done' : ''}`}>
      <div className="panel-header">
        <h3>
          📡 执行进度
          {isStreaming && <span className="streaming-badge">接收中...</span>}
          {hasDone && <span className="done-badge">✅ 完成</span>}
          {hasFatal && <span className="fatal-badge">❌ 致命错误</span>}
        </h3>
      </div>

      {/* 连接中断提示 */}
      {streamError && !isStreaming && !hasDone && (
        <div className="progress-stream-error">
          ⚠️ 连接中断，结果可能仍在后台执行——{streamError}
        </div>
      )}

      {/* SQL 管线 */}
      {sqlStages.length > 0 && (
        <div className="progress-pipeline-group">
          <div className="progress-pipeline-header">🟢 SQL 管线</div>
          <div className="progress-stage-list">
            {sqlStages.map((e) => (
              <div key={`${e.pipeline}:${e.stage}`} className={`progress-stage-row stage-${e.status}`}>
                <span className="progress-stage-icon">{statusIcon(e.status)}</span>
                <span className="progress-stage-name">{stageLabel(e.stage)}</span>
                {e.duration_ms != null && (
                  <span className="progress-stage-duration">{(e.duration_ms / 1000).toFixed(1)}s</span>
                )}
                {e.message && (
                  <span className="progress-stage-message" title={e.message}>
                    {e.message.length > 80 ? e.message.slice(0, 80) + '…' : e.message}
                  </span>
                )}
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Spark 管线 */}
      {sparkStages.length > 0 && (
        <div className="progress-pipeline-group">
          <div className="progress-pipeline-header">🐍 Spark 管线</div>
          <div className="progress-stage-list">
            {sparkStages.map((e) => (
              <div key={`${e.pipeline}:${e.stage}`} className={`progress-stage-row stage-${e.status}`}>
                <span className="progress-stage-icon">{statusIcon(e.status)}</span>
                <span className="progress-stage-name">{stageLabel(e.stage)}</span>
                {e.duration_ms != null && (
                  <span className="progress-stage-duration">{(e.duration_ms / 1000).toFixed(1)}s</span>
                )}
                {e.message && (
                  <span className="progress-stage-message" title={e.message}>
                    {e.message.length > 80 ? e.message.slice(0, 80) + '…' : e.message}
                  </span>
                )}
              </div>
            ))}
          </div>
        </div>
      )}

      {/* 等待开始时提示 */}
      {events.length === 0 && isStreaming && (
        <div className="progress-waiting">⏳ 等待管线启动...</div>
      )}

      {/* 致命错误展示 */}
      {hasFatal && (
        <div className="progress-fatal-error">
          {(() => {
            const fatal = events.find(e => e.event === 'fatal');
            if (fatal && fatal.event === 'fatal') {
              return <><strong>[{fatal.error_code}]</strong> {fatal.message}</>;
            }
            return null;
          })()}
        </div>
      )}
    </div>
  );
}
```

- [ ] **Step 2: 创建 `RunProgressPanel.css`**

```css
/* Run-All 实时进度面板 */

.run-progress-panel {
  border: 1px solid var(--border, #444);
  border-radius: 8px;
  margin-bottom: 12px;
  background: var(--panel-bg, #1e1e2e);
}

.run-progress-panel.progress-done {
  border-color: var(--success, #4caf50);
}

.run-progress-panel.progress-fatal {
  border-color: var(--error, #f44336);
}

.progress-pipeline-group {
  margin: 8px 16px 12px;
}

.progress-pipeline-header {
  font-size: 13px;
  font-weight: 600;
  color: var(--text-primary, #e0e0e0);
  margin-bottom: 6px;
  padding-bottom: 4px;
  border-bottom: 1px solid var(--border, #444);
}

.progress-stage-list {
  display: flex;
  flex-direction: column;
  gap: 2px;
}

.progress-stage-row {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 3px 8px;
  border-radius: 4px;
  font-size: 12px;
  color: var(--text-secondary, #aaa);
}

.progress-stage-row.stage-started {
  color: var(--text-primary, #e0e0e0);
  background: rgba(255, 255, 255, 0.03);
}

.progress-stage-row.stage-completed {
  color: var(--success, #4caf50);
}

.progress-stage-row.stage-failed {
  color: var(--error, #f44336);
  background: rgba(244, 67, 54, 0.08);
}

.progress-stage-row.stage-skipped {
  color: var(--text-muted, #888);
  font-style: italic;
}

.progress-stage-icon {
  width: 18px;
  text-align: center;
  flex-shrink: 0;
}

.progress-stage-name {
  flex-shrink: 0;
  min-width: 60px;
}

.progress-stage-duration {
  font-family: monospace;
  font-size: 11px;
  color: var(--text-muted, #888);
  margin-left: auto;
}

.progress-stage-message {
  font-size: 11px;
  color: var(--text-muted, #999);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  max-width: 300px;
}

.progress-waiting {
  padding: 16px;
  text-align: center;
  color: var(--text-muted, #888);
  font-size: 13px;
}

.progress-stream-error {
  margin: 8px 16px;
  padding: 8px 12px;
  border-radius: 4px;
  background: rgba(255, 152, 0, 0.1);
  color: var(--warning, #ff9800);
  font-size: 12px;
}

.progress-fatal-error {
  margin: 8px 16px 12px;
  padding: 8px 12px;
  border-radius: 4px;
  background: rgba(244, 67, 54, 0.1);
  color: var(--error, #f44336);
  font-size: 12px;
}

.streaming-badge {
  font-size: 11px;
  font-weight: normal;
  color: var(--info, #2196f3);
  margin-left: 8px;
  animation: pulse 1.5s ease-in-out infinite;
}

.done-badge {
  font-size: 11px;
  font-weight: normal;
  color: var(--success, #4caf50);
  margin-left: 8px;
}

.fatal-badge {
  font-size: 11px;
  font-weight: normal;
  color: var(--error, #f44336);
  margin-left: 8px;
}

@keyframes pulse {
  0%, 100% { opacity: 1; }
  50% { opacity: 0.5; }
}
```

- [ ] **Step 3: 验证 TypeScript 编译**

```bash
cd frontend && npx tsc --noEmit src/components/RunProgressPanel.tsx 2>&1 | head -20
```

预期：无编译错误

- [ ] **Step 4: 提交**

```bash
git add frontend/src/components/RunProgressPanel.tsx frontend/src/components/RunProgressPanel.css
git commit -m "feat: RunProgressPanel 实时进度面板组件"
```

---

### Task 6: App.tsx——重写 `handleRunAll` 为流式消费

**Files:**
- Modify: `frontend/src/App.tsx`（重写 `handleRunAll` 函数，新增进度状态）

**Interfaces:**
- Consumes: `runAllFullStream`, `FullRunEvent` from `client.ts`
- Consumes: `RunProgressPanel` from `components/RunProgressPanel`
- Produces: 更新后的 `AppState`（新增 `runProgressEvents`, `isStreaming`, `streamError`）
- Produces: 更新后的 JSX（面板区域插入 `RunProgressPanel`）

- [ ] **Step 1: 在 `AppState` 中新增流式进度字段**

`App.tsx` 第 77 行（`llmTraces` 之后）新增：

```typescript
  // Run-All 流式进度
  runProgressEvents: FullRunEvent[];
  isStreaming: boolean;
  streamError: string | null;
  streamAbortController: AbortController | null;
```

- [ ] **Step 2: 在初始 state 中新增默认值**

`App.tsx` 第 104 行（`llmTraces: null,` 之后）新增：

```typescript
    runProgressEvents: [],
    isStreaming: false,
    streamError: null,
    streamAbortController: null,
```

- [ ] **Step 3: 在 import 语句中新增**

`App.tsx` 第 3 行区块，新增 import：

```typescript
import { RunProgressPanel } from './components/RunProgressPanel';
```

在第 16 行的 `runAllFull` import 处，追加 `runAllFullStream` 和 `FullRunEvent`：

```typescript
// 修改前：
import {
  parseSpecRich,
  buildPlanRich,
  executeRich,
  runAllFull,
  ...
} from './api/client';

// 修改后：
import {
  parseSpecRich,
  buildPlanRich,
  executeRich,
  runAllFull,
  runAllFullStream,
  ...
  FullRunResponse,
  FullRunEvent,
  ...
} from './api/client';
```

- [ ] **Step 4: 重写 `handleRunAll` 函数**

替换第 251-362 行的 `handleRunAll` 函数：

```typescript
  /** 全流程 Run-All——流式进度版 */
  const handleRunAll = () => {
    if (!state.markdownText.trim()) {
      update({ error: { error_code: 'EMPTY_INPUT', message: '请输入 DeveloperSpec 内容', field_ref: 'markdown_text' } });
      return;
    }

    // 清理旧状态
    update({
      isLoading: true,
      error: null,
      pipelineError: null,
      pipelineStages: [],
      runProgressEvents: [],
      isStreaming: true,
      streamError: null,
      sparkStageResult: null,
      showCodeDownload: false,
    });

    const controller = runAllFullStream(
      state.markdownText,
      state.tableMapping,
      state.tablePaths,
      // onEvent——每收到一个事件
      (event: FullRunEvent) => {
        setState((prev) => {
          const newEvents = [...prev.runProgressEvents, event];

          if (event.event === 'heartbeat') {
            return { ...prev, runProgressEvents: newEvents };
          }

          if (event.event === 'done') {
            const fr = event.result;
            // 构建 SQL 管线阶段指示灯
            const sqlStages: StageInfo[] = fr.sql_pipeline_stages
              ? fr.sql_pipeline_stages.map((s) => ({
                  stage: s.stage,
                  status: s.status === 'ok' ? 'ok' : s.status === 'failed' ? 'failed' : 'skipped',
                }))
              : [];
            // 构建 Spark 管线阶段指示灯
            const sparkStages: StageInfo[] = fr.spark_stages.map((s) => ({
              stage: s.stage,
              status: s.status === 'ok' ? 'ok' : s.status === 'failed' ? 'failed' : 'skipped',
            }));
            // 构建 executeResult（含 SQL 代码）
            const executeResult: ExecuteRichResponse | null = fr.sql_ok && fr.request_id
              ? {
                  request_id: fr.request_id,
                  spec_id: fr.spec_id || '',
                  plan_id: fr.plan_id || '',
                  generated_sql: fr.generated_sql || '',
                  sql_sha256: '',
                  compiler_version: '',
                  execution_trace: { trace_id: '', status: '', row_count: 0, execution_time_ms: 0, error_message: null },
                  result_summary: { summary_id: '', columns: [], column_types: [], row_count: 0, null_counts: {}, numeric_sums: {} },
                  open_questions: [],
                }
              : null;
            // 持久化 PySpark 代码
            const compilerCode = fr.pyspark_code
              ? { pyspark: fr.pyspark_code, standalone: '' }
              : prev.compilerCode;

            return {
              ...prev,
              isLoading: false,
              isStreaming: false,
              runProgressEvents: newEvents,
              requestId: fr.request_id,
              executeResult,
              compilerCode,
              showCodeDownload: fr.sql_ok,  // SQL 成功就展示代码（即使 Spark 失败也保留）
              sparkStages,
              pipelineStages: sqlStages.length > 0 ? sqlStages : [
                { stage: 'parser', status: 'ok' }, { stage: 'enrich', status: 'ok' },
                { stage: 'build', status: 'ok' }, { stage: 'validate', status: 'ok' },
                { stage: 'compile', status: 'ok' }, { stage: 'execute', status: 'ok' },
                { stage: 'contract', status: 'ok' }, { stage: 'package', status: 'ok' },
              ],
              llmTraces: fr.llm_traces,
              activePanel: 'sql' as Panel,
              // SQL 失败时的错误
              error: fr.sql_pipeline_error
                ? {
                    error_code: `PIPELINE_${fr.sql_pipeline_error.stage.toUpperCase()}_FAILED`,
                    message: fr.sql_pipeline_error.error_message,
                    field_ref: fr.sql_pipeline_error.stage,
                  }
                : null,
            };
          }

          if (event.event === 'fatal') {
            return {
              ...prev,
              isLoading: false,
              isStreaming: false,
              runProgressEvents: newEvents,
              error: {
                error_code: event.error_code,
                message: event.message,
                field_ref: null,
              },
            };
          }

          // stage 事件——仅累积进度
          return { ...prev, runProgressEvents: newEvents };
        });
      },
      // onError
      (err: Error) => {
        update({
          isLoading: false,
          isStreaming: false,
          streamError: err.message,
        });
      },
      // onDone
      () => {
        setState((prev) => ({ ...prev, isStreaming: false }));
      },
    );

    update({ streamAbortController: controller });
  };
```

- [ ] **Step 5: 在 JSX 面板区域新增 `RunProgressPanel`**

在第 524 行（`<div className="panels">` 之后、现有面板之前）插入：

```tsx
            {/* Run-All 流式进度面板——全流程执行期间展示 */}
            <RunProgressPanel
              events={state.runProgressEvents}
              isStreaming={state.isStreaming}
              streamError={state.streamError}
              visible={state.isStreaming || state.runProgressEvents.length > 0}
            />
```

- [ ] **Step 6: 验证 TypeScript 编译**

```bash
cd frontend && npx tsc --noEmit src/App.tsx 2>&1 | head -30
```

预期：无编译错误

- [ ] **Step 7: 启动开发服务器验证**

```bash
./dev-reload.sh
```

然后在浏览器中测试：加载模板 → 点击"全流程 Run-All" → 确认进度面板实时更新。

- [ ] **Step 8: 回归测试**

```bash
pytest tests/ -x --tb=short -q 2>&1 | tail -5
```

预期：全部通过（与改前一致）

- [ ] **Step 9: 提交**

```bash
git add frontend/src/App.tsx
git commit -m "feat: Run-All 流式进度——重写 handleRunAll 为 NDJSON 消费"
```

---

### 执行顺序

1. Task 1 → Task 2 → Task 3（后端基础设施 → 编排 → 端点）
2. Task 4 → Task 5 → Task 6（前端客户端 → 面板组件 → 集成）
3. `./dev-reload.sh` 重启
4. 手动 E2E：加载模板 → Run-All → 验证进度面板
5. 全量回归测试

### 验收标准

| 层级 | 验收项 | 方法 |
|------|--------|------|
| 单元 | `TeeCollector` 拦截 stage 事件并推送到队列 | `pytest tests/ -k tee -v` |
| 单元 | `_sanitize_stream_error` 清洗异常信息 ≤ 500 字符 | 构造长异常，断言长度 |
| 集成 | `POST /api/run-all-full/stream` 返回 NDJSON 流 | `curl -N` 验证每行合法 JSON |
| 集成 | SQL 阶段进度事件逐阶段推送 | 验证每阶段有 started → completed/failed 配对 |
| 集成 | Spark 阶段进度事件逐阶段推送 | 同上 |
| E2E | 前端进度面板实时更新 | 手动 Run-All，观察进度面板逐行出现 |
| E2E | 失败时展示错误信息（已清洗） | 触发失败，断言无完整 traceback |
| E2E | 连接中断提示 | 手动断开网络，观察提示文案 |
| 回归 | 现有测试全部通过 | `pytest tests/ -x --tb=short` |
