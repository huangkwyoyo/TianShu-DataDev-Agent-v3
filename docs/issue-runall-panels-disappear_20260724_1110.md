# Run-All 完成后前端面板消失问题——完整分析

> 提供给 Codex 的问题分析文档

---

## 1. 问题描述

点击"Run All"按钮执行全流程后，管道完成后**输出区只留下 MD 编辑器（SpecEditor）和执行进度面板（RunProgressPanel）**。以下面板全部消失：

| 面板 | 组件 | 预期触发条件 |
|------|------|-------------|
| 解析摘要 | `ParsePreview` | `state.specResult !== null` |
| 构建步骤 | `PlanStepsPanel` | `state.planResult?.steps?.length > 0` |
| SQL 代码展示 | `SqlDisplay` | `state.executeResult !== null && state.activePanel === 'sql'` |
| LLM 调用追踪 | `LlmTracePanel` | `state.executeResult !== null` |
| 代码下载区 | Code section | `state.showCodeDownload === true` |
| Join 证据 | `JoinEvidencePanel` | `state.planResult?.join_evidence?.length > 0` |

同时**顶部状态条一直闪黄色**（`status-loading`），因为 `isLoading` 从未被置为 `false`。

---

## 2. 架构——数据流

```
后端 run_all_full_stream()
  ↓ 后台线程执行 SQL+Spark 管线
  ↓ 汇总 FullRunResponse
  ↓ 将 {"event": "done", "result": FullRunResponse} 放入 event_queue
  ↓ 生成器 yield json.dumps(event) + "\n"
  ↓ NDJSON 流 → FastAPI StreamingResponse
  ↓
前端 fetch + ReadableStream
  ↓ TextDecoder + 按行分割
  ↓ JSON.parse → onEvent(event)
  ↓ event.event === 'done' → setState() 更新所有面板数据
  ↓
  ↓ 流结束 → onDone() 回调
React 重新渲染 → 面板展示
```

---

## 3. 根因——三层安全失效

### 第 1 层：`onDone` 回调不清理 `isLoading`

**文件**：`frontend/src/App.tsx:410-413`

```typescript
// 修复前——只清 isStreaming，不清 isLoading
onDone: () => {
  setState((prev) => ({ ...prev, isStreaming: false }));
}
// 修复后——同时清理 isLoading
onDone: () => {
  setState((prev) => ({ ...prev, isLoading: false, isStreaming: false }));
}
```

**后果**：如果 `done` 事件处理失败，`isLoading` 永远为 `true`，顶部条永远闪黄。`onDone` 是 NDJSON 流正常结束时的**最后一道安全网**，但这里没有兜底的 `isLoading: false`。

### 第 2 层：NDJSON catch 块沉默吞掉所有异常

**文件**：`frontend/src/api/client.ts:599-604`

```typescript
// 修复前——空 catch，错误完全沉默
try {
  const event = JSON.parse(trimmed) as FullRunEvent;
  onEvent?.(event);
} catch {
  // 忽略解析失败的行（畸形 JSON）
}
// 修复后——输出到控制台
} catch (parseErr) {
  console.error('[Stream] JSON 解析失败或 onEvent 异常:', parseErr, '行预览:', trimmed.slice(0, 200));
}
```

**后果**：如果 `JSON.parse` 失败，或 `onEvent` callback（即 `setState`）抛出异常，错误被完全吞掉，没有人知道发生了异常。且 React 的 `setState` 在 updater 函数内部抛异常时，会**丢弃整个状态更新**。

### 第 3 层：done 处理器无 try/catch 保护

**文件**：`frontend/src/App.tsx:295-382`

```typescript
// 修复前——整个 done 处理块没有任何 try/catch
if (event.event === 'done') {
  const fr = event.result;
  // ... 访问 fr 的各种属性 ...
  return { ...prev, isLoading: false, ... };
}
// 修复后——外层 try/catch
if (event.event === 'done') {
  try {
    // ... 完整处理逻辑 ...
  } catch (doneErr) {
    console.error('[RunAll done] 处理器异常:', doneErr);
    return { ...prev, isLoading: false, isStreaming: false, runProgressEvents: newEvents };
  }
}
```

**后果**：如果 `done` 处理中任意一行抛出异常（如访问 `fr.xxx` 但字段为 `undefined`），React 的 setState updater 抛异常后，`setState` 丢弃整个更新。所有面板数据（specResult、planResult、executeResult、showCodeDownload、llmTraces）全部丢失，前端 UI 停留在初始状态。

### 第 4 层（防御性）：后端 json.dumps 无保护

**文件**：`src/tianshu_datadev/api/pipeline.py:3149-3167`

```python
# 修复前——没有 try/except
line = json.dumps(event, ensure_ascii=False) + "\n"
yield line
# 修复后——有 try/except 保护
try:
    line = json.dumps(event, ensure_ascii=False) + "\n"
    yield line
except Exception as _serr:
    logger.error("流式事件 JSON 序列化失败: %s", _serr)
    yield json.dumps({"event": "fatal", ...}) + "\n"
    return
```

**后果**：如果 `FullRunResponse` 中有不可 JSON 序列化的字段（如 Enum 实例、datetime 对象），`json.dumps` 抛出异常，生成器崩溃，NDJSON 流提前终止。`done` 事件永远不会发送到前端，前端流循环 `break` 后调用 `onDone`，但由于第 1 层的缺陷，`isLoading` 无法清零。

---

## 4. 修复总结

| # | 文件 | 改动 | 层 | 作用 |
|---|------|------|-----|------|
| 1 | `App.tsx:412` | `onDone` 增设 `isLoading: false` | 安全网 | 即使 done 事件未成功处理，loading 态也清零 |
| 2 | `client.ts:602-603` | NDJSON catch 从空块改为 `console.error` | 诊断 | 不再沉默丢弃解析/处理异常 |
| 3 | `App.tsx:295-382` | done 处理块外层 try/catch | 容错 | 异常不会导致整个状态更新被丢弃 |
| 4 | `pipeline.py:3152-3167` | json.dumps 外层 try/except，发送 fatal 事件后终止 | 容错 | 反序列化失败不会沉默丢事件 |
| 5 | `pipeline.py:3123-3129` | done 事件入队前加 `logger.info` | 运营 | 确认后端是否产出了 spec_result/steps |

---

## 5. 前端渲染逻辑——面板可见性条件

### ParsePreview

```tsx
// App.tsx:634
{state.specResult && (
  <ParsePreview spec={state.specResult} visible={true} />
)}
```

**需要**：`state.specResult !== null`

**数据来源**：done 事件中 `fr.spec_result` → `setState({ specResult: fr.spec_result })`

---

### PlanStepsPanel

```tsx
// App.tsx:654
{state.planResult && state.planResult.steps?.length > 0 && (
  <PlanStepsPanel steps={state.planResult.steps} ... />
)}
```

**需要**：`state.planResult !== null && state.planResult.steps.length > 0`

**数据来源**：done 事件中由 `fr.steps` 构造：
```typescript
planResult: fr.steps?.length
  ? {
      request_id: fr.request_id || '',
      spec_id: fr.spec_id || '',
      plan_id: fr.plan_id || '',
      step_count: fr.steps.length,
      step_types: fr.steps.map(s => s.step_type),
      steps: fr.steps,
      multi_table: (fr.join_evidence?.length || 0) > 0,
      validation_passed: fr.sql_ok,
      open_questions: [],
      join_evidence: fr.join_evidence || [],
    }
  : prev.planResult
```

---

### SqlDisplay

```tsx
// App.tsx:766
{state.executeResult && (
  <SqlDisplay
    sql={state.executeResult.generated_sql}
    summary={state.executeResult.result_summary}
    visible={state.activePanel === 'sql'}
  />
)}
```

**需要**：`state.executeResult !== null && state.activePanel === 'sql'`

**数据来源**：done 事件中由 `fr.sql_ok` + `fr.request_id` 构造：
```typescript
const executeResult: ExecuteRichResponse | null = fr.sql_ok && fr.request_id
  ? {
      request_id: fr.request_id,
      spec_id: fr.spec_id || '',
      plan_id: fr.plan_id || '',
      generated_sql: fr.generated_sql || '',
      ...
    }
  : null;
```

同时 `activePanel: 'sql'` 也在 done 事件中设置。

---

### LlmTracePanel

```tsx
// App.tsx:776
<LlmTracePanel
  traces={state.llmTraces}
  visible={state.executeResult !== null}
/>
```

**需要**：`state.executeResult !== null`（visible 条件）

**数据来源**：done 事件中 `fr.llm_traces` → `setState({ llmTraces: fr.llm_traces })`

**缺陷**：visible 条件绑定的是 `state.executeResult !== null`，而不是 `state.llmTraces !== null`。即便 `llmTraces` 有数据，如果 `executeResult` 为 null，LLM 追踪面板也不显示。而在 `onDone callback` 里，`executeResult` 只在 `fr.sql_ok && fr.request_id` 时才有值——如果 SQL 管线失败，llm_traces（来自 Spark 阶段）即使有数据也不会显示。

---

### 代码下载区 (Code section)

```tsx
// App.tsx:673
{state.showCodeDownload && ( ... )}
```

**需要**：`state.showCodeDownload === true`

**数据来源**：done 事件中 `fr.sql_ok` → `setState({ showCodeDownload: fr.sql_ok })`

**依赖**：`state.executeResult?.generated_sql`（显示 SQL 代码块）和 `state.compilerCode`（显示 PySpark 代码块）

---

### JoinEvidencePanel

```tsx
// App.tsx:650
{state.planResult && state.planResult.join_evidence?.length > 0 && (
  <JoinEvidencePanel evidence={state.planResult.join_evidence} />
)}
```

**需要**：`state.planResult !== null && planResult.join_evidence.length > 0`

**数据来源**：done 事件中 `fr.join_evidence` → 嵌入 `planResult` 构造。

---

## 6. 完整的失败链路

```
1. 后端生成 done 事件
2. json.dumps 抛出异常（Enum/datetime 不可序列化）
3. 生成器崩溃，NDJSON 流提前结束
4. fetch while 循环的 done=true 触发 onDone()
5. onDone() 只设 isStreaming=false，不设 isLoading=false
6. 面板数据从未写入 state→UI 无变化
7. 顶部条闪黄（isLoading=true，永远不清零）
```

**或**

```
1. done 事件成功发送
2. onEvent() 中 JSON.parse 成功
3. onEvent 内部 setState updater 抛出异常（如访问 undefined 属性）
4. catch 块为空（修复前），错误完全沉默
5. React 丢弃本次 setState 更新
6. onDone() 同样只清 isStreaming，不清 isLoading
7. 面板数据从未写入 state→UI 无变化
```

**或**

```
1. done 事件成功发送
2. onEvent() 中 JSON.parse 成功
3. onEvent 内部 setState updater 抛出异常
4. done 处理器无 try/catch（修复前），异常传播到 React 调度器
5. setState 丢弃状态更新
6. 同上——面板不显示，loading 不清零
```

---

## 7. 验证方式

1. 打开浏览器控制台（F12 → Console）
2. 执行 Run-All
3. 观察日志：
   - `[RunAll done] keys: ...` → 显示后端返回的所有字段名
   - `[RunAll done] spec_result? true/false steps? N` → 关键字段是否存在
   - 红色 `[RunAll done] 处理器异常: ...` → done 处理失败
   - 红色 `[Stream] JSON 解析失败或 onEvent 异常: ...` → NDJSON 解析/处理失败
   - 后端日志 `run_all_full_stream done event:` → 确认后端产出了 done 事件

4. 面板应全部展示（当管线成功完成时）：
   - RunProgressPanel（收起）
   - ParsePreview（解析摘要）
   - PlanStepsPanel（构建步骤，含 Join 证据）
   - SqlDisplay（SQL 代码）
   - LlmTracePanel（横向指示灯）
   - 代码下载区（SQL + PySpark 下载）
