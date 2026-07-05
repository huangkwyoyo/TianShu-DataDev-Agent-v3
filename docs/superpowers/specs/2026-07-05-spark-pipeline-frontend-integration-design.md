# Spark 管线前端集成——PipelineStageIndicator 双管线展示

> 日期：2026-07-05 | 状态：设计完成，待实施计划
> 基于 brainstorming 会话形成——3 轮澄清 + 用户审批通过

## 1. 目标

在现有前端工作台中接入 Spark 管线阶段结果展示——用户先通过 SQL Run-All 产出 `request_id`，再点击"Spark 验证"按钮触发 Spark Orchestrator 执行，结果以独立的 `PipelineStageIndicator` 展示在 SQL 管线指示灯旁边。

**不代表**：实时流式进度、WebSocket/SSE 推送、Spark 物理执行、生产上线批准。

## 2. 用户交互流程

```
1. 用户在编辑器中编写 DeveloperSpec
2. 点击 "全流程 Run-All" → SQL 管线执行 → 返回 request_id
3. SQL PipelineStageIndicator 展示 6 阶段结果
4. "Spark 验证"按钮启用（依赖 request_id 非空）
5. 用户点击 "Spark 验证" → POST /api/spark/verify
6. 右侧出现第二个 PipelineStageIndicator——展示 Spark 6 阶段结果
```

## 3. 后端设计

### 3.1 新端点：`POST /api/spark/verify`

**请求**
```json
{
  "request_id": "req_xxx"
}
```

**处理流程**
```
Pipeline.export_artifacts(request_id)
  ├─ None → 404 SPARK_ARTIFACTS_NOT_FOUND
  ├─ sql_build_plan 或 data_transform_contract 缺失 → 422 SPARK_ARTIFACTS_INCOMPLETE
  └─ 完整 →
      adapt_lite_to_v1(contract) → DataTransformContractV1
      → SparkOrchestrator.run(contract=v1, sql_plan=sql_build_plan)
      → SparkReviewBuilder.build(state)
      → 200 响应
```

**成功响应（200）**
```json
{
  "request_id": "req_xxx",
  "spark_stages": [
    {"stage": "MAPPER", "status": "ok"},
    {"stage": "DEVELOPER", "status": "skipped"},
    {"stage": "COMPILER", "status": "ok"},
    {"stage": "VALIDATOR", "status": "ok"},
    {"stage": "COMPARATOR", "status": "ok"},
    {"stage": "PHYSICAL_VERIFIER", "status": "skipped"}
  ],
  "overall_status": "LOGIC_CONSISTENT_PHYSICAL_NOT_EXECUTED",
  "comparator_status": "LOGIC_EQUIVALENT",
  "review_ready": true,
  "package_id": "pkg_xxx",
  "errors": []
}
```

**错误响应**

| 错误码 | HTTP | 触发条件 |
|--------|------|----------|
| `SPARK_ARTIFACTS_NOT_FOUND` | 404 | `export_artifacts(request_id)` 返回 None |
| `SPARK_ARTIFACTS_INCOMPLETE` | 422 | `sql_build_plan` 或 `data_transform_contract` 为 None |
| `SPARK_VERIFY_FAILED` | 500 | Orchestrator 执行异常 |

### 3.2 后端状态 → 前端 status 映射

| SparkPipelineState 值 | 前端 status |
|------------------------|-------------|
| `SUCCESS` | `ok` |
| `FAILURE` | `failed` |
| `HUMAN_REVIEW` | `failed` |
| `SKIPPED` | `skipped` |
| `NOT_EXECUTED` | `skipped` |

### 3.3 修改文件

| 文件 | 改动 |
|------|------|
| `src/tianshu_datadev/api/models.py` | 新增 `SparkVerifyRequest`、`SparkVerifyResponse`、`SparkStageItem` 模型 |
| `src/tianshu_datadev/api/routes.py` | 新增 `POST /api/spark/verify` 端点（~40 行） |

## 4. 前端设计

### 4.1 API 客户端扩展

**文件**：`frontend/src/api/client.ts`

新增类型：
```typescript
/** Spark 单个阶段结果 */
export interface SparkStageItem {
  stage: string;
  status: 'ok' | 'failed' | 'skipped';
}

/** Spark 验证响应 */
export interface SparkVerifyResponse {
  request_id: string;
  spark_stages: SparkStageItem[];
  overall_status: string;
  comparator_status: string;
  review_ready: boolean;
  package_id: string;
  errors: string[];
}
```

新增方法：
```typescript
/** 触发 Spark 管线验证 */
export function sparkVerify(requestId: string): Promise<SparkVerifyResponse> {
  return apiPost('/spark/verify', { request_id: requestId });
}
```

### 4.2 PipelineStageIndicator 扩展

**文件**：`frontend/src/components/PipelineStageIndicator.tsx`

改动（复用现有组件，不新建）：
- `Props` 新增可选 `title?: string`——默认 "流水线阶段"
- `STAGE_CN` 映射扩展——新增 Spark 6 阶段：

```typescript
const STAGE_CN: Record<string, string> = {
  // SQL 侧（已有）
  parser: '解析', enrich: '增强', build: '构建',
  validate: '验证', compile: '编译', execute: '执行',
  // Spark 侧（新增）
  MAPPER: '映射', DEVELOPER: '标注', COMPILER: '编译',
  VALIDATOR: '校验', COMPARATOR: '对比', PHYSICAL_VERIFIER: '物理验证',
};
```

- 下拉框 header 使用 `title` 而非硬编码 "流水线阶段"

### 4.3 App.tsx 集成

**文件**：`frontend/src/App.tsx`

改动：
1. `AppState` 新增 `sparkStages: StageInfo[]`、`sparkResult: SparkVerifyResponse | null`
2. 新增 "Spark 验证" 按钮——`disabled={!state.requestId || state.isLoading}`
3. 新增点击处理函数 `handleSparkVerify`
4. 右侧 header 区域新增第二个 `PipelineStageIndicator`——传入 `title="Spark 管线"`

```typescript
// 按钮
<button
  className="btn btn-accent"
  disabled={!state.requestId || state.isLoading}
  onClick={handleSparkVerify}
>
  Spark 验证
</button>

// 第二个指示灯
<PipelineStageIndicator
  stages={state.sparkStages}
  error={null}
  title="Spark 管线"
/>
```

### 4.4 修改文件

| 文件 | 改动 |
|------|------|
| `frontend/src/api/client.ts` | 新增 2 类型 + 1 方法（~30 行） |
| `frontend/src/components/PipelineStageIndicator.tsx` | 扩展 `title` prop + `STAGE_CN`（~10 行） |
| `frontend/src/App.tsx` | 新增按钮 + 状态 + 第二个指示灯（~40 行） |

## 5. 全局约束

- **允许**：修改 `api/models.py`（新增请求/响应模型）
- **允许**：修改 `api/routes.py`（新增 1 个端点）
- **允许**：修改前端 `client.ts`、`PipelineStageIndicator.tsx`、`App.tsx`
- **允许**：新增 `tests/api/test_spark_verify.py`（API 端点测试）
- **禁止**：修改 SQL Pipeline 语义（`run_all` / `execute` / `build_plan` 不动）
- **禁止**：修改 `Orchestrator.run()` 接口和逻辑
- **禁止**：修改 `PlanComparator` 判定规则
- **禁止**：引入真实 LLM、生产数据、Spark 物理执行
- **禁止**：在 UI 中使用"实时"描述——这是一次性请求-响应
- **表述约定**：`review_ready=true` 只写"自动审查材料就绪"，不写"生产可上线"

## 6. 测试策略

### 后端测试（`tests/api/test_spark_verify.py`）

1. **正常流程**：先跑 `run_all` → 拿到 `request_id` → `POST /api/spark/verify` → 200 + 6 阶段状态 + `review_ready=true`
2. **无效 request_id**：`POST /api/spark/verify` with 不存在的 request_id → 404 `SPARK_ARTIFACTS_NOT_FOUND`
3. **TTL 过期**：`export_artifacts` 因 TTL 返回 None → 404
4. **阶段失败场景**：注入失败 → `review_ready=false`

### 前端验证

- "Spark 验证"按钮在无 request_id 时 disabled
- 点击后第二个指示灯出现并展示 6 阶段
- 错误响应正确展示在 ErrorDisplay

## 7. 验收命令

```bash
# 后端
python -m pytest tests/api/test_spark_verify.py -v --tb=short
python -m pytest tests/api/ tests/spark/ -q
python -m ruff check src/tianshu_datadev/api/ tests/api/

# 前端
cd frontend && npx tsc --noEmit
```
