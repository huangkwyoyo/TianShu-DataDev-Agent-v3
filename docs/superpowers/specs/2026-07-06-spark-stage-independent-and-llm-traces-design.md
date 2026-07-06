# Spark 阶段独立触发 + LLM 调用追踪 — 前端交互增强设计

> 设计日期：2026-07-06 | 状态：已确认，待实施
> 目标：让 spark_first 阶段可在 sql_first 的 execute-rich 成功后手动触发，同时为展示 LLM 调用信息预留 llm_traces 字段。

## 1. 背景与现状问题

### 1.1 Spark 管线入口过窄

当前 Spark 管线只能通过"全流程 Run-All"触发。根因不是前端按钮问题，而是 `execute-rich` 成功路径只缓存了 `plan`、`compiled_sql`、`trace`、`summary`，没有 `data_transform_contract`。`/api/spark/verify` 调用 `export_artifacts(request_id)` 时发现 `contract` 为 None，返回 422。

### 1.2 LLM 调用无可见性

`LlmResponse` 已有 `token_usage` 和 `latency_ms` 字段，但前端完全不可见。用户无法了解各 LLM 节点的调用次数、Token 消耗和延迟。

## 2. 设计目标

1. **Spark 独立触发**：SQL 编译执行成功后，6 个 Spark 阶段各为独立按钮，手动触发，不再依赖 Run-All
2. **LLM 调用可见**：SQL 编译执行后、Spark 单阶段后、Run-All 后，前端展示已累积的 LLM 调用诊断信息
3. **零破坏**：不解析 SQL 文本、不调 LLM 生成 Contract、LLM traces 不影响路由/验证/REVIEW_READY

## 3. 全局约束

- Contract 必须从已验证的 SqlBuildPlan / SqlProgram 确定性抽取，使用现有 `DataTransformContractExtractor`
- LLM traces 只能含诊断元数据：`node_name`、`model`、`token_usage`、`latency_ms`、`status`、`error_type`
- traces 不可进入 IR、不可影响路由、不可参与 REVIEW_READY 判定
- traces 不做长期 Memory——最多 request-scoped cache
- 不新增自定义 Contract 抽取函数——不重复现有规则
- 不新增独立 `GET /api/llm-traces/{request_id}` 端点（除非后续需要跨请求审计）
- 所有代码注释使用中文

## 4. 架构变更总览

```
                        DeveloperSpec
                             │
              ┌──────────────┼──────────────┐
              ▼              ▼              ▼
           解析预览       构建 Plan      编译执行
              │              │              │
              │              │   ┌──────────┘
              │              │   ▼
              │              │  SqlBuildPlan/SqlProgram
              │              │  + ExecutionTrace + ResultSummary
              │              │  + DataTransformContract  ← 前置修正
              │              │  + llm_traces (SQL侧最多4节点)
              │              │
              ▼              ▼              ▼
         ┌──────────────────────────────────────┐
         │     Spark 独立阶段 (6 个按钮)          │
         │  ┌────┬────┬────┬────┬────┬────┐    │
         │  │映射│标注│编译│校验│对比│物理│    │
         │  └────┴────┴────┴────┴────┴────┘    │
         │  各按钮 enable/disable 绑定依赖状态    │
         │  每次执行返回该阶段新增 llm_traces     │
         └──────────────────────────────────────┘
                        │
                        ▼
              ┌──────────────────┐
              │  Run-All 一键执行  │
              │  (全量 llm_traces) │
              └──────────────────┘
```

## 5. 后端设计

### 5.1 前置修正：execute-rich 成功后抽取 Contract

**文件**：`src/tianshu_datadev/api/pipeline.py`

在 `execute_rich()` 成功路径中，`_store_result()` 调用前，插入 Contract 抽取：

```python
# ── 确定性抽取 Contract（供 Spark 管线使用）──
# 前提：plan 已通过 Validator 校验，sql_program/compiled_sql 已在当前作用域
from tianshu_datadev.artifacts.contract_extractor import DataTransformContractExtractor

extractor = DataTransformContractExtractor()
contract = None
if sql_program is not None:
    contract = extractor.extract_v1(sql_program)
elif plan is not None:
    contract = extractor.extract(plan)

self._store_result(request_id, {
    ...
    "contract": contract,  # 新增字段
    "llm_traces": self._get_llm_traces(request_id),  # 新增字段
})
```

**关键约束**：
- 使用现有 `DataTransformContractExtractor.extract()` / `extract_v1()`，不自定义抽取规则
- SqlProgram 路径优先使用 `extract_v1()`（已处理 statements、step_dag、temp_tables、CASE/Window）
- 单 SqlBuildPlan 路径使用 `extract()`（Lite 版）
- `export_artifacts()` 已有读取 `"contract"` 字段的逻辑，无需修改

### 5.2 LLM Trace 数据模型

**文件**：`src/tianshu_datadev/llm/models.py`（新增类）

```python
class LlmTraceNode(StrictModel):
    """单个 LLM 节点调用诊断元数据。
    
    仅含诊断信息——不含 prompt 原文、raw response、业务数据。
    不可进入 IR、不可影响路由、不可参与 REVIEW_READY 判定。
    """
    node_name: str
    # 合法值：
    #   "parse_developer_spec" | "relationship_planner" |
    #   "sql_build_planner" | "sql_program_planner" | "spark_developer"
    model: str              # 实际模型标识（Fake 时为 "fake"）
    token_usage: dict[str, int] = {}
    # {"prompt_tokens": N, "completion_tokens": N, "total_tokens": N}
    latency_ms: int = 0     # 总延迟（毫秒）
    status: str = "skipped"
    # "valid" | "invalid" | "skipped" | "error"
    error_type: str | None = None
    # 失败时的 AdapterError 类型字符串
```

**累积规则**（Pipeline 内部）：
- `_results[request_id]` 中维护 `llm_traces: dict[str, LlmTraceNode]`
- 每次 LLM Gateway 返回 `LlmResponse` 后调用 `_record_llm_trace(request_id, response)`
- 同一 `node_name` 多次调用 → 保留最后一次（不聚合）
- 累积方法签名：`def _record_llm_trace(self, request_id: str, response: LlmResponse) -> None`

**透出规则**：
- `execute-rich` 响应 → 返回 SQL 侧已累积的 1-4 个节点
- `spark/*` 单阶段响应 → 返回该阶段新增节点（如有，即 DEVELOPER 阶段）
- `run-all` / `run-all-rich` 响应 → 返回全量 1-5 个节点
- 所有响应模型中 `llm_traces` 字段为可选 `dict[str, LlmTraceNode] | None`

### 5.3 Spark 阶段 Dispatcher

**文件**：`src/tianshu_datadev/api/pipeline.py`（新增方法）

```python
def run_spark_stage(
    self,
    request_id: str,
    stage: SparkPipelineStage,
) -> SparkStageResponse:
    """执行单个 Spark 管线阶段。

    流程：
    1. export_artifacts(request_id) → 获取 contract + sql_plan
    2. _get_or_create_spark_context(request_id) → 获取或创建阶段上下文
    3. _check_stage_dependencies(stage, context) → 依赖门禁
    4. 执行该阶段（复用现有组件，不通过 SparkOrchestrator.run()）
    5. 缓存中间产物到 SparkStageContext
    6. 收集 llm_traces（仅 DEVELOPER 阶段产生）
    7. 返回 SparkStageResponse
    """
```

**SparkStageContext**（Pipeline 内部类，`_spark_contexts: dict[str, SparkStageContext]`）：

```python
@dataclass
class SparkStageContext:
    """request_id 级别的 Spark 阶段中间产物缓存。"""
    spark_plan: SparkPlan | None = None
    compile_result: SparkCompileResult | None = None
    comparator_report: PlanComparisonReport | None = None
    stage_results: dict[str, str] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
```

**SparkDependencyMissingError 异常类**（`pipeline.py` 内部）：

```python
class SparkDependencyMissingError(Exception):
    """Spark 阶段依赖缺失异常——由 _check_stage_dependencies 抛出。"""
    def __init__(self, stage: SparkPipelineStage, missing: list[str]):
        self.stage = stage
        self.missing = missing
        super().__init__(
            f"阶段 {stage.value} 缺少前置产物：{', '.join(missing)}"
        )
```

**不直接复用 `SparkOrchestrator.run()`** 的原因：`run()` 会重置内部缓存（`self._cached_plan = None`），无法支持单阶段逐步执行。Pipeline 层自行维护 `SparkStageContext`，只调用各阶段组件的核心方法。

**依赖门禁**（`_check_stage_dependencies`）：

| 阶段 | 需要的前置产物 | 缺失时返回 |
|------|--------------|-----------|
| MAPPER | `data_transform_contract` | `SPARK_DEPENDENCY_MISSING` (422) |
| DEVELOPER | `spark_plan` | `SPARK_DEPENDENCY_MISSING` (422) |
| COMPILER | `spark_plan` | `SPARK_DEPENDENCY_MISSING` (422) |
| VALIDATOR | `SparkCompileResult.raw_pyspark` | `SPARK_DEPENDENCY_MISSING` (422) |
| COMPARATOR | `sql_plan` + `spark_plan` + `contract` | `SPARK_DEPENDENCY_MISSING` (422) |
| PHYSICAL_VERIFIER | `compiled_sql` + `compile_result` | 当前保持 SKIPPED |

**DEVELOPER 特殊处理**：如果 `SparkDeveloperService` 未配置 → 不阻断，返回 SKIPPED。

### 5.4 REST 端点

**文件**：`src/tianshu_datadev/api/routes.py`

**请求模型**：

```python
class SparkStageRequest(StrictModel):
    request_id: str
```

**响应模型**：

```python
class SparkStageResponse(StrictModel):
    request_id: str
    stage: str                    # 阶段名
    status: str                   # "ok" | "failed" | "skipped"
    missing_dependencies: list[str] = []  # 依赖缺失时的缺失项列表
    errors: list[str] = []        # 错误信息
    spark_stages: list[SparkStageItem] = []  # 当前全部阶段状态（用于前端更新指示灯）
    llm_traces: dict[str, LlmTraceNode] | None = None  # 本阶段新增的 LLM 追踪
```

**6 个端点**（统一委托给内部函数）：

```python
@api_router.post("/spark/map")
async def spark_map(request: Request, body: SparkStageRequest):
    return _handle_spark_stage(request, body.request_id, SparkPipelineStage.MAPPER)

@api_router.post("/spark/develop")
async def spark_develop(request: Request, body: SparkStageRequest):
    return _handle_spark_stage(request, body.request_id, SparkPipelineStage.DEVELOPER)

@api_router.post("/spark/compile")
async def spark_compile(request: Request, body: SparkStageRequest):
    return _handle_spark_stage(request, body.request_id, SparkPipelineStage.COMPILER)

@api_router.post("/spark/validate")
async def spark_validate(request: Request, body: SparkStageRequest):
    return _handle_spark_stage(request, body.request_id, SparkPipelineStage.VALIDATOR)

@api_router.post("/spark/compare")
async def spark_compare(request: Request, body: SparkStageRequest):
    return _handle_spark_stage(request, body.request_id, SparkPipelineStage.COMPARATOR)

@api_router.post("/spark/physical-verify")
async def spark_physical_verify(request: Request, body: SparkStageRequest):
    return _handle_spark_stage(request, body.request_id, SparkPipelineStage.PHYSICAL_VERIFIER)


def _handle_spark_stage(
    request: Request,
    request_id: str,
    stage: SparkPipelineStage,
) -> SparkStageResponse | JSONResponse:
    """Spark 阶段统一处理——参数校验、异常转换、调用 dispatcher。"""
    pipeline = request.app.state.pipeline
    try:
        return pipeline.run_spark_stage(request_id, stage)
    except SparkDependencyMissingError as e:
        return JSONResponse(
            status_code=422,
            content={
                "error_code": "SPARK_DEPENDENCY_MISSING",
                "message": str(e),
                "field_ref": e.stage.value if e.stage else None,
                "missing_dependencies": e.missing,
            },
        )
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={
                "error_code": "SPARK_STAGE_FAILED",
                "message": f"Spark 阶段 {stage.value} 执行异常：{e}",
                "field_ref": stage.value,
            },
        )
```

**保留原有端点**：`POST /api/spark/verify` 保留作为一键全链路入口（向后兼容）。

## 6. 前端设计

### 6.1 Spark 阶段按钮组件

**文件**：`frontend/src/components/SparkStageButtons.tsx`（新建）

6 个独立按钮，横排展示。每个按钮的 disabled/enabled 状态与后端 artifact 状态绑定：

```
┌────────────────────────────────────────────────────────────┐
│  Spark 管线节点                                             │
│  ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐ │
│  │ ① 映射 │ │ ② 标注 │ │ ③ 编译 │ │ ④ 校验 │ │ ⑤ 对比 │ │⑥物理验证│ │
│  │ MAPPER │ │DEVELOP │ │COMPILER│ │VALIDATOR│ │COMPARA │ │PHYSICAL│ │
│  │  ⬜    │ │  ⬜    │ │  ⬜    │ │  ⬜    │ │  ⬜    │ │  ⬜    │ │
│  └────────┘ └────────┘ └────────┘ └────────┘ └────────┘ └────────┘ │
└────────────────────────────────────────────────────────────┘
```

**按钮状态规则**：

| 条件 | 视觉表现 | 交互 |
|------|---------|------|
| 上游依赖满足，未执行 | 正常色 + ⬜ | 可点击 |
| 上游依赖缺失 | 灰色 + ⬜，tooltip 显示缺失项 | **disabled** |
| 已执行成功 | 绿色 ✅ | 可重复点击 |
| 已执行失败 | 红色 ❌ + tooltip 显示错误 | 可点击重试 |
| 已 skipped | 灰色 ⏭️ | 可点击 |

**依赖链计算**（前端 `useMemo`）：

```typescript
// sparkStages 来自后端每次响应返回的 spark_stages 数组
function computeAvailableStages(
  stages: StageInfo[]
): Set<string> {
  const available = new Set<string>();
  available.add('MAPPER'); // 始终可用（依赖 contract——execute-rich 已确保）
  
  if (stages.find(s => s.stage === 'MAPPER' && s.status === 'ok')) {
    available.add('DEVELOPER');
    available.add('COMPILER');
  }
  if (stages.find(s => s.stage === 'COMPILER' && s.status === 'ok')) {
    available.add('VALIDATOR');
  }
  if (stages.find(s => s.stage === 'MAPPER' && s.status === 'ok')) {
    available.add('COMPARATOR'); // 额外需要 sql_plan——execute-rich 已确保
  }
  if (stages.find(s => s.stage === 'COMPILER' && s.status === 'ok')) {
    available.add('PHYSICAL_VERIFIER');
  }
  return available;
}
```

### 6.2 LLM 调用追踪面板

**文件**：`frontend/src/components/LlmTracePanel.tsx`（新建）

可折叠表格，紧跟对应阶段的产出面板下方展示。

```typescript
interface LlmTraceNode {
  node_name: string;
  model: string;
  token_usage: Record<string, number>;
  latency_ms: number;
  status: string;
  error_type: string | null;
}

interface Props {
  traces: Record<string, LlmTraceNode> | null;
  visible: boolean;
}
```

**展示规则**：
- 无数据时返回 `null`（不渲染）
- 有数据时显示可折叠面板，默认折叠
- 表头：`节点名称 | 模型 | Prompt Token | Completion Token | 总 Token | 耗时 | 状态`
- 底部汇总行显示合计
- 节点名称使用中文映射（同现有 `STAGE_CN` 模式）

**出现位置**：
- "编译执行"成功后 → 紧跟 SQL 结果面板
- Spark 单阶段完成后 → 紧跟 Spark 阶段结果
- "全流程 Run-All"成功后 → 紧跟 Package 面板
- `llm_traces` 为 `null` 或空 → 不显示

### 6.3 App.tsx 变更摘要

- 新增状态字段：`sparkStageResults: StageInfo[]`
- 移除：单 `handleSparkVerify` 函数
- 新增：`handleSparkStage(stage: string)` 函数——调用对应 `/api/spark/{stage}` 端点
- "Spark 验证"按钮 → 替换为 `<SparkStageButtons>` 组件
- 条件渲染 `<LlmTracePanel>` ——编译执行后、Spark 单阶段后、Run-All 后

### 6.4 API Client 新增

**文件**：`frontend/src/api/client.ts`

```typescript
// Spark 单阶段请求
export interface SparkStageRequest {
  request_id: string;
}

// Spark 单阶段响应
export interface SparkStageResponse {
  request_id: string;
  stage: string;
  status: 'ok' | 'failed' | 'skipped';
  missing_dependencies: string[];
  errors: string[];
  spark_stages: SparkStageItem[];
  llm_traces: Record<string, LlmTraceNode> | null;
}

// 6 个阶段 API 方法
const SPARK_STAGES = ['map', 'develop', 'compile', 'validate', 'compare', 'physical-verify'] as const;

export function runSparkStage(
  requestId: string,
  stage: string,
): Promise<SparkStageResponse> {
  return apiPost(`/spark/${stage}`, { request_id: requestId });
}
```

### 6.5 已有响应模型扩展

以下已有接口的响应类型需要新增可选 `llm_traces` 字段：

| 接口 | 当前 TypeScript 类型 | 新增字段 |
|------|---------------------|---------|
| `POST /api/execute-rich` | `ExecuteRichResponse` | `llm_traces?: Record<string, LlmTraceNode>` |
| `POST /api/run-all` | `RunAllResponse` | `llm_traces?: Record<string, LlmTraceNode>` |
| `POST /api/run-all-rich` | （同上） | `llm_traces?: Record<string, LlmTraceNode>` |

## 7. 测试计划

### 7.1 后端 pytest

**文件**：`tests/api/test_spark_stage_independent.py`（新建）

| 测试用例 | 验证点 |
|---------|--------|
| `test_execute_rich_produces_contract` | execute-rich 成功后 `export_artifacts()` 返回的 bundle 含非空 `contract` |
| `test_spark_map_after_execute_rich` | execute-rich 后调用 `POST /api/spark/map` 返回 200 |
| `test_spark_compile_missing_spark_plan` | 未执行 MAPPER 直接调用 COMPILE 返回 422 + `SPARK_DEPENDENCY_MISSING` |
| `test_spark_developer_skipped_without_service` | DEVELOPER 未配置时返回 SKIPPED，不阻断 |
| `test_spark_validate_missing_compile_result` | 未执行 COMPILER 直接调用 VALIDATE 返回 422 |
| `test_spark_compare_needs_sql_and_spark_plan` | 缺少 SqlBuildPlan 或 SparkPlan 时返回 422 |
| `test_llm_traces_in_execute_rich_response` | execute-rich 响应含 `llm_traces` 字段 |
| `test_llm_traces_in_spark_stage_response` | spark 单阶段响应含 `llm_traces` 字段 |
| `test_llm_traces_not_in_review_ready` | llm_traces 不参与 REVIEW_READY 判定 |

### 7.2 前端（可选，后续 Phase）

| 测试 | 验证点 |
|------|--------|
| Spark 按钮初始状态 | execute-rich 完成后仅 MAPPER 可点击 |
| 依赖链点亮 | MAPPER 成功后 DEVELOPER + COMPILER 变为可点击 |
| LlmTracePanel 渲染 | 有 traces 数据时正确渲染表格 |
| 错误展示 | 依赖缺失时显示 422 错误提示 |

## 8. 不在设计范围内

- 独立 `GET /api/llm-traces/{request_id}` 端点（页面刷新后 traces 丢失——可后续添加）
- Spark 阶段并发执行（当前逐阶段手动触发）
- LLM traces 跨请求聚合统计面板
- PHYSICAL_VERIFIER 的真实 Spark 执行环境接入
- 6 个端点外的一键全链路 Spark 按钮（已有的 `/api/spark/verify` 保持不变）

## 9. 关键文件变更清单

| 文件 | 操作 | 内容 |
|------|:--:|------|
| `src/tianshu_datadev/api/pipeline.py` | 修改 | execute_rich() 添加 Contract 抽取 + llm_traces 累积；新增 `run_spark_stage()` + `SparkStageContext` |
| `src/tianshu_datadev/api/routes.py` | 修改 | 新增 6 个 `/api/spark/*` 端点 + `_handle_spark_stage()` |
| `src/tianshu_datadev/llm/models.py` | 修改 | 新增 `LlmTraceNode` 模型 |
| `src/tianshu_datadev/api/models.py` | 修改 | 新增 `SparkStageRequest`、`SparkStageResponse` |
| `frontend/src/components/SparkStageButtons.tsx` | 新建 | 6 个独立 Spark 阶段按钮组件 |
| `frontend/src/components/LlmTracePanel.tsx` | 新建 | LLM 调用追踪面板组件 |
| `frontend/src/App.tsx` | 修改 | 替换单按钮为 SparkStageButtons；条件渲染 LlmTracePanel |
| `frontend/src/api/client.ts` | 修改 | 新增 `runSparkStage()` + 相关类型 |
| `tests/api/test_spark_stage_independent.py` | 新建 | 9 个 pytest 用例 |
