---
title: SparkDeveloperService 注入设计文档
date: 2026-07-07
status: draft
phase: Phase 8 — Spark 管线 DEVELOPER 阶段 LLM 语义标注注入
reviewers: [huangkwyoyo]
---

# SparkDeveloperService 注入设计文档

## 1. 本轮执行的阶段

**Phase 8 / Sprint**: 将 `SparkDeveloperService`（LLM 语义标注）接入真实请求链路，使前端点击「标注」按钮后不再显示 SKIPPED，而是调用 DeepSeek API 产出 `AnnotatedSparkPlan` 并展示在前端面板。

**详细范围（In Scope）：**

| 组件 | 操作类型 | 说明 |
|------|----------|------|
| `app.py` | 修改 | 加入 `load_dotenv()`、创建 `AnthropicAdapter` + `PromptManager` + `SparkDeveloperService`，注入到 `Pipeline` 和 `app.state` |
| `pipeline.py` | 修改 | `__init__` 增加 `developer_service` 参数；`SparkStageContext` 增加 `annotation_result` 字段；`_do_spark_develop()` 改为真实调用 |
| `routes.py` | 不动 | 本轮仅接入单阶段 `/spark/develop`，`/spark/verify` 暂不注入 developer_service。原因：`SparkPipelineState.derive_overall_status()` 将任意 FAILURE 判为 REPAIR_NEEDED，若 DEVELOPER 调用失败会影响批量验证结论。|
| `orchestrator.py` | 不动 | `SparkOrchestrator` 已有 `developer_service=None` 构造参数，`_run_developer()` 逻辑不变。|
| `client.ts` | 修改 | `SparkStageResult` 增加 `annotations` / `annotation_count` / `warnings` 类型字段 |
| `SparkStageResultPanel.tsx` | 修改 | DEVELOPER 展示区从纯文字改为标注结果表格 |
| `SparkStageResultPanel.css` | 修改 | 标注表格样式 |

**排除范围（Out of Scope）：**

| 组件 | 不动的原因 |
|------|------------|
| `developer.py` (`SparkDeveloperService`) | 核心 LLM 标注逻辑，本轮的注入目标而非修改对象 |
| `annotations.py` (`AnnotatedSparkPlan` / `AnnotationValidator`) | Schema 与校验逻辑，本轮只消费不修改 |
| `orchestrator.py` (`SparkOrchestrator`) | 已有 `developer_service=None` 构造参数，本轮不传参，orchestrator 内部 `_run_developer()` 逻辑不变 |
| `routes.py` | 本轮不修改——批量路径暂不注入 developer_service。单阶段路径通过 Pipeline 内部调用 |
| `prompts/templates/spark_annotator/v001.md` | Prompt 模板内容，不在本轮迭代范围内 |
| 所有 SQL 管线组件（Parser / Builder / Compiler / Executor） | 不相关 |
| 前端整体路由 / 状态管理架构 | 只扩展 `SparkStageResult` 类型和单个面板渲染 |

## 2. 本轮的权限

### 可以做的事（授权列表）

```
[Pipeline]  __init__ 接收 developer_service 参数
         │
         ├── [Pipeline]  _do_spark_develop() 改为真实调用 ← ⭐ 主要改动
         │
         ├── [SparkStageContext] 增加 annotation_result 字段
         │
         ├── [app.py]  create_app() 中创建 SparkDeveloperService 并传入
         │
         ├── [client.ts]  SparkStageResult 加 annotations / warnings 字段
         │
         └── [SparkStageResultPanel.tsx/css] 标注结果表格展示

本轮不修改的文件：
  - routes.py（/spark/verify 暂不注入，避让 derive_overall_status）
  - orchestrator.py（已有构造参数，不传参）
```

具体每项修改的权限说明：

#### 2.1 `app.py`
- **可以**：调用 `load_dotenv()` 加载 `.env`
- **可以**：创建 `AnthropicAdapter`（无参构造，从环境变量自动读 key）
- **可以**：创建 `PromptManager`
- **可以**：创建 `SparkDeveloperService.from_provider_adapter()`
- **可以**：将 service 传入 `Pipeline(developer_service=...)`
- **可以**：将 service 存入 `app.state.spark_developer_service`
- **不可以**：硬编码 API key / base_url / model（全部走环境变量）
- **不可以**：修改 `AnthropicAdapter` 构造逻辑

#### 2.2 `pipeline.py`
- **可以**：在 `__init__` 中增加 `developer_service` 形参（默认 None）
- **可以**：在 `_do_spark_develop()` 中调用 `self._spark_developer_service.annotate()`
- **可以**：异常捕获，FAILURE 不阻断后续阶段
- **不可以**：修改其他阶段（MAPPER / COMPILER / VALIDATOR / COMPARATOR / PHYSICAL_VERIFIER）
- **不可以**：修改 `run_spark_stage()` 的 dispatch 逻辑
- **不可以**：修改 `SparkStageContext` 已有字段的类型或含义

#### 2.3 `routes.py`
- **可以**：无改动。本轮不修改 routes.py
- **不可以**：修改批量路径 /spark/verify 的异常处理逻辑
- **不可以**：修改单阶段路径的 dispatch 逻辑
- **不可以**：向 SparkOrchestrator 传入 developer_service（避让 derive_overall_status 将 FAILURE 判为 REPAIR_NEEDED 的问题）

#### 2.4 前端
- **可以**：`SparkStageResult` 增加新字段（向后兼容）
- **可以**：`SparkStageResultPanel.tsx` 增加 `type === 'developer'` 的渲染分支
- **不可以**：修改其他 stage 类型（mapper / compiler / validator / comparator / physical_verify）的渲染
- **不可以**：修改 `App.tsx` 的面板布局或状态管理逻辑

### 不可做的事（禁止清单）

| 操作 | 理由 |
|------|------|
| 修改 `SparkDeveloperService` 内部逻辑 | 核心注入目标，不是修改对象 |
| 修改 `AnnotatedSparkPlan` / `AnnotationValidator` | Schema 和校验规则不动 |
| 修改 `SparkOrchestrator._run_developer()` | 已有构造参数，本轮不传参，不修改 orchestrator |
| 向 /spark/verify 注入 developer_service | 避让 `derive_overall_status` 将 FAILURE 判为 REPAIR_NEEDED |
| 修改 Prompt 模板 (`v001.md`) | Prompt 工程不在本轮 |
| 修改 SQL 管线任何文件 | 不相关 |
| 修改前端路由 / 状态管理架构 | 越界 |
| 引入新的 pip / npm 依赖 | 复用已有 httpx / pydantic / FastAPI |
| 硬编码 API key / base_url / model 到代码中 | 安全红线 |

## 3. 哪些边界不能触碰

### 架构边界图

```
                    ┌──────────────────────────────┐
                    │      app.py (create_app)      │
                    │  ┌──────────────────────────┐ │
                    │  │  SparkDeveloperService   │ │  ← 创建实例，不碰内部
                    │  │  (只调 from_provider_    │ │
                    │  │   adapter(), 不读源码)    │ │
                    │  └──────────┬───────────────┘ │
                    └─────────────┼─────────────────┘
                                  │ 注入
                    ┌─────────────▼─────────────────┐
                    │       Pipeline.__init__        │
                    │  self._spark_developer_service │
                    └─────────────┬─────────────────┘
                                  │ 调用
                    ┌─────────────▼─────────────────┐
                    │    _do_spark_develop()         │
                    │   context.annotation_result =  │
                    │   service.annotate(plan)       │
                    └─────────────┬─────────────────┘
                                  │ 产出
                    ══════════════╪═══════════════════════ 不可跨越
                                  │
                    ┌─────────────▼─────────────────┐
                    │      AnnotatedSparkPlan        │  ← 只读不写
                    │      AnnotationValidator       │
                    │      SparkPlan                 │
                    │      SparkCompiler             │
                    │      SparkOrchestrator         │
                    └────────────────────────────────┘
```

### 具体边界列表

**边界 1：`SparkDeveloperService` 内部实现不动**
- `annotate()` 方法不做修改
- `from_provider_adapter()` 工厂方法不做修改
- `_build_prompt()` 不做修改
- 引用 `SparkPlan` 的契约不变

**边界 2：`AnnotatedSparkPlan` Schema 不动**
- `StepAnnotation` 字段不做增减
- `AnnotationValidator` 校验规则不做修改
- `compute_annotation_hash()` 不做修改

**边界 3：`SparkOrchestrator` 不动**
- `_run_developer()` 的 skipped/FAILURE 逻辑不动（本轮不向 orchestrator 传 developer_service，所以 orchestrator 内 DEVELOPER 永远走 skipped 分支）
- `SparkPipelineState` 不动
- `derive_overall_status()` 不动（任意 FAILURE → REPAIR_NEEDED 的逻辑维持不变）
- `run()` 方法的签名和逻辑不动

**边界 4：Prompt 模板不动**
- `prompts/templates/spark_annotator/v001.md` 不做任何修改
- Prompt 版本号不升级

**边界 5：前端架构不动**
- `App.tsx` 的状态管理、面板布局、按钮逻辑不动
- SparkStageButtons 组件不动
- 后端 API 响应格式：已有字段不做 rename/deprecate，只做扩展

**违反后果**：如果任何修改触碰到上述边界，视为 **C 类架构风险**，需走 CRCS C 类流程（风险评估 → Owner 批准 → 安全回归），**不允许自动修复**。

## 4. 数据流与错误处理

### 正常路径

```
用户点击「标注」
  → /spark/develop POST {request_id}
    → Pipeline.run_spark_stage(DEVELOPER)
      → _check_stage_dependencies()     ← Mapper 没跑过 → 422
      → _do_spark_develop()
          ├─ service is None? ............ → SKIPPED
          ├─ spark_plan is None? ......... → SKIPPED
          ├─ developer_service.annotate()
          │    ├─ PromptManager 加载模板
          │    ├─ AnthropicAdapter.invoke()
          │    │    ├─ httpx POST → DeepSeek API
          │    │    └─ ← AnnotatedSparkPlan JSON
          │    ├─ AnnotationValidator.validate()
          │    └─ ← AnnotatedSparkPlan
          ├─ SUCCESS → context.annotation_result = annotated
          └─ 返回 result { type, message, annotations[], warnings }
```

### 异常路径

| 异常场景 | 状态 | 影响 |
|----------|------|------|
| API key 未配置 | SKIPPED | 不阻断后续阶段 |
| SparkPlan 为 None | SKIPPED | 不阻断（MAPPER 失败导致） |
| DeepSeek API 返回 401/403 | FAILURE | 不阻断，日志记录错误 |
| DeepSeek API 超时（AnthropicAdapter 默认 120s） | FAILURE（重试 1 次后） | 不阻断 |
| LLM 产出 json 解析失败 | FAILURE | 不阻断 |
| AnnotationValidator 校验不通过 | FAILURE | 不阻断 |
| httpx 网络错误 | FAILURE（重试 1 次后） | 不阻断 |

### 重试策略

```
AnthropicAdapter.invoke() 失败
  ↓
属于 AdapterError？──是──→ attempt < max_retries (1)？──是──→ 重试
     否                          否
     ↓                           ↓
  抛出异常（不重试）        抛出异常（标记 FAILURE）
```

## 5. 具体改动（含每处验收与交接）

### 5.1 `app.py` — 加载环境变量 + 创建 SparkDeveloperService

**改动位置**：`src/tianshu_datadev/api/app.py`，`create_app()` 函数开头

**具体代码**：

```python
# 新增导入
from tianshu_datadev.config import load_dotenv
from tianshu_datadev.spark.developer import SparkDeveloperService
from tianshu_datadev.prompts.manager import PromptManager
from tianshu_datadev.llm.adapters.anthropic_adapter import AnthropicAdapter
import logging

logger = logging.getLogger(__name__)

def create_app(pipeline=None):
    # ── Phase 8: 加载 .env 环境变量（优先于任何 LLM 组件初始化）──
    load_dotenv()

    # ── Phase 8: 创建 SparkDeveloperService ──
    # 生命周期：应用启动时创建一次，复用直到进程退出
    # 权限：仅创建实例并传入 Pipeline，不修改 developer.py 内部
    # 边界：不通读/修改 AnthropicAdapter 源码，不硬编码 API key
    # 失败降级：创建失败时 DEVELOPER 自动 SKIPPED，不阻塞应用启动
    # ── 预检 API key ──
    # AnthropicAdapter 构造函数不会校验 key 空值（仅在 invoke() 时抛错），
    # 因此需显式检查：无 key 时 service 置 None → DEVELOPER 稳定 SKIPPED（而非运行时 FAILURE）
    spark_developer_service = None
    api_key = os.environ.get("DEEPSEEK_API_KEY") or os.environ.get("ANTHROPIC_API_KEY")
    if api_key:
        try:
            adapter = AnthropicAdapter()
            prompt_manager = PromptManager()
            spark_developer_service = SparkDeveloperService.from_provider_adapter(
                adapter, prompt_manager, max_llm_retries=1
            )
            logger.info("SparkDeveloperService 初始化成功——DEVELOPER 阶段将调用 DeepSeek API")
        except Exception as exc:
            logger.warning(
                "SparkDeveloperService 创建失败（key 存在但初始化异常），"
                "DEVELOPER 阶段将标记 SKIPPED: %s", exc
            )
    else:
        logger.info(
            "未检测到 DEEPSEEK_API_KEY 或 ANTHROPIC_API_KEY——"
            "SparkDeveloperService 跳过，DEVELOPER 阶段将标记 SKIPPED"
        )

    # 注入到 Pipeline
    if pipeline is None:
        db_path = _discover_nyc_duckdb()
        if os.environ.get("TIANSHU_E2E_MODE") == "true":
            pipeline = Pipeline(
                default_table_paths=_discover_csv_fixtures(),
                duckdb_path=db_path,
                developer_service=spark_developer_service,
            )
        else:
            pipeline = Pipeline(
                duckdb_path=db_path,
                developer_service=spark_developer_service,
            )

    app.state.pipeline = pipeline
    # 保留在 app.state 供未来/调试使用。本轮 routes.py 不消费此值（/spark/verify 暂不注入）。
    app.state.spark_developer_service = spark_developer_service
```

**验收**：
1. ✅ 有 `.env` 且配置了 `DEEPSEEK_API_KEY` → 日志输出 `SparkDeveloperService 初始化成功`，`app.state.spark_developer_service` 非 None
2. ✅ 无 `.env` 或 key 为空 → 日志输出 `未检测到...跳过`，`app.state.spark_developer_service` 为 None（DEVELOPER 稳定 SKIPPED）
3. ✅ 有 key 但初始化异常（如 PromptManager 加载失败）→ 日志 output `创建失败...标记 SKIPPED`，service 为 None
4. ✅ 环境变量从 `.env` 读取，不硬编码在代码中

**阻塞**：如 key 存在但启动日志输出 `创建失败`，检查 `.env` 格式或 API key 有效性

**完成**：启动后日志符合预期即可，转入下一改动验收

---

### 5.2 `pipeline.py` — Pipeline 接收 developer_service

**改动位置**：`src/tianshu_datadev/api/pipeline.py`
- `Pipeline.__init__()` — 增加参数
- `SparkStageContext` — 增加 `annotation_result` 字段
- `_do_spark_develop()` — 改为真实调用
- `run_spark_stage()` — DEVELOPER 结果构建增加标注数据

#### 5.2.1 `__init__` 增加参数

```python
class Pipeline:
    def __init__(
        self,
        base_output_dir: str = "generated/review_packages",
        adapter: ProviderAdapter | None = None,
        snapshot_builder: SnapshotBuilder | None = None,
        snapshot_provider: SnapshotSourceProvider | None = None,
        default_table_paths: dict[str, str] | None = None,
        duckdb_path: str | None = None,
        # ── Phase 8: SparkDeveloperService 注入（可选）──
        # 权限：仅存储引用，不修改 developer.py
        # 边界：不接触 SparkOrchestrator、SparkCompiler 等下游组件
        # 失败降级：None 时 _do_spark_develop 自动 SKIPPED，不阻断管线
        developer_service=None,  # SparkDeveloperService | None
    ):
        ...
        self._spark_developer_service = developer_service
```

**验收**：
1. ✅ 不传 `developer_service` 时行为不变（默认 None → SKIPPED）
2. ✅ 传入后 Pipeline 内部可访问 `self._spark_developer_service`

**阻塞**：如类型不匹配导致运行时异常，回退为默认 None 后重试

**完成**：`Pipeline(developer_service=mock_service)` 可正常初始化

#### 5.2.2 `SparkStageContext` 增加字段

```python
@dataclass
class SparkStageContext:
    spark_plan: "SparkPlan | None" = None
    compile_result: "SparkCompileResult | None" = None
    standalone_pyspark: str | None = None
    comparator_report: "PlanComparisonReport | None" = None
    # ── Phase 8: DEVELOPER 阶段产物缓存 ──
    # 权限：仅存储 AnnotatedSparkPlan，不修改其 Schema
    # 边界：不接触 AnnotationValidator 或 annotations.py 其他逻辑
    # 验收：DEVELOPER 成功后此字段非 None，且内容为合法 AnnotatedSparkPlan
    annotation_result: "AnnotatedSparkPlan | None" = None
    stage_results: dict[str, str] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
```

**验收**：
1. ✅ 所有现有测试不因新增字段而失败
2. ✅ `SparkStageContext().annotation_result` 默认值为 `None`

**阻塞**：无（新增可选字段，零影响）

**完成**：pytest 全部通过

#### 5.2.3 `_do_spark_develop()` 改为真实调用

```python
def _do_spark_develop(self, context: SparkStageContext) -> None:
    """执行 DEVELOPER 阶段——LLM 语义标注。

    Phase 8 注入流程：
    1. 检查 self._spark_developer_service 是否可用
    2. 检查 context.spark_plan 是否存在（MAPPER 前置）
    3. 调用 annotate() 产出 AnnotatedSparkPlan
    4. 缓存到 context.annotation_result
    5. 标记 SUCCESS / FAILURE / SKIPPED

    权限：仅调用 service.annotate()，不修改 service 内部实现
    边界：异常时标记 FAILURE，不阻断后续阶段（安全降级）
    验收：成功时 context.stage_results["DEVELOPER"] == "SUCCESS"
    阻塞：任何异常降级为 FAILURE，日志记录原因
    完成：DEVELOPER 阶段状态正确反映 LLM 调用结果
    """
    if self._spark_developer_service is None:
        context.stage_results["DEVELOPER"] = "SKIPPED"
        context.errors.append("[DEVELOPER] SKIPPED: 未注入 SparkDeveloperService")
        return

    if context.spark_plan is None:
        context.stage_results["DEVELOPER"] = "SKIPPED"
        context.errors.append("[DEVELOPER] SKIPPED: 无 SparkPlan（MAPPER 未执行或失败）")
        return

    try:
        annotated = self._spark_developer_service.annotate(context.spark_plan)
        context.annotation_result = annotated
        context.stage_results["DEVELOPER"] = "SUCCESS"
    except Exception as e:
        context.stage_results["DEVELOPER"] = "FAILURE"
        context.errors.append(f"[DEVELOPER] 标注异常：{e}")
```

**验收**：
1. ✅ `service=None` → SKIPPED
2. ✅ `spark_plan=None` → SKIPPED
3. ✅ `annotate()` 成功 → SUCCESS + `annotation_result` 非空
4. ✅ `annotate()` 抛异常 → FAILURE + 日志记录异常
5. ✅ FAILURE 状态下 `context.stage_results["DEVELOPER"]` 为 `"FAILURE"`
6. ✅ 现有测试 `test_run_without_llm_developer_skips_annotations` 仍通过
7. ✅ 不存在 `spark_developer_service` 导入循环或类型错误

**阻塞**：如果 `from_provider_adapter()` 创建的 service 无法正常初始化，检查 `developer.py` 中 `llm_call=None → ValueError` 的防御逻辑。这是在 `app.py` 创建阶段捕获的，不会影响运行时。

**完成**：单测通过，手动测试验证三态（SKIPPED / SUCCESS / FAILURE）

#### 5.2.4 `run_spark_stage()` DEVELOPER 结果构建

```python
# ── Phase 8: DEVELOPER 结果构建 ──
# 权限：仅从 context.annotation_result 读取标注数据，不修改其结构
# 边界：不接触 AnnotatedSparkPlan Schema（annotations.py 不动）
# 验收：ok 态返回 annotations 数组，skipped/failed 态返回 message
if stage == SparkPipelineStage.DEVELOPER:
    if current_status == "ok" and context.annotation_result is not None:
        ann = context.annotation_result
        result = {
            "type": "developer",
            "message": f"LLM 语义标注完成——{len(ann.annotations)} 个步骤",
            "annotation_count": len(ann.annotations),
            "annotations": [
                {
                    "step_id": a.step_id,
                    "intent": a.intent.value if hasattr(a.intent, "value") else str(a.intent),
                    "intent_detail": a.intent_detail,
                    "operation_summary": a.operation_summary,
                }
                for a in ann.annotations
            ],
            "warnings": [
                {
                    "warning_id": w.warning_id,
                    "severity": w.severity,
                    "description": w.description,
                }
                for w in ann.warnings
            ],
        }
    else:
        result = {
            "type": "developer",
            "message": (
                "LLM 语义标注失败"
                if current_status == "failed"
                else "LLM 语义标注阶段——未注入 SparkDeveloperService，已标记 SKIPPED"
            ),
            "skipped": current_status == "skipped",
        }
```

**验收**：
1. ✅ `current_status == "ok"` 时 `result` 含 `annotations` 数组
2. ✅ `current_status == "skipped"` 时 `result` 含 `skipped: True`
3. ✅ `current_status == "failed"` 时 `result.message` 包含失败提示
4. ✅ 已有字段无 break change（`type`, `message` 等仍存在）

**阻塞**：如果 `ann.intent` 不是 `StepIntent` 枚举（可能是普通 str），`hasattr(a.intent, "value")` 防御已处理

**完成**：API 返回的 JSON 中 `result` 字段符合预期格式

---

### 5.3 `routes.py` — 本轮不修改

**决策背景**：
`/spark/verify` 使用 `SparkOrchestrator.run()` 全链路执行，`derive_overall_status()` 将任意 FAILURE 判为 `REPAIR_NEEDED`（`orchestrator.py:124`）。若 DEVELOPER 调用 DeepSeek API 失败，会影响批量验证的全局结论。

因此本轮**只通过单阶段 `/spark/develop` API 注入 developer_service**，`/spark/verify` 继续保持原有行为（DEVELOPER 标记 SKIPPED）。待后续设计明确编排策略后再开放批量路径。

---

### 5.4 前端 `client.ts` — SparkStageResult 类型扩展

**改动位置**：`frontend/src/api/client.ts`

```typescript
// ── Phase 8: SparkDeveloperService 标注结果 ──
// 权限：仅增加新字段，不改动已有字段类型或含义
// 边界：不接触前端路由/状态管理
// 验收：TS 编译无错误，WebSocket/API 响应中 annotatinos 字段正常解析
export interface SparkStageResult {
  // ... 现有字段不变 ...

  // DEVELOPER 阶段标注输出（Phase 8）
  annotations?: {
    step_id: string;
    intent: string;
    intent_detail: string;
    operation_summary: string;
  }[];
  annotation_count?: number;
  warnings?: {
    warning_id: string;
    severity: string;
    description: string;
  }[];
}
```

**验收**：
1. ✅ `cd frontend && npx tsc --noEmit` 无错误
2. ✅ `SparkStageResult` 已有字段类型不变

**阻塞**：如 TS 编译报错，检查接口定义拼写

**完成**：TypeScript 编译通过

---

### 5.5 前端 `SparkStageResultPanel.tsx` — DEVELOPER 展示

**改动位置**：`frontend/src/components/SparkStageResultPanel.tsx`

```tsx
// ── Phase 8: DEVELOPER 标注结果表格展示 ──
// 权限：仅修改 DEVELOPER 分支，不改动其他 stage 渲染
// 边界：不修改 MAPPER/COMPILER/VALIDATOR/COMPARATOR/PHYSICAL_VERIFIER 的渲染
// 验收：标注按钮点击后展示步骤表格
// 阻塞：result.annotations 为空时显示占位消息
// 完成：表格正确展示每步标注的 intent/意图/操作描述

// 修改 DEVELOPER 展示区（替换当前纯文字渲染）：
{result.type === 'developer' && !result.skipped && status === 'ok' && (
  <>
    <div className="section-title">🏷️ LLM 语义标注结果</div>
    <div className="spark-plan-summary">
      <span className="stat-label">标注步骤数</span>
      <span className="stat-value">{result.annotation_count}</span>
    </div>
    {result.annotations && result.annotations.length > 0 ? (
      <table className="spark-step-table">
        <thead>
          <tr>
            <th>步骤 ID</th>
            <th>意图分类</th>
            <th>业务意图</th>
            <th>操作描述</th>
          </tr>
        </thead>
        <tbody>
          {result.annotations.map((a, i) => (
            <tr key={i}>
              <td><code className="step-type-badge">{a.step_id}</code></td>
              <td><span className="intent-badge">{a.intent}</span></td>
              <td className="step-desc">{a.intent_detail}</td>
              <td className="step-desc">{a.operation_summary}</td>
            </tr>
          ))}
        </tbody>
      </table>
    ) : (
      <div className="spark-result-message">标注结果为空</div>
    )}
    {result.warnings && result.warnings.length > 0 && (
      <div className="spark-result-errors">
        <p className="error-title">⚠️ 标注警告</p>
        <ul className="error-list">
          {result.warnings.map((w, i) => (
            <li key={i}>[{w.severity}] {w.description}</li>
          ))}
        </ul>
      </div>
    )}
  </>
)}

// 失败态展示
{result.type === 'developer' && status === 'failed' && (
  <div className="spark-result-errors">
    <p className="error-title">❌ LLM 语义标注失败</p>
    {result.message && <p className="spark-result-note">{result.message}</p>}
  </div>
)}
```

**验收**：
1. ✅ `result.type === 'developer' && status === 'ok'` 时展示标注结果表格
2. ✅ 表格列：步骤 ID / 意图分类 / 业务意图 / 操作描述
3. ✅ `result.annotations` 为空时显示「标注结果为空」
4. ✅ 失败态展示「LLM 语义标注失败」
5. ✅ 其他 stage 渲染完全不变
6. ✅ 前端无运行时错误

**阻塞**：如 `result.annotations` 未定义导致渲染异常，`result.annotations &&` 短路已防御

**完成**：手动测试 DEVELOPER / SKIPPED / FAILED 三态渲染正常

---

### 5.6 前端 `SparkStageResultPanel.css` — 新增样式

```css
/* ── Phase 8: 标注结果样式 ── */
/* 权限：仅新增，不改动已有样式 */
.intent-badge {
  display: inline-block;
  background: #e8f0fe;
  color: #1a73e8;
  font-size: 10px;
  font-weight: 600;
  padding: 2px 6px;
  border-radius: 3px;
  text-transform: uppercase;
  letter-spacing: 0.3px;
  font-family: 'Cascadia Code', 'Fira Code', monospace;
}
```

**验收**：
1. ✅ CSS 选择器不与其他组件冲突
2. ✅ intent 标签视觉清晰可辨

**阻塞**：无

**完成**：样式渲染正常

## 6. 完整验收清单

| # | 验收项 | 验证方式 | 关联改动 |
|---|--------|----------|----------|
| 1 | 应用启动日志输出 `SparkDeveloperService 初始化成功` | 日志观察 | 5.1 app.py |
| 2 | 启动日志不输出 `创建失败` 类 warning | 日志观察 | 5.1 app.py |
| 3 | DEEPSEEK_API_KEY 从环境变量读取，不硬编码 | 代码审查 | 5.1 app.py |
| 4 | Pipeline 接受 developer_service 后正常运行 | 启动验证 | 5.2.1 pipeline.py |
| 5 | SparkStageContext.annotation_result 默认 None | pytest | 5.2.2 pipeline.py |
| 6 | _do_spark_develop 三态正确（SKIPPED/SUCCESS/FAILURE） | pytest + 手动验证 | 5.2.3 pipeline.py |
| 7 | DEVELOPER 结果含 annotations 数组 | API 验证 | 5.2.4 pipeline.py |
| 8 | SparkStageResult 类型编译无 TS 错误 | `npx tsc --noEmit` | 5.4 client.ts |
| 9 | 前端标注按钮点击后展示标注表格 | 视觉确认 | 5.5 SparkStageResultPanel.tsx |
| 10 | 标注表格行数 = SparkPlan steps 数 | 核对 | 5.5 SparkStageResultPanel.tsx |
| 11 | 有真实 key 时 `/spark/develop` 返回 `status=ok` 且 `result.annotation_count > 0` | API 验证 | 全链路 |
| 11b | 错 key 时 `/spark/develop` 返回 `status=failed` 且 `errors` 包含 `[DEVELOPER] 标注异常` | API 验证 + 手动改错 key | 全链路 |
| 11c | 单元测试用 fake developer_service 验证 `annotate()` 被调用 | `pytest tests/spark/ -v -k develop` | 5.2.3 pipeline.py |
| 12 | API key 错误/断网时展示 FAILURE（不崩溃） | 手动改错 key 或断网 | 全链路 |
| 13 | 现有测试全部通过 | `pytest tests/spark/ -v` | 全链路 |
| 14 | 前端编译无 TS 错误 | `cd frontend && npx tsc --noEmit` | 全链路 |
| 15 | /spark/verify 批量路径的 DEVELOPER 保持 SKIPPED（不受本改动影响） | 验证 `/spark/verify` 响应 | 全链路 |

## 7. 完成/阻塞交接

### 完成态

1. 以上 17 条验收项全部通过
2. 代码提交 commit message：
   ```
   feat(spark): 接入 SparkDeveloperService 真实 LLM 标注——DEVELOPER 阶段不再 SKIPPED

   - app.py: load_dotenv + AnthropicAdapter + SparkDeveloperService 创建与注入
   - pipeline.py: Pipeline.__init__ 接收 developer_service, _do_spark_develop 改为真实调用
   - pipeline.py: SparkStageContext 增加 annotation_result, DEVELOPER 结果构建含标注数据
   - routes.py: 本轮不修改（/spark/verify 暂不注入，避让 derive_overall_status）
   - client.ts: SparkStageResult 增加 annotations/annotation_count/warnings 字段
   - SparkStageResultPanel.tsx/css: DEVELOPER 展示标注结果表格 + intent 标签样式

   Closes: Phase 8 DEVELOPER LLM 标注注入
   Co-Authored-By: Claude <noreply@anthropic.com>
   ```
3. 通知用户：「标注阶段已接入 DeepSeek API，点击「标注」按钮可查看 LLM 语义标注结果」

### 阻塞态

1. 记录阻塞原因到 `docs/superpowers/blocks/YYYY-MM-DD-spark-developer-blocked.md`
2. 格式：
   ```markdown
   ## 阻塞报告
   - 日期：2026-07-07
   - 阻塞项：[具体问题]
   - 影响范围：DEVELOPER 阶段无法调用 LLM，降级为 SKIPPED
   - 已尝试的解决方案：[列出尝试过的修复]
   - 建议方案：[下一步建议]
   ```
3. DEVELOPER 阶段自动降级：
   - **API key 未配置** → app.py preflight 检查 → `spark_developer_service = None` → 自动 **SKIPPED**
   - **API key 无效/过期** → service 创建成功，但 `invoke()` 时 AnthropicAdapter 抛 AdapterError → `_do_spark_develop` 异常捕获 → **FAILURE**
   - **运行时网络错误** → `_do_spark_develop` 异常捕获 → **FAILURE**
   - 上述两种降级均**不影响**核心 MAPPER→COMPILER→VALIDATOR→COMPARATOR→PHYSICAL_VERIFIER 链路
4. 通知用户：「标注阶段阻塞，当前降级为 SKIPPED/FAILURE，核心链路不受影响」
