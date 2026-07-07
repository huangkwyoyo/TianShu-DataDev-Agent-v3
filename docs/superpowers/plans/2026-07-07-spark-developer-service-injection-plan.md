# SparkDeveloperService 注入 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 `SparkDeveloperService`（LLM 语义标注）接入 Pipeline 单阶段 `/spark/develop` 路径，使前端点击「标注」按钮后调用 DeepSeek API 产出 `AnnotatedSparkPlan` 并展示在前端面板。

**Architecture:** Pipeline 接收 `developer_service` 实例，`_do_spark_develop()` 从硬编码 SKIPPED 改为真实调用 `service.annotate(plan)` 并缓存结果到 `context.annotation_result`；`app.py` 在启动时通过 preflight 检查 API key 后创建 service 并注入；前端 `SparkStageResult` 扩展 `annotations` 字段，面板渲染标注结果表格。

**Tech Stack:** FastAPI + Python 3.12 + AnthropicAdapter (httpx) + PromptManager + React 18 + TypeScript

## Global Constraints

- 不修改 `developer.py` / `annotations.py` / `SparkOrchestrator` / `routes.py` / Prompt 模板
- API key 仅从环境变量读取，不硬编码
- `load_dotenv()` 必须在任何 LLM 组件创建前调用
- FAILURE 不阻断后续阶段（MAPPER→COMPILER→VALIDATOR→COMPARATOR→PHYSICAL_VERIFIER）
- 前端接口仅扩展 `SparkStageResult`，不修改已有字段
- 不引入新的 pip / npm 依赖

---

### Task 1: `app.py` — 加载 .env + 创建并注入 SparkDeveloperService

**Files:**
- Modify: `src/tianshu_datadev/api/app.py`
- Test: manual (startup log observation)

**Interfaces:**
- Consumes: `SparkDeveloperService`, `AnthropicAdapter`, `PromptManager`, `load_dotenv`
- Produces: `app.state.spark_developer_service` (可被外部检查)
- Boundary: `Pipeline.__init__` 新增 `developer_service` 形参

- [ ] **Step 1: 修改 `create_app()` 函数—在开头加入 load_dotenv、API key preflight 检查和 SparkDeveloperService 创建**

在 `create_app()` 函数开头，现有代码中 `app = FastAPI(...)` 之前插入（约第 88 行）：

```python
import logging

from tianshu_datadev.config import load_dotenv
from tianshu_datadev.spark.developer import SparkDeveloperService
from tianshu_datadev.prompts.manager import PromptManager
from tianshu_datadev.llm.adapters.anthropic_adapter import AnthropicAdapter

logger = logging.getLogger(__name__)
```

然后在 `create_app()` 函数体第一行：

```python
def create_app(pipeline=None):
    # ── Phase 8: 加载 .env 环境变量 ──
    load_dotenv()

    # ── Phase 8: 创建 SparkDeveloperService（API Key preflight）──
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
```

- [ ] **Step 2: 修改 Pipeline 实例化—传入 developer_service**

找到 `create_app()` 中现有 Pipeline 创建逻辑（约第 105-114 行），在所有 `Pipeline(...)` 调用中增加 `developer_service=spark_developer_service`：

```python
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
```

- [ ] **Step 3: 在 app.state 中存储 spark_developer_service**

在 `app.state.pipeline = pipeline` 之后增加：

```python
    app.state.pipeline = pipeline
    app.state.spark_developer_service = spark_developer_service
```

- [ ] **Step 4: 验证启动日志**

```bash
# 确保 .env 存在且有 key
cat .env
# 预期包含 DEEPSEEK_API_KEY=sk-xxxx

# 启动后端
cd D:/Program\ Files/gitvscode/TianShu-DataDev-Agent-v3
uv run uvicorn tianshu_datadev.api.app:create_app --reload --host 0.0.0.0 --port 8000
```

预期日志输出：`SparkDeveloperService 初始化成功——DEVELOPER 阶段将调用 DeepSeek API`

- [ ] **Step 5: 验证无 key 时降级**

```bash
# 临时移除 key 验证降级行为
DEEPSEEK_API_KEY="" uvicorn tianshu_datadev.api.app:create_app --host 0.0.0.0 --port 8001
```

预期日志输出：`未检测到 DEEPSEEK_API_KEY 或 ANTHROPIC_API_KEY——SparkDeveloperService 跳过`

- [ ] **Step 6: Commit**

```bash
git add src/tianshu_datadev/api/app.py
git commit -m "feat(app): load_dotenv + API key preflight + SparkDeveloperService 创建与注入

- create_app 开头调用 load_dotenv() 加载 .env
- preflight 检查 DEEPSEEK_API_KEY，无 key 时 service=None（稳定 SKIPPED）
- 创建 SparkDeveloperService 并注入到 Pipeline 和 app.state
- 异常捕获，失败时不阻塞应用启动

Phase 8 — LLM 语义标注注入
Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 2: `pipeline.py` — Pipeline 接收 + 执行 SparkDeveloperService

**Files:**
- Modify: `src/tianshu_datadev/api/pipeline.py`
- Test: `tests/spark/test_orchestrator.py` (验证 `_do_spark_develop` 三态)

**Interfaces:**
- Consumes: `developer_service` from `Pipeline.__init__`
- Produces: `SparkStageContext.annotation_result` (AnnotatedSparkPlan | None)

- [ ] **Step 1: 修改 `Pipeline.__init__` — 增加 `developer_service` 参数**

在现有 `__init__` 签名末尾增加：

```python
    def __init__(
        self,
        base_output_dir: str = "generated/review_packages",
        adapter: ProviderAdapter | None = None,
        snapshot_builder: SnapshotBuilder | None = None,
        snapshot_provider: SnapshotSourceProvider | None = None,
        default_table_paths: dict[str, str] | None = None,
        duckdb_path: str | None = None,
        # ── Phase 8: SparkDeveloperService 注入（可选）──
        developer_service=None,  # SparkDeveloperService | None，None → SKIPPED
    ):
```

在 `__init__` 方法体中存储：

```python
        self._spark_developer_service = developer_service
```

- [ ] **Step 2: 修改 `SparkStageContext` — 增加 `annotation_result` 字段**

找到 `@dataclass class SparkStageContext`（约第 2695 行），在 `comparator_report` 后增加：

```python
    comparator_report: "PlanComparisonReport | None" = None
    # ── Phase 8: DEVELOPER 阶段产物缓存 ──
    annotation_result: "AnnotatedSparkPlan | None" = None
```

- [ ] **Step 3: 重写 `_do_spark_develop()` — 从 SKIPPED 改为真实调用**

找到现有 `_do_spark_develop()` 方法（约第 2505 行），替换为：

```python
    def _do_spark_develop(self, context: SparkStageContext) -> None:
        """执行 DEVELOPER 阶段——LLM 语义标注。

        Phase 8: 注入 SparkDeveloperService 后调用真实 LLM 标注，
        异常时标记 FAILURE，不阻断后续阶段。
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

- [ ] **Step 4: 修改 DEVELOPER 结果构建 — 带标注数据**

找到 `run_spark_stage()` 中 DEVELOPER 结果构建部分（约第 2442 行），替换为：

```python
        # ── Phase 8: DEVELOPER 结果构建（含标注数据）──
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

- [ ] **Step 5: 编写单元测试 — 验证 `_do_spark_develop` 三态**

在 `tests/spark/test_orchestrator.py` 中增加（或修改现有 `test_run_without_llm_developer_skips_annotations`）：

```python
from unittest.mock import MagicMock
from tianshu_datadev.spark.developer import SparkDeveloperService

def test_do_spark_develop_with_service_success(self):
    """注入 mock service 时 DEVLOPER 阶段返回 SUCCESS 且 annotation_result 非空。"""
    from tianshu_datadev.api.pipeline import Pipeline, SparkStageContext
    from tianshu_datadev.spark.annotations import AnnotatedSparkPlan, StepAnnotation, StepIntent

    # 准备
    mock_ann = AnnotatedSparkPlan(
        plan_id="test",
        baseline_plan_hash="abc",
        annotations=[
            StepAnnotation(step_id="SparkReadStep_0", step_index=0, step_type="read",
                           intent=StepIntent.SOURCE, intent_detail="读取数据"),
        ],
    )
    mock_service = MagicMock(spec=SparkDeveloperService)
    mock_service.annotate.return_value = mock_ann

    pipeline = Pipeline(developer_service=mock_service)
    ctx = SparkStageContext()
    ctx.spark_plan = MagicMock()

    # 执行
    pipeline._do_spark_develop(ctx)

    # 验证
    assert ctx.stage_results["DEVELOPER"] == "SUCCESS"
    assert ctx.annotation_result is not None
    assert ctx.annotation_result.annotations[0].step_id == "SparkReadStep_0"
    mock_service.annotate.assert_called_once_with(ctx.spark_plan)

def test_do_spark_develop_service_failure(self):
    """service 抛异常时 DVELOPER 标记 FAILURE。"""
    from tianshu_datadev.api.pipeline import Pipeline, SparkStageContext

    mock_service = MagicMock(spec=SparkDeveloperService)
    mock_service.annotate.side_effect = ValueError("API 调用失败")

    pipeline = Pipeline(developer_service=mock_service)
    ctx = SparkStageContext()
    ctx.spark_plan = MagicMock()

    pipeline._do_spark_develop(ctx)

    assert ctx.stage_results["DEVELOPER"] == "FAILURE"
    assert any("[DEVELOPER] 标注异常" in e for e in ctx.errors)

def test_do_spark_develop_no_service_skips(self):
    """service=None 时 DEVLOPER 标记 SKIPPED。"""
    from tianshu_datadev.api.pipeline import Pipeline, SparkStageContext

    pipeline = Pipeline()  # developer_service=None
    ctx = SparkStageContext()

    pipeline._do_spark_develop(ctx)

    assert ctx.stage_results["DEVELOPER"] == "SKIPPED"
    assert any("未注入" in e for e in ctx.errors)
```

- [ ] **Step 6: 运行测试**

```bash
cd D:/Program\ Files/gitvscode/TianShu-DataDev-Agent-v3
uv run pytest tests/spark/test_orchestrator.py -v -k develop
```

预期输出：至少 3 个测试 PASS（原有 skipped + 新增 success + failure）

- [ ] **Step 7: Commit**

```bash
git add src/tianshu_datadev/api/pipeline.py tests/spark/test_orchestrator.py
git commit -m "feat(pipeline): Pipeline 接收 SparkDeveloperService，_do_spark_develop 改为真实调用

- Pipeline.__init__ 新增 developer_service 形参
- SparkStageContext 新增 annotation_result 字段
- _do_spark_develop 从硬编码 SKIPPED 改为调用 service.annotate()
- DEVELOPER 结果构建含 annotations/warnings 数组
- 新增 3 个单元测试验证 SKIPPED/SUCCESS/FAILURE 三态

Phase 8 — LLM 语义标注注入
Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 3: 前端 `client.ts` — SparkStageResult 类型扩展

**Files:**
- Modify: `frontend/src/api/client.ts`
- Test: `cd frontend && npx tsc --noEmit`

**Interfaces:**
- Produces: `SparkStageResult` 扩展字段（`annotations`, `annotation_count`, `warnings`）

- [ ] **Step 1: 在 `SparkStageResult` 接口增加新字段**

找到 `export interface SparkStageResult`（约第 410 行），在 `skipped?: boolean` 后增加：

```typescript
  // ── Phase 8: DEVELOPER 阶段标注输出 ──
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
```

- [ ] **Step 2: TypeScript 编译检查**

```bash
cd D:/Program\ Files/gitvscode/TianShu-DataDev-Agent-v3/frontend
npx tsc --noEmit
```

预期输出：无类型错误

- [ ] **Step 3: Commit**

```bash
git add frontend/src/api/client.ts
git commit -m "feat(types): SparkStageResult 增加 annotations/annotation_count/warnings 字段

Phase 8 — LLM 语义标注注入
Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 4: 前端 `SparkStageResultPanel.tsx/css` — DEVELOPER 标注表格渲染

**Files:**
- Modify: `frontend/src/components/SparkStageResultPanel.tsx`
- Modify: `frontend/src/components/SparkStageResultPanel.css`
- Test: Manual (点击「标注」按钮后看渲染效果)

**Interfaces:**
- Consumes: `SparkStageResult.annotations`, `SparkStageResult.annotation_count`, `SparkStageResult.warnings`

- [ ] **Step 1: 修改 `SparkStageResultPanel.tsx` — 增加 DEVELOPER succeess/failed 渲染**

在现有 DEVELOPER 分支（`{result.type === 'developer' && !result.skipped ...}`），替换为：

```tsx
      {/* DEVELOPER——标注结果表格（Phase 8） */}
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

      {/* DEVELOPER——失败态展示（Phase 8） */}
      {result.type === 'developer' && status === 'failed' && (
        <div className="spark-result-errors">
          <p className="error-title">❌ LLM 语义标注失败</p>
          {result.message && <p className="spark-result-note">{result.message}</p>}
        </div>
      )}
```

- [ ] **Step 2: 修改 `SparkStageResultPanel.css` — 增加 intent-badge 样式**

在文件末尾追加：

```css
/* ── Phase 8: LLM 标注意图分类标签 ── */
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

- [ ] **Step 3: TypeScript 编译检查 + Vite dev server 验证**

```bash
cd D:/Program\ Files/gitvscode/TianShu-DataDev-Agent-v3/frontend
npx tsc --noEmit
# 预期：无类型错误

# 启动前端
npm run dev
```

- [ ] **Step 4: 手动 E2E 验证**

操作步骤：
1. 加载任意模板（如 `tpl_aggregation`）
2. Run All → 生成 `request_id`
3. 点击「映射」→ 等待状态变 ok
4. 点击「标注」→ 验证：
   - 不显示 SKIPPED
   - 面板展示标注表格（步骤 ID / 意图分类 / 业务意图 / 操作描述）
   - 行数 = MAPPER 阶段展示的步骤数
5. 修改 `.env` 中的 key 为无效值 → 重启 → 重复步骤 1-3 → 验证 FAILURE 态

- [ ] **Step 5: Commit**

```bash
git add frontend/src/components/SparkStageResultPanel.tsx frontend/src/components/SparkStageResultPanel.css
git commit -m "feat(ui): SparkStageResultPanel 增加 DEVELOPER 标注结果表格渲染

- SUCCESS 态展示步骤标注表格（step_id / intent / 业务意图 / 操作描述）
- FAILURE 态展示错误提示
- 新增 .intent-badge 样式

Phase 8 — LLM 语义标注注入
Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## 验收清单

实现完成后逐项验证：

| # | 验收项 | 验证方式 |
|---|--------|----------|
| 1 | 有 key 时启动日志输出 `SparkDeveloperService 初始化成功` | 日志观察 |
| 2 | 无 key 时启动日志输出 `未检测到...跳过` | 日志观察 |
| 3 | `app.state.spark_developer_service` 与 key 存在性一致 | 代码审查 |
| 4 | `Pipeline(developer_service=None)` 时 DEVELOPER 正常 SKIPPED | pytest |
| 5 | `_do_spark_develop` SUCCESS 态返回 annotations 数组 | pytest + API |
| 6 | `_do_spark_develop` FAILURE 态异常捕获 | pytest |
| 7 | 前端 TS 编译无错误 | `npx tsc --noEmit` |
| 8 | 有真实 key 时标注面板展示步骤表格 | E2E 操作 |
| 9 | 错 key 时标注面板展示 FAILURE | 手动改错 key |
| 10 | 全量测试通过 | `pytest tests/spark/ -v` |
| 11 | `/spark/verify` 批量路径 DEVELOPER 仍为 SKIPPED | 验证响应 |
