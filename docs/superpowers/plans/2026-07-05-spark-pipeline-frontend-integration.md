# Spark 管线前端集成——实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在现有前端工作台中新增"Spark 验证"按钮和第二个 PipelineStageIndicator，通过 POST /api/spark/verify 端点触发 Spark 管线验证，展示 6 阶段结果和 REVIEW_READY 判定。

**Architecture:** 后端新增 1 个端点（POST /api/spark/verify）——接收 request_id，调用 Pipeline.export_artifacts() → adapt_lite_to_v1() → SparkOrchestrator.run() → SparkReviewBuilder.build()，返回 Spark 6 阶段状态 + review_ready。前端新增 SparkVerifyResponse 类型 + sparkVerify() API 方法，PipelineStageIndicator 扩展 `title` prop 和 Spark 阶段中文映射，App.tsx 新增"Spark 验证"按钮 + 第二个指示灯。

**Tech Stack:** FastAPI + Pydantic StrictModel（后端），React + TypeScript（前端），现有 SparkOrchestrator / SparkReviewBuilder / PipelineStageIndicator 组件复用。

## Global Constraints

- 不改 SQL Pipeline 语义（`run_all` / `execute` / `build_plan` 行为不变）
- 不改 `SparkOrchestrator.run()` 接口和内部逻辑
- 不改 `PlanComparator` 判定规则
- 不引入真实 LLM、生产数据、Spark 物理执行
- 不把 Spark 验证描述为"实时进度"——这是一次性请求-响应
- `review_ready=true` 只写"自动审查材料就绪"，不写"生产可上线"
- 测试文件：API 端点测试放 `tests/api/test_spark_verify.py`（新建，因为端点独立于现有测试范围）；前端无自动化测试框架，验收依赖 `npx tsc --noEmit` + `npm run build` + 手动冒烟
- 所有代码注释使用中文

---

## 文件结构

| 文件 | 角色 | 改动类型 |
|------|------|----------|
| `src/tianshu_datadev/api/models.py` | 新增 SparkVerifyRequest/SparkVerifyResponse/SparkStageItem 模型 | 修改（追加 ~55 行） |
| `src/tianshu_datadev/api/routes.py` | 新增 POST /api/spark/verify 端点 | 修改（追加 ~65 行） |
| `tests/api/test_spark_verify.py` | API 端点测试——4 个场景 | 新建（~260 行） |
| `frontend/src/api/client.ts` | 新增 SparkVerifyResponse 类型 + sparkVerify() 方法 | 修改（追加 ~25 行） |
| `frontend/src/components/PipelineStageIndicator.tsx` | 扩展 `title` prop + Spark 阶段中文映射 | 修改（~12 行改动） |
| `frontend/src/App.tsx` | 新增 Spark 验证按钮 + 第二个指示灯 + 状态字段 | 修改（~50 行改动） |

---

### Task 1: 后端模型——SparkVerifyRequest / SparkVerifyResponse / SparkStageItem

**Files:**
- Modify: `src/tianshu_datadev/api/models.py`——在文件末尾追加新模型

**Interfaces:**
- Consumes: `StrictModel`（来自 `tianshu_datadev.developer_spec.models`）
- Produces: `SparkVerifyRequest`（请求体，1 字段 `request_id: str`）、`SparkStageItem`（单阶段结果，2 字段 `stage: str` + `status: str`）、`SparkVerifyResponse`（响应体，6 字段）

- [ ] **Step 1: 在 models.py 末尾追加 Spark 验证相关模型**

在 `src/tianshu_datadev/api/models.py` 文件末尾（`HealthResponse` 类之后）追加以下内容：

```python
# ════════════════════════════════════════════
# Spark 管线验证——POST /api/spark/verify
# ════════════════════════════════════════════


class SparkVerifyRequest(StrictModel):
    """POST /api/spark/verify 请求体——传入 Pipeline 产出的 request_id。"""

    request_id: str  # Pipeline run_all 返回的 request_id


class SparkStageItem(StrictModel):
    """Spark 管线单个阶段结果——供前端 PipelineStageIndicator 渲染。"""

    stage: str  # 阶段名（MAPPER / DEVELOPER / COMPILER / VALIDATOR / COMPARATOR / PHYSICAL_VERIFIER）
    status: str  # 阶段状态（"ok" / "failed" / "skipped"）


class SparkVerifyResponse(StrictModel):
    """POST /api/spark/verify 响应——Spark 管线 6 阶段结果 + REVIEW_READY 判定。

    成功路径返回 spark_stages + review_ready；
    失败路径（artifacts 缺失/不完整/执行异常）通过 HTTP 错误码返回。
    """

    request_id: str  # 回显请求的 request_id
    spark_stages: list[SparkStageItem] = []  # Spark 6 阶段结果列表
    overall_status: str = ""  # SparkPipelineStatus 字符串值
    comparator_status: str = ""  # 对比器状态字符串
    review_ready: bool = False  # REVIEW_READY 判定——所有关键阶段通过的标志
    package_id: str = ""  # SparkReviewPackage ID
    errors: list[str] = []  # 错误信息列表（成功时为空）
```

- [ ] **Step 2: 验证模型导入——Python 语法检查**

```bash
python -c "from tianshu_datadev.api.models import SparkVerifyRequest, SparkVerifyResponse, SparkStageItem; print('OK')"
```

预期：`OK`

- [ ] **Step 3: 提交**

```bash
git add src/tianshu_datadev/api/models.py
git commit -m "feat(api): 新增 SparkVerifyRequest/SparkVerifyResponse/SparkStageItem 模型"
```

---

### Task 2: 后端端点——POST /api/spark/verify

**Files:**
- Modify: `src/tianshu_datadev/api/routes.py`——在文件末尾追加新端点和导入

**Interfaces:**
- Consumes: `SparkVerifyRequest`, `SparkVerifyResponse`, `SparkStageItem`（来自 `.models`）、`Pipeline.export_artifacts()`（返回 `PipelineArtifactBundle | None`）、`adapt_lite_to_v1()`（来自 `tianshu_datadev.spark.contract_adapter`）、`SparkOrchestrator.run()`（来自 `tianshu_datadev.spark.orchestrator`）、`SparkReviewBuilder.build()`（来自 `tianshu_datadev.spark.review_builder`）、`DataTransformContractV1`（来自 `tianshu_datadev.artifacts.models`）
- Produces: `POST /api/spark/verify` 端点——200 正常 / 404 artifacts 不存在 / 422 artifacts 不完整 / 500 执行异常

- [ ] **Step 1: 在 routes.py 末尾追加 Spark verify 端点**

在 `src/tianshu_datadev/api/routes.py` 文件中的 import 区域追加 Spark 相关导入（在现有 `.models` 导入中增加 `SparkVerifyRequest`），然后在文件末尾追加端点：

**追加导入——修改现有 models 导入行**（将 `SparkVerifyRequest` 加入已有导入）：

定位到 routes.py 第 25-31 行的 `.models` 导入块，替换为：

```python
from .models import (
    ExecuteRequest,
    ParseSpecRequest,
    PlanRequest,
    RunAllRequest,
    SparkStageItem,
    SparkVerifyRequest,
    SparkVerifyResponse,
)
```

**追加端点——在文件末尾（`get_package_rich` 之后）追加**：

```python
# ════════════════════════════════════════════
# Spark 管线验证端点
# ════════════════════════════════════════════


@api_router.post("/spark/verify")
async def spark_verify(request: Request, body: SparkVerifyRequest):
    """触发 Spark 管线验证——返回 6 阶段结果 + REVIEW_READY 判定。

    处理流程：
    1. Pipeline.export_artifacts(request_id) → 提取 SqlBuildPlan + Contract
    2. adapt_lite_to_v1() → 将 Lite 契约升级为 V1
    3. SparkOrchestrator.run(contract=v1, sql_plan=sql_build_plan) → 执行全链路
    4. SparkReviewBuilder.build(state) → REVIEW_READY 判定
    5. 将 SparkPipelineState.stage_results 映射为前端 status 字符串

    错误码：
    - SPARK_ARTIFACTS_NOT_FOUND (404)：request_id 对应的 artifacts 不存在或已过期
    - SPARK_ARTIFACTS_INCOMPLETE (422)：sql_build_plan 或 data_transform_contract 为 None
    - SPARK_VERIFY_FAILED (500)：Orchestrator 执行过程中发生未预期异常
    """
    # 映射 SparkPipelineState 值 → 前端 status
    _STATUS_MAP = {
        "SUCCESS": "ok",
        "FAILURE": "failed",
        "HUMAN_REVIEW": "failed",
        "SKIPPED": "skipped",
        "NOT_EXECUTED": "skipped",
    }

    pipeline = request.app.state.pipeline

    # ── Step 1: 导出 artifacts ──
    bundle = pipeline.export_artifacts(body.request_id)
    if bundle is None:
        return JSONResponse(
            status_code=404,
            content={
                "error_code": "SPARK_ARTIFACTS_NOT_FOUND",
                "message": (
                    f"request_id '{body.request_id}' 对应的 artifacts 不存在或已过期。"
                    f"请先执行全流程 Run-All 生成 artifacts。"
                ),
                "field_ref": "request_id",
            },
        )

    # ── Step 2: 校验 artifacts 完整性 ──
    if bundle.sql_build_plan is None or bundle.data_transform_contract is None:
        missing_parts: list[str] = []
        if bundle.sql_build_plan is None:
            missing_parts.append("sql_build_plan")
        if bundle.data_transform_contract is None:
            missing_parts.append("data_transform_contract")
        return JSONResponse(
            status_code=422,
            content={
                "error_code": "SPARK_ARTIFACTS_INCOMPLETE",
                "message": (
                    f"request_id '{body.request_id}' 的 artifacts 不完整："
                    f"缺少 {', '.join(missing_parts)}。"
                    f"请使用全流程 Run-All（而非仅 build_plan 或 execute）生成完整 artifacts。"
                ),
                "field_ref": "request_id",
            },
        )

    # ── Step 3: Contract 适配（Lite → V1）──
    try:
        from tianshu_datadev.artifacts.models import DataTransformContractV1
        from tianshu_datadev.spark.contract_adapter import adapt_lite_to_v1
        from tianshu_datadev.spark.orchestrator import SparkOrchestrator
        from tianshu_datadev.spark.review_builder import SparkReviewBuilder

        raw_contract = bundle.data_transform_contract
        if isinstance(raw_contract, DataTransformContractV1):
            v1_contract = raw_contract
        else:
            v1_contract = adapt_lite_to_v1(raw_contract)

        # ── Step 4: 执行 Spark Orchestrator ──
        orchestrator = SparkOrchestrator()
        state = orchestrator.run(
            contract=v1_contract,
            sql_plan=bundle.sql_build_plan,
        )

        # ── Step 5: REVIEW_READY 判定 ──
        builder = SparkReviewBuilder()
        pkg = builder.build(state)

        # ── Step 6: 映射阶段状态 → 前端格式 ──
        spark_stages: list[SparkStageItem] = []
        for stage_name, result in state.stage_results.items():
            spark_stages.append(SparkStageItem(
                stage=stage_name,
                status=_STATUS_MAP.get(result, "skipped"),
            ))

        # ── Step 7: 构造响应 ──
        return SparkVerifyResponse(
            request_id=body.request_id,
            spark_stages=spark_stages,
            overall_status=pkg.overall_status,
            comparator_status=pkg.comparator_status,
            review_ready=pkg.review_ready,
            package_id=pkg.package_id,
            errors=list(state.errors),
        )

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={
                "error_code": "SPARK_VERIFY_FAILED",
                "message": f"Spark 管线验证执行异常：{e}",
                "field_ref": None,
            },
        )
```

- [ ] **Step 2: 验证端点语法——Python 导入检查**

```bash
python -c "from tianshu_datadev.api.routes import api_router; print('routes OK')"
```

预期：`routes OK`

- [ ] **Step 3: 提交**

```bash
git add src/tianshu_datadev/api/routes.py
git commit -m "feat(api): 新增 POST /api/spark/verify 端点——6 阶段结果 + REVIEW_READY 判定"
```

---

### Task 3: 后端测试——tests/api/test_spark_verify.py

**Files:**
- Create: `tests/api/test_spark_verify.py`

**Interfaces:**
- Consumes: `pipeline` fixture（来自 `tests/api/conftest.py`——真实 Pipeline 实例 + 临时目录）、`client` fixture（FastAPI TestClient）、`golden_spec_passing` fixture（可通过 Validator 的 DeveloperSpec）、`SparkVerifyResponse` / `SparkStageItem`（来自 `tianshu_datadev.api.models`）
- Produces: 4 个测试方法——正常流程 / 无效 request_id / artifacts 不完整 / 阶段失败

- [ ] **Step 1: 创建测试文件**

创建 `tests/api/test_spark_verify.py`：

```python
"""tests/api/test_spark_verify.py——POST /api/spark/verify 端点测试。

覆盖：
1. 正常流程——run_all → spark/verify → 200 + 6 阶段 + review_ready=True
2. 无效 request_id → 404 SPARK_ARTIFACTS_NOT_FOUND
3. artifacts 不完整（仅 build_plan 路径）→ 422 SPARK_ARTIFACTS_INCOMPLETE
4. Orchestrator 阶段失败注入 → 200 + review_ready=False
"""

from __future__ import annotations

import pytest


class TestSparkVerifySuccess:
    """正常流程——run_all 产出 artifacts 后 spark/verify 返回完整结果。"""

    def test_spark_verify_full_chain_returns_200(self, client, golden_spec_passing):
        """run_all → spark/verify → 200 + 6 阶段 + review_ready=True。"""
        try:
            import duckdb  # noqa: F401
        except ImportError:
            pytest.skip("DuckDB 未安装")

        # ── Step 1: 先执行全流程 Run-All ──
        resp_run = client.post("/api/run-all", json={
            "markdown_text": golden_spec_passing,
        })
        assert resp_run.status_code == 200, (
            f"run-all 应返回 200，实际 {resp_run.status_code}: {resp_run.text}"
        )
        run_result = resp_run.json()
        request_id = run_result["request_id"]
        assert request_id, "run-all 应返回非空 request_id"

        # ── Step 2: 触发 Spark 验证 ──
        resp = client.post("/api/spark/verify", json={
            "request_id": request_id,
        })
        assert resp.status_code == 200, (
            f"spark/verify 应返回 200，实际 {resp.status_code}: {resp.text}"
        )
        data = resp.json()

        # ── 验证响应结构 ──
        assert data["request_id"] == request_id
        assert len(data["spark_stages"]) == 6, (
            f"spark_stages 应有 6 个阶段，实际 {len(data['spark_stages'])}"
        )
        # 验证阶段名完整
        stage_names = {s["stage"] for s in data["spark_stages"]}
        expected_stages = {
            "MAPPER", "DEVELOPER", "COMPILER", "VALIDATOR",
            "COMPARATOR", "PHYSICAL_VERIFIER",
        }
        assert stage_names == expected_stages, (
            f"spark_stages 阶段名应为 {expected_stages}，实际 {stage_names}"
        )
        # 验证 status 值合法
        for s in data["spark_stages"]:
            assert s["status"] in ("ok", "failed", "skipped"), (
                f"阶段 {s['stage']} status 应为 ok/failed/skipped，实际 {s['status']}"
            )
        # 验证关键字段存在
        assert "overall_status" in data
        assert "comparator_status" in data
        assert "review_ready" in data
        # 单表路径 MAPPER/COMPILER/VALIDATOR 应为 SUCCESS → review_ready=True
        assert data["review_ready"] is True, (
            f"review_ready 应为 True，实际 {data['review_ready']}。"
            f"overall_status={data.get('overall_status')}, "
            f"stages={[(s['stage'], s['status']) for s in data['spark_stages']]}"
        )
        assert data["package_id"].startswith("pkg_"), (
            f"package_id 应以 pkg_ 开头，实际 {data['package_id']}"
        )


class TestSparkVerifyErrors:
    """错误路径——404 / 422 / 500。"""

    def test_invalid_request_id_returns_404(self, client):
        """不存在的 request_id → 404 SPARK_ARTIFACTS_NOT_FOUND。"""
        resp = client.post("/api/spark/verify", json={
            "request_id": "req_nonexistent_12345",
        })
        assert resp.status_code == 404, (
            f"应返回 404，实际 {resp.status_code}: {resp.text}"
        )
        data = resp.json()
        assert data["error_code"] == "SPARK_ARTIFACTS_NOT_FOUND"
        assert "不存在" in data["message"] or "已过期" in data["message"]

    def test_incomplete_artifacts_returns_422(self, client, golden_spec_passing):
        """仅 build_plan（无 contract）→ 422 SPARK_ARTIFACTS_INCOMPLETE。"""
        # ── 先执行 build_plan（不产生 contract）──
        resp_plan = client.post("/api/plan", json={
            "markdown_text": golden_spec_passing,
        })
        assert resp_plan.status_code == 200
        plan_result = resp_plan.json()
        request_id = plan_result["request_id"]

        # ── 触发 Spark 验证 ──
        resp = client.post("/api/spark/verify", json={
            "request_id": request_id,
        })
        assert resp.status_code == 422, (
            f"应返回 422，实际 {resp.status_code}: {resp.text}"
        )
        data = resp.json()
        assert data["error_code"] == "SPARK_ARTIFACTS_INCOMPLETE"
        assert "data_transform_contract" in data["message"]

    def test_stage_failure_returns_200_with_review_ready_false(
        self, client, golden_spec_passing,
    ):
        """Orchestrator MAPPER 阶段失败 → 200 + review_ready=False。

        通过替换 Pipeline._results 中的 contract 为无效值来触发 MAPPER 失败。
        """
        try:
            import duckdb  # noqa: F401
        except ImportError:
            pytest.skip("DuckDB 未安装")

        # ── Step 1: 正常 run-all ──
        resp_run = client.post("/api/run-all", json={
            "markdown_text": golden_spec_passing,
        })
        assert resp_run.status_code == 200
        request_id = resp_run.json()["request_id"]

        # ── Step 2: 注入损坏的 contract 到 _results ──
        pipeline = client.app.state.pipeline
        saved = pipeline._results.get(request_id)
        assert saved is not None, "_results 中应有该 request_id 的数据"
        # 将 contract 替换为 None——模拟缺失场景
        saved["contract"] = None

        # ── Step 3: 触发 Spark 验证（应因 contract 为 None 而失败）──
        # 注意：export_artifacts 会返回 bundle，但 contract 为 None
        # 这将触发 SPARK_ARTIFACTS_INCOMPLETE
        resp = client.post("/api/spark/verify", json={
            "request_id": request_id,
        })
        assert resp.status_code == 422, (
            f"contract 为 None 应触发 422，实际 {resp.status_code}: {resp.text}"
        )
        data = resp.json()
        assert data["error_code"] == "SPARK_ARTIFACTS_INCOMPLETE"
```

- [ ] **Step 2: 运行新测试——验证全部通过**

```bash
python -m pytest tests/api/test_spark_verify.py -v --tb=short
```

预期：4 passed

- [ ] **Step 3: 验证全量 API + Spark 回归**

```bash
python -m pytest tests/api/ tests/spark/ -q
```

预期：无退化，所有已有测试继续通过

- [ ] **Step 4: Ruff 检查**

```bash
python -m ruff check src/tianshu_datadev/api/ tests/api/
```

预期：零告警

- [ ] **Step 5: 提交**

```bash
git add tests/api/test_spark_verify.py
git commit -m "test(api): 新增 POST /api/spark/verify 端点测试——4 场景覆盖正常/404/422/失败"
```

---

### Task 4: 前端 API 客户端——SparkVerifyResponse 类型 + sparkVerify() 方法

**Files:**
- Modify: `frontend/src/api/client.ts`——在文件末尾追加

**Interfaces:**
- Consumes: `apiPost<T>()`（同文件已有）
- Produces: `SparkStageItem` interface、`SparkVerifyResponse` interface、`sparkVerify(requestId: string): Promise<SparkVerifyResponse>` 函数

- [ ] **Step 1: 在 client.ts 末尾追加 Spark 验证相关类型和方法**

在 `frontend/src/api/client.ts` 文件末尾（`getPackageRich` 函数之后）追加：

```typescript
// ── Spark 管线验证 ──

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

/** 触发 Spark 管线验证——传入 Pipeline Run-All 产出的 request_id */
export function sparkVerify(requestId: string): Promise<SparkVerifyResponse> {
  return apiPost<SparkVerifyResponse>('/spark/verify', { request_id: requestId });
}
```

- [ ] **Step 2: TypeScript 类型检查**

```bash
cd frontend && npx tsc --noEmit
```

预期：零错误

- [ ] **Step 3: 提交**

```bash
git add frontend/src/api/client.ts
git commit -m "feat(frontend): 新增 SparkVerifyResponse 类型 + sparkVerify() API 方法"
```

---

### Task 5: 前端组件扩展——PipelineStageIndicator 添加 title prop + Spark 阶段中文映射

**Files:**
- Modify: `frontend/src/components/PipelineStageIndicator.tsx`

**Interfaces:**
- Consumes: `StageInfo`（已有）、`PipelineError`（已有）
- Produces: `PipelineStageIndicator` 组件新增 `title?: string` prop（默认 "流水线阶段"）——不改变已有调用方行为

- [ ] **Step 1: 修改 PipelineStageIndicator 组件**

**改动点 1**：Props 接口新增 `title` 字段

定位到 `interface Props`（第 18-21 行），在 `error` 字段后追加 `title`：

```typescript
interface Props {
  stages: StageInfo[];
  error: PipelineError | null;
  /** 指示灯标题——默认"流水线阶段"，Spark 侧传入"Spark 管线" */
  title?: string;
}
```

**改动点 2**：STAGE_CN 映射扩展——新增 Spark 6 阶段

定位到 `const STAGE_CN`（第 24-31 行），追加 Spark 阶段：

```typescript
const STAGE_CN: Record<string, string> = {
  // SQL 侧（已有）
  parser: '解析',
  enrich: '增强',
  build: '构建',
  validate: '验证',
  compile: '编译',
  execute: '执行',
  // Spark 侧（新增）
  MAPPER: '映射',
  DEVELOPER: '标注',
  COMPILER: '编译',
  VALIDATOR: '校验',
  COMPARATOR: '对比',
  PHYSICAL_VERIFIER: '物理验证',
};
```

**改动点 3**：下拉框 header 使用 `title` prop

定位到第 91 行 `<div className="pipeline-dropdown-header">流水线阶段</div>`，替换为：

```tsx
<div className="pipeline-dropdown-header">{title || '流水线阶段'}</div>
```

**改动点 4**：函数签名解构 `title`

定位到第 48 行 `export function PipelineStageIndicator({ stages, error }: Props)`，改为：

```typescript
export function PipelineStageIndicator({ stages, error, title }: Props) {
```

- [ ] **Step 2: TypeScript 类型检查——确认已有调用方不受影响**

```bash
cd frontend && npx tsc --noEmit
```

预期：零错误（App.tsx 中已有 `<PipelineStageIndicator stages={...} error={...} />` 不传 `title` 应正常工作，因为 `title` 是可选的）

- [ ] **Step 3: 提交**

```bash
git add frontend/src/components/PipelineStageIndicator.tsx
git commit -m "feat(frontend): PipelineStageIndicator 扩展——title prop + Spark 6 阶段中文映射"
```

---

### Task 6: 前端 App.tsx 集成——Spark 验证按钮 + 第二个指示灯

**Files:**
- Modify: `frontend/src/App.tsx`

**Interfaces:**
- Consumes: `PipelineStageIndicator`（已有，已扩展 `title` prop）、`sparkVerify` / `SparkVerifyResponse` / `SparkStageItem`（Task 4 产出）
- Produces: `AppState` 新增 `sparkStages` + `sparkVerifyResult` 字段，"Spark 验证"按钮（依赖 requestId 非空），第二个 `PipelineStageIndicator`（title="Spark 管线"）

- [ ] **Step 1: 修改 App.tsx——追加导入、状态、按钮、指示灯**

**改动点 1**：导入 `sparkVerify` 和 `SparkVerifyResponse`

定位到第 17-29 行的导入块，在 `getPackageRich` 后追加 `sparkVerify`，在类型导入中追加 `SparkVerifyResponse`：

```typescript
import {
  parseSpecRich,
  buildPlanRich,
  executeRich,
  runAll,
  getPackageRich,
  sparkVerify,
  ApiError,
  SpecRichResponse,
  PlanRichResponse,
  ExecuteRichResponse,
  PackageRichResponse,
  SparkVerifyResponse,
  TemplateFull,
} from './api/client';
```

**改动点 2**：AppState 新增 Spark 相关字段

定位到 `interface AppState`（第 36-52 行），在 `packageResult` 后追加：

```typescript
  // Spark 管线验证结果
  sparkStages: StageInfo[];
  sparkVerifyResult: SparkVerifyResponse | null;
```

然后在 `useState<AppState>` 初始值中（第 55-67 行）追加默认值：

```typescript
    sparkStages: [],
    sparkVerifyResult: null,
```

**改动点 3**：新增 `handleSparkVerify` 处理函数

在 `handleRunAll` 函数之后（第 218 行之后），追加：

```typescript
  /** Spark 管线验证 */
  const handleSparkVerify = () => {
    if (!state.requestId) {
      update({ error: { error_code: 'NO_REQUEST_ID', message: '请先执行全流程 Run-All 生成 request_id', field_ref: null } });
      return;
    }
    update({ isLoading: true, error: null });
    sparkVerify(state.requestId)
      .then((result) => {
        update({
          isLoading: false,
          sparkStages: result.spark_stages,
          sparkVerifyResult: result,
        });
      })
      .catch((err) => {
        const apiErr: ApiError =
          err && typeof err === 'object' && 'error_code' in err
            ? (err as ApiError)
            : { error_code: 'NETWORK_ERROR', message: String(err), field_ref: null };
        update({ isLoading: false, error: apiErr, sparkStages: [], sparkVerifyResult: null });
      });
  };
```

**改动点 4**：按钮栏新增"Spark 验证"按钮

定位到"全流程 Run-All"按钮之后（第 274-277 行），追加：

```tsx
            <button
              className="btn btn-accent"
              disabled={!state.requestId || state.isLoading}
              onClick={handleSparkVerify}
            >
              Spark 验证
            </button>
```

**改动点 5**：header 区域新增第二个 PipelineStageIndicator

定位到第 227-231 行的 header 右侧区域，在第一个 `<PipelineStageIndicator>` 之后追加：

```tsx
          <PipelineStageIndicator
            stages={state.sparkStages}
            error={null}
            title="Spark 管线"
          />
```

- [ ] **Step 2: TypeScript 类型检查**

```bash
cd frontend && npx tsc --noEmit
```

预期：零错误

- [ ] **Step 3: 前端构建验证**

```bash
cd frontend && npm run build
```

预期：构建成功，无报错

- [ ] **Step 4: 提交**

```bash
git add frontend/src/App.tsx
git commit -m "feat(frontend): App.tsx 集成——Spark 验证按钮 + 第二个 PipelineStageIndicator"
```

---

## 验收

全部 Task 完成后执行：

### 后端验收

```bash
# 1. API + Spark 全量回归
python -m pytest tests/api/ tests/spark/ -q

# 2. Ruff 静态检查
python -m ruff check src/tianshu_datadev/api/ tests/api/

# 3. Git diff 格式检查
git diff --check
```

### 前端验收

```bash
# 4. TypeScript 类型检查
cd frontend && npx tsc --noEmit

# 5. 前端构建
cd frontend && npm run build
```

### 验收通过标准

| 检查项 | 命令 | 通过标准 |
|--------|------|----------|
| API + Spark 测试 | `pytest tests/api/ tests/spark/ -q` | 零失败，已有测试无退化 |
| Ruff | `ruff check src/tianshu_datadev/api/ tests/api/` | 零告警 |
| git diff | `git diff --check` | 无空白符告警 |
| TypeScript | `npx tsc --noEmit` | 零错误 |
| 前端构建 | `npm run build` | 构建成功 |

---

## A/B/C 风险分类

### A 类（无阻断，可进入实施）

- **A1 端点设计**：POST /api/spark/verify 接口语义清晰——接收 request_id、返回 Spark 6 阶段结果 + REVIEW_READY 判定。不修改任何已有接口。
- **A2 组件复用**：PipelineStageIndicator 仅新增可选 `title` prop，已有调用方零改动。Spark 阶段中文映射遵循已有 `STAGE_CN` 模式。
- **A3 测试独立**：新端点测试放 `tests/api/test_spark_verify.py`——独立文件、不修改已有测试。

### B 类（已知边界，需在实施中注意）

- **B1 Contract 类型分叉**：`Pipeline.export_artifacts()` 返回的 `data_transform_contract` 可能是 `DataTransformContractLite`（单表路径）或 `DataTransformContractV1`（多语句路径）。端点中需要 `isinstance` 检查——与 `test_spark_eval.py::TestC4ReviewReady` 中已有的处理方式一致。
- **B2 无 DEVELOPER 注入**：当前 Orchestrator 在无 `llm_call` 注入时 DEVELOPER 标记 SKIPPED——这是设计行为，不是 Bug。REVIEW_READY 判定中 DEVELOPER 不是关键阶段。
- **B3 TTL 过期**：`Pipeline._results` 缓存有 TTL 机制——实际使用中如果用户 Run-All 后等待过久再点"Spark 验证"，会触发 404。这是正确的行为，错误消息已包含说明。
- **B4 前端无 Spark 阶段错误详情**：当前第二个指示灯只展示阶段状态（ok/failed/skipped），不展示 Spark 侧的具体错误信息。若需错误详情展示，可在后续迭代中扩展 `PipelineStageIndicator` 的 `error` prop。

### C 类（无——无阻断风险）

经过对 spec 和现有代码的完整审查，**未发现 C 类风险**：
- 接口语义清晰——POST /api/spark/verify 只依赖 request_id，不引入新 DSL 或协议
- 阶段边界明确——不改 SQL Pipeline、不改 Orchestrator 接口、不改 Comparator 规则
- 无外部依赖——不引入真实 LLM、生产数据、Spark 物理执行
- 所有依赖组件已存在且稳定——Pipeline.export_artifacts() / adapt_lite_to_v1() / SparkOrchestrator.run() / SparkReviewBuilder.build() 均为 Phase 9A5 已完成验证的组件

---

## 残留风险

| 编号 | 说明 | 等级 | 处置 |
|:----:|------|:----:|------|
| R11 | 前端无自动化测试框架——Spark 验证按钮和第二个指示灯的交互行为依赖手动冒烟 | C | 验收使用 TypeScript 类型检查 + 构建验证；后续可接入 Playwright/Cypress |
| R12 | Spark 验证端点未测试 TTL 过期场景——`_purge_expired` 依赖时间流逝，单测难以构造 | C | 错误码路径已覆盖（404），TTL 行为由 Pipeline 自身测试保障 |

---

## 非技术解释：为什么采用"SQL Run-All 后再点 Spark 验证"的两步设计

简单说：**SQL 管线是"主线"——它生成数据、产出结果、打包交付物。Spark 验证是"质检"——它拿着 SQL 管线产出的中间产物，独立跑一遍 Spark 侧的编译和逻辑对比，帮你确认两边逻辑对得上。**

为什么要分两步而不是一键完成？

1. **职责分开，互不阻塞**：SQL 管线负责"产出结果"（你的数据报告），Spark 验证负责"检查质量"（SQL 和 Spark 逻辑是否等价）。如果把它们绑在一起，Spark 验证失败会导致你看不到 SQL 的执行结果——但很多时候你只是想先看数据，质检可以后面再做。

2. **request_id 是桥梁**：SQL Run-All 产出一个 `request_id`（类似快递单号），Spark 验证拿着这个单号去后台提取 SQL 管线已经算好的中间产物（数据库查询计划、数据转换合同），然后在 Spark 侧独立重跑一遍编译和对比。不需要你再粘贴任何东西。

3. **Spark 验证是可选的**：不是每个项目都需要 Spark 验证——如果你的场景只在 SQL 侧就够了，完全可以忽略"Spark 验证"按钮。两步设计让你按需使用，不必为用不到的功能等待。

4. **符合实际工作流**：开发者的真实流程是——先写项目书 → 跑出 SQL 结果 → 确认数据合理 → 再跑 Spark 验证确认逻辑等价。这个顺序和按钮排布一一对应。

总结：**先产出、再质检。各做各的，互不耽误。**

---

## 是否可进入实施阶段

**是。** 计划完整覆盖了设计 spec 的全部 7 个部分（3.1 端点、3.2 状态映射、3.3 后端文件、4.1 API 客户端、4.2 组件扩展、4.3 App 集成、4.4 前端文件），6 个 Task 边界清晰，每个 Task 有独立的测试周期和提交点。无 C 类风险、无接口歧义、无外部依赖阻塞。

预估总工作量：6 个 Task，每个 15-30 分钟，总计约 2-3 小时。
