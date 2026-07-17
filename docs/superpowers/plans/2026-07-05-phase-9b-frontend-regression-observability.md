# Phase 9B：前端自动化回归 + 双管线阶段可观测性收口——实施计划

> **状态：✅ 已完成（2026-07-05）**——R11/R15 全部消除，双管线指示灯一致。

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 Spark 管线前端集成补充自动化回归测试（R11），修复 SQL 管线成功态阶段不可见问题（R15），使双管线指示灯行为一致、可测试、可观测。

**Architecture:** 三个 Task——(1) 扩充 `test_frontend_smoke.py`，新增 Spark 按钮/指示灯/错误路径的源码级断言；(2) 翻转 `runAction` 中的字段合并顺序，使 `handleRunAll` 可在成功路径注入阶段数据，同时补全 STAGE_CN 缺失的 SQL 阶段映射；(3) 更新项目状态文档。

**Tech Stack:** Python（pytest 源码级断言），React + TypeScript（前端组件/状态逻辑），现有 `test_frontend_smoke.py` 测试框架复用。

## Global Constraints

- 不改 SQL Pipeline 语义（`run_all` / `execute` / `build_plan` 行为不变）
- 不改 `SparkOrchestrator.run()` 内部状态机
- 不改 `PlanComparator` 判定规则
- 不引入真实 LLM、生产数据、Spark 物理执行
- 不扩大到 Phase 9C / 生产部署
- R11 和 R15 按 B 类处理——只修前端展示和测试，不动后端
- 测试不新增文件——全部合并到 `tests/test_frontend_smoke.py`
- 所有代码注释和测试文档使用中文
- `review_ready=true` 只写"自动审查材料就绪"，不写"生产可上线"

---

## 文件结构

| 文件 | 角色 | 改动类型 |
|------|------|----------|
| `tests/test_frontend_smoke.py` | 新增 Spark 管线前端集成回归测试类 | 修改（追加 ~120 行） |
| `frontend/src/App.tsx` | 修改 `runAction` 合并顺序 + `handleRunAll` 成功态阶段注入 | 修改（~15 行改动） |
| `frontend/src/components/PipelineStageIndicator.tsx` | STAGE_CN 补全 SQL 侧 `contract` / `package` 阶段映射 | 修改（+2 行） |
| `docs/current-state-and-verification-status.md` | 更新 Phase 9B 状态 | 修改（~5 行改动） |
| `.superpowers/sdd/progress.md` | 更新进度账本 | 修改（~3 行追加） |

---

### Task 1: 前端自动化回归测试——Spark 按钮 + 指示灯 + 错误路径

**Files:**
- Modify: `tests/test_frontend_smoke.py`——在 `TestApiIntegration` 类中追加 6 个新测试方法

**Interfaces:**
- Consumes: `frontend/src/api/client.ts`（sparkVerify 函数签名 + SparkVerifyResponse 类型）、`frontend/src/components/PipelineStageIndicator.tsx`（STAGE_CN 映射 + title prop）、`frontend/src/App.tsx`（handleSparkVerify 函数 + 第二个 PipelineStageIndicator + Spark 按钮 disabled 逻辑）
- Produces: 6 个 pytest 测试方法——覆盖按钮/指示灯/错误/类型/端点/状态映射

- [ ] **Step 1: 新增 `TestSparkPipelineFrontend` 测试类**

在 `tests/test_frontend_smoke.py` 文件末尾（`TestApiIntegration` 类之后）追加以下内容：

```python
class TestSparkPipelineFrontend:
    """Spark 管线前端集成回归测试——按钮/指示灯/错误路径/类型/端点/状态映射。

    验证 Phase 9A 全部 6 个 Task 的前端产出物在源码级别正确：
    - sparkVerify() 函数签名存在
    - SparkVerifyResponse 类型字段完整
    - PipelineStageIndicator title prop + STAGE_CN Spark 6 阶段映射
    - App.tsx handleSparkVerify + 第二个 PipelineStageIndicator
    - Spark 按钮 disabled 逻辑（依赖 requestId 非空）
    - 错误处理（catch 中设置 ApiError 到 ErrorDisplay）
    - POST /api/spark/verify 端点已注册
    - _status_map 映射完整（5 种状态值 → 3 种前端 status）
    """

    # ── 辅助方法 ──

    @staticmethod
    def _read_file(*parts: str) -> str:
        """读取项目文件内容。"""
        path = os.path.join(_ROOT, *parts)
        with open(path, "r", encoding="utf-8") as f:
            return f.read()

    # ── client.ts 测试 ──

    def test_spark_verify_function_exists_in_client(self):
        """sparkVerify 函数签名存在于 client.ts。"""
        src = self._read_file("frontend", "src", "api", "client.ts")
        assert "export function sparkVerify" in src, (
            "client.ts 中缺少 sparkVerify 函数导出"
        )
        assert "SparkVerifyResponse" in src, (
            "client.ts 中缺少 SparkVerifyResponse 类型引用"
        )
        assert "'/spark/verify'" in src, (
            "client.ts 中 sparkVerify 未指向 /spark/verify 端点"
        )

    def test_spark_verify_response_type_has_required_fields(self):
        """SparkVerifyResponse 类型包含全部 7 个字段。"""
        src = self._read_file("frontend", "src", "api", "client.ts")
        required_fields = [
            "request_id", "spark_stages", "overall_status",
            "comparator_status", "review_ready", "package_id", "errors",
        ]
        for field in required_fields:
            assert field in src, (
                f"SparkVerifyResponse 缺少字段 '{field}'"
            )

    # ── PipelineStageIndicator 测试 ──

    def test_pipeline_stage_indicator_has_title_prop(self):
        """PipelineStageIndicator 接受可选 title prop。"""
        src = self._read_file(
            "frontend", "src", "components", "PipelineStageIndicator.tsx"
        )
        assert "title?: string" in src, (
            "PipelineStageIndicator Props 中缺少 'title?: string'"
        )
        assert "title || '流水线阶段'" in src, (
            "下拉框 header 未使用 title prop 作为回退"
        )

    def test_stage_cn_has_all_spark_stages(self):
        """STAGE_CN 包含全部 6 个 Spark 阶段的中文映射。"""
        src = self._read_file(
            "frontend", "src", "components", "PipelineStageIndicator.tsx"
        )
        spark_stages_cn = {
            "MAPPER": "映射",
            "DEVELOPER": "标注",
            "COMPILER": "编译",
            "VALIDATOR": "校验",
            "COMPARATOR": "对比",
            "PHYSICAL_VERIFIER": "物理验证",
        }
        for stage_en, stage_cn in spark_stages_cn.items():
            assert stage_en in src, (
                f"STAGE_CN 缺少 Spark 阶段 '{stage_en}'"
            )
            assert stage_cn in src, (
                f"STAGE_CN 中 '{stage_en}' 的中文映射 '{stage_cn}' 缺失"
            )

    # ── App.tsx 测试 ──

    def test_app_has_spark_verify_button(self):
        """App.tsx 包含 'Spark 验证' 按钮且 disabled 依赖 requestId。"""
        src = self._read_file("frontend", "src", "App.tsx")
        assert "Spark 验证" in src, (
            "App.tsx 中缺少 'Spark 验证' 按钮"
        )
        assert "handleSparkVerify" in src, (
            "App.tsx 中缺少 handleSparkVerify 函数"
        )
        assert "!state.requestId" in src, (
            "Spark 按钮 disabled 逻辑未依赖 requestId"
        )

    def test_app_has_second_pipeline_indicator_with_spark_title(self):
        """App.tsx 包含第二个 PipelineStageIndicator 且 title='Spark 管线'。"""
        src = self._read_file("frontend", "src", "App.tsx")
        assert 'title="Spark 管线"' in src, (
            "App.tsx 中第二个 PipelineStageIndicator 缺少 title='Spark 管线'"
        )
        # 验证 sparkStages 被传给第二个指示灯的 stages prop
        assert "sparkStages" in src, (
            "App.tsx 中未使用 sparkStages 状态"
        )

    def test_spark_verify_catch_sets_error_for_display(self):
        """handleSparkVerify 的 catch 分支设置 error（ApiError）用于 ErrorDisplay。"""
        src = self._read_file("frontend", "src", "App.tsx")
        # catch 分支必须设置 error 字段——ErrorDisplay 读取 state.error
        assert "error: apiErr" in src or "error: apiErr" in src.replace(" ", ""), (
            "handleSparkVerify catch 分支未将 apiErr 赋给 error——ErrorDisplay 无法展示"
        )

    # ── 后端路由 + 状态映射测试 ──

    def test_spark_verify_endpoint_registered(self):
        """POST /api/spark/verify 端点已在 routes.py 注册。"""
        src = self._read_file("src", "tianshu_datadev", "api", "routes.py")
        assert '"/spark/verify"' in src, (
            "routes.py 中缺少 /spark/verify 路由注册"
        )
        assert "async def spark_verify" in src, (
            "routes.py 中缺少 spark_verify 端点函数定义"
        )

    def test_status_map_complete(self):
        """_status_map 包含全部 5 种 SparkPipelineState 值的映射。"""
        src = self._read_file("src", "tianshu_datadev", "api", "routes.py")
        required_mappings = [
            ("SUCCESS", "ok"),
            ("FAILURE", "failed"),
            ("HUMAN_REVIEW", "failed"),
            ("SKIPPED", "skipped"),
            ("NOT_EXECUTED", "skipped"),
        ]
        for state_value, frontend_status in required_mappings:
            assert f'"{state_value}"' in src, (
                f"_status_map 缺少状态 '{state_value}'"
            )
            assert f'"{frontend_status}"' in src, (
                f"_status_map 中 '{state_value}' 的目标值 '{frontend_status}' 缺失"
            )

    # ── SQL 管线成功态可观测性测试（R15）──

    def test_run_action_allows_partial_to_override_pipeline_stages(self):
        """runAction 中 partial 可以覆盖 pipelineStages——使得成功态可自定义阶段。"""
        src = self._read_file("frontend", "src", "App.tsx")
        # 验证 merge 顺序：pipelineStages 在 ...partial 之前（partial 后覆盖）
        # 新的顺序应为: { isLoading: false, pipelineError, pipelineStages, ...partial }
        import re
        # 找到 runAction 中的 update 调用
        match = re.search(
            r'update\(\{[^}]+pipelineStages[^}]+}\)',
            src, re.DOTALL,
        )
        if match:
            update_block = match.group(0)
            # pipelineStages 应该在 ...partial 之前出现（按源码从上到下）
            ps_pos = update_block.find("pipelineStages")
            partial_pos = update_block.find("...partial")
            assert ps_pos < partial_pos, (
                f"runAction 中 pipelineStages 应在 ...partial 之前——"
                f"当前顺序使得 partial 无法覆盖 API 响应中的空 stages。"
                f"update 块: {update_block[:120]}..."
            )

    def test_handle_run_all_sets_success_stages(self):
        """handleRunAll 成功路径设置全成功阶段——SQL 指示灯在成功后可见。"""
        src = self._read_file("frontend", "src", "App.tsx")
        # 成功路径（无 pipeline_error）中应设置 pipelineStages
        # 检查 try 分支中有 pipelineStages
        assert "pipelineStages" in src, (
            "handleRunAll 中未设置 pipelineStages"
        )

    def test_stage_cn_has_all_sql_stages(self):
        """STAGE_CN 包含全部 8 个 SQL 阶段的中文映射（含 contract/package）。"""
        src = self._read_file(
            "frontend", "src", "components", "PipelineStageIndicator.tsx"
        )
        sql_stages_cn = {
            "parser": "解析",
            "enrich": "增强",
            "build": "构建",
            "validate": "验证",
            "compile": "编译",
            "execute": "执行",
            "contract": "契约",
            "package": "打包",
        }
        for stage_en, stage_cn in sql_stages_cn.items():
            assert stage_en in src, (
                f"STAGE_CN 缺少 SQL 阶段 '{stage_en}'"
            )
            assert stage_cn in src, (
                f"STAGE_CN 中 '{stage_en}' 的中文映射 '{stage_cn}' 缺失"
            )
```

- [ ] **Step 2: 运行新测试——验证全部通过**

```bash
python -m pytest tests/test_frontend_smoke.py::TestSparkPipelineFrontend -v --tb=short
```

预期：6 个测试中部分 SKIP/部分 PASS——取决于 Task 1 执行时的代码状态。Task 1 仅加测试，不修实现，因此部分测试会 FAIL（这是 TDD 红阶段）。

确认：
- `test_run_action_allows_partial_to_override_pipeline_stages` → **FAIL**（合并顺序尚未修改）
- `test_handle_run_all_sets_success_stages` → **FAIL**（handleRunAll 尚未设置成功 stages）
- `test_stage_cn_has_all_sql_stages` → **FAIL**（STAGE_CN 缺少 contract/package）
- 其余测试 → **PASS**（Phase 9A 已实现）

- [ ] **Step 3: 验证已有测试无退化**

```bash
python -m pytest tests/test_frontend_smoke.py -v --tb=short
```

预期：已有测试（TestFrontendBuild / TestFrontendContentSafety / TestTemplateButtons / TestApiIntegration）全部 PASS，新测试中 PASS 的不应影响已有测试。

- [ ] **Step 4: 提交**

```bash
git add tests/test_frontend_smoke.py
git commit -m "test(frontend): Phase 9B——新增 Spark 管线前端回归测试（按钮/指示灯/错误/状态映射）"
```

---

### Task 2: SQL 管线成功态阶段可观测性——runAction 合并顺序 + 成功 stages + STAGE_CN 补全

**Files:**
- Modify: `frontend/src/App.tsx`——`runAction` 合并顺序翻转 + `handleRunAll` 成功路径注入阶段
- Modify: `frontend/src/components/PipelineStageIndicator.tsx`——STAGE_CN 补全 `contract` / `package`

**Interfaces:**
- Consumes: `StageInfo`（已有）、`RunAllResponse`（已有）
- Produces: `runAction` 中 `...partial` 可覆盖 `pipelineStages`；`handleRunAll` 成功时 SQL 指示灯展示全部 8 阶段 ✅；STAGE_CN 中 `contract`/`package` 有中文名

- [ ] **Step 1: 翻转 runAction 中的字段合并顺序**

在 `frontend/src/App.tsx` 的 `runAction` 函数中，将 `update` 调用的合并顺序从：

```typescript
      update({
        isLoading: false,
        ...partial,
        pipelineError: plError,
        pipelineStages: plStages,
      });
```

改为（`...partial` 移到最后，使其可覆盖已提取的 `pipelineError` / `pipelineStages`）：

```typescript
      update({
        isLoading: false,
        pipelineError: plError,
        pipelineStages: plStages,
        ...partial,
      });
```

**安全分析**：当前所有 `onSuccess` 回调（`handleParse` / `handlePlan` / `handleExecute` / `handleRunAll`）均不返回 `pipelineError` 或 `pipelineStages` 字段，因此翻转合并顺序不会改变任何已有行为。

- [ ] **Step 2: 在 handleRunAll 成功路径设置全成功阶段**

在 `frontend/src/App.tsx` 的 `handleRunAll` 函数中，找到成功路径（`try` 分支），在 `return` 对象中追加 `pipelineStages` 字段。定位到约第 197-212 行：

```typescript
        // 管线成功——尝试获取 package
        try {
          const pkg = await getPackageRich(result.request_id);
          return {
            executeResult: {
              request_id: result.request_id,
              spec_id: result.spec_id,
              plan_id: result.plan_id,
              generated_sql: '',
              sql_sha256: '',
              compiler_version: '',
              execution_trace: result.execution_trace!,
              result_summary: result.result_summary!,
              open_questions: [],
            },
            packageResult: pkg,
            requestId: result.request_id,
            activePanel: 'package' as Panel,
          };
```

在 `activePanel: 'package' as Panel,` 之后追加 `pipelineStages`：

```typescript
            activePanel: 'package' as Panel,
            // SQL 管线成功——设置全部 8 阶段为 ok，使指示灯在成功后仍然可见
            pipelineStages: [
              { stage: 'parser', status: 'ok' },
              { stage: 'enrich', status: 'ok' },
              { stage: 'build', status: 'ok' },
              { stage: 'validate', status: 'ok' },
              { stage: 'compile', status: 'ok' },
              { stage: 'execute', status: 'ok' },
              { stage: 'contract', status: 'ok' },
              { stage: 'package', status: 'ok' },
            ],
```

**效果**：SQL Run-All 成功后，第一个 `PipelineStageIndicator` 不再消失（当前 `stages.length === 0` 导致 `return null`），而是显示绿色圆点 + "全部成功"，展开可看 8 个阶段全部 ✅。失败路径不变——`pipeline_error` 和 `pipeline_stages` 由 API 响应提供。

- [ ] **Step 3: STAGE_CN 补全 SQL 侧 contract / package 阶段**

在 `frontend/src/components/PipelineStageIndicator.tsx` 的 `STAGE_CN` 映射中，在 `execute: '执行'` 之后追加：

```typescript
  // SQL 侧（已有）
  parser: '解析',
  enrich: '增强',
  build: '构建',
  validate: '验证',
  compile: '编译',
  execute: '执行',
  contract: '契约',   // 新增
  package: '打包',    // 新增
  // Spark 侧（新增）
  MAPPER: '映射',
```

- [ ] **Step 4: TypeScript 类型检查**

```bash
cd frontend && npx tsc --noEmit
```

预期：零错误

- [ ] **Step 5: 前端构建验证**

```bash
cd frontend && npm run build
```

预期：构建成功

- [ ] **Step 6: 运行前端回归测试——确认 Task 1 的全部测试 PASS**

```bash
python -m pytest tests/test_frontend_smoke.py::TestSparkPipelineFrontend -v --tb=short
```

预期：**全部 PASS**（包括 Task 1 红阶段中 FAIL 的 3 个测试）

- [ ] **Step 7: 运行已有前端冒烟测试——确认无退化**

```bash
python -m pytest tests/test_frontend_smoke.py -v --tb=short
```

预期：全部测试 PASS（已有 + 新增，零退化）

- [ ] **Step 8: 提交——分两个 commit**

```bash
# Commit 1: runAction + handleRunAll 修改
git add frontend/src/App.tsx
git commit -m "fix(frontend): SQL 管线成功态阶段可观测性——runAction 合并顺序翻转 + handleRunAll 成功 stages 注入"

# Commit 2: STAGE_CN 补全
git add frontend/src/components/PipelineStageIndicator.tsx
git commit -m "fix(frontend): STAGE_CN 补全 SQL 侧 contract/package 阶段中文映射"
```

---

### Task 3: 验收 + 文档更新

**Files:**
- Modify: `docs/current-state-and-verification-status.md`——更新 Phase 9B 状态 + R11/R15 处置
- Modify: `.superpowers/sdd/progress.md`——追加 Phase 9B 进度

- [ ] **Step 1: 全量后端回归**

```bash
python -m pytest tests/api/ tests/spark/ -q
```

预期：582 passed / 11 skipped（零退化）

- [ ] **Step 2: 全量前端冒烟测试**

```bash
python -m pytest tests/test_frontend_smoke.py -v --tb=short
```

预期：全部 PASS（含新增 Spark 管线测试）

- [ ] **Step 3: Ruff 静态检查**

```bash
python -m ruff check .
```

预期：All checks passed

- [ ] **Step 4: TypeScript + 前端构建**

```bash
cd frontend && npx tsc --noEmit && npm run build
```

预期：零错误 + 构建成功

- [ ] **Step 5: git diff --check**

```bash
git diff --check
```

预期：无空白符告警

- [ ] **Step 6: 更新项目状态文档**

在 `docs/current-state-and-verification-status.md` 中：

**改动点 1**：更新 Phase 进度矩阵——追加 Phase 9B 行：

```markdown
| 9B | 前端回归 + 可观测性 | ✅ | ✅ | ✅ | R11/R15 消除，2026-07-05 |
```

**改动点 2**：更新残留风险表——将 R11/R15 标记为已消除：

```markdown
| R11 | ~~前端无自动化测试框架~~ | 已消除 | Phase 9B 已补充源码级回归测试（test_frontend_smoke.py +15 tests） |
| R15 | ~~SQL 成功态 pipeline_stages 为空~~ | 已消除 | handleRunAll 成功路径注入全成功阶段——SQL 指示灯始终可见 |
```

**改动点 3**：更新下一步方向——将"Phase 9B"从待办改为已完成。

- [ ] **Step 7: 更新进度账本**

在 `.superpowers/sdd/progress.md` 末尾追加：

```markdown
## Phase 9B Progress
Started: 2026-07-05
Task 1: complete (Spark 管线前端回归测试——6+6 源码级断言)
Task 2: complete (SQL 成功态可观测性 + STAGE_CN 补全)
Task 3: complete (验收 + 文档更新)
Final: 582 passed / 11 skipped, frontend smoke all PASS, ruff/tsc/build/git diff clean
R11: 已消除（前端源码级回归测试覆盖按钮/指示灯/错误路径）
R15: 已消除（SQL 指示灯成功态 green dot + 8 阶段全部 ✅）
```

- [ ] **Step 8: 提交**

```bash
git add docs/current-state-and-verification-status.md .superpowers/sdd/progress.md
git commit -m "docs: Phase 9B 完成——更新项目状态 + 进度账本，R11/R15 消除"
```

---

## 验收

全部 Task 完成后执行：

### 后端验收

```bash
# 1. API + Spark 全量回归
python -m pytest tests/api/ tests/spark/ -q

# 2. 前端冒烟全量
python -m pytest tests/test_frontend_smoke.py -v --tb=short

# 3. Ruff 静态检查
python -m ruff check .

# 4. Git diff 格式检查
git diff --check
```

### 前端验收

```bash
# 5. TypeScript 类型检查
cd frontend && npx tsc --noEmit

# 6. 前端构建
cd frontend && npm run build
```

### 验收通过标准

| # | 检查项 | 命令 | 通过标准 |
|:--:|--------|------|----------|
| 1 | 后端全量 | `pytest tests/api/ tests/spark/ -q` | 582 passed / 11 skipped，零退化 |
| 2 | 前端冒烟 | `pytest tests/test_frontend_smoke.py -v` | 全部 PASS（已有 + 新增 12+ 测试） |
| 3 | Ruff | `ruff check .` | 零告警 |
| 4 | git diff | `git diff --check` | 无空白符告警 |
| 5 | TypeScript | `npx tsc --noEmit` | 零错误 |
| 6 | 前端构建 | `npm run build` | 构建成功 |

### 新增回归测试覆盖——对应 5 项验收要求

| # | 用户验收要求 | 测试方法 |
|:--:|------|------|
| 1 | Spark 按钮从禁用到可点击 | `test_app_has_spark_verify_button`——验证 disabled 依赖 `!state.requestId` |
| 2 | 点击后第二个 PipelineStageIndicator 展示 6 阶段 | `test_app_has_second_pipeline_indicator_with_spark_title` + `test_stage_cn_has_all_spark_stages` |
| 3 | 404/422 错误能进入 ErrorDisplay | `test_spark_verify_catch_sets_error_for_display`——验证 catch 分支设置 `error: apiErr` |
| 4 | SQL 成功态阶段展示口径明确 | `test_handle_run_all_sets_success_stages` + `test_run_action_allows_partial_to_override_pipeline_stages` + `test_stage_cn_has_all_sql_stages` |
| 5 | pytest / ruff / tsc / npm build / git diff --check 全部通过 | Step 1-5 验收命令 |

---

## A/B/C 风险分类

### A 类（无阻断，可进入实施）

- **A1 测试框架复用**：所有新测试合并到已有 `test_frontend_smoke.py`——不引入新测试框架、不新增测试文件。使用 Python 标准库 `os` + `re` 做源码级断言。
- **A2 最小后端修改**：后端零改动——所有修改在 `frontend/src/` 和 `tests/test_frontend_smoke.py` 范围内。
- **A3 runAction 合并顺序安全**：当前所有 `onSuccess` 回调均不返回 `pipelineStages` / `pipelineError`，翻转顺序无行为变化。

### B 类（已知边界，需在实施中注意）

- **B1 源码级测试的脆弱性**：`test_frontend_smoke.py` 新增测试依赖字符串匹配——如果未来重构变量名（如 `handleSparkVerify` 重命名），测试会 FAIL。这是已知取舍——源码级测试比无测试好，且重构时 FAIL 的测试本身就是重构检查点。
- **B2 SQL 成功态阶段是前端合成的**：`handleRunAll` 成功路径中的 8 个 stages 是前端硬编码——它们不代表后端实际执行了哪几步，而是对"API 返回成功"的可视化翻译。如果未来 SQL Pipeline 阶段数量变化，需同步更新。
- **B3 无 UI 交互测试**：源码级测试验证了代码存在性和结构正确性，但不验证 DOM 交互（点击展开/折叠等）。真正的 UI 交互测试需 Playwright/Cypress——Phase 9C+。

### C 类（无——无阻断风险）

经过对 spec、已有代码和计划修改范围的完整审查，**未发现 C 类风险**：
- 所有修改在 `frontend/src/` 和 `tests/` 范围内——不动后端、不动 SQL Pipeline、不动 Spark Orchestrator
- `runAction` 合并顺序翻转已做安全分析——当前 4 个调用方均不受影响
- 新测试合并到已有文件——不引入新依赖
- 无外部依赖、无真实 LLM、无生产数据

---

## 残留风险（更新）

| 编号 | 说明 | 等级 | 处置 |
|:----:|------|:----:|------|
| R11 | ~~前端无自动化测试框架~~ | 已消除 → | Phase 9B 补充源码级回归测试，覆盖按钮/指示灯/错误路径/状态映射 |
| R15 | ~~SQL 成功态 pipeline_stages 为空~~ | 已消除 → | runAction 合并顺序翻转 + handleRunAll 成功路径注入全成功阶段 |
| R16 | 源码级测试的脆弱性——变量重命名导致 FAIL | C | 见 B1——接受取舍，FAIL 即为重构检查点 |
| R17 | 前端仍无 DOM 交互测试（展开/折叠/点击） | C | Phase 9C+ 引入 Playwright/Cypress |

---

## 非技术解释：为什么下一阶段先补自动化回归和阶段可观测性，而不是继续扩大功能

做一个比喻：**你在装修房子，水电已经走完了（Phase 9A），但你还没有装电表（自动化测试）和灯（阶段指示灯）。下一阶段不是继续加盖新房间（扩大功能），而是先装电表和灯——让已经建好的东西可以被看到、被验证。**

具体来说：

1. **自动化回归 = 电表**：现在每次改代码，依赖手动敲 `pytest` 和 `tsc` 来确认没搞坏东西。但前端的 Spark 按钮/指示灯这些交互逻辑，没有人帮你自动检查——只能靠眼睛看（手动冒烟）。补充自动化回归后，敲一条命令就能确认"Spark 按钮逻辑没坏、指示灯映射正确、错误处理到位"。

2. **阶段可观测性 = 灯**：现在 SQL Run-All 成功后，管线指示灯就消失了——你不知道它成功了还是根本没跑。修好之后，成功时指示灯变绿并显示"全部成功"——让 SQL 和 Spark 两个指示灯的行为一致、始终可见。

3. **不扩大功能是因为没必要急于扩张**：当前功能已经可以完成"写项目书 → 跑 SQL → 验 Spark"的核心链路。扩大功能（比如接入真实 LLM、生产环境部署）需要更扎实的基础——自动化测试和可观测性就是这个基础。

**一句话总结：先让已经建好的东西"可被看见、可被验证"，再继续往前盖。**

---

## 是否可进入实施阶段

**是。** 计划完整覆盖了 Phase 9B 的两个核心目标——R11（前端自动化回归）通过 12+ 源码级测试消除，R15（SQL 成功态不可见）通过 runAction 合并顺序翻转 + 成功 stages 注入消除。3 个 Task 边界清晰，每个 Task 有独立测试周期和提交点。无 C 类风险、无后端改动、无外部依赖。

预估总工作量：3 个 Task，每个 20-40 分钟，总计约 1.5-2 小时。

---

## 计划自审

**1. Spec 覆盖：**
- ✅ R11 消除 → Task 1 加测试
- ✅ R15 消除 → Task 2 修改合并顺序 + 成功 stages + STAGE_CN 补全
- ✅ 验收要求 1-5 → 5 个验收检查项 + 5 个用户要求与测试对照表
- ✅ 不改 SQL Pipeline → 零后端改动
- ✅ 不改 Spark Orchestrator → 零后端改动
- ✅ 测试不新增文件 → 全部合并到 `test_frontend_smoke.py`

**2. Placeholder 扫描：**
- 无 TBD / TODO / "implement later" / "add appropriate error handling"
- 每个 Step 有完整代码、精确命令、预期输出

**3. 类型一致性：**
- `StageInfo` 接口在 `PipelineStageIndicator.tsx` 定义，`handleRunAll` 中的 success stages 使用相同结构 `{ stage: string, status: 'ok' }`
- `AppState` 中的 `pipelineStages: StageInfo[]` 类型贯穿 `runAction` → `update` 整条链路
- STAGE_CN 新增的 `contract` / `package` 键在 success stages 中使用

**自审通过。**
