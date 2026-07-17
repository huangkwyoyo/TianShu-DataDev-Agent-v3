# Phase 9C DOM E2E 测试 — 实施计划

> **状态：✅ 已完成（2026-07-05）**——6/6 Playwright 测试通过。

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 用 Playwright + 真实 FastAPI 后端为前端补齐 5 条 DOM 交互回归测试路径

**Architecture:** Playwright 轻量直连——`webServer` 同时启动 FastAPI + Vite dev server，测试文件直接操作 `page` 对象，通过 4 个 `data-testid` 属性定位关键 DOM 节点，无 Page Object 抽象层

**Tech Stack:** Playwright (~1.45+), TypeScript, FastAPI (uvicorn), Vite dev server

**设计文档：** `docs/superpowers/specs/2026-07-05-phase-9c-dom-e2e-testing-design.md`

## Global Constraints

- 不改后端业务逻辑——后端仅作为 E2E 运行时依赖，零代码改动
- 不改 Spark/SQL Pipeline 语义
- 不引入新业务样本——复用现有 `test_fact` 表
- 不为测试改生产接口——不新增 API 端点、不修改 API 签名
- 不接入真实 LLM——E2E 测试路径不触发任何 LLM 调用
- 不启动生产写入——输出到临时目录
- 4 个 data-testid 为上限——不新增其他生产代码改动
- 按钮/输入/模板选择优先使用 `getByRole`/`getByLabel`，不加 data-testid
- 如发现必须修改生产逻辑才能让测试通过，立即停止并按 A/B/C 分类报告
- 所有代码注释使用中文

## Pre-Flight 已知问题：模板表名不匹配

**现状：** 后端 `templates.py` 定义的 6 个模板（"汇总表"、"标签表"、"多步骤加工"等）全部引用生产表（`dwd.user_events`、`dwd.orders`、`dim.product` 等）。测试数据库只有 `test_fact` 和 `order_info`。直接选择模板后点击 Run-All 必定失败（表不存在）。

**决定（不新增后端模板）：** 模板路径测试调整为两步验证：
1. UI 验证——选择模板后断言编辑器内容已更新（证明 TemplateSelector 工作正常）
2. 数据流验证——用 `test_fact` 兼容的手工 Spec（`MANUAL_SUMMARY_SPEC`）执行 Run-All

`run-all.spec.ts` 只测步骤 1，步骤 2 由 `spec-editor.spec.ts` 覆盖。两个测试互补构成完整的"模板→编辑器→执行"证据链。

## 文件结构

```
frontend/
├── playwright.config.ts                       # 新建
├── package.json                               # 修改：依赖 + scripts
├── src/
│   ├── App.tsx                                # 修改：2 个 data-testid wrapper
│   └── components/
│       ├── ErrorDisplay.tsx                   # 修改：1 个 data-testid
│       └── PipelineStageIndicator.tsx         # 修改：1 个 data-testid + 1 个 testId prop
└── tests/
    └── e2e/
        ├── fixtures/
        │   └── developer-specs.ts             # 新建
        ├── helpers/
        │   └── api.ts                         # 新建
        └── specs/
            ├── run-all.spec.ts               # 新建
            ├── spark-verify.spec.ts          # 新建
            ├── error-display.spec.ts         # 新建
            ├── spec-editor.spec.ts           # 新建
            └── stage-indicator.spec.ts       # 新建
```

---

### Task 1: 基础设施——安装 Playwright + 配置 + fixtures + helpers

**Files:**
- Create: `frontend/playwright.config.ts`
- Create: `frontend/tests/e2e/fixtures/developer-specs.ts`
- Create: `frontend/tests/e2e/helpers/api.ts`
- Modify: `frontend/package.json`

**Interfaces:**
- Consumes: 设计文档 §4 Playwright 配置、§5 测试数据、§7 辅助函数
- Produces:
  - `playwright.config.ts` — `defineConfig({...})` 默认导出
  - `TEMPLATE_NAMES: { readonly SUMMARY: string }` — 模板名称常量
  - `MANUAL_SUMMARY_SPEC: string` — 有效手工 Spec（引用 test_fact）
  - `INVALID_SPEC: string` — 无效 Spec（引用不存在表）
  - `waitForBackend(page: Page): Promise<void>` — 等待前端加载
  - `waitForExecutionComplete(page: Page, timeout?: number): Promise<'success' | 'error'>` — 等待 Run-All/Spark 完成

- [ ] **Step 1: 安装 Playwright 依赖**

```bash
cd frontend && npm install -D @playwright/test && npx playwright install chromium
```

预期：`package.json` 中新增 `@playwright/test` devDependency，`package-lock.json` 更新

- [ ] **Step 2: 更新 package.json 添加 E2E 测试脚本**

在 `frontend/package.json` 的 `scripts` 块中替换 `test` 脚本，新增 `test:e2e` 系列：

```json
{
  "scripts": {
    "dev": "vite",
    "build": "tsc -b && vite build",
    "preview": "vite preview",
    "test": "echo \"No frontend unit tests configured yet\" && exit 0",
    "test:e2e": "npx playwright test",
    "test:e2e:headed": "npx playwright test --headed",
    "test:e2e:debug": "npx playwright test --debug"
  }
}
```

**注意：** `test:e2e` 必须从仓库根目录执行（`PYTHONPATH` 需解析 `tianshu_datadev` 包）。`package.json` 中不写 `cd ..`——由执行者保证工作目录正确。

- [ ] **Step 3: 创建 playwright.config.ts**

创建 `frontend/playwright.config.ts`：

```typescript
import { defineConfig } from '@playwright/test';

export default defineConfig({
  testDir: './tests/e2e/specs',
  timeout: 60000,
  expect: { timeout: 10000 },
  fullyParallel: false,
  retries: 0,
  use: {
    baseURL: 'http://127.0.0.1:5173',
    trace: 'retain-on-failure',
    screenshot: 'only-on-failure',
  },
  webServer: [
    {
      command: 'python -m uvicorn tianshu_datadev.api.main:app --host 127.0.0.1 --port 8000',
      port: 8000,
      timeout: 30000,
      reuseExistingServer: true,
    },
    {
      command: 'npm run dev -- --host 127.0.0.1 --port 5173',
      port: 5173,
      timeout: 15000,
      reuseExistingServer: true,
    },
  ],
});
```

- [ ] **Step 4: 验证配置可被 Playwright 识别**

```bash
cd frontend && npx playwright test --list
```

预期：`No tests found`（尚无测试文件，但配置语法正确）

- [ ] **Step 5: 创建 fixtures/developer-specs.ts**

创建 `frontend/tests/e2e/fixtures/developer-specs.ts`：

```typescript
/**
 * Phase 9C E2E 测试数据——两类分离：
 * 1. 模板路径用例——通过 TemplateSelector 真实选择（仅用于 UI 验证）
 * 2. 手工编辑路径用例——在 SpecEditor 中粘贴（用于数据流验证）
 *
 * 注意：后端生产模板引用 dwd.* 表，测试数据库只有 test_fact。
 * 模板路径测试只验证 UI 选择行为，不执行 Run-All。
 */

/** 模板名称——与后端 templates.py 中的 name 字段严格一致 */
export const TEMPLATE_NAMES = {
  /** 汇总表模板——单表聚合，后端 template_id: tpl_aggregation */
  SUMMARY: '汇总表',
} as const;

/**
 * 有效手工 Spec——引用 test_fact 表，确保在测试数据库中可执行
 * 对应后端 tests/fixtures/sql/test_fact.csv
 */
export const MANUAL_SUMMARY_SPEC = `
# 用户行为汇总表
## 数据源
- test_fact
## 输出
- 汇总表：每日用户行为汇总，按日期和事件类型分组，计数去重用户
`;

/**
 * 无效 Spec——引用不存在的数据源，触发 RUNTIME_FAIL 错误
 * 用于错误展示路径测试
 */
export const INVALID_SPEC = `
# 无效项目书
## 数据源
- not_existing_table
## 输出
- 明细表：读取不存在的数据源
`;
```

- [ ] **Step 6: 创建 helpers/api.ts**

创建 `frontend/tests/e2e/helpers/api.ts`：

```typescript
import type { Page } from '@playwright/test';

/**
 * 等待前端页面加载完成。
 * webServer 已确保端口在监听，这里做最后一次 page load 确认。
 */
export async function waitForBackend(page: Page): Promise<void> {
  await page.goto('/');
  await page.waitForLoadState('networkidle');
}

/**
 * 等待 Run-All 或 Spark 验证执行完成。
 * 通过 data-testid 检测成功态或错误态元素的出现。
 *
 * @param page - Playwright Page 对象
 * @param timeout - 最长等待时间（ms），默认 30000
 * @returns 'success' 如果出现 run-all-status 或 spark-status，'error' 如果出现 error-display
 */
export async function waitForExecutionComplete(
  page: Page,
  timeout = 30000,
): Promise<'success' | 'error'> {
  await page.waitForFunction(
    () => {
      const hasSuccess =
        document.querySelector('[data-testid="run-all-status"]') ||
        document.querySelector('[data-testid="spark-status"]');
      const hasError = document.querySelector('[data-testid="error-display"]');
      return hasSuccess || hasError;
    },
    { timeout },
  );
  const hasError = await page.$('[data-testid="error-display"]');
  return hasError ? 'error' : 'success';
}
```

- [ ] **Step 7: 验证 fixtures/helpers 无 TypeScript 错误**

```bash
cd frontend && npx tsc --noEmit
```

预期：0 错误（`tests/e2e/` 目录如果在 `tsconfig.json` 的 `include` 范围外则无影响；如果在范围内则通过）

- [ ] **Step 8: 提交**

```bash
git add frontend/package.json frontend/package-lock.json frontend/playwright.config.ts frontend/tests/e2e/
git commit -m "feat(e2e): Phase 9C 基础设施——Playwright 配置、fixtures、helpers

- 安装 @playwright/test + Chromium 浏览器
- playwright.config.ts: webServer 启动 FastAPI + Vite
- fixtures/developer-specs.ts: 模板名称 + 手工 Spec（两类分离）
- helpers/api.ts: waitForBackend + waitForExecutionComplete

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 2: 生产代码改动——4 个 data-testid 属性

**Files:**
- Modify: `frontend/src/App.tsx` — 2 个 `data-testid` wrapper
- Modify: `frontend/src/components/ErrorDisplay.tsx` — 1 个 `data-testid`
- Modify: `frontend/src/components/PipelineStageIndicator.tsx` — 1 个 `data-testid` + 1 个 `testId` prop

**Interfaces:**
- Consumes: 设计文档 §8 生产代码改动表格
- Produces:
  - `App.tsx` JSX: `data-testid="run-all-status"` 和 `data-testid="spark-status"` 可用
  - `ErrorDisplay.tsx` root div: `data-testid="error-display"` 可用
  - `PipelineStageIndicator.tsx`: `testId?: string` prop → root div `data-testid={testId}`
  - `PipelineStageIndicator.tsx` dropdown div: `data-testid="stage-list"` 可用

**边界**：不改组件接口签名（仅追加可选 prop）、不改 props 类型（仅扩展）、不改业务逻辑、不改样式和文案

- [ ] **Step 1: PipelineStageIndicator 添加 testId prop + stage-list data-testid**

修改 `frontend/src/components/PipelineStageIndicator.tsx`：

**Props 接口——追加 `testId` 可选字段：**

找到 `interface Props` 块，在 `title?: string;` 之后追加：

```typescript
  /** Phase 9C: E2E 测试定位——根元素 data-testid 属性 */
  testId?: string;
```

**根 div——添加 `data-testid`：**

找到 `<div className="pipeline-indicator" ref={ref}>`，改为：

```tsx
    <div className="pipeline-indicator" ref={ref} data-testid={testId}>
```

**下拉列表 div——添加固定 testid：**

找到 `<div className="pipeline-dropdown">`，改为：

```tsx
        <div className="pipeline-dropdown" data-testid="stage-list">
```

- [ ] **Step 2: App.tsx 传递 testId 给两个 PipelineStageIndicator**

修改 `frontend/src/App.tsx`，找到 header 中的两个 `<PipelineStageIndicator>` 使用处，添加 `testId` prop：

```tsx
          <PipelineStageIndicator
            stages={state.pipelineStages}
            error={state.pipelineError}
            testId="run-all-status"
          />
          <PipelineStageIndicator
            stages={state.sparkStages}
            error={null}
            title="Spark 管线"
            testId="spark-status"
          />
```

**设计说明**：`data-testid` 放在 PipelineStageIndicator 根元素上。该组件在 `stages.length === 0 && !error` 时返回 `null`，因此 `waitForExecutionComplete` 中的 `document.querySelector('[data-testid="run-all-status"]')` 只会在 Run-All 成功后才匹配到元素——无需额外条件渲染。

- [ ] **Step 3: ErrorDisplay 添加 data-testid**

修改 `frontend/src/components/ErrorDisplay.tsx`，找到根 `<div className="error-display" style={{ borderColor }}>`，改为：

```tsx
    <div className="error-display" style={{ borderColor }} data-testid="error-display">
```

- [ ] **Step 4: 验证——TypeScript 编译 + 构建**

```bash
cd frontend && npx tsc --noEmit
cd frontend && npm run build
```

预期：0 错误，构建成功

- [ ] **Step 5: 验证——后端回归无退化**

```bash
python -m ruff check src/
python -m pytest tests/api/ tests/spark/ -q
```

预期：ruff 零告警，通过数 ≈ 588，无退化

- [ ] **Step 6: 提交**

```bash
git add frontend/src/App.tsx frontend/src/components/ErrorDisplay.tsx frontend/src/components/PipelineStageIndicator.tsx
git commit -m "feat(e2e): Phase 9C 生产代码添加 4 个 data-testid 测试桩

- PipelineStageIndicator: 新增可选 testId prop → root data-testid
- PipelineStageIndicator: dropdown div 添加 data-testid='stage-list'
- App.tsx: SQL 指示灯 testId='run-all-status', Spark 指示灯 testId='spark-status'
- ErrorDisplay.tsx: 根容器添加 data-testid='error-display'

边界：不改接口/逻辑/样式/文案，仅追加 data-testid 属性

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 3: 正向路径 E2E 测试——run-all.spec.ts + spec-editor.spec.ts

**Files:**
- Create: `frontend/tests/e2e/specs/run-all.spec.ts`
- Create: `frontend/tests/e2e/specs/spec-editor.spec.ts`

**Interfaces:**
- Consumes: `waitForBackend`、`waitForExecutionComplete`（helpers/api.ts）
- Consumes: `TEMPLATE_NAMES`、`MANUAL_SUMMARY_SPEC`（fixtures/developer-specs.ts）
- Consumes: `data-testid="run-all-status"`、`data-testid="error-display"`（Task 2）
- Produces: 2 个可独立执行的 E2E 测试

**已知约束**：后端模板引用生产表（`dwd.*`），测试数据库只有 `test_fact`。`run-all.spec.ts` 的模板选择测试仅验证 UI 加载行为（不触发 Run-All）。Run-All 数据流验证由 `spec-editor.spec.ts` 通过手工 Spec 覆盖。

- [ ] **Step 1: 创建 run-all.spec.ts**

创建 `frontend/tests/e2e/specs/run-all.spec.ts`：

```typescript
import { test, expect } from '@playwright/test';
import { waitForBackend } from '../helpers/api';
import { TEMPLATE_NAMES } from '../fixtures/developer-specs';

test.describe('模板选择路径', () => {
  test('选择汇总表模板后编辑器内容更新', async ({ page }) => {
    // 等待前端加载
    await waitForBackend(page);

    // 等待模板列表加载完成（TemplateSelector 在 mount 时 fetchTemplates）
    const templateButton = page.locator('.template-card', {
      hasText: TEMPLATE_NAMES.SUMMARY,
    });
    await expect(templateButton).toBeVisible({ timeout: 10000 });

    // 点击选择汇总表模板
    await templateButton.click();

    // 断言：编辑器 textarea 中出现了内容（不再是空）
    const textarea = page.locator('.spec-editor textarea');
    const value = await textarea.inputValue();
    expect(value.length).toBeGreaterThan(0);
    expect(value).toContain('汇总表');
  });
});
```

- [ ] **Step 2: 创建 spec-editor.spec.ts**

创建 `frontend/tests/e2e/specs/spec-editor.spec.ts`：

```typescript
import { test, expect } from '@playwright/test';
import { waitForBackend, waitForExecutionComplete } from '../helpers/api';
import { MANUAL_SUMMARY_SPEC } from '../fixtures/developer-specs';

test.describe('手工 Spec 编辑路径', () => {
  test('手工输入有效 Spec → Run-All → 成功状态展示', async ({ page }) => {
    // 等待前端加载
    await waitForBackend(page);

    // 在 SpecEditor 的 textarea 中粘贴手工 Spec
    const textarea = page.locator('.spec-editor textarea');
    await textarea.fill(MANUAL_SUMMARY_SPEC.trim());

    // 点击 Run-All 按钮
    await page.getByRole('button', { name: '全流程 Run-All' }).click();

    // 等待执行完成（最长 30s）
    const result = await waitForExecutionComplete(page, 30000);
    expect(result).toBe('success');

    // 断言：SQL 管线成功指示灯可见
    await expect(
      page.locator('[data-testid="run-all-status"]'),
    ).toBeVisible();
  });
});
```

- [ ] **Step 3: 验证 run-all.spec.ts 通过**

从仓库根目录执行：

```bash
cd frontend && npx playwright test tests/e2e/specs/run-all.spec.ts
```

预期：1/1 通过

- [ ] **Step 4: 验证 spec-editor.spec.ts 通过**

```bash
cd frontend && npx playwright test tests/e2e/specs/spec-editor.spec.ts
```

预期：1/1 通过

- [ ] **Step 5: TypeScript 编译验证**

```bash
cd frontend && npx tsc --noEmit
```

预期：0 错误

- [ ] **Step 6: 提交**

```bash
git add frontend/tests/e2e/specs/run-all.spec.ts frontend/tests/e2e/specs/spec-editor.spec.ts
git commit -m "feat(e2e): Phase 9C 正向路径测试——模板选择 + 手工 Spec Run-All

- run-all.spec.ts: 选择汇总表模板 → 编辑器内容更新（UI 验证）
- spec-editor.spec.ts: 手工输入 test_fact Spec → Run-All → 成功态

已知约束：模板引用 dwd.* 表，模板路径只做 UI 验证，Run-All 数据流由手工路径覆盖

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 4: 异常+验证路径 E2E 测试——error-display + spark-verify + stage-indicator

**Files:**
- Create: `frontend/tests/e2e/specs/error-display.spec.ts`
- Create: `frontend/tests/e2e/specs/spark-verify.spec.ts`
- Create: `frontend/tests/e2e/specs/stage-indicator.spec.ts`

**Interfaces:**
- Consumes: `waitForBackend`、`waitForExecutionComplete`（helpers/api.ts）
- Consumes: `INVALID_SPEC`、`MANUAL_SUMMARY_SPEC`（fixtures/developer-specs.ts）
- Consumes: `data-testid="error-display"`、`data-testid="spark-status"`、`data-testid="stage-list"`（Task 2）
- Produces: 3 个可独立执行的 E2E 测试

**前置条件**：Task 3 已验证 Run-All 基本流程可工作。spark-verify 和 stage-indicator 依赖 Run-All 预完成。

- [ ] **Step 1: 创建 error-display.spec.ts**

创建 `frontend/tests/e2e/specs/error-display.spec.ts`：

```typescript
import { test, expect } from '@playwright/test';
import { waitForBackend, waitForExecutionComplete } from '../helpers/api';
import { INVALID_SPEC } from '../fixtures/developer-specs';

test.describe('错误展示路径', () => {
  test('手工输入无效 Spec → Run-All → ErrorDisplay 展示错误', async ({
    page,
  }) => {
    // 等待前端加载
    await waitForBackend(page);

    // 在 SpecEditor 中粘贴无效 Spec（引用不存在的表）
    const textarea = page.locator('.spec-editor textarea');
    await textarea.fill(INVALID_SPEC.trim());

    // 点击 Run-All
    await page.getByRole('button', { name: '全流程 Run-All' }).click();

    // 等待执行完成（错误场景下后端返回更快，最长 15s）
    const result = await waitForExecutionComplete(page, 15000);
    expect(result).toBe('error');

    // 断言：错误展示区域可见
    const errorDisplay = page.locator('[data-testid="error-display"]');
    await expect(errorDisplay).toBeVisible();

    // 断言：错误信息包含表/数据源不存在相关提示
    const errorText = await errorDisplay.textContent();
    expect(errorText).toMatch(/不存在|RUNTIME_FAIL|not_existing|not found/i);
  });
});
```

- [ ] **Step 2: 创建 spark-verify.spec.ts**

创建 `frontend/tests/e2e/specs/spark-verify.spec.ts`：

```typescript
import { test, expect } from '@playwright/test';
import { waitForBackend, waitForExecutionComplete } from '../helpers/api';
import { MANUAL_SUMMARY_SPEC } from '../fixtures/developer-specs';

test.describe('Spark 验证路径', () => {
  test('Run-All 成功后 → Spark Verify → Spark 阶段展示', async ({
    page,
  }) => {
    // 等待前端加载
    await waitForBackend(page);

    // Step 1: 完成 Run-All
    const textarea = page.locator('.spec-editor textarea');
    await textarea.fill(MANUAL_SUMMARY_SPEC.trim());
    await page.getByRole('button', { name: '全流程 Run-All' }).click();
    const runResult = await waitForExecutionComplete(page, 30000);
    expect(runResult).toBe('success');

    // 确认 SQL 指示灯可见（Run-All 成功）
    await expect(
      page.locator('[data-testid="run-all-status"]'),
    ).toBeVisible();

    // Step 2: 点击 Spark 验证按钮
    await page.getByRole('button', { name: 'Spark 验证' }).click();

    // 等待 Spark 验证完成
    const sparkResult = await waitForExecutionComplete(page, 30000);
    expect(sparkResult).toBe('success');

    // 断言：Spark 指示灯可见
    const sparkStatus = page.locator('[data-testid="spark-status"]');
    await expect(sparkStatus).toBeVisible();

    // 断言：Spark 阶段数为 6
    // （Spark Plan / Compile / Validate / Compare / Physical Verify / Review）
    const sparkStageRows = sparkStatus.locator('.pipeline-stage-row');
    await expect(sparkStageRows).toHaveCount(6);
  });
});
```

- [ ] **Step 3: 创建 stage-indicator.spec.ts**

创建 `frontend/tests/e2e/specs/stage-indicator.spec.ts`：

```typescript
import { test, expect } from '@playwright/test';
import { waitForBackend, waitForExecutionComplete } from '../helpers/api';
import { MANUAL_SUMMARY_SPEC } from '../fixtures/developer-specs';

test.describe('PipelineStageIndicator 交互', () => {
  test('Run-All 成功后 → 点击指示灯展开阶段列表 → 再次点击收起', async ({
    page,
  }) => {
    // 等待前端加载
    await waitForBackend(page);

    // 完成 Run-All
    const textarea = page.locator('.spec-editor textarea');
    await textarea.fill(MANUAL_SUMMARY_SPEC.trim());
    await page.getByRole('button', { name: '全流程 Run-All' }).click();
    const runResult = await waitForExecutionComplete(page, 30000);
    expect(runResult).toBe('success');

    // 确认 SQL 指示灯可见
    const sqlIndicator = page.locator('[data-testid="run-all-status"]');
    await expect(sqlIndicator).toBeVisible();

    // 点击指示灯触发按钮展开
    const triggerButton = sqlIndicator.locator('.pipeline-trigger');
    await triggerButton.click();

    // 断言：阶段列表展开，显示阶段名称
    const stageList = page.locator('[data-testid="stage-list"]');
    await expect(stageList).toBeVisible();
    // 至少有一个阶段行（Run-All 成功后应有 8 个阶段）
    const stageRows = stageList.locator('.pipeline-stage-row');
    await expect(stageRows.first()).toBeVisible();

    // 再次点击收起
    await triggerButton.click();

    // 断言：阶段列表收起（不可见）
    await expect(stageList).not.toBeVisible();
  });
});
```

- [ ] **Step 4: 验证 error-display.spec.ts 通过**

```bash
cd frontend && npx playwright test tests/e2e/specs/error-display.spec.ts
```

预期：1/1 通过

- [ ] **Step 5: 验证 spark-verify.spec.ts 通过**

```bash
cd frontend && npx playwright test tests/e2e/specs/spark-verify.spec.ts
```

预期：1/1 通过

- [ ] **Step 6: 验证 stage-indicator.spec.ts 通过**

```bash
cd frontend && npx playwright test tests/e2e/specs/stage-indicator.spec.ts
```

预期：1/1 通过

- [ ] **Step 7: TypeScript 编译验证**

```bash
cd frontend && npx tsc --noEmit
```

预期：0 错误

- [ ] **Step 8: 提交**

```bash
git add frontend/tests/e2e/specs/error-display.spec.ts frontend/tests/e2e/specs/spark-verify.spec.ts frontend/tests/e2e/specs/stage-indicator.spec.ts
git commit -m "feat(e2e): Phase 9C 异常+验证路径测试——错误展示、Spark Verify、指示灯交互

- error-display.spec.ts: 无效 Spec → Run-All → ErrorDisplay 可见 + 错误信息断言
- spark-verify.spec.ts: Run-All 完成 → Spark 验证 → Spark 指示灯可见
- stage-indicator.spec.ts: 点击指示灯展开/收起阶段列表

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 5: 全量回归验收 + 文档更新

**Files:**
- Modify: `docs/current-state-and-verification-status.md` — 添加 Phase 9C 行 + 更新残留风险
- Modify: `.superpowers/sdd/progress.md` — SDD 进度记录（如使用 SDD 执行）

**Interfaces:**
- Consumes: Task 1-4 完成态（全量 E2E 可运行）
- Produces: 更新后的状态仪表盘文档

- [ ] **Step 1: 全量 E2E 测试**

```bash
cd frontend && npx playwright test
```

预期：5/5 全部通过

- [ ] **Step 2: 后端全量回归**

```bash
python -m pytest tests/api/ tests/spark/ -q
```

预期：通过数 ≈ 588，零退化

- [ ] **Step 3: 前端冒烟测试**

```bash
python -m pytest tests/frontend/ -q
```

预期：23/23 通过（Phase 9B 已建立）

- [ ] **Step 4: 静态检查**

```bash
python -m ruff check src/
cd frontend && npx tsc --noEmit
cd frontend && npm run build
git diff --check
```

预期：四项全绿

- [ ] **Step 5: 更新项目状态仪表盘**

修改 `docs/current-state-and-verification-status.md`：

**Phase 进度矩阵——添加 Phase 9C 行：**

在 "9B-P0" 行之后追加：

```markdown
| 9C | DOM E2E 交互测试 | ✅ | ✅ | ✅ | 5/5 Playwright 全通过，2026-07-05 |
```

**残留风险——添加 R11 更新备注：**

将 R11 行更新为：

```markdown
| R11 | ~~前端无自动化测试框架~~ | 已消除 | Phase 9B 源码级 + Phase 9C Playwright E2E |
```

**下一步方向——移除 Phase 9C 条目：**

将 §5 中的 "Phase 9C+——DOM 交互测试（Playwright/Cypress）" 改为已完成状态。

**测试基线——更新：**

将测试基线更新为新的通过数（含 E2E 5/5）。

- [ ] **Step 6: SDD 进度记录（如适用）**

如果使用 SDD 执行，在 `.superpowers/sdd/progress.md` 中添加 Phase 9C Progress 节。

- [ ] **Step 7: 提交**

```bash
git add docs/current-state-and-verification-status.md
git commit -m "docs: Phase 9C DOM E2E 测试完成——状态仪表盘更新

- Phase 进度矩阵新增 9C 行
- R11 更新为源码级 + Playwright E2E 双重消除
- 测试基线更新

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## 验收标准（全量）

| 检查项 | 命令 | 预期 |
|--------|------|------|
| E2E 全量 | `cd frontend && npx playwright test` | 5/5 通过 |
| 后端回归 | `python -m pytest tests/api/ tests/spark/ -q` | ≈588 passed |
| 前端冒烟 | `python -m pytest tests/frontend/ -q` | 23 passed |
| Ruff | `python -m ruff check src/` | 0 告警 |
| TypeScript | `cd frontend && npx tsc --noEmit` | 0 错误 |
| 构建 | `cd frontend && npm run build` | 成功 |
| Git diff | `git diff --check` | 无空白告警 |

## 已知风险

| 风险 | 等级 | 缓解 |
|------|:---:|------|
| FastAPI 首次启动超时 | B | `timeout: 30000`，本地预启验证 |
| 模板引用 `dwd.*` 表，测试 DB 无此表 | **已处置** | 模板路径只做 UI 验证；数据流用 `test_fact` 手工 Spec |
| Spark Verify 耗时较长（可能 >30s） | C | 首次运行验证后调整 timeout |
| Windows 上 `webServer` 进程残留 | C | 文档说明手动 kill 端口 5173/8000 |
