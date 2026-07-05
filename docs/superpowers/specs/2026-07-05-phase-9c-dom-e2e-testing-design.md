# Phase 9C DOM 交互测试 — 设计文档

> 文档版本：2026-07-05 | 状态：设计定稿
> 关联计划：`docs/superpowers/plans/2026-07-05-phase-9c-e2e-implementation.md`（待生成）

## 1. 目标

为前端补齐真实 DOM 交互回归证据。当前前端有 13 个 TypeScript 源文件、零 DOM 测试框架，后端有 588 passed / 11 skipped。Phase 9B 已覆盖源码级冒烟测试（`test_frontend_smoke.py`），本 Phase 负责浏览器内交互验证。

## 2. 技术决策

| 项目 | 决策 | 排除方案 |
|------|------|----------|
| 框架 | Playwright | Cypress（单浏览器）、双框架分层（过度设计） |
| 后端 | 真实 FastAPI（`uvicorn`） | MSW mock（不测真实链路） |
| 架构模式 | 轻量直连（方案 A） | Page Object（过度设计）、组件测试+E2E 分层（配置复杂度高） |
| 测试数据 | 两类分离：模板名 + 硬编码 Spec | 全部硬编码（弱化模板路径覆盖）、用户录制（需 UI 稳定） |
| 等待策略 | `data-testid` 属性检测 | 纯文本选择器（不稳定） |
| 元素定位 | 4 个 `data-testid` + `getByRole`/`getByLabel` 混合 | 全 testid（入侵性强）、全文本（不稳定） |

## 3. 文件结构

```
frontend/
├── playwright.config.ts          # Playwright 全局配置
├── package.json                  # 新增 test:e2e 脚本 + @playwright/test 依赖
└── tests/
    └── e2e/
        ├── fixtures/
        │   └── developer-specs.ts    # 模板名称 + 手工输入 Spec 文本
        ├── helpers/
        │   └── api.ts                # 后端等待 + 执行完成检测
        └── specs/
            ├── run-all.spec.ts       # 模板选择 → Run-All → 成功
            ├── spark-verify.spec.ts  # Run-All 后 → Spark Verify → 6 阶段
            ├── error-display.spec.ts # 无效 Spec → Run-All → ErrorDisplay
            ├── spec-editor.spec.ts   # 手工 Spec → Run-All → 成功
            └── stage-indicator.spec.ts # 指示灯展开/收起
```

## 4. Playwright 配置

```typescript
// frontend/playwright.config.ts
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

### 启动约束

- `npm run test:e2e` 必须从**仓库根目录**执行（确保 `PYTHONPATH` 能解析 `tianshu_datadev` 包）
- `webServer` 命令不带 `cd ..`——启动路径由执行目录决定
- 首次启动 FastAPI + Spark 模块导入需要 ~15-25s，`timeout: 30000` 留足余量

### package.json 变更

```json
{
  "scripts": {
    "test:e2e": "npx playwright test",
    "test:e2e:headed": "npx playwright test --headed",
    "test:e2e:debug": "npx playwright test --debug"
  }
}
```

依赖安装：
```bash
cd frontend && npm install -D @playwright/test && npx playwright install chromium
```

## 5. 测试数据（fixtures）

两类分离，事实源不交叉：

```typescript
// frontend/tests/e2e/fixtures/developer-specs.ts

/** 模板路径用例——通过 TemplateSelector 真实选择 */
export const TEMPLATE_NAMES = {
  SUMMARY: '汇总表模板',
  DETAIL: '明细表模板',
} as const;

/** 手工编辑路径用例——在 SpecEditor 中粘贴 */
export const MANUAL_SUMMARY_SPEC = `
# 用户行为汇总表
## 数据源
- test_fact
## 输出
- 汇总表：每日用户行为汇总，按日期和事件类型分组，计数去重用户
`;

/** 无效 Spec——含不存在的数据源，触发运行时错误 */
export const INVALID_SPEC = `
# 无效项目书
## 数据源
- not_existing_table
## 输出
- 明细表：读取不存在的数据源
`;
```

**设计原则**：
- 模板路径用例只用 `TEMPLATE_NAMES`，通过 UI 选择器选择——模板文案变化时自动跟随
- 手工编辑路径用例用硬编码文本——固定输入保证可复现
- 不依赖后端 `/api/templates` 的返回格式

## 6. 测试规格（5 条路径）

### 6.1 Run-All 快乐路径 (`run-all.spec.ts`)

```
test('选择汇总表模板 → Run-All → 成功状态展示')
  given: 后端健康，前端已加载
  when:
    1. 在 TemplateSelector 中选择 "汇总表模板"
    2. 点击 Run-All 按钮
    3. 等待执行完成（最长 30s）
  then:
    - run-all-status 区域可见
    - 无 error-display 弹出
```

### 6.2 Spark Verify 快乐路径 (`spark-verify.spec.ts`)

```
test('Run-All 成功后 → Spark Verify → Spark 阶段展示')
  given: 已完成 Run-All（beforeEach 内执行模板选择 + Run-All）
  when:
    1. 点击 Spark 验证按钮
    2. 等待执行完成（最长 30s）
  then:
    - spark-status 区域可见
    - 阶段数为 6（Spark Plan / Compile / Validate / Compare / Physical Verify / Review）
    - 无 error-display 弹出
```

**注意**：Spark Verify 依赖 Run-All 完成态，每个 test 在 `beforeEach` 中独立完成 Run-All，不跨 test 共用浏览器状态。

### 6.3 错误展示路径 (`error-display.spec.ts`)

```
test('手工输入无效 Spec → Run-All → ErrorDisplay 展示错误')
  given: 后端健康，前端已加载
  when:
    1. 在 SpecEditor 中粘贴 INVALID_SPEC
    2. 点击 Run-All
    3. 等待执行完成（最长 15s）
  then:
    - error-display 区域可见
    - 错误信息包含表/数据源不存在或 RUNTIME_FAIL 的提示
```

### 6.4 自定义 Spec 编辑 (`spec-editor.spec.ts`)

```
test('手工输入有效 Spec → Run-All → 成功')
  given: 后端健康，前端已加载
  when:
    1. 在 SpecEditor 中粘贴 MANUAL_SUMMARY_SPEC
    2. 点击 Run-All
    3. 等待执行完成（最长 30s）
  then:
    - run-all-status 区域可见
    - 无 error-display 弹出
```

### 6.5 PipelineStageIndicator 展开/收起 (`stage-indicator.spec.ts`)

```
test('Run-All 成功后 → 点击 SQL 指示灯 → 阶段列表展开 → 再次点击收起')
  given: 已完成 Run-All（beforeEach 内执行）
  when:
    1. 确认 SQL PipelineStageIndicator 可见
    2. 点击指示灯区域
    3. 断言 stage-list 可见且展示阶段名称
    4. 再次点击
    5. 断言 stage-list 收起（不可见或高度为 0）
```

## 7. 辅助函数（helpers）

```typescript
// frontend/tests/e2e/helpers/api.ts
import type { Page } from '@playwright/test';

/**
 * 等待前端页面加载完成
 * webServer 已确保端口监听，这里做最后一次 page load 确认
 */
export async function waitForBackend(page: Page): Promise<void> {
  await page.goto('/');
  await page.waitForLoadState('networkidle');
}

/**
 * 等待 Run-All 或 Spark 验证完成
 * 通过 data-testid 检测成功态或错误态的出现
 *
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

## 8. 生产代码改动（测试桩）

共 4 个 `data-testid` 属性添加，改动量 4 行 JSX：

| 文件 | 位置 | 属性 | 用途 |
|------|------|------|------|
| `App.tsx` | Run-All 执行结果区域 | `data-testid="run-all-status"` | E2E 断言成功态 |
| `App.tsx` | Spark 验证结果区域 | `data-testid="spark-status"` | E2E 断言 Spark 完成态 |
| `ErrorDisplay.tsx` | 根容器 | `data-testid="error-display"` | E2E 断言错误展示 |
| `PipelineStageIndicator.tsx` | 阶段列表容器 | `data-testid="stage-list"` | E2E 断言展开/收起 |

**边界**：
- 不改组件接口、不改 props、不改业务逻辑、不改样式和文案
- 按钮/输入/模板选择优先使用 `getByRole`/`getByLabel`，不加 data-testid
- 如发现必须修改生产逻辑才能让测试通过，立即停止并按 A/B/C 分类报告

## 9. 验收标准

### E2E 测试

```bash
cd frontend && npx playwright test
```

预期：5 条路径全部通过

### 后端回归

```bash
python -m pytest tests/api/ tests/spark/ -q
```

预期：通过数 ≈ 588，无退化

### 静态检查

```bash
python -m ruff check src/
cd frontend && npx tsc --noEmit
cd frontend && npm run build
git diff --check
```

预期：四项全绿

## 10. 全局边界

| 边界 | 说明 |
|------|------|
| 不改后端业务逻辑 | 后端仅作为 E2E 的运行时依赖，零代码改动 |
| 不改 Spark/SQL Pipeline 语义 | Run-All 和 Spark Verify 的行为与当前生产完全一致 |
| 不引入新业务样本 | 复用现有 `test_fact` 表和后端 fixture |
| 不为测试改生产接口 | 不新增 API 端点、不修改 API 签名 |
| 不接入真实 LLM | E2E 测试路径不触发任何 LLM 调用 |
| 不启动生产写入 | 输出到临时目录 |
| 4 个 data-testid 为上限 | 不新增其他生产代码改动 |

## 11. 已知风险

| 风险 | 等级 | 缓解 |
|------|:---:|------|
| FastAPI 首次启动超时（PySpark 模块导入慢） | B | `timeout: 30000`，本地预启验证 |
| SQL 成功态阶段数量未定稿 | B | 断言用 testid 存在性而非阶段计数（Spark 6 阶段除外） |
| Windows 路径问题（`PYTHONPATH`、bash 转义） | C | `test:e2e` 限定从仓库根执行 |
| `reuseExistingServer: true` 时残留旧进程 | C | 文档说明 `npx playwright test --reuse-existing-server=false` 可强制重启 |
