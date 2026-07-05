import { test, expect } from '@playwright/test';
import { waitForBackend } from '../helpers/api';
import { INVALID_SPEC } from '../fixtures/developer-specs';

test.describe('错误展示路径', () => {
  test('手工输入无效 Spec → Run-All → PipelineStageIndicator 展示错误', async ({
    page,
  }) => {
    // 等待前端加载
    await waitForBackend(page);

    // 在 SpecEditor 中粘贴无效 Spec（不包含 fenced code block，触发 Parser 级错误）
    const textarea = page.locator('.spec-editor textarea');
    await textarea.fill(INVALID_SPEC.trim());

    // 点击 Run-All
    await page.getByRole('button', { name: '全流程 Run-All' }).click();

    // 等待 PipelineStageIndicator 出现（pipeline 完成后 stages 更新）
    const runAllStatus = page.locator('[data-testid="run-all-status"]');
    await expect(runAllStatus).toBeVisible({ timeout: 15000 });

    // 断言：状态圆点为 error 态（dot-error class）
    const statusDot = runAllStatus.locator('.status-dot');
    await expect(statusDot).toHaveClass(/dot-error/);

    // 断言：摘要文本包含"失败"字样（解析失败/执行失败等）
    const summaryText = runAllStatus.locator('.pipeline-summary-text');
    await expect(summaryText).toContainText('失败');
  });
});
