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
