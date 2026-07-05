import { test, expect } from '@playwright/test';
import { waitForBackend, waitForExecutionComplete } from '../helpers/api';
import { MANUAL_SUMMARY_SPEC } from '../fixtures/developer-specs';

test.describe('手工 Spec 编辑路径', () => {
  /** Phase 9C-R16：table_paths 已由后端 CSV fixture 自动发现补齐，Run-All 可成功执行。 */
  test('手工输入有效 Spec → Run-All → 成功状态展示', async ({ page }) => {
    await waitForBackend(page);

    const textarea = page.locator('.spec-editor textarea');
    await textarea.fill(MANUAL_SUMMARY_SPEC.trim());

    await page.getByRole('button', { name: '全流程 Run-All' }).click();

    const result = await waitForExecutionComplete(page, 30000);
    expect(result).toBe('success');

    await expect(
      page.locator('[data-testid="run-all-status"]'),
    ).toBeVisible();
  });
});
