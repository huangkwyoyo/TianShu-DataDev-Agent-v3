import { test, expect } from '@playwright/test';
import { waitForBackend, waitForExecutionComplete } from '../helpers/api';
import { MANUAL_SUMMARY_SPEC } from '../fixtures/developer-specs';

test.describe('手工 Spec 编辑路径', () => {
  /**
   * E2E 环境中未配置 table_paths（DuckDB CSV 路径），Run-All 在 execute 阶段
   * 因 DuckDB 找不到 test_fact 表而返回 pipeline_error，无法成功完成。
   * 因此该测试暂被跳过——待 table_paths 注入后可恢复。
   *
   * Phase 9C review 发现"waitForExecutionComplete 假阳性"问题：修复前该测试
   * 因 dot-error 渲染（PipelineStageIndicator 仍会显示）而错误地返回 'success'，
   * 产生假性通过。修复后该测试如实失败，需要 table_paths 环境才能验证成功路径。
   */
  test.skip('手工输入有效 Spec → Run-All → 成功状态展示（需 table_paths 配置）', async ({ page }) => {
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
