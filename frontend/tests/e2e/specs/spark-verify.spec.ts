import { test, expect } from '@playwright/test';
import { waitForBackend, waitForExecutionComplete } from '../helpers/api';
import { MANUAL_SUMMARY_SPEC } from '../fixtures/developer-specs';

test.describe('Spark 验证路径', () => {
  test('Run-All 完成后（含管线错误）→ Spark Verify → 后端返回 422，ErrorDisplay 展示错误码', async ({
    page,
  }) => {
    // 等待前端加载
    await waitForBackend(page);

    // Step 1: 完成 Run-All（E2E 环境中无 table_paths，execute 阶段因 DuckDB 找不到 test_fact 表而失败，
    //          但 parser/build/compile 均成功，request_id 被正确设置）
    const textarea = page.locator('.spec-editor textarea');
    await textarea.fill(MANUAL_SUMMARY_SPEC.trim());
    await page.getByRole('button', { name: '全流程 Run-All' }).click();

    // 等待 PipelineStageIndicator 出现（管线执行完毕，含 pipeline_error）
    const runAllStatus = page.locator('[data-testid="run-all-status"]');
    await expect(runAllStatus).toBeVisible({ timeout: 30000 });

    // Step 2: 点击 Spark 验证按钮（此时 request_id 已设置，按钮处于启用态）
    await page.getByRole('button', { name: 'Spark 验证' }).click();

    // 等待 API 响应——后端因缺少 data_transform_contract 返回 422，
    // 前端 API 客户端抛出异常，触发 ErrorDisplay
    const sparkResult = await waitForExecutionComplete(page, 30000);
    expect(sparkResult).toBe('error');

    // 断言：ErrorDisplay 可见且包含 SPARK_ARTIFACTS_INCOMPLETE 错误码
    const errorDisplay = page.locator('[data-testid="error-display"]');
    await expect(errorDisplay).toBeVisible();
    const errorText = await errorDisplay.textContent();
    expect(errorText).toMatch(/SPARK_ARTIFACTS_INCOMPLETE|spark.*contract/i);
  });

  /**
   * Spark 验证成功路径测试。
   *
   * 当前 E2E 环境中未配置 table_paths（DuckDB CSV 路径），Run-All 在 execute 阶段
   * 因找不到 test_fact 表而返回 pipeline_error，导致 request_id 虽被设置但管线状态
   * 非完成态（dot-error），无法进入 Spark 验证成功路径（SPARK_VERIFIED）。
   *
   * 待后端配置 table_paths 后，此测试应：
   * 1. Run-All 全部成功（dot-ok）
   * 2. 点击 Spark 验证
   * 3. 等待 spark-status 出现并检查 dot-ok
   * 4. 断言 Spark 验证通过
   */
  test.skip('Spark验证 — 成功路径（需 table_paths 配置）', async ({ page }) => {
    await waitForBackend(page);

    const textarea = page.locator('.spec-editor textarea');
    await textarea.fill(MANUAL_SUMMARY_SPEC.trim());
    await page.getByRole('button', { name: '全流程 Run-All' }).click();

    // 等待 SQL 管线全部成功（dot-ok）
    const runAllStatus = page.locator('[data-testid="run-all-status"]');
    await expect(runAllStatus).toBeVisible({ timeout: 30000 });
    const runAllDot = runAllStatus.locator('.status-dot');
    await expect(runAllDot).toHaveClass(/dot-ok/);

    // 点击 Spark 验证按钮
    await page.getByRole('button', { name: 'Spark 验证' }).click();

    // 等待 Spark 验证完成并检查成功状态
    const sparkResult = await waitForExecutionComplete(page, 30000);
    expect(sparkResult).toBe('success');

    const sparkStatus = page.locator('[data-testid="spark-status"]');
    await expect(sparkStatus).toBeVisible();
    const sparkDot = sparkStatus.locator('.status-dot');
    await expect(sparkDot).toHaveClass(/dot-ok/);
  });
});
