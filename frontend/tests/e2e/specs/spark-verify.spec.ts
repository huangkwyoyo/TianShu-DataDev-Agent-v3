import { test, expect } from '@playwright/test';
import { waitForBackend, waitForExecutionComplete } from '../helpers/api';
import { MANUAL_SUMMARY_SPEC } from '../fixtures/developer-specs';

test.describe('Spark 验证路径', () => {
  test('Run-All 成功 → Spark Verify → Spark 阶段指示灯展示', async ({
    page,
  }) => {
    // 等待前端加载
    await waitForBackend(page);

    // Step 1: Run-All 成功——table_paths 已由后端自动发现 CSV fixture
    const textarea = page.locator('.spec-editor textarea');
    await textarea.fill(MANUAL_SUMMARY_SPEC.trim());
    await page.getByRole('button', { name: '全流程 Run-All' }).click();

    // 确认 Run-All 全部成功（dot-ok）
    const runAllStatus = page.locator('[data-testid="run-all-status"]');
    await expect(runAllStatus).toBeVisible({ timeout: 30000 });
    const runAllDot = runAllStatus.locator('.status-dot');
    await expect(runAllDot).toHaveClass(/dot-ok/);

    // Step 2: 点击 Spark 验证按钮——此时 request_id 有效，SparkOrchestrator 完整执行
    await page.getByRole('button', { name: 'Spark 验证' }).click();

    // 等待 Spark 验证完成
    const sparkResult = await waitForExecutionComplete(page, 60000);
    expect(sparkResult).toBe('success');

    // 断言：spark-status 可见，阶段指示灯为 dot-ok（ok+skipped 混合视为完成）
    const sparkStatus = page.locator('[data-testid="spark-status"]');
    await expect(sparkStatus).toBeVisible();
    const sparkDot = sparkStatus.locator('.status-dot');
    await expect(sparkDot).toHaveClass(/dot-ok/);

    // 断言：展开阶段列表可看到 6 个 Spark 阶段
    await sparkStatus.locator('.pipeline-trigger').click();
    const stageList = page.locator('[data-testid="stage-list"]');
    await expect(stageList).toBeVisible();
    const stageRows = stageList.locator('.pipeline-stage-row');
    await expect(stageRows).toHaveCount(6);
  });

  test('Spark验证 — dot-ok 确认 + review_ready 判定', async ({ page }) => {
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
    const sparkResult = await waitForExecutionComplete(page, 60000);
    expect(sparkResult).toBe('success');

    const sparkStatus = page.locator('[data-testid="spark-status"]');
    await expect(sparkStatus).toBeVisible();
    const sparkDot = sparkStatus.locator('.status-dot');
    await expect(sparkDot).toHaveClass(/dot-ok/);
  });
});
