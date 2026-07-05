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
 * 通过检查 PipelineStageIndicator 的状态圆点类名区分真实成功与管线错误：
 * - .dot-ok = 所有阶段均成功 → 返回 'success'
 * - .dot-error = 至少一个阶段失败 → 返回 'error'
 * - ErrorDisplay 出现（API 级错误，如 422）→ 返回 'error'
 *
 * @param page - Playwright Page 对象
 * @param timeout - 最长等待时间（ms），默认 30000
 * @returns 'success' 仅当管线全部成功（dot-ok），否则返回 'error'
 */
export async function waitForExecutionComplete(
  page: Page,
  timeout = 30000,
): Promise<'success' | 'error'> {
  // 等待任意结果容器出现（管线指示器或错误展示组件）
  await page.waitForFunction(
    () => {
      const hasRunAllStatus =
        document.querySelector('[data-testid="run-all-status"]');
      const hasSparkStatus =
        document.querySelector('[data-testid="spark-status"]');
      const hasError = document.querySelector('[data-testid="error-display"]');
      return hasRunAllStatus || hasSparkStatus || hasError;
    },
    { timeout },
  );

  // 优先检查 ErrorDisplay（API 级错误，如 422 Unprocessable Entity）
  if (await page.locator('[data-testid="error-display"]').isVisible()) {
    return 'error';
  }

  // 检查 Spark 管线状态——必须有 dot-ok 才算成功
  const sparkStatus = page.locator('[data-testid="spark-status"]');
  if (await sparkStatus.isVisible()) {
    return (await sparkStatus.locator('.status-dot.dot-ok').isVisible())
      ? 'success'
      : 'error';
  }

  // 检查 SQL 管线状态——必须有 dot-ok 才算成功
  const runAllStatus = page.locator('[data-testid="run-all-status"]');
  if (await runAllStatus.isVisible()) {
    return (await runAllStatus.locator('.status-dot.dot-ok').isVisible())
      ? 'success'
      : 'error';
  }

  // 不应到达此处（waitForFunction 确保至少一个容器可见）
  return 'error';
}
