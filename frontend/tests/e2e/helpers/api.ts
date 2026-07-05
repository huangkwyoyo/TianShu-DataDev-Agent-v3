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
