import { expect, test } from '@playwright/test';

test.describe('模板列表恢复', () => {
  test('首次加载失败后打开菜单会重新请求', async ({ page }) => {
    let listRequests = 0;
    await page.route('**/api/templates', async (route) => {
      listRequests += 1;
      if (listRequests === 1) {
        await route.fulfill({
          status: 503,
          contentType: 'application/json',
          body: JSON.stringify({ message: 'backend starting' }),
        });
        return;
      }
      await route.continue();
    });

    await page.goto('/');
    await page.getByRole('button', { name: /Templates/ }).click();

    await expect(page.locator('.template-dropdown .tpl-name').first()).toBeVisible();
    await expect(page.getByText('模板加载失败')).toHaveCount(0);
    expect(listRequests).toBe(2);
  });
});
