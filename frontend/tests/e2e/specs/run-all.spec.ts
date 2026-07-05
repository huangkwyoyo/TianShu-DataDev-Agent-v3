import { test, expect } from '@playwright/test';
import { waitForBackend } from '../helpers/api';
import { TEMPLATE_NAMES } from '../fixtures/developer-specs';

test.describe('模板选择路径', () => {
  test('选择汇总表模板后编辑器内容更新', async ({ page }) => {
    // 等待前端加载
    await waitForBackend(page);

    // 等待模板列表加载完成（TemplateSelector 在 mount 时 fetchTemplates）
    const templateButton = page.locator('.template-card', {
      hasText: TEMPLATE_NAMES.SUMMARY,
    });
    await expect(templateButton).toBeVisible({ timeout: 10000 });

    // 点击选择汇总表模板
    await templateButton.click();

    // 等待编辑器内容更新（fetchTemplate 异步完成后触发 re-render）
    const textarea = page.locator('.spec-editor textarea');
    await expect(textarea).not.toHaveValue('', { timeout: 10000 });

    // 断言：编辑器内容包含模板相关文本
    const value = await textarea.inputValue();
    expect(value.length).toBeGreaterThan(0);
    expect(value).toContain('汇总表');
  });
});
