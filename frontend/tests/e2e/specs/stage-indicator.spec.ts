import { test, expect } from '@playwright/test';
import { waitForBackend } from '../helpers/api';
import { MANUAL_SUMMARY_SPEC } from '../fixtures/developer-specs';

test.describe('PipelineStageIndicator 交互', () => {
  test('Run-All 完成后 → 点击指示灯展开阶段列表 → 再次点击收起', async ({
    page,
  }) => {
    // 等待前端加载
    await waitForBackend(page);

    // 完成 Run-All（pipeline 执行完成后 stages 被设置，PipelineStageIndicator 渲染）
    const textarea = page.locator('.spec-editor textarea');
    await textarea.fill(MANUAL_SUMMARY_SPEC.trim());
    await page.getByRole('button', { name: '全流程 Run-All' }).click();

    // 确认 PipelineStageIndicator 可见（不论成功/失败，只要 stages 非空即可）
    const sqlIndicator = page.locator('[data-testid="run-all-status"]');
    await expect(sqlIndicator).toBeVisible({ timeout: 30000 });

    // 点击指示灯触发按钮展开
    const triggerButton = sqlIndicator.locator('.pipeline-trigger');
    await triggerButton.click();

    // 断言：阶段列表展开，显示阶段名称
    const stageList = page.locator('[data-testid="stage-list"]');
    await expect(stageList).toBeVisible();
    // 至少有一个阶段行（Run-All 完成后应有 8 个阶段）
    const stageRows = stageList.locator('.pipeline-stage-row');
    await expect(stageRows.first()).toBeVisible();

    // 再次点击收起
    await triggerButton.click();

    // 断言：阶段列表收起（不可见）
    await expect(stageList).not.toBeVisible();
  });
});
