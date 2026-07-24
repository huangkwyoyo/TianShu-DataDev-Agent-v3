import { test, expect } from '@playwright/test';
import type { Locator } from '@playwright/test';

test('Run-All 完成后保留全部结果面板并按审查顺序展示', async ({ page }) => {
  const doneResult = {
    request_id: 'req-panel',
    pipeline_error: null,
    pipeline_stages: [{ stage: 'package', status: 'ok' }],
    sql_ok: true,
    sql_pipeline_error: null,
    sql_pipeline_stages: [{ stage: 'package', status: 'ok' }],
    generated_sql: 'SELECT 1 AS trip_count',
    spec_id: 'spec-panel',
    plan_id: 'plan-panel',
    package_id: 'package-panel',
    spec_result: {
      request_id: 'req-panel',
      spec_id: 'spec-panel',
      spec_hash: 'a'.repeat(64),
      title: '面板回归',
      description: '验证 Run-All 完整输出。',
      tables: [{
        table_alias: 't',
        source_table: 'source_table',
        row_count: 1,
        role: null,
        column_count: 1,
        has_time_field: false,
        has_partition: false,
      }],
      metrics: [{
        metric_name: '行数',
        aggregation: 'COUNT',
        input_column: null,
        alias: 'trip_count',
      }],
      dimensions: [],
      joins: [],
      time_range: null,
      output_spec: {
        columns: ['trip_count'],
        grain: [],
        sort_columns: [],
        limit: null,
      },
      open_questions: [],
      parse_warnings: [],
    },
    steps: [{
      step_type: 'project',
      step_id: 'project_1',
      description: '输出 trip_count',
    }],
    join_evidence: [],
    spark_ok: true,
    spark_stages: [{
      stage: 'PHYSICAL_VERIFIER',
      status: 'ok',
      errors: [],
    }],
    pyspark_code: 'def transform(inputs, params): return inputs["t"]',
    standalone_pyspark: 'print("spark")',
    llm_traces: {
      spec_enricher: {
        node_name: 'spec_enricher',
        model: 'test-model',
        token_usage: {
          prompt_tokens: 10,
          completion_tokens: 5,
          total_tokens: 15,
        },
        latency_ms: 12,
        status: 'valid',
        error_type: null,
      },
    },
    comparator_status: 'LOGIC_EQUIVALENT',
    requires_human_review: false,
    review_ready: true,
  };

  await page.route('**/api/run-all-full/stream', async (route) => {
    const body = [
      JSON.stringify({
        event: 'stage',
        pipeline: 'spark',
        stage: 'PHYSICAL_VERIFIER',
        status: 'completed',
        duration_ms: 113900,
        message: '全量逐行一致 | DuckDB 45 行 ↔ Spark 45 行 | 全量对比 | 行数✅ · Schema✅',
      }),
      JSON.stringify({ event: 'done', result: doneResult }),
      '',
    ].join('\n');
    await route.fulfill({
      status: 200,
      contentType: 'application/x-ndjson',
      body,
    });
  });
  await page.route('**/api/artifacts/req-panel/status', async (route) => {
    await route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({
        request_id: 'req-panel',
        artifacts_ready: true,
        available_artifacts: [],
      }),
    });
  });

  await page.goto('/');
  await page.locator('.spec-editor textarea').fill('---\nspec:\n  type: aggregate_table\n---');
  await page.getByRole('button', { name: /Run All/ }).click();

  const progress = page.locator('.run-progress-panel');
  const parse = page.getByRole('heading', { name: /解析预览/ });
  const plan = page.getByRole('heading', { name: /SqlBuildPlan 步骤/ });
  const sql = page.getByRole('heading', { name: /生成的 SQL/ });
  const llm = page.getByText('LLM 调用追踪', { exact: true });
  const code = page.locator('[data-testid="code-download-panel"]');

  await expect(progress).toBeVisible();
  await expect(progress).toContainText('全量逐行一致');
  await expect(progress).toHaveCSS('max-height', '180px');
  await expect(parse).toBeVisible();
  await expect(plan).toBeVisible();
  await expect(sql).toBeVisible();
  await expect(llm).toBeVisible();
  await expect(code).toBeVisible();

  const y = async (locator: Locator) => (await locator.boundingBox())!.y;
  expect(await y(progress)).toBeLessThan(await y(parse));
  expect(await y(parse)).toBeLessThan(await y(plan));
  expect(await y(plan)).toBeLessThan(await y(sql));
  expect(await y(sql)).toBeLessThan(await y(llm));
  expect(await y(llm)).toBeLessThan(await y(code));
});
