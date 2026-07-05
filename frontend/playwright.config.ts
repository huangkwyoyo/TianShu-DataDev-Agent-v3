import { defineConfig } from '@playwright/test';

export default defineConfig({
  testDir: './tests/e2e/specs',
  timeout: 60000,
  expect: { timeout: 10000 },
  fullyParallel: false,
  retries: 0,
  use: {
    baseURL: 'http://127.0.0.1:5173',
    trace: 'retain-on-failure',
    screenshot: 'only-on-failure',
  },
  webServer: [
    {
      // Phase 9C-R16 边界硬化：通过 Python 一行命令在进程内设置 TIANSHU_E2E_MODE=true
      // 避免环境变量跨进程传递问题（Windows cmd.exe 不支持 Unix 风格 env 前缀）
      // 生产路径使用 tianshu_datadev.api.app:create_app（不启用 CSV fixture 自动发现）
      command: 'python -c "import os; os.environ[\'TIANSHU_E2E_MODE\']=\'true\'; from tianshu_datadev.api.app import create_app; import uvicorn; uvicorn.run(create_app(), host=\'127.0.0.1\', port=8000)"',
      port: 8000,
      timeout: 30000,
      reuseExistingServer: true,
    },
    {
      command: 'npm run dev -- --host 127.0.0.1 --port 5173',
      port: 5173,
      timeout: 15000,
      reuseExistingServer: true,
    },
  ],
});
