import { existsSync } from 'node:fs';
import { createServer } from 'vite';

const shutdownPath = process.env.TIANSHU_E2E_SHUTDOWN_FILE;
if (!shutdownPath) {
  throw new Error('缺少 TIANSHU_E2E_SHUTDOWN_FILE');
}

const server = await createServer({
  configFile: 'vite.config.ts',
  server: {
    host: '127.0.0.1',
    port: 15173,
    strictPort: true,
  },
});
await server.listen();

const timer = setInterval(async () => {
  if (!existsSync(shutdownPath)) {
    return;
  }
  clearInterval(timer);
  await server.close();
}, 100);
