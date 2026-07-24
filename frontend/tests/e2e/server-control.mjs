import { spawn } from 'node:child_process';
import { existsSync, rmSync, writeFileSync } from 'node:fs';
import net from 'node:net';
import path from 'node:path';

const E2E_PORTS = [15173, 18000];

function canConnect(port) {
  return new Promise((resolve) => {
    const socket = net.createConnection({ host: '127.0.0.1', port });
    socket.setTimeout(500);
    socket.once('connect', () => {
      socket.destroy();
      resolve(true);
    });
    const reject = () => {
      socket.destroy();
      resolve(false);
    };
    socket.once('error', reject);
    socket.once('timeout', reject);
  });
}

async function waitForPort(port, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await canConnect(port)) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 200));
  }
  throw new Error(`E2E 服务端口 ${port} 在 ${timeoutMs}ms 内未就绪`);
}

async function waitForPortClosed(port, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (!(await canConnect(port))) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error(`E2E 服务端口 ${port} 在 ${timeoutMs}ms 内未关闭`);
}

export async function stopE2eServers(shutdownPath) {
  writeFileSync(shutdownPath, 'stop', 'utf8');
  try {
    await Promise.all(E2E_PORTS.map((port) => waitForPortClosed(port, 10000)));
  } finally {
    rmSync(shutdownPath, { force: true });
  }
}

export async function startE2eServers() {
  for (const port of E2E_PORTS) {
    if (await canConnect(port)) {
      throw new Error(`E2E 专属端口 ${port} 已被占用，拒绝复用旧服务`);
    }
  }

  const frontendRoot = process.cwd();
  const projectRoot = path.resolve(frontendRoot, '..');
  const shutdownPath = path.resolve(
    frontendRoot,
    'test-results',
    '.e2e-shutdown',
  );
  if (existsSync(shutdownPath)) {
    rmSync(shutdownPath, { force: true });
  }
  const backendCode = [
    "import os; os.environ['TIANSHU_E2E_MODE']='true'",
    "os.environ['DEEPSEEK_API_KEY']=''",
    "os.environ['TIANSHU_RUN_ID']=''",
    'import tianshu_datadev.temp_manager as tm; tm.ensure_temp_dir=lambda:None',
    'import tianshu_datadev.api.app as app_module',
    'app_module._discover_nyc_duckdb=lambda:None',
    'import threading, time, uvicorn',
    "config=uvicorn.Config(app_module.create_app(), host='127.0.0.1', port=18000)",
    'server=uvicorn.Server(config)',
    "shutdown_path=os.environ['TIANSHU_E2E_SHUTDOWN_FILE']",
    'def watch_shutdown():\n while not os.path.exists(shutdown_path): time.sleep(0.1)\n server.should_exit=True',
    'threading.Thread(target=watch_shutdown, daemon=True).start()',
    'server.run()',
  ].join('\n');

  const backend = spawn('python', ['-c', backendCode], {
    cwd: projectRoot,
    detached: true,
    stdio: 'ignore',
    windowsHide: true,
    env: {
      ...process.env,
      TIANSHU_E2E_SHUTDOWN_FILE: shutdownPath,
    },
  });
  backend.unref();

  const frontend = spawn(
    'node',
    ['tests/e2e/vite-server.mjs'],
    {
      cwd: frontendRoot,
      detached: true,
      stdio: 'ignore',
      windowsHide: true,
      env: {
        ...process.env,
        TIANSHU_API_TARGET: 'http://127.0.0.1:18000',
        TIANSHU_E2E_SHUTDOWN_FILE: shutdownPath,
      },
    },
  );
  frontend.unref();

  try {
    await Promise.all([
      waitForPort(18000, 30000),
      waitForPort(15173, 15000),
    ]);
  } catch (error) {
    await stopE2eServers(shutdownPath);
    throw error;
  }
  return shutdownPath;
}
