import path from 'node:path';
import { stopE2eServers } from './server-control.mjs';

export default async function globalTeardown() {
  const shutdownPath = path.resolve(
    process.cwd(),
    'test-results',
    '.e2e-shutdown',
  );
  await stopE2eServers(shutdownPath);
}
