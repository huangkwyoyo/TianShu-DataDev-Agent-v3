import { startE2eServers } from './server-control.mjs';

export default async function globalSetup() {
  await startE2eServers();
}
