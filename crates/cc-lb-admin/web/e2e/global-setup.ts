import { spawn } from 'child_process';
import path from 'path';

export default async function globalSetup() {
  const server = spawn('bun', ['run', 'mock-server.ts'], {
    cwd: path.join(process.cwd()),
    stdio: 'inherit',
    detached: true,
  });
  server.unref();
  process.env.MOCK_SERVER_PID = server.pid?.toString();
  
  // Wait for server to start
  await new Promise((resolve) => setTimeout(resolve, 1000));
}
