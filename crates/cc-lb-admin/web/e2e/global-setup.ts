import { spawn, type ChildProcess } from 'child_process';
import path from 'path';
import fs from 'fs';

let ccLbProcess: ChildProcess;
let fakeAnthropicProcess: ChildProcess;

export default async function globalSetup() {
  const workspaceRoot = path.resolve(process.cwd(), '../../..');
  
  const dataDir = path.join(workspaceRoot, 'data-test');
  if (fs.existsSync(dataDir)) {
    fs.rmSync(dataDir, { recursive: true, force: true });
  }
  fs.mkdirSync(dataDir, { recursive: true });

  // Create a temporary config file for cc-lb
  const configPath = path.join(workspaceRoot, 'cc-lb-test-config.toml');
  const config = `
[runtime]
data_dir = "${dataDir}"

[storage]
kind = "redb"
path = "${dataDir}/storage.redb"

[listener]
proxy_addr = "127.0.0.1:8080"
admin_addr = "127.0.0.1:8082"
metrics_addr = "127.0.0.1:9091"

[admin]
token_env = "CC_LB_ADMIN_TOKEN"

[api_keys.price_catalog]
cache_path = "${dataDir}/litellm.json"

[oauth.anthropic]
client_id = "test-client"
client_secret = "test-secret"
auth_url = "http://localhost:8081/oauth/authorize"
token_url = "http://localhost:8081/oauth/token"
redirect_uri = "http://localhost:5173/admin/oauth/callback"
scopes = ["messages", "files"]
`;
  fs.writeFileSync(configPath, config);
  console.log('Starting fake-anthropic...');
  fakeAnthropicProcess = spawn('cargo', ['run', '-p', 'fake-anthropic', '--', '--port', '8081'], {
    cwd: workspaceRoot,
    stdio: 'ignore',
  });

  console.log('Starting cc-lb-server...');
  ccLbProcess = spawn('cargo', ['run', '-p', 'cc-lb-server', '--', 'serve', '--config', configPath], {
    cwd: workspaceRoot,
    stdio: 'inherit',
      env: { 
        ...process.env, 
        RUST_LOG: 'info', 
        CC_LB_MASTER_KEY: '0000000000000000000000000000000000000000000000000000000000000000', 
        CC_LB_ADMIN_TOKEN: 'test-admin-token',
        CC_LB_BOOTSTRAP_ADMIN_TOKEN: 'test-admin-token',
        TEST_API_KEY: 'sk-ant-test-api-key-value'
      }
  });

  await waitForAdminReady();
  await seedDummyUpstream();
  
  // Store processes globally so teardown can kill them
  process.env.__CC_LB_PROCESS_PID__ = ccLbProcess.pid?.toString();
  process.env.__FAKE_ANTHROPIC_PROCESS_PID__ = fakeAnthropicProcess.pid?.toString();
  process.env.__CONFIG_PATH__ = configPath;
}

async function waitForAdminReady() {
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    try {
      const response = await fetch('http://127.0.0.1:8082/admin/v1/status', {
        headers: { Authorization: 'Bearer test-admin-token' },
      });
      if (response.ok) {
        return;
      }
    } catch {
      // Retry until the server is ready.
    }
    await new Promise(resolve => setTimeout(resolve, 500));
  }
  throw new Error('cc-lb admin API did not become ready');
}

async function seedDummyUpstream() {
  const response = await fetch('http://127.0.0.1:8082/admin/v1/upstreams', {
    method: 'POST',
    headers: {
      Authorization: 'Bearer test-admin-token',
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      name: 'dummy',
      kind: 'custom',
      base_url: 'http://localhost:8081',
      api_key_env: 'TEST_API_KEY',
    }),
  });
  if (!response.ok) {
    throw new Error(`failed to seed dummy upstream: ${response.status} ${await response.text()}`);
  }
}
