import fs from 'fs';

export default async function globalTeardown() {
  const ccLbPid = process.env.__CC_LB_PROCESS_PID__;
  const fakeAnthropicPid = process.env.__FAKE_ANTHROPIC_PROCESS_PID__;
  const configPath = process.env.__CONFIG_PATH__;

  if (ccLbPid) {
    try { process.kill(parseInt(ccLbPid)); } catch (err) { console.warn('teardown step failed:', err); }
  }
  if (fakeAnthropicPid) {
    try { process.kill(parseInt(fakeAnthropicPid)); } catch (err) { console.warn('teardown step failed:', err); }
  }
  if (configPath && fs.existsSync(configPath)) {
    fs.unlinkSync(configPath);
  }
}
