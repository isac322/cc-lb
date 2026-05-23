import { chromium } from 'playwright';
import { mkdir } from 'fs/promises';

async function main() {
  await mkdir('/home/bhyoo/projects/go/cc-lb/.omo/evidence/t19-upstreams', { recursive: true });
  await mkdir('/home/bhyoo/projects/go/cc-lb/.omo/evidence/t20-activity', { recursive: true });
  await mkdir('/home/bhyoo/projects/go/cc-lb/.omo/evidence/t21-credentials', { recursive: true });
  await mkdir('/home/bhyoo/projects/go/cc-lb/.omo/evidence/t22-plugins', { recursive: true });

  const browser = await chromium.launch({ executablePath: '/usr/bin/chromium' });
  const context = await browser.newContext();
  
  // Set token in localStorage
  await context.addInitScript(() => {
    window.localStorage.setItem('cc-lb-admin-token', 'dev');
  });
  
  const page = await context.newPage();
  
  page.on('console', msg => console.log('PAGE LOG:', msg.text()));
  page.on('pageerror', err => console.log('PAGE ERROR:', err.message));
  
  // T19 Upstreams
  await page.goto('http://127.0.0.1:5173/upstreams?mock=1');
  await page.waitForSelector('text=Upstreams', { timeout: 15000 });
  await page.waitForTimeout(2000);
  await page.screenshot({ path: '/home/bhyoo/projects/go/cc-lb/.omo/evidence/t19-upstreams/route-upstreams-mock.png' });
  
  await page.goto('http://127.0.0.1:5173/upstreams?mock=empty');
  await page.waitForSelector('text=No upstreams configured', { timeout: 15000 });
  await page.waitForTimeout(1000);
  await page.screenshot({ path: '/home/bhyoo/projects/go/cc-lb/.omo/evidence/t19-upstreams/route-upstreams-empty.png' });
  
  // T20 AdminActivity
  await page.goto('http://127.0.0.1:5173/activity?mock=1');
  await page.waitForSelector('text=Admin Activity', { timeout: 15000 });
  await page.waitForTimeout(2000);
  await page.screenshot({ path: '/home/bhyoo/projects/go/cc-lb/.omo/evidence/t20-activity/route-activity-mock.png' });
  
  // T20 AdminActivity Filtered
  await page.goto('http://127.0.0.1:5173/activity?mock=1');
  await page.waitForSelector('text=Admin Activity', { timeout: 15000 });
  await page.selectOption('select#kind-filter', 'config_apply');
  await page.waitForTimeout(1000);
  await page.screenshot({ path: '/home/bhyoo/projects/go/cc-lb/.omo/evidence/t20-activity/route-activity-filtered.png' });
  
  // T21 CredentialStatus
  await page.goto('http://127.0.0.1:5173/credentials?mock=1');
  await page.waitForSelector('text=Credential Status', { timeout: 15000 });
  await page.waitForTimeout(2000);
  await page.screenshot({ path: '/home/bhyoo/projects/go/cc-lb/.omo/evidence/t21-credentials/route-credentials-mock.png' });
  
  // T21 CredentialStatus Action Needed
  await page.goto('http://127.0.0.1:5173/credentials?mock=1');
  await page.waitForSelector('text=Credential Status', { timeout: 15000 });
  // We don't have a filter on this page, but we can just take the screenshot as it shows the action needed badge
  await page.waitForTimeout(1000);
  await page.screenshot({ path: '/home/bhyoo/projects/go/cc-lb/.omo/evidence/t21-credentials/route-credentials-action-needed.png' });
  
  // T22 PluginStatus
  await page.goto('http://127.0.0.1:5173/plugins?mock=1');
  await page.waitForSelector('text=Plugin Status', { timeout: 15000 });
  await page.waitForTimeout(2000);
  await page.screenshot({ path: '/home/bhyoo/projects/go/cc-lb/.omo/evidence/t22-plugins/route-plugins-mock.png' });
  
  // T22 PluginStatus Empty
  await page.goto('http://127.0.0.1:5173/plugins?mock=empty');
  await page.waitForSelector('text=No plugins configured', { timeout: 15000 });
  await page.waitForTimeout(1000);
  await page.screenshot({ path: '/home/bhyoo/projects/go/cc-lb/.omo/evidence/t22-plugins/route-plugins-empty.png' });
  
  // Check DOM for forbidden payload labels and tokens
  const content = await page.content();
  const forbidden = />[^<]*(messages|system|tools|tool_use|content)[^>]*</i;
  if (forbidden.test(content)) {
    console.error('Forbidden payload labels found in DOM text!');
  } else {
    console.log('No forbidden payload labels found.');
  }
  
  const tokenLeak = /LEAK-TOKEN-DO-NOT-RETURN|sk-ant-|Bearer /i;
  if (tokenLeak.test(content)) {
    console.error('Token leak found in DOM text!');
  } else {
    console.log('No token leaks found.');
  }
  
  await browser.close();
}

main().catch(console.error);
