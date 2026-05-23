import { chromium } from 'playwright';
import { mkdir } from 'fs/promises';

async function main() {
  await mkdir('/home/bhyoo/projects/go/cc-lb/.omo/evidence/t17-management', { recursive: true });

  const browser = await chromium.launch({ executablePath: '/usr/bin/chromium' });
  const context = await browser.newContext();
  
  // Set token in localStorage
  await context.addInitScript(() => {
    window.localStorage.setItem('cc-lb-admin-token', 'dev');
  });
  
  const page = await context.newPage();
  
  page.on('console', msg => console.log('PAGE LOG:', msg.text()));
  page.on('pageerror', err => console.log('PAGE ERROR:', err.message));
  
  // 1. route-management-mock.png
  await page.goto('http://127.0.0.1:5173/management?mock=1');
  await page.waitForSelector('text=Principals', { timeout: 15000 });
  await page.waitForTimeout(2000);
  await page.screenshot({ path: '/home/bhyoo/projects/go/cc-lb/.omo/evidence/t17-management/route-management-mock.png' });
  
  // 2. route-management-key-dialog.png
  await page.goto('http://127.0.0.1:5173/management?mock=1&dialog=issue');
  await page.waitForSelector('text=This is the only time the key will be displayed. Copy it now.', { timeout: 10000 });
  await page.waitForTimeout(1000);
  await page.screenshot({ path: '/home/bhyoo/projects/go/cc-lb/.omo/evidence/t17-management/route-management-key-dialog.png' });
  
  // 3. route-management-credentials.png
  await page.goto('http://127.0.0.1:5173/management?mock=1&tab=credentials');
  await page.waitForSelector('text=Credentials', { timeout: 10000 });
  await page.waitForTimeout(1000);
  await page.screenshot({ path: '/home/bhyoo/projects/go/cc-lb/.omo/evidence/t17-management/route-management-credentials.png' });
  
  // 4. route-management-rotate.png
  await page.goto('http://127.0.0.1:5173/management?mock=1&dialog=rotate');
  await page.waitForSelector('text=Rotate Credential', { timeout: 10000 });
  await page.waitForTimeout(1000);
  await page.screenshot({ path: '/home/bhyoo/projects/go/cc-lb/.omo/evidence/t17-management/route-management-rotate.png' });
  
  // Check DOM for forbidden payload labels
  const content = await page.content();
  const forbidden = />[^<]*(messages|system|tools|tool_use|content)[^>]*</i;
  if (forbidden.test(content)) {
    console.error('Forbidden payload labels found in DOM text!');
    const matches = content.match(new RegExp(`.{0,30}(${forbidden.source}).{0,30}`, 'gi'));
    console.log('Matches:', matches);
  } else {
    console.log('No forbidden payload labels found.');
  }
  
  await browser.close();
}

main().catch(console.error);
