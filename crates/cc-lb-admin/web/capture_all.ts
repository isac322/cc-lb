import { chromium } from 'playwright';
import { mkdir } from 'fs/promises';

async function main() {
  await mkdir('/home/bhyoo/projects/go/cc-lb/.omo/evidence/t14-overview', { recursive: true });
  await mkdir('/home/bhyoo/projects/go/cc-lb/.omo/evidence/t15-principal-limits', { recursive: true });
  await mkdir('/home/bhyoo/projects/go/cc-lb/.omo/evidence/t17-management', { recursive: true });
  await mkdir('/home/bhyoo/projects/go/cc-lb/.omo/evidence/t24-app-shell', { recursive: true });

  const browser = await chromium.launch({ executablePath: '/usr/bin/chromium' });
  const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
  
  await context.addInitScript(() => {
    window.localStorage.setItem('cc-lb-admin-token', 'dev');
  });
  
  const page = await context.newPage();
  
  page.on('console', msg => console.log('PAGE LOG:', msg.text()));
  page.on('pageerror', err => console.log('PAGE ERROR:', err.message));
  
  const capture = async (url: string, path: string, selector: string) => {
    console.log(`Capturing ${path}...`);
    await page.goto(url);
    await page.waitForSelector(selector, { timeout: 15000 });
    await page.waitForTimeout(2000);
    await page.screenshot({ path });
  };

  // T14
  await capture('http://127.0.0.1:5173/?mock=1', '/home/bhyoo/projects/go/cc-lb/.omo/evidence/t14-overview/route-overview-mock.png', 'text=Overview');
  
  // T15
  await capture('http://127.0.0.1:5173/principals?mock=1', '/home/bhyoo/projects/go/cc-lb/.omo/evidence/t15-principal-limits/route-principals-mock.png', 'text=Principal Limits');
  
  // T17
  await capture('http://127.0.0.1:5173/management?mock=1', '/home/bhyoo/projects/go/cc-lb/.omo/evidence/t17-management/route-management-mock.png', 'text=Principals');
  
  // T24
  await capture('http://127.0.0.1:5173/?mock=1', '/home/bhyoo/projects/go/cc-lb/.omo/evidence/t24-app-shell/route-overview.png', 'text=Overview');
  await capture('http://127.0.0.1:5173/principals?mock=1', '/home/bhyoo/projects/go/cc-lb/.omo/evidence/t24-app-shell/route-principals.png', 'text=Principal Limits');
  await capture('http://127.0.0.1:5173/log?mock=1', '/home/bhyoo/projects/go/cc-lb/.omo/evidence/t24-app-shell/route-log.png', 'text=Realtime Log');
  await capture('http://127.0.0.1:5173/management?mock=1', '/home/bhyoo/projects/go/cc-lb/.omo/evidence/t24-app-shell/route-management.png', 'text=Principals');
  await capture('http://127.0.0.1:5173/settings?mock=1', '/home/bhyoo/projects/go/cc-lb/.omo/evidence/t24-app-shell/route-settings.png', 'text=Settings');
  await capture('http://127.0.0.1:5173/upstreams?mock=1', '/home/bhyoo/projects/go/cc-lb/.omo/evidence/t24-app-shell/route-upstreams.png', 'text=Upstreams');
  await capture('http://127.0.0.1:5173/activity?mock=1', '/home/bhyoo/projects/go/cc-lb/.omo/evidence/t24-app-shell/route-activity.png', 'text=Admin Activity');
  await capture('http://127.0.0.1:5173/credentials?mock=1', '/home/bhyoo/projects/go/cc-lb/.omo/evidence/t24-app-shell/route-credentials.png', 'text=Credential Status');
  await capture('http://127.0.0.1:5173/plugins?mock=1', '/home/bhyoo/projects/go/cc-lb/.omo/evidence/t24-app-shell/route-plugins.png', 'text=Plugin Status');

  await browser.close();
}

main().catch(console.error);
