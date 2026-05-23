import { chromium } from 'playwright';

async function main() {
  const browser = await chromium.launch({ executablePath: '/usr/bin/chromium' });
  const context = await browser.newContext();
  
  // Set token in localStorage
  await context.addInitScript(() => {
    window.localStorage.setItem('cc-lb-admin-token', 'mock_token');
  });
  
  const page = await context.newPage();
  
  page.on('console', msg => console.log('PAGE LOG:', msg.text()));
  page.on('pageerror', err => console.log('PAGE ERROR:', err.message));
  
  // 1. route-log-mock.png
  await page.goto('http://localhost:5173/log?mock=1');
  await page.waitForSelector('text=Realtime Log', { timeout: 15000 }); // Wait for Vite to compile and render
  await page.waitForTimeout(2000); // wait for events to populate
  await page.screenshot({ path: '../../.omo/evidence/t16-realtime-log/route-log-mock.png' });
  
  // 2. route-log-paused.png
  try {
    await page.click('button:has-text("Pause")', { timeout: 5000 });
    await page.waitForTimeout(2000); // wait for badge to increment
    await page.screenshot({ path: '../../.omo/evidence/t16-realtime-log/route-log-paused.png' });
  } catch (e) {
    console.error('Failed to click Pause button', e);
    await page.screenshot({ path: '../../.omo/evidence/t16-realtime-log/error-state.png' });
  }
  
  // 3. route-log-filtered.png
  await page.goto('http://localhost:5173/log?mock=1&upstream=anthropic_direct&status_class=4xx');
  await page.waitForSelector('text=Realtime Log', { timeout: 10000 });
  await page.waitForTimeout(2000);
  await page.screenshot({ path: '../../.omo/evidence/t16-realtime-log/route-log-filtered.png' });
  
  // 4. route-log-empty.png
  await page.goto('http://localhost:5173/log');
  await page.waitForSelector('text=Realtime Log', { timeout: 10000 });
  await page.waitForTimeout(1000);
  await page.screenshot({ path: '../../.omo/evidence/t16-realtime-log/route-log-empty.png' });
  
  // Check DOM for forbidden payload labels
  const content = await page.content();
  // Only match text content, not HTML attributes
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
