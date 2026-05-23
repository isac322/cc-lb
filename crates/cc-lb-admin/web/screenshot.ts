import { chromium } from 'playwright';

async function run() {
  const browser = await chromium.launch({ executablePath: '/usr/bin/chromium' });
  const page = await browser.newPage();
  
  // Set viewport size
  await page.setViewportSize({ width: 1280, height: 800 });

  // Mock state
  await page.goto('http://localhost:5173/admin/principals?mock=1');
  await page.waitForTimeout(2000); // Wait for animations/rendering
  await page.screenshot({ path: '../../.omo/evidence/t15-principal-limits/route-principals-mock.png' });

  // Empty state (no mock, no data)
  await page.goto('http://localhost:5173/admin/principals');
  await page.waitForTimeout(2000);
  await page.screenshot({ path: '../../.omo/evidence/t15-principal-limits/route-principals-empty.png' });

  await browser.close();
}

run().catch(console.error);
