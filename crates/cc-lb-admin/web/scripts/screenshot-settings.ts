import { chromium } from 'playwright';
import { join } from 'path';
import { mkdir } from 'fs/promises';

async function main() {
  const outDir = join(process.cwd(), '../../../.omo/evidence/t18-settings');
  await mkdir(outDir, { recursive: true });

  const browser = await chromium.launch();
  const context = await browser.newContext({
    viewport: { width: 1280, height: 800 },
  });

  // Seed localStorage with a fake token so we bypass the login screen
  await context.addInitScript(() => {
    localStorage.setItem('cc-lb-admin-token', 'fake-token');
  });

  const page = await context.newPage();

  // 1. route-settings-mock.png
  await page.goto('http://localhost:5173/settings?mock=1');
  await page.waitForSelector('text=Listener');
  await page.screenshot({ path: join(outDir, 'route-settings-mock.png') });
  await page.screenshot({ path: join(outDir, 'route-settings-mock.png') });

  // 2. route-settings-validate-error.png
  await page.goto('http://localhost:5173/settings?mock=1&dialog=validate-error');
  await page.waitForSelector('text=Listener');
  await page.click('button:has-text("Validate")');
  await page.waitForSelector('text=Validation Error:');
  await page.screenshot({ path: join(outDir, 'route-settings-validate-error.png') });

  // 3. route-settings-apply-confirm.png
  await page.goto('http://localhost:5173/settings?mock=1&dialog=apply');
  await page.waitForSelector('text=Apply Configuration');
  await page.screenshot({ path: join(outDir, 'route-settings-apply-confirm.png') });

  // 4. route-settings-history.png
  await page.goto('http://localhost:5173/settings?mock=1');
  await page.waitForSelector('text=Recent Revisions');
  await page.screenshot({ path: join(outDir, 'route-settings-history.png') });

  // 5. route-settings-diff.png
  await page.goto('http://localhost:5173/settings?mock=1&dialog=diff');
  await page.waitForSelector('text=Configuration Diff');
  await page.screenshot({ path: join(outDir, 'route-settings-diff.png') });

  await browser.close();
}

main().catch(console.error);
