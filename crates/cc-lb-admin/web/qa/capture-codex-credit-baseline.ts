import { mkdir, writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { type Browser, chromium, expect } from '@playwright/test';
import { ID, installFixtures, scenarios } from '../e2e/codexCreditsFixture';

const [appRoot, outputRoot, sourceSha, portArg = '5174'] = process.argv.slice(2);
if (!appRoot || !outputRoot || !sourceSha) {
  throw new Error('Usage: bun qa/capture-codex-credit-baseline.ts <base-web-root> <output-root> <base-sha> [port]');
}

const port = Number(portArg);
const server = Bun.spawn(
  [process.execPath, resolve(appRoot, 'node_modules/vite/bin/vite.js'), '--host', '127.0.0.1', '--port', String(port), '--strictPort'],
  { cwd: appRoot, stdout: 'pipe', stderr: 'inherit' },
);
let browser: Browser | undefined;
let readyTimer: NodeJS.Timeout | undefined;

try {
  browser = await chromium.launch();
  const deadline = Promise.withResolvers<never>();
  readyTimer = setTimeout(
    () => deadline.reject(new Error('Baseline Vite did not become ready within 30 seconds')),
    30_000,
  );
  await Promise.race([
    (async () => {
      const reader = server.stdout.getReader();
      const decoder = new TextDecoder();
      let output = '';
      try {
        while (true) {
          const { done, value } = await reader.read();
          if (done) throw new Error(`Baseline Vite exited before readiness: ${output}`);
          output += decoder.decode(value, { stream: true });
          if (output.includes('Local:')) return;
        }
      } finally {
        reader.releaseLock();
      }
    })(),
    deadline.promise,
  ]);
  clearTimeout(readyTimer);
  await mkdir(outputRoot, { recursive: true });
  const captures = [];
  for (const width of [390, 768, 1440]) {
    const context = await browser.newContext({ viewport: { width, height: 900 }, colorScheme: 'light' });
    try {
      const page = await context.newPage();
      await installFixtures(page, scenarios[0].observations);
      await page.goto(`http://127.0.0.1:${port}/`);
      const overview = page.getByTestId('upstream-usage-row');
      await expect(overview).toHaveCount(1);
      await expect(overview.getByRole('meter', { name: '5h window used', exact: true })).toHaveAttribute('aria-valuenow', '25');
      await expect(overview.getByRole('meter', { name: '7d window used', exact: true })).toHaveAttribute('aria-valuenow', '50');
      await page.screenshot({ path: resolve(outputRoot, `${width}-overview.png`), fullPage: true });
      const overviewCredits = await page.getByTestId('codex-credit-facts').count();
      await overview.click();
      await expect(page).toHaveURL(
        (url) => url.pathname === '/upstreams' && url.searchParams.get('selectedId') === ID,
      );
      const detail = page.getByTestId('quota-snapshot-grid');
      await expect(detail).toBeVisible();
      await expect(detail.getByRole('meter', { name: '5h', exact: true })).toHaveAttribute('aria-valuenow', '25');
      await expect(detail.getByRole('meter', { name: '7d', exact: true })).toHaveAttribute('aria-valuenow', '50');
      await page.screenshot({ path: resolve(outputRoot, `${width}-detail.png`), fullPage: true });
      captures.push({ width, overviewCreditSummaries: overviewCredits, detailCreditSummaries: await page.getByTestId('codex-credit-facts').count() });
    } finally {
      await context.close();
    }
  }
  await writeFile(resolve(outputRoot, 'capture.json'), JSON.stringify({ sourceSha, fixture: 'codexCreditsFixture', syntheticAdminApi: true, captures }, null, 2));
  console.log(`Captured baseline Overview/detail at all three widths for ${sourceSha}`);
} finally {
  clearTimeout(readyTimer);
  try {
    await browser?.close();
  } finally {
    server.kill('SIGTERM');
    await server.exited;
  }
}
