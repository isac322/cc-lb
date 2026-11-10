import { mkdir, writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { type Browser, chromium, expect } from '@playwright/test';
import type { createServer as CreateViteServer } from 'vite';
import { ID, installFixtures, scenarios } from '../e2e/codexCreditsFixture';

const [appRoot, outputRoot, sourceSha, portArg = '5174'] = process.argv.slice(2);
if (!appRoot || !outputRoot || !sourceSha) {
  throw new Error('Usage: bun qa/capture-codex-credit-baseline.ts <base-web-root> <output-root> <base-sha> [port]');
}

const port = Number(portArg);
// The base worktree is runtime-selected and must use its own frozen Vite version.
const { createServer }: { createServer: typeof CreateViteServer } = await import(
  pathToFileURL(resolve(appRoot, 'node_modules/vite/dist/node/index.js')).href
);
const server = await createServer({
  root: appRoot,
  server: { host: '127.0.0.1', port, strictPort: true },
});
let browser: Browser | undefined;
const deadline = Promise.withResolvers<never>();
const readyTimer = setTimeout(
  () => deadline.reject(new Error('Baseline Vite did not become ready within 30 seconds')),
  30_000,
);

try {
  // Resolve the server's actual listen promise; CLI log styling is not readiness.
  await Promise.race([server.listen(), deadline.promise]);
  clearTimeout(readyTimer);
  browser = await chromium.launch();
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
    await server.close();
  }
}
