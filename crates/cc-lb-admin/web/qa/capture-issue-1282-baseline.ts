import { mkdir, writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { type Browser, chromium, expect } from '@playwright/test';
import type { createServer as CreateViteServer } from 'vite';
import { type FixtureState, installFixtures, PRINCIPAL_ID } from '../e2e/issue1282Fixture';

const [appRoot, outputRoot, sourceSha, portArg = '5175'] = process.argv.slice(2);
if (!appRoot || !outputRoot || !sourceSha) {
  throw new Error('Usage: bun qa/capture-issue-1282-baseline.ts <base-web-root> <output-root> <base-sha> [port]');
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
  for (const viewport of [
    { width: 390, height: 844 },
    { width: 1440, height: 900 },
  ]) {
    for (const theme of ['dark', 'light'] as const) {
      const context = await browser.newContext({
        viewport,
        colorScheme: theme,
        reducedMotion: 'reduce',
      });
      try {
        const page = await context.newPage();
        const state: FixtureState = { registry: 'empty', referencesFail: false };
        await installFixtures(page, theme, state);
        const origin = `http://127.0.0.1:${port}`;
        const { width, height } = viewport;
        await page.goto(`${origin}/principals?selectedId=${PRINCIPAL_ID}`);
        await expect(page.locator('html')).toHaveAttribute('data-theme', theme);
        await page.getByTestId('cache-keepalive-card').getByRole('button', { name: 'Settings', exact: true }).click();
        const drawer = page.getByRole('dialog', { name: /Cache keepalive settings/i });
        await expect(drawer).toBeVisible();
        const shortLabel = drawer.getByText(/5m (?:TTL|\/ 30m)/);
        const longLabel = drawer.getByText(/1h (?:TTL|\/ 12h)/);
        await expect(shortLabel).toBeVisible();
        await expect(longLabel).toBeVisible();
        await expect
          .poll(async () => {
            const box = await drawer.boundingBox();
            const opacity = await drawer.evaluate((el) => getComputedStyle(el).opacity);
            return box !== null && box.x < width && box.width > 0 && opacity === '1';
          })
          .toBe(true);
        const keepaliveFile = `keepalive-drawer-${width}-${theme}.png`;
        await page.screenshot({ path: resolve(outputRoot, keepaliveFile) });
        captures.push({
          state: 'keepalive-drawer', width, height, theme, file: keepaliveFile,
          shortLabel: await shortLabel.innerText(), longLabel: await longLabel.innerText(),
        });

        state.registry = 'error';
        state.referencesFail = true;
        await page.goto(`${origin}/plugins`);
        const count = page.getByTestId('plugin-count-slot');
        await expect(count).toBeVisible();
        const emptyCatalog =
          width < 768
            ? page.locator('li', { hasText: 'No plugins uploaded.' })
            : page.getByRole('cell', { name: 'No plugins uploaded.', exact: true });
        await expect(emptyCatalog.or(page.getByRole('alert').first())).toBeVisible();
        const catalogFile = `plugin-catalog-initial-error-${width}-${theme}.png`;
        await page.screenshot({ path: resolve(outputRoot, catalogFile) });
        captures.push({
          state: 'plugin-catalog-initial-error', width, height, theme, file: catalogFile,
          countText: await count.innerText(), alertCount: await page.getByRole('alert').count(),
        });

        state.registry = 'populated';
        await page.goto(`${origin}/plugins?plugin=plugin-1`);
        await expect(page.getByRole('heading', { name: 'Used by', exact: true })).toBeVisible();
        await expect(
          page
            .getByText('This plugin is uploaded but not used anywhere yet.', { exact: true })
            .or(page.getByRole('alert').first()),
        ).toBeVisible();
        const detailFile = `plugin-detail-usage-error-${width}-${theme}.png`;
        await page.screenshot({ path: resolve(outputRoot, detailFile) });
        captures.push({ state: 'plugin-detail-usage-error', width, height, theme, file: detailFile });
      } finally {
        await context.close();
      }
    }
  }
  await writeFile(resolve(outputRoot, 'capture.json'), JSON.stringify({ sourceSha, fixture: 'issue1282Fixture', syntheticAdminApi: true, captures }, null, 2));
  console.log(`Captured baseline keepalive drawer and plugin error states in both viewports and themes for ${sourceSha}`);
} finally {
  clearTimeout(readyTimer);
  try {
    await browser?.close();
  } finally {
    await server.close();
  }
}
