import { expect, test } from '@playwright/test';
import { installFixtures } from './issue1282Fixture';
import { poolUpstreams, subscriptionPreference, upstreamPools } from './issue1297Fixture';

// OpenAI-only exercises every responsive arrangement; the remaining pools and
// query states are distributed across breakpoints rather than a full cross product.
const scenarios = [
  { pool: 'Anthropic-only', width: 1440 },
  { pool: 'OpenAI-only', width: 390 },
  { pool: 'OpenAI-only', width: 768 },
  { pool: 'OpenAI-only', width: 1440 },
  { pool: 'Gemini-only', width: 390 },
  { pool: 'mixed', width: 768 },
  { pool: 'API-key-only', width: 1440 },
  { pool: 'pending', width: 390 },
  { pool: 'error', width: 768 },
] as const;

for (const { pool, width } of scenarios) {
  test(`registry copy survives ${pool} upstreams in catalog and Inspect at ${width}px`, async ({ page }, testInfo) => {
    await page.setViewportSize({ width, height: width === 390 ? 844 : 1024 });
    await page.emulateMedia({ reducedMotion: 'reduce' });
    await installFixtures(page, 'dark');
    await page.route('**/admin/v1/plugins/registry', (route) => route.fulfill({
      json: { entries: [subscriptionPreference] },
    }));
    await page.route(`**/admin/v1/plugins/registry/${subscriptionPreference.id}/references`, (route) => route.fulfill({
      json: {
        registry: subscriptionPreference,
        refcount: 0,
        reference_fingerprint: 'fixture-references',
        references: [],
      },
    }));
    await page.route('**/admin/v1/upstreams', (route) => {
      // Leave the intercepted request unresolved to exercise a truly pending
      // query, not an empty successful fleet. Playwright disposes it at teardown.
      if (pool === 'pending') return;
      if (pool === 'error') {
        return route.fulfill({ status: 503, json: {
          error: { code: 'unavailable', message: 'Upstreams unavailable' },
        } });
      }
      return route.fulfill({ json: { upstreams: poolUpstreams(upstreamPools[pool]) } });
    });
    await page.goto('/plugins');

    const catalogCopy = page.getByText(subscriptionPreference.description, { exact: false });
    await expect(catalogCopy.filter({ visible: true })).toHaveCount(1);
    await expect(page.getByText(/quota health is not checked\./)).toHaveCount(0);
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
    await page.screenshot({ path: testInfo.outputPath(`catalog-${pool}-${width}.png`) });
    if (width < 768) {
      await page.getByRole('button', { name: new RegExp(subscriptionPreference.name) }).click();
    } else {
      await page.getByRole('button', { name: `Inspect ${subscriptionPreference.name}`, exact: true }).click();
    }
    await expect(page.getByRole('button', { name: 'Back to plugins' })).toBeVisible();
    await expect(page.getByText(subscriptionPreference.description, { exact: true })).toBeVisible();
    for (const sentence of [
      subscriptionPreference.metadata.purpose,
      subscriptionPreference.metadata.keeps,
      subscriptionPreference.metadata.drops,
      subscriptionPreference.metadata.empty_behavior,
      ...subscriptionPreference.metadata.examples,
    ]) {
      await expect(page.getByText(sentence, { exact: true })).toBeVisible();
    }
    await expect(page.getByText(/quota health is not checked\./)).toHaveCount(0);
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
    await page.screenshot({ path: testInfo.outputPath(`inspect-${pool}-${width}.png`), fullPage: true });
  });
}
