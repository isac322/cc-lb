import { expect, type Page, test } from '@playwright/test';
import { type FixtureState, installFixtures, principal, PRINCIPAL_ID } from './issue1282Fixture';

async function expectNoHorizontalOverflow(page: Page) {
  expect(
    await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth),
  ).toBe(true);
}

for (const { viewport, theme } of [
  { viewport: { width: 1440, height: 900 }, theme: 'dark' },
  { viewport: { width: 1440, height: 900 }, theme: 'light' },
  { viewport: { width: 390, height: 844 }, theme: 'dark' },
  { viewport: { width: 390, height: 844 }, theme: 'light' },
] as const) {
  test(`Cache keepalive has dual-horizon copy without overflow at ${viewport.width}px in ${theme}`, async ({ page }, testInfo) => {
    await page.setViewportSize(viewport);
    await page.emulateMedia({ reducedMotion: 'reduce' });
    await installFixtures(page, theme);
    await page.goto(`/principals?selectedId=${PRINCIPAL_ID}`);
    await expect(page.locator('html')).toHaveAttribute('data-theme', theme);
    await page.getByTestId('cache-keepalive-card').getByRole('button', { name: 'Settings', exact: true }).click();

    const drawer = page.getByRole('dialog', { name: /Cache keepalive settings/i });
    await expect(drawer).toBeVisible();
    await expect(drawer.getByLabel('Renewal lead time · 5m / 30m')).toHaveValue('45');
    await expect(drawer.getByText(
      'Seconds. Renews 45s before an Anthropic 5m or OpenAI 30m cache expires.',
      { exact: true },
    )).toBeVisible();
    await expect(drawer.getByLabel('Renewal lead time · 1h / 12h')).toHaveValue('400');
    await expect(drawer.getByText(
      'Seconds. Renews 400s before an Anthropic 1h cache expires. OpenAI 12h retention is recorded, not renewed.',
      { exact: true },
    )).toBeVisible();
    await expect(page.getByText(/5m TTL|1h TTL/)).toHaveCount(0);
    await expectNoHorizontalOverflow(page);
    await page.screenshot({ path: testInfo.outputPath(`keepalive-drawer-${viewport.width}-${theme}.png`) });
  });
}

for (const viewport of [
  { width: 390, height: 844 },
  { width: 768, height: 1024 },
  { width: 1440, height: 900 },
]) {
  for (const theme of ['dark', 'light'] as const) {
    test(`Plugin errors recover and retain stale data at ${viewport.width}px in ${theme}`, async ({ page }, testInfo) => {
      const state: FixtureState = { registry: 'error', referencesFail: true };
      await page.setViewportSize(viewport);
      await page.emulateMedia({ reducedMotion: 'reduce' });
      await installFixtures(page, theme, state);
      await page.goto('/plugins');
      await expect(page.locator('html')).toHaveAttribute('data-theme', theme);

      // Production retries once before settling; auto-wait for the final error.
      const alert = page.getByRole('alert');
      await expect(alert).toHaveCount(1);
      await expect(alert).toContainText('Could not load plugin library');
      await expect(alert).toBeVisible();
      const count = page.getByTestId('plugin-count-slot');
      await expect(count).toHaveText('Unavailable');
      await expect(count).not.toContainText('0 available');
      await expect(page.getByText('No plugins uploaded.', { exact: true })).toHaveCount(0);
      const retry = alert.getByRole('button', { name: 'Retry', exact: true });
      await expect(retry).toBeVisible();
      await expect(page.getByRole('button', { name: 'Clean orphaned uploads', exact: true })).toBeEnabled();
      if (viewport.width === 390) {
        const bounds = await retry.boundingBox();
        expect(bounds).not.toBeNull();
        expect(bounds?.height).toBeGreaterThanOrEqual(40);
      }
      await expectNoHorizontalOverflow(page);
      await page.screenshot({ path: testInfo.outputPath(`plugin-catalog-initial-error-${viewport.width}-${theme}.png`) });

      state.registry = 'populated';
      await retry.click();
      await expect(count).toHaveText('2 available · 0 not used anywhere');
      const pluginRow = (name: string) => viewport.width < 768
        ? page.getByRole('button', { name: new RegExp(`^${name}`) })
        : page.getByRole('row').filter({ hasText: name });
      await expect(pluginRow('First plugin')).toBeVisible();
      await expect(pluginRow('Second plugin')).toBeVisible();
      await expect(alert).toHaveCount(0);
      await expectNoHorizontalOverflow(page);
      await page.screenshot({ path: testInfo.outputPath(`plugin-catalog-recovered-${viewport.width}-${theme}.png`) });

      // Keep the singleton QueryClient alive. Advance only Date (not timers)
      // beyond staleTime, then reconnect to exercise its real refetch path.
      state.registry = 'error';
      const now = await page.evaluate(() => Date.now());
      await page.clock.setFixedTime(new Date(now + 10_001));
      await page.context().setOffline(true);
      await page.context().setOffline(false);
      const warning = page.getByRole('status').filter({ hasText: 'Could not refresh plugin library' });
      await expect(warning).toBeVisible();
      await expect(warning).toHaveCount(1);
      await expect(warning).toContainText('Showing the last loaded plugin list.');
      await expect(count).toHaveText('2 available · 0 not used anywhere');
      await expect(pluginRow('First plugin')).toBeVisible();
      await expect(pluginRow('Second plugin')).toBeVisible();
      await expect(alert).toHaveCount(0);
      await expect(page.getByText('No plugins uploaded.', { exact: true })).toHaveCount(0);
      await expectNoHorizontalOverflow(page);
      await page.screenshot({ path: testInfo.outputPath(`plugin-catalog-stale-warning-${viewport.width}-${theme}.png`) });

      state.registry = 'populated';
      await warning.getByRole('button', { name: 'Retry', exact: true }).click();
      await expect(warning).toHaveCount(0);
      await (
        viewport.width < 768
          ? pluginRow('First plugin')
          : pluginRow('First plugin').getByRole('button', {
              name: 'Inspect First plugin',
              exact: true,
            })
      ).click();
      await expect(page.getByRole('heading', { name: 'First plugin', exact: true })).toBeVisible();
      const usedBy = page.getByTestId('plugin-used-by-slot');
      const usageAlert = usedBy.getByRole('alert');
      await expect(usageAlert).toHaveCount(1);
      await expect(usageAlert).toBeVisible();
      await expect(usageAlert).toContainText('Could not load plugin usage');
      await expect(usedBy.getByText('This plugin is uploaded but not used anywhere yet.', { exact: true })).toHaveCount(0);
      await expectNoHorizontalOverflow(page);
      await expect(usageAlert.getByRole('button', { name: 'Retry', exact: true })).toBeVisible();
      await page.screenshot({ path: testInfo.outputPath(`plugin-detail-usage-error-${viewport.width}-${theme}.png`) });

      state.referencesFail = false;
      await usageAlert.getByRole('button', { name: 'Retry', exact: true }).click();
      await expect(usedBy.getByRole('link', { name: principal.name, exact: true })).toBeVisible();
      await expect(usedBy.getByRole('link', { name: principal.name, exact: true })).toHaveAttribute('href', `/principals?selectedId=${PRINCIPAL_ID}`);
      await expect(usedBy.getByRole('alert')).toHaveCount(0);
      await expect(usedBy.getByRole('button', { name: 'Retry', exact: true })).toHaveCount(0);
      await expectNoHorizontalOverflow(page);
    });
  }
}

test('Healthy empty plugin registry remains a genuine empty library', async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  await installFixtures(page, 'dark');
  await page.goto('/plugins');
  await expect(page.getByTestId('plugin-count-slot')).toHaveText('0 available · 0 not used anywhere');
  await expect(page.getByRole('cell', { name: 'No plugins uploaded.', exact: true })).toBeVisible();
  await expect(page.getByRole('alert')).toHaveCount(0);
  await expect(page.getByRole('button', { name: 'Retry', exact: true })).toHaveCount(0);
  await expectNoHorizontalOverflow(page);
});
