import { expect, type Locator, type Page, test } from '@playwright/test';
import {
  installFixtures,
  PENDING_GEMINI_FLEET,
  STALE_AND_ABSENT_FLEET,
  WEEKLY_ONLY_FLEET,
} from './overviewSharedQuotaColumnsFixture';

const VIEWPORTS = [
  { width: 390, height: 844 },
  { width: 768, height: 1024 },
  { width: 1440, height: 900 },
] as const;
const THEMES = ['dark', 'light'] as const;

async function expectNoHorizontalOverflow(page: Page) {
  expect(
    await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth),
  ).toBe(true);
  const overflowing = await page
    .locator(
      '[data-testid="upstream-usage-row"], [data-testid="usage-cell"], ' +
        '[data-testid="usage-cell-group"], [data-testid="usage-slot-placeholder"], ' +
        '[data-testid="usage-cell-status"]',
    )
    .evaluateAll((elements) =>
      elements
        .filter((element) => element.scrollWidth > element.clientWidth + 1)
        .map((element) => element.getAttribute('data-window') ?? element.textContent),
    );
  expect(overflowing).toEqual([]);
}

const slotWindows = (cell: Locator) =>
  cell.locator('[data-window]').evaluateAll((slots) =>
    slots.map((slot) => slot.getAttribute('data-window')),
  );

const headerLabels = (page: Page) =>
  page.getByTestId('upstream-usage-header').locator(':scope > span').allTextContents();

for (const viewport of VIEWPORTS) {
  for (const theme of THEMES) {
    test(`all-provider Overview maps every provider into shared 5h/7d columns at ${viewport.width}px in ${theme}`, async ({ page }, testInfo) => {
      await page.setViewportSize(viewport);
      await installFixtures(page, { theme });
      await page.goto('/');
      await expect(page.locator('html')).toHaveAttribute('data-theme', theme);

      const rows = page.getByTestId('upstream-usage-row');
      await expect(rows).toHaveCount(5);
      // Wait for the quota poll: while pending the OpenAI row keeps the Codex
      // credits column open until its credit reading is known.
      await expect.poll(() => headerLabels(page)).toEqual(['Upstream', '5h', '7d', 'Coupons']);
      // Pool cards keep their own legends; the table never shows the
      // provider-page column labels.
      await expect(rows.getByText(/Gemini 5h|Claude·OSS/)).toHaveCount(0);

      const claude = rows.filter({ hasText: 'Claude Max' });
      await expect.poll(() => slotWindows(claude.locator('[data-column="7d"]'))).toEqual(['7d', '7d_fable']);
      await expect(claude.getByText('Fable', { exact: true })).toBeVisible();
      await expect(claude.getByRole('meter', { name: '5h window used', exact: true })).toHaveAttribute('aria-valuenow', '10');
      await expect(claude.getByRole('meter', { name: '7d window used', exact: true })).toHaveAttribute('aria-valuenow', '20');
      await expect(claude.getByRole('meter', { name: '7d (Fable) window used', exact: true })).toHaveAttribute('aria-valuenow', '60');

      const codex = rows.filter({ hasText: 'Codex Pro' });
      await expect(codex.getByRole('meter', { name: '5h window used', exact: true })).toHaveAttribute('aria-valuenow', '40');
      await expect(codex.getByRole('meter', { name: '7d window used', exact: true })).toHaveAttribute('aria-valuenow', '50');

      // Both Gemini groups: separate slots, same order in both columns.
      const gemini = rows.filter({ hasText: 'Gemini both groups' });
      await expect.poll(() => slotWindows(gemini.locator('[data-column="5h"]'))).toEqual(['gemini_5h', 'third_party_5h']);
      await expect.poll(() => slotWindows(gemini.locator('[data-column="7d"]'))).toEqual(['gemini_7d', 'third_party_7d']);
      await expect(gemini.getByText('Gemini models', { exact: true })).toHaveCount(2);
      await expect(gemini.getByText('Claude & GPT-OSS', { exact: true })).toHaveCount(2);
      await expect(gemini.getByText('Gemini models', { exact: true }).first()).toBeVisible();
      await expect(gemini.getByRole('meter', { name: 'Gemini models · 5-hour limit window used', exact: true })).toHaveAttribute('aria-valuenow', '30');
      await expect(gemini.getByRole('meter', { name: 'Claude and GPT-OSS models · 5-hour limit window used', exact: true })).toHaveAttribute('aria-valuenow', '90');

      // Weekly exhausted: the withheld 5h stays as a neutral placeholder.
      const weekly = rows.filter({ hasText: 'Gemini weekly exhausted' });
      await expect.poll(() => slotWindows(weekly.locator('[data-column="5h"]'))).toEqual(['gemini_5h', 'third_party_5h']);
      const notReported = weekly.locator('[data-testid="usage-slot-placeholder"][data-window="gemini_5h"]');
      await expect(notReported).toBeVisible();
      await expect(notReported).toContainText('Not reported');
      await expect(notReported).not.toContainText('%');
      await expect(notReported.getByRole('meter')).toHaveCount(0);
      const noReading = weekly.locator('[data-testid="usage-slot-placeholder"][data-window="third_party_5h"]');
      await expect(noReading).toContainText('No reading');
      await expect(noReading.getByRole('meter')).toHaveCount(0);
      await expect(weekly.getByRole('meter', { name: 'Gemini models · 5-hour limit window used', exact: true })).toHaveCount(0);
      await expect(weekly.getByRole('meter', { name: 'Gemini models · weekly limit window used', exact: true })).toHaveAttribute('aria-valuenow', '100');

      await expect(rows.filter({ hasText: 'Claude API key' }).getByText('No subscription quota', { exact: true })).toBeVisible();

      await expectNoHorizontalOverflow(page);
      const table = page.getByRole('list', { name: 'Upstream usage per window' }).locator('..');
      await table.screenshot({ path: testInfo.outputPath(`overview-shared-${viewport.width}-${theme}.png`) });
    });
  }
}

for (const provider of ['gemini', 'anthropic'] as const) {
  test(`${provider} provider page keeps its own window columns`, async ({ page }) => {
    await page.setViewportSize({ width: 1440, height: 900 });
    await installFixtures(page, { theme: 'dark' });
    await page.goto(`/providers/${provider}`);
    await expect(page.getByTestId('upstream-usage-row').first()).toBeVisible();
    await expect.poll(() => headerLabels(page)).toEqual(
      provider === 'gemini'
        ? ['Upstream', 'Gemini 5h', 'Gemini 7d', 'Claude·OSS 5h', 'Claude·OSS 7d', 'Coupons']
        : ['Upstream', '5h', '7d', '7d (Fable)', 'Coupons'],
    );
    await expect(page.locator('[data-testid="usage-cell-group"]')).toHaveCount(0);
  });
}

for (const viewport of VIEWPORTS) {
  for (const theme of THEMES) {
    test(`weekly-only fleet keeps both shared columns at ${viewport.width}px in ${theme}`, async ({ page }, testInfo) => {
      await page.setViewportSize(viewport);
      await installFixtures(page, { theme, fleet: WEEKLY_ONLY_FLEET });
      await page.goto('/');
      await expect(page.locator('html')).toHaveAttribute('data-theme', theme);
      const rows = page.getByTestId('upstream-usage-row');
      await expect(rows).toHaveCount(4);
      const claude = rows.filter({ hasText: 'Claude weekly plan' });
      await expect(claude.getByRole('meter', { name: '7d window used', exact: true })).toHaveAttribute('aria-valuenow', '20');
      await expect.poll(() => headerLabels(page)).toEqual(['Upstream', '5h', '7d', 'Coupons']);

      // Proven-absent 5h: a quiet dash named "Not offered", never a figure.
      const absent = claude.locator('[data-testid="usage-cell-status"][data-window="5h"]');
      await expect(absent).toBeVisible();
      await expect(absent).toHaveAttribute('data-state', 'not-offered');
      await expect(absent).toContainText('—');
      await expect(absent).toContainText('Not offered');
      await expect(absent).not.toContainText('%');
      await expect(claude.getByRole('meter', { name: '5h window used', exact: true })).toHaveCount(0);

      const codex = rows.filter({ hasText: 'Codex weekly plan' });
      const unread = codex.locator('[data-testid="usage-cell-status"][data-window="5h"]');
      await expect(unread).toBeVisible();
      await expect(unread).toHaveAttribute('data-state', 'no-reading');
      await expect(unread).toContainText('No reading');
      await expect(codex.getByRole('meter', { name: '5h window used', exact: true })).toHaveCount(0);
      await expect(codex.getByRole('meter', { name: '7d window used', exact: true })).toHaveAttribute('aria-valuenow', '30');

      // No Gemini group has a reading: both cells read "No reading".
      const gemini = rows.filter({ hasText: 'Gemini unread' });
      await expect(gemini.locator('[data-testid="usage-cell-status"]')).toHaveCount(2);
      await expect(gemini.getByText('No reading', { exact: true })).toHaveCount(2);
      await expect(gemini.getByRole('meter')).toHaveCount(0);

      await expectNoHorizontalOverflow(page);
      const table = page.getByRole('list', { name: 'Upstream usage per window' }).locator('..');
      await table.screenshot({ path: testInfo.outputPath(`overview-weekly-only-${viewport.width}-${theme}.png`) });
    });
  }
}

for (const viewport of VIEWPORTS) {
  for (const theme of THEMES) {
    test(`stale readings carry a Stale marker and all-absent Gemini reads Not offered at ${viewport.width}px in ${theme}`, async ({ page }, testInfo) => {
      await page.setViewportSize(viewport);
      await installFixtures(page, { theme, fleet: STALE_AND_ABSENT_FLEET });
      await page.goto('/');
      await expect(page.locator('html')).toHaveAttribute('data-theme', theme);
      const rows = page.getByTestId('upstream-usage-row');
      await expect(rows).toHaveCount(3);

      // Stale Anthropic 5h: the old figure is labelled, never shown as current.
      const claude = rows.filter({ hasText: 'Claude stale 5h' });
      const claude5h = claude.locator('[data-testid="usage-cell"][data-window="5h"]');
      await expect(claude5h).toHaveAttribute('data-state', 'stale');
      await expect(claude5h.getByText('Stale · 12m ago', { exact: true })).toBeVisible();
      await expect(claude5h).not.toContainText('resets in');
      const staleMeter = claude.getByRole('meter', { name: '5h window used, stale reading', exact: true });
      await expect(staleMeter).toHaveAttribute('aria-valuenow', '42');
      await expect(staleMeter.locator('[data-slot="pace-marker"]')).toHaveCount(0);
      await expect(claude.getByRole('meter', { name: '7d window used', exact: true }).locator('[data-slot="pace-marker"]')).toHaveCount(1);

      // Stale Gemini slot beside a fresh 7d slot of the same group.
      const gemini = rows.filter({ hasText: 'Gemini stale 5h' });
      const staleSlot = gemini.locator('[data-testid="usage-cell"][data-window="gemini_5h"]');
      await expect(staleSlot).toHaveAttribute('data-state', 'stale');
      await expect(staleSlot.getByText('Stale · 3h ago', { exact: true })).toBeVisible();
      await expect(staleSlot.getByRole('meter', { name: 'Gemini models · 5-hour limit window used, stale reading', exact: true }).locator('[data-slot="pace-marker"]')).toHaveCount(0);
      const fresh7d = gemini.locator('[data-testid="usage-cell"][data-window="gemini_7d"]');
      await expect(fresh7d).not.toHaveAttribute('data-state', 'stale');
      await expect(fresh7d).toContainText('resets in');

      // All four Gemini windows absent: Not offered in both columns, no blank.
      const absent = rows.filter({ hasText: 'Gemini not offered' }).getByTestId('usage-cell-status');
      await expect(absent).toHaveCount(2);
      for (const window of ['5h', '7d']) {
        const cell = absent.and(page.locator(`[data-window="${window}"]`));
        await expect(cell).toHaveAttribute('data-state', 'not-offered');
        await expect(cell).toContainText('—');
        await expect(cell).toContainText('Not offered');
        await expect(cell).not.toContainText('No reading');
      }

      await expectNoHorizontalOverflow(page);
      const table = page.getByRole('list', { name: 'Upstream usage per window' }).locator('..');
      await table.screenshot({ path: testInfo.outputPath(`overview-stale-absent-${viewport.width}-${theme}.png`) });
    });
  }
}

for (const viewport of [VIEWPORTS[0], VIEWPORTS[2]]) {
  test(`pending all-Gemini subscription fleet shows only 5h/7d skeletons at ${viewport.width}px`, async ({ page }) => {
    await page.setViewportSize(viewport);
    await installFixtures(page, { theme: 'dark', fleet: PENDING_GEMINI_FLEET, holdLatest: true });
    await page.goto('/');
    const rows = page.getByTestId('upstream-usage-row');
    await expect(rows).toHaveCount(3);
    expect(await headerLabels(page)).toEqual(['Upstream', '5h', '7d', 'Coupons']);
    for (const name of ['Gemini both groups', 'Gemini weekly exhausted']) {
      const row = rows.filter({ hasText: name });
      await expect.poll(() => slotWindows(row)).toEqual(['5h', '7d']);
      await expect(row.locator('.skeleton')).toHaveCount(6);
    }
    await expect(page.getByText(/7d \(Fable\)|Codex credits/)).toHaveCount(0);
    await expect(rows.getByRole('meter')).toHaveCount(0);
    await expectNoHorizontalOverflow(page);
  });
}
