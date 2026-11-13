import { expect, type Locator, type Page, test } from '@playwright/test';
import {
  absentWindows, ID, installQuotaSettledFixtures, latestRow, LONG_MODEL,
  NAME, NOW, quotaSnapshot, rawMeter,
} from './quotaSettledStatesFixture';

function quota(page: Page) {
  return page.getByRole('region', { name: 'Quota', exact: true });
}
async function openQuota(page: Page) {
  await page.goto(`/upstreams?selectedId=${ID}`);
  await expect(quota(page)).toBeVisible();
}
async function expectNoOverflow(page: Page) {
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
  const section = quota(page);
  expect(await section.evaluate((el) => el.scrollWidth <= el.clientWidth)).toBe(true);
  for (const row of await section.getByRole('listitem').all()) {
    expect(await row.evaluate((el) => el.scrollWidth <= el.clientWidth)).toBe(true);
  }
}
async function expectNeutralRaw(section: Locator) {
  const raw = section.locator('[data-period-unknown]');
  await expect(raw.locator('[data-slot="pace-marker"]')).toHaveCount(0);
  await expect(raw.locator('[class*="warn"], [class*="danger"]')).toHaveCount(0);
  for (const meter of await raw.getByRole('meter').all()) {
    await expect(meter).not.toHaveAttribute('aria-valuetext', /even pace/);
  }
}
async function expectReadableRaw(section: Locator) {
  await expectReadable(section.locator('[data-period-unknown]'), '[title], .text-caption, .text-text');
}
async function expectReadable(rowsLocator: Locator, leafSelector: string) {
  const ratios = await rowsLocator.evaluateAll((rows, selector) => {
    const canvas = document.createElement('canvas');
    canvas.width = canvas.height = 1;
    const ctx = canvas.getContext('2d');
    if (!ctx) throw new Error('Canvas color conversion unavailable');
    function rgba(color: string): number[] {
      ctx!.clearRect(0, 0, 1, 1);
      ctx!.fillStyle = color;
      ctx!.fillRect(0, 0, 1, 1);
      return [...ctx!.getImageData(0, 0, 1, 1).data].map((v) => v / 255);
    }
    function luminance(rgb: number[]) {
      const linear = rgb.slice(0, 3).map((c) => c <= 0.04045 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4);
      return linear[0] * 0.2126 + linear[1] * 0.7152 + linear[2] * 0.0722;
    }
    return rows.flatMap((row) => [...row.querySelectorAll<HTMLElement>(selector)]
      .filter((el) => el.textContent?.trim() && el.childElementCount === 0)
      .map((el) => {
        const ancestors: Element[] = [];
        for (let node: Element | null = el; node; node = node.parentElement) ancestors.unshift(node);
        let background = [1, 1, 1];
        for (const node of ancestors) {
          const color = rgba(getComputedStyle(node).backgroundColor);
          background = background.map((v, i) => color[i] * color[3] + v * (1 - color[3]));
        }
        const color = rgba(getComputedStyle(el).color);
        const foreground = background.map((v, i) => color[i] * color[3] + v * (1 - color[3]));
        const a = luminance(foreground);
        const b = luminance(background);
        return { text: el.textContent, ratio: (Math.max(a, b) + 0.05) / (Math.min(a, b) + 0.05) };
      }));
  }, leafSelector);
  expect(ratios.length).toBeGreaterThan(0);
  for (const result of ratios) expect(result.ratio, result.text ?? '').toBeGreaterThanOrEqual(4.5);
}

// QA-22: twelve layout/theme/locale combinations plus five state-transition cases.
for (const width of [390, 768, 1440]) {
  for (const theme of ['light', 'dark'] as const) {
    for (const locale of ['en-US', 'ko-KR'] as const) {
      test(`mixed canonical and raw quota: ${width}px ${theme} ${locale}`, async ({ page }, testInfo) => {
        await page.setViewportSize({ width, height: 900 });
        const fixtures = await installQuotaSettledFixtures(page, {
          theme, locale,
          latest: latestRow([quotaSnapshot()], [rawMeter(), rawMeter({
            group: 'third_party', model_id: null, bucket_id: 'provider-bucket-with-a-long-identifier',
            origin: 'retrieve_user_quota_summary', remaining_fraction: 0, utilization: 1,
            reset_time_unix_secs: null,
          })]),
        });
        await openQuota(page);
        const section = quota(page);
        // Canonical gemini_5h, its unreported weekly sibling placeholder, two raw meters.
        await expect(section.getByRole('listitem')).toHaveCount(4);
        await expect(section.locator('[data-window="gemini_7d"]')).toHaveAttribute('data-placeholder', 'no-reading');
        // The status is never clipped by a squeezed meter column.
        const placeholderText = section.locator('[data-slot="window-placeholder"]');
        await expect(placeholderText).toHaveText('No reading');
        expect(await placeholderText.evaluate((el) => el.scrollWidth <= el.clientWidth)).toBe(true);
        await expect(section.getByTestId('pace-legend')).toHaveCount(1);
        await expect(section.locator('[data-slot="pace-marker"]')).toHaveCount(1);
        await expect(section.getByTestId('quota-freshness')).toBeVisible();
        await expect(section.getByTitle(LONG_MODEL, { exact: true })).toBeVisible();
        await expect(section.locator('[data-period-unknown="gemini"]').getByRole('meter')).toHaveAttribute('aria-valuetext', '99% used');
        await expect(section.locator('[data-period-unknown="third_party"]').getByRole('meter')).toHaveAttribute('aria-valuetext', '100% used');
        await expect(section.locator('[data-period-unknown="gemini"]')).toContainText(locale === 'ko-KR' ? '2시간 후 초기화' : 'Resets in 2h');
        await expect(section.locator('[data-period-unknown="third_party"]')).not.toContainText('Not started');
        const resetValue = section.locator('[data-period-unknown="gemini"] [data-slot="window-reset-value"]');
        await expect(resetValue).toHaveClass(locale === 'ko-KR' ? /text-right/ : /text-left/);
        await expectNeutralRaw(section);
        await expectNoOverflow(page);
        await expectReadableRaw(section);
        expect(fixtures.requests.some((url) => url.searchParams.get('windows')?.includes('gemini_5h'))).toBe(true);
        await page.screenshot({ path: testInfo.outputPath('settled-quota.png'), fullPage: true });
      });
    }
  }
}

test('settled missing, empty, absent and unobserved data have distinct quiet states', async ({ page }) => {
  const fixtures = await installQuotaSettledFixtures(page, { provider: 'openai' });
  await openQuota(page);
  const section = quota(page);
  await expect(section.getByText(`No subscription quota data for ${NAME}`, { exact: true })).toBeVisible();
  await fixtures.poll(latestRow([]));
  await expect(section.getByText(`No subscription quota data for ${NAME}`, { exact: true })).toBeVisible();
  await fixtures.poll(latestRow(absentWindows('openai')));
  await expect(section.getByText('Not offered', { exact: true })).toBeVisible();
  await fixtures.poll(latestRow(absentWindows('openai').map((snap) => ({ ...snap, state: 'unobserved' }))));
  await expect(section.getByText('No quota reading yet', { exact: true })).toBeVisible();
  await expect(section.getByTestId('pace-legend')).toHaveCount(0);
  await expect(section.getByTestId('quota-freshness')).toHaveCount(0);
  await expect(section.getByTestId('quota-snapshot-grid')).toHaveCount(0);
  await expectNoOverflow(page);
});

test('raw-only rows survive empty and all-absent windows with quiet missing or expired resets', async ({ page }) => {
  const fixtures = await installQuotaSettledFixtures(page, { latest: latestRow([], [rawMeter({ reset_time_unix_secs: null })]) });
  await openQuota(page);
  const section = quota(page);
  await expect(section.getByRole('listitem')).toHaveCount(1);
  await expect(section.getByTestId('quota-freshness')).toBeVisible();
  await fixtures.poll(latestRow(absentWindows('gemini'), [rawMeter({
    reset_time_unix_secs: NOW - 1, state: 'stale', observed_at_unix_millis: (NOW - 600) * 1000, age_secs: 600,
  })]));
  await expect(section.getByRole('listitem')).toHaveCount(1);
  await expect(section.getByTestId('quota-freshness')).toContainText('Stale:');
  await expect(section.locator('[data-period-unknown]')).not.toContainText(/Not started|Resets in/);
  await expect(section.getByTestId('pace-legend')).toHaveCount(0);
  await expectNeutralRaw(section);
  await expectNoOverflow(page);
});

test('live polls transition not-offered to raw to canonical and back without reloading', async ({ page }) => {
  let navigations = 0;
  page.on('framenavigated', (frame) => { if (frame === page.mainFrame()) navigations += 1; });
  const fixtures = await installQuotaSettledFixtures(page, { latest: latestRow(absentWindows('gemini')) });
  await openQuota(page);
  const section = quota(page);
  await expect(section.getByText('Not offered', { exact: true })).toBeVisible();
  const initialNavigations = navigations;
  await fixtures.poll(latestRow(absentWindows('gemini'), [rawMeter()]));
  await expect(section.locator('[data-period-unknown]')).toHaveCount(1);
  await expect(section.getByTestId('pace-legend')).toHaveCount(0);
  await fixtures.poll(latestRow([quotaSnapshot()]));
  await expect(section.locator('[data-period-unknown]')).toHaveCount(0);
  await expect(section.locator('[data-window="gemini_5h"]')).toBeVisible();
  await expect(section.getByTestId('pace-legend')).toHaveCount(1);
  await fixtures.poll(latestRow(absentWindows('gemini')));
  await expect(section.getByText('Not offered', { exact: true })).toBeVisible();
  await expect(section.getByTestId('quota-freshness')).toHaveCount(0);
  await expect(section.getByTestId('pace-legend')).toHaveCount(0);
  expect(navigations).toBe(initialNavigations);
});

test('credits-only and not-started rows never advertise pace', async ({ page }) => {
  const fixtures = await installQuotaSettledFixtures(page, { provider: 'openai', latest: latestRow([
    ...absentWindows('openai'), quotaSnapshot({ window: 'overage', utilization: null, credits: {
      has_credits: true, unlimited: false, limit_reached: false, balance: '20',
      approx_local_messages: null, approx_cloud_messages: null,
    } }),
  ]) });
  await openQuota(page);
  const section = quota(page);
  await expect(section.getByTestId('codex-credit-facts')).toBeVisible();
  await expect(section.getByRole('meter')).toHaveCount(0);
  await expect(section.getByTestId('pace-legend')).toHaveCount(0);
  await fixtures.poll(latestRow([quotaSnapshot({ window: '5h', utilization: 0, resets_at_unix_secs: null })]));
  await expect(section.getByText('Not started', { exact: true })).toBeVisible();
  await expect(section.locator('[data-slot="pace-marker"]')).toHaveCount(0);
  await expect(section.getByTestId('pace-legend')).toHaveCount(0);
  await fixtures.poll(latestRow([quotaSnapshot({ window: '5h' })]));
  await expect(section.getByTestId('pace-legend')).toHaveCount(1);
  await expectNoOverflow(page);
});

test('Anthropic placeholders remain visible until explicit canonical absence', async ({ page }) => {
  const fixtures = await installQuotaSettledFixtures(page, { provider: 'anthropic', latest: latestRow(
    absentWindows('anthropic').map((snap) => ({ ...snap, state: 'unobserved', observed_at_unix_millis: null })),
  ) });
  await openQuota(page);
  const section = quota(page);
  await expect(section.getByRole('listitem')).toHaveCount(2);
  await expect(section.getByRole('meter')).toHaveCount(2);
  await expect(section.getByTestId('quota-freshness')).toHaveCount(0);
  await expect(section.getByTestId('pace-legend')).toHaveCount(0);
  await fixtures.poll(latestRow(absentWindows('anthropic')));
  await expect(section.getByText('Not offered', { exact: true })).toBeVisible();
  await expect(section.getByTestId('quota-snapshot-grid')).toHaveCount(0);
  await expectNoOverflow(page);
});

// Rule A: a Gemini group keeps the sibling of a reported window as a neutral
// placeholder. Never 0%, never inferred from the weekly 100%, never a meter.
const weeklyExhausted = (window: 'gemini_7d' | 'third_party_7d') =>
  quotaSnapshot({ window, utilization: 1, resets_at_unix_secs: NOW + 3 * 86400 });
const absentWindow = (window: 'gemini_5h' | 'third_party_5h' | 'gemini_7d' | 'third_party_7d') =>
  quotaSnapshot({ window, state: 'absent', utilization: null, resets_at_unix_secs: null });

async function expectPlaceholder(page: Page, window: string, state: 'not-reported' | 'no-reading') {
  const text = state === 'not-reported' ? 'Not reported' : 'No reading';
  const row = quota(page).locator(`[role="listitem"][data-window="${window}"]`);
  await expect(row).toHaveAttribute('data-placeholder', state);
  await expect(row).toContainText(text);
  await expect(row).not.toContainText(/%|Resets|Not started/);
  await expect(row.getByRole('meter')).toHaveCount(0);
  await expect(row.locator('[data-slot="pace-marker"]')).toHaveCount(0);
}

for (const width of [390, 768, 1440]) {
  for (const theme of ['light', 'dark'] as const) {
    test(`QA-1/9: weekly 100% with 5h absent keeps a neutral placeholder: ${width}px ${theme}`, async ({ page }, testInfo) => {
      await page.setViewportSize({ width, height: 900 });
      await installQuotaSettledFixtures(page, {
        theme,
        latest: latestRow([
          absentWindow('gemini_5h'), weeklyExhausted('gemini_7d'),
          absentWindow('third_party_5h'), absentWindow('third_party_7d'),
        ]),
      });
      await openQuota(page);
      const section = quota(page);
      await expect(section.getByRole('listitem')).toHaveCount(2);
      await expectPlaceholder(page, 'gemini_5h', 'not-reported');
      await expect(section.locator('[data-window="gemini_7d"]').getByRole('meter')).toHaveAttribute('aria-valuetext', /^100% used/);
      await expect(section.getByText('Not offered', { exact: true })).toHaveCount(0);
      await expectReadable(section.locator('[data-placeholder]'), '[data-slot="window-placeholder"], [data-slot="window-placeholder"] *');
      if (width >= 768) {
        const cell = page.getByTestId('upstream-row-quota').locator('[data-window="gemini_5h"]');
        await expect(cell).toHaveAttribute('data-placeholder', 'not-reported');
        await expect(cell).toBeVisible();
        await expect(cell).toHaveText('Gemini 5h—Not reported');
      }
      await expectNoOverflow(page);
      await page.screenshot({ path: testInfo.outputPath('gemini-placeholder.png'), fullPage: true });
    });
  }
}

test('QA-1/3/4: placeholders follow absent, disabled, unobserved and whole-group-absent polls', async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  const fixtures = await installQuotaSettledFixtures(page, {
    latest: latestRow([absentWindow('gemini_5h'), weeklyExhausted('gemini_7d')]),
  });
  await openQuota(page);
  const section = quota(page);
  await expectPlaceholder(page, 'gemini_5h', 'not-reported');

  // A disabled 5h bucket is persisted as `absent`, like an omitted one; per group.
  await fixtures.poll(latestRow([
    quotaSnapshot({ window: 'gemini_5h', utilization: 0.3 }), quotaSnapshot({ window: 'gemini_7d', utilization: 0.5 }),
    absentWindow('third_party_5h'), weeklyExhausted('third_party_7d'),
  ]));
  await expect(section.getByRole('listitem')).toHaveCount(4);
  await expect(section.getByRole('meter')).toHaveCount(3);
  await expectPlaceholder(page, 'third_party_5h', 'not-reported');

  await fixtures.poll(latestRow([
    quotaSnapshot({ window: 'gemini_5h', state: 'unobserved', utilization: null, resets_at_unix_secs: null, observed_at_unix_millis: null }),
    weeklyExhausted('gemini_7d'),
  ]));
  await expectPlaceholder(page, 'gemini_5h', 'no-reading');

  await fixtures.poll(latestRow([
    absentWindow('gemini_5h'), absentWindow('gemini_7d'),
    quotaSnapshot({ window: 'third_party_5h', utilization: 0.9 }), quotaSnapshot({ window: 'third_party_7d', utilization: 0.3 }),
  ]));
  await expect(section.getByRole('listitem')).toHaveCount(2);
  await expect(section.locator('[data-placeholder]')).toHaveCount(0);
  await expect(section.locator('[data-window^="gemini_"]')).toHaveCount(0);

  await fixtures.poll(latestRow(absentWindows('gemini')));
  await expect(section.getByText('Not offered', { exact: true })).toBeVisible();
  await expectNoOverflow(page);
});

// A t2 poll with weekly 100% and an unlabeled bucket per group omits 5h; the
// backend keeps the t1 5h rows, which must read as stale, never current.
function retainedPoll(fiveHourObservedAt: number) {
  const fiveHour = (window: 'gemini_5h' | 'third_party_5h') => quotaSnapshot({
    window, utilization: 0.2, observed_at_unix_millis: fiveHourObservedAt,
    age_secs: NOW - fiveHourObservedAt / 1000,
  });
  return latestRow(
    [fiveHour('gemini_5h'), weeklyExhausted('gemini_7d'), fiveHour('third_party_5h'), weeklyExhausted('third_party_7d')],
    [rawMeter({ reset_time_unix_secs: null }), rawMeter({ group: 'third_party', reset_time_unix_secs: null })],
  );
}

for (const width of [768, 1440]) {
  for (const theme of ['light', 'dark'] as const) {
    test(`retained Gemini 5h after a weekly-100% poll reads stale, never current: ${width}px ${theme}`, async ({ page }, testInfo) => {
      await page.setViewportSize({ width, height: 900 });
      const fixtures = await installQuotaSettledFixtures(page, {
        theme,
        latest: latestRow([
          quotaSnapshot({ window: 'gemini_5h', utilization: 0.2 }), quotaSnapshot({ window: 'gemini_7d', utilization: 0.6 }),
          quotaSnapshot({ window: 'third_party_5h', utilization: 0.2 }), quotaSnapshot({ window: 'third_party_7d', utilization: 0.6 }),
        ]),
      });
      await openQuota(page);
      const section = quota(page);
      await expect(section.locator('[data-state="stale"]')).toHaveCount(0);

      await fixtures.poll(retainedPoll((NOW - 7200) * 1000));
      const list = page.getByTestId('upstream-row-quota');
      for (const window of ['gemini_5h', 'third_party_5h']) {
        const row = section.locator(`[role="listitem"][data-window="${window}"]`);
        await expect(row).toHaveAttribute('data-state', 'stale');
        await expect(row).toContainText('Stale · 2h ago');
        await expect(row).not.toContainText(/Resets|Not started/);
        await expect(row.locator('[data-slot="pace-marker"]')).toHaveCount(0);
        await expect(row.getByRole('meter')).toHaveAccessibleName(/stale reading$/);
        const cell = list.locator(`[data-window="${window}"]`);
        await expect(cell).toHaveAttribute('data-state', 'stale');
        await expect(cell).toBeVisible();
        await expect(cell.locator('[aria-hidden="true"]')).toHaveText('Stale');
        await expect(cell).toHaveAttribute('title', /last reading 20% · Stale reading, 2h ago$/);
      }
      await expectReadable(section.locator('[data-state="stale"]'), '[data-slot="window-stale"]');
      await expectReadable(list.locator('[data-state="stale"]'), '[aria-hidden="true"]');
      for (const cell of await list.locator('[data-window]').all()) {
        expect(await cell.evaluate((el) => el.scrollWidth <= el.clientWidth)).toBe(true);
      }
      await expectNoOverflow(page);
      await page.screenshot({ path: testInfo.outputPath('gemini-retained.png'), fullPage: true });

      // Healthy control: the same poll with 5h observed alongside its group.
      await fixtures.poll(retainedPoll(NOW * 1000));
      await expect(section.locator('[data-state="stale"]')).toHaveCount(0);
      await expect(list.locator('[data-window="gemini_5h"]')).toHaveText('Gemini 5h20%');
      await expect(section.locator('[role="listitem"][data-window="gemini_5h"]')).toContainText('20% used');
    });
  }
}
