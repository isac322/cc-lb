import { expect, type Locator, type Page, test } from '@playwright/test';
import type { AggregateResponse, PoolHistoryResponse } from '../src/lib/api';
import { TIME_PRESET_SECONDS, type TimePreset } from '../src/lib/timePresets';
import { installFixtures, NOW } from './codexCreditsFixture';

const providers = ['anthropic', 'openai', 'gemini'] as const;
const RECENT_PROVIDER_READING = /^Provider data just now/;
const STALE_PROVIDER_READING = /^Provider data 10m ago/;
const windows = {
  anthropic: ['5h', '7d', '7d_fable'],
  openai: ['5h', '7d'],
  gemini: ['gemini_5h', 'gemini_7d', 'third_party_5h', 'third_party_7d'],
};

type TailState = 'fresh-tail' | 'numeric-stale' | 'gap-extended';
type PoolState = 'stale' | 'mixed' | 'recovered' | 'first-read' | TailState;
type Provider = (typeof providers)[number];
const surfaces = ['overview', ...providers] as const;

async function installPoolFixtures(page: Page, state: PoolState) {
  await installFixtures(page, []);
  await page.route('**/admin/v1/dashboard/summary?**', (route) => {
    const range = (new URL(route.request().url()).searchParams.get('range') ?? '24h') as TimePreset;
    return route.fulfill({
      json: {
        range,
        step: range === '1h' || range === '6h' ? 'minute' : 'hour',
        window_start_unix_secs: NOW - TIME_PRESET_SECONDS[range],
        window_end_unix_secs: NOW,
        totals: {
          request_count: 1, input_tokens: 100, output_tokens: 20, cache_creation_input_tokens: 0,
          cache_read_input_tokens: 0, error_count: 0, error_rate: 0, virtual_cost_micros: 0, avg_latency_ms: 10,
        },
        sparkline: { buckets: [] },
        observed: true,
      },
    });
  });
  await page.route('**/admin/v1/upstreams', (route) => route.fulfill({
    json: {
      upstreams: providers.map((provider) => ({
        id: `${provider}-pool-fixture`,
        name: `${provider} pool`,
        kind: `${provider}_oauth`,
        enabled: true,
        spec_revision: 1,
        base_url: `https://${provider}.example`,
        warmup_enabled: false,
        warmup_dialect_plugin: null,
        status: {
          last_apply_error: null,
          last_apply_at_unix_secs: null,
          last_warmup_at_unix_secs: null,
        },
      })),
    },
  }));
  await page.route('**/admin/v1/subscription-quotas/aggregate?**', (route) => {
    const provider = new URL(route.request().url()).searchParams.get('provider') as Provider;
    const response: AggregateResponse & { weighting: 'plan' | 'equal' } = {
      now_unix_secs: NOW,
      window_anchor_unix_secs: NOW,
      max_staleness_secs: 300,
      upstream_count: 2,
      caveats: [],
      weighting: provider === 'anthropic' ? 'plan' : 'equal',
      windows: (windows[provider] ?? []).map((window) => ({
        window,
        cc_window_start_unix_secs: NOW - 3600,
        cc_window_reset_unix_secs: NOW + 3600,
        used_tokens: 100,
        utilization: 0.8,
        utilization_percent: 80,
        capacity_to_now_tokens_estimate: 500,
        projected_capacity_tokens_estimate: 1000,
        remaining_to_now_tokens_estimate: 400,
        confidence: 'high',
        contributing_upstreams: 2,
        stale_upstreams: 0,
        missing_capacity_upstreams: 0,
        provider_lots: [{
          upstream_id: `${provider}-pool-fixture`,
          upstream_name: `${provider} pool`,
          window,
          source: 'api',
          state: 'fresh',
          provider_start_unix_secs: NOW - 3600,
          provider_reset_unix_secs: NOW + 3600,
          observed_at_unix_millis: NOW * 1000,
          utilization: 0.8,
          capacity_estimate_tokens: 1000,
          used_before_cc_window_tokens: 0,
          capacity_to_now_tokens_estimate: 500,
          projected_capacity_tokens_estimate: 1000,
          confidence: 'high',
          capacity_ratio: 1,
        }],
        caveats: [],
      })),
    };
    return route.fulfill({ json: response });
  });
  await page.route('**/admin/v1/subscription-quotas/pool-history?**', (route) => {
    const provider = new URL(route.request().url()).searchParams.get('provider') as Provider;
    const anthropic = provider === 'anthropic';
    const stale = !anthropic && state === 'stale';
    const mixed = !anthropic && state === 'mixed';
    const numericStale = !anthropic && state === 'numeric-stale';
    const tail = !anthropic && (state === 'fresh-tail' || numericStale || state === 'gap-extended');
    const value = anthropic ? 25 : stale ? null : mixed ? 31 : tail ? 40 : 64;
    const response: PoolHistoryResponse & { weighting: 'plan' | 'equal' } = {
      now_unix_secs: NOW,
      weighting: anthropic ? 'plan' : 'equal',
      windows: (windows[provider] ?? []).map((window) => ({
        window,
        latest: {
          snapshot_at_unix_secs: NOW,
          utilization: value == null ? null : value / 100,
          utilization_percent: value,
          contributing_upstreams: stale ? 0 : 1,
          eligible_upstreams: 2,
          stale_upstreams: stale ? 2 : mixed || numericStale ? 1 : 0,
          max_observed_at_unix_millis: stale ? null : NOW * 1000,
          ...(!anthropic ? {
            oldest_observed_at_unix_millis: (NOW - (stale || mixed || numericStale ? 600 : 30)) * 1000,
            max_staleness_secs: 300,
            missing_observation_upstreams: 0,
          } : {}),
        },
        series: !anthropic && state === 'first-read' ? [] : tail ? (
          state === 'gap-extended' ? [
            { snapshot_at_unix_secs: NOW - 3300, utilization_percent: 40 },
            { snapshot_at_unix_secs: NOW - 3000, utilization_percent: null },
            { snapshot_at_unix_secs: NOW - 2700, utilization_percent: 40 },
          ] : [
            { snapshot_at_unix_secs: NOW - 6300, utilization_percent: 40 },
            { snapshot_at_unix_secs: NOW - 2700, utilization_percent: 40 },
          ]
        ) : [
          { snapshot_at_unix_secs: NOW - 120, utilization_percent: anthropic ? 25 : 64 },
          { snapshot_at_unix_secs: NOW - 60, utilization_percent: anthropic ? 25 : null },
        ],
      })),
    };
    return route.fulfill({ json: response });
  });
}

for (const width of [390, 768, 1440]) {
  for (const state of ['stale', 'mixed', 'recovered', 'first-read'] as const) {
    for (const surface of surfaces) {
      test(`Provider pools keep ${state} facts on ${surface} at ${width}px`, async ({ page }, testInfo) => {
        await page.setViewportSize({ width, height: 900 });
        await installPoolFixtures(page, state);
        const visibleProviders = surface === 'overview' ? providers : [surface];
        // Initial unscoped requests belong to the loading installation view.
        // Wait for the actual provider cards' data before checking their DOM.
        const poolResponses = Promise.all(visibleProviders.flatMap((provider) =>
          ['aggregate', 'pool-history'].map((endpoint) =>
            page.waitForResponse((response) => {
              const url = new URL(response.url());
              return url.pathname === `/admin/v1/subscription-quotas/${endpoint}` &&
                url.searchParams.get('provider') === provider && response.ok();
            }).then((response) => response.finished()),
          ),
        ));
        await page.goto(surface === 'overview' ? '/' : `/providers/${surface}`);
        expect(await poolResponses).toEqual(visibleProviders.flatMap(() => [null, null]));

        for (const provider of visibleProviders) {
          const card = page.getByTestId(
            surface === 'overview' ? `provider-pool-${provider}` : 'pool-quota-card',
          );
          await expect(card).toBeVisible();
          const staleProvider = provider !== 'anthropic' && (state === 'stale' || state === 'mixed');
          const value = provider === 'anthropic' ? '25%' : state === 'stale' ? '—' : state === 'mixed' ? '31%' : '64%';
          if (surface === 'overview') {
            await expect(card.locator('[data-slot="provider-pool-headline"]')).toContainText(value);
          }
          await expect(card.locator('[data-slot="pool-quota-legend-used"]')).toHaveText(
            windows[provider].map(() => value),
          );
          // Headings share the rows' columns: each right edge lines up with its values.
          const columnEdges = await card.locator('[data-slot="pool-quota-legend-grid"]').evaluate((grid) =>
            [
              ['Used', '[data-slot="pool-quota-legend-used"]'],
              ['Resets in', '[data-slot="pool-quota-legend-reset"]'],
            ].map(([heading, cells]) => ({
              heading: Math.round(
                [...grid.querySelectorAll('[data-slot="pool-quota-legend-header"] > *')]
                  .find((el) => el.textContent === heading)
                  ?.getBoundingClientRect().right ?? Number.NaN,
              ),
              cells: [...grid.querySelectorAll(cells)].map((el) => Math.round(el.getBoundingClientRect().right)),
            })),
          );
          for (const column of columnEdges) {
            expect(column.cells).toEqual(column.cells.map(() => column.heading));
          }
          await expect(card.getByText('Stale reading', { exact: true })).toHaveCount(staleProvider ? 1 : 0);
          if (surface !== 'overview') {
            await expect(card.getByText(staleProvider ? STALE_PROVIDER_READING : RECENT_PROVIDER_READING)).toBeVisible();
          } else if (provider !== 'anthropic') {
            await expect(card.getByText(staleProvider ? 'Provider data 10m ago' : 'Provider data just now', { exact: true })).toBeVisible();
          }
          if (surface === 'overview' && provider !== 'anthropic' && state === 'stale') {
            await expect(card.getByLabel('Current pool usage unavailable')).toHaveText('—');
            await expect(card.locator('[data-slot="provider-pool-headline"]')).not.toContainText('used');
          }
          expect(await card.evaluate((element) => element.scrollWidth <= element.clientWidth)).toBe(true);
        }
        expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
        await page.evaluate(() => document.fonts.ready);
        await page.screenshot({
          path: testInfo.outputPath('provider-pool.png'),
          fullPage: true,
          animations: 'disabled',
        });
      });
    }
  }
}

async function expectTailGeometry(card: Locator, state: TailState | 'recovered', windowCount: number) {
  // Area's stroke overlays use area-curve; Line uses line-curve. Never inspect
  // filled areas: their closing edge reaches the baseline, not the recorded tail.
  const strokes = card.locator('path.recharts-line-curve, path.recharts-area-curve');
  await expect(strokes).toHaveCount(windowCount);
  const geometry = await strokes.evaluateAll((paths) => paths.map((element) => {
    const path = element as SVGPathElement;
    const grid = path.closest('svg')?.querySelector('.recharts-cartesian-grid-horizontal line');
    if (!grid) throw new Error('Pool chart plot grid is missing');
    const left = Number(grid.getAttribute('x1'));
    const right = Number(grid.getAttribute('x2'));
    const segments = (path.getAttribute('d') ?? '').split('M').slice(1).map((segment) => `M${segment}`);
    return {
      left,
      right,
      end: path.getPointAtLength(path.getTotalLength()).x,
      segments: segments.map((d) => {
        const segment = document.createElementNS('http://www.w3.org/2000/svg', 'path');
        segment.setAttribute('d', d);
        return {
          start: segment.getPointAtLength(0).x,
          end: segment.getPointAtLength(segment.getTotalLength()).x,
          length: segment.getTotalLength(),
        };
      }),
    };
  }));
  for (const stroke of geometry) {
    const plotWidth = stroke.right - stroke.left;
    expect(plotWidth).toBeGreaterThan(0);
    const tailFraction = state === 'numeric-stale' ? 0.25 : state === 'recovered' ? 3480 / 3600 : 1;
    expect(Math.abs(stroke.end - (stroke.left + plotWidth * tailFraction))).toBeLessThan(1);
    expect(stroke.segments).toHaveLength(state === 'gap-extended' ? 2 : 1);
    if (state === 'gap-extended') {
      const [before, after] = stroke.segments;
      expect(Math.abs(before.start - (stroke.left + plotWidth / 12))).toBeLessThan(1);
      expect(Math.abs(after.start - (stroke.left + plotWidth * 0.25))).toBeLessThan(1);
      expect(after.start).toBeGreaterThan(before.end);
      expect(after.length).toBeGreaterThan(0);
      expect(Math.abs(after.end - stroke.right)).toBeLessThan(1);
    } else if (state === 'numeric-stale' || state === 'recovered') {
      expect(stroke.end).toBeLessThan(stroke.right - 1);
    }
    if (state === 'fresh-tail' || state === 'numeric-stale') {
      expect(Math.abs(stroke.segments[0].start - stroke.left)).toBeLessThan(1);
      expect(stroke.segments[0].length).toBeGreaterThan(0);
    }
  }
}

for (const width of [390, 768, 1440]) {
  for (const state of ['fresh-tail', 'numeric-stale', 'gap-extended', 'recovered'] as const) {
    for (const surface of ['overview', 'openai', 'gemini'] as const) {
      test(`Provider pool ${state} strokes on ${surface} at ${width}px`, async ({ page }, testInfo) => {
        await page.setViewportSize({ width, height: 900 });
        await installPoolFixtures(page, state);
        await page.addInitScript(() => localStorage.setItem('cclb.timezone', 'UTC'));
        const visibleProviders = surface === 'overview' ? ['openai', 'gemini'] as const : [surface];
        const poolResponses = Promise.all(visibleProviders.map((provider) =>
          page.waitForResponse((response) => {
            const url = new URL(response.url());
            return url.pathname === '/admin/v1/subscription-quotas/pool-history' &&
              url.searchParams.get('provider') === provider &&
              url.searchParams.get('relative_range_secs') === '3600' && response.ok();
          }).then((response) => response.finished()),
        ));
        await page.goto(surface === 'overview' ? '/?range=1h' : `/providers/${surface}?range=1h`);
        expect(await poolResponses).toEqual(visibleProviders.map(() => null));
        for (const provider of visibleProviders) {
          const card = page.getByTestId(
            surface === 'overview' ? `provider-pool-${provider}` : 'pool-quota-card',
          );
          await expect(card).toBeVisible();
          await expectTailGeometry(card, state, windows[provider].length);
          await expect(card.locator('[data-slot="pool-quota-legend-used"]')).toHaveText(
            windows[provider].map(() => state === 'recovered' ? '64%' : '40%'),
          );
          if (surface === 'overview') {
            await expect(card.locator('[data-slot="provider-pool-headline"]')).toContainText(
              state === 'recovered' ? '64%' : '40%',
            );
          }
          await expect(card.getByText('Stale reading', { exact: true })).toHaveCount(
            state === 'numeric-stale' ? 1 : 0,
          );
          if (surface !== 'overview') {
            const proof = state === 'gap-extended'
              ? '3 snapshots · 11-11 11:05 → 11-11 11:15'
              : state === 'recovered'
                ? '2 snapshots · 11-11 11:58 → 11-11 11:59'
                : '1 snapshots · 11-11 11:15 → 11-11 11:15';
            await expect(card.getByTestId('pool-quota-captions')).toContainText(proof);
          }
          expect(await card.evaluate((element) => element.scrollWidth <= element.clientWidth)).toBe(true);
        }
        expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
        await page.evaluate(() => document.fonts.ready);
        await page.screenshot({
          path: testInfo.outputPath('provider-pool.png'),
          fullPage: true,
          animations: 'disabled',
        });
      });
    }
  }
}

// Rule A: weekly 100% while every account's Gemini 5h is absent keeps a
// neutral 5h legend row; the chart never plots the 5h window.
async function installGeminiWeeklyExhaustedPool(page: Page) {
  const reported = (window: string) => window === 'gemini_7d';
  await page.route('**/admin/v1/subscription-quotas/aggregate?**', (route) => {
    if (new URL(route.request().url()).searchParams.get('provider') !== 'gemini') return route.fallback();
    const response: AggregateResponse & { weighting: 'equal' } = {
      now_unix_secs: NOW, window_anchor_unix_secs: NOW, max_staleness_secs: 300,
      upstream_count: 1, caveats: [], weighting: 'equal',
      windows: windows.gemini.map((window) => ({
        window,
        cc_window_start_unix_secs: NOW - 3600,
        cc_window_reset_unix_secs: NOW + 3600,
        pool_pace_fraction: reported(window) ? 0.5 : null,
        next_reset_unix_secs: reported(window) ? NOW + 86400 : null,
        used_tokens: 100,
        utilization: reported(window) ? 1 : null,
        utilization_percent: reported(window) ? 100 : null,
        capacity_to_now_tokens_estimate: 500,
        projected_capacity_tokens_estimate: 1000,
        remaining_to_now_tokens_estimate: 0,
        confidence: 'high',
        contributing_upstreams: reported(window) ? 1 : 0,
        stale_upstreams: 0,
        missing_capacity_upstreams: 0,
        provider_lots: [{
          upstream_id: 'gemini-pool-fixture', upstream_name: 'gemini pool', window, source: 'api',
          state: reported(window) ? 'fresh' : 'absent',
          provider_start_unix_secs: null, provider_reset_unix_secs: reported(window) ? NOW + 86400 : null,
          observed_at_unix_millis: NOW * 1000, utilization: reported(window) ? 1 : null,
          capacity_estimate_tokens: 1000, used_before_cc_window_tokens: 0,
          capacity_to_now_tokens_estimate: 500, projected_capacity_tokens_estimate: 1000,
          confidence: 'high', capacity_ratio: 1,
        }],
        caveats: [],
      })),
    };
    return route.fulfill({ json: response });
  });
  await page.route('**/admin/v1/subscription-quotas/pool-history?**', (route) => {
    if (new URL(route.request().url()).searchParams.get('provider') !== 'gemini') return route.fallback();
    const response: PoolHistoryResponse & { weighting: 'equal' } = {
      now_unix_secs: NOW,
      weighting: 'equal',
      windows: windows.gemini.map((window) => ({
        window,
        latest: {
          snapshot_at_unix_secs: NOW,
          utilization: reported(window) ? 1 : null,
          utilization_percent: reported(window) ? 100 : null,
          contributing_upstreams: reported(window) ? 1 : 0,
          eligible_upstreams: reported(window) ? 1 : 0,
          stale_upstreams: 0,
          max_observed_at_unix_millis: NOW * 1000,
          oldest_observed_at_unix_millis: (NOW - 30) * 1000,
          max_staleness_secs: 300,
          missing_observation_upstreams: 0,
        },
        // Older 5h buckets must never be drawn as if current.
        series: [
          { snapshot_at_unix_secs: NOW - 120, utilization_percent: reported(window) ? 100 : 40 },
          { snapshot_at_unix_secs: NOW - 60, utilization_percent: reported(window) ? 100 : 40 },
        ],
      })),
    };
    return route.fulfill({ json: response });
  });
}

for (const width of [390, 768, 1440]) {
  for (const theme of ['light', 'dark'] as const) {
    for (const surface of ['overview', 'gemini'] as const) {
      test(`QA-1/9: Gemini pool keeps a not-reported 5h legend row on ${surface} at ${width}px ${theme}`, async ({ page }, testInfo) => {
        await page.setViewportSize({ width, height: 900 });
        await installPoolFixtures(page, 'recovered');
        await installGeminiWeeklyExhaustedPool(page);
        await page.addInitScript((value) => localStorage.setItem('cclb.theme', value), theme);
        await page.goto(surface === 'overview' ? '/' : '/providers/gemini');
        const card = page.getByTestId(surface === 'overview' ? 'provider-pool-gemini' : 'pool-quota-card');
        const rows = card.getByTestId('pool-quota-legend-slot');
        await expect(rows).toHaveCount(2);
        await expect(rows.nth(0)).toHaveAttribute('data-window', 'gemini_5h');
        await expect(rows.nth(0)).toHaveAttribute('data-placeholder', 'not-reported');
        await expect(rows.nth(0)).toContainText('Not reported');
        await expect(rows.nth(0).getByRole('meter')).toHaveCount(0);
        await expect(rows.nth(0)).not.toContainText(/%/);
        await expect(rows.nth(1).locator('[data-slot="pool-quota-legend-used"]')).toHaveText('100%');
        // Only the weekly window is plotted.
        await expect(card.locator('path.recharts-area-curve')).toHaveCount(1);
        expect(await card.evaluate((element) => element.scrollWidth <= element.clientWidth)).toBe(true);
        expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
        await page.screenshot({ path: testInfo.outputPath('gemini-pool-placeholder.png'), fullPage: true });
      });
    }
  }
}
