import { expect, test } from '@playwright/test';
import type { AggregateResponse, PoolHistoryResponse } from '../src/lib/api';
import { installFixtures, NOW } from './codexCreditsFixture';

const providers = ['anthropic', 'openai', 'gemini'] as const;
const RECENT_PROVIDER_READING = /^Provider data just now/;
const STALE_PROVIDER_READING = /^Provider data 10m ago/;
const windows = {
  anthropic: ['5h', '7d', '7d_fable'],
  openai: ['5h', '7d'],
  gemini: ['gemini_5h', 'gemini_7d', 'third_party_5h', 'third_party_7d'],
};

for (const { width, state } of [390, 768, 1440].flatMap((width) =>
  (['stale', 'mixed', 'recovered', 'first-read'] as const).map((state) => ({
    width,
    state,
  })),
)) {
  test(`Provider pools keep ${state} facts at ${width}px`, async ({ page }) => {
    await page.setViewportSize({ width, height: 900 });
    await installFixtures(page, []);
    await page.route('**/admin/v1/upstreams', (route) => route.fulfill({
      json: { upstreams: providers.map((provider) => ({
        id: `${provider}-pool-fixture`, name: `${provider} pool`,
        kind: `${provider}_oauth`, enabled: true, spec_revision: 1,
        base_url: `https://${provider}.example`, warmup_enabled: false,
        warmup_dialect_plugin: null,
        status: { last_apply_error: null, last_apply_at_unix_secs: null, last_warmup_at_unix_secs: null },
      })) },
    }));
    await page.route('**/admin/v1/subscription-quotas/aggregate?**', (route) => {
      const provider = new URL(route.request().url()).searchParams.get('provider') as typeof providers[number];
      const response: AggregateResponse = {
        now_unix_secs: NOW, window_anchor_unix_secs: NOW,
        max_staleness_secs: 300, upstream_count: 2, caveats: [],
        windows: (windows[provider] ?? []).map((window) => ({
          window, cc_window_start_unix_secs: NOW - 3600, cc_window_reset_unix_secs: NOW + 3600,
          used_tokens: 100, utilization: 0.8, utilization_percent: 80,
          capacity_to_now_tokens_estimate: 500, projected_capacity_tokens_estimate: 1000,
          remaining_to_now_tokens_estimate: 400, confidence: 'high', contributing_upstreams: 2,
          stale_upstreams: 0, missing_capacity_upstreams: 0,
          provider_lots: [{
            upstream_id: `${provider}-pool-fixture`, upstream_name: `${provider} pool`,
            window, source: 'api', state: 'fresh',
            provider_start_unix_secs: NOW - 3600, provider_reset_unix_secs: NOW + 3600,
            observed_at_unix_millis: NOW * 1000, utilization: 0.8,
            capacity_estimate_tokens: 1000, used_before_cc_window_tokens: 0,
            capacity_to_now_tokens_estimate: 500, projected_capacity_tokens_estimate: 1000,
            confidence: 'high', capacity_ratio: 1,
          }], caveats: [],
        })),
      };
      return route.fulfill({ json: response });
    });
    await page.route('**/admin/v1/subscription-quotas/pool-history?**', (route) => {
      const provider = new URL(route.request().url()).searchParams.get('provider') as typeof providers[number];
      const anthropic = provider === 'anthropic';
      const stale = !anthropic && state === 'stale';
      const mixed = !anthropic && state === 'mixed';
      const value = anthropic ? 25 : stale ? null : mixed ? 31 : 64;
      const response: PoolHistoryResponse = {
        now_unix_secs: NOW,
        windows: (windows[provider] ?? []).map((window) => ({
          window,
          latest: {
            snapshot_at_unix_secs: NOW, utilization: value == null ? null : value / 100,
            utilization_percent: value, contributing_upstreams: stale ? 0 : 1,
            eligible_upstreams: 2, stale_upstreams: stale ? 2 : mixed ? 1 : 0,
            max_observed_at_unix_millis: stale ? null : NOW * 1000,
            ...(!anthropic ? {
              oldest_observed_at_unix_millis: (NOW - (stale || mixed ? 600 : 30)) * 1000,
              max_staleness_secs: 300, missing_observation_upstreams: 0,
            } : {}),
          },
          series: !anthropic && state === 'first-read' ? [] : [
            { snapshot_at_unix_secs: NOW - 120, utilization_percent: anthropic ? 25 : 64 },
            { snapshot_at_unix_secs: NOW - 60, utilization_percent: anthropic ? 25 : null },
          ],
        })),
      };
      return route.fulfill({ json: response });
    });

    await page.goto('/');
    for (const provider of providers) {
      const card = page.getByTestId(`provider-pool-${provider}`);
      await expect(card).toBeVisible();
      const staleProvider = provider !== 'anthropic' && (state === 'stale' || state === 'mixed');
      const value = provider === 'anthropic' ? '25%' : state === 'stale' ? '—' : state === 'mixed' ? '31%' : '64%';
      await expect(card.locator('[data-slot="provider-pool-headline"]')).toContainText(value);
      const legends = card.locator('[data-slot="pool-quota-legend-used"]');
      await expect(legends).toHaveCount(windows[provider].length);
      for (const legend of await legends.all()) await expect(legend).toHaveText(value);
      await expect(card.getByText('Stale reading', { exact: true })).toHaveCount(staleProvider ? 1 : 0);
      if (provider !== 'anthropic') {
        await expect(card.getByText(staleProvider ? 'Provider data 10m ago' : 'Provider data just now', { exact: true })).toBeVisible();
      }
      if (provider !== 'anthropic' && state === 'stale') {
        await expect(card.getByLabel('Current pool usage unavailable')).toHaveText('—');
        await expect(card.locator('[data-slot="provider-pool-headline"]')).not.toContainText('used');
      }
      expect(await card.evaluate((element) => element.scrollWidth <= element.clientWidth)).toBe(true);
    }
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);

    for (const provider of providers) {
      await page.goto(`/providers/${provider}`);
      const card = page.getByTestId('pool-quota-card');
      await expect(card).toBeVisible();
      const staleProvider =
        provider !== 'anthropic' && (state === 'stale' || state === 'mixed');
      const value =
        provider === 'anthropic'
          ? '25%'
          : state === 'stale'
            ? '—'
            : state === 'mixed'
              ? '31%'
              : '64%';
      const legends = card.locator('[data-slot="pool-quota-legend-used"]');
      await expect(legends).toHaveCount(windows[provider].length);
      for (const legend of await legends.all()) {
        await expect(legend).toHaveText(value);
      }
      await expect(
        card.getByText('Stale reading', { exact: true }),
      ).toHaveCount(staleProvider ? 1 : 0);
      await expect(
        card.getByText(
          staleProvider ? STALE_PROVIDER_READING : RECENT_PROVIDER_READING,
        ),
      ).toBeVisible();
      expect(
        await card.evaluate(
          (element) => element.scrollWidth <= element.clientWidth,
        ),
      ).toBe(true);
      expect(
        await page.evaluate(
          () => document.documentElement.scrollWidth <= window.innerWidth,
        ),
      ).toBe(true);
    }
  });
}
