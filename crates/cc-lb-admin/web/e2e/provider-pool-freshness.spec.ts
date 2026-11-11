import { expect, type Page, test } from '@playwright/test';
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

type PoolState = 'stale' | 'mixed' | 'recovered' | 'first-read';
type Provider = (typeof providers)[number];
const surfaces = ['overview', ...providers] as const;

async function installPoolFixtures(page: Page, state: PoolState) {
  await installFixtures(page, []);
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
    const value = anthropic ? 25 : stale ? null : mixed ? 31 : 64;
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
          stale_upstreams: stale ? 2 : mixed ? 1 : 0,
          max_observed_at_unix_millis: stale ? null : NOW * 1000,
          ...(!anthropic ? {
            oldest_observed_at_unix_millis: (NOW - (stale || mixed ? 600 : 30)) * 1000,
            max_staleness_secs: 300,
            missing_observation_upstreams: 0,
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
}

for (const width of [390, 768, 1440]) {
  for (const state of ['stale', 'mixed', 'recovered', 'first-read'] as const) {
    for (const surface of surfaces) {
      test(`Provider pools keep ${state} facts on ${surface} at ${width}px`, async ({ page }) => {
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
      });
    }
  }
}
