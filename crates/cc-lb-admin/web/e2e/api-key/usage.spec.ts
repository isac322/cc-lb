// T2: the API usage card and its month strip. Reported, stopped and stale
// states exist only against api.anthropic.com, so `/usage` (and the access
// snapshot beside it) are FX-PW envelopes in the plan §7 wire shape; the
// estimated custom-endpoint and real-zero states come from the real server.

import type { Page } from '@playwright/test';
import type {
  ApiKeyAccess,
  Upstream,
  UpstreamKeyUsageResponse,
} from '../../src/lib/api';
import { apiKey, expect, openUpstream, test } from './harness';

type RouteEnvelope = (
  path: string | RegExp,
  status: number,
  body: unknown,
  method?: string,
) => Promise<void>;

const NOW = Math.floor(Date.now() / 1000);
const HOUR = 3_600;

const ORG_OK: ApiKeyAccess = {
  verified: true,
  checked_at_unix_secs: NOW - 600,
  key_class: 'org_scoped',
  inference: 'ok',
  workspace: { mode: 'selected', id: 'wrkspc_01Prod00aa', name: 'Production' },
  workspace_list: 'ok',
  usage_report: 'ok',
  key_type: 'service_account',
  last_error: null,
};

function monthStart(): number {
  const d = new Date();
  return Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), 1) / 1000;
}
function monthEnd(): number {
  const d = new Date();
  return Date.UTC(d.getUTCFullYear(), d.getUTCMonth() + 1, 1) / 1000;
}

function bucket(start: number, tokens: number) {
  return {
    bucket_start_unix_secs: start,
    request_count: 0,
    input_tokens: tokens,
    output_tokens: 0,
    cache_creation_input_tokens: 0,
    cache_read_input_tokens: 0,
    error_count: 0,
    virtual_cost_micros: 0,
    latency_ms_sum: 0,
    latency_count: 0,
  };
}

function usage(
  overrides: Partial<UpstreamKeyUsageResponse> = {},
): UpstreamKeyUsageResponse {
  return {
    range: '24h',
    step: 'hour',
    group_by: 'model',
    window_start_unix_secs: NOW - 24 * HOUR,
    window_end_unix_secs: NOW,
    series: [
      { key: 'claude-sonnet-4', buckets: [bucket(NOW - 3 * HOUR, 1_200)] },
      { key: '', buckets: [bucket(NOW - 3 * HOUR, 300)] },
    ],
    observed: true,
    source: 'real',
    attribution: 'api_key',
    attribution_workspace_id: null,
    stale: false,
    last_reported_unix_secs: NOW - 3 * HOUR,
    totals: { cost_usd_micros: 412_300_000, cost_is_estimate: true },
    fix: null,
    month: {
      start_unix_secs: monthStart(),
      end_unix_secs: monthEnd(),
      tokens: 48_213_904,
      cost_usd_micros: 412_300_000,
      billing_stop: null,
    },
    ...overrides,
  };
}

/** FX-PW: Anthropic-host access snapshot plus a `/usage` answer. */
async function serve(
  routeEnvelope: RouteEnvelope,
  upstream: Upstream,
  access: ApiKeyAccess,
  status: number,
  body: unknown,
) {
  const shaped = { ...upstream, base_url: null, api_key_access: access };
  await routeEnvelope(/\/admin\/v1\/upstreams$/, 200, { upstreams: [shaped] }, 'GET');
  await routeEnvelope(
    new RegExp(`/admin/v1/upstreams/${upstream.id}$`),
    200,
    shaped,
    'GET',
  );
  await routeEnvelope(
    new RegExp(`/admin/v1/upstreams/${upstream.id}/usage`),
    status,
    body,
    'GET',
  );
}

function card(page: Page) {
  return page.getByTestId('api-usage-card');
}

test.describe('API usage card', () => {
  // UI-U-05, UI-U-13: reported key usage, strip cells, "Other" model.
  test('reported: strip cells and attribution caption', async ({
    page,
    seedApiKeyUpstream,
    routeEnvelope,
    toasts,
  }) => {
    const upstream = await seedApiKeyUpstream({ key: apiKey('reported') });
    await serve(routeEnvelope, upstream, ORG_OK, 200, usage());
    await openUpstream(page, upstream.id);

    await expect(card(page)).toContainText(
      'Tokens reported by Anthropic · updates about every 5 min',
    );
    const strip = page.getByTestId('api-key-credits');
    await expect(strip.locator('[data-cell="Tokens used"]')).toContainText('48.2');
    await expect(strip.locator('[data-cell="Tokens used"]')).toContainText('Reported');
    await expect(strip.locator('[data-cell="Credits used"]')).toContainText('$412.30');
    await expect(strip.locator('[data-cell="Balance"]')).toContainText('Not in API');
    await expect(
      strip.getByRole('link', { name: /Billing/ }),
    ).toHaveAttribute('target', '_blank');
    await expect(card(page)).toContainText(
      "All of this key's usage, including traffic outside cc-lb.",
    );
    await expect(page.getByTestId('api-usage-legend-slot')).toContainText('Other');
    expect(await toasts.errorCount()).toBe(0);
  });

  // UI-U-11: 401/403 → Stopped banner, Last report, Access Stopped.
  test('stopped: banner with the fix, Last report card', async ({
    page,
    seedApiKeyUpstream,
    routeEnvelope,
  }) => {
    const upstream = await seedApiKeyUpstream({ key: apiKey('stopped') });
    await serve(
      routeEnvelope,
      upstream,
      ORG_OK,
      200,
      usage({ stale: true, fix: { code: 'usage_poll_failed' } }),
    );
    await openUpstream(page, upstream.id);

    await expect(page.getByText('Usage reports stopped')).toBeVisible();
    await expect(page.getByText("Raise the linked account's org role → get reported usage.")).toBeVisible();
    await expect(card(page).getByText('Last report', { exact: true })).toBeVisible();
    await expect(card(page)).toContainText('reports stopped');
    await expect(page.getByTestId('api-key-usage-mode')).toContainText('Stopped');
  });

  // UI-U-14: two 5xx ticks → stale, no fix, never a role fix.
  test('stale transient: no Stopped banner and no fix anywhere', async ({
    page,
    seedApiKeyUpstream,
    routeEnvelope,
  }) => {
    const upstream = await seedApiKeyUpstream({ key: apiKey('stale') });
    await serve(routeEnvelope, upstream, ORG_OK, 200, usage({ stale: true }));
    await openUpstream(page, upstream.id);

    await expect(card(page)).toContainText("Anthropic hasn't answered since");
    await expect(card(page).getByText('Last report', { exact: true })).toBeVisible();
    await expect(page.getByText('Usage reports stopped')).toHaveCount(0);
    await expect(page.getByText(/org role/)).toHaveCount(0);
    await expect(page.getByTestId('api-key-usage-mode')).toContainText('Reported');
  });

  // UI-U-15: right after Add/Replace, before the immediate poll lands.
  test('first poll pending: no Estimated-vs-Reported contradiction', async ({
    page,
    seedApiKeyUpstream,
    routeEnvelope,
  }) => {
    const upstream = await seedApiKeyUpstream({ key: apiKey('pending') });
    await serve(
      routeEnvelope,
      upstream,
      ORG_OK,
      200,
      usage({ source: 'estimated', fix: null }),
    );
    await openUpstream(page, upstream.id);

    await expect(page.getByTestId('api-key-usage-mode')).toContainText('Reported');
    await expect(card(page)).toContainText('Reported figures arrive within 5 min');
    await expect(
      card(page).getByRole('button', { name: 'Get reported usage' }),
    ).toHaveCount(0);
    await expect(card(page).getByText('Estimated', { exact: true })).toHaveCount(0);
  });

  // UI-U-03: the pointer focuses Access → Usage data.
  test('Get reported usage focuses the Usage data row', async ({
    page,
    seedApiKeyUpstream,
    routeEnvelope,
  }) => {
    const upstream = await seedApiKeyUpstream({ key: apiKey('pointer') });
    await serve(
      routeEnvelope,
      upstream,
      { ...ORG_OK, usage_report: 'denied' },
      200,
      usage({ source: 'estimated', fix: { code: 'role_insufficient' } }),
    );
    await openUpstream(page, upstream.id);
    await card(page).getByRole('button', { name: 'Get reported usage' }).click();
    await expect(page.getByTestId('api-key-usage-mode')).toBeFocused();
    await expect(card(page)).not.toContainText('org role');
  });

  // UI-U-08: billing stops, each with its fix on screen.
  for (const [kind, label, fix] of [
    ['credits_exhausted', 'Out of credits', 'Buy credits or turn on auto-reload'],
    ['tier_spend_cap', 'Spend limit reached', 'Request a tier increase in Console'],
    ['spend_limit', 'Spend limit reached', 'Raise the limit in Console'],
    ['billing_error', 'Billing issue', 'Fix the payment method in Console'],
  ] as const) {
    test(`billing stop ${kind}`, async ({
      page,
      seedApiKeyUpstream,
      routeEnvelope,
    }) => {
      const upstream = await seedApiKeyUpstream({ key: apiKey(`bil-${kind}`) });
      const base = usage();
      await serve(
        routeEnvelope,
        upstream,
        ORG_OK,
        200,
        usage({
          month: {
            ...base.month,
            billing_stop: { kind, observed_at_unix_secs: NOW - 180 },
          },
        }),
      );
      await openUpstream(page, upstream.id);
      const balance = page
        .getByTestId('api-key-credits')
        .locator('[data-cell="Balance"]');
      await expect(balance).toContainText(label);
      await expect(page.getByTestId('api-key-credits')).toContainText(fix);
    });
  }

  // UI-U-09: one error surface, zero toasts.
  test('a failed load shows one alert in the strip and no toast', async ({
    page,
    seedApiKeyUpstream,
    routeEnvelope,
    toasts,
  }) => {
    const upstream = await seedApiKeyUpstream({ key: apiKey('load-fail') });
    await serve(routeEnvelope, upstream, ORG_OK, 500, {
      error: 'storage_unavailable',
    });
    await openUpstream(page, upstream.id);
    await expect(card(page).getByRole('alert')).toHaveCount(1);
    await expect(card(page).getByRole('alert')).toContainText(
      "Couldn't load this month:",
    );
    await expect(
      page.getByTestId('api-key-credits').locator('[data-cell="Tokens used"]'),
    ).not.toContainText('0');
    expect(await toasts.errorCount()).toBe(0);
  });

  // UI-U-12, UI-U-07: the real server on a custom endpoint, no traffic yet.
  test('custom endpoint: estimates only, a real zero', async ({
    page,
    seedApiKeyUpstream,
    toasts,
  }) => {
    const upstream = await seedApiKeyUpstream({ key: apiKey('custom-zero') });
    await openUpstream(page, upstream.id);

    await expect(card(page).getByText('Estimated', { exact: true })).toBeVisible();
    await page
      .getByRole('radiogroup', { name: 'Metric' })
      .getByRole('radio', { name: 'Cost' })
      .click();
    await expect(card(page)).toContainText(
      'Real usage needs api.anthropic.com Admin API access. This upstream points elsewhere → showing estimates only. Cost computed at list price.',
    );
    const strip = page.getByTestId('api-key-credits');
    await expect(strip.locator('[data-cell="Tokens used"]')).toContainText('0');
    await expect(strip.locator('[data-cell="Credits used"]')).toContainText('$0.00');
    await expect(card(page)).toContainText('No usage in selected range');
    expect(await toasts.errorCount()).toBe(0);
  });
});
