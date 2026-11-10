import type { Page } from '@playwright/test';
import type { QuotaCredits, QuotaSnapshot } from '../src/lib/api';

export const NOW = Date.parse('2026-11-11T12:00:00Z') / 1000;
export const ID = 'codex-credit-fixture';
const upstream = {
  id: ID, name: 'Codex credit fixture', kind: 'openai_oauth', enabled: true,
  spec_revision: 1, base_url: 'https://api.openai.com', warmup_enabled: false,
  warmup_dialect_plugin: null,
  status: { last_apply_error: null, last_apply_at_unix_secs: null, last_warmup_at_unix_secs: null },
};

function credits(overrides: Partial<QuotaCredits>): QuotaCredits {
  return {
    has_credits: null, unlimited: null, limit_reached: null, balance: null,
    approx_local_messages: null, approx_cloud_messages: null, ...overrides,
  };
}

function snapshot(overrides: Partial<QuotaSnapshot>): QuotaSnapshot {
  return {
    window: 'overage', state: 'fresh', source: 'api', utilization: null,
    status: null, resets_at_unix_secs: null, surpassed_threshold: null,
    representative_claim: null, disabled_reason: null, extra_usage_enabled: null,
    extra_usage_monthly_limit: null, extra_usage_used_credits: null,
    observed_at_unix_millis: NOW * 1000, age_secs: 0, ...overrides,
  };
}

export const scenarios = [
  { name: 'available', status: 'Available', observations: [snapshot({ credits: credits({ has_credits: true, balance: '62500.0000000000' }) })] },
  { name: 'unavailable', status: 'Unavailable', observations: [snapshot({ credits: credits({ has_credits: false, balance: '0.0000000000' }) })] },
  { name: 'unlimited without has_credits', status: 'Unlimited', observations: [snapshot({ credits: credits({ has_credits: false, unlimited: true }) })] },
  { name: 'fresh header conflicts with API block', status: 'Blocked', observations: [
    snapshot({ credits: credits({ limit_reached: true, balance: '20.0000000000' }) }),
    snapshot({ window: 'unified', source: 'header', observed_at_unix_millis: NOW * 1000 + 1000,
      credits: credits({ has_credits: true, balance: '21.0000000000' }) }),
  ] },
  { name: 'unknown balance', status: 'Unknown', observations: [snapshot({ credits: credits({}) })] },
  { name: 'stale balance and availability', status: 'Stale', observations: [snapshot({ state: 'stale', age_secs: 600,
    observed_at_unix_millis: (NOW - 600) * 1000, credits: credits({ has_credits: true, balance: '10.0000000000' }) })] },
  { name: 'stale header restriction survives API clear', status: 'Unavailable', observations: [
    snapshot({ credits: credits({ has_credits: true, balance: '9.0000000000' }), fallback_available: true }),
    snapshot({ window: 'unified', state: 'stale', source: 'header', fallback_available: false,
      age_secs: 600, observed_at_unix_millis: (NOW - 600) * 1000 }),
  ] },
];

export async function installFixtures(page: Page, observations: QuotaSnapshot[]) {
  const latestRequests: URL[] = [];
  const windows = [
    snapshot({ window: '5h', utilization: 0.25, resets_at_unix_secs: NOW + 3600 }),
    snapshot({ window: '7d', utilization: 0.5, resets_at_unix_secs: NOW + 86400 }),
    ...observations,
  ];
  await page.clock.install({ time: new Date(NOW * 1000) });
  await page.addInitScript(() => {
    localStorage.setItem('cc-lb-admin-token', 'synthetic-e2e-token');
    localStorage.setItem('cclb.locale', 'en-US');
  });
  await page.route('**/admin/**', async (route) => {
    const url = new URL(route.request().url());
    const path = url.pathname;
    const json = (body: unknown) => route.fulfill({
      status: 200, contentType: 'application/json', body: JSON.stringify(body),
    });
    if (path === '/admin/v1/auth/session') return json({
      authority: 'e2e', subject: 'operator', kind: 'human', provider_id: 'static',
      email: null, display_name: 'Synthetic QA', expires_at_unix_secs: null, auth_mode: 'static_token',
    });
    if (path === '/admin/health') return json({ status: 'ok', version: 'fixture', git_sha: 'fixture', uptime_secs: 1 });
    if (path === '/admin/v1/upstreams') return json({ upstreams: [upstream] });
    if (path === `/admin/v1/upstreams/${ID}`) return route.fulfill({
      status: 200, contentType: 'application/json', headers: { etag: '"1"' }, body: JSON.stringify(upstream),
    });
    if (path === '/admin/v1/principals') return json({
      principals: [{ id: 'credit-qa-principal', name: 'Credit QA principal' }],
    });
    if (path === '/admin/v1/plugins/registry') return json({ entries: [] });
    if (path.endsWith('/oauth/status')) return json({
      upstream_id: ID, kind: 'openai_oauth', has_credentials: true, status: 'active',
      expires_at_unix_secs: NOW + 3600, refresh_token_present: true, scopes: [],
    });
    if (path.endsWith('/subscription-metadata')) return json({
      upstream_id: ID, subscription_metadata: null, organization_metadata: null,
    });
    if (path.endsWith('/warmup')) return json({
      upstream_id: ID, last_attempt: null, recent_attempts: [], next_scheduled_at_unix_secs: null,
      recent_summary_7d: { success: 0, skipped: 0, transient_failure: 0, permanent_failure: 0 }, dialect_plugin: null,
    });
    if (path === '/admin/v1/status') return json({
      version: 'fixture', git_sha: 'fixture', uptime_secs: 1, generation: 1,
      build: { rust_version: 'fixture', profile: 'debug', target: 'fixture' },
      upstreams: [{ id: ID, name: upstream.name, status: 'active', last_apply_at_unix_secs: null, last_apply_error: null }],
    });
    if (path === '/admin/v1/dashboard/summary') return json({
      range: '24h', step: 'hour', window_start_unix_secs: NOW - 86400, window_end_unix_secs: NOW,
      // A populated pool is required: the real Overview intentionally replaces
      // quota sections with onboarding until a principal and request exist.
      totals: { request_count: 1, input_tokens: 100, output_tokens: 20, cache_creation_input_tokens: 0,
        cache_read_input_tokens: 0, error_count: 0, error_rate: 0, virtual_cost_micros: 0, avg_latency_ms: 10 },
      sparkline: { buckets: [] }, observed: true,
    });
    if (path === '/admin/v1/dashboard/usage') return json({
      range: url.searchParams.get('range') ?? '24h', step: url.searchParams.get('step') ?? 'hour',
      group_by: url.searchParams.get('group_by') ?? 'model', window_start_unix_secs: NOW - 86400,
      window_end_unix_secs: NOW, observed: true, series: [],
    });
    if (path === '/admin/v1/events/recent') return json({ events: [], observed: true, count: 0, limit: 200 });
    if (path === '/admin/v1/events/stream') return route.fulfill({
      status: 200, contentType: 'text/event-stream', body: ': connected\n\n',
    });
    if (path === '/admin/v1/subscription-quotas/latest') {
      latestRequests.push(url);
      // Honor the actual API window filter: a missing Overage request must fail this scenario.
      const requested = url.searchParams.get('windows')?.split(',');
      return json({ now_unix_secs: NOW, max_staleness_secs: 300,
        upstreams: [{ upstream_id: ID, upstream_name: upstream.name,
          windows: windows.filter((window) => !requested || requested.includes(window.window)) }],
      });
    }
    if (path === '/admin/v1/subscription-quotas/aggregate') return json({
      now_unix_secs: NOW, window_anchor_unix_secs: NOW - 86400, max_staleness_secs: 300,
      upstream_count: 1, windows: [], caveats: [],
    });
    if (path === '/admin/v1/subscription-quotas/pool-history') return json({ now_unix_secs: NOW, windows: [] });
    if (path === '/admin/v1/subscription-quotas/series') return json({
      since_unix_secs: Number(url.searchParams.get('since_unix_secs')),
      until_unix_secs: Number(url.searchParams.get('until_unix_secs')),
      bucket_secs: Number(url.searchParams.get('bucket_secs')), source: 'merged', series: [],
    });
    if (path.includes('/cache-keepalive')) return json({ sessions: [], tasks: [], enabled: false });
    return route.fulfill({ status: 404, contentType: 'application/json', body: JSON.stringify({ error: `Unmapped fixture: ${path}` }) });
  });
  return latestRequests;
}
