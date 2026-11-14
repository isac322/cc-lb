import type { Page } from '@playwright/test';
import type { PeriodUnknownQuotaMeter, QuotaSnapshot, UpstreamLatestSnapshots } from '../src/lib/api';
import type { ProviderFamily } from '../src/lib/providers';

export const NOW = Date.parse('2026-11-14T12:00:00Z') / 1000;
export const ID = 'settled-quota-fixture';
export const NAME = 'Subscription quota fixture';
export const LONG_MODEL = 'gemini-very-long-provider-model-identifier-for-truncation';

export function quotaSnapshot(overrides: Partial<QuotaSnapshot> = {}): QuotaSnapshot {
  return {
    window: 'gemini_5h', state: 'fresh', source: 'api', utilization: 0.25,
    status: null, resets_at_unix_secs: NOW + 3600, surpassed_threshold: null,
    representative_claim: null, disabled_reason: null, extra_usage_enabled: null,
    extra_usage_monthly_limit: null, extra_usage_used_credits: null,
    observed_at_unix_millis: NOW * 1000, age_secs: 0, ...overrides,
  };
}

export function rawMeter(overrides: Partial<PeriodUnknownQuotaMeter> = {}): PeriodUnknownQuotaMeter {
  return {
    group: 'gemini', model_id: LONG_MODEL, remaining_fraction: 0.01, utilization: 0.99,
    reset_time_unix_secs: NOW + 7200, origin: 'retrieve_user_quota', source: 'api',
    state: 'fresh', observed_at_unix_millis: NOW * 1000, age_secs: 0, ...overrides,
  };
}

export function absentWindows(provider: ProviderFamily): QuotaSnapshot[] {
  const windows = provider === 'gemini'
    ? ['gemini_5h', 'gemini_7d', 'third_party_5h', 'third_party_7d'] as const
    : ['5h', '7d'] as const;
  return windows.map((window) => quotaSnapshot({ window, state: 'absent', utilization: null, resets_at_unix_secs: null }));
}

export function latestRow(
  windows: QuotaSnapshot[],
  period_unknown_meters: PeriodUnknownQuotaMeter[] = [],
): UpstreamLatestSnapshots {
  return { upstream_id: ID, upstream_name: NAME, windows, period_unknown_meters };
}

/** Mutable provider response: tests advance the real query poll, never reload the SPA. */
export async function installQuotaSettledFixtures(page: Page, options: {
  provider?: ProviderFamily;
  latest?: UpstreamLatestSnapshots | null;
  locale?: 'en-US' | 'ko-KR';
  theme?: 'light' | 'dark';
} = {}) {
  const provider = options.provider ?? 'gemini';
  let latest = options.latest ?? null;
  const requests: URL[] = [];
  const upstream = {
    id: ID, name: NAME, kind: `${provider}_oauth`, enabled: true, spec_revision: 1,
    base_url: null, warmup_enabled: false, warmup_dialect_plugin: null,
    status: { last_apply_error: null, last_apply_at_unix_secs: null, last_warmup_at_unix_secs: null },
  };
  await page.clock.install({ time: new Date(NOW * 1000) });
  await page.addInitScript(({ locale, theme }) => {
    localStorage.setItem('cc-lb-admin-token', 'synthetic-settled-quota-token');
    localStorage.setItem('cclb.locale', locale);
    localStorage.setItem('cclb.theme', theme);
    localStorage.setItem('cclb.timezone', 'UTC');
  }, { locale: options.locale ?? 'en-US', theme: options.theme ?? 'light' });
  await page.route('**/admin/**', async (route) => {
    const url = new URL(route.request().url());
    const path = url.pathname;
    const json = (body: unknown) => route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(body) });
    if (path === '/admin/v1/auth/session') return json({
      authority: 'e2e', subject: 'operator', kind: 'human', provider_id: 'static',
      email: null, display_name: 'Synthetic QA', expires_at_unix_secs: null, auth_mode: 'static_token',
    });
    if (path === '/admin/health') return json({ status: 'ok', version: 'fixture', git_sha: 'fixture', uptime_secs: 1 });
    if (path === '/admin/v1/upstreams') return json({ upstreams: [upstream] });
    if (path === `/admin/v1/upstreams/${ID}`) return route.fulfill({ status: 200, contentType: 'application/json', headers: { etag: '"1"' }, body: JSON.stringify(upstream) });
    if (path === '/admin/v1/principals') return json({ principals: [] });
    if (path === '/admin/v1/plugins/registry') return json({ entries: [] });
    if (path.endsWith('/oauth/status')) return json({
      upstream_id: ID, kind: upstream.kind, has_credentials: true, status: 'active',
      expires_at_unix_secs: NOW + 86400, refresh_token_present: true, scopes: [],
    });
    if (path.endsWith('/subscription-metadata')) return json({ upstream_id: ID, subscription_metadata: null, organization_metadata: null });
    if (path.endsWith('/warmup')) return json({ upstream_id: ID, dialect_plugin: null, lanes: [] });
    if (path === '/admin/v1/status') return json({
      version: 'fixture', git_sha: 'fixture', uptime_secs: 1, generation: 1,
      build: { rust_version: 'fixture', profile: 'debug', target: 'fixture' },
      upstreams: [{ id: ID, name: NAME, status: 'active', last_apply_at_unix_secs: null, last_apply_error: null }],
    });
    if (path === '/admin/v1/dashboard/usage') return json({
      range: url.searchParams.get('range') ?? '24h', step: url.searchParams.get('step') ?? 'hour',
      group_by: url.searchParams.get('group_by') ?? 'model', window_start_unix_secs: NOW - 86400,
      window_end_unix_secs: NOW, observed: false, series: [],
    });
    if (path === '/admin/v1/events/recent') return json({ events: [], observed: false, count: 0, limit: 200 });
    if (path === '/admin/v1/events/stream') return route.fulfill({ status: 200, contentType: 'text/event-stream', body: ': connected\n\n' });
    if (path === '/admin/v1/subscription-quotas/latest') {
      requests.push(url);
      const requested = url.searchParams.get('windows')?.split(',');
      const row = latest == null ? null : {
        ...latest,
        windows: latest.windows.filter((snapshot) => !requested || requested.includes(snapshot.window)),
      };
      return json({ now_unix_secs: NOW, max_staleness_secs: 300, upstreams: row == null ? [] : [row] });
    }
    if (path === '/admin/v1/subscription-quotas/series') return json({
      since_unix_secs: Number(url.searchParams.get('since_unix_secs')),
      until_unix_secs: Number(url.searchParams.get('until_unix_secs')),
      bucket_secs: Number(url.searchParams.get('bucket_secs')), source: 'merged', series: [],
    });
    if (path.includes('/cache-keepalive')) return json({ sessions: [], tasks: [], enabled: false });
    if (path.includes('/coupons')) return json({ coupons: [], total: 0 });
    return route.fulfill({ status: 404, contentType: 'application/json', body: JSON.stringify({ error: `Unmapped fixture: ${path}` }) });
  });
  return {
    requests,
    async poll(next: UpstreamLatestSnapshots | null) {
      latest = next;
      const response = page.waitForResponse((response) => response.url().includes('/subscription-quotas/latest') && response.status() === 200);
      await page.clock.runFor(5001);
      await response;
    },
  };
}
