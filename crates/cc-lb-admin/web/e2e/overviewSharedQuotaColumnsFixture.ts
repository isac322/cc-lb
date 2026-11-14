import type { Page } from '@playwright/test';
import type { PeriodUnknownQuotaMeter, QuotaSnapshot } from '../src/lib/api';

// Synthetic mixed fleet for the all-provider Overview's shared 5h / 7d
// columns. Every response is local; no backend or real account is used.
export const NOW = Date.parse('2026-11-14T12:00:00Z') / 1000;

type Kind =
  | 'anthropic_oauth'
  | 'openai_oauth'
  | 'gemini_oauth'
  | 'anthropic_api_key';

function upstream(id: string, name: string, kind: Kind) {
  return {
    id, name, kind, enabled: true, spec_revision: 1, base_url: null,
    warmup_enabled: false, warmup_dialect_plugin: null,
    status: { last_apply_error: null, last_apply_at_unix_secs: null, last_warmup_at_unix_secs: null },
  };
}

function snapshot(
  window: QuotaSnapshot['window'],
  utilization: number | null,
  state: QuotaSnapshot['state'] = 'fresh',
): QuotaSnapshot {
  const reading = state === 'fresh' || state === 'stale';
  const weekly = window.endsWith('7d') || window === '7d_fable';
  return {
    window, state, source: 'api', utilization, status: null,
    resets_at_unix_secs: reading ? NOW + (weekly ? 3 * 86400 : 2 * 3600) : null,
    surpassed_threshold: null, representative_claim: null, disabled_reason: null,
    extra_usage_enabled: null, extra_usage_monthly_limit: null, extra_usage_used_credits: null,
    observed_at_unix_millis: state === 'unobserved' ? null : NOW * 1000,
    age_secs: state === 'unobserved' ? null : 0,
  };
}

export const FLEET = [
  {
    upstream: upstream('claude-max', 'Claude Max', 'anthropic_oauth'),
    windows: [snapshot('5h', 0.1), snapshot('7d', 0.2), snapshot('7d_fable', 0.6)],
  },
  {
    upstream: upstream('codex-pro', 'Codex Pro', 'openai_oauth'),
    windows: [snapshot('5h', 0.4), snapshot('7d', 0.5)],
  },
  {
    upstream: upstream('gemini-both', 'Gemini both groups', 'gemini_oauth'),
    windows: [
      snapshot('gemini_5h', 0.3), snapshot('gemini_7d', 0.35),
      snapshot('third_party_5h', 0.9), snapshot('third_party_7d', 0.7),
    ],
  },
  {
    // Weekly at 100% while Google withholds (absent) or has not yet sent
    // (unobserved) the 5h reading: never 0%, never inferred 100%.
    upstream: upstream('gemini-weekly', 'Gemini weekly exhausted', 'gemini_oauth'),
    windows: [
      snapshot('gemini_5h', null, 'absent'), snapshot('gemini_7d', 1),
      snapshot('third_party_5h', null, 'unobserved'), snapshot('third_party_7d', 0.4),
    ],
  },
  { upstream: upstream('claude-key', 'Claude API key', 'anthropic_api_key'), windows: [] },
];

/** OAuth Gemini rows plus an API key: shared layout, no quota reading yet. */
export const PENDING_GEMINI_FLEET = [FLEET[2]!, FLEET[3]!, FLEET[4]!];

/**
 * No row reports a 5h figure: Anthropic proves 5h absent, OpenAI has not
 * observed it, and Gemini has no group reading at all. Both shared columns
 * must still render.
 */
export const WEEKLY_ONLY_FLEET = [
  {
    upstream: upstream('claude-weekly', 'Claude weekly plan', 'anthropic_oauth'),
    windows: [snapshot('5h', null, 'absent'), snapshot('7d', 0.2)],
  },
  {
    upstream: upstream('codex-weekly', 'Codex weekly plan', 'openai_oauth'),
    windows: [snapshot('5h', null, 'unobserved'), snapshot('7d', 0.3)],
  },
  {
    upstream: upstream('gemini-unread', 'Gemini unread', 'gemini_oauth'),
    windows: [
      snapshot('gemini_5h', null, 'unobserved'), snapshot('gemini_7d', null, 'unobserved'),
      snapshot('third_party_5h', null, 'absent'), snapshot('third_party_7d', null, 'absent'),
    ],
  },
  FLEET[4]!,
];

function staleSnapshot(window: QuotaSnapshot['window'], utilization: number, ageSecs: number): QuotaSnapshot {
  return {
    ...snapshot(window, utilization, 'stale'),
    observed_at_unix_millis: (NOW - ageSecs) * 1000,
    age_secs: ageSecs,
  };
}

/** Stale readings beside fresh ones, plus a Gemini upstream proven to offer nothing. */
export const STALE_AND_ABSENT_FLEET = [
  {
    upstream: upstream('claude-stale', 'Claude stale 5h', 'anthropic_oauth'),
    windows: [staleSnapshot('5h', 0.42, 12 * 60), snapshot('7d', 0.2)],
  },
  {
    upstream: upstream('gemini-stale', 'Gemini stale 5h', 'gemini_oauth'),
    windows: [
      staleSnapshot('gemini_5h', 0.3, 3 * 3600), snapshot('gemini_7d', 0.35),
      snapshot('third_party_5h', null, 'absent'), snapshot('third_party_7d', null, 'absent'),
    ],
  },
  {
    upstream: upstream('gemini-absent', 'Gemini not offered', 'gemini_oauth'),
    windows: [
      snapshot('gemini_5h', null, 'absent'), snapshot('gemini_7d', null, 'absent'),
      snapshot('third_party_5h', null, 'absent'), snapshot('third_party_7d', null, 'absent'),
    ],
  },
];

function observedAgo(snap: QuotaSnapshot, ageSecs: number): QuotaSnapshot {
  return { ...snap, observed_at_unix_millis: (NOW - ageSecs) * 1000, age_secs: ageSecs };
}

function rawMeter(group: PeriodUnknownQuotaMeter['group']): PeriodUnknownQuotaMeter {
  return {
    group, model_id: `${group}-unlabeled`, known_period: null, remaining_fraction: 0.5,
    utilization: 0.5, reset_time_unix_secs: null, origin: 'retrieve_user_quota_summary',
    source: 'api', state: 'fresh', observed_at_unix_millis: NOW * 1000, age_secs: 0,
  };
}

interface FleetEntry {
  upstream: { id: string; name: string; kind: Kind };
  windows: QuotaSnapshot[];
  periodUnknownMeters?: PeriodUnknownQuotaMeter[];
}

/**
 * t1 (an hour ago): both 5h windows at 20%. t2 (now): both weeklies at 100%
 * plus unlabeled raw meters; the 5h rows are retained from t1 but `/latest`
 * still says `fresh`. The control upstream observed everything at t2.
 */
export const RETAINED_FLEET: FleetEntry[] = [
  {
    upstream: upstream('gemini-retained', 'Gemini retained 5h', 'gemini_oauth'),
    windows: [
      observedAgo(snapshot('gemini_5h', 0.2), 3600), snapshot('gemini_7d', 1),
      observedAgo(snapshot('third_party_5h', 0.2), 3600), snapshot('third_party_7d', 1),
    ],
    periodUnknownMeters: [rawMeter('gemini'), rawMeter('third_party')],
  },
  {
    upstream: upstream('gemini-current', 'Gemini current control', 'gemini_oauth'),
    windows: [
      snapshot('gemini_5h', 0.2), snapshot('gemini_7d', 1),
      snapshot('third_party_5h', 0.2), snapshot('third_party_7d', 1),
    ],
    periodUnknownMeters: [rawMeter('gemini'), rawMeter('third_party')],
  },
  // A second provider makes `/` the all-provider (shared) Overview.
  FLEET[0]!,
];

export async function installFixtures(
  page: Page,
  {
    theme,
    fleet = FLEET,
    holdLatest = false,
  }: { theme: 'dark' | 'light'; fleet?: readonly FleetEntry[]; holdLatest?: boolean },
) {
  await page.clock.install({ time: new Date(NOW * 1000) });
  await page.addInitScript((selectedTheme) => {
    localStorage.setItem('cc-lb-admin-token', 'synthetic-e2e-token');
    localStorage.setItem('cclb.locale', 'en-US');
    localStorage.setItem('cclb.theme', selectedTheme);
  }, theme);
  const upstreams = fleet.map((entry) => entry.upstream);
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
    if (path === '/admin/v1/upstreams') return json({ upstreams });
    if (path === '/admin/v1/upstreams/limit-resets') return json({ upstreams: [] });
    if (path === '/admin/v1/principals') return json({
      principals: [{ id: 'shared-columns-principal', name: 'Shared columns principal' }],
    });
    if (path === '/admin/v1/plugins/registry') return json({ entries: [] });
    if (path.endsWith('/oauth/status')) {
      const id = path.split('/').at(-3) ?? '';
      return json({
        upstream_id: id, kind: upstreams.find((u) => u.id === id)?.kind ?? 'anthropic_oauth',
        has_credentials: true, status: 'active', expires_at_unix_secs: NOW + 3600,
        refresh_token_present: true, scopes: [],
      });
    }
    if (path === '/admin/v1/status') return json({
      version: 'fixture', git_sha: 'fixture', uptime_secs: 1, generation: 1,
      build: { rust_version: 'fixture', profile: 'debug', target: 'fixture' },
      upstreams: upstreams.map((u) => ({
        id: u.id, name: u.name, status: 'active', last_apply_at_unix_secs: null, last_apply_error: null,
      })),
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
      // Pending: leave the quota poll unanswered so the table stays loading.
      if (holdLatest) return;
      const requestedWindows = url.searchParams.get('windows')?.split(',');
      const requestedIds = url.searchParams.get('upstream_ids')?.split(',');
      return json({
        now_unix_secs: NOW, max_staleness_secs: 300,
        upstreams: fleet
          .filter((entry) => !requestedIds || requestedIds.includes(entry.upstream.id))
          .map((entry) => ({
            upstream_id: entry.upstream.id, upstream_name: entry.upstream.name,
            windows: entry.windows.filter(
              (window) => !requestedWindows || requestedWindows.includes(window.window),
            ),
            period_unknown_meters: entry.periodUnknownMeters ?? [],
          })),
      });
    }
    if (path === '/admin/v1/subscription-quotas/aggregate') return json({
      now_unix_secs: NOW, window_anchor_unix_secs: NOW - 86400, max_staleness_secs: 300,
      upstream_count: upstreams.length, windows: [], caveats: [],
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
}
