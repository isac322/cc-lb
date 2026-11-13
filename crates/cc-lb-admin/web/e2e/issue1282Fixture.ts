import type { Page } from '@playwright/test';

export const PRINCIPAL_ID = 'openai-principal';
export const principal = {
  id: PRINCIPAL_ID,
  name: 'openai-principal',
  kind: 'machine',
  enabled: true,
  revision: 7,
  allowed_models: ['gpt-5.6'],
  allowed_upstreams: [],
  default_limits: [],
  cache_keepalive: {
    enabled: true,
    refresh_lead_time_5m_secs: 45,
    refresh_lead_time_1h_secs: 400,
    max_refreshes_per_session: 12,
    max_total_duration_secs: 14_400,
    snapshot_max_bytes: 8_388_608,
    min_cache_tokens: 10_000,
    classifier: {
      extra_wait_for_user_tools: [],
      treat_end_turn_as_ambiguous: false,
      openai_responses_enabled: true,
    },
  },
};

export const upstream = {
  id: 'openai-upstream',
  name: 'OpenAI API key',
  kind: 'openai_api_key',
  enabled: true,
  base_url: null,
  spec_revision: 1,
  warmup_enabled: false,
  warmup_dialect_plugin: null,
  status: {
    last_apply_error: null,
    last_apply_at_unix_secs: null,
    last_warmup_at_unix_secs: null,
  },
};

export const summary = {
  renewing_now: 1,
  sessions_last_5m: 2,
  retained_successful_hits: 3,
  cost_saved: null,
  total_spent: 0,
  total_avoided: null,
  unlisted_sessions: 0,
  unlisted_renewals: 0,
  unlisted_spent: 0,
  unlisted_net: 0,
  evidence_status: 'none',
  unknown_cycles: 0,
  retained_from_ms: null,
};

export const plugins = ['First plugin', 'Second plugin'].map((name, index) => ({
  id: `plugin-${index + 1}`,
  sha256_hex: String(index + 1).repeat(64),
  name,
  original_filename: `plugin-${index + 1}.wasm`,
  description: `${name} description`,
  usage: `${name} usage`,
  hook_metadata: {},
  label: null,
  size_bytes: 1024,
  refcount: 1,
  revision: 1,
  uploaded_at_unix_secs: 1_700_000_000,
  is_builtin: false,
  version: '1.0.0',
  metadata: null,
  supported_slots: ['router'],
}));

export type FixtureState = {
  registry: 'error' | 'populated' | 'empty';
  referencesFail: boolean;
};

export async function installFixtures(
  page: Page,
  theme: 'dark' | 'light',
  state: FixtureState = { registry: 'empty', referencesFail: false },
) {
  await page.addInitScript((nextTheme) => {
    window.localStorage.setItem('cc-lb-admin-token', 'mock-token');
    window.localStorage.setItem('cclb.locale', 'en-US');
    window.localStorage.setItem('cclb.theme', nextTheme);
  }, theme);
  await page.route('**/admin/**', async (route) => {
    const { pathname } = new URL(route.request().url());
    const json = (body: unknown, status = 200) =>
      route.fulfill({
        status,
        contentType: 'application/json',
        body: JSON.stringify(body),
      });

    if (pathname === '/admin/v1/auth/session') {
      return json({
        authority: 'e2e',
        subject: 'operator',
        kind: 'human',
        provider_id: 'static-token',
        email: null,
        display_name: 'E2E operator',
        expires_at_unix_secs: null,
        auth_mode: 'static_token',
      });
    }
    if (pathname === '/admin/health' || pathname === '/admin/v1/status') {
      return json({
        status: 'ok',
        version: 'e2e',
        git_sha: 'fixture',
        uptime_secs: 1,
        build: { rust_version: 'fixture', profile: 'dev', target: 'fixture' },
        generation: 1,
        upstreams: [],
      });
    }
    if (pathname === '/admin/v1/principals') {
      return json({ principals: [principal] });
    }
    if (pathname === '/admin/v1/upstreams') {
      return json({ upstreams: [upstream] });
    }
    if (pathname === '/admin/v1/plugins/registry') {
      return state.registry === 'error'
        ? json({ error: { code: 'unavailable', message: 'Service unavailable' } }, 503)
        : json({ entries: state.registry === 'populated' ? plugins : [] });
    }
    if (pathname === '/admin/v1/plugins/registry/plugin-1/references') {
      return state.referencesFail
        ? json({ error: { code: 'unavailable', message: 'Service unavailable' } }, 503)
        : json({
            registry: plugins[0],
            refcount: 1,
            reference_fingerprint: 'fixture-references',
            references: [{
              kind: 'plugin_chain',
              chain_entry_id: 'chain-1',
              principal_id: PRINCIPAL_ID,
              principal_name: principal.name,
              slot: 'router',
              revision: 7,
            }],
          });
    }
    if (/^\/admin\/v1\/principals\/[^/]+\/plugin-chain$/.test(pathname)) {
      return json({ entries: [] });
    }
    if (/^\/admin\/v1\/principals\/[^/]+\/router-terminal$/.test(pathname)) {
      return json({ strategy: 'first-pick', revision: 7 });
    }
    if (/^\/admin\/v1\/principals\/[^/]+\/keys$/.test(pathname)) {
      return json({ keys: [] });
    }
    if (pathname === '/admin/v1/dashboard/usage') {
      return json({
        range: '24h',
        step: 'hour',
        group_by: 'principal',
        window_start_unix_secs: 1_700_000_000,
        window_end_unix_secs: 1_700_086_400,
        series: [],
        observed: true,
      });
    }
    if (pathname === '/admin/v1/events/recent') {
      return json({ events: [], next_cursor: null, count: 0, limit: 50, observed: true });
    }
    if (/^\/admin\/v1\/principals\/[^/]+\/cache-keepalive(?:\/summary)?$/.test(pathname)) {
      return json({ summary, groups: null, rows: [], next_cursor: null });
    }
    return json({});
  });
}
