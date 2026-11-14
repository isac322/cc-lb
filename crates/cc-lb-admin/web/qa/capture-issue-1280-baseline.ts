import { mkdir, writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { type Browser, chromium, expect, type Locator, type Page } from '@playwright/test';
import type { createServer as CreateViteServer } from 'vite';
import type {
  ApiKeyAccess,
  PricingCompleteness,
  Upstream,
  UpstreamKeyUsageResponse,
  UsageBucket,
} from '../src/lib/api';

const [appRoot, outputRoot, sourceSha, portArg = '5176', ciMergeSha = sourceSha, prHeadSha = sourceSha] =
  process.argv.slice(2);
if (!appRoot || !outputRoot || !sourceSha) {
  throw new Error(
    'Usage: bun qa/capture-issue-1280-baseline.ts <web-root> <output-root> <served-tree-sha> [port] [ci-merge-sha] [pr-head-sha]',
  );
}

const NOW = Date.parse('2026-11-14T12:00:00Z') / 1000;
const PRINCIPAL = 'pricing-principal';
const API_KEY_UPSTREAM_ID = 'api-key-pricing';
const RESIDUAL_SCENARIOS = [
  { name: 'legacy-zero', state: 'unknown' as const, caption: 'Price unknown', overrides: {} },
  { name: 'legacy-unpriced', state: 'unknown' as const, caption: 'Price unknown', overrides: { request_count: 2, unpriced_request_count: 1 } },
  { name: 'legacy-auxiliary', state: 'unknown' as const, caption: 'Price unknown', overrides: { request_count: 2, unmetered_request_count: 1 } },
  { name: 'legacy-priced-zero', state: 'partial' as const, caption: 'Partly priced', overrides: { request_count: 2, priced_request_count: 1 } },
  { name: 'positive-legacy', state: 'partial' as const, caption: 'Partly priced', overrides: { virtual_cost_micros: 2_500_000 } },
  { name: 'known-zero', state: 'known_zero' as const, caption: null, overrides: { priced_request_count: 1 } },
  { name: 'auxiliary', state: 'unmetered' as const, caption: 'No billable usage', overrides: { unmetered_request_count: 1 } },
] as const;
const TS_API_SCENARIOS = [
  {
    name: 'ts-unknown-unpriced',
    state: 'unknown' as const,
    caption: 'Price unknown',
    modelKey: 'ts',
    overrides: { request_count: 2, unpriced_request_count: 1 },
  },
  {
    name: 'ts-partial-control',
    state: 'partial' as const,
    caption: 'Partly priced',
    modelKey: 'ts',
    overrides: {
      request_count: 2,
      priced_request_count: 1,
      unpriced_request_count: 1,
      virtual_cost_micros: 500_000,
    },
  },
] as const;
const API_CAPTURE_SCENARIOS = [
  ...RESIDUAL_SCENARIOS.map((scenario) => ({ ...scenario, modelKey: undefined })),
  ...TS_API_SCENARIOS,
];
async function centerAndAssertUsableViewport(
  page: Page,
  target: Locator,
  label: string,
) {
  await target.evaluate((element) => {
    element.scrollIntoView({ block: 'center', inline: 'nearest', behavior: 'auto' });
  });
  const viewport = page.viewportSize();
  if (!viewport) throw new Error(`Viewport unavailable while positioning ${label}`);
  const bounds = await target.boundingBox();
  expect(bounds, `${label} must have a rendered bounding box`).not.toBeNull();
  const topMargin = viewport.width < 1024 ? 64 : 16;
  const bottomMargin = viewport.width < 1024 ? 80 : 16;
  expect(bounds!.y, `${label} is hidden above the usable viewport`).toBeGreaterThanOrEqual(topMargin);
  expect(bounds!.y + bounds!.height, `${label} is hidden below the usable viewport`).toBeLessThanOrEqual(viewport.height - bottomMargin);
}

// Keep these synthetic inputs identical to overview-cost-completeness.spec.ts.
// Do not assert pricing copy: the same harness captures pre- and post-fix bases.
async function installPricingFixtures(
  page: Page,
  state: PricingCompleteness,
  theme: 'dark' | 'light',
  suppliedBuckets?: UsageBucket[],
) {
  const buckets: UsageBucket[] = suppliedBuckets ?? Array.from({ length: 5 }, (_, index) => {
    const unpriced = state === 'unpriced' || (state === 'partial' && index === 2);
    const priced = state === 'priced' || state === 'known_zero' || (state === 'partial' && !unpriced);
    return {
      bucket_start_unix_secs: NOW - (5 - index) * 3600,
      request_count: 1, input_tokens: state === 'unmetered' ? 0 : 100,
      output_tokens: 0, cache_creation_input_tokens: 0, cache_read_input_tokens: 0,
      error_count: 0, latency_ms_sum: 10, latency_count: 1,
      virtual_cost_micros: state === 'priced' ? 500_000 : state === 'partial' && !unpriced ? (index % 2 === 0 ? 750_000 : 500_000) : 0,
      pricing_completeness: unpriced ? 'unpriced' : state === 'partial' ? 'priced' : state,
      priced_request_count: priced ? 1 : 0,
      unpriced_request_count: unpriced ? 1 : 0,
      unmetered_request_count: state === 'unmetered' ? 1 : 0,
    };
  });
  const amount = buckets.reduce((sum, bucket) => sum + bucket.virtual_cost_micros, 0);
  const counts = {
    pricing_completeness: state,
    priced_request_count: buckets.reduce((sum, bucket) => sum + (bucket.priced_request_count ?? 0), 0),
    unpriced_request_count: buckets.reduce((sum, bucket) => sum + (bucket.unpriced_request_count ?? 0), 0),
    unmetered_request_count: buckets.reduce((sum, bucket) => sum + (bucket.unmetered_request_count ?? 0), 0),
  };
  const requestCount = buckets.reduce((sum, bucket) => sum + bucket.request_count, 0);
  const inputTokens = buckets.reduce((sum, bucket) => sum + bucket.input_tokens, 0);
  const outputTokens = buckets.reduce((sum, bucket) => sum + bucket.output_tokens, 0);
  const cacheCreationInputTokens = buckets.reduce((sum, bucket) => sum + bucket.cache_creation_input_tokens, 0);
  const cacheReadInputTokens = buckets.reduce((sum, bucket) => sum + bucket.cache_read_input_tokens, 0);
  const errorCount = buckets.reduce((sum, bucket) => sum + bucket.error_count, 0);
  const latencyCount = buckets.reduce((sum, bucket) => sum + bucket.latency_count, 0);
  const latencySum = buckets.reduce((sum, bucket) => sum + bucket.latency_ms_sum, 0);
  const averageLatencyMs = latencyCount === 0 ? 0 : latencySum / latencyCount;
  const event = {
    event_id: 'pricing-event', request_id: 'pricing-request', ts: NOW - 60, ts_ms: (NOW - 60) * 1000,
    principal_id: PRINCIPAL, upstream_id: 'anthropic-pricing', upstream_name: 'Anthropic pricing',
    model: 'pricing-model', event_kind: 'messages', source_kind: 'proxy',
    status: 200, duration_ms: 10, input_tokens: buckets[0]?.input_tokens ?? 0, output_tokens: buckets[0]?.output_tokens ?? 0,
    cost_usd_micros: state === 'unpriced' || state === 'unknown' || state === 'partial' ? null : state === 'priced' ? 500_000 : 0,
  };
  const upstreams = ['anthropic', 'openai'].map((provider) => ({
    id: `${provider}-pricing`, name: `${provider} pricing`, kind: `${provider}_api_key`,
    enabled: true, spec_revision: 1, base_url: null, warmup_enabled: false, warmup_dialect_plugin: null,
    status: { last_apply_error: null, last_apply_at_unix_secs: null, last_warmup_at_unix_secs: null },
  }));
  await page.clock.setFixedTime(new Date(NOW * 1000));
  await page.addInitScript(({ theme }) => {
    localStorage.setItem('cc-lb-admin-token', 'synthetic-pricing-qa');
    localStorage.setItem('cclb.locale', 'en-US');
    localStorage.setItem('cclb.theme', theme);
    localStorage.setItem('cclb.timeRange', '24h');
  }, { theme });
  await page.route('**/admin/**', async (route) => {
    const url = new URL(route.request().url());
    const path = url.pathname;
    const json = (body: unknown) => route.fulfill({ json: body });
    if (path === '/admin/v1/auth/session') return json({ authority: 'e2e', subject: 'operator', kind: 'human', provider_id: 'static', email: null, display_name: 'Pricing QA', expires_at_unix_secs: null, auth_mode: 'static_token' });
    if (path === '/admin/health') return json({ status: 'ok', version: 'fixture', git_sha: 'fixture', uptime_secs: 1 });
    if (path === '/admin/v1/upstreams') return json({ upstreams });
    if (path === '/admin/v1/principals') return json({ principals: [{ id: PRINCIPAL, name: 'Pricing principal', kind: 'machine', enabled: true, revision: 1, allowed_models: [], allowed_upstreams: [], default_limits: [] }] });
    if (path === '/admin/v1/plugins/registry') return json({ entries: [] });
    if (path === '/admin/v1/status') return json({ version: 'fixture', git_sha: 'fixture', uptime_secs: 1, generation: 1, build: { rust_version: 'fixture', profile: 'debug', target: 'fixture' }, upstreams: [] });
    if (path === '/admin/v1/dashboard/summary') return json({
      range: '24h', step: 'hour', window_start_unix_secs: NOW - 86400, window_end_unix_secs: NOW, observed: true,
      totals: { ...counts, request_count: requestCount, input_tokens: inputTokens, output_tokens: outputTokens, cache_creation_input_tokens: cacheCreationInputTokens, cache_read_input_tokens: cacheReadInputTokens, error_count: errorCount, error_rate: requestCount === 0 ? 0 : errorCount / requestCount, virtual_cost_micros: amount, avg_latency_ms: averageLatencyMs },
      sparkline: { buckets },
    });
    if (path === '/admin/v1/dashboard/usage') return json({
      range: '24h', step: 'hour', group_by: url.searchParams.get('group_by') ?? 'principal',
      window_start_unix_secs: NOW - 86400, window_end_unix_secs: NOW, observed: true,
      series: [{ key: PRINCIPAL, buckets }],
    });
    if (path === '/admin/v1/events/recent') return json({ events: [event], observed: true, count: 1, limit: 200 });
    if (path === '/admin/v1/events/detail') return json(event);
    if (path === '/admin/v1/events/stream') return route.fulfill({ status: 200, contentType: 'text/event-stream', body: ': connected\n\n' });
    if (path === '/admin/v1/subscription-quotas/latest') return json({ now_unix_secs: NOW, max_staleness_secs: 300, upstreams: [] });
    if (path === '/admin/v1/subscription-quotas/aggregate') return json({ now_unix_secs: NOW, window_anchor_unix_secs: NOW, max_staleness_secs: 300, upstream_count: 0, windows: [], caveats: [] });
    if (path === '/admin/v1/subscription-quotas/pool-history') return json({ now_unix_secs: NOW, range_secs: 86400, windows: [] });
    if (path === '/admin/v1/upstreams/limit-resets') return json({ upstreams: [] });
    if (path.includes('/cache-keepalive')) return json({ sessions: [], tasks: [], enabled: false });
    return route.fulfill({ status: 404, json: { error: `Unmapped pricing fixture: ${path}` } });
  });
}
async function installApiKeyFixtures(
  page: Page,
  state: PricingCompleteness,
  theme: 'dark' | 'light',
  buckets: UsageBucket[],
  modelKey = 'claude-sonnet-4',
) {
  await installPricingFixtures(page, state, theme, buckets);
  const amount = buckets.reduce((sum, bucket) => sum + bucket.virtual_cost_micros, 0);
  const access: ApiKeyAccess = {
    verified: true,
    checked_at_unix_secs: NOW - 600,
    key_class: 'org_scoped',
    inference: 'ok',
    workspace: { mode: 'selected', id: 'wrkspc_01Visual', name: 'Visual QA' },
    workspace_list: 'not_applicable',
    usage_report: 'not_applicable',
    key_type: 'service_account',
    last_error: null,
    provider: 'anthropic',
    provider_report_state: 'estimated',
  };
  const upstream: Upstream = {
    id: API_KEY_UPSTREAM_ID,
    name: 'Estimated pricing API key',
    kind: 'anthropic_api_key',
    enabled: true,
    base_url: 'https://example.invalid',
    api_key_access: access,
    warmup_enabled: false,
    warmup_dialect_plugin: null,
    spec_revision: 1,
    status: { last_apply_error: null, last_apply_at_unix_secs: null, last_warmup_at_unix_secs: null },
  };
  const usage: UpstreamKeyUsageResponse = {
    range: '24h',
    step: 'hour',
    group_by: 'model',
    window_start_unix_secs: NOW - 24 * 3600,
    window_end_unix_secs: NOW,
    series: [{ key: modelKey, buckets }],
    observed: true,
    source: 'estimated',
    provider: 'anthropic',
    provider_report_state: 'estimated',
    attribution: 'api_key',
    attribution_workspace_id: null,
    stale: false,
    last_reported_unix_secs: null,
    totals: { cost_usd_micros: amount, cost_is_estimate: true },
    fix: { code: 'non_anthropic_base_url' },
    month: {
      start_unix_secs: NOW - 14 * 86400,
      end_unix_secs: NOW + 14 * 86400,
      tokens: buckets.reduce((sum, bucket) => sum + bucket.input_tokens + bucket.output_tokens, 0),
      cost_usd_micros: amount,
      pricing_completeness: state,
      priced_request_count: buckets.reduce((sum, bucket) => sum + (bucket.priced_request_count ?? 0), 0),
      unpriced_request_count: buckets.reduce((sum, bucket) => sum + (bucket.unpriced_request_count ?? 0), 0),
      unmetered_request_count: buckets.reduce((sum, bucket) => sum + (bucket.unmetered_request_count ?? 0), 0),
      billing_stop: null,
    },
  };
  await page.route('**/admin/**', async (route) => {
    const url = new URL(route.request().url());
    const path = url.pathname;
    const json = (body: unknown) => route.fulfill({ json: body });
    if (path === '/admin/v1/upstreams') return json({ upstreams: [upstream] });
    if (path === `/admin/v1/upstreams/${API_KEY_UPSTREAM_ID}`) return json(upstream);
    if (path === `/admin/v1/upstreams/${API_KEY_UPSTREAM_ID}/usage`) return json(usage);
    if (path === `/admin/v1/upstreams/${API_KEY_UPSTREAM_ID}/workspaces`) return json({ status: 'not_applicable' });
    if (path === `/admin/v1/upstreams/${API_KEY_UPSTREAM_ID}/subscription-metadata`) return json({ upstream_id: API_KEY_UPSTREAM_ID, windows: [] });
    if (path === `/admin/v1/upstreams/${API_KEY_UPSTREAM_ID}/oauth/status`) return json({ upstream_id: API_KEY_UPSTREAM_ID, kind: upstream.kind, has_credentials: true, status: 'not_applicable' });
    return route.fallback();
  });
}

const port = Number(portArg);
// Load the selected base worktree's frozen Vite, not the head worktree's version.
const { createServer }: { createServer: typeof CreateViteServer } = await import(
  pathToFileURL(resolve(appRoot, 'node_modules/vite/dist/node/index.js')).href
);
const server = await createServer({
  root: appRoot,
  server: { host: '127.0.0.1', port, strictPort: true },
});
let browser: Browser | undefined;
const deadline = Promise.withResolvers<never>();
const readyTimer = setTimeout(
  () => deadline.reject(new Error('Issue-1280 Vite did not become ready within 30 seconds')),
  30_000,
);

try {
  await Promise.race([server.listen(), deadline.promise]);
  clearTimeout(readyTimer);
  browser = await chromium.launch();
  await mkdir(outputRoot, { recursive: true });
  const captures = [];
  const scenarios = [
    { name: 'unpriced', state: 'unpriced' as const },
    { name: 'partial', state: 'partial' as const },
    { name: 'known_zero', state: 'known_zero' as const },
    ...RESIDUAL_SCENARIOS,
  ];
  for (const viewport of [
    { width: 390, height: 844 },
    { width: 768, height: 900 },
    { width: 1440, height: 900 },
  ]) {
    for (const theme of ['dark', 'light'] as const) {
      for (const scenario of scenarios) {
        const context = await browser.newContext({ viewport, colorScheme: theme, reducedMotion: 'reduce' });
        try {
          const page = await context.newPage();
          const residual = RESIDUAL_SCENARIOS.find(({ name }) => name === scenario.name);
          const buckets = residual
            ? [{
                bucket_start_unix_secs: NOW - 3600,
                request_count: 1,
                input_tokens: 0,
                output_tokens: 0,
                cache_creation_input_tokens: 0,
                cache_read_input_tokens: 0,
                error_count: 0,
                latency_ms_sum: 10,
                latency_count: 1,
                virtual_cost_micros: 0,
                priced_request_count: 0,
                unpriced_request_count: 0,
                unmetered_request_count: 0,
                pricing_completeness: scenario.state,
                ...residual.overrides,
              } satisfies UsageBucket]
            : undefined;
          await installPricingFixtures(page, scenario.state, theme, buckets);
          const origin = `http://127.0.0.1:${port}`;
          const { width, height } = viewport;
          await page.goto(`${origin}/providers/anthropic?range=24h`);
          await expect(page.locator('html')).toHaveAttribute('data-theme', theme);
          const cost = page.getByTestId('overview-kpi-cost');
          await expect(cost).toBeVisible();
          await expect(page.getByTestId('top-principal-row')).toHaveCount(1);
          await page.evaluate(() => document.fonts.ready);
          const costText = await cost.innerText();
          await centerAndAssertUsableViewport(page, cost, `Cost KPI ${scenario.name} ${width}px ${theme}`);
          const overviewFile = `overview-${scenario.name}-${width}-${theme}.png`;
          await page.screenshot({ path: resolve(outputRoot, overviewFile), fullPage: false, animations: 'disabled' });
          const principals = page.getByRole('region', { name: 'Top principals' });
          await expect(principals).toBeVisible();
          await expect(principals.getByTestId('top-principal-row')).toHaveCount(1);
          await centerAndAssertUsableViewport(page, principals, `Top principals ${scenario.name} ${width}px ${theme}`);
          const principalsFile = `principals-${scenario.name}-${width}-${theme}.png`;
          await page.screenshot({ path: resolve(outputRoot, principalsFile), fullPage: false, animations: 'disabled' });

          await page.goto(`${origin}/?range=24h`);
          await page.getByRole('row', { name: 'View request pricing-event' }).getByText('pricing-model', { exact: true }).click();
          const drawer = page.getByRole('dialog', { name: 'Request detail' });
          await expect(drawer).toBeVisible();
          const drawerFile = `drawer-${scenario.name}-${width}-${theme}.png`;
          await page.screenshot({ path: resolve(outputRoot, drawerFile), animations: 'disabled' });
          captures.push({ surface: 'overview', scenario: scenario.name, width, height, theme, overviewFile, principalsFile, drawerFile, costText, drawerText: await drawer.innerText() });
        } finally {
          await context.close();
        }
      }
      for (const scenario of API_CAPTURE_SCENARIOS) {
        const context = await browser.newContext({ viewport, colorScheme: theme, reducedMotion: 'reduce' });
        try {
          const page = await context.newPage();
          const buckets: UsageBucket[] = [{
            bucket_start_unix_secs: NOW - 3600,
            request_count: 1,
            input_tokens: 0,
            output_tokens: 0,
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 0,
            error_count: 0,
            latency_ms_sum: 10,
            latency_count: 1,
            virtual_cost_micros: 0,
            priced_request_count: 0,
            unpriced_request_count: 0,
            unmetered_request_count: 0,
            pricing_completeness: scenario.state,
            ...scenario.overrides,
          }];
          await installApiKeyFixtures(page, scenario.state, theme, buckets, scenario.modelKey);
          const origin = `http://127.0.0.1:${port}`;
          const { width, height } = viewport;
          await page.goto(`${origin}/upstreams?selectedId=${API_KEY_UPSTREAM_ID}`);
          const usageCard = page.getByTestId('api-usage-card');
          await expect(usageCard).toBeVisible();
          await usageCard.getByRole('radiogroup', { name: 'Metric' }).getByRole('radio', { name: 'Cost' }).click();
          await page.evaluate(() => document.fonts.ready);
          const credits = usageCard.locator('[data-cell="Credits used"]');
          await expect(credits).toBeVisible();
          const creditsText = await credits.innerText();
          await centerAndAssertUsableViewport(page, credits, `API credits ${scenario.name} ${width}px ${theme}`);
          const apiKeyFile = `api-key-${scenario.name}-${width}-${theme}.png`;
          await page.screenshot({ path: resolve(outputRoot, apiKeyFile), fullPage: false, animations: 'disabled' });
          captures.push({ surface: 'api-key-estimated', scenario: scenario.name, modelKey: scenario.modelKey ?? 'claude-sonnet-4', expectedCaption: scenario.caption, creditsText, width, height, theme, apiKeyFile });
        } finally {
          await context.close();
        }
      }
    }
  }
  await writeFile(resolve(outputRoot, 'capture.json'), JSON.stringify({
    sourceSha,
    actualServedTreeSha: sourceSha,
    ciMergeSha,
    prHeadSha,
    fixture: 'issue-1280-shared-pricing-and-api-key',
    syntheticAdminApi: true,
    syntheticProviderApi: true,
    captures,
  }, null, 2));
  await writeFile(resolve(outputRoot, 'source-sha.txt'), `${sourceSha}\n`);
  await writeFile(resolve(outputRoot, 'provenance.json'), JSON.stringify({ actualServedTreeSha: sourceSha, ciMergeSha, prHeadSha }, null, 2));
  console.log(`Captured issue-1280 pricing and estimated API-key views for ${sourceSha}`);
} finally {
  clearTimeout(readyTimer);
  try {
    await browser?.close();
  } finally {
    await server.close();
  }
}
