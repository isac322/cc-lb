import { mkdir, writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { type Browser, chromium, expect, type Page } from '@playwright/test';
import type { createServer as CreateViteServer } from 'vite';
import type { PricingCompleteness, UsageBucket } from '../src/lib/api';

const [appRoot, outputRoot, sourceSha, portArg = '5176'] = process.argv.slice(2);
if (!appRoot || !outputRoot || !sourceSha) {
  throw new Error('Usage: bun qa/capture-issue-1280-baseline.ts <base-web-root> <output-root> <base-sha> [port]');
}

const NOW = Date.parse('2026-11-14T12:00:00Z') / 1000;
const PRINCIPAL = 'pricing-principal';

// Keep these synthetic inputs identical to overview-cost-completeness.spec.ts.
// Do not assert pricing copy: the same harness captures pre- and post-fix bases.
async function installPricingFixtures(page: Page, state: PricingCompleteness, theme: 'dark' | 'light') {
  const buckets: UsageBucket[] = Array.from({ length: 5 }, (_, index) => {
    const unpriced = state === 'unpriced' || (state === 'partial' && index === 2);
    const priced = state === 'priced' || state === 'known_zero' || (state === 'partial' && !unpriced);
    return {
      bucket_start_unix_secs: NOW - (5 - index) * 3600,
      request_count: 1, input_tokens: 100,
      output_tokens: 0, cache_creation_input_tokens: 0, cache_read_input_tokens: 0,
      error_count: 0, latency_ms_sum: 10, latency_count: 1,
      virtual_cost_micros: state === 'priced' ? 500_000 : state === 'partial' && !unpriced ? (index % 2 === 0 ? 750_000 : 500_000) : 0,
      pricing_completeness: unpriced ? 'unpriced' : state === 'partial' ? 'priced' : state,
      priced_request_count: priced ? 1 : 0,
      unpriced_request_count: unpriced ? 1 : 0,
      unmetered_request_count: 0,
    };
  });
  const amount = buckets.reduce((sum, bucket) => sum + bucket.virtual_cost_micros, 0);
  const counts = {
    pricing_completeness: state,
    priced_request_count: buckets.reduce((sum, bucket) => sum + (bucket.priced_request_count ?? 0), 0),
    unpriced_request_count: buckets.reduce((sum, bucket) => sum + (bucket.unpriced_request_count ?? 0), 0),
    unmetered_request_count: 0,
  };
  const event = {
    event_id: 'pricing-event', request_id: 'pricing-request', ts: NOW - 60, ts_ms: (NOW - 60) * 1000,
    principal_id: PRINCIPAL, upstream_id: 'anthropic-pricing', upstream_name: 'Anthropic pricing',
    model: 'pricing-model', event_kind: 'messages', source_kind: 'proxy',
    status: 200, duration_ms: 10, input_tokens: 100, output_tokens: 10,
    cost_usd_micros: state === 'unpriced' || state === 'partial' ? null : state === 'priced' ? 500_000 : 0,
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
      totals: { ...counts, request_count: 5, input_tokens: 500, output_tokens: 0, cache_creation_input_tokens: 0, cache_read_input_tokens: 0, error_count: 0, error_rate: 0, virtual_cost_micros: amount, avg_latency_ms: 10 },
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
  () => deadline.reject(new Error('Baseline Vite did not become ready within 30 seconds')),
  30_000,
);

try {
  await Promise.race([server.listen(), deadline.promise]);
  clearTimeout(readyTimer);
  browser = await chromium.launch();
  await mkdir(outputRoot, { recursive: true });
  const captures = [];
  for (const viewport of [
    { width: 390, height: 844 },
    { width: 1440, height: 900 },
  ]) {
    for (const theme of ['dark', 'light'] as const) {
      for (const state of ['unpriced', 'partial', 'known_zero'] as const) {
        const context = await browser.newContext({ viewport, colorScheme: theme, reducedMotion: 'reduce' });
        try {
          const page = await context.newPage();
          await installPricingFixtures(page, state, theme);
          const origin = `http://127.0.0.1:${port}`;
          const { width, height } = viewport;
          await page.goto(`${origin}/providers/anthropic?range=24h`);
          await expect(page.locator('html')).toHaveAttribute('data-theme', theme);
          const cost = page.getByTestId('overview-kpi-cost');
          await expect(cost).toBeVisible();
          await expect(page.getByTestId('top-principal-row')).toHaveCount(1);
          await page.evaluate(() => document.fonts.ready);
          const costText = await cost.innerText();
          const overviewFile = `overview-${state}-${width}-${theme}.png`;
          await page.screenshot({ path: resolve(outputRoot, overviewFile), fullPage: true, animations: 'disabled' });

          await page.goto(`${origin}/?range=24h`);
          await page.getByRole('row', { name: 'View request pricing-event' }).getByText('pricing-model', { exact: true }).click();
          const drawer = page.getByRole('dialog', { name: 'Request detail' });
          await expect(drawer).toBeVisible();
          const drawerFile = `drawer-${state}-${width}-${theme}.png`;
          await page.screenshot({ path: resolve(outputRoot, drawerFile), animations: 'disabled' });
          captures.push({ state, width, height, theme, overviewFile, drawerFile, costText, drawerText: await drawer.innerText() });
        } finally {
          await context.close();
        }
      }
    }
  }
  await writeFile(resolve(outputRoot, 'capture.json'), JSON.stringify({ sourceSha, fixture: 'overview-cost-completeness', syntheticAdminApi: true, captures }, null, 2));
  console.log(`Captured baseline pricing overview and request drawer in both viewports and themes for ${sourceSha}`);
} finally {
  clearTimeout(readyTimer);
  try {
    await browser?.close();
  } finally {
    await server.close();
  }
}
