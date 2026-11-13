import { expect, type Locator, type Page, test } from '@playwright/test';
import type { PricingCompleteness, UsageBucket } from '../src/lib/api';

const NOW = Date.parse('2026-11-14T12:00:00Z') / 1000;
const PRINCIPAL = 'pricing-principal';
const STATES: readonly PricingCompleteness[] = ['priced', 'known_zero', 'partial', 'unpriced', 'unmetered', 'unknown'];
const CAPTION: Partial<Record<PricingCompleteness, string>> = {
  unpriced: 'List price unavailable', unknown: 'Price unknown', partial: '1 unpriced',
};

function scenarioBuckets(state: PricingCompleteness): UsageBucket[] {
  return Array.from({ length: 5 }, (_, index) => {
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
}

async function installPricingFixtures(page: Page, state: PricingCompleteness, theme: 'dark' | 'light') {
  const buckets = scenarioBuckets(state);
  const amount = buckets.reduce((sum, bucket) => sum + bucket.virtual_cost_micros, 0);
  const counts = {
    pricing_completeness: state,
    priced_request_count: buckets.reduce((sum, bucket) => sum + (bucket.priced_request_count ?? 0), 0),
    unpriced_request_count: buckets.reduce((sum, bucket) => sum + (bucket.unpriced_request_count ?? 0), 0),
    unmetered_request_count: buckets.reduce((sum, bucket) => sum + (bucket.unmetered_request_count ?? 0), 0),
  };
  const event = {
    event_id: 'pricing-event', request_id: 'pricing-request', ts: NOW - 60, ts_ms: (NOW - 60) * 1000,
    principal_id: PRINCIPAL, upstream_id: 'anthropic-pricing', upstream_name: 'Anthropic pricing',
    model: 'pricing-model', event_kind: 'messages', source_kind: 'proxy',
    status: 200, duration_ms: 10, input_tokens: 100, output_tokens: 10,
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
      totals: { ...counts, request_count: 5, input_tokens: state === 'unmetered' ? 0 : 500, output_tokens: 0, cache_creation_input_tokens: 0, cache_read_input_tokens: 0, error_count: 0, error_rate: 0, virtual_cost_micros: amount, avg_latency_ms: 10 },
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
  return amount;
}

async function expectFits(page: Page, surface: Locator) {
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
  expect(await surface.evaluate((element) => element.scrollWidth <= element.clientWidth)).toBe(true);
}

async function expectNeutralReadable(element: Locator) {
  await expect(element).not.toHaveClass(/(?:warn|danger|amber|red|zinc)-/);
  const contrast = await element.evaluate((element) => {
    const canvas = document.createElement('canvas');
    canvas.width = canvas.height = 1;
    const context = canvas.getContext('2d');
    if (!context) throw new Error('Canvas is required for computed contrast');
    const ancestors: Element[] = [];
    for (let parent: Element | null = element; parent; parent = parent.parentElement) ancestors.unshift(parent);
    for (const ancestor of ancestors) {
      context.fillStyle = getComputedStyle(ancestor).backgroundColor;
      context.fillRect(0, 0, 1, 1);
    }
    const background = context.getImageData(0, 0, 1, 1).data;
    context.clearRect(0, 0, 1, 1);
    context.fillStyle = getComputedStyle(element).color;
    context.fillRect(0, 0, 1, 1);
    const foreground = context.getImageData(0, 0, 1, 1).data;
    const luminance = (channels: Uint8ClampedArray) => {
      const linear = Array.from(channels).slice(0, 3).map((channel) => {
        const value = channel / 255;
        return value <= 0.04045 ? value / 12.92 : ((value + 0.055) / 1.055) ** 2.4;
      });
      return linear[0] * 0.2126 + linear[1] * 0.7152 + linear[2] * 0.0722;
    };
    const fg = luminance(foreground);
    const bg = luminance(background);
    return (Math.max(fg, bg) + 0.05) / (Math.min(fg, bg) + 0.05);
  });
  expect(contrast).toBeGreaterThanOrEqual(4.5);
}

for (const width of [390, 768, 1440]) {
  for (const theme of ['dark', 'light'] as const) {
    for (const state of STATES) {
      test(`List-price ${state} stays honest at ${width}px in ${theme}`, async ({ page }, testInfo) => {
        await page.setViewportSize({ width, height: width === 390 ? 844 : 900 });
        await page.emulateMedia({ colorScheme: theme, reducedMotion: 'reduce' });
        const amount = await installPricingFixtures(page, state, theme);
        await page.goto('/providers/anthropic?range=24h');
        const tile = page.getByTestId('overview-kpi-cost');
        await expect(tile).toBeVisible();
        const indeterminate = state === 'unpriced' || state === 'unknown';
        const figure = `$${(amount / 1_000_000).toFixed(2)}${state === 'partial' ? '+' : ''}`;
        await expect(tile.locator('[data-slot="value"]')).toContainText(indeterminate ? '—' : figure);
        const caption = CAPTION[state];
        if (caption) {
          const reason = tile.getByText(caption, { exact: true });
          await expect(reason).toBeVisible();
          await expect(reason).toHaveClass(/text-text-faint/);
          await expectNeutralReadable(reason);
        }
        if (indeterminate) await expect(tile).not.toContainText('$0.00');
        const principal = page.getByTestId('top-principal-row');
        await expect(principal).toHaveCount(1);
        const cost = principal.locator('[data-slot="cost"]');
        await expect(cost).toHaveText(indeterminate ? '—' : figure);
        if (indeterminate) await expect(principal.locator('td').last()).toHaveText('—');
        if (state === 'partial') {
          const stroke = tile.locator('path.recharts-area-curve');
          await expect(stroke).toHaveCount(1);
          expect(await stroke.getAttribute('d')).toMatch(/M.*M/);
        }
        await expectFits(page, tile);
        await expectFits(page, principal);
        const capture = state === 'unpriced' || state === 'partial' || state === 'known_zero';
        if (capture) {
          await page.evaluate(() => document.fonts.ready);
          await page.screenshot({
            path: testInfo.outputPath(`overview-${state}-${width}-${theme}.png`),
            fullPage: true,
            animations: 'disabled',
          });
        }

        await page.goto('/?range=24h');
        const multi = page.getByTestId('multi-provider-overview');
        await expect(multi).toBeVisible();
        const merged = page.getByTestId('multi-provider-principal-row');
        await expect(merged).toHaveCount(1);
        const totalCost = merged.locator('td').nth(1);
        await expect(totalCost).toContainText(indeterminate ? '—' : `$${(amount * 2 / 1_000_000).toFixed(2)}${state === 'partial' ? '+' : ''}`);
        if (indeterminate) {
          const reason = merged.getByTitle(CAPTION[state]!).first();
          await expectNeutralReadable(reason);
          await expect(merged.locator('td').nth(2)).toHaveText('—');
          await expect(multi.locator('tfoot')).not.toContainText('100%');
          await expect(multi.locator('tfoot')).not.toContainText('$0.00');
        }
        if (state === 'partial') await expect(totalCost).toContainText('2 unpriced');
        await expectFits(page, totalCost);

        const request = page.getByRole('row', { name: 'View request pricing-event' });
        await request.getByText('pricing-model', { exact: true }).click();
        const drawer = page.getByRole('dialog', { name: 'Request detail' });
        await expect(drawer).toBeVisible();
        if (indeterminate || state === 'partial') {
          const unavailable = drawer.getByText('List price unavailable', { exact: true });
          await expect(unavailable).toBeVisible();
          await expectNeutralReadable(unavailable);
          await expect(drawer).not.toContainText('No cost');
          await expect(drawer).not.toContainText('$0.0000');
        } else if (state === 'priced') {
          await expect(drawer.getByText('Per-category cost not recorded')).toBeVisible();
          await expect(drawer.getByText('$0.5000')).toBeVisible();
          await expect(drawer).not.toContainText('No cost');
        } else {
          await expect(drawer.getByText('No cost', { exact: true })).toBeVisible();
        }
        await expectFits(page, drawer);
        if (capture) {
          await page.screenshot({
            path: testInfo.outputPath(`drawer-${state}-${width}-${theme}.png`),
            animations: 'disabled',
          });
        }
      });
    }
  }
}
