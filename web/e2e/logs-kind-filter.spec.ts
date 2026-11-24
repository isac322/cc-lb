import { expect, test } from '@playwright/test';
import type { RequestEvent } from '../src/lib/api';

test('All kinds includes history, streamed, and resumed renewals without changing Messages', async ({
  page,
}) => {
  const now = Math.floor(Date.now() / 1_000);
  const event = (
    id: string,
    offset: number,
    renewal = false,
  ): RequestEvent => ({
    event_id: id,
    request_id: id,
    ts: now - 10 + offset,
    ts_ms: (now - 10 + offset) * 1_000,
    source_kind: renewal ? 'renewal' : 'proxy',
    event_kind: renewal ? null : 'messages',
    status: 200,
    duration_ms: 10,
  });
  const message = event('history-message', 0);
  const renewal = event('history-renewal', 1, true);
  const streamedRenewal = event('stream-renewal', 2, true);
  const resumedRenewal = event('delta-renewal', 3, true);
  const requests: URL[] = [];

  await page.addInitScript(() => {
    window.localStorage.setItem('cc-lb-admin-token', 'mock-token');
  });
  await page.route('**/admin/**', async (route) => {
    const url = new URL(route.request().url());
    const json = (body: unknown) =>
      route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify(body),
      });
    if (url.pathname === '/admin/v1/auth/session') {
      return json({
        authority: 'e2e',
        subject: 'e2e-operator',
        kind: 'human',
        provider_id: 'static',
        email: null,
        display_name: 'E2E Operator',
        expires_at_unix_secs: null,
        auth_mode: 'static_token',
      });
    }
    if (url.pathname.startsWith('/admin/v1/events/')) {
      requests.push(url);
      if (url.searchParams.get('event_kind') === 'all') {
        return route.fulfill({
          status: 400,
          contentType: 'application/json',
          body: JSON.stringify({ error: 'invalid_event_kind' }),
        });
      }
      // Mirror the API's default renewal exclusion, not the selected UI label.
      const kind = url.searchParams.get('event_kind');
      const includeRenewals =
        url.searchParams.get('source_kind') === 'all' || kind === 'renewal';
      if (url.pathname.endsWith('/recent')) {
        const events = [renewal, message].filter((row) =>
          row.source_kind === 'renewal' ? includeRenewals : kind !== 'renewal',
        );
        return json({
          events,
          observed: true,
          count: events.length,
          limit: 50,
          cursor: 2,
        });
      }
      if (url.pathname.endsWith('/histogram')) {
        return json({ buckets: [], bucket_ms: 60_000, bucket_count: 0 });
      }
      if (url.pathname.endsWith('/delta')) {
        return json({
          events: includeRenewals ? [resumedRenewal] : [],
          next_cursor: 4,
          exhausted: true,
        });
      }
      if (url.pathname.endsWith('/stream')) {
        const update = {
          phase: 'final',
          payload: { event: streamedRenewal, cursor: 3 },
        };
        return route.fulfill({
          status: 200,
          contentType: 'text/event-stream',
          body: includeRenewals
            ? `id: 3\ndata: ${JSON.stringify(update)}\n\n`
            : 'event: cursor\nid: 2\ndata: {}\n\n',
        });
      }
    }
    if (url.pathname === '/admin/v1/upstreams') return json({ upstreams: [] });
    if (url.pathname === '/admin/v1/principals') return json({ principals: [] });
    if (url.pathname === '/admin/health') {
      return json({ status: 'ok', version: 'test', git_sha: 'test', uptime_secs: 1 });
    }
    return json({});
  });

  await page.goto('/logs');
  await expect(page.getByLabel('View request history-message')).toBeVisible();
  await expect(page.getByLabel('View request history-renewal')).toHaveCount(0);
  await page.getByRole('button', { name: /^Filters/ }).click();
  await page.getByRole('combobox', { name: 'Kind', exact: true }).click();
  await page.getByRole('option', { name: 'All kinds', exact: true }).click();
  await expect(page).toHaveURL(/event_kind=all/);
  await expect(page.getByLabel('View request history-renewal')).toBeVisible();
  await expect(page.getByLabel('View request history-message')).toBeVisible();
  await expect(page.getByLabel('View request stream-renewal')).toBeVisible();

  const liveTail = page.getByRole('button', {
    name: 'Live tail logs',
    exact: true,
  });
  await liveTail.click();
  await expect(liveTail).toHaveAttribute('aria-pressed', 'false');
  await liveTail.click();
  await expect(page.getByLabel('View request delta-renewal')).toBeVisible();

  for (const endpoint of ['recent', 'histogram', 'stream', 'delta']) {
    const allRequests = requests.filter(
      (url) =>
        url.pathname.endsWith(`/${endpoint}`) &&
        url.searchParams.get('source_kind') === 'all',
    );
    expect(
      allRequests.length,
      `${endpoint} preserves All kinds`,
    ).toBeGreaterThan(0);
    expect(
      allRequests.every((url) => !url.searchParams.has('event_kind')),
    ).toBe(true);
  }
  expect(
    requests.some((url) => url.searchParams.get('event_kind') === 'all'),
  ).toBe(false);
  const delta = requests.find((url) => url.pathname.endsWith('/delta'));
  expect(delta?.searchParams.get('since_cursor')).toBe('3');

  await page.getByRole('combobox', { name: 'Kind', exact: true }).click();
  await page.getByRole('option', { name: 'Renewals', exact: true }).click();
  await expect(page.getByLabel('View request history-renewal')).toBeVisible();
  await expect(page.getByLabel('View request history-message')).toHaveCount(0);

  await page.getByRole('combobox', { name: 'Kind', exact: true }).click();
  await page.getByRole('option', { name: 'Messages', exact: true }).click();
  await expect(page.getByLabel('View request history-message')).toBeVisible();
  await expect(page.getByLabel('View request history-renewal')).toHaveCount(0);
  await expect(page.getByLabel('View request stream-renewal')).toHaveCount(0);
  await expect(page.getByLabel('View request delta-renewal')).toHaveCount(0);
  await expect(page).not.toHaveURL(/event_kind=/);
});
