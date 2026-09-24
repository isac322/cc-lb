import { expect, test } from '@playwright/test';

test('errors pages contain matching rows even behind newer successes', async ({
  page,
}, testInfo) => {
  const requests: string[] = [];
  const errors = Array.from({ length: 51 }, (_, index) => ({
    event_id: `error-${index}`,
    request_id: `error-${index}`,
    ts: 1_800_000_000 + index,
    ts_ms: (1_800_000_000 + index) * 1_000,
    status: index === 0 ? 200 : 429,
    error_code: index === 0 ? 'stream_error' : null,
    duration_ms: 10,
  })).reverse();
  const successes = Array.from({ length: 60 }, (_, index) => ({
    event_id: `success-${index}`,
    request_id: `success-${index}`,
    ts: 1_800_000_100 + index,
    ts_ms: (1_800_000_100 + index) * 1_000,
    status: 200,
    error_code: null,
    duration_ms: 10,
  })).reverse();

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
    if (url.pathname === '/admin/events/recent') {
      requests.push(url.search);
      const matching =
        url.searchParams.get('status_class') === 'errors'
          ? errors
          : [...successes, ...errors];
      const cursor = Number(url.searchParams.get('until_ts_ms'));
      const pageRows = matching
        .filter((event) => !cursor || event.ts_ms < cursor)
        .slice(0, Number(url.searchParams.get('limit')));
      return json({
        events: pageRows,
        observed: pageRows.length > 0,
        count: pageRows.length,
        limit: Number(url.searchParams.get('limit')),
      });
    }
    if (url.pathname === '/admin/events/histogram') {
      return json({ buckets: [], bucket_ms: 60_000, bucket_count: 0 });
    }
    if (url.pathname === '/admin/health') {
      return json({ status: 'ok', version: 'test', git_sha: 'test', uptime_secs: 1 });
    }
    return json({});
  });

  await page.goto(
    '/logs?status=errors&since_unix_secs=1800000000&until_unix_secs=1800000200',
  );
  const pagination = page.getByRole('navigation', { name: 'Log pagination' });
  await expect(pagination).toContainText('Showing 1–50 of 50+');
  await expect(page.getByRole('row')).toHaveCount(51);
  await page.screenshot({ path: testInfo.outputPath('errors-first-page.png') });
  await page.getByRole('button', { name: 'Next page' }).click();
  await expect(pagination).toContainText('Showing 51–51 of 51');
  await expect(page.getByRole('row')).toHaveCount(2);
  await expect(page.getByRole('button', { name: 'Next page' })).toBeDisabled();
  await page.screenshot({ path: testInfo.outputPath('errors-second-page.png') });

  errors.unshift({
    event_id: 'new-error',
    request_id: 'new-error',
    ts: 1_800_000_200,
    ts_ms: 1_800_000_200_000,
    status: 429,
    error_code: null,
    duration_ms: 10,
  });
  await page.reload();
  await expect(pagination).toContainText('Showing 1–50 of 50+');
  await page.getByRole('button', { name: 'Next page' }).click();
  await expect(pagination).toContainText('Showing 51–52 of 52');
  await page.screenshot({ path: testInfo.outputPath('errors-after-new-error.png') });
  expect(requests.length).toBeGreaterThanOrEqual(2);
  expect(
    requests.every(
      (request) => new URLSearchParams(request).get('status_class') === 'errors',
    ),
  ).toBe(true);
});
