import { spawnSync } from 'node:child_process';
import { readFileSync, statSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { expect, type Page, type Response, test } from '@playwright/test';

// QA-08: real HTTP requests through path-only proxies. QA-24/25: real SPA
// drawers in Chromium. QA-26: read-only SQLite identifier snapshots.
const manifestPath = process.env.DETAIL_FIXTURE_MANIFEST;
if (!manifestPath) throw new Error('DETAIL_FIXTURE_MANIFEST is required');
const manifest = JSON.parse(readFileSync(manifestPath, 'utf8')) as {
  mode: 'old' | 'new';
  origins: Record<'direct' | 'forward' | 'redirect', string>;
  principal_id: string;
  keepalive_ids: Record<string, string>;
  control_session_id: string;
  event_ids: { renewal: string; ordinary: string };
  credentials_file: string;
  db_snapshot_before: unknown;
};
const credentials = readFileSync(manifest.credentials_file, 'utf8');
if ((statSync(manifest.credentials_file).mode & 0o777) !== 0o600)
  throw new Error('fixture credentials must have mode 0600');
const token = /^QA_ADMIN_TOKEN=(.+)$/m.exec(credentials)?.[1];
if (!token) throw new Error('fixture credentials are missing the admin token');
const headers = { Authorization: `Bearer ${token}`, Accept: 'application/json' };
const qaDir = path.dirname(fileURLToPath(import.meta.url));
type Kind = 'keepalive' | 'event';
type Transport = keyof typeof manifest.origins;

function endpoint(kind: Kind, id: string): string {
  const prefix = kind === 'keepalive'
    ? `/admin/v1/principals/${manifest.principal_id}/cache-keepalive`
    : '/admin/v1/events/detail';
  if (manifest.mode === 'old')
    return `${prefix}/${encodeURIComponent(id)}`;
  return `${prefix}${kind === 'keepalive' ? '/detail' : ''}?${new URLSearchParams({ id })}`;
}

function matchesDetail(response: Response, kind: Kind, id: string): boolean {
  if (response.request().method() !== 'GET' || response.status() === 307)
    return false;
  const url = new URL(response.url());
  const expected = new URL(endpoint(kind, id), manifest.origins.direct);
  if (manifest.mode === 'new')
    return url.pathname === expected.pathname && url.searchParams.get('id') === id;
  const prefix = kind === 'keepalive'
    ? `/admin/v1/principals/${manifest.principal_id}/cache-keepalive/`
    : '/admin/v1/events/detail/';
  return url.pathname.startsWith(prefix) &&
    decodeURIComponent(url.pathname.slice(prefix.length)) === id;
}

function broken(transport: Transport, id: string): boolean {
  return manifest.mode === 'old' && transport !== 'direct' && id.includes('/');
}

function identifierSnapshot(): unknown {
  const result = spawnSync(
    process.env.DETAIL_PYTHON ?? 'python3',
    [path.join(qaDir, 'detail-transport-fixture.py'), '--snapshot', '--manifest', manifestPath!],
    { encoding: 'utf8' },
  );
  if (result.status !== 0) throw new Error(`fixture snapshot failed: ${result.stderr}`);
  return JSON.parse(result.stdout);
}

async function signIn(page: Page, origin: string): Promise<void> {
  await page.goto(origin);
  await expect(page.getByRole('heading', { name: 'Admin token required' })).toBeVisible();
  await page.getByLabel('Bearer token').fill(token!);
  await page.getByRole('button', { name: 'Sign in' }).click();
  await expect(page.getByRole('heading', { name: 'Admin token required' })).toBeHidden();
}

test('QA-08 HTTP detail matrix preserves exact IDs and bodies across path-only proxies', async ({ request }, testInfo) => {
  const resources: Array<{ kind: Kind; id: string }> = [
    ...Object.values(manifest.keepalive_ids).map((id) => ({ kind: 'keepalive' as const, id })),
    { kind: 'keepalive', id: manifest.control_session_id },
    ...Object.entries(manifest.keepalive_ids)
      .filter(([state]) => state !== 'not_tracked')
      .map(([, id]) => ({ kind: 'event' as const, id: `renewal:${id}:1` })),
    { kind: 'event', id: manifest.event_ids.ordinary },
  ];
  const evidence: unknown[] = [];
  for (const resource of resources) {
    const target = endpoint(resource.kind, resource.id);
    const direct = await request.get(manifest.origins.direct + target, { headers });
    expect(direct.status()).toBe(200);
    const expected = await direct.json();
    expect(expected[resource.kind === 'keepalive' ? 'id' : 'event_id']).toBe(resource.id);
    for (const transport of ['direct', 'forward', 'redirect'] as const) {
      const first = await request.get(manifest.origins[transport] + target, { headers, maxRedirects: 0 });
      let final = first;
      const expectsRedirect = manifest.mode === 'old' && transport === 'redirect' && resource.id.includes('/');
      if (expectsRedirect) {
        expect(first.status()).toBe(307);
        const expectedLocation = target.replace(/%2f/gi, '/');
        expect(first.headers().location).toBe(expectedLocation);
        final = await request.get(manifest.origins[transport] + expectedLocation, { headers, maxRedirects: 0 });
      } else {
        expect(first.status()).not.toBe(307);
      }
      if (broken(transport, resource.id)) {
        expect(final.status()).toBe(404);
        expect(await final.body()).toHaveLength(0);
      } else {
        expect(final.status()).toBe(200);
        expect(await final.json()).toEqual(expected);
      }
      evidence.push({ ...resource, transport, target, first_status: first.status(), location: first.headers().location ?? null, status: final.status(), body: await final.text() });
    }
  }
  await testInfo.attach('http-detail-matrix.json', { body: JSON.stringify(evidence, null, 2), contentType: 'application/json' });
  expect(identifierSnapshot()).toEqual(manifest.db_snapshot_before);
});

for (const transport of ['direct', 'forward', 'redirect'] as const) {
  test(`QA-24/25/26 ${transport}: all keepalive states and event drawers use real-server detail`, async ({ page, request }, testInfo) => {
    const origin = manifest.origins[transport];
    const consoleErrors: string[] = [];
    const detailResponses: Array<{ kind: Kind; id: string; status: number; body: unknown }> = [];
    page.on('pageerror', (error) => consoleErrors.push(error.message));
    try {
      await signIn(page, origin);
      page.on('console', (message) => {
        if (message.type() === 'error') consoleErrors.push(message.text());
      });
      await page.goto(`${origin}/principals?selectedId=${manifest.principal_id}`);
      await page.getByTestId('cache-keepalive-card').getByRole('button', { name: 'Sessions', exact: true }).click();
      const drawer = page.getByRole('dialog', { name: 'Cache keepalive sessions' });
      await expect(drawer).toBeVisible();
      let previousId: string | null = null;
      for (const [state, id] of Object.entries(manifest.keepalive_ids)) {
        const direct = await request.get(manifest.origins.direct + endpoint('keepalive', id), { headers });
        expect(direct.status()).toBe(200);
        const expected = await direct.json();
        expect(expected.id).toBe(id);
        if (state === 'error') expect(expected.error).toBe('renewal dispatch unavailable');
        else expect(expected.state).toBe(state);
        const pending = page.waitForResponse((response) => matchesDetail(response, 'keepalive', id));
        await drawer.locator(`li[data-key=${JSON.stringify(id)}]`).getByRole('button').click();
        const response = await pending;
        if (broken(transport, id)) {
          expect(response.status()).toBe(404);
          await expect(page.getByTestId('session-detail-error')).toBeVisible();
          await expect(page.getByTestId('session-detail-content')).toHaveCount(0);
        } else {
          expect(response.status()).toBe(200);
          const actual = await response.json();
          expect(actual).toEqual(expected);
          const pane = page.getByTestId('session-detail-content');
          await expect(pane).toBeVisible();
          await expect(pane.locator('dt').filter({ hasText: /^Session ID$/ }).locator('xpath=following-sibling::dd[1]')).toHaveText(id);
          await expect(pane.locator('dt').filter({ hasText: /^Total renewals$/ }).locator('xpath=following-sibling::dd[1]')).toHaveText(String(expected.total_renewals));
          if (state === 'not_tracked') await expect(pane).toContainText('no renewals fired');
          else await expect(pane).toContainText(`saved $${expected.total_avoided.toFixed(3)}`);
          await expect(page.getByTestId('session-detail-error')).toHaveCount(0);
          if (state === 'error') await expect(pane).toContainText(expected.error);
          await expect(pane.locator('dt').filter({ hasText: /^TTL$/ }).locator('xpath=following-sibling::dd[1]')).toHaveText(expected.ttl ?? '—');
          const stateLabel: Record<string, string> = { renewed: 'Renewed', scheduled: 'Scheduled', capped: 'Capped', expired: 'Expired', not_tracked: 'Not tracked' };
          await expect(pane.getByText(stateLabel[expected.state], { exact: true }).and(pane.locator('span'))).toBeVisible();
          for (const turn of expected.turns)
            await expect(pane).toContainText(`Turn ${turn.turn_number}`);
          await pane.getByRole('button', { name: 'Config in effect at schedule time' }).click();
          if (expected.config_snapshot)
            await expect(pane.locator('dt').filter({ hasText: /^Max renewals$/ }).locator('xpath=following-sibling::dd[1]')).toHaveText(String(expected.config_snapshot.max_renewals));
          await pane.getByRole('button', { name: 'Raw session record' }).click();
          expect(JSON.parse(await pane.locator('pre').innerText())).toEqual(expected.raw_record);
          if (previousId && previousId !== id) await expect(pane).not.toContainText(previousId);
          if (state === 'renewed') {
            const polled = await page.waitForResponse((item) => matchesDetail(item, 'keepalive', id), { timeout: 15_000 });
            expect(polled.status()).toBe(200);
            expect(await polled.json()).toEqual(expected);
          }
        }
        detailResponses.push({ kind: 'keepalive', id, status: response.status(), body: response.status() === 200 ? await response.json() : await response.text() });
        previousId = id;
        await page.getByRole('button', { name: 'Back to sessions' }).click();
      }
      for (const [kind, id] of Object.entries(manifest.event_ids)) {
        // Explicit renewal filter avoids the unrelated Logs all-kinds omission (#1050).
        await page.goto(`${origin}/logs?event_kind=${kind === 'renewal' ? 'renewal' : 'messages'}`);
        const direct = await request.get(manifest.origins.direct + endpoint('event', id), { headers });
        expect(direct.status()).toBe(200);
        const expected = await direct.json();
        const pending = page.waitForResponse((response) => matchesDetail(response, 'event', id));
        await page.getByRole('row', { name: `View request ${id}`, exact: true }).click();
        const response = await pending;
        const drawer = page.getByRole('dialog', { name: 'Request detail' });
        await expect(drawer).toBeVisible();
        await expect(drawer).toContainText(expected.request_id);
        if (broken(transport, id)) {
          expect(response.status()).toBe(404);
          await expect(drawer).not.toContainText('Body bytes');
        } else {
          expect(response.status()).toBe(200);
          expect(await response.json()).toEqual(expected);
          await expect(drawer).toContainText('Latency');
          await expect(drawer).toContainText('Cost');
          await expect(drawer).toContainText('Body bytes');
        }
        detailResponses.push({ kind: 'event', id, status: response.status(), body: response.status() === 200 ? await response.json() : await response.text() });
      }
      if (manifest.mode === 'new' || transport === 'direct') expect(consoleErrors).toEqual([]);
    } finally {
      expect(identifierSnapshot()).toEqual(manifest.db_snapshot_before);
      await testInfo.attach(`${transport}-drawer-evidence.json`, { body: JSON.stringify({ mode: manifest.mode, detailResponses, consoleErrors }, null, 2), contentType: 'application/json' });
    }
  });
}
