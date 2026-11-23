// T2: the API-key detail pane against the real server and fake-anthropic.
// Key notices (K1–K3) and Access (Endpoint, Workspace, Re-check). States the
// real server cannot reach without api.anthropic.com use FX-PW envelopes
// (`routeEnvelope`); nothing here imports src/preview.

import type { Page } from '@playwright/test';
import type { ApiKeyAccess, Upstream } from '../../src/lib/api';
import { apiKey, expect, openUpstream, test } from './harness';

const UNAUTHORIZED = {
  kind: 'status',
  status: 401,
  body: {
    type: 'error',
    error: { type: 'authentication_error', message: 'invalid x-api-key' },
  },
} as const;

/** A closed loopback port: every probe fails to connect. */
const DEAD_ENDPOINT = 'http://127.0.0.1:9';

const ORG_SELECTED: ApiKeyAccess = {
  verified: true,
  checked_at_unix_secs: Math.floor(Date.now() / 1000) - 600,
  key_class: 'org_scoped',
  inference: 'ok',
  workspace: { mode: 'selected', id: 'wrkspc_01Prod00aa', name: 'Production' },
  workspace_list: 'ok',
  usage_report: 'ok',
  key_type: 'service_account',
  last_error: null,
};

/** FX-PW: the list and detail answer `upstream` with `access` (Anthropic host). */
async function serveAs(
  routeEnvelope: (
    path: string | RegExp,
    status: number,
    body: unknown,
    method?: string,
  ) => Promise<void>,
  upstream: Upstream,
  access: ApiKeyAccess,
) {
  const shaped = { ...upstream, base_url: null, api_key_access: access };
  await routeEnvelope(/\/admin\/v1\/upstreams$/, 200, { upstreams: [shaped] }, 'GET');
  await routeEnvelope(
    new RegExp(`/admin/v1/upstreams/${upstream.id}$`),
    200,
    shaped,
    'GET',
  );
}

function access(page: Page) {
  return page.getByTestId('api-key-access');
}

test.describe('key notices', () => {
  // UI-K-02 then UI-K-01: never checked, Re-check finds a revoked key. The
  // fake is a custom base_url, so K1 names the endpoint, not Anthropic.
  test('K2 Re-check turns into K1 when the endpoint rejects the key', async ({
    page,
    fake,
    seedApiKeyUpstream,
    toasts,
  }) => {
    const key = apiKey('revoked');
    const upstream = await seedApiKeyUpstream({ key, skipPreflight: true });
    await openUpstream(page, upstream.id);

    const k2 = page.getByRole('status').filter({ hasText: 'Key not checked yet' });
    await expect(k2).toContainText(
      'Re-check → see what this key can do at Anthropic.',
    );
    await expect(access(page)).toContainText('Never checked');
    await expect(access(page).getByRole('button', { name: 'Re-check' })).toHaveCount(0);

    await fake.setKeyPolicies({
      [key]: { models: UNAUTHORIZED, count_tokens: UNAUTHORIZED },
    });
    await k2.getByRole('button', { name: 'Re-check' }).click();

    const k1 = page
      .getByRole('alert')
      .filter({ hasText: 'Your endpoint rejected this key' });
    await expect(k1).toContainText(
      'Your endpoint rejected this key. Check the key and the Base URL. Replace the key → requests through this upstream succeed again.',
    );
    await expect(k1).not.toContainText('Anthropic');
    await expect(page.getByRole('button', { name: 'Replace key' })).toHaveCount(1);
    await expect(access(page).getByRole('status')).toHaveText('Key rejected');
    await expect(access(page)).toContainText(
      'Workspace and usage reports appear after a successful check.',
    );
    expect(await toasts.errorCount()).toBe(0);
  });

  // UI-K-03: saved without checking at a dead endpoint.
  test('K3 names the endpoint, not Anthropic', async ({
    page,
    seedApiKeyUpstream,
    toasts,
  }) => {
    const upstream = await seedApiKeyUpstream({
      key: apiKey('dead'),
      baseUrl: DEAD_ENDPOINT,
      skipPreflight: true,
    });
    await openUpstream(page, upstream.id);
    await page
      .getByRole('status')
      .filter({ hasText: 'Key not checked yet' })
      .getByRole('button', { name: 'Re-check' })
      .click();

    const k3 = page.getByRole('status').filter({ hasText: 'Last check failed' });
    // D-UX4: the backend `last_error` names "the endpoint" on a custom host.
    await expect(k3).toContainText("Couldn't reach the endpoint");
    await expect(k3).toContainText('Re-check → confirm the key works.');
    await expect(access(page)).toContainText('Last attempt');
    expect(await toasts.errorCount()).toBe(0);
  });
});

test.describe('Access', () => {
  // UI-AC-01, UI-AC-02, UI-AC-17 (a): a reachable custom endpoint.
  test('endpoint edit saves, toasts, and returns focus to Change', async ({
    page,
    fake,
    seedApiKeyUpstream,
    toasts,
  }) => {
    const upstream = await seedApiKeyUpstream({ key: apiKey('endpoint') });
    await openUpstream(page, upstream.id);

    const row = access(page).getByTestId('upstream-endpoint-row');
    await expect(row).toContainText(fake.url);
    await expect(row).toContainText(
      'Requests and key checks go here instead of Anthropic.',
    );
    const change = row.getByRole('button', { name: 'Change endpoint' });
    await change.click();
    const field = row.getByLabel('Base URL');
    await expect(field).toBeFocused();
    const other = fake.url.replace('127.0.0.1', 'localhost');
    await field.fill(other);
    await row.getByRole('button', { name: 'Save' }).click();

    await expect.poll(() => toasts.texts()).toContain('Custom endpoint saved');
    await expect(row.getByRole('button', { name: 'Change endpoint' })).toBeFocused();
    await expect(row).toContainText(other);
    await expect(access(page).getByRole('status')).toHaveText('Key accepted');
    await expect(access(page).getByTestId('api-key-usage-mode')).toContainText(
      "Use Anthropic's endpoint → get reported usage.",
    );
    expect(await toasts.errorCount()).toBe(0);
  });

  // UI-AC-03: Use default + Undo. The PUT is answered by FX-PW so no probe
  // ever leaves for api.anthropic.com.
  test('Use default clears, toasts with Undo, Undo restores the URL', async ({
    page,
    seedApiKeyUpstream,
    routeEnvelope,
    toasts,
  }) => {
    const upstream = await seedApiKeyUpstream({ key: apiKey('default') });
    const cleared = {
      ...upstream,
      base_url: null,
      spec_revision: upstream.spec_revision + 1,
    };
    await routeEnvelope(
      new RegExp(`/admin/v1/upstreams/${upstream.id}$`),
      200,
      cleared,
      'PUT',
    );
    await openUpstream(page, upstream.id);

    const row = access(page).getByTestId('upstream-endpoint-row');
    await row.getByRole('button', { name: 'Change endpoint' }).click();
    const put = page.waitForRequest(
      (r) => r.method() === 'PUT' && r.url().endsWith(upstream.id),
    );
    await row.getByRole('button', { name: 'Use default' }).click();
    expect((await put).postDataJSON()).toEqual({ clear_base_url: true });
    await expect.poll(() => toasts.texts()).toContain('Using the default endpoint');

    const undo = page.waitForRequest(
      (r) => r.method() === 'PUT' && r.url().endsWith(upstream.id),
    );
    await page.getByRole('button', { name: 'Undo' }).click();
    expect((await undo).postDataJSON()).toEqual({ base_url: upstream.base_url });
    expect(await toasts.errorCount()).toBe(0);
  });

  // UI-AC-16: a header Re-check that fails at runtime.
  test('a failing header Re-check alerts in Access, never toasts', async ({
    page,
    seedApiKeyUpstream,
    routeEnvelope,
    toasts,
  }) => {
    const upstream = await seedApiKeyUpstream({ key: apiKey('recheck') });
    await routeEnvelope(
      new RegExp(`/admin/v1/upstreams/${upstream.id}/preflight$`),
      500,
      { error: 'api_key_credential_decrypt_failed' },
      'POST',
    );
    await openUpstream(page, upstream.id);
    await access(page).getByRole('button', { name: 'Re-check' }).click();
    await expect(access(page).getByRole('alert')).toContainText('Re-check failed:');
    expect(await toasts.errorCount()).toBe(0);
  });

  // UI-AC-08: change workspace from the list (Anthropic host via FX-PW).
  test('Change workspace from the list re-probes and offers Undo', async ({
    page,
    seedApiKeyUpstream,
    routeEnvelope,
    toasts,
  }) => {
    const upstream = await seedApiKeyUpstream({ key: apiKey('org-list') });
    await serveAs(routeEnvelope, upstream, ORG_SELECTED);
    await routeEnvelope(
      new RegExp(`/admin/v1/upstreams/${upstream.id}/workspaces$`),
      200,
      {
        workspaces: [
          { id: 'wrkspc_01Prod00aa', name: 'Production' },
          { id: 'wrkspc_01Stag00bb', name: 'Staging' },
        ],
      },
      'GET',
    );
    await routeEnvelope(
      new RegExp(`/admin/v1/upstreams/${upstream.id}$`),
      200,
      { ...upstream, base_url: null, spec_revision: upstream.spec_revision + 1 },
      'PUT',
    );
    await openUpstream(page, upstream.id);

    await access(page).getByRole('button', { name: 'Change workspace' }).click();
    await access(page).getByRole('combobox', { name: 'Workspace' }).click();
    await page.getByRole('option', { name: /Staging/ }).click();
    const put = page.waitForRequest(
      (r) => r.method() === 'PUT' && r.url().endsWith(upstream.id),
    );
    await access(page).getByRole('button', { name: 'Save' }).click();
    const request = await put;
    expect(request.postDataJSON()).toEqual({ workspace_id: 'wrkspc_01Stag00bb' });
    expect(request.headers()['if-match']).toBe(`W/"${upstream.spec_revision}"`);
    await expect.poll(() => toasts.texts()).toContain('Workspace set to Staging');
    await expect(page.getByRole('button', { name: 'Undo' })).toBeVisible();
    expect(await toasts.errorCount()).toBe(0);
  });

  // UI-AC-09: manual ids — local refusal, then the server's 404.
  test('manual workspace id: local check, then an inline 404', async ({
    page,
    seedApiKeyUpstream,
    routeEnvelope,
    toasts,
  }) => {
    const upstream = await seedApiKeyUpstream({ key: apiKey('org-manual') });
    await serveAs(routeEnvelope, upstream, {
      ...ORG_SELECTED,
      workspace_list: 'denied',
    });
    await routeEnvelope(
      new RegExp(`/admin/v1/upstreams/${upstream.id}$`),
      400,
      { error: { code: 'workspace_not_found', message: 'not found' } },
      'PUT',
    );
    await openUpstream(page, upstream.id);

    await access(page).getByRole('button', { name: 'Change workspace' }).click();
    const field = access(page).getByLabel('Workspace ID');
    await expect(field).toBeFocused();
    await field.fill('ws_1');
    await field.press('Enter');
    await expect(access(page).getByRole('alert')).toHaveText(
      'Workspace IDs start with wrkspc_.',
    );

    await field.fill('wrkspc_01Gone0000');
    await access(page).getByRole('button', { name: 'Save' }).click();
    await expect(access(page).getByRole('alert')).toHaveText(
      "This key can't see workspace wrkspc_01Gone0000 — check the id, or the key's account lacks access.",
    );
    await expect(field).toHaveValue('wrkspc_01Gone0000');
    expect(await toasts.errorCount()).toBe(0);
  });

  // UI-AC-10: list outcomes, zero toasts in every branch.
  for (const [label, status, body, expected] of [
    [
      'denied',
      403,
      { error: { code: 'workspace_list_denied', message: 'denied' } },
      "This key's account lacks the role to list workspaces.",
    ],
    [
      'custom host',
      403,
      { error: { code: 'non_anthropic_base_url', message: 'custom host' } },
      'Workspace lists need api.anthropic.com — paste the ID.',
    ],
    [
      'upstream error',
      502,
      { error: { code: 'probe_unreachable', message: 'unreachable' } },
      "Couldn't load workspaces:",
    ],
  ] as const) {
    test(`workspace list ${label}`, async ({
      page,
      seedApiKeyUpstream,
      routeEnvelope,
      toasts,
    }) => {
      const upstream = await seedApiKeyUpstream({ key: apiKey(`list-${status}`) });
      await serveAs(routeEnvelope, upstream, ORG_SELECTED);
      await routeEnvelope(
        new RegExp(`/admin/v1/upstreams/${upstream.id}/workspaces$`),
        status,
        body,
        'GET',
      );
      await openUpstream(page, upstream.id);
      await access(page).getByRole('button', { name: 'Change workspace' }).click();
      await expect(access(page)).toContainText(expected);
      expect(await toasts.errorCount()).toBe(0);
    });
  }
});
