// T2: OAuth panes keep their own surfaces (OAU-02) and share the Endpoint row
// with API keys (UI-O-01..03). OAuth upstreams and the sign-in draft are FX-PW
// envelopes; no request leaves for claude.ai or api.anthropic.com.

import type { Page } from '@playwright/test';
import type {
  DraftCompleteResponse,
  Upstream,
  UpstreamOAuthStatusResponse,
} from '../../src/lib/api';
import type { LimitResetsResponse } from '../../src/lib/limitResets';
import type { WarmupSummary } from '../../src/lib/queries';
import { expect, openUpstream, openUpstreams, test } from './harness';

type RouteEnvelope = (
  path: string | RegExp,
  status: number,
  body: unknown,
  method?: string,
) => Promise<void>;

const NOW = Math.floor(Date.now() / 1000);
const OAUTH_ID = '0a0a0a0a-0000-4000-8000-00000000a0a0';

function oauthUpstream(baseUrl: string | null, revision = 4): Upstream {
  return {
    id: OAUTH_ID,
    name: 'OAuth upstream',
    kind: 'anthropic_oauth',
    enabled: true,
    spec_revision: revision,
    base_url: baseUrl,
    warmup_enabled: false,
    warmup_dialect_plugin: null,
    status: {
      last_apply_error: null,
      last_apply_at_unix_secs: null,
      last_warmup_at_unix_secs: null,
    },
  };
}

function oauthStatus(): UpstreamOAuthStatusResponse {
  return {
    upstream_id: OAUTH_ID,
    kind: 'anthropic_oauth',
    has_credentials: true,
    status: 'active',
    expires_at_unix_secs: NOW + 30 * 86_400,
    refresh_token_present: true,
    refresh_token_expires_at_unix_secs: null,
    mode: 'long_lived_365d',
    can_refresh: false,
    scopes: ['user:inference'],
  };
}

/** The OAuth upstream exists only as FX-PW envelopes, so every read the
 *  OAuth pane makes is answered here; the real server would 404 them. */
async function serveOAuth(routeEnvelope: RouteEnvelope, upstream: Upstream) {
  const warmup: WarmupSummary = {
    upstream_id: OAUTH_ID,
    last_attempt: null,
    recent_attempts: [],
    next_scheduled_at_unix_secs: null,
    recent_summary_7d: {
      success: 0,
      skipped: 0,
      transient_failure: 0,
      permanent_failure: 0,
    },
    dialect_plugin: null,
  };
  const limitResets: LimitResetsResponse = {
    account_id: null,
    organization_id: null,
    cedar_ember: null,
  };
  await routeEnvelope(/\/admin\/v1\/upstreams$/, 200, { upstreams: [upstream] }, 'GET');
  await routeEnvelope(
    new RegExp(`/admin/v1/upstreams/${OAUTH_ID}$`),
    200,
    upstream,
    'GET',
  );
  await routeEnvelope(
    new RegExp(`/admin/v1/upstreams/${OAUTH_ID}/oauth/status`),
    200,
    oauthStatus(),
    'GET',
  );
  await routeEnvelope(
    new RegExp(`/admin/v1/upstreams/${OAUTH_ID}/warmup$`),
    200,
    warmup,
    'GET',
  );
  await routeEnvelope(
    new RegExp(`/admin/v1/upstreams/${OAUTH_ID}/limit-resets$`),
    200,
    limitResets,
    'GET',
  );
}

function endpointRow(page: Page) {
  return page.getByTestId('upstream-endpoint-row');
}

test.describe('OAuth pane', () => {
  // OAU-02 + UI-O-01: Quota card as before, no API-key surfaces, Endpoint row.
  test('no API usage card or Access; the Credential footer has Endpoint', async ({
    page,
    routeEnvelope,
    toasts,
  }) => {
    await serveOAuth(routeEnvelope, oauthUpstream(null));
    await openUpstream(page, OAUTH_ID);

    await expect(page.getByRole('heading', { name: 'Quota', exact: true })).toBeVisible();
    await expect(page.getByTestId('api-usage-card')).toHaveCount(0);
    await expect(page.getByTestId('api-key-credits')).toHaveCount(0);
    await expect(page.getByTestId('api-key-access')).toHaveCount(0);
    await expect(endpointRow(page)).toContainText('Anthropic (default)');
    await expect(
      endpointRow(page).getByRole('button', { name: 'Change endpoint' }),
    ).toBeVisible();
    await expect(page.getByRole('button', { name: 'Use a custom endpoint' })).toHaveCount(0);
    expect(await toasts.errorCount()).toBe(0);
  });

  // UI-O-02: edit, then Use default + Undo, as on API-key panes.
  test('custom endpoint edit and Use default with Undo', async ({
    page,
    routeEnvelope,
    toasts,
  }) => {
    await serveOAuth(routeEnvelope, oauthUpstream(null));
    await routeEnvelope(
      new RegExp(`/admin/v1/upstreams/${OAUTH_ID}$`),
      200,
      oauthUpstream('https://proxy.example.com', 5),
      'PUT',
    );
    await openUpstream(page, OAUTH_ID);

    await endpointRow(page).getByRole('button', { name: 'Change endpoint' }).click();
    const field = endpointRow(page).getByLabel('Base URL');
    await expect(field).toBeFocused();
    await expect(field).toHaveValue('');
    await field.fill('https://proxy.example.com');
    const put = page.waitForRequest(
      (r) => r.method() === 'PUT' && r.url().endsWith(OAUTH_ID),
    );
    await endpointRow(page).getByRole('button', { name: 'Save' }).click();
    const request = await put;
    expect(request.postDataJSON()).toEqual({ base_url: 'https://proxy.example.com' });
    expect(request.headers()['if-match']).toBe('W/"4"');
    await expect.poll(() => toasts.texts()).toContain('Custom endpoint saved');
    await expect(
      endpointRow(page).getByRole('button', { name: 'Change endpoint' }),
    ).toBeFocused();

    // The list now reports the stored custom URL.
    await serveOAuth(routeEnvelope, oauthUpstream('https://proxy.example.com', 5));
    await routeEnvelope(
      new RegExp(`/admin/v1/upstreams/${OAUTH_ID}$`),
      200,
      oauthUpstream(null, 6),
      'PUT',
    );
    await page.reload();
    await expect(endpointRow(page)).toContainText('https://proxy.example.com');
    await expect(endpointRow(page)).toContainText(
      'Requests go here instead of Anthropic.',
    );
    await endpointRow(page).getByRole('button', { name: 'Change endpoint' }).click();
    const clear = page.waitForRequest(
      (r) => r.method() === 'PUT' && r.url().endsWith(OAUTH_ID),
    );
    await endpointRow(page).getByRole('button', { name: 'Use default' }).click();
    expect((await clear).postDataJSON()).toEqual({ clear_base_url: true });
    await expect.poll(() => toasts.texts()).toContain('Using the default endpoint');
    const undo = page.waitForRequest(
      (r) => r.method() === 'PUT' && r.url().endsWith(OAUTH_ID),
    );
    await page.getByRole('button', { name: 'Undo' }).click();
    expect((await undo).postDataJSON()).toEqual({
      base_url: 'https://proxy.example.com',
    });
    expect(await toasts.errorCount()).toBe(0);
  });

  // UI-O-03: the create flow's last-row disclosure stores the URL, and the
  // new pane's Endpoint row shows it.
  test('OAuth Finish with a custom endpoint stores and shows the URL', async ({
    page,
    routeEnvelope,
  }) => {
    const created = oauthUpstream('https://proxy.example.com', 1);
    const draft: DraftCompleteResponse = {
      state_token: 'draft-state',
      suggested_name: 'OAuth upstream',
      subscription_metadata: null,
      organization_metadata: null,
      mode: 'long_lived_365d',
      long_lived_fallback: false,
      fallback_reason: null,
      granted_expires_in_secs: 365 * 86_400,
    };
    await routeEnvelope(/\/admin\/v1\/upstreams$/, 200, { upstreams: [] }, 'GET');
    await routeEnvelope(
      /\/admin\/v1\/oauth\/draft\/start$/,
      200,
      {
        authorize_url: 'https://claude.ai/oauth/authorize?state=draft-state',
        state_token: 'draft-state',
      },
      'POST',
    );
    await routeEnvelope(/\/admin\/v1\/oauth\/draft\/complete$/, 200, draft, 'POST');
    await routeEnvelope(
      /\/admin\/v1\/upstreams\/from-oauth-draft$/,
      200,
      created,
      'POST',
    );
    await openUpstreams(page);

    await page.getByRole('button', { name: 'Add upstream' }).click();
    const dialog = page.getByRole('dialog', { name: 'New upstream' });
    await dialog.getByRole('button', { name: 'Continue' }).click();
    await dialog.getByRole('button', { name: 'I already have a code' }).click();
    const code = dialog.getByRole('textbox', { name: 'Authorization code' });
    await code.fill('oauthcode-1234567890abcdef');
    await code.press('Enter');

    const opener = dialog.getByRole('button', { name: 'Use a custom endpoint' });
    await expect(opener).toHaveAttribute('aria-expanded', 'false');
    await opener.click();
    const field = dialog.getByRole('textbox', { name: /Base URL/ });
    await expect(field).toBeFocused();
    await expect(field).toHaveValue('');
    await field.fill('https://proxy.example.com');

    await serveOAuth(routeEnvelope, created);
    const create = page.waitForRequest((r) =>
      r.url().endsWith('/admin/v1/upstreams/from-oauth-draft'),
    );
    await dialog.getByRole('button', { name: 'Add upstream' }).click();
    expect((await create).postDataJSON()).toMatchObject({
      base_url: 'https://proxy.example.com',
    });
    await expect(endpointRow(page)).toContainText('https://proxy.example.com');
  });
});
