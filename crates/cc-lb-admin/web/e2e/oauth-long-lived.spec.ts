import { expect, type Page, test } from '@playwright/test';
import type { UpstreamOAuthStatusResponse } from '../src/lib/api';

type UpstreamFixture = {
  id: string;
  name: string;
  kind: 'anthropic_api_key' | 'anthropic_oauth';
  enabled: boolean;
  spec_revision: number;
  base_url: string;
  api_key_env?: string | null;
  warmup_enabled: boolean;
  warmup_dialect_plugin: null;
  status: {
    last_apply_error: string | null;
    last_apply_at_unix_secs: number | null;
    last_warmup_at_unix_secs: number | null;
  };
};

const DAY_SECS = 24 * 60 * 60;

function oauthUpstream(): UpstreamFixture {
  return {
    id: 'oauth-healthy',
    name: 'oauth-healthy',
    kind: 'anthropic_oauth',
    enabled: true,
    spec_revision: 4,
    base_url: 'https://api.anthropic.com',
    warmup_enabled: true,
    warmup_dialect_plugin: null,
    status: {
      last_apply_error: null,
      last_apply_at_unix_secs: null,
      last_warmup_at_unix_secs: Math.floor(Date.now() / 1000) - 20 * 60,
    },
  };
}

// A long-lived credential stores the refresh token but reports no expiry for
// it and never refreshes; the access token is the only real deadline.
function longLivedStatus(
  accessExpiresAtUnixSecs: number,
  overrides: Partial<UpstreamOAuthStatusResponse> = {},
): UpstreamOAuthStatusResponse {
  return {
    upstream_id: 'oauth-healthy',
    kind: 'anthropic_oauth',
    has_credentials: true,
    status: 'active',
    expires_at_unix_secs: accessExpiresAtUnixSecs,
    refresh_token_present: true,
    refresh_token_expires_at_unix_secs: null,
    mode: 'long_lived_365d',
    can_refresh: false,
    scopes: ['user:inference'],
    ...overrides,
  };
}

async function installAppFixtures(
  page: Page,
  options: {
    oauthStatus?: UpstreamOAuthStatusResponse;
    draftStartRequests?: unknown[];
  } = {},
) {
  const upstreams = [oauthUpstream()];
  const oauthStatus = options.oauthStatus ?? longLivedStatus(0);

  await page.addInitScript(() => {
    window.localStorage.setItem('cc-lb-admin-token', 'mock-token');
  });

  await page.route('**/admin/**', async (route) => {
    const request = route.request();
    const url = new URL(request.url());
    const pathname = url.pathname;
    const method = request.method();
    const json = (status: number, body: unknown, headers: Record<string, string> = {}) =>
      route.fulfill({
        status,
        contentType: 'application/json',
        headers,
        body: JSON.stringify(body),
      });

    // AuthRequiredGate blocks every route until this resolves; without it the
    // app renders "Unable to verify admin session" and nothing else mounts.
    if (pathname === '/admin/v1/auth/session' && method === 'GET') {
      return json(200, {
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
    if (pathname === '/admin/v1/upstreams' && method === 'GET') {
      return json(200, { upstreams });
    }
    if (pathname === '/admin/v1/plugins/registry' && method === 'GET') {
      return json(200, { entries: [] });
    }
    const upstreamMatch = pathname.match(/^\/admin\/v1\/upstreams\/([^/]+)$/);
    if (upstreamMatch && method === 'GET') {
      const upstream = upstreams.find((candidate) => candidate.id === upstreamMatch[1]);
      return upstream ? json(200, upstream, { etag: `"${upstream.spec_revision}"` }) : json(404, { error: 'not_found' });
    }
    const oauthStatusMatch = pathname.match(/^\/admin\/v1\/upstreams\/([^/]+)\/oauth\/status$/);
    if (oauthStatusMatch && method === 'GET') {
      return json(200, { ...oauthStatus, upstream_id: oauthStatusMatch[1] });
    }
    if (pathname === '/admin/v1/oauth/draft/start' && method === 'POST') {
      const body = request.postDataJSON() as Record<string, unknown> | null;
      options.draftStartRequests?.push(body);
      return json(200, {
        authorize_url: 'https://claude.ai/oauth/authorize?mock=1',
        state_token: 'mock-state-token',
      });
    }
    if (pathname.match(/^\/admin\/v1\/upstreams\/([^/]+)\/subscription-metadata/)) {
      return json(200, { upstream_id: pathname.split('/')[4], subscription_metadata: null, organization_metadata: null });
    }
    if (pathname === '/admin/v1/status') {
      return json(200, {
        version: 'mock',
        git_sha: 'mock',
        uptime_secs: 1,
        build: { rust_version: 'mock', profile: 'debug', target: 'mock' },
        generation: 1,
        upstreams: upstreams.map((upstream) => ({ id: upstream.id, name: upstream.name, status: upstream.enabled ? 'active' : 'disabled', last_apply_at_unix_secs: null, last_apply_error: null })),
      });
    }
    if (pathname === '/admin/v1/dashboard/usage') {
      return json(200, {
        range: url.searchParams.get('range') ?? '24h',
        step: url.searchParams.get('step') ?? 'hour',
        group_by: url.searchParams.get('group_by') ?? 'model',
        window_start_unix_secs: Math.floor(Date.now() / 1000) - 3600,
        window_end_unix_secs: Math.floor(Date.now() / 1000),
        observed: true,
        series: [],
      });
    }
    if (pathname === '/admin/v1/events/recent') {
      return json(200, { events: [], observed: true, count: 0, limit: 5 });
    }
    if (pathname === '/admin/v1/subscription-quotas/latest') {
      return json(200, {
        now_unix_secs: Math.floor(Date.now() / 1000),
        max_staleness_secs: 300,
        upstreams: [{ upstream_id: 'oauth-healthy', windows: [] }],
      });
    }
    if (pathname === '/admin/v1/subscription-quotas/series') {
      return json(200, { since_unix_secs: 0, until_unix_secs: 0, bucket_secs: 60, source: 'merged', series: [] });
    }
    return json(200, {});
  });
}

test.describe('OAuth long-lived credential display', () => {
  test('Scenario: healthy long-lived credential shows the 365-day mode and an unused refresh token', async ({
    page,
  }) => {
    const now = Math.floor(Date.now() / 1000);
    await installAppFixtures(page, {
      oauthStatus: longLivedStatus(now + 335 * DAY_SECS),
    });
    await page.setViewportSize({ width: 1280, height: 800 });
    await page.goto('/upstreams?selectedId=oauth-healthy');

    await expect(page.getByTestId('oauth-status-loaded-grid')).toBeVisible();

    // The card badge is driven by the access-token clock, not the refresh token.
    await expect(page.getByText('Long-lived', { exact: true })).toBeVisible();

    const credentialMode = page.getByTestId('oauth-credential-mode');
    await expect(credentialMode).toContainText('365-day token');
    await expect(credentialMode).toContainText('Never refreshed');

    // The refresh token is stored but unusable, so its expiry row is hidden.
    const cardBody = page.getByTestId('oauth-status-card-body');
    await expect(cardBody).toContainText('stored, unused');
    await expect(page.getByTestId('oauth-refresh-token-expiry')).toHaveCount(0);

    // No reconnect nudge while the access token is months out.
    await expect(page.getByText('Long-lived token expiring soon')).toHaveCount(0);
    await expect(page.getByText('Long-lived token expired')).toHaveCount(0);
  });

  test('Scenario: expired stored refresh token does not mark a long-lived credential expired', async ({
    page,
  }) => {
    const now = Math.floor(Date.now() / 1000);
    // Regression fixture: the backend still reports the stored refresh token's
    // real (lapsed) expiry. The pre-feature UI read that clock and rendered the
    // credential as Expired even though the access token has ~11 months left.
    await installAppFixtures(page, {
      oauthStatus: longLivedStatus(now + 335 * DAY_SECS, {
        refresh_token_expires_at_unix_secs: now - 5 * DAY_SECS,
      }),
    });
    await page.setViewportSize({ width: 1280, height: 800 });
    await page.goto('/upstreams?selectedId=oauth-healthy');

    await expect(page.getByTestId('oauth-status-loaded-grid')).toBeVisible();
    await expect(page.getByText('Long-lived', { exact: true })).toBeVisible();

    // No expired/danger state and no reconnect notice anywhere on the page.
    await expect(page.getByText(/expired/i)).toHaveCount(0);
    await expect(page.getByText('Reconnect required')).toHaveCount(0);
    await expect(page.getByRole('alert')).toHaveCount(0);

    const cardBody = page.getByTestId('oauth-status-card-body');
    await expect(cardBody).toContainText('stored, unused');
    await expect(page.getByTestId('oauth-refresh-token-expiry')).toHaveCount(0);
  });

  test('Scenario: long-lived credential inside the 14-day window nudges reconnect off the access-token clock', async ({
    page,
  }) => {
    const now = Math.floor(Date.now() / 1000);
    await installAppFixtures(page, {
      oauthStatus: longLivedStatus(now + 10 * DAY_SECS),
    });
    await page.setViewportSize({ width: 1280, height: 800 });
    await page.goto('/upstreams?selectedId=oauth-healthy');

    await expect(page.getByTestId('oauth-status-loaded-grid')).toBeVisible();

    const notice = page
      .getByRole('status')
      .filter({ hasText: 'Long-lived token expiring soon' });
    await expect(notice).toBeVisible();
    await expect(notice).toContainText('cannot be refreshed');
    await expect(
      notice.getByRole('button', { name: 'Reconnect' }),
    ).toBeVisible();

    // The badge warns off the same access-token deadline.
    await expect(page.getByText('Login expiring', { exact: true })).toBeVisible();
    await expect(page.getByTestId('oauth-refresh-token-expiry')).toHaveCount(0);
  });

  test('Scenario: refreshing credential still shows the refresh-token expiry', async ({
    page,
  }) => {
    const now = Math.floor(Date.now() / 1000);
    await installAppFixtures(page, {
      oauthStatus: {
        upstream_id: 'oauth-healthy',
        kind: 'anthropic_oauth',
        has_credentials: true,
        status: 'active',
        expires_at_unix_secs: now + 3600,
        refresh_token_present: true,
        refresh_token_expires_at_unix_secs: now + 20 * DAY_SECS,
        mode: 'refreshing',
        can_refresh: true,
        scopes: ['user:inference'],
      },
    });
    await page.setViewportSize({ width: 1280, height: 800 });
    await page.goto('/upstreams?selectedId=oauth-healthy');

    await expect(page.getByTestId('oauth-status-loaded-grid')).toBeVisible();
    await expect(page.getByText('Connected', { exact: true })).toBeVisible();

    const credentialMode = page.getByTestId('oauth-credential-mode');
    await expect(credentialMode).toContainText('Refreshing');
    await expect(credentialMode).toContainText('Auto-refreshes');

    const refreshExpiry = page.getByTestId('oauth-refresh-token-expiry');
    await expect(refreshExpiry).toBeVisible();
    await expect(refreshExpiry).toContainText('expires');
  });

  test('Scenario: create dialog starts the OAuth flow without a credential-mode choice', async ({
    page,
  }) => {
    const draftStartRequests: unknown[] = [];
    const now = Math.floor(Date.now() / 1000);
    await installAppFixtures(page, {
      oauthStatus: longLivedStatus(now + 335 * DAY_SECS),
      draftStartRequests,
    });
    await page.setViewportSize({ width: 1280, height: 800 });
    await page.goto('/upstreams');

    await page.getByRole('button', { name: 'New', exact: true }).click();
    const dialog = page.getByRole('dialog', { name: 'New upstream' });
    await expect(dialog).toBeVisible();
    await dialog.getByRole('button', { name: 'Continue' }).click();

    // Choosing the Claude path posts an empty body to the draft-start endpoint
    // when the sign-in step mounts: cc-lb always requests the long-lived grant
    // and decides the outcome itself.
    const signIn = dialog.getByRole('link', { name: 'Sign in with Claude' });
    await expect(signIn).toHaveAttribute(
      'href',
      'https://claude.ai/oauth/authorize?mock=1',
    );
    expect(draftStartRequests).toEqual([{}]);

    // The code comes back on the paste step; the dialog never shows the raw
    // authorize URL or the state token.
    await dialog
      .getByRole('button', { name: 'I already have a code' })
      .click();
    await expect(
      dialog.getByRole('textbox', { name: 'Authorization code' }),
    ).toBeVisible();
    await expect(dialog.getByRole('button', { name: 'Connect' })).toBeVisible();
    await expect(dialog.getByText('mock-state-token')).toHaveCount(0);
  });
});
