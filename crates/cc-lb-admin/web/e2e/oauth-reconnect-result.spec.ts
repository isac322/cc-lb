import { expect, test, type Locator, type Page } from '@playwright/test';

type Provider = 'anthropic' | 'openai' | 'gemini';
type Scenario = 'same' | 'different' | 'mismatch' | 'unknown' | 'lifetime';

type CurrentCase = {
  provider: Provider;
  scenario: Scenario;
  completed: boolean;
};

const WIDTHS = [390, 768, 1440] as const;
const UPSTREAM_ID = 'oauth-reconnect-qa';

function upstream(provider: Provider) {
  return {
    id: UPSTREAM_ID,
    name: `${provider} reconnect QA`,
    kind: `${provider}_oauth`,
    enabled: true,
    spec_revision: 4,
    base_url:
      provider === 'anthropic'
        ? 'https://api.anthropic.com'
        : provider === 'openai'
          ? 'https://api.openai.com'
          : 'https://generativelanguage.googleapis.com',
    warmup_enabled: false,
    warmup_dialect_plugin: null,
    status: {
      last_apply_error: null,
      last_apply_at_unix_secs: null,
      last_warmup_at_unix_secs: null,
    },
  };
}

function identity(provider: Provider, account: 'before' | 'after') {
  const suffix = account === 'before' ? 'before' : 'after';
  return {
    subscription_metadata: null,
    organization_metadata: null,
    account_email: `${provider}-${suffix}@example.com`,
    account_id: `${provider}-account-${suffix}`,
    plan: provider === 'gemini' ? 'standard' : 'team',
    project_id: provider === 'gemini' ? 'code-assist-project' : null,
  };
}

function metadataFor(current: CurrentCase) {
  if (current.scenario === 'unknown' && current.completed)
    return {
      subscription_metadata: null,
      organization_metadata: null,
      account_email: null,
      account_id: null,
      plan: null,
      project_id: null,
    };
  if (current.scenario === 'different' && current.completed)
    return identity(current.provider, 'after');
  return identity(current.provider, 'before');
}

function completionFor(current: CurrentCase) {
  return {
    upstream_id: UPSTREAM_ID,
    expires_at_unix_secs: Math.floor(Date.now() / 1000) + 3600,
    access_token_fingerprint: 'qa-fingerprint',
    mode: current.scenario === 'lifetime' && current.provider === 'anthropic'
      ? 'long_lived_365d'
      : 'refreshing',
    long_lived_fallback: false,
    fallback_reason: null,
    granted_expires_in_secs: null,
  };
}

async function installFixtures(page: Page, current: CurrentCase) {
  await page.addInitScript(() => {
    window.localStorage.setItem('cc-lb-admin-token', 'oauth-reconnect-qa');
  });

  await page.route('**/admin/**', async (route) => {
    const request = route.request();
    const url = new URL(request.url());
    const pathname = url.pathname;
    const method = request.method();
    const json = (status: number, body: unknown) =>
      route.fulfill({
        status,
        contentType: 'application/json',
        body: JSON.stringify(body),
      });

    if (pathname === '/admin/v1/auth/session' && method === 'GET') {
      return json(200, {
        authority: 'e2e',
        subject: 'oauth-reconnect-operator',
        kind: 'human',
        provider_id: 'static',
        email: null,
        display_name: 'OAuth reconnect operator',
        expires_at_unix_secs: null,
        auth_mode: 'static_token',
      });
    }
    if (pathname === '/admin/v1/upstreams' && method === 'GET')
      return json(200, { upstreams: [upstream(current.provider)] });
    if (pathname === '/admin/v1/plugins/registry' && method === 'GET')
      return json(200, { entries: [] });
    if (pathname === `/admin/v1/upstreams/${UPSTREAM_ID}` && method === 'GET')
      return json(200, upstream(current.provider));
    if (pathname === `/admin/v1/upstreams/${UPSTREAM_ID}/oauth/status`) {
      return json(200, {
        upstream_id: UPSTREAM_ID,
        kind: `${current.provider}_oauth`,
        has_credentials: true,
        status: 'active',
        expires_at_unix_secs: Math.floor(Date.now() / 1000) + 3600,
        refresh_token_present: true,
        refresh_token_expires_at_unix_secs: null,
        mode:
          current.scenario === 'lifetime' && current.provider === 'anthropic'
            ? 'long_lived_365d'
            : 'refreshing',
        can_refresh: current.provider !== 'anthropic' || current.scenario !== 'lifetime',
        scopes: ['user:inference'],
      });
    }
    if (pathname === `/admin/v1/upstreams/${UPSTREAM_ID}/subscription-metadata/refresh`)
      return json(200, metadataFor(current));
    if (pathname === `/admin/v1/upstreams/${UPSTREAM_ID}/subscription-metadata`)
      return json(200, metadataFor(current));
    if (pathname === `/admin/v1/upstreams/${UPSTREAM_ID}/warmup`)
      return json(200, { upstream_id: UPSTREAM_ID, dialect_plugin: null, lanes: [{ lane: 'primary', eligible: true, ineligible_reason: null, target_windows: ['5h', '7d'], next_scheduled_at_unix_secs: null, last_attempt: null, recent_attempts: [] }] });
    if (pathname === `/admin/v1/upstreams/${UPSTREAM_ID}/limit-resets`)
      return json(200, { upstream_id: UPSTREAM_ID, windows: [] });
    if (pathname === '/admin/v1/status')
      return json(200, { version: 'qa', git_sha: 'qa', uptime_secs: 1, build: { rust_version: 'qa', profile: 'debug', target: 'qa' }, generation: 1, upstreams: [] });
    if (pathname === '/admin/v1/dashboard/usage')
      return json(200, { range: '24h', step: 'hour', group_by: 'model', window_start_unix_secs: 0, window_end_unix_secs: 0, observed: true, series: [] });
    if (pathname === '/admin/v1/events/recent')
      return json(200, { events: [], observed: true, count: 0, limit: 5 });
    if (pathname === '/admin/v1/subscription-quotas/latest')
      return json(200, { now_unix_secs: Math.floor(Date.now() / 1000), max_staleness_secs: 300, upstreams: [{ upstream_id: UPSTREAM_ID, windows: [] }] });
    if (pathname === '/admin/v1/subscription-quotas/series')
      return json(200, { since_unix_secs: 0, until_unix_secs: 0, bucket_secs: 60, source: 'merged', series: [] });
    if (pathname === `/admin/v1/upstreams/${UPSTREAM_ID}/oauth/start` && method === 'POST')
      return json(200, {
        authorize_url: 'https://provider.example/oauth/authorize?state=qa-state',
        state_token: 'qa-state-token',
        provider: current.provider,
        flow: current.provider === 'openai' ? 'paste_code' : 'browser',
        expires_at_unix_secs: Math.floor(Date.now() / 1000) + 900,
      });
    if (pathname === `/admin/v1/upstreams/${UPSTREAM_ID}/oauth/complete` && method === 'POST') {
      if (current.scenario === 'mismatch') {
        const label = current.provider === 'openai' ? 'ChatGPT' : 'Google';
        return json(409, { error: 'account_mismatch', detail: `Reconnect with the same ${label} account as the saved upstream.` });
      }
      current.completed = true;
      return json(200, completionFor(current));
    }
    return json(200, {});
  });
}

function providerSignInLabel(provider: Provider) {
  return provider === 'anthropic'
    ? 'Sign in with Claude'
    : provider === 'openai'
      ? 'Sign in with ChatGPT'
      : 'Sign in with Google';
}

function callbackValue(provider: Provider) {
  return provider === 'anthropic'
    ? 'https://claude.example/oauth/callback?code=oauthcode-1234567890abcdef&state=qa-state'
    : provider === 'openai'
      ? 'http://localhost:1455/auth/callback?code=ac_reconnect_1234567890abcdef&state=qa-state'
      : 'http://localhost:51121/oauth-callback?code=gemini-code-1234567890abcdef&state=qa-state';
}

async function openReconnect(page: Page, current: CurrentCase) {
  await page.goto(`/upstreams?selectedId=${UPSTREAM_ID}&action=reconnect`);
  const dialog = page.getByRole('dialog').last();
  await expect(dialog).toBeVisible();
  await dialog.getByRole('link', { name: providerSignInLabel(current.provider) }).click();
  const code = dialog.getByLabel('Authorization code');
  await expect(code).toBeVisible();
  await code.fill(callbackValue(current.provider));
  await dialog.getByRole('button', { name: 'Connect' }).click();
  return dialog;
}

async function assertGeometry(page: Page, dialog: Locator) {
  await expect.poll(() =>
    page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth),
  ).toBe(true);
  await expect.poll(() =>
    dialog.evaluate((element) => element.scrollWidth <= element.clientWidth),
  ).toBe(true);
  const box = await dialog.boundingBox();
  expect(box).not.toBeNull();
  expect(box!.x).toBeGreaterThanOrEqual(0);
  expect(box!.x + box!.width).toBeLessThanOrEqual(page.viewportSize()!.width);
}

test('OAuth reconnect results stay readable at supported widths', async ({ page }) => {
  const current: CurrentCase = { provider: 'anthropic', scenario: 'same', completed: false };
  await installFixtures(page, current);

  const cases: Array<Pick<CurrentCase, 'provider' | 'scenario'>> = [
    { provider: 'anthropic', scenario: 'same' },
    { provider: 'openai', scenario: 'same' },
    { provider: 'gemini', scenario: 'same' },
    { provider: 'anthropic', scenario: 'different' },
    { provider: 'openai', scenario: 'mismatch' },
    { provider: 'gemini', scenario: 'mismatch' },
    { provider: 'anthropic', scenario: 'unknown' },
    { provider: 'openai', scenario: 'unknown' },
    { provider: 'gemini', scenario: 'unknown' },
    { provider: 'anthropic', scenario: 'lifetime' },
    { provider: 'openai', scenario: 'lifetime' },
    { provider: 'gemini', scenario: 'lifetime' },
  ];

  for (const width of WIDTHS) {
    await page.setViewportSize({ width, height: 900 });
    for (const next of cases) {
      current.provider = next.provider;
      current.scenario = next.scenario;
      current.completed = false;
      const dialog = await openReconnect(page, current);
      await assertGeometry(page, dialog);

      if (current.scenario === 'mismatch') {
        const label = current.provider === 'openai' ? 'ChatGPT' : 'Google';
        await expect(dialog).toContainText(
          `Reconnect with the same ${label} account`,
        );
        await expect(dialog.getByRole('button', { name: 'Done' })).toHaveCount(0);
        await expect(dialog).not.toContainText('bills the new account');
        await assertGeometry(page, dialog);
        await dialog.getByRole('button', { name: 'Cancel' }).click();
      } else {
        await expect(dialog.getByRole('button', { name: 'Done' })).toBeVisible();
        if (current.scenario === 'different') {
          await expect(dialog).toContainText('A different Claude account was connected');
          await expect(dialog).toContainText('Before');
          await expect(dialog).toContainText('Now');
          await expect(dialog).toContainText('bills the new account');
        } else if (current.scenario === 'unknown') {
          await expect(dialog).toContainText('Account details are still loading');
        } else if (current.scenario === 'same') {
          const sameCopy =
            current.provider === 'anthropic'
              ? 'Same account as before — nothing else changed.'
              : current.provider === 'openai'
                ? 'Same ChatGPT account as before — nothing else changed.'
                : 'Same Google account as before — nothing else changed.';
          await expect(dialog).toContainText(sameCopy);
          if (current.provider !== 'anthropic')
            await expect(dialog).not.toContainText('Claude');
        } else if (current.scenario === 'lifetime' && current.provider === 'anthropic') {
          await expect(dialog).toContainText('Long-lived connection');
        } else {
          await expect(dialog).toContainText('Refreshing connection — renews automatically.');
        }
        await assertGeometry(page, dialog);
        await dialog.getByRole('button', { name: 'Done' }).click();
      }
    }
  }
});
