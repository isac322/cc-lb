import { expect, type Locator, type Page, test } from '@playwright/test';
import type { Principal } from '../src/lib/queries';

type ApiKeyRecord = {
  key_id: string;
  label: string | null;
  issued_at_unix_secs: number;
  revoked_at_unix_secs: number | null;
  last_4: string;
  last_used_at_unix_secs: number | null;
};

type CredentialRecord = {
  principal_id: string;
  provider: string;
  kind: string;
  identity: string;
  associated_principals: string[];
  has_credentials: boolean;
  expires_at_unix_secs: number | null;
  status: string;
  cred_id: string;
};

type Deferred<T> = {
  promise: Promise<T>;
  resolve: (value: T) => void;
};

const PRINCIPAL_ID = 'principal-pending-actions';
const ISSUED_PLAINTEXT = 'cc-live-e2e-plaintext-once';
const HEALTH_RESPONSE = {
  status: 'ok',
  version: 'e2e',
  git_sha: 'fixture',
  uptime_secs: 120,
};

function deferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((next) => {
    resolve = next;
  });
  return { promise, resolve };
}

function principal(
  id: string,
  name: string,
  overrides: Partial<Principal> = {},
): Principal {
  return {
    id,
    name,
    kind: 'machine',
    enabled: true,
    revision: 7,
    allowed_models: ['claude-sonnet-4-5'],
    allowed_upstreams: [],
    default_limits: [],
    cache_keepalive: null,
    ...overrides,
  };
}

async function storeAdminToken(page: Page) {
  await page.addInitScript(() => {
    window.localStorage.setItem('cc-lb-admin-token', 'mock-token');
  });
}

async function tryModalDismissals(
  page: Page,
  dialog: Locator,
) {
  const close = dialog.getByRole('button', { name: 'Close dialog' });
  await expect(close).toBeDisabled();
  await close.click({ force: true });
  await expect(dialog).toBeVisible();

  await page.keyboard.press('Escape');
  await expect(dialog).toBeVisible();

  const backdrop = page.locator('.bg-modal-backdrop:visible').last();
  await expect(backdrop).toBeVisible();
  await backdrop.click({ position: { x: 4, y: 4 } });
  await expect(dialog).toBeVisible();
}

async function installPrincipalFixtures(page: Page) {
  await storeAdminToken(page);

  const principals = [
    principal('principal-other', 'other-principal', { kind: 'human' }),
    principal(PRINCIPAL_ID, 'pending-actions-fixture'),
  ];
  let keys: ApiKeyRecord[] = [
    {
      key_id: 'key-existing',
      label: 'workstation',
      issued_at_unix_secs: 1_724_000_000,
      revoked_at_unix_secs: null,
      last_4: '1234',
      last_used_at_unix_secs: 1_724_003_600,
    },
  ];
  const issuedKey: ApiKeyRecord = {
    key_id: 'key-new',
    label: 'e2e-ci',
    issued_at_unix_secs: 1_724_010_000,
    revoked_at_unix_secs: null,
    last_4: '9abc',
    last_used_at_unix_secs: null,
  };
  const responseGate = deferred<void>();
  const requestStarted = deferred<void>();
  const issueBodies: unknown[] = [];
  const unexpectedRequests: string[] = [];
  let issuePostCount = 0;
  let keyListGetCount = 0;

  await page.route('**/admin/**', async (route) => {
    const request = route.request();
    const url = new URL(request.url());
    const { pathname } = url;
    const method = request.method();
    const json = (status: number, body: unknown) =>
      route.fulfill({
        status,
        contentType: 'application/json',
        body: JSON.stringify(body),
      });

    if (method === 'GET' && pathname === '/admin/health') {
      return json(200, HEALTH_RESPONSE);
    }
    if (method === 'GET' && pathname === '/admin/v1/principals') {
      return json(200, { principals });
    }
    if (method === 'GET' && pathname === '/admin/v1/upstreams') {
      return json(200, { upstreams: [] });
    }
    if (method === 'GET' && pathname === '/admin/v1/plugins/registry') {
      return json(200, { entries: [] });
    }
    if (
      method === 'GET' &&
      /^\/admin\/v1\/principals\/[^/]+\/plugin-chain$/.test(pathname)
    ) {
      return json(200, { entries: [] });
    }
    if (
      method === 'GET' &&
      /^\/admin\/v1\/principals\/[^/]+\/router-terminal$/.test(pathname)
    ) {
      return json(200, { strategy: 'first-pick', revision: 7 });
    }
    if (
      method === 'GET' &&
      /^\/admin\/v1\/principals\/[^/]+\/cache-keepalive$/.test(pathname)
    ) {
      return json(200, {
        summary: {
          renewing_now: 0,
          sessions_last_5m: 0,
          renewals_fired: 0,
          cost_saved: 0,
        },
        rows: [],
        next_cursor: null,
      });
    }
    if (method === 'GET' && pathname === '/admin/events/recent') {
      return json(200, { events: [], observed: true, count: 0, limit: 5 });
    }
    const principalKeysMatch = pathname.match(
      /^\/admin\/v1\/principals\/([^/]+)\/keys$/,
    );
    if (method === 'GET' && principalKeysMatch) {
      if (principalKeysMatch[1] === PRINCIPAL_ID) {
        keyListGetCount += 1;
        return json(200, { keys });
      }
      return json(200, { keys: [] });
    }
    if (
      method === 'POST' &&
      pathname === `/admin/v1/principals/${PRINCIPAL_ID}/keys`
    ) {
      issuePostCount += 1;
      issueBodies.push(request.postDataJSON());
      requestStarted.resolve(undefined);
      await responseGate.promise;
      keys = [...keys, issuedKey];
      return json(200, {
        key_id: issuedKey.key_id,
        plaintext_key: ISSUED_PLAINTEXT,
      });
    }

    unexpectedRequests.push(`${method} ${pathname}${url.search}`);
    return json(501, { error: 'unmocked_e2e_request' });
  });

  return {
    issueBodies,
    issuePostCount: () => issuePostCount,
    keyListGetCount: () => keyListGetCount,
    releaseIssue: () => responseGate.resolve(undefined),
    requestStarted: requestStarted.promise,
    unexpectedRequests,
  };
}

async function installCredentialFixtures(page: Page) {
  await storeAdminToken(page);

  const principals = [principal(PRINCIPAL_ID, 'pending-actions-fixture')];
  let credentials: CredentialRecord[] = [
    {
      principal_id: PRINCIPAL_ID,
      provider: 'anthropic',
      kind: 'api_key',
      identity: 'key@example.com',
      associated_principals: [PRINCIPAL_ID],
      has_credentials: true,
      expires_at_unix_secs: null,
      status: 'active',
      cred_id: 'credential-fixture',
    },
  ];
  const responseGate = deferred<void>();
  const requestStarted = deferred<void>();
  const unexpectedRequests: string[] = [];
  let credentialGetCount = 0;
  let revokePostCount = 0;

  await page.route('**/admin/**', async (route) => {
    const request = route.request();
    const url = new URL(request.url());
    const { pathname } = url;
    const method = request.method();
    const json = (status: number, body: unknown) =>
      route.fulfill({
        status,
        contentType: 'application/json',
        body: JSON.stringify(body),
      });

    if (method === 'GET' && pathname === '/admin/health') {
      return json(200, HEALTH_RESPONSE);
    }
    if (method === 'GET' && pathname === '/admin/v1/principals') {
      return json(200, { principals });
    }
    if (method === 'GET' && pathname === '/admin/v1/upstreams') {
      return json(200, { upstreams: [] });
    }
    if (method === 'GET' && pathname === '/admin/credentials') {
      credentialGetCount += 1;
      return json(200, { credentials, observed: true });
    }
    if (method === 'GET' && pathname === '/admin/oauth/status') {
      return json(200, { credentials: [], observed: true });
    }
    if (
      method === 'POST' &&
      pathname ===
        '/admin/credentials/anthropic/credential-fixture/revoke'
    ) {
      revokePostCount += 1;
      requestStarted.resolve(undefined);
      await responseGate.promise;
      credentials = [];
      return json(200, {});
    }

    unexpectedRequests.push(`${method} ${pathname}${url.search}`);
    return json(501, { error: 'unmocked_e2e_request' });
  });

  return {
    credentialGetCount: () => credentialGetCount,
    releaseRevoke: () => responseGate.resolve(undefined),
    requestStarted: requestStarted.promise,
    revokePostCount: () => revokePostCount,
    unexpectedRequests,
  };
}

test.describe('pending admin actions', () => {
  test('API key issuance submits once, locks dismissal, preserves plaintext, and refreshes the key list', async ({
    page,
  }) => {
    const fixtures = await installPrincipalFixtures(page);
    await page.goto('/principals');

    await page
      .locator('aside button', { hasText: 'pending-actions-fixture' })
      .click();
    await expect(
      page.getByRole('heading', { name: 'pending-actions-fixture' }),
    ).toBeVisible();

    await page.getByRole('button', { name: 'Issue Key' }).click();
    let dialog = page.getByRole('dialog', { name: 'Issue API key' });
    await expect(dialog).toBeVisible();
    await dialog.getByRole('textbox', { name: 'Label' }).fill('e2e-ci');

    const issue = dialog.getByRole('button', { name: 'Issue' });
    await issue.evaluate((button) => {
      const element = button as HTMLButtonElement;
      element.click();
      element.click();
    });
    await fixtures.requestStarted;

    const issuing = dialog.getByRole('button', { name: 'Issuing...' });
    await expect(issuing).toBeVisible();
    await expect(issuing).toBeDisabled();
    await expect(issuing).toHaveAttribute('aria-busy', 'true');
    await expect(dialog.getByRole('textbox', { name: 'Label' })).toBeDisabled();

    const cancel = dialog.getByRole('button', { name: 'Cancel' });
    await expect(cancel).toBeDisabled();
    await cancel.click({ force: true });
    await expect(dialog).toBeVisible();
    await issuing.click({ force: true });
    await tryModalDismissals(page, dialog);

    expect(fixtures.issuePostCount()).toBe(1);
    expect(fixtures.issueBodies).toEqual([{ label: 'e2e-ci' }]);

    fixtures.releaseIssue();

    dialog = page.getByRole('dialog', { name: 'API key issued' });
    await expect(dialog).toBeVisible();
    await expect(dialog.getByText(ISSUED_PLAINTEXT, { exact: true })).toBeVisible();
    await expect(
      dialog.getByText(
        'Copy the key now. It will not be shown again, so this dialog stays open until you choose Done.',
      ),
    ).toBeVisible();
    await tryModalDismissals(page, dialog);
    await expect(dialog.getByText(ISSUED_PLAINTEXT, { exact: true })).toBeVisible();

    await expect(
      page.getByTestId('api-keys-table-slot').getByText('key-new', {
        exact: true,
      }),
    ).toBeVisible();
    expect(fixtures.keyListGetCount()).toBeGreaterThan(1);

    await dialog.getByRole('button', { name: 'Done' }).click();
    await expect(dialog).toBeHidden();
    await expect(page.getByText(ISSUED_PLAINTEXT, { exact: true })).toBeHidden();
    expect(fixtures.unexpectedRequests).toEqual([]);
  });

  test('credential revoke locks its modal until the delayed request succeeds', async ({
    page,
  }) => {
    const fixtures = await installCredentialFixtures(page);
    await page.goto('/credentials');

    const apiKeys = page.getByTestId('api-keys-slot');
    await expect(apiKeys.getByText('key@example.com', { exact: true })).toBeVisible();
    const revoke = apiKeys.locator('button', { hasText: 'Revoke' });
    await revoke.click();

    const dialog = page.getByRole('dialog', { name: 'Revoke credential?' });
    await expect(dialog).toBeVisible();
    const confirm = dialog.getByRole('button', { name: 'Confirm revoke' });
    await confirm.evaluate((button) => {
      const element = button as HTMLButtonElement;
      element.click();
      element.click();
    });
    await fixtures.requestStarted;

    const revoking = dialog.getByRole('button', { name: 'Revoking...' });
    await expect(revoking).toBeVisible();
    await expect(revoking).toBeDisabled();
    await expect(revoking).toHaveAttribute('aria-busy', 'true');
    await expect(
      dialog.getByRole('status'),
    ).toHaveText('Revoking anthropic credential — waiting for the server.');

    await expect(dialog.getByRole('button', { name: 'Cancel' })).toBeDisabled();
    await expect(revoke).toBeDisabled();
    await revoking.click({ force: true });
    await dialog.getByRole('button', { name: 'Cancel' }).click({ force: true });
    await tryModalDismissals(page, dialog);
    expect(fixtures.revokePostCount()).toBe(1);

    fixtures.releaseRevoke();

    await expect(dialog).toBeHidden();
    await expect(apiKeys.getByText('No API keys', { exact: true })).toBeVisible();
    expect(fixtures.credentialGetCount()).toBeGreaterThan(1);
    expect(fixtures.unexpectedRequests).toEqual([]);
  });
});
