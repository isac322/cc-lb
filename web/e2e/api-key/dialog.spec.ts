// Connect dialog, API-key path, against the real server (T2). FX-FAKE: the
// key goes to fake-anthropic as a custom endpoint, scripted per key. FX-PW:
// Anthropic-host-only verdicts are `page.route` envelopes on /preflight.
// FX-DEAD: a closed loopback port.

import {
  apiKey,
  currentRailStep,
  e2eState,
  expect,
  keyField,
  liveUpstreams,
  openApiKeyDialog,
  openUpstreams,
  orgNeedsWorkspace,
  orgSelected,
  PRODUCTION_WORKSPACE,
  pasteInto,
  preflightWire,
  primaryButtons,
  recordPreflights,
  STATUS_401,
  STATUS_403,
  test,
  useCustomEndpoint,
} from './harness';
import type { Locator } from '@playwright/test';

const PREFLIGHT = '/admin/v1/upstreams/preflight';
const BOUND_ID = 'wrkspc_01BoundWorkspaceZZ9q';

test.describe('kind step and rail', () => {
  test('UI-D-01 / UI-CP-05: two kind cards, one primary per step', async ({
    page,
  }) => {
    await openUpstreams(page);
    await page.getByRole('button', { name: 'Add upstream' }).first().click();
    const dialog = page.getByRole('dialog');
    await expect(dialog.getByRole('radio')).toHaveCount(2);
    await expect(
      dialog.locator('label', { hasText: 'Claude subscription' }),
    ).toContainText('Recommended');
    await expect(primaryButtons(dialog)).toHaveText(['Continue']);
    await dialog.getByRole('radio', { name: /Anthropic API key/ }).click();
    await dialog.getByRole('button', { name: 'Continue' }).click();
    await expect(currentRailStep(dialog)).toContainText('Key');
    await expect(dialog.locator('ol[aria-label="Steps"] li')).toHaveCount(3);
    await keyField(dialog).fill(apiKey('cp05'));
    await expect(primaryButtons(dialog)).toHaveText(['Check key']);
  });

  test('UI-D-05: phones show one live step line instead of the rail', async ({
    page,
    routeEnvelope,
  }) => {
    await page.setViewportSize({ width: 390, height: 844 });
    await routeEnvelope(PREFLIGHT, 200, orgNeedsWorkspace('ok'), 'POST');
    const dialog = await openApiKeyDialog(page);
    await expect(dialog.locator('ol[aria-label="Steps"]')).toBeHidden();
    const line = dialog.locator('[aria-live="polite"]');
    await expect(line).toHaveText('Step 2 of 3 · Key');
    await pasteInto(keyField(dialog), apiKey('d05'));
    await expect(line).toHaveText('Step 3 of 4 · Workspace');
  });
});

test.describe('key step', () => {
  test('UI-D-06: a clean paste checks itself; typing waits for Check key', async ({
    page,
    fake,
  }) => {
    const preflights = recordPreflights(page);
    const dialog = await openApiKeyDialog(page);
    await useCustomEndpoint(dialog, fake.url);
    const pasted = apiKey('d06paste');
    await pasteInto(keyField(dialog), pasted);
    await expect(dialog.getByText('Key verified')).toBeVisible();
    expect(preflights).toEqual([{ api_key_value: pasted, base_url: fake.url }]);

    await dialog.getByRole('button', { name: 'Back' }).click();
    const typed = apiKey('d06typed');
    await keyField(dialog).fill(typed);
    expect(preflights).toHaveLength(1);
    await keyField(dialog).press('Enter');
    await expect(dialog.getByText('Key verified')).toBeVisible();
    expect(preflights).toHaveLength(2);

    // Back on the Anthropic host, a gated prefix is refused locally.
    await dialog.getByRole('button', { name: 'Back' }).click();
    await dialog
      .getByRole('button', { name: 'Use the default endpoint' })
      .click();
    await keyField(dialog).fill('');
    await pasteInto(keyField(dialog), 'sk-ant-admin01-gatedNeverSent');
    await expect(dialog.getByRole('alert')).toBeVisible();
    expect(preflights).toHaveLength(2);
  });

  for (const [id, key, fieldError, notice] of [
    [
      'UI-D-07',
      'sk-ant-admin01-adminKeyBody0123456789',
      "This is an Admin key — it manages the org and can't call models.",
      'Use a standard API key → get inference.',
    ],
    [
      'UI-D-08',
      'sk-ant-api01-enterpriseKeyBody012345',
      "This is an Enterprise management key — it can't call models.",
      'Use a Console API key → paste it here.',
    ],
  ] as const) {
    test(`${id}: gated on the Anthropic host with no request`, async ({
      page,
      toasts,
    }) => {
      const preflights = recordPreflights(page);
      const dialog = await openApiKeyDialog(page);
      await pasteInto(keyField(dialog), key);
      await expect(dialog.getByText(fieldError)).toBeVisible();
      await expect(keyField(dialog)).toHaveAttribute('aria-invalid', 'true');
      await expect(keyField(dialog)).toHaveValue(key);
      const alert = dialog.getByRole('alert');
      await expect(alert).toHaveCount(1);
      await expect(alert).toContainText(notice);
      await expect(
        alert.getByRole('button', { name: 'How to create a key' }),
      ).toBeVisible();
      await expect(
        dialog.getByRole('button', { name: 'Check key' }),
      ).toBeDisabled();
      await expect(
        dialog.getByRole('button', { name: "Don't have a key?" }),
      ).toHaveCount(0);
      expect(preflights).toHaveLength(0);
      expect(await toasts.count()).toBe(0);
    });
  }

  test('UI-D-09: an OAuth token swaps the primary to Sign in with Claude', async ({
    page,
  }) => {
    const dialog = await openApiKeyDialog(page);
    await pasteInto(keyField(dialog), 'sk-ant-oat01-oauthTokenBody012345');
    await expect(
      dialog.getByText(
        'That is an OAuth token, not an API key — it expires and cc-lb cannot refresh it.',
      ),
    ).toBeVisible();
    await expect(dialog.getByRole('alert')).toContainText(
      'Sign in with Claude → get a managed session.',
    );
    await expect(primaryButtons(dialog)).toHaveText([
      'Sign in with Claude instead',
    ]);
    await dialog
      .getByRole('button', { name: 'Sign in with Claude instead' })
      .click();
    await expect(currentRailStep(dialog)).toContainText('Sign in');
    await expect(
      dialog.getByRole('link', { name: 'Sign in with Claude' }),
    ).toBeVisible();
  });

  test('UI-D-28: a custom endpoint still gets the server gate, same copy', async ({
    page,
    fake,
  }) => {
    const preflights = recordPreflights(page);
    const dialog = await openApiKeyDialog(page);
    await useCustomEndpoint(dialog, fake.url);
    await pasteInto(keyField(dialog), 'sk-ant-admin01-adminKeyCustomHost01');
    await expect(
      dialog.getByText(
        "This is an Admin key — it manages the org and can't call models.",
      ),
    ).toBeVisible();
    await expect(dialog.getByRole('alert')).toContainText(
      'Use a standard API key → get inference.',
    );
    expect(preflights).toHaveLength(1);
    // The server gate refuses before any probe (zero outbound, GATE-07).
    expect(await fake.requests('/v1/')).toHaveLength(0);

    await keyField(dialog).fill('');
    await pasteInto(keyField(dialog), 'sk-ant-oat01-oauthTokenCustomHost01');
    await expect(primaryButtons(dialog)).toHaveText([
      'Sign in with Claude instead',
    ]);
  });

  test('UI-D-12: a 401 on the Anthropic host', async ({
    page,
    routeEnvelope,
  }) => {
    await routeEnvelope(
      PREFLIGHT,
      200,
      preflightWire({
        verdict: 'rejected',
        inference: 'denied',
        remediation: ['key_invalid'],
      }),
      'POST',
    );
    const dialog = await openApiKeyDialog(page);
    await pasteInto(keyField(dialog), apiKey('d12'));
    await expect(
      dialog.getByText(
        "Anthropic rejected this key — check for a typo, or it's expired or deleted.",
      ),
    ).toBeVisible();
    const alert = dialog.getByRole('alert');
    await expect(alert).toContainText('Paste it again or create a fresh key.');
    await expect(
      alert.getByRole('button', { name: 'How to create a key' }),
    ).toBeVisible();
    await keyField(dialog).fill(apiKey('d12edit'));
    await expect(primaryButtons(dialog)).toHaveText(['Check key']);
  });

  test('UI-D-13: a 401 from a custom endpoint names the endpoint', async ({
    page,
    fake,
  }) => {
    const key = apiKey('d13');
    await fake.setKeyPolicies({ [key]: { models: STATUS_401 } });
    const dialog = await openApiKeyDialog(page);
    await useCustomEndpoint(dialog, fake.url);
    await pasteInto(keyField(dialog), key);
    await expect(
      dialog.getByText('Your endpoint rejected this key.'),
    ).toBeVisible();
    await expect(dialog.getByRole('alert')).toHaveText(
      'Check the key and Base URL → Check key.',
    );
    await expect(
      dialog.getByRole('button', { name: 'How to create a key' }),
    ).toHaveCount(0);
    await expect(
      dialog.getByRole('button', { name: "Don't have a key?" }),
    ).toHaveCount(0);
  });

  test('UI-D-14: count_tokens 403 → Save anyway → Key not verified', async ({
    page,
    fake,
    toasts,
  }) => {
    const key = apiKey('d14');
    await fake.setKeyPolicies({ [key]: { count_tokens: STATUS_403 } });
    const dialog = await openApiKeyDialog(page);
    await useCustomEndpoint(dialog, fake.url);
    await pasteInto(keyField(dialog), key);
    await expect(dialog.getByRole('alert')).toHaveText(
      "Valid key, but Anthropic won't let it count tokens. Save anyway → add it unverified.",
    );
    await expect(primaryButtons(dialog)).toHaveText(['Check key']);
    await dialog.getByRole('button', { name: 'Save anyway' }).click();
    await expect(
      dialog.getByRole('heading', { name: 'Key not verified' }),
    ).toBeVisible();
    await expect(
      dialog.getByText(
        "Anthropic accepted the key but wouldn't let it count tokens. The upstream shows Key not verified until a re-check passes.",
      ),
    ).toBeVisible();
    await dialog.getByRole('button', { name: 'Add upstream' }).click();
    await expect(dialog).toBeHidden();
    await expect(page.getByText('Key not verified').first()).toBeVisible();
    expect(await toasts.errorCount()).toBe(0);
  });

  test('UI-D-15: an unreachable endpoint → Try again → Save without checking', async ({
    page,
    toasts,
  }) => {
    const dead = e2eState().deadUrl;
    const preflights = recordPreflights(page);
    const dialog = await openApiKeyDialog(page);
    await useCustomEndpoint(dialog, dead);
    await pasteInto(keyField(dialog), apiKey('d15'));
    const alert = dialog.getByRole('alert');
    await expect(alert).toHaveText(
      "Couldn't reach your endpoint. Try again → check the key.",
    );
    await expect(primaryButtons(dialog)).toHaveText(['Try again']);
    await dialog.getByRole('button', { name: 'Try again' }).click();
    await expect.poll(() => preflights.length).toBe(2);
    await expect(alert).toBeVisible();
    await dialog.getByRole('button', { name: 'Save without checking' }).click();
    await expect(
      dialog.getByText(
        'The upstream shows Key not verified until a Re-check passes.',
      ),
    ).toBeVisible();
    await expect(dialog.getByText(/checks it again later/)).toHaveCount(0);
    await dialog.getByRole('button', { name: 'Add upstream' }).click();
    await expect(dialog).toBeHidden();
    // Amended (CRT-08 wins): a skipped check makes zero outbound calls, so
    // the pane reads K2 "Key not checked yet", not K3.
    await expect(
      page.getByRole('status').filter({ hasText: 'Key not checked yet' }),
    ).toBeVisible();
    await expect(
      page.getByTestId('api-key-access').getByRole('status'),
    ).toHaveText('Key not verified');
    await expect(page.getByText('Last check failed')).toHaveCount(0);
    expect(await toasts.errorCount()).toBe(0);
  });

  test('UI-D-16: cc-lb request failure is one danger alert, no toast', async ({
    page,
    toasts,
  }) => {
    await page.route(
      (url) => url.pathname === PREFLIGHT,
      (route) => route.abort('connectionrefused'),
    );
    const dialog = await openApiKeyDialog(page);
    await pasteInto(keyField(dialog), apiKey('d16'));
    const alert = dialog.getByRole('alert');
    await expect(alert).toHaveCount(1);
    await expect(alert).toContainText("cc-lb couldn't check the key:");
    await expect(
      alert.getByRole('button', { name: 'Try again' }),
    ).toBeVisible();
    expect(await toasts.count()).toBe(0);
  });

  test('UI-D-27 / UI-AY-04: custom endpoint disclosure and focus', async ({
    page,
  }) => {
    const dialog = await openApiKeyDialog(page);
    await expect(dialog.getByRole('textbox', { name: /Base URL/ })).toHaveCount(
      0,
    );
    const opener = dialog.getByRole('button', {
      name: 'Use a custom endpoint',
    });
    await expect(opener).toHaveAttribute('aria-expanded', 'false');
    const controls = await opener.getAttribute('aria-controls');
    await opener.click();
    const field = dialog.getByRole('textbox', { name: /Base URL/ });
    await expect(field).toHaveValue('');
    await expect(field).toBeFocused();
    await expect(field).toHaveAttribute('id', controls ?? '');
    const fold = dialog.getByRole('button', {
      name: 'Use the default endpoint',
    });
    await expect(fold).toHaveAttribute('aria-expanded', 'true');
    await fold.click();
    await expect(
      dialog.getByRole('button', { name: 'Use a custom endpoint' }),
    ).toBeFocused();
  });

  test('UI-D-31: a hidden character inside the key never leaves the browser', async ({
    page,
  }) => {
    const preflights = recordPreflights(page);
    const dialog = await openApiKeyDialog(page);
    for (const hidden of ['\u200B', '\u00A0']) {
      await keyField(dialog).fill('');
      await pasteInto(keyField(dialog), `sk-ant-api03-ab${hidden}cd0123456789`);
      await expect(
        dialog.getByText(
          'Keys contain only letters, digits, - and _. Paste it again.',
        ),
      ).toBeVisible();
      await expect(dialog.getByRole('alert')).toHaveCount(0);
    }
    expect(preflights).toHaveLength(0);
  });

  test('UI-D-32: server invalid_input on the key is the Key field error', async ({
    page,
    routeEnvelope,
    toasts,
  }) => {
    await routeEnvelope(
      PREFLIGHT,
      400,
      {
        error: 'invalid_input',
        field: 'api_key_value',
        reason: 'API keys contain no spaces or line breaks',
      },
      'POST',
    );
    const dialog = await openApiKeyDialog(page);
    await pasteInto(keyField(dialog), apiKey('d32'));
    await expect(
      dialog.getByText('API keys contain no spaces or line breaks'),
    ).toBeVisible();
    await expect(keyField(dialog)).toHaveAttribute('aria-invalid', 'true');
    await expect(dialog.getByRole('alert')).toHaveCount(0);
    await expect(dialog.getByRole('button', { name: 'Try again' })).toHaveCount(
      0,
    );
    expect(await toasts.count()).toBe(0);
  });
});

test.describe('workspace step and Finish', () => {
  test('UI-D-17: a bound key on a custom endpoint', async ({ page, fake }) => {
    const key = apiKey('d17');
    await fake.setKeyPolicies({ [key]: { bound_workspace: BOUND_ID } });
    const dialog = await openApiKeyDialog(page);
    await useCustomEndpoint(dialog, fake.url);
    await pasteInto(keyField(dialog), key);
    await expect(dialog.getByText('Key verified')).toBeVisible();
    await expect(dialog.locator('ol[aria-label="Steps"] li')).toHaveCount(3);
    await expect(dialog.getByText('Key accepted')).toBeVisible();
    await expect(dialog.getByText('Fixed by the key')).toBeVisible();
    await expect(dialog.getByText('wrkspc_…ZZ9q')).toBeVisible();
    await expect(dialog.getByText('Estimates only')).toBeVisible();
    await expect(
      dialog.getByText(
        'Org checks (workspaces, usage reporting) need api.anthropic.com — usage stays estimated.',
      ),
    ).toBeVisible();
  });

  test('UI-D-18: an org key picks a workspace from the list', async ({
    page,
    routeEnvelope,
  }) => {
    const preflights = recordPreflights(page);
    await routeEnvelope(PREFLIGHT, 200, orgNeedsWorkspace('ok'), 'POST');
    const dialog = await openApiKeyDialog(page);
    const key = apiKey('d18');
    await pasteInto(keyField(dialog), key);
    await expect(
      dialog.getByRole('heading', { name: 'Pick a workspace' }),
    ).toBeVisible();
    await expect(
      dialog.getByText(
        'This key can act in several workspaces — choose the one it bills.',
      ),
    ).toBeVisible();
    await expect(
      dialog.getByText('You can change this later in Access.'),
    ).toBeVisible();
    // Nothing is preselected: Continue waits for an explicit choice.
    const continueButton = dialog.getByRole('button', { name: 'Continue' });
    await expect(continueButton).toBeDisabled();
    await dialog.getByRole('combobox', { name: 'Workspace' }).click();
    await page.getByRole('option', { name: /Production/ }).click();
    await routeEnvelope(PREFLIGHT, 200, orgSelected('ok'), 'POST');
    await continueButton.click();
    await expect(dialog.getByText('Key verified')).toBeVisible();
    expect(preflights.at(-1)).toEqual({
      api_key_value: key,
      workspace_id: PRODUCTION_WORKSPACE.id,
    });
    await expect(dialog.getByText('Production')).toBeVisible();
    await expect(dialog.getByText('Change it later in Access')).toBeVisible();
    await expect(dialog.getByText('Reported by Anthropic')).toBeVisible();
    await expect(dialog.getByText('Service account')).toBeVisible();
  });

  test('UI-D-19: list denied → manual id with Console help', async ({
    page,
    routeEnvelope,
  }) => {
    await routeEnvelope(PREFLIGHT, 200, orgNeedsWorkspace('denied'), 'POST');
    const dialog = await openApiKeyDialog(page);
    await pasteInto(keyField(dialog), apiKey('d19'));
    await expect(
      dialog.getByText(
        'This key can act in several workspaces — paste the one it bills.',
      ),
    ).toBeVisible();
    await expect(dialog.getByPlaceholder('wrkspc_…')).toBeFocused();
    await expect(
      dialog.getByText("This key's account lacks the role to list workspaces."),
    ).toBeVisible();
    const link = dialog.getByRole('link', {
      name: /Open Workspaces in Console/,
    });
    await expect(link).toHaveAttribute(
      'href',
      'https://platform.claude.com/settings/workspaces',
    );
    await expect(link).toHaveAttribute('target', '_blank');
    await expect(link).toHaveAttribute('rel', /noopener/);
    await expect(
      dialog.getByRole('button', { name: 'Re-check' }),
    ).toBeVisible();
  });

  test('UI-D-20: a custom endpoint cannot list workspaces', async ({
    page,
    fake,
  }) => {
    const key = apiKey('d20');
    const workspace = 'wrkspc_01CustomHostWorkspace77';
    await fake.setKeyPolicies({
      [key]: {
        requires_workspace: true,
        workspaces: [{ id: workspace, name: 'Custom' }],
      },
    });
    const dialog = await openApiKeyDialog(page);
    await useCustomEndpoint(dialog, fake.url);
    await pasteInto(keyField(dialog), key);
    await expect(
      dialog.getByText(
        'Workspace lists need api.anthropic.com — paste the ID.',
      ),
    ).toBeVisible();
    await dialog.getByPlaceholder('wrkspc_…').fill(workspace);
    await dialog.getByRole('button', { name: 'Continue' }).click();
    await expect(dialog.getByText('Key verified')).toBeVisible();
    await expect(
      dialog.getByText(
        'Org checks (workspaces, usage reporting) need api.anthropic.com — usage stays estimated.',
      ),
    ).toBeVisible();
  });

  test('UI-D-21: list error → Re-check asks again', async ({
    page,
    routeEnvelope,
  }) => {
    const preflights = recordPreflights(page);
    await routeEnvelope(PREFLIGHT, 200, orgNeedsWorkspace('error'), 'POST');
    const dialog = await openApiKeyDialog(page);
    await pasteInto(keyField(dialog), apiKey('d21'));
    await expect(
      dialog.getByText(
        "Anthropic didn't return the list — Re-check, or paste the ID.",
      ),
    ).toBeVisible();
    expect(preflights).toHaveLength(1);
    await dialog.getByRole('button', { name: 'Re-check' }).click();
    await expect.poll(() => preflights.length).toBe(2);
  });

  test('UI-D-22: a malformed id never leaves the browser', async ({
    page,
    routeEnvelope,
  }) => {
    const preflights = recordPreflights(page);
    await routeEnvelope(PREFLIGHT, 200, orgNeedsWorkspace('denied'), 'POST');
    const dialog = await openApiKeyDialog(page);
    await pasteInto(keyField(dialog), apiKey('d22'));
    const input = dialog.getByPlaceholder('wrkspc_…');
    await input.fill('ws_123');
    await dialog.getByRole('button', { name: 'Continue' }).click();
    await expect(
      dialog.getByText('Workspace IDs start with wrkspc_.'),
    ).toBeVisible();
    await expect(input).toHaveValue('ws_123');
    expect(preflights).toHaveLength(1);
  });

  test('UI-D-23: an unknown workspace is one warning alert; the value stays', async ({
    page,
    fake,
  }) => {
    const key = apiKey('d23');
    await fake.setKeyPolicies({
      [key]: {
        requires_workspace: true,
        workspaces: [{ id: 'wrkspc_01KnownWorkspace0000', name: 'Known' }],
      },
    });
    const dialog = await openApiKeyDialog(page);
    await useCustomEndpoint(dialog, fake.url);
    await pasteInto(keyField(dialog), key);
    const input = dialog.getByPlaceholder('wrkspc_…');
    const unknown = 'wrkspc_01UnknownWorkspace404';
    await input.fill(unknown);
    await dialog.getByRole('button', { name: 'Continue' }).click();
    const alert = dialog.getByRole('alert');
    await expect(alert).toHaveCount(1);
    await expect(alert).toHaveText(
      `This key can't see workspace ${unknown} — check the id, or the key's account lacks access.`,
    );
    await expect(input).toHaveValue(unknown);
    await expect(
      dialog.getByRole('button', { name: 'Continue' }),
    ).toBeEnabled();
  });

  test('UI-D-24 / UI-D-25: usage fact and its one fix on Finish', async ({
    page,
    routeEnvelope,
  }) => {
    await routeEnvelope(PREFLIGHT, 200, orgNeedsWorkspace('ok'), 'POST');
    const dialog = await openApiKeyDialog(page);
    await pasteInto(keyField(dialog), apiKey('d24'));
    // The paste check must be answered before the next envelope replaces it.
    await expect(
      dialog.getByRole('heading', { name: 'Pick a workspace' }),
    ).toBeVisible();
    await dialog.getByRole('combobox', { name: 'Workspace' }).click();
    await page.getByRole('option', { name: /Production/ }).click();
    await routeEnvelope(PREFLIGHT, 200, orgSelected('denied'), 'POST');
    await dialog.getByRole('button', { name: 'Continue' }).click();
    await expect(dialog.getByText('Estimates only')).toBeVisible();
    await expect(
      dialog.getByText(
        "Raise the linked account's org role → get reported usage.",
      ),
    ).toBeVisible();

    await dialog.getByRole('button', { name: 'Back' }).click();
    await routeEnvelope(PREFLIGHT, 200, orgSelected('error'), 'POST');
    await dialog.getByRole('button', { name: 'Continue' }).click();
    await expect(dialog.getByText('Estimates for now')).toBeVisible();
    await expect(
      dialog.getByText(
        "Anthropic didn't answer the usage check. Re-check in Access → get reported usage.",
      ),
    ).toBeVisible();
  });

  test('UI-D-26: Name and add persists the checked access', async ({
    page,
    fake,
    adminApi,
    toasts,
  }) => {
    const dialog = await openApiKeyDialog(page);
    await useCustomEndpoint(dialog, fake.url);
    await pasteInto(keyField(dialog), apiKey('d26'));
    const name = dialog.getByRole('textbox', { name: /Upstream name/ });
    await expect(name).toHaveValue('Anthropic API key');
    await name.fill('e2e-d26-key');
    await dialog.getByRole('button', { name: 'Add upstream' }).click();
    await expect(dialog).toBeHidden();
    await expect(page).toHaveURL(/selectedId=/);
    await expect
      .poll(() => toasts.texts())
      .toContainEqual(expect.stringContaining('e2e-d26-key added'));
    expect(await toasts.errorCount()).toBe(0);
    await expect(page.getByText('Key accepted').first()).toBeVisible();
    const row = (await liveUpstreams(adminApi)).find(
      (u) => u.name === 'e2e-d26-key',
    );
    // The server stores a parsed URL, so a bare origin gains its root path.
    expect(row?.base_url).toBe(new URL(fake.url).href);
    expect(row?.api_key_access?.verified).toBe(true);
    expect(row?.api_key_access?.inference).toBe('ok');
  });

  test('UI-D-29: the Guide step', async ({ page }) => {
    const dialog = await openApiKeyDialog(page);
    await dialog.getByRole('button', { name: "Don't have a key?" }).click();
    await expect(currentRailStep(dialog)).toContainText('Create the key');
    await expect(dialog.getByRole('radio')).toHaveCount(2);
    await expect(
      dialog.locator('label', { hasText: 'Full access' }),
    ).toContainText('Recommended');
    const console = dialog.getByRole('link', {
      name: /Open the API keys page/,
    });
    await expect(console).toHaveAttribute(
      'href',
      'https://platform.claude.com/settings/keys',
    );
    await expect(console).toHaveAttribute('target', '_blank');
    await expect(console).toHaveAttribute('rel', /noopener/);
    await expect(primaryButtons(dialog)).toHaveText([
      'I created it — continue',
    ]);
    await expect(
      dialog.getByRole('button', { name: 'Paste a key instead' }),
    ).toBeVisible();
    // Entering from a rejection and continuing clears the rejected key.
    await dialog.getByRole('button', { name: 'Paste a key instead' }).click();
    await pasteInto(keyField(dialog), 'sk-ant-admin01-rejectedThenGuide0');
    await dialog.getByRole('button', { name: 'How to create a key' }).click();
    await dialog
      .getByRole('button', { name: 'I created it — continue' })
      .click();
    await expect(keyField(dialog)).toHaveValue('');
  });

  test('UI-D-30: abandoning at each step creates nothing; focus returns', async ({
    page,
    fake,
    adminApi,
  }) => {
    await openUpstreams(page);
    const trigger = page.getByRole('button', { name: 'Add upstream' }).first();
    const steps: Array<(dialog: Locator) => Promise<void>> = [
      async () => {},
      async (dialog) => {
        await dialog.getByRole('radio', { name: /Anthropic API key/ }).click();
        await dialog.getByRole('button', { name: 'Continue' }).click();
      },
      async (dialog) => {
        await dialog.getByRole('radio', { name: /Anthropic API key/ }).click();
        await dialog.getByRole('button', { name: 'Continue' }).click();
        await useCustomEndpoint(dialog, fake.url);
        await pasteInto(keyField(dialog), apiKey('d30'));
        await expect(dialog.getByText('Key verified')).toBeVisible();
      },
    ];
    for (const [index, reach] of steps.entries()) {
      await trigger.click();
      const dialog = page.getByRole('dialog');
      await reach(dialog);
      if (index % 2 === 0) {
        await dialog.getByRole('button', { name: 'Cancel' }).click();
      } else {
        await page.keyboard.press('Escape');
      }
      await expect(dialog).toBeHidden();
      await expect(trigger).toBeFocused();
    }
    expect(await liveUpstreams(adminApi)).toHaveLength(0);
  });

  test('UI-D-33: a key revoked between Check and Add', async ({
    page,
    fake,
    adminApi,
    toasts,
  }) => {
    const key = apiKey('d33');
    const dialog = await openApiKeyDialog(page);
    await useCustomEndpoint(dialog, fake.url);
    await pasteInto(keyField(dialog), key);
    await expect(dialog.getByText('Key verified')).toBeVisible();
    await fake.setKeyPolicies({
      [key]: { models: STATUS_401, count_tokens: STATUS_401 },
    });
    await dialog.getByRole('button', { name: 'Add upstream' }).click();
    await expect(
      dialog.getByText('Your endpoint rejected this key.'),
    ).toBeVisible();
    await expect(currentRailStep(dialog)).toContainText('Key');
    await expect(dialog.getByRole('alert')).toHaveCount(1);
    expect(await toasts.count()).toBe(0);
    expect(await liveUpstreams(adminApi)).toHaveLength(0);
  });
});

test.describe('accessibility, toasts, layout', () => {
  test('UI-AY-01: focus lands on each step’s input and returns to the trigger', async ({
    page,
    routeEnvelope,
  }) => {
    await openUpstreams(page);
    const trigger = page.getByRole('button', { name: 'Add upstream' }).first();
    await trigger.click();
    const dialog = page.getByRole('dialog');
    const radio = dialog.getByRole('radio', { name: /Claude subscription/ });
    await radio.focus();
    await page.keyboard.press('ArrowDown');
    await expect(
      dialog.getByRole('radio', { name: /Anthropic API key/ }),
    ).toBeChecked();
    await dialog.getByRole('button', { name: 'Continue' }).click();
    await expect(keyField(dialog)).toBeFocused();
    await routeEnvelope(PREFLIGHT, 200, orgNeedsWorkspace('denied'), 'POST');
    await pasteInto(keyField(dialog), apiKey('ay01'));
    await expect(dialog.getByPlaceholder('wrkspc_…')).toBeFocused();
    await routeEnvelope(PREFLIGHT, 200, orgSelected('ok'), 'POST');
    await dialog.getByPlaceholder('wrkspc_…').fill(PRODUCTION_WORKSPACE.id);
    await dialog.getByRole('button', { name: 'Continue' }).click();
    await expect(
      dialog.getByRole('textbox', { name: /Upstream name/ }),
    ).toBeFocused();
    await page.keyboard.press('Escape');
    await expect(dialog).toBeHidden();
    await expect(trigger).toBeFocused();
  });

  test('UI-AY-02: keyboard-only add', async ({ page, fake, adminApi }) => {
    await openUpstreams(page);
    const trigger = page.getByRole('button', { name: 'Add upstream' }).first();
    await trigger.focus();
    await page.keyboard.press('Enter');
    const dialog = page.getByRole('dialog');
    await dialog.getByRole('radio', { name: /Claude subscription/ }).focus();
    await page.keyboard.press('ArrowDown');
    await dialog.getByRole('button', { name: 'Continue' }).focus();
    await page.keyboard.press('Enter');
    await expect(keyField(dialog)).toBeFocused();
    // Reach the disclosure by keyboard: it is the last row of the step.
    await dialog.getByRole('button', { name: 'Use a custom endpoint' }).focus();
    await page.keyboard.press('Enter');
    await page.keyboard.type(fake.url);
    // Base URL → "Don't have a key?" → the key field.
    await page.keyboard.press('Shift+Tab');
    await page.keyboard.press('Shift+Tab');
    await expect(keyField(dialog)).toBeFocused();
    await pasteInto(keyField(dialog), apiKey('ay02'));
    const name = dialog.getByRole('textbox', { name: /Upstream name/ });
    await expect(name).toBeFocused();
    await page.keyboard.press('Enter');
    await expect(dialog).toBeHidden();
    expect(await liveUpstreams(adminApi)).toHaveLength(1);
  });

  test('UI-TS-01: happy paths raise zero error toasts', async ({
    page,
    fake,
    toasts,
  }) => {
    const dialog = await openApiKeyDialog(page);
    await useCustomEndpoint(dialog, fake.url);
    await pasteInto(keyField(dialog), apiKey('ts01'));
    await dialog
      .getByRole('textbox', { name: /Upstream name/ })
      .fill('e2e-ts01');
    await dialog.getByRole('button', { name: 'Add upstream' }).click();
    await expect(dialog).toBeHidden();
    await page.getByRole('button', { name: 'Replace key' }).first().click();
    const replace = page.getByRole('dialog');
    await pasteInto(keyField(replace), apiKey('ts01b'));
    await replace.getByRole('button', { name: 'Save key' }).click();
    await expect(replace).toBeHidden();
    await page.getByRole('button', { name: 'Re-check' }).first().click();
    await expect(page.getByText('Key accepted').first()).toBeVisible();
    for (const range of ['7d', '30d', '24h']) {
      const option = page.getByRole('radio', { name: range });
      if ((await option.count()) > 0) await option.first().click();
    }
    const texts = await toasts.texts();
    expect(texts).toContainEqual(expect.stringContaining('e2e-ts01 added'));
    expect(texts).toContainEqual(
      expect.stringContaining('Key replaced on e2e-ts01'),
    );
    expect(await toasts.errorCount()).toBe(0);
  });

  test('UI-TS-02: expected refusals are inline only', async ({
    page,
    fake,
    routeEnvelope,
    toasts,
  }) => {
    const dialog = await openApiKeyDialog(page);
    for (const key of [
      'sk-ant-admin01-refusalAdmin0000000',
      'sk-ant-api01-refusalEnterprise0000',
      'sk-ant-oat01-refusalOauth00000000',
    ]) {
      await keyField(dialog).fill('');
      await pasteInto(keyField(dialog), key);
      await expect(dialog.getByRole('alert')).toBeVisible();
    }
    await keyField(dialog).fill('');
    await routeEnvelope(
      PREFLIGHT,
      200,
      preflightWire({
        verdict: 'rejected',
        inference: 'denied',
        remediation: ['key_invalid'],
      }),
      'POST',
    );
    await pasteInto(keyField(dialog), apiKey('ts02a'));
    await expect(dialog.getByRole('alert')).toBeVisible();
    await routeEnvelope(PREFLIGHT, 200, orgNeedsWorkspace('denied'), 'POST');
    await keyField(dialog).fill('');
    await pasteInto(keyField(dialog), apiKey('ts02b'));
    // The paste check must be answered before the next envelope replaces it.
    await expect(dialog.getByPlaceholder('wrkspc_…')).toBeVisible();
    await routeEnvelope(
      PREFLIGHT,
      200,
      preflightWire({
        verdict: 'needs_workspace',
        keyClass: 'org_scoped',
        workspaceList: 'denied',
        remediation: ['workspace_not_found'],
      }),
      'POST',
    );
    await dialog
      .getByPlaceholder('wrkspc_…')
      .fill('wrkspc_01Missing0000000000');
    await dialog.getByRole('button', { name: 'Continue' }).click();
    await expect(dialog.getByRole('alert')).toBeVisible();
    await dialog.getByRole('button', { name: 'Cancel' }).click();

    // Unreachable (FX-DEAD) and a save-time rejection (UI-D-33).
    await page.unrouteAll({ behavior: 'wait' });
    const second = await openApiKeyDialog(page);
    await useCustomEndpoint(second, e2eState().deadUrl);
    await pasteInto(keyField(second), apiKey('ts02c'));
    await expect(second.getByRole('alert')).toBeVisible();
    await second.getByRole('textbox', { name: /Base URL/ }).fill(fake.url);
    const revoked = apiKey('ts02d');
    await keyField(second).fill('');
    await pasteInto(keyField(second), revoked);
    await expect(second.getByText('Key verified')).toBeVisible();
    await fake.setKeyPolicies({
      [revoked]: { models: STATUS_401, count_tokens: STATUS_401 },
    });
    await second.getByRole('button', { name: 'Add upstream' }).click();
    await expect(second.getByRole('alert')).toBeVisible();
    expect(await toasts.count()).toBe(0);
  });

  test('UI-MB-01: no horizontal overflow at 390 on any step', async ({
    page,
    routeEnvelope,
  }) => {
    await page.setViewportSize({ width: 390, height: 844 });
    const fits = async () =>
      expect(
        await page.evaluate(() => document.documentElement.scrollWidth),
      ).toBeLessThanOrEqual(390);
    await openUpstreams(page);
    await page.getByRole('button', { name: 'Add upstream' }).first().click();
    const dialog = page.getByRole('dialog');
    await fits();
    await dialog.getByRole('radio', { name: /Anthropic API key/ }).click();
    await dialog.getByRole('button', { name: 'Continue' }).click();
    await fits();
    await dialog.getByRole('button', { name: "Don't have a key?" }).click();
    await fits();
    await dialog.getByRole('button', { name: 'Paste a key instead' }).click();
    await useCustomEndpoint(
      dialog,
      'https://a-very-long-proxy-hostname.internal.example.com/some/deep/path',
    );
    await fits();
    await dialog
      .getByRole('button', { name: 'Use the default endpoint' })
      .click();
    await routeEnvelope(PREFLIGHT, 200, orgNeedsWorkspace('denied'), 'POST');
    await pasteInto(keyField(dialog), apiKey('mb01'));
    await expect(dialog.getByPlaceholder('wrkspc_…')).toBeVisible();
    await fits();
    await routeEnvelope(PREFLIGHT, 200, orgSelected('denied'), 'POST');
    await dialog.getByPlaceholder('wrkspc_…').fill(PRODUCTION_WORKSPACE.id);
    await dialog.getByRole('button', { name: 'Continue' }).click();
    await expect(dialog.getByText('Key verified')).toBeVisible();
    await fits();
  });
});
