// Replace key: the pane opens the same connect dialog in `replace` mode
// (UI-R-01, UI-R-02) and its save-time outcomes (UI-D-33, UI-D-34).

import type { ApiKeyAccess, Upstream } from '../../src/lib/api';
import {
  apiKey,
  currentRailStep,
  e2eState,
  expect,
  keyField,
  openUpstream,
  orgNeedsWorkspace,
  orgSelected,
  PRODUCTION_WORKSPACE,
  pasteInto,
  primaryButtons,
  STATUS_401,
  test,
} from './harness';

const PREFLIGHT = '/admin/v1/upstreams/preflight';
const BOUND_ID = 'wrkspc_01BoundWorkspaceZZ9q';
const UPSTREAM_READS = /^\/admin\/v1\/upstreams(\/[^/]+)?$/;

function boundAccess(): ApiKeyAccess {
  return {
    verified: true,
    checked_at_unix_secs: Math.floor(Date.now() / 1000),
    key_class: 'workspace_bound',
    inference: 'ok',
    workspace: { mode: 'bound', id: BOUND_ID },
    workspace_list: 'not_applicable',
    usage_report: 'not_applicable',
    key_type: null,
    last_error: null,
  };
}

function orgAccess(): ApiKeyAccess {
  return {
    verified: true,
    checked_at_unix_secs: Math.floor(Date.now() / 1000),
    key_class: 'org_scoped',
    inference: 'ok',
    workspace: {
      mode: 'selected',
      id: PRODUCTION_WORKSPACE.id,
      name: PRODUCTION_WORKSPACE.name,
    },
    workspace_list: 'ok',
    usage_report: 'ok',
    key_type: 'service_account',
    last_error: null,
  };
}

/** Shows `id` as an Anthropic-host row with `access` (FX-PW over real rows). */
function asAnthropicHost(
  id: string,
  access: () => ApiKeyAccess,
): (body: unknown) => unknown {
  const patchRow = (row: unknown) =>
    row && typeof row === 'object' && 'id' in row && row.id === id
      ? { ...row, base_url: null, api_key_access: access() }
      : row;
  return (body) => {
    if (body && typeof body === 'object' && 'upstreams' in body) {
      const { upstreams } = body;
      return {
        ...body,
        upstreams: Array.isArray(upstreams)
          ? upstreams.map(patchRow)
          : upstreams,
      };
    }
    return patchRow(body);
  };
}

test('UI-R-01: Replace from a rejected stored key (K1)', async ({
  page,
  fake,
  seedApiKeyUpstream,
  toasts,
}) => {
  const oldKey = apiKey('r01old');
  const upstream = await seedApiKeyUpstream({ key: oldKey, name: 'e2e-r01' });
  await fake.setKeyPolicies({
    [oldKey]: { models: STATUS_401, count_tokens: STATUS_401 },
  });
  const puts: Array<{ ifMatch: string | null; body: unknown }> = [];
  page.on('request', (request) => {
    if (
      request.method() === 'PUT' &&
      new URL(request.url()).pathname === `/admin/v1/upstreams/${upstream.id}`
    ) {
      puts.push({
        ifMatch: request.headers()['if-match'] ?? null,
        body: request.postDataJSON(),
      });
    }
  });

  await openUpstream(page, upstream.id);
  await page.getByRole('button', { name: 'Re-check' }).first().click();
  await expect(
    page.getByRole('alert').filter({ hasText: 'rejected this key' }),
  ).toBeVisible();
  await page.getByRole('button', { name: 'Replace key' }).first().click();

  const dialog = page.getByRole('dialog', { name: 'Replace key for e2e-r01' });
  await expect(dialog.getByText('Paste the new key')).toBeVisible();
  // The stored key is rejected, so the dialog doesn't promise it keeps working.
  await expect(
    dialog.getByText('Requests use the new key once you save.'),
  ).toBeVisible();
  await expect(dialog.getByText(/keeps working/)).toHaveCount(0);
  await expect(dialog.locator('ol[aria-label="Steps"] li')).toHaveCount(2);
  await expect(currentRailStep(dialog)).toContainText('Key');
  await expect(
    dialog.getByRole('button', { name: 'Use a custom endpoint' }),
  ).toHaveCount(0);

  const newKey = apiKey('r01new');
  await pasteInto(keyField(dialog), newKey);
  await expect(dialog.getByText('Key verified')).toBeVisible();
  await expect(
    dialog.getByText('Check what it can do, then save the key.'),
  ).toBeVisible();
  await expect(primaryButtons(dialog)).toHaveText(['Save key']);
  await dialog.getByRole('button', { name: 'Save key' }).click();
  await expect(dialog).toBeHidden();

  expect(puts).toHaveLength(1);
  expect(puts[0]?.ifMatch).toBe(`W/"${upstream.spec_revision}"`);
  expect(puts[0]?.body).toMatchObject({ api_key_value: newKey });
  expect(await toasts.texts()).toContainEqual(
    expect.stringContaining('Key replaced on e2e-r01'),
  );
  expect(await toasts.errorCount()).toBe(0);
  await expect(
    page.getByRole('alert').filter({ hasText: 'rejected this key' }),
  ).toHaveCount(0);
  await expect(page.getByText('Key accepted').first()).toBeVisible();
});

test('UI-R-02: Replace from the bound-key usage nudge (FX-PW)', async ({
  page,
  seedApiKeyUpstream,
  patchEnvelope,
  routeEnvelope,
}) => {
  const upstream = await seedApiKeyUpstream({
    key: apiKey('r02old'),
    name: 'e2e-r02',
  });
  let access = boundAccess;
  await patchEnvelope(
    UPSTREAM_READS,
    asAnthropicHost(upstream.id, () => access()),
  );

  await openUpstream(page, upstream.id);
  await page
    .getByRole('button', { name: 'Replace it with an All workspaces key' })
    .click();
  const dialog = page.getByRole('dialog', { name: 'Replace key for e2e-r02' });
  await expect(dialog.getByText('Paste the new key')).toBeVisible();
  await expect(
    dialog.getByText('The current key keeps working until you save.'),
  ).toBeVisible();

  await routeEnvelope(PREFLIGHT, 200, orgNeedsWorkspace('ok'), 'POST');
  await pasteInto(keyField(dialog), apiKey('r02new'));
  await expect(
    dialog.getByRole('heading', { name: 'Pick a workspace' }),
  ).toBeVisible();
  await expect(dialog.locator('ol[aria-label="Steps"] li')).toHaveCount(3);
  // A bound key has no chosen workspace to carry over: pick one explicitly.
  await expect(dialog.getByRole('button', { name: 'Continue' })).toBeDisabled();
  await dialog.getByRole('combobox', { name: 'Workspace' }).click();
  await page.getByRole('option', { name: /Production/ }).click();
  await routeEnvelope(PREFLIGHT, 200, orgSelected('ok'), 'POST');
  await dialog.getByRole('button', { name: 'Continue' }).click();
  await expect(dialog.getByText('Reported by Anthropic')).toBeVisible();

  // The PUT re-probes on api.anthropic.com in production: FX-PW it.
  const saved: Upstream = {
    ...upstream,
    base_url: null,
    spec_revision: upstream.spec_revision + 1,
    api_key_access: orgAccess(),
  };
  await routeEnvelope(`/admin/v1/upstreams/${upstream.id}`, 200, saved, 'PUT');
  access = orgAccess;
  await dialog.getByRole('button', { name: 'Save key' }).click();
  await expect(dialog).toBeHidden();
  await expect(page.getByTestId('api-key-usage-mode')).toContainText(
    'Reported',
  );
});

test('UI-D-33 (replace): a key revoked before Save key stays on Key', async ({
  page,
  fake,
  seedApiKeyUpstream,
  toasts,
}) => {
  const upstream = await seedApiKeyUpstream({
    key: apiKey('d33old'),
    name: 'e2e-d33r',
  });
  await openUpstream(page, upstream.id);
  await page.getByRole('button', { name: 'Replace key' }).first().click();
  const dialog = page.getByRole('dialog', { name: 'Replace key for e2e-d33r' });
  const newKey = apiKey('d33new');
  await pasteInto(keyField(dialog), newKey);
  await expect(dialog.getByText('Key verified')).toBeVisible();
  await fake.setKeyPolicies({
    [newKey]: { models: STATUS_401, count_tokens: STATUS_401 },
  });
  await dialog.getByRole('button', { name: 'Save key' }).click();
  await expect(
    dialog.getByText('Your endpoint rejected this key.'),
  ).toBeVisible();
  await expect(currentRailStep(dialog)).toContainText('Key');
  await expect(dialog.getByRole('alert')).toHaveCount(1);
  expect(await toasts.count()).toBe(0);
});

test('UI-D-34: Replace on a dead custom endpoint', async ({
  page,
  seedApiKeyUpstream,
  toasts,
}) => {
  const upstream = await seedApiKeyUpstream({
    key: apiKey('d34old'),
    name: 'e2e-d34',
    baseUrl: e2eState().deadUrl,
    skipPreflight: true,
  });
  await openUpstream(page, upstream.id);
  await page.getByRole('button', { name: 'Replace key' }).first().click();
  const dialog = page.getByRole('dialog', { name: 'Replace key for e2e-d34' });
  await expect(
    dialog.getByRole('button', { name: 'Use a custom endpoint' }),
  ).toHaveCount(0);
  await pasteInto(keyField(dialog), apiKey('d34new'));
  await expect(dialog.getByRole('alert')).toHaveText(
    "Couldn't reach your endpoint. Try again → check the key.",
  );
  await dialog.getByRole('button', { name: 'Save without checking' }).click();
  await dialog.getByRole('button', { name: 'Save key' }).click();
  await expect(dialog).toBeHidden();
  // Amended (CRT-08 wins): a skipped check makes zero outbound calls, so the
  // pane reads K2 "Key not checked yet", not K3.
  await expect(
    page.getByRole('status').filter({ hasText: 'Key not checked yet' }),
  ).toBeVisible();
  await expect(
    page.getByTestId('api-key-access').getByRole('status'),
  ).toHaveText('Key not verified');
  await expect(page.getByText('Last check failed')).toHaveCount(0);
  expect(await toasts.errorCount()).toBe(0);
});
