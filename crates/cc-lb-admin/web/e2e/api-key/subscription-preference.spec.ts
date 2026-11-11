import { randomUUID } from 'node:crypto';
import {
  expect,
  test,
  type APIRequestContext,
  type Page,
} from '@playwright/test';
import type { Upstream } from '../../src/lib/api';
import type {
  PluginChainEntry,
  PluginEntry,
  Principal,
} from '../../src/lib/queries';
import { e2eState } from './harness';

const PROVIDERS = ['anthropic', 'openai', 'gemini'] as const;
type Provider = (typeof PROVIDERS)[number];
const WIDTHS = [390, 768, 1440] as const;
const SWITCH_NAME = 'Prefer subscription upstreams';
const PRINCIPAL_WIDE_COPY =
  "Provider scope only changes which controls are shown. Changing this setting affects this principal's router chain across all providers.";

function adminUrl(path: string): string {
  return `${e2eState().adminUrl}/admin/v1${path}`;
}

function authHeaders(): Record<string, string> {
  return { authorization: `Bearer ${e2eState().adminToken}` };
}

async function readJson<T>(
  request: APIRequestContext,
  path: string,
): Promise<T> {
  const response = await request.get(adminUrl(path), {
    headers: authHeaders(),
  });
  expect(response.status(), `GET ${path}`).toBe(200);
  return (await response.json()) as T;
}

async function createJson<T>(
  request: APIRequestContext,
  path: string,
  data: unknown,
): Promise<T> {
  const response = await request.post(adminUrl(path), {
    headers: authHeaders(),
    data,
  });
  expect(response.status(), `POST ${path}`).toBe(201);
  return (await response.json()) as T;
}

async function removeOwned(
  request: APIRequestContext,
  path: string,
  revision: number,
): Promise<void> {
  const response = await request.delete(adminUrl(path), {
    headers: { ...authHeaders(), 'if-match': `W/"${revision}"` },
  });
  expect(response.status(), `DELETE ${path}`).toBe(204);
}

interface Scenario {
  principal: Principal;
  pluginId: string;
  chain: () => Promise<PluginChainEntry[]>;
}

// Unlike the API-key harness fixtures, these tests never clear shared inventory
// or require fake-anthropic. Cleanup is restricted to the records created here.
async function withScenario(
  request: APIRequestContext,
  provider: Provider,
  run: (scenario: Scenario) => Promise<void>,
): Promise<void> {
  const tag = `subscription-preference-${provider}-${randomUUID()}`;
  const upstream = await createJson<Upstream>(request, '/upstreams', {
    name: tag,
    kind: `${provider}_oauth`,
    warmup_enabled: false,
    base_url: 'http://127.0.0.1:9',
    ...(provider === 'anthropic'
      ? {}
      : {
          oauth_token_bundle: {
            access_token: 'test-access-token',
            refresh_token: 'test-refresh-token',
            expires_at_unix_secs: 4000000000,
            scopes: [],
          },
        }),
    ...(provider === 'gemini' ? { project_id: 'test-project' } : {}),
  });
  let createdPrincipalId: string | undefined;
  try {
    const created = await createJson<{ id: string }>(request, '/principals', {
      name: tag,
      kind: 'machine',
      allowed_upstreams: [upstream.id],
    });
    createdPrincipalId = created.id;
    const principal = await readJson<Principal>(
      request,
      `/principals/${createdPrincipalId}`,
    );
    expect(principal.allowed_upstreams).toEqual([upstream.id]);
    const registry = await readJson<{ entries: PluginEntry[] }>(
      request,
      '/plugins/registry',
    );
    const plugin = registry.entries.find(
      (entry) => entry.name === 'subscription-preference',
    );
    expect(plugin, 'built-in subscription-preference plugin').toBeDefined();
    const principalId = principal.id;
    const chain = async () =>
      (
        await readJson<{ entries: PluginChainEntry[] }>(
          request,
          `/principals/${principalId}/plugin-chain?slot=router`,
        )
      ).entries;
    // New principals inherit a default router chain. Start the interaction
    // scenario off without touching any other principal's configuration.
    for (const entry of await chain()) {
      const current = await readJson<PluginChainEntry>(
        request,
        `/plugin-chain-entries/${entry.id}`,
      );
      await removeOwned(
        request,
        `/plugin-chain-entries/${entry.id}`,
        current.revision,
      );
    }
    expect(await chain()).toEqual([]);
    await run({ principal, pluginId: plugin!.id, chain });
  } finally {
    try {
      if (createdPrincipalId) {
        const current = await readJson<Principal>(
          request,
          `/principals/${createdPrincipalId}`,
        );
        await removeOwned(
          request,
          `/principals/${createdPrincipalId}`,
          current.revision,
        );
      }
    } finally {
      const current = await readJson<Upstream>(
        request,
        `/upstreams/${upstream.id}`,
      );
      await removeOwned(
        request,
        `/upstreams/${upstream.id}`,
        current.spec_revision,
      );
    }
  }
}

async function openPrincipal(
  page: Page,
  principalId: string,
  provider: Provider,
): Promise<void> {
  await page.addInitScript((token) => {
    localStorage.setItem('cc-lb-admin-token', token);
    localStorage.setItem('cclb.locale', 'en-US');
  }, e2eState().adminToken);
  const url = new URL('/principals', e2eState().webUrl);
  url.searchParams.set('provider', provider);
  url.searchParams.set('selectedId', principalId);
  await page.goto(url.toString());
  await expect(
    page.getByRole('tab', { name: 'Basic', exact: true }),
  ).toHaveAttribute('aria-selected', 'true');
  const control = page.getByRole('switch', { name: SWITCH_NAME, exact: true });
  await expect(control).toBeVisible();
  await expect(control).toHaveAccessibleName(SWITCH_NAME);
  await expect(control).toBeEnabled();
  await expect(
    page.getByText(PRINCIPAL_WIDE_COPY, { exact: true }),
  ).toBeVisible();
}

function undoFor(page: Page, message: string) {
  return page
    .locator('[data-sonner-toast]')
    .filter({ hasText: message })
    .getByRole('button', { name: 'Undo', exact: true });
}

async function expectPersisted(
  page: Page,
  scenario: Scenario,
  enabled: boolean,
): Promise<void> {
  await expect(
    page.getByRole('switch', { name: SWITCH_NAME, exact: true }),
  ).toBeChecked({ checked: enabled });
  const entries = await scenario.chain();
  if (enabled) {
    expect(entries).toHaveLength(1);
    expect(entries[0]).toMatchObject({
      principal_id: scenario.principal.id,
      slot: 'router',
      wasm_registry_id: scenario.pluginId,
    });
  } else {
    expect(entries).toEqual([]);
  }
}

async function expectNoHorizontalOverflow(page: Page): Promise<void> {
  const overflow = await page.evaluate(
    () =>
      document.documentElement.scrollWidth -
      document.documentElement.clientWidth,
  );
  expect(overflow, 'page has no horizontal overflow').toBeLessThanOrEqual(1);
}

async function seedEnabled(
  request: APIRequestContext,
  scenario: Scenario,
): Promise<PluginChainEntry[]> {
  await createJson<PluginChainEntry>(
    request,
    `/principals/${scenario.principal.id}/plugin-chain`,
    { slot: 'router', wasm_registry_id: scenario.pluginId, order: 0 },
  );
  return scenario.chain();
}

for (const provider of PROVIDERS) {
  for (const width of WIDTHS) {
    test(`${provider} OAuth subscription preference persists and supports both Undo actions at ${width}px`, async ({
      page,
      request,
    }, testInfo) => {
      await page.setViewportSize({ width, height: 1000 });
      await withScenario(request, provider, async (scenario) => {
        await openPrincipal(page, scenario.principal.id, provider);
        const control = page.getByRole('switch', {
          name: SWITCH_NAME,
          exact: true,
        });
        await expectPersisted(page, scenario, false);
        await expectNoHorizontalOverflow(page);
        await control.scrollIntoViewIfNeeded();
        await page.screenshot({
          path: testInfo.outputPath('subscription-off.png'),
        });

        await expect(control).toBeEnabled();
        await control.press('Space');
        await expectPersisted(page, scenario, true);
        await undoFor(page, 'Subscription preference turned on').click();
        await expectPersisted(page, scenario, false);

        await expect(control).toBeEnabled();
        await control.press('Space');
        await expectPersisted(page, scenario, true);
        await expect(control).toBeEnabled();
        await control.press('Space');
        await expectPersisted(page, scenario, false);
        await undoFor(page, 'Subscription preference turned off').click();
        await expectPersisted(page, scenario, true);
        const persisted = await scenario.chain();

        await page.reload();
        await expectPersisted(page, scenario, true);
        expect(await scenario.chain()).toEqual(persisted);
        await expectNoHorizontalOverflow(page);
        await control.scrollIntoViewIfNeeded();
        await page.screenshot({
          path: testInfo.outputPath('subscription-restored.png'),
        });
      });
    });
  }
}

test('a pending DELETE keeps the real chain checked and disables the switch until it completes', async ({
  page,
  request,
}, testInfo) => {
  await withScenario(request, 'openai', async (scenario) => {
    const original = await seedEnabled(request, scenario);
    await openPrincipal(page, scenario.principal.id, 'openai');
    await expectPersisted(page, scenario, true);
    const { promise: gate, resolve: release } = Promise.withResolvers<void>();
    const entryPath = `/admin/v1/plugin-chain-entries/${original[0]!.id}`;
    await page.route(`**${entryPath}`, async (route) => {
      if (route.request().method() === 'DELETE') await gate;
      await route.continue();
    });
    const pending = page.waitForRequest(
      (outgoing) =>
        new URL(outgoing.url()).pathname === entryPath &&
        outgoing.method() === 'DELETE',
    );
    const completed = page.waitForResponse(
      (response) =>
        new URL(response.url()).pathname === entryPath &&
        response.request().method() === 'DELETE',
    );
    const control = page.getByRole('switch', {
      name: SWITCH_NAME,
      exact: true,
    });
    try {
      await control.press('Space');
      await pending;
      await expect(control).toBeDisabled();
      await expect(control).toHaveAttribute('aria-busy', 'true');
      await expectPersisted(page, scenario, true);
      expect(await scenario.chain()).toEqual(original);
      await expect(
        page.getByRole('status').filter({ hasText: 'Turning off...' }),
      ).toBeVisible();
      await page.screenshot({
        path: testInfo.outputPath('subscription-delete-pending.png'),
      });
    } finally {
      release();
    }
    expect((await completed).status()).toBe(204);
    await expectPersisted(page, scenario, false);
    await expect(control).toBeEnabled();
    await expect(control).not.toHaveAttribute('aria-busy', 'true');
  });
});

for (const failure of [
  {
    status: 409,
    code: 'stale_revision',
    message: 'Concurrent update detected — please retry',
  },
  {
    status: 500,
    code: 'storage_error',
    message: 'Internal Server Error',
  },
] as const) {
  test(`a ${failure.code} DELETE failure preserves the checked state and real chain without success Undo`, async ({
    page,
    request,
  }, testInfo) => {
    await withScenario(request, 'gemini', async (scenario) => {
      const original = await seedEnabled(request, scenario);
      await openPrincipal(page, scenario.principal.id, 'gemini');
      await expectPersisted(page, scenario, true);
      const entryPath = `/admin/v1/plugin-chain-entries/${original[0]!.id}`;
      // Explicit fault injection only: all reads, fixture writes, and successful
      // mutations elsewhere in this file still go through the real backend.
      await page.route(`**${entryPath}`, async (route) => {
        if (route.request().method() !== 'DELETE') {
          await route.continue();
          return;
        }
        await route.fulfill({
          status: failure.status,
          contentType: 'application/json',
          body: JSON.stringify({
            error: failure.code,
            ...(failure.status === 409
              ? { current_revision: original[0]!.revision + 1 }
              : {}),
          }),
        });
      });
      const failed = page.waitForResponse(
        (response) =>
          new URL(response.url()).pathname === entryPath &&
          response.request().method() === 'DELETE',
      );
      const control = page.getByRole('switch', {
        name: SWITCH_NAME,
        exact: true,
      });
      await control.press('Space');
      expect((await failed).status()).toBe(failure.status);
      await expect(
        page.getByText(failure.message, { exact: true }),
      ).toBeVisible();
      await expectPersisted(page, scenario, true);
      expect(await scenario.chain()).toEqual(original);
      await expect(control).toBeEnabled();
      await expect(
        page.getByRole('button', { name: 'Undo', exact: true }),
      ).toHaveCount(0);
      await expect(
        page.locator('[data-sonner-toast][data-type="success"]'),
      ).toHaveCount(0);
      await page.screenshot({
        path: testInfo.outputPath(`subscription-delete-${failure.code}.png`),
      });
    });
  });
}
