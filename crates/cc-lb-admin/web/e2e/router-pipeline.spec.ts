import { expect, type Page, test } from '@playwright/test';
import fs from 'fs';
import path from 'path';
import type {
  PluginChainEntry,
  PluginEntry,
  Principal,
} from '../src/lib/queries';
import { fulfillAuthenticatedSession } from './support/auth-session';

const evidenceDir = path.join(process.cwd(), '../../../.omo/evidence');

function principal(id: string, name: string): Principal {
  return {
    id,
    name,
    kind: 'admin',
    enabled: true,
    revision: 1,
    allowed_models: [],
    allowed_upstreams: [],
    default_limits: [],
    cache_keepalive: null,
  };
}

function plugin(
  id: string,
  name: string,
  metadata: PluginEntry['metadata'] = null,
): PluginEntry {
  return {
    id,
    sha256_hex: '0'.repeat(64),
    name,
    original_filename: `${name}.wasm`,
    description: '',
    usage: '',
    hook_metadata: {},
    label: null,
    size_bytes: 1,
    refcount: 0,
    revision: 1,
    uploaded_at_unix_secs: 1_700_000_000,
    metadata,
    supported_slots: ['router'],
  };
}

async function installRouterFixtures(page: Page) {
  const principals = [
    principal('principal-admin', 'admin'),
    principal('principal-engineering', 'engineering-shared'),
  ];
  const plugins = [
    plugin('plugin-subscription', 'subscription-preference', {
      purpose: 'Prefer upstreams with usable subscription quota.',
      keeps: 'Candidates with available quota.',
      drops: 'Candidates with exhausted quota when alternatives exist.',
      empty_behavior: 'Keep the original candidates.',
      examples: ['Prefer a healthy subscription-backed upstream.'],
    }),
    plugin('plugin-rate-limit', 'rate-limiter'),
    plugin('plugin-canary', 'canary-router'),
  ];
  const chains = new Map<string, PluginChainEntry[]>([
    [
      'principal-admin',
      [
        {
          id: 'entry-admin-subscription',
          principal_id: 'principal-admin',
          slot: 'router',
          order: 0,
          wasm_registry_id: 'plugin-subscription',
          config: {},
          sse_per_event: false,
          batched_events_per_flush: 1,
          batched_flush_ms: 100,
          revision: 1,
        },
        {
          id: 'entry-admin-rate-limit',
          principal_id: 'principal-admin',
          slot: 'router',
          order: 1,
          wasm_registry_id: 'plugin-rate-limit',
          config: {},
          sse_per_event: false,
          batched_events_per_flush: 1,
          batched_flush_ms: 100,
          revision: 1,
        },
      ],
    ],
    [
      'principal-engineering',
      [
        {
          id: 'entry-engineering-rate-limit',
          principal_id: 'principal-engineering',
          slot: 'router',
          order: 0,
          wasm_registry_id: 'plugin-rate-limit',
          config: {},
          sse_per_event: false,
          batched_events_per_flush: 1,
          batched_flush_ms: 100,
          revision: 1,
        },
      ],
    ],
  ]);
  const terminalStrategies = new Map(
    principals.map(({ id }) => [id, { strategy: 'first-pick', revision: 1 }]),
  );
  let nextEntry = 1;

  await page.addInitScript(() => {
    window.localStorage.setItem('cc-lb-admin-token', 'mock-token');
  });

  await page.route('**/admin/**', async (route) => {
    if (await fulfillAuthenticatedSession(route)) return;
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
      return json(200, {
        status: 'ok',
        version: 'e2e',
        git_sha: 'fixture',
        uptime_secs: 1,
      });
    }
    if (method === 'GET' && pathname === '/admin/v1/principals') {
      return json(200, { principals });
    }
    if (method === 'GET' && pathname === '/admin/v1/upstreams') {
      return json(200, { upstreams: [] });
    }
    if (method === 'GET' && pathname === '/admin/v1/plugins/registry') {
      return json(200, { entries: plugins });
    }
    if (method === 'GET' && pathname === '/admin/events/recent') {
      return json(200, { events: [], observed: true, count: 0, limit: 5 });
    }

    const chainMatch = pathname.match(
      /^\/admin\/v1\/principals\/([^/]+)\/plugin-chain$/,
    );
    if (chainMatch && method === 'GET') {
      const entries =
        url.searchParams.get('slot') === 'router'
          ? (chains.get(chainMatch[1]) ?? [])
          : [];
      return json(200, { entries });
    }
    if (chainMatch && method === 'POST') {
      const body = request.postDataJSON() as {
        slot: PluginChainEntry['slot'];
        wasm_registry_id: string;
        order?: number;
      };
      const entries = chains.get(chainMatch[1]) ?? [];
      const entry: PluginChainEntry = {
        id: `entry-added-${nextEntry++}`,
        principal_id: chainMatch[1],
        slot: body.slot,
        order: body.order ?? entries.length,
        wasm_registry_id: body.wasm_registry_id,
        config: {},
        sse_per_event: false,
        batched_events_per_flush: 1,
        batched_flush_ms: 100,
        revision: 1,
      };
      entries.push(entry);
      chains.set(chainMatch[1], entries);
      return json(201, entry);
    }

    const reorderMatch = pathname.match(
      /^\/admin\/v1\/principals\/([^/]+)\/plugin-chain\/reorder$/,
    );
    if (reorderMatch && method === 'POST') {
      const body = request.postDataJSON() as {
        entries: Array<{ id: string; order: number }>;
      };
      const entries = chains.get(reorderMatch[1]) ?? [];
      for (const update of body.entries) {
        const entry = entries.find(({ id }) => id === update.id);
        if (entry) {
          entry.order = update.order;
          entry.revision += 1;
        }
      }
      entries.sort((left, right) => left.order - right.order);
      return json(200, { entries });
    }

    const terminalMatch = pathname.match(
      /^\/admin\/v1\/principals\/([^/]+)\/router-terminal$/,
    );
    if (terminalMatch && method === 'GET') {
      return json(
        200,
        terminalStrategies.get(terminalMatch[1]) ?? {
          strategy: 'first-pick',
          revision: 1,
        },
      );
    }
    if (terminalMatch && method === 'PUT') {
      const current = terminalStrategies.get(terminalMatch[1]) ?? {
        strategy: 'first-pick',
        revision: 1,
      };
      const body = request.postDataJSON() as { strategy: string };
      const next = {
        strategy: body.strategy,
        revision: current.revision + 1,
      };
      terminalStrategies.set(terminalMatch[1], next);
      return json(200, next);
    }

    if (
      method === 'GET' &&
      /^\/admin\/v1\/principals\/[^/]+\/(keys|limits)$/.test(pathname)
    ) {
      return json(
        200,
        pathname.endsWith('/keys')
          ? { keys: [] }
          : { principal_id: '', observed: true, identities: [] },
      );
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

    return json(501, {
      error: `Unexpected router E2E request: ${method} ${pathname}${url.search}`,
    });
  });
}

async function openPrincipal(page: Page, name: string) {
  await page.goto('/principals');
  await page.locator('aside button', { hasText: name }).first().click();
  await expect(page.getByRole('heading', { name: 'Router' })).toBeVisible();
}

test.describe('Router Pipeline', () => {
  test.beforeEach(async ({ page }) => {
    fs.mkdirSync(evidenceDir, { recursive: true });
    await installRouterFixtures(page);
  });

  test('drag reorder, terminal toggle, and hover panels', async ({ page }) => {
    await openPrincipal(page, 'admin');

    const pluginList = page
      .getByRole('list')
      .filter({
        has: page.getByRole('button', {
          name: 'subscription-preference',
          exact: true,
        }),
      })
      .first();
    await expect(pluginList).toBeVisible();
    const terminalRow = page
      .locator('div.bg-overlay-1')
      .filter({ hasText: 'Terminal step' })
      .first();
    await expect(terminalRow).toBeVisible();
    await expect(
      terminalRow.locator('button[aria-label="Drag to reorder"]'),
    ).toHaveCount(0);

    await page.getByRole('tab', { name: 'Advanced' }).click();
    const chainRows = pluginList
      .locator('li[data-key]:not([data-key^="connector-"])')
      .filter({ hasNotText: 'Add filter' });
    const initialCount = await chainRows.count();
    await page.getByText('Add filter', { exact: true }).click();
    await page
      .getByRole('dialog')
      .getByRole('button', { name: /canary-router/ })
      .click();
    await expect(chainRows).toHaveCount(initialCount + 1);

    const firstItemText = await chainRows
      .nth(0)
      .locator('button.hover\\:underline')
      .textContent();
    await chainRows.nth(0).locator('button').nth(2).click();
    await expect
      .poll(async () =>
        (await chainRows
          .nth(1)
          .locator('button.hover\\:underline')
          .textContent()) === firstItemText,
      )
      .toBeTruthy();

    await chainRows
      .filter({ hasText: 'subscription-preference' })
      .first()
      .locator('button.hover\\:underline')
      .click();
    const drawer = page.getByRole('dialog');
    await expect(drawer).toBeVisible();
    await expect(drawer.getByTestId('plugin-purpose')).toBeVisible();
    await expect(drawer.getByTestId('plugin-keeps')).toBeVisible();
    await expect(drawer.getByTestId('plugin-drops')).toBeVisible();
    await expect(drawer.getByTestId('plugin-empty-behavior')).toBeVisible();
    await expect(drawer.getByTestId('plugin-examples')).toBeVisible();
    await drawer.getByRole('button', { name: 'Close' }).click();
    await expect(drawer).not.toBeVisible();

    const responsePromise = page.waitForResponse(
      (response) =>
        response.url().includes('/router-terminal') &&
        response.request().method() === 'PUT',
    );
    await page.locator('label', { hasText: 'Random' }).last().click();
    await responsePromise;

    await page.reload();
    await expect(page.getByRole('heading', { name: 'Router' })).toBeVisible();
    await page.getByRole('tab', { name: 'Advanced' }).click();
    await expect(
      page.locator('input[name="term-strategy"][value="random"]'),
    ).toBeChecked();
  });

  test('complex chain auto-opens Advanced with disabled Basic', async ({ page }) => {
    await openPrincipal(page, 'engineering-shared');

    const basicTab = page.getByRole('tab', { name: 'Basic' });
    await expect(basicTab).toHaveAttribute('aria-disabled', 'true');
    await expect(page.getByRole('tab', { name: 'Advanced' })).toHaveAttribute(
      'aria-selected',
      'true',
    );
    await basicTab.click({ force: true });
    await expect(
      page
        .getByRole('dialog')
        .getByText(
          'Basic requires a router chain containing only subscription-preference. Open Advanced to edit the full chain.',
        ),
    ).toBeVisible();
  });

  test('dashed placeholder opens picker and adds filter', async ({ page }) => {
    await openPrincipal(page, 'admin');
    await page.getByRole('tab', { name: 'Advanced' }).click();
    await page.getByText('Add filter', { exact: true }).click();

    const picker = page
      .getByRole('dialog')
      .getByRole('button', { name: /canary-router/ });
    await expect(picker).toBeVisible();
    await picker.click();
    await expect(
      page.getByRole('button', { name: 'canary-router', exact: true }),
    ).toBeVisible();

    await page.getByText('Add filter', { exact: true }).click();
    const disabledPicker = page
      .getByRole('dialog')
      .getByRole('button', { name: /canary-router/ });
    await expect(disabledPicker).toBeDisabled();
    await expect(disabledPicker).toContainText('Already in chain');
  });

  test('terminal radio cards reflect strategy change', async ({ page }) => {
    await openPrincipal(page, 'admin');
    await page.getByRole('tab', { name: 'Advanced' }).click();
    await page.locator('label', { hasText: 'Random' }).last().click();
    await expect(
      page.locator('input[name="term-strategy"][value="random"]'),
    ).toBeChecked();
  });
});
