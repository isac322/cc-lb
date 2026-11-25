import { expect, type Page, test } from '@playwright/test';

type PluginChainEntry = {
  id: string;
  principal_id: string;
  slot: 'router' | 'shape';
  wasm_registry_id: string;
  order: number;
  config: Record<string, unknown>;
  revision: number;
};

type PluginRegistryItem = {
  id: string;
  sha256_hex: string;
  name: string;
  original_filename: string;
  size_bytes: number;
  refcount: number;
  revision: number;
  uploaded_at_unix_secs: number;
  supported_slots: Array<'router' | 'shape'>;
  hook_metadata: Record<string, unknown>;
  metadata: {
    purpose: string;
    keeps: string;
    drops: string;
    empty_behavior: string;
    examples: string[];
  };
};

function parseIfMatchRevision(header: string | undefined): number | null {
  if (!header) return null;
  const trimmed = header.trim();
  const inner =
    trimmed.startsWith('W/"') && trimmed.endsWith('"')
      ? trimmed.slice(3, -1)
      : trimmed.startsWith('"') && trimmed.endsWith('"')
        ? trimmed.slice(1, -1)
        : trimmed;
  if (!/^\d+$/.test(inner)) return null;
  return Number.parseInt(inner, 10);
}

async function installRouterPipelineFixtures(
  page: Page,
  options?: { adminTerminalStrategy?: 'first-pick' | 'random' },
) {
  const plugins: PluginRegistryItem[] = [
    {
      id: 'subscription-preference',
      sha256_hex: '1'.repeat(64),
      name: 'subscription-preference',
      original_filename: 'subscription-preference.wasm',
      size_bytes: 2048,
      refcount: 2,
      revision: 1,
      uploaded_at_unix_secs: 1_718_380_800,
      supported_slots: ['router'],
      hook_metadata: { router: { wire_version: 1 } },
      metadata: {
        purpose: 'Prefer subscription upstreams before falling back to API keys.',
        keeps: 'Subscription upstreams with remaining quota.',
        drops: 'None unless all subscription upstreams are excluded.',
        empty_behavior: 'Passes all candidate upstreams through unchanged.',
        examples: ['Prefer OAuth pool before metered keys'],
      },
    },
    {
      id: 'canary-router',
      sha256_hex: '2'.repeat(64),
      name: 'canary-router',
      original_filename: 'canary-router.wasm',
      size_bytes: 2048,
      refcount: 1,
      revision: 1,
      uploaded_at_unix_secs: 1_718_380_800,
      supported_slots: ['router'],
      hook_metadata: { router: { wire_version: 1 } },
      metadata: {
        purpose: 'Route canary requests to designated staging upstreams.',
        keeps: 'Upstreams matching the canary header.',
        drops: 'Non-canary upstreams when canary header is set.',
        empty_behavior: 'Falls back to default candidate pool.',
        examples: ['Send canary cohort to test upstream'],
      },
    },
    {
      id: 'cost-router',
      sha256_hex: '3'.repeat(64),
      name: 'cost-router',
      original_filename: 'cost-router.wasm',
      size_bytes: 2048,
      refcount: 1,
      revision: 1,
      uploaded_at_unix_secs: 1_718_380_800,
      supported_slots: ['router'],
      hook_metadata: { router: { wire_version: 1 } },
      metadata: {
        purpose: 'Prefer lower-cost upstreams when quota permits.',
        keeps: 'Upstreams below the configured cost ceiling.',
        drops: 'Upstreams exceeding the cost ceiling.',
        empty_behavior: 'Passes all candidate upstreams through unchanged.',
        examples: ['Cap per-request cost'],
      },
    },
  ];

  const principals = [
    {
      id: 'principal-admin',
      name: 'admin',
      kind: 'human' as const,
      enabled: true,
      allowed_models: [],
      allowed_upstreams: [],
      default_limits: [],
      revision: 1,
      metadata: {},
    },
    {
      id: 'principal-engineering-shared',
      name: 'engineering-shared',
      kind: 'service' as const,
      enabled: true,
      allowed_models: [],
      allowed_upstreams: [],
      default_limits: [],
      revision: 1,
      metadata: {},
    },
  ];

  const chains = new Map<string, PluginChainEntry[]>([
    [
      'principal-admin',
      [
        {
          id: 'entry-admin-1',
          principal_id: 'principal-admin',
          slot: 'router',
          wasm_registry_id: 'subscription-preference',
          order: 0,
          config: {},
          revision: 1,
        },
      ],
    ],
    [
      'principal-engineering-shared',
      [
        {
          id: 'entry-eng-1',
          principal_id: 'principal-engineering-shared',
          slot: 'router',
          wasm_registry_id: 'subscription-preference',
          order: 0,
          config: {},
          revision: 1,
        },
        {
          id: 'entry-eng-2',
          principal_id: 'principal-engineering-shared',
          slot: 'router',
          wasm_registry_id: 'cost-router',
          order: 1,
          config: {},
          revision: 2,
        },
      ],
    ],
  ]);

  const terminalStrategies = new Map<
    string,
    { strategy: 'first-pick' | 'random'; revision: number }
  >([
    [
      'principal-admin',
      { strategy: options?.adminTerminalStrategy ?? 'first-pick', revision: 1 },
    ],
    ['principal-engineering-shared', { strategy: 'first-pick', revision: 1 }],
  ]);

  let nextEntrySeq = 10;

  await page.addInitScript(() => {
    window.localStorage.setItem('cc-lb-admin-token', 'mock-token');
  });

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
      return json(200, {
        status: 'ok',
        version: 'test',
        git_sha: 'test',
        uptime_secs: 1,
      });
    }
    if (method === 'GET' && pathname === '/admin/v1/auth/session') {
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
    if (method === 'GET' && pathname === '/admin/v1/principals') {
      return json(200, { principals });
    }
    if (method === 'GET' && pathname === '/admin/v1/upstreams') {
      return json(200, { upstreams: [] });
    }
    if (method === 'GET' && pathname === '/admin/v1/upstreams/limit-resets') {
      return json(200, { upstreams: [] });
    }
    if (method === 'GET' && pathname === '/admin/v1/plugins/registry') {
      return json(200, { entries: plugins });
    }
    if (method === 'GET' && pathname === '/admin/v1/dashboard/usage') {
      return json(200, {
        range: url.searchParams.get('range') ?? '7d',
        step: url.searchParams.get('step') ?? 'hour',
        group_by: url.searchParams.get('group_by') ?? 'principal',
        window_start_unix_secs: 1_799_395_200,
        window_end_unix_secs: 1_800_000_000,
        observed: true,
        series: [],
      });
    }
    if (method === 'GET' && pathname === '/admin/v1/events/recent') {
      return json(200, { events: [], observed: true, count: 0, limit: 500 });
    }

    const chainMatch = pathname.match(
      /^\/admin\/v1\/principals\/([^/]+)\/plugin-chain$/,
    );
    if (chainMatch) {
      const pid = chainMatch[1];
      if (method === 'GET') {
        const slot = url.searchParams.get('slot');
        const entries = (chains.get(pid) ?? []).filter(
          (entry) => !slot || entry.slot === slot,
        );
        return json(200, { entries });
      }
      if (method === 'POST') {
        const body = request.postDataJSON() as {
          slot: 'router' | 'shape';
          wasm_registry_id: string;
          order?: number;
          config?: Record<string, unknown>;
        };
        const current = [...(chains.get(pid) ?? [])];
        const created: PluginChainEntry = {
          id: `entry-created-${++nextEntrySeq}`,
          principal_id: pid,
          slot: body.slot,
          wasm_registry_id: body.wasm_registry_id,
          order: body.order ?? current.length,
          config: body.config ?? {},
          revision: 1,
        };
        current.push(created);
        chains.set(pid, current);
        return json(200, created);
      }
    }

    const reorderMatch = pathname.match(
      /^\/admin\/v1\/principals\/([^/]+)\/plugin-chain\/reorder$/,
    );
    if (method === 'POST' && reorderMatch) {
      const pid = reorderMatch[1];
      const body = request.postDataJSON() as {
        entries: Array<{ id: string; order: number; expected_revision: number }>;
      };
      const current = [...(chains.get(pid) ?? [])];
      const byId = new Map(current.map((entry) => [entry.id, entry]));
      const updated = current.map((entry) => {
        const spec = body.entries.find((item) => item.id === entry.id);
        return spec
          ? { ...entry, order: spec.order, revision: entry.revision + 1 }
          : entry;
      });
      updated.sort((a, b) => a.order - b.order);
      chains.set(pid, updated);
      return json(200, {
        entries: updated.filter((entry) => byId.has(entry.id)),
      });
    }

    const entryMatch = pathname.match(
      /^\/admin\/v1\/plugin-chain-entries\/([^/]+)$/,
    );
    if (method === 'DELETE' && entryMatch) {
      const entryId = entryMatch[1];
      for (const [pid, entries] of chains.entries()) {
        if (entries.some((entry) => entry.id === entryId)) {
          chains.set(
            pid,
            entries
              .filter((entry) => entry.id !== entryId)
              .map((entry, idx) => ({ ...entry, order: idx })),
          );
          return json(200, { deleted: true });
        }
      }
    }

    const terminalMatch = pathname.match(
      /^\/admin\/v1\/principals\/([^/]+)\/router-terminal$/,
    );
    if (terminalMatch) {
      const pid = terminalMatch[1];
      const current = terminalStrategies.get(pid) ?? {
        strategy: 'first-pick' as const,
        revision: 1,
      };
      if (method === 'GET') {
        return route.fulfill({
          status: 200,
          contentType: 'application/json',
          headers: { etag: `W/"${current.revision}"` },
          body: JSON.stringify(current),
        });
      }
      if (method === 'PUT') {
        const expectedRevision = parseIfMatchRevision(
          request.headers()['if-match'],
        );
        if (expectedRevision === null) {
          return json(428, { error: 'if_match_required' });
        }
        if (expectedRevision !== current.revision) {
          return json(409, {
            error: 'stale_revision',
            current_revision: current.revision,
          });
        }
        let rawBody: unknown;
        try {
          rawBody = request.postDataJSON();
        } catch {
          return json(400, { error: 'invalid_json' });
        }
        if (
          typeof rawBody !== 'object' ||
          rawBody === null ||
          Array.isArray(rawBody) ||
          typeof (rawBody as { strategy?: unknown }).strategy !== 'string'
        ) {
          return json(422, { error: 'missing_strategy' });
        }
        const strategy = (rawBody as { strategy: string }).strategy;
        if (strategy !== 'first-pick' && strategy !== 'random') {
          return json(400, {
            error: 'invalid_router_terminal_strategy',
            allowed: ['first-pick', 'random'],
          });
        }
        const updated = {
          strategy,
          revision: current.revision + 1,
        };
        terminalStrategies.set(pid, updated);
        return route.fulfill({
          status: 200,
          contentType: 'application/json',
          headers: { etag: `W/"${updated.revision}"` },
          body: JSON.stringify(updated),
        });
      }
    }

    if (
      method === 'GET' &&
      /^\/admin\/v1\/principals\/[^/]+\/keys$/.test(pathname)
    ) {
      return json(200, { keys: [] });
    }
    if (
      method === 'GET' &&
      /^\/admin\/v1\/principals\/[^/]+\/cache-keepalive$/.test(pathname)
    ) {
      return json(200, {
        summary: {
          renewing_now: 0,
          sessions_last_5m: 0,
          retained_successful_hits: 0,
          cost_saved: 0,
          total_spent: 0,
          total_avoided: 0,
          unlisted_sessions: 0,
          unlisted_renewals: 0,
          unlisted_spent: 0,
          unlisted_net: 0,
          evidence_status: 'none',
          unknown_cycles: 0,
          not_needed_cycles: 0,
          retained_from_ms: null,
        },
        groups: null,
        rows: [],
        next_cursor: null,
      });
    }

    return json(404, { error: `Unexpected route: ${method} ${pathname}` });
  });
}

function routerSection(page: Page) {
  return page.locator('section', {
    has: page.getByRole('heading', { name: 'Router', exact: true }),
  });
}

function chainItems(page: Page) {
  return routerSection(page).locator(
    'ul > li[data-key]:not([data-key="add-filter-placeholder"])',
  );
}

test.describe('Router Pipeline', () => {
  test('reorder and metadata drawer', async ({
    page,
  }, testInfo) => {
    await installRouterPipelineFixtures(page);
    await page.goto('/principals');

    await expect(
      page.getByRole('heading', { name: 'Principals', exact: true }),
    ).toBeVisible();

    // Step 1: principal load
    await page.locator('aside button', { hasText: 'admin' }).first().click();
    await expect(
      page.getByRole('heading', { name: 'Router', exact: true }),
    ).toBeVisible();
    await page.screenshot({
      path: testInfo.outputPath('chain-loaded-after-principal-select.png'),
    });

    // Step 2: Advanced tab router slot list rendered
    await page.getByRole('tab', { name: 'Advanced' }).click();
    const section = routerSection(page);
    await expect(section.locator('ul').first()).toBeVisible();
    await page.screenshot({
      path: testInfo.outputPath('router-slot-list-rendered.png'),
    });

    // Step 3: terminal row locked - no drag handle
    const terminalHeader = section.getByText('Terminal step', { exact: true });
    await expect(terminalHeader).toBeVisible();
    await expect(
      section.locator('button[aria-label="Drag to reorder"]'),
    ).toHaveCount(0);
    await page.screenshot({
      path: testInfo.outputPath('terminal-row-locked.png'),
    });

    // Step 4: add second router plugin
    const items = chainItems(page);
    await expect(
      items.filter({ hasText: 'subscription-preference' }).first(),
    ).toBeVisible();
    const initialCount = await items.count();
    await section.getByText('Add filter', { exact: true }).click();
    await page
      .locator('.glass-strong.w-64 button')
      .filter({ hasText: 'canary-router' })
      .first()
      .click();
    await expect(items).toHaveCount(initialCount + 1);
    await page.screenshot({
      path: testInfo.outputPath('second-router-plugin-added.png'),
    });

    // Step 5: reorder via down button
    const firstItemText = await items
      .nth(0)
      .locator('button')
      .first()
      .textContent();
    await items
      .nth(0)
      .getByRole('button', { name: 'Move filter down' })
      .click();

    await expect
      .poll(async () => items.nth(1).locator('button').first().textContent())
      .toBe(firstItemText);

    await page.screenshot({
      path: testInfo.outputPath('reorder-via-down-button.png'),
    });

    const infoButton = items
      .filter({ hasText: 'subscription-preference' })
      .first()
      .getByRole('button', { name: 'subscription-preference', exact: true });
    await infoButton.click();

    const drawer = page.getByRole('dialog');
    await expect(drawer).toBeVisible();
    await expect(drawer.getByTestId('plugin-purpose')).toBeVisible();
    await expect(drawer.getByTestId('plugin-keeps')).toBeVisible();
    await expect(drawer.getByTestId('plugin-drops')).toBeVisible();
    await expect(drawer.getByTestId('plugin-empty-behavior')).toBeVisible();
    await expect(drawer.getByTestId('plugin-examples')).toBeVisible();

    await drawer.getByRole('button', { name: 'Close' }).click();
    await expect(drawer).not.toBeVisible();
  });

  test('complex chain auto-opens Advanced with disabled Basic', async ({
    page,
  }) => {
    await installRouterPipelineFixtures(page);
    await page.goto('/principals');

    await expect(
      page.getByRole('heading', { name: 'Principals', exact: true }),
    ).toBeVisible();

    await page
      .locator('aside button', { hasText: 'engineering-shared' })
      .first()
      .click();
    await expect(
      page.getByRole('heading', { name: 'Router', exact: true }),
    ).toBeVisible();

    const basicTab = page.getByRole('tab', { name: 'Basic' });
    await expect(basicTab).toHaveAttribute('aria-disabled', 'true');

    const advancedTab = page.getByRole('tab', { name: 'Advanced' });
    await expect(advancedTab).toHaveAttribute('aria-selected', 'true');

    await basicTab.click({ force: true });
    await expect(
      page
        .getByText(
          'Basic requires a router chain containing only subscription-preference. Open Advanced to edit the full chain.',
        )
        .first(),
    ).toBeVisible();
  });

  test('dashed placeholder opens picker and adds filter', async ({ page }) => {
    await installRouterPipelineFixtures(page);
    await page.goto('/principals');

    await expect(
      page.getByRole('heading', { name: 'Principals', exact: true }),
    ).toBeVisible();

    await page.locator('aside button', { hasText: 'admin' }).first().click();
    await expect(
      page.getByRole('heading', { name: 'Router', exact: true }),
    ).toBeVisible();

    await page.getByRole('tab', { name: 'Advanced' }).click();

    const items = chainItems(page);
    await expect(
      items.filter({ hasText: 'subscription-preference' }).first(),
    ).toBeVisible();

    const section = routerSection(page);
    await section.getByText('Add filter', { exact: true }).click();

    const pluginName = 'canary-router';
    const picker = page
      .locator('.glass-strong.w-64 button:not([disabled])')
      .filter({ hasText: pluginName });
    await expect(picker).toBeVisible();
    await picker.click();

    await expect(
      items.filter({ hasText: 'subscription-preference' }).first(),
    ).toBeVisible();
    await expect(items.filter({ hasText: pluginName }).first()).toBeVisible();

    await section.getByText('Add filter', { exact: true }).click();

    const disabledPicker = page
      .locator('.glass-strong.w-64 button')
      .filter({ hasText: pluginName });
    await expect(disabledPicker).toBeDisabled();
    await expect(disabledPicker).toContainText('Already in chain');
  });

  test('terminal radio cards reflect strategy change', async ({ page }) => {
    await installRouterPipelineFixtures(page, {
      adminTerminalStrategy: 'random',
    });
    await page.goto('/principals');

    await expect(
      page.getByRole('heading', { name: 'Principals', exact: true }),
    ).toBeVisible();

    await page.locator('aside button', { hasText: 'admin' }).first().click();
    await expect(
      page.getByRole('heading', { name: 'Router', exact: true }),
    ).toBeVisible();

    await page.getByRole('tab', { name: 'Advanced' }).click();

    const section = routerSection(page);
    const termGroup = section.getByRole('radiogroup', {
      name: 'When multiple upstreams qualify, pick',
    });
    await expect(termGroup.getByRole('radio', { name: /Random/ })).toBeChecked();

    const putResponsePromise = page.waitForResponse(
      (response) =>
        response.url().includes('/admin/v1/principals/principal-admin/router-terminal') &&
        response.request().method() === 'PUT',
    );
    await section.locator('label', { hasText: 'First eligible' }).click();
    const putResponse = await putResponsePromise;
    expect(putResponse.status()).toBe(200);
    expect(await putResponse.json()).toEqual({
      strategy: 'first-pick',
      revision: 2,
    });

    const firstEligibleRadio = termGroup.getByRole('radio', {
      name: /First eligible/,
    });
    await expect(firstEligibleRadio).toBeChecked();

    const reloadGetPromise = page.waitForResponse(
      (response) =>
        response.url().includes('/admin/v1/principals/principal-admin/router-terminal') &&
        response.request().method() === 'GET',
    );
    await page.reload();
    const reloadGetResponse = await reloadGetPromise;
    expect(reloadGetResponse.status()).toBe(200);
    expect(await reloadGetResponse.json()).toEqual({
      strategy: 'first-pick',
      revision: 2,
    });
    await expect(
      page.getByRole('heading', { name: 'Router', exact: true }),
    ).toBeVisible();
    await page.getByRole('tab', { name: 'Advanced' }).click();
    await expect(
      routerSection(page)
        .getByRole('radiogroup', { name: 'When multiple upstreams qualify, pick' })
        .getByRole('radio', { name: /First eligible/ }),
    ).toBeChecked();
  });
});
