import { expect, test } from '@playwright/test';

test('provider-aware API-key create flow at supported widths', async ({ page }) => {
  const created: Array<Record<string, unknown>> = [];

  await page.addInitScript(() => {
    localStorage.setItem('cc-lb-admin-token', 'qa');
  });
  await page.route('**/admin/**', async (route) => {
    const request = route.request();
    const { pathname } = new URL(request.url());
    const json = (body: unknown, status = 200) =>
      route.fulfill({
        status,
        contentType: 'application/json',
        body: JSON.stringify(body),
      });

    if (pathname === '/admin/v1/auth/session') {
      return json({
        authority: 'qa',
        subject: 'qa',
        kind: 'human',
        provider_id: 'qa',
        email: null,
        display_name: 'QA',
        expires_at_unix_secs: 2_000_000_000,
        auth_mode: 'static_token',
      });
    }
    if (pathname === '/admin/v1/upstreams' && request.method() === 'GET') {
      return json({ upstreams: [] });
    }
    if (pathname === '/admin/v1/status') {
      return json({
        version: 'qa',
        git_sha: 'qa',
        uptime_secs: 1,
        build: { rust_version: 'qa', profile: 'debug', target: 'qa' },
        generation: 1,
        upstreams: [],
      });
    }
    if (pathname === '/admin/v1/upstreams/preflight') {
      return json({
        verdict: 'ok',
        key_hint: 'provider-...-test',
        classification: {
          key_class: 'unknown',
          needs_workspace: false,
          workspace: { mode: 'required' },
          key_type: 'personal',
        },
        workspaces: { state: 'not_applicable', items: [] },
        capabilities: {
          inference: 'ok',
          workspace_list: 'not_applicable',
          usage_report: 'not_applicable',
          api_key_resolution: 'not_applicable',
        },
        remediation: [],
        checked_at: 2_000_000_000,
      });
    }
    if (pathname === '/admin/v1/upstreams' && request.method() === 'POST') {
      const body = JSON.parse(request.postData() ?? '{}') as Record<
        string,
        unknown
      >;
      created.push(body);
      return json(
        {
          id: `qa-${created.length}`,
          name: body.name,
          kind: body.kind,
          enabled: true,
          spec_revision: 1,
          base_url: body.base_url ?? null,
          warmup_enabled: false,
          warmup_dialect_plugin: null,
          status: {
            last_apply_error: null,
            last_apply_at_unix_secs: null,
            last_warmup_at_unix_secs: null,
          },
        },
        201,
      );
    }
    return json({});
  });

  for (const width of [390, 768, 1440]) {
    await page.setViewportSize({ width, height: 900 });
    await page.goto('/upstreams');
    await page.getByRole('button', { name: 'Add upstream' }).first().click();
    const dialog = page.getByRole('dialog');
    await dialog.getByRole('button', { name: /OpenAI/ }).click();
    await dialog.getByRole('button', { name: 'Continue' }).click();
    await dialog.getByRole('radio', { name: /API key/ }).click();
    await dialog.getByRole('button', { name: 'Continue' }).click();
    await expect(dialog).toContainText('https://api.openai.com');
    await dialog
      .getByPlaceholder('Paste the provider key')
      .fill('sk-proj-browser-qa');
    await dialog.getByRole('button', { name: 'Check key' }).click();
    await expect(dialog).toContainText('OpenAI key verified');
    await expect(dialog).not.toContainText('sk-ant-');
    await expect(dialog).not.toContainText('Workspace');
    await dialog.getByLabel('Project ID').fill('proj-browser');
    await dialog.getByLabel('Upstream name').fill(`OpenAI browser ${width}`);
    await dialog.getByRole('button', { name: 'Add upstream' }).click();
    await expect(dialog).toBeHidden();
    await page.reload();
  }

  expect(created).toHaveLength(3);
  expect(
    created.every(
      (body) => body.kind === 'openai_api_key' && body.project_id === 'proj-browser',
    ),
  ).toBe(true);
});
