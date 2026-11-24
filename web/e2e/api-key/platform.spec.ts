// Build, secrets, OAuth regression and flake policy for the API-key work
// (PRE-01, SEC-11, OAU-01, FLK-06).

import { readdirSync, readFileSync, statSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import type { Page } from '@playwright/test';
import {
  apiKey,
  expect,
  keyField,
  openApiKeyDialog,
  openUpstreams,
  pasteInto,
  test,
  useCustomEndpoint,
} from './harness';

const here = path.dirname(fileURLToPath(import.meta.url));
const webDir = path.resolve(here, '../..');

function filesUnder(dir: string): string[] {
  return readdirSync(dir).flatMap((entry) => {
    const full = path.join(dir, entry);
    return statSync(full).isDirectory() ? filesUnder(full) : [full];
  });
}

interface ClientTrace {
  local: string;
  session: string;
  idb: string[];
  url: string;
  title: string;
  html: string;
}

function clientTrace(page: Page): Promise<ClientTrace> {
  return page.evaluate(async () => {
    const dump = (storage: Storage) =>
      JSON.stringify(
        Array.from({ length: storage.length }, (_, i) => {
          const name = storage.key(i) ?? '';
          return [name, storage.getItem(name)];
        }),
      );
    const databases =
      typeof indexedDB.databases === 'function'
        ? await indexedDB.databases()
        : [];
    return {
      local: dump(localStorage),
      session: dump(sessionStorage),
      idb: databases.map((db) => db.name ?? ''),
      url: location.href,
      title: document.title,
      html: document.documentElement.outerHTML,
    };
  });
}

function expectNoKey(trace: ClientTrace, consoleLines: string[], key: string) {
  for (const value of [
    trace.local,
    trace.session,
    trace.url,
    trace.title,
    trace.html,
    ...trace.idb,
    ...consoleLines,
  ]) {
    expect(value).not.toContain(key);
  }
}

test('PRE-01: the production bundle carries no preview code', async ({
  page,
}) => {
  const markers =
    /preview-scenario-key|apiKeyScenarios|registerPreviewTransport|wrkspc_01Preview/;
  const offenders = filesUnder(path.join(webDir, 'dist')).filter((file) =>
    markers.test(readFileSync(file, 'latin1')),
  );
  expect(offenders).toEqual([]);

  const upstreamCalls: number[] = [];
  page.on('response', (response) => {
    if (new URL(response.url()).pathname === '/admin/v1/upstreams') {
      upstreamCalls.push(response.status());
    }
  });
  await openUpstreams(page);
  await expect(page.getByText('preview-scenario-key')).toHaveCount(0);
  await expect(page.getByRole('button', { name: /preview/i })).toHaveCount(0);
  // Every list call reached the real server (no transport in between).
  expect(upstreamCalls.length).toBeGreaterThan(0);
  expect(upstreamCalls.every((status) => status === 200)).toBe(true);
});

test('SEC-11: the key never lands in web storage, the URL, the title or the console', async ({
  page,
  fake,
}) => {
  const consoleLines: string[] = [];
  page.on('console', (message) => consoleLines.push(message.text()));
  page.on('pageerror', (error) => consoleLines.push(String(error)));

  const saved = apiKey('sec11saved');
  const dialog = await openApiKeyDialog(page);
  const field = keyField(dialog);
  await expect(field).toHaveAttribute('type', 'password');
  await expect(field).toHaveAttribute('autocomplete', 'off');
  await expect(field).toHaveAttribute('spellcheck', 'false');
  await useCustomEndpoint(dialog, fake.url);
  await pasteInto(field, saved);
  await expect(dialog.getByText('Key verified')).toBeVisible();
  await dialog.getByRole('button', { name: 'Add upstream' }).click();
  await expect(dialog).toBeHidden();
  expectNoKey(await clientTrace(page), consoleLines, saved);

  // A pasted-then-cancelled key leaves nothing behind either.
  const cancelled = apiKey('sec11cancel');
  const again = await openApiKeyDialog(page);
  await useCustomEndpoint(again, fake.url);
  await pasteInto(keyField(again), cancelled);
  await expect(again.getByText('Key verified')).toBeVisible();
  await again.getByRole('button', { name: 'Cancel' }).click();
  await expect(again).toBeHidden();
  expectNoKey(await clientTrace(page), consoleLines, cancelled);

  const reopened = await openApiKeyDialog(page);
  await expect(keyField(reopened)).toHaveValue('');
});

test('OAU-01: the default path is still Sign in with Claude, custom endpoint included', async ({
  page,
  routeEnvelope,
}) => {
  await openUpstreams(page);
  const draftStart = page.waitForResponse(
    (response) =>
      new URL(response.url()).pathname === '/admin/v1/oauth/draft/start',
  );
  await page.getByRole('button', { name: 'Add upstream' }).first().click();
  const dialog = page.getByRole('dialog');
  await expect(
    dialog.getByRole('radio', { name: /Claude subscription/ }),
  ).toBeChecked();
  await dialog.getByRole('button', { name: 'Continue' }).click();

  // The real server's draft-start body (startOauthDraft's return type).
  const start = (await (await draftStart).json()) as {
    authorize_url: string;
    state_token: string;
  };
  const signIn = dialog.getByRole('link', { name: 'Sign in with Claude' });
  await expect(signIn).toHaveAttribute('href', start.authorize_url);
  const state = new URL(start.authorize_url).searchParams.get('state');

  // Completing contacts Anthropic's profile endpoints: FX-PW from here on.
  const completeBodies: unknown[] = [];
  const createBodies: unknown[] = [];
  page.on('request', (request) => {
    const pathname = new URL(request.url()).pathname;
    if (pathname === '/admin/v1/oauth/draft/complete') {
      completeBodies.push(request.postDataJSON());
    }
    if (pathname === '/admin/v1/upstreams/from-oauth-draft') {
      createBodies.push(request.postDataJSON());
    }
  });
  await routeEnvelope(
    '/admin/v1/oauth/draft/complete',
    200,
    {
      state_token: start.state_token,
      suggested_name: 'Claude Max',
      subscription_metadata: null,
      organization_metadata: null,
      mode: 'refreshing',
      long_lived_fallback: false,
      fallback_reason: null,
      granted_expires_in_secs: null,
    },
    'POST',
  );
  await routeEnvelope(
    '/admin/v1/upstreams/from-oauth-draft',
    201,
    {
      id: '00000000-0000-4000-8000-00000000a0a1',
      name: 'Claude Max',
      kind: 'anthropic_oauth',
      enabled: true,
      base_url: 'http://127.0.0.1:9',
      warmup_enabled: true,
      warmup_dialect_plugin: null,
      spec_revision: 1,
      status: {
        last_apply_error: null,
        last_apply_at_unix_secs: null,
        last_warmup_at_unix_secs: null,
      },
    },
    'POST',
  );

  await dialog.getByRole('button', { name: 'I already have a code' }).click();
  const code = 'fakeAuthorizationCode0123456789';
  await pasteInto(
    dialog.getByPlaceholder('Paste the code from the Claude page'),
    `${code}#${state}`,
  );
  await expect(dialog.getByText('Signed in')).toBeVisible();
  expect(completeBodies).toEqual([{ state_token: start.state_token, code }]);

  const opener = dialog.getByRole('button', { name: 'Use a custom endpoint' });
  await expect(opener).toHaveAttribute('aria-expanded', 'false');
  await opener.click();
  await dialog
    .getByRole('textbox', { name: /Base URL/ })
    .fill('http://127.0.0.1:9');
  await dialog.getByRole('button', { name: 'Add upstream' }).click();
  await expect.poll(() => createBodies.length).toBe(1);
  expect(createBodies[0]).toEqual({
    state_token: start.state_token,
    name: 'Claude Max',
    base_url: 'http://127.0.0.1:9',
  });
});

test('FLK-06: the suite runs with reduced motion and no fixed waits', async ({
  page,
}) => {
  await openUpstreams(page);
  expect(
    await page.evaluate(
      () => matchMedia('(prefers-reduced-motion: reduce)').matches,
    ),
  ).toBe(true);
  for (const file of filesUnder(here).filter((f) => f.endsWith('.ts'))) {
    const source = readFileSync(file, 'utf8');
    expect(source, file).not.toMatch(/waitForTimeout\(|setTimeout\(/);
  }
});
