// Shared fixtures for the API-key real-server suite (playwright.api-key.config).
//
// - `fake`: fake-anthropic's debug routes (`/__key_policies`, `/__requests`),
//   wire shapes per impl-plan §1.11. A key with no policy behaves as today.
// - `adminApi` / `seedApiKeyUpstream`: the real admin API with the static token.
// - `routeEnvelope` / `patchEnvelope`: FX-PW, for Anthropic-host-only states the
//   real server can never reach in a test. The later registration wins.
// - `toasts`: every sonner toast the page ever rendered, even after it left.
// Each test starts with no live upstreams, no key policies, no recorded calls.

import { randomBytes } from 'node:crypto';
import { readFileSync } from 'node:fs';
import {
  test as base,
  expect,
  type Locator,
  type Page,
} from '@playwright/test';
import type {
  AnthropicWorkspace,
  ApiKeyClass,
  ApiKeyOrgCapability,
  ApiKeyType,
  ApiKeyWorkspace,
  PreflightWireResponse,
  RemediationCode,
  Upstream,
} from '../../src/lib/api';

export { expect };

// ─── state handed over by global-setup ───────────────────────────────────────

export interface E2eState {
  adminUrl: string;
  webUrl: string;
  fakeUrl: string;
  /** A loopback port nothing listens on (FX-DEAD). */
  deadUrl: string;
  adminToken: string;
  pids: number[];
  dir: string;
}

let cachedState: E2eState | null = null;
export function e2eState(): E2eState {
  if (cachedState) return cachedState;
  const statePath = process.env.CCLB_E2E_STATE;
  if (!statePath) {
    throw new Error(
      'CCLB_E2E_STATE is unset: run with -c playwright.api-key.config.ts',
    );
  }
  cachedState = JSON.parse(readFileSync(statePath, 'utf8')) as E2eState;
  return cachedState;
}

// ─── fake-anthropic wire shapes (impl-plan §1.11) ────────────────────────────

export type Script =
  | { kind: 'default' }
  | {
      kind: 'status';
      status: number;
      body?: unknown;
      headers?: Record<string, string>;
    }
  | { kind: 'sequence'; steps: Script[] }
  | { kind: 'delay'; secs: number; then?: Script }
  | { kind: 'redirect'; status: number; location: string }
  | { kind: 'huge'; bytes: number }
  | { kind: 'stall' };

export interface FakeWorkspace {
  id: string;
  name: string;
}

export interface FakeApiKey {
  id: string;
  partial_key_hint: string;
  principal?: unknown;
  scope?: unknown;
}

export interface KeyPolicy {
  models?: Script;
  count_tokens?: Script;
  bound_workspace?: string | null;
  requires_workspace?: boolean;
  workspaces?: FakeWorkspace[];
  workspaces_list?: Script;
  usage_report?: Script;
  api_keys?: Script;
  usage_pages?: unknown[];
  api_key_items?: FakeApiKey[];
}

export interface RecordedRequest {
  method: string;
  path: string;
  query: string | null;
  headers: Record<string, string>;
  /** Recorded when the body is at most 64 KiB. */
  body?: string | null;
}

/** Anthropic's 401 for a revoked or mistyped key. */
export const STATUS_401: Script = {
  kind: 'status',
  status: 401,
  body: {
    type: 'error',
    error: { type: 'authentication_error', message: 'invalid x-api-key' },
  },
};

/** count_tokens refused for a valid key (`key_forbidden`). */
export const STATUS_403: Script = {
  kind: 'status',
  status: 403,
  body: {
    type: 'error',
    error: {
      type: 'permission_error',
      message: 'This API key does not have permission to use this endpoint.',
    },
  },
};

// ─── keys ────────────────────────────────────────────────────────────────────

/** A fresh standard key; `tag` keeps traces readable. */
export function apiKey(tag: string): string {
  return `sk-ant-api03-${tag}-${randomBytes(16).toString('hex')}`;
}

// ─── admin API ───────────────────────────────────────────────────────────────

export interface AdminApi {
  get<T>(path: string): Promise<T>;
  post<T>(path: string, body: unknown, ifMatch?: number): Promise<T>;
  put<T>(path: string, body: unknown, ifMatch: number): Promise<T>;
  /** Raw call for status assertions. */
  request(
    method: string,
    path: string,
    body?: unknown,
    ifMatch?: number,
  ): Promise<{ status: number; body: unknown }>;
}

function adminApi(state: E2eState): AdminApi {
  const request = async (
    method: string,
    path: string,
    body?: unknown,
    ifMatch?: number,
  ) => {
    const headers: Record<string, string> = {
      authorization: `Bearer ${state.adminToken}`,
    };
    if (body !== undefined) headers['content-type'] = 'application/json';
    if (ifMatch !== undefined) headers['if-match'] = `W/"${ifMatch}"`;
    const res = await fetch(`${state.adminUrl}${path}`, {
      method,
      headers,
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    const text = await res.text();
    return { status: res.status, body: text ? JSON.parse(text) : null };
  };
  const ok = async <T>(
    method: string,
    path: string,
    body?: unknown,
    ifMatch?: number,
  ): Promise<T> => {
    const res = await request(method, path, body, ifMatch);
    if (res.status >= 300) {
      throw new Error(
        `${method} ${path} → ${res.status} ${JSON.stringify(res.body)}`,
      );
    }
    return res.body as T;
  };
  return {
    get: (path) => ok('GET', path),
    post: (path, body, ifMatch) => ok('POST', path, body, ifMatch),
    put: (path, body, ifMatch) => ok('PUT', path, body, ifMatch),
    request,
  };
}

export async function liveUpstreams(api: AdminApi): Promise<Upstream[]> {
  const { upstreams } = await api.get<{ upstreams: Upstream[] }>(
    '/admin/v1/upstreams',
  );
  return upstreams;
}

// ─── fake-anthropic control ──────────────────────────────────────────────────

export interface FakeAnthropic {
  url: string;
  setKeyPolicies(policies: Record<string, KeyPolicy>): Promise<void>;
  clearKeyPolicies(): Promise<void>;
  requests(pathPrefix?: string): Promise<RecordedRequest[]>;
  clearRequests(): Promise<void>;
}

function fakeAnthropic(state: E2eState): FakeAnthropic {
  const call = async (method: string, path: string, body?: unknown) => {
    const res = await fetch(`${state.fakeUrl}${path}`, {
      method,
      headers:
        body === undefined ? undefined : { 'content-type': 'application/json' },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    if (!res.ok) {
      throw new Error(`fake ${method} ${path} → ${res.status}`);
    }
    return res;
  };
  return {
    url: state.fakeUrl,
    async setKeyPolicies(policies) {
      for (const [key, policy] of Object.entries(policies)) {
        await call('POST', '/__key_policies', { key, policy });
      }
    },
    async clearKeyPolicies() {
      await call('DELETE', '/__key_policies');
    },
    async requests(pathPrefix) {
      const res = await call('GET', '/__requests');
      const all = (await res.json()) as RecordedRequest[];
      return pathPrefix
        ? all.filter((r) => r.path.startsWith(pathPrefix))
        : all;
    },
    async clearRequests() {
      await call('POST', '/__requests/clear');
    },
  };
}

// ─── toasts ──────────────────────────────────────────────────────────────────

/** Records each sonner toast once, so a toast that already left still counts. */
const TOAST_RECORDER = `(() => {
  const seen = new WeakSet();
  const toasts = [];
  Object.defineProperty(window, '__cclbToasts', { value: toasts });
  const scan = (root) => {
    if (!(root instanceof Element)) return;
    const found = root.matches('[data-sonner-toast]')
      ? [root]
      : Array.from(root.querySelectorAll('[data-sonner-toast]'));
    for (const el of found) {
      if (seen.has(el)) continue;
      seen.add(el);
      toasts.push(el);
    }
  };
  new MutationObserver((records) => {
    for (const record of records) {
      for (const node of record.addedNodes) scan(node);
    }
  }).observe(document, { childList: true, subtree: true });
})();`;

export interface Toasts {
  count(): Promise<number>;
  errorCount(): Promise<number>;
  texts(): Promise<string[]>;
}

function toastsOf(page: Page): Toasts {
  const read = () =>
    page.evaluate(() => {
      const list: unknown = Reflect.get(window, '__cclbToasts');
      const toasts = Array.isArray(list) ? list : [];
      return toasts
        .filter((el): el is Element => el instanceof Element)
        .map((el) => ({
          type: el.getAttribute('data-type') ?? '',
          // The title only: an action button ("Undo") is not part of the message.
          text: (el.querySelector('[data-title]') ?? el).textContent ?? '',
        }));
    });
  return {
    count: async () => (await read()).length,
    errorCount: async () =>
      (await read()).filter((t) => t.type === 'error').length,
    texts: async () => (await read()).map((t) => t.text),
  };
}

// ─── FX-PW envelopes ─────────────────────────────────────────────────────────

export type RouteEnvelope = (
  path: string | RegExp,
  status: number,
  body: unknown,
  method?: string,
) => Promise<void>;

export type PatchEnvelope = (
  path: string | RegExp,
  patch: (body: unknown) => unknown,
  method?: string,
) => Promise<void>;

function pathMatcher(path: string | RegExp): (url: URL) => boolean {
  return typeof path === 'string'
    ? (url) => url.pathname === path
    : (url) => path.test(url.pathname);
}

// ─── fixtures ────────────────────────────────────────────────────────────────

export interface SeedOptions {
  name?: string;
  key: string;
  /** Defaults to the fake (FX-FAKE); `null` stores the Anthropic default. */
  baseUrl?: string | null;
  workspaceId?: string;
  skipPreflight?: boolean;
}

interface Fixtures {
  adminToken: string;
  adminApi: AdminApi;
  fake: FakeAnthropic;
  seedApiKeyUpstream: (options: SeedOptions) => Promise<Upstream>;
  toasts: Toasts;
  routeEnvelope: RouteEnvelope;
  patchEnvelope: PatchEnvelope;
  cleanSlate: undefined;
}

export const test = base.extend<Fixtures>({
  // biome-ignore lint/correctness/noEmptyPattern: Playwright fixture signature.
  baseURL: async ({}, use) => {
    await use(e2eState().webUrl);
  },
  // biome-ignore lint/correctness/noEmptyPattern: Playwright fixture signature.
  adminToken: async ({}, use) => {
    await use(e2eState().adminToken);
  },
  // biome-ignore lint/correctness/noEmptyPattern: Playwright fixture signature.
  adminApi: async ({}, use) => {
    await use(adminApi(e2eState()));
  },
  // biome-ignore lint/correctness/noEmptyPattern: Playwright fixture signature.
  fake: async ({}, use) => {
    await use(fakeAnthropic(e2eState()));
  },
  cleanSlate: [
    async ({ adminApi, fake }, use) => {
      for (const upstream of await liveUpstreams(adminApi)) {
        await adminApi.request(
          'DELETE',
          `/admin/v1/upstreams/${upstream.id}`,
          undefined,
          upstream.spec_revision,
        );
      }
      await fake.clearKeyPolicies();
      await fake.clearRequests();
      await use(undefined);
    },
    { auto: true },
  ],
  page: async ({ page, adminToken }, use) => {
    await page.addInitScript((token) => {
      localStorage.setItem('cc-lb-admin-token', token);
    }, adminToken);
    await page.addInitScript(TOAST_RECORDER);
    await use(page);
  },
  toasts: async ({ page }, use) => {
    await use(toastsOf(page));
  },
  seedApiKeyUpstream: async ({ adminApi, fake }, use, testInfo) => {
    let n = 0;
    await use(async (options) => {
      n += 1;
      const baseUrl =
        options.baseUrl === undefined ? fake.url : options.baseUrl;
      const res = await adminApi.request('POST', '/admin/v1/upstreams', {
        name: options.name ?? `e2e-${testInfo.testId.slice(0, 8)}-${n}`,
        kind: 'anthropic_api_key',
        base_url: baseUrl,
        api_key_value: options.key,
        workspace_id: options.workspaceId ?? null,
        skip_preflight: options.skipPreflight ?? false,
      });
      if (res.status !== 201) {
        throw new Error(
          `seed upstream → ${res.status} ${JSON.stringify(res.body)}`,
        );
      }
      return res.body as Upstream;
    });
  },
  routeEnvelope: async ({ page }, use) => {
    await use(async (path, status, body, method) => {
      const matches = pathMatcher(path);
      await page.route(
        (url) => matches(url),
        async (route) => {
          if (method && route.request().method() !== method.toUpperCase()) {
            await route.fallback();
            return;
          }
          await route.fulfill({
            status,
            contentType: 'application/json',
            headers: { 'cache-control': 'no-store' },
            body: JSON.stringify(body),
          });
        },
      );
    });
  },
  patchEnvelope: async ({ page }, use) => {
    await use(async (path, patch, method = 'GET') => {
      const matches = pathMatcher(path);
      await page.route(
        (url) => matches(url),
        async (route) => {
          if (route.request().method() !== method.toUpperCase()) {
            await route.fallback();
            return;
          }
          const response = await route.fetch();
          const json: unknown = await response.json();
          await route.fulfill({ response, json: patch(json) });
        },
      );
    });
  },
});

// ─── page helpers ────────────────────────────────────────────────────────────

export async function openUpstreams(page: Page): Promise<void> {
  await page.goto('/upstreams');
  await expect(
    page.getByRole('button', { name: 'Add upstream' }).first(),
  ).toBeVisible();
}

export async function openUpstream(page: Page, id: string): Promise<void> {
  await page.goto(`/upstreams?selectedId=${encodeURIComponent(id)}`);
}

/** Opens Add upstream and walks to the API-key Key step. */
export async function openApiKeyDialog(page: Page): Promise<Locator> {
  await openUpstreams(page);
  await page.getByRole('button', { name: 'Add upstream' }).first().click();
  const dialog = page.getByRole('dialog');
  await dialog.getByRole('radio', { name: /Anthropic API key/ }).click();
  await dialog.getByRole('button', { name: 'Continue' }).click();
  await expect(dialog.getByPlaceholder('sk-ant-…')).toBeFocused();
  return dialog;
}

export function keyField(dialog: Locator): Locator {
  return dialog.getByPlaceholder('sk-ant-…');
}

/**
 * Pastes like an operator: a real `paste` event carrying `clipboardData`, so
 * the dialog's paste-first path runs (no clipboard permission needed).
 */
export async function pasteInto(field: Locator, text: string): Promise<void> {
  await field.focus();
  await field.evaluate((el, value) => {
    const data = new DataTransfer();
    data.setData('text/plain', value);
    el.dispatchEvent(
      new ClipboardEvent('paste', {
        clipboardData: data,
        bubbles: true,
        cancelable: true,
      }),
    );
  }, text);
}

/** Opens the custom-endpoint disclosure on the Key step and fills it. */
export async function useCustomEndpoint(
  dialog: Locator,
  url: string,
): Promise<void> {
  await dialog.getByRole('button', { name: 'Use a custom endpoint' }).click();
  const field = dialog.getByRole('textbox', { name: /Base URL/ });
  await expect(field).toBeFocused();
  await field.fill(url);
}

/** Visible primary buttons (a disabled primary renders as a secondary). */
export function primaryButtons(scope: Locator): Locator {
  return scope.locator('button.bg-accent:visible');
}

export function currentRailStep(dialog: Locator): Locator {
  return dialog.locator('ol[aria-label="Steps"] li[aria-current="step"]');
}

// ─── FX-PW preflight envelopes (plan §7 wire) ────────────────────────────────

export const PRODUCTION_WORKSPACE: AnthropicWorkspace = {
  id: 'wrkspc_01ProductionWorkspace4f2a',
  name: 'Production',
};

/** A preflight answer the real server would give on api.anthropic.com. */
export function preflightWire(p: {
  verdict: PreflightWireResponse['verdict'];
  keyClass?: ApiKeyClass;
  workspace?: ApiKeyWorkspace;
  keyType?: ApiKeyType | null;
  inference?: PreflightWireResponse['capabilities']['inference'];
  workspaceList?: ApiKeyOrgCapability;
  usageReport?: ApiKeyOrgCapability;
  items?: AnthropicWorkspace[];
  remediation?: RemediationCode[];
}): PreflightWireResponse {
  const workspaceList = p.workspaceList ?? 'not_applicable';
  return {
    verdict: p.verdict,
    key_hint: 'sk-ant-api03-...AbCd',
    classification: {
      key_class: p.keyClass ?? 'unknown',
      needs_workspace: p.verdict === 'needs_workspace',
      workspace: p.workspace ?? { mode: 'required' },
      key_type: p.keyType ?? null,
    },
    workspaces: {
      state: workspaceList,
      items: workspaceList === 'ok' ? (p.items ?? [PRODUCTION_WORKSPACE]) : [],
    },
    capabilities: {
      inference: p.inference ?? 'ok',
      workspace_list: workspaceList,
      usage_report: p.usageReport ?? 'not_applicable',
      api_key_resolution:
        p.usageReport === 'ok' ? 'resolved' : 'not_applicable',
    },
    remediation: (p.remediation ?? []).map((code) => ({ code })),
    checked_at: Math.floor(Date.now() / 1000),
  };
}

/** The org key that needs a workspace (s06 list, s07 denied, error). */
export function orgNeedsWorkspace(
  list: ApiKeyOrgCapability,
): PreflightWireResponse {
  return preflightWire({
    verdict: 'needs_workspace',
    keyClass: 'org_scoped',
    workspaceList: list,
    remediation:
      list === 'denied'
        ? ['workspace_required', 'workspace_list_denied']
        : ['workspace_required'],
  });
}

/** The org key with Production picked; `usage` decides the Finish notice. */
export function orgSelected(usage: ApiKeyOrgCapability): PreflightWireResponse {
  return preflightWire({
    verdict: 'ok',
    keyClass: 'org_scoped',
    keyType: 'service_account',
    workspace: {
      mode: 'selected',
      id: PRODUCTION_WORKSPACE.id,
      name: PRODUCTION_WORKSPACE.name,
    },
    workspaceList: 'ok',
    usageReport: usage,
    // A transient usage-check error carries no fix code (R2-2).
    remediation: usage === 'denied' ? ['role_insufficient'] : [],
  });
}

/** Preflight requests the page sent, recorded from the browser side. */
export function recordPreflights(page: Page): unknown[] {
  const bodies: unknown[] = [];
  page.on('request', (request) => {
    const url = new URL(request.url());
    if (
      request.method() === 'POST' &&
      /^\/admin\/v1\/upstreams\/(preflight|[^/]+\/preflight)$/.test(
        url.pathname,
      )
    ) {
      bodies.push(request.postDataJSON());
    }
  });
  return bodies;
}
