// The connect dialog's API-key path against the real lib/api.ts transport:
// `fetch` is the only stub, so wire parsing (nested §5 codes, flat
// `invalid_input`), `meta.inlineError` and the shared Query/MutationCache toast
// rules run for real. QA ids are named in each test.

import { QueryClientProvider } from '@tanstack/react-query';
import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  within,
} from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest';

vi.mock('sonner', () => ({
  toast: { error: vi.fn(), success: vi.fn() },
}));

import { toast } from 'sonner';
import type {
  AnthropicWorkspace,
  ApiKeyAccess,
  ApiKeyClass,
  ApiKeyOrgCapability,
  ApiKeyType,
  ApiKeyWorkspace,
  PreflightWireResponse,
  RemediationCode,
  Upstream,
} from '../../../lib/api';
import { queryClient } from '../../../lib/queryClient';
import { STALE_EDITOR_MESSAGE } from '../staleRevision';
import { KEY_CHARACTERS_REASON } from './parseApiKeyPaste';
import {
  UpstreamConnectDialog,
  type UpstreamConnectTarget,
} from './UpstreamConnectDialog';

// ─── fetch stub ──────────────────────────────────────────────────────────────

interface Call {
  method: string;
  path: string;
  body: unknown;
  headers: Headers;
}
interface Reply {
  status: number;
  body: unknown;
}
let calls: Call[] = [];
let route: (call: Call) => Reply | Promise<Reply>;

const PREFLIGHT = '/admin/v1/upstreams/preflight';
const UPSTREAMS = '/admin/v1/upstreams';

function preflightCalls(): Call[] {
  return calls.filter((c) => c.method === 'POST' && c.path === PREFLIGHT);
}

/** Whether a preflight asked about a specific workspace. */
function carriesWorkspace(call: Call): boolean {
  const { body } = call;
  return Boolean(body && typeof body === 'object' && 'workspace_id' in body);
}

beforeEach(() => {
  calls = [];
  route = (call) => ({
    status: 404,
    body: { error: 'unexpected', message: `${call.method} ${call.path}` },
  });
  vi.stubGlobal(
    'fetch',
    vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const call: Call = {
        method: init?.method ?? 'GET',
        path: String(input),
        body: typeof init?.body === 'string' ? JSON.parse(init.body) : null,
        headers: new Headers(init?.headers),
      };
      calls.push(call);
      const reply = await route(call);
      return new Response(JSON.stringify(reply.body), {
        status: reply.status,
        headers: { 'content-type': 'application/json' },
      });
    }),
  );
});

afterEach(() => {
  cleanup();
  queryClient.clear();
  vi.unstubAllGlobals();
  vi.mocked(toast.error).mockReset();
  vi.mocked(toast.success).mockReset();
});

// ─── wire fixtures (plan §7 envelope) ────────────────────────────────────────

const BOUND_ID = 'wrkspc_01BoundWorkspaceZZ9q';
const PROD: AnthropicWorkspace = {
  id: 'wrkspc_01ProductionWorkspace4f2a',
  name: 'Production',
};

function wire(p: {
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
      items: workspaceList === 'ok' ? (p.items ?? [PROD]) : [],
    },
    capabilities: {
      inference: p.inference ?? 'ok',
      workspace_list: workspaceList,
      usage_report: p.usageReport ?? 'not_applicable',
      api_key_resolution: 'not_applicable',
    },
    remediation: (p.remediation ?? []).map((code) => ({ code })),
    checked_at: 1_790_000_000,
  };
}

const BOUND = wire({
  verdict: 'ok',
  keyClass: 'workspace_bound',
  workspace: { mode: 'bound', id: BOUND_ID },
  remediation: ['key_workspace_scoped_usage_unavailable'],
});
const ORG_LIST = wire({
  verdict: 'needs_workspace',
  keyClass: 'org_scoped',
  workspaceList: 'ok',
  remediation: ['workspace_required'],
});
const ORG_DENIED = wire({
  verdict: 'needs_workspace',
  keyClass: 'org_scoped',
  workspaceList: 'denied',
  remediation: ['workspace_required', 'workspace_list_denied'],
});
const ORG_SELECTED = wire({
  verdict: 'ok',
  keyClass: 'org_scoped',
  keyType: 'service_account',
  workspace: { mode: 'selected', id: PROD.id, name: PROD.name },
  workspaceList: 'ok',
  usageReport: 'ok',
});
const CUSTOM_OK = wire({
  verdict: 'ok',
  workspace: { mode: 'bound' },
  remediation: ['non_anthropic_base_url'],
});
const UNREACHABLE = wire({
  verdict: 'unreachable',
  inference: 'unverified',
  remediation: ['probe_unreachable'],
});
const FORBIDDEN = wire({
  verdict: 'rejected',
  inference: 'denied',
  remediation: ['key_forbidden'],
});
function rejected(code: RemediationCode): PreflightWireResponse {
  return wire({
    verdict: 'rejected',
    inference: 'denied',
    remediation: [code],
  });
}

function upstream(overrides: Partial<Upstream> = {}): Upstream {
  return {
    id: 'up-api-1',
    name: 'prod-key',
    kind: 'anthropic_api_key',
    enabled: true,
    base_url: null,
    api_key_access: null,
    warmup_enabled: false,
    warmup_dialect_plugin: null,
    spec_revision: 7,
    status: {
      last_apply_error: null,
      last_apply_at_unix_secs: null,
      last_warmup_at_unix_secs: null,
    },
    ...overrides,
  };
}

/** A stored org key that works and bills Production. */
const ORG_ACCESS: ApiKeyAccess = {
  verified: true,
  checked_at_unix_secs: 1_790_000_000,
  key_class: 'org_scoped',
  inference: 'ok',
  workspace: { mode: 'selected', id: PROD.id, name: PROD.name },
  workspace_list: 'ok',
  usage_report: 'ok',
  key_type: 'service_account',
  last_error: null,
};

async function pickWorkspace(dialog: HTMLElement, name: RegExp) {
  const user = userEvent.setup();
  await user.click(within(dialog).getByRole('combobox', { name: 'Workspace' }));
  await user.click(await screen.findByRole('option', { name }));
}

// ─── DOM helpers ─────────────────────────────────────────────────────────────

const KEY = 'sk-ant-api03-validKeyBody_0123456789-AbCd';
const ADMIN_KEY = 'sk-ant-admin01-adminKeyBody0123456789';
const ENTERPRISE_KEY = 'sk-ant-api01-enterpriseKeyBody0123456';
const OAT_KEY = 'sk-ant-oat01-oauthTokenBody0123456789';

function renderDialog(target: UpstreamConnectTarget) {
  const onClose = vi.fn();
  const onCreated = vi.fn();
  const tree = (next: UpstreamConnectTarget) => (
    <QueryClientProvider client={queryClient}>
      <UpstreamConnectDialog
        target={next}
        onClose={onClose}
        onCreated={onCreated}
      />
    </QueryClientProvider>
  );
  const view = render(tree(target));
  return {
    dialog: screen.getByRole('dialog'),
    onClose,
    onCreated,
    /** The parent re-rendering with new props, as a poll refresh does. */
    retarget: (next: UpstreamConnectTarget) => view.rerender(tree(next)),
  };
}

function openApiKeyStep() {
  const rendered = renderDialog({ mode: 'create' });
  fireEvent.click(
    within(rendered.dialog).getByRole('radio', { name: /Anthropic API key/ }),
  );
  fireEvent.click(
    within(rendered.dialog).getByRole('button', { name: 'Continue' }),
  );
  return rendered;
}

function keyInput(dialog: HTMLElement): HTMLInputElement {
  return within(dialog).getByPlaceholderText('sk-ant-…') as HTMLInputElement;
}

async function paste(dialog: HTMLElement, text: string) {
  await act(async () => {
    fireEvent.paste(keyInput(dialog), {
      clipboardData: { getData: () => text },
    });
  });
}

function railItems(dialog: HTMLElement): HTMLElement[] {
  return within(
    within(dialog).getByRole('list', { name: 'Steps' }),
  ).getAllByRole('listitem');
}

function railTitles(dialog: HTMLElement): string[] {
  return railItems(dialog).map(
    (li) =>
      li.querySelector('span.block.text-label')?.firstChild?.textContent ?? '',
  );
}

function currentStep(dialog: HTMLElement): string {
  const current = railItems(dialog).filter(
    (li) => li.getAttribute('aria-current') === 'step',
  );
  expect(current).toHaveLength(1);
  return (
    current[0]?.querySelector('span.block.text-label')?.firstChild
      ?.textContent ?? ''
  );
}

/** Visible primary buttons; a disabled primary renders as a secondary. */
function primaryButtons(dialog: HTMLElement): HTMLButtonElement[] {
  return within(dialog)
    .getAllByRole('button')
    .filter((b): b is HTMLButtonElement => b.classList.contains('bg-accent'));
}

function alerts(dialog: HTMLElement): HTMLElement[] {
  return within(dialog).queryAllByRole('alert');
}

/** The Key field's error, resolved through `aria-describedby` (UI-AY-06). */
function describedError(input: HTMLElement): string | null {
  expect(input.getAttribute('aria-invalid')).toBe('true');
  const id = input.getAttribute('aria-describedby') ?? '';
  const el = id ? document.getElementById(id) : null;
  expect(el?.className).toContain('text-danger-text');
  return el?.textContent ?? null;
}

/** UI-CP-01 absences: retired wording never renders anywhere. */
const RETIRED_COPY = [
  'checks it again later',
  "upstream's Settings",
  'Changeable in Settings',
  'Working',
  'Unverified',
];
function expectNoRetiredCopy(dialog: HTMLElement) {
  for (const phrase of RETIRED_COPY) {
    expect(dialog.textContent ?? '').not.toContain(phrase);
  }
}

/** UI-CP-02: a nudge names one action and one result. */
function expectOneArrow(el: HTMLElement) {
  expect((el.textContent ?? '').split('→').length - 1).toBe(1);
}

// ─── Kind step and rail ──────────────────────────────────────────────────────

describe('kind step and rail (UI-D-01, UI-D-02, UI-D-03, UI-AY-03)', () => {
  test('UI-D-01: two kind cards; choosing API key goes to Key', () => {
    const { dialog } = renderDialog({ mode: 'create' });
    const radios = within(dialog).getAllByRole('radio');
    expect(radios).toHaveLength(2);
    const subscription = within(dialog)
      .getByText('Claude subscription')
      .closest('label');
    expect(subscription?.textContent).toContain('Recommended');
    expect(railTitles(dialog)[0]).toBe('Connection');
    expect(currentStep(dialog)).toBe('Connection');
    expect(primaryButtons(dialog).map((b) => b.textContent)).toEqual([
      'Continue',
    ]);

    fireEvent.click(
      within(dialog).getByRole('radio', { name: /Anthropic API key/ }),
    );
    fireEvent.click(within(dialog).getByRole('button', { name: 'Continue' }));
    expect(railTitles(dialog)).toEqual(['Connection', 'Key', 'Name & save']);
    expect(currentStep(dialog)).toBe('Key');
  });

  test('UI-D-02 / UI-AY-03: exactly one current step, done and upcoming announced', () => {
    const { dialog } = openApiKeyStep();
    const ol = within(dialog).getByRole('list', { name: 'Steps' });
    expect(ol.tagName).toBe('OL');
    expect(ol.querySelectorAll('[aria-current="step"]')).toHaveLength(1);
    const [done, current, upcoming] = railItems(dialog);
    expect(done?.textContent).toContain('(done)');
    expect(current?.textContent).toContain('(current)');
    expect(upcoming?.textContent).toContain('(upcoming)');
    expect(current?.querySelector('.bg-selected')).not.toBeNull();
    expect(done?.querySelector('.bg-selected')).toBeNull();
    // The narrow-screen step line is re-read whole on every change, so a
    // screen reader says "Step 2 of 3 · Key", not just the changed parts.
    const line = within(dialog).getByText(/^Step \d+ of \d+$/).parentElement;
    expect(line?.getAttribute('aria-live')).toBe('polite');
    expect(line?.getAttribute('aria-atomic')).toBe('true');
    expect(line?.textContent).toMatch(/^Step \d+ of \d+ · \S/);
  });

  test('UI-D-03: rail shape per branch', async () => {
    const { dialog } = openApiKeyStep();
    // Guide is inserted before Key.
    fireEvent.click(
      within(dialog).getByRole('button', { name: "Don't have a key?" }),
    );
    expect(railTitles(dialog)).toEqual([
      'Connection',
      'Create the key',
      'Key',
      'Name & save',
    ]);
    expect(currentStep(dialog)).toBe('Create the key');
    fireEvent.click(
      within(dialog).getByRole('button', { name: 'Paste a key instead' }),
    );
    expect(currentStep(dialog)).toBe('Key');
    cleanup();

    // Org-scoped: Workspace joins the rail once the key asks for one.
    route = () => ({ status: 200, body: ORG_LIST });
    const org = openApiKeyStep();
    expect(railTitles(org.dialog)).toEqual([
      'Connection',
      'Key',
      'Name & save',
    ]);
    await paste(org.dialog, KEY);
    await within(org.dialog).findByText('Pick a workspace');
    expect(railTitles(org.dialog)).toEqual([
      'Connection',
      'Key',
      'Workspace',
      'Name & save',
    ]);
    expect(currentStep(org.dialog)).toBe('Workspace');
    cleanup();

    // Replace: no Connection, the last step reads Save.
    const replace = renderDialog({ mode: 'replace', upstream: upstream() });
    expect(railTitles(replace.dialog)).toEqual(['Key', 'Save']);
    expect(currentStep(replace.dialog)).toBe('Key');
  });
});

// ─── Key step ────────────────────────────────────────────────────────────────

describe('key step (UI-D-06…11, UI-D-22, UI-D-27, UI-D-29, UI-D-31, UI-D-32)', () => {
  test('UI-D-06: a clean paste checks itself; typing waits for Check key / Enter', async () => {
    const pending = Promise.withResolvers<Reply>();
    route = () => pending.promise;
    const { dialog } = openApiKeyStep();
    await paste(dialog, `  ${KEY}\n`);
    expect(preflightCalls()).toHaveLength(1);
    expect(preflightCalls()[0]?.body).toEqual({ api_key_value: KEY });
    expect(
      within(dialog).getByRole('button', { name: 'Checking…' }),
    ).toBeTruthy();
    await act(async () => {
      pending.resolve({ status: 200, body: BOUND });
    });
    await within(dialog).findByText('Key verified');
    cleanup();

    calls = [];
    route = () => ({ status: 200, body: BOUND });
    const typed = openApiKeyStep();
    fireEvent.change(keyInput(typed.dialog), { target: { value: KEY } });
    expect(preflightCalls()).toHaveLength(0);
    await act(async () => {
      fireEvent.keyDown(keyInput(typed.dialog), { key: 'Enter' });
    });
    expect(preflightCalls()).toHaveLength(1);
    await within(typed.dialog).findByText('Key verified');
  });

  test('UI-AY-01: the key field keeps focus through a check and its rejection', async () => {
    const pending = Promise.withResolvers<Reply>();
    route = () => pending.promise;
    const { dialog } = openApiKeyStep();
    const input = keyInput(dialog);
    input.focus();
    await paste(dialog, KEY);
    expect(preflightCalls()).toHaveLength(1);
    // Busy, not disabled: a disabled focused field drops focus to <body>.
    expect(input.disabled).toBe(false);
    expect(input.readOnly).toBe(true);
    expect(input.getAttribute('aria-busy')).toBe('true');
    expect(document.activeElement).toBe(input);
    await act(async () => {
      pending.resolve({ status: 200, body: rejected('key_invalid') });
    });
    expect(alerts(dialog)).toHaveLength(1);
    expect(keyInput(dialog)).toBe(input);
    expect(input.readOnly).toBe(false);
    expect(input.hasAttribute('aria-busy')).toBe(false);
    expect(document.activeElement).toBe(input);
  });

  test.each([
    [
      'UI-D-07 admin',
      ADMIN_KEY,
      "This is an Admin key — it manages the org and can't call models.",
      'Use a standard API key → get inference. How to create a key',
    ],
    [
      'UI-D-08 enterprise',
      ENTERPRISE_KEY,
      "This is an Enterprise management key — it can't call models.",
      'Use a Console API key → paste it here. How to create a key',
    ],
  ])(
    '%s: gated before any request, one alert, Guide in the notice',
    async (_, key, fieldError, notice) => {
      const { dialog } = openApiKeyStep();
      await paste(dialog, key);
      expect(preflightCalls()).toHaveLength(0);
      expect(keyInput(dialog).value).toBe(key);
      expect(describedError(keyInput(dialog))).toBe(fieldError);
      const [alert, ...rest] = alerts(dialog);
      expect(rest).toHaveLength(0);
      expect(alert?.textContent).toBe(notice);
      expectOneArrow(alert as HTMLElement);
      expect(
        within(dialog).getByRole('button', { name: 'Check key' }),
      ).toHaveProperty('disabled', true);
      expect(
        within(dialog).queryByRole('button', { name: "Don't have a key?" }),
      ).toBeNull();
      expectNoRetiredCopy(dialog);
      fireEvent.click(
        within(alert as HTMLElement).getByRole('button', {
          name: 'How to create a key',
        }),
      );
      expect(currentStep(dialog)).toBe('Create the key');
    },
  );

  test('UI-D-09: an OAuth token swaps the primary for Sign in with Claude', async () => {
    route = (call) =>
      call.path === '/admin/v1/oauth/draft/start'
        ? {
            status: 200,
            body: {
              authorize_url: 'https://claude.ai/oauth/authorize?state=s',
              state_token: 'draft-state',
            },
          }
        : { status: 404, body: {} };
    const { dialog } = openApiKeyStep();
    await paste(dialog, OAT_KEY);
    expect(preflightCalls()).toHaveLength(0);
    expect(describedError(keyInput(dialog))).toBe(
      'That is an OAuth token, not an API key — it expires and cc-lb cannot refresh it.',
    );
    const [alert, ...rest] = alerts(dialog);
    expect(rest).toHaveLength(0);
    expect(alert?.textContent).toBe(
      "Sign in with Claude → get a managed session.Workload identity federation tokens won't work either.",
    );
    expect(
      within(dialog).queryByRole('button', { name: 'Check key' }),
    ).toBeNull();
    expect(primaryButtons(dialog).map((b) => b.textContent)).toEqual([
      'Sign in with Claude instead',
    ]);
    await act(async () => {
      fireEvent.click(
        within(dialog).getByRole('button', {
          name: 'Sign in with Claude instead',
        }),
      );
    });
    expect(railTitles(dialog)).toEqual([
      'Connection',
      'Sign in',
      'Paste code',
      'Name & save',
    ]);
    expect(currentStep(dialog)).toBe('Sign in');
  });

  test('UI-D-10: s01–s03 share one danger field error and one notice tone', async () => {
    const tones: string[] = [];
    for (const key of [ADMIN_KEY, ENTERPRISE_KEY, OAT_KEY]) {
      const { dialog } = openApiKeyStep();
      await paste(dialog, key);
      describedError(keyInput(dialog));
      tones.push(alerts(dialog)[0]?.getAttribute('data-tone') ?? '');
      cleanup();
    }
    expect(new Set(tones).size).toBe(1);
  });

  test('UI-D-11: exactly one help path per key state', async () => {
    const helpPaths = (dialog: HTMLElement) => ({
      dontHave:
        within(dialog).queryByRole('button', { name: "Don't have a key?" }) !==
        null,
      howTo:
        within(dialog).queryByRole('button', {
          name: 'How to create a key',
        }) !== null,
    });

    const empty = openApiKeyStep();
    expect(helpPaths(empty.dialog)).toEqual({ dontHave: true, howTo: false });
    cleanup();

    for (const key of [ADMIN_KEY, ENTERPRISE_KEY]) {
      const gated = openApiKeyStep();
      await paste(gated.dialog, key);
      expect(helpPaths(gated.dialog)).toEqual({ dontHave: false, howTo: true });
      cleanup();
    }

    const oat = openApiKeyStep();
    await paste(oat.dialog, OAT_KEY);
    expect(helpPaths(oat.dialog)).toEqual({ dontHave: true, howTo: false });
    cleanup();

    route = () => ({ status: 200, body: rejected('key_invalid') });
    const invalid = openApiKeyStep();
    await paste(invalid.dialog, KEY);
    await within(invalid.dialog).findByText(
      "Anthropic rejected this key — check for a typo, or it's expired or deleted.",
    );
    expect(helpPaths(invalid.dialog)).toEqual({ dontHave: false, howTo: true });
    cleanup();

    const custom = openApiKeyStep();
    fireEvent.click(
      within(custom.dialog).getByRole('button', {
        name: 'Use a custom endpoint',
      }),
    );
    fireEvent.change(
      within(custom.dialog).getByRole('textbox', { name: /Base URL/ }),
      {
        target: { value: 'https://proxy.example.com' },
      },
    );
    await paste(custom.dialog, KEY);
    await within(custom.dialog).findByText('Your endpoint rejected this key.');
    expect(helpPaths(custom.dialog)).toEqual({ dontHave: false, howTo: false });
    cleanup();

    route = () => ({ status: 200, body: UNREACHABLE });
    const down = openApiKeyStep();
    await paste(down.dialog, KEY);
    await within(down.dialog).findByText(/Couldn't reach Anthropic/);
    expect(helpPaths(down.dialog)).toEqual({ dontHave: true, howTo: false });
  });

  test('UI-D-27 / UI-AY-04 / UI-CP-08: custom endpoint disclosure', async () => {
    route = () => ({ status: 200, body: rejected('key_invalid') });
    const { dialog } = openApiKeyStep();
    // Principle 7: Base URL only behind the disclosure in the dialog.
    expect(
      within(dialog).queryByRole('textbox', { name: /Base URL/ }),
    ).toBeNull();
    const opener = within(dialog).getByRole('button', {
      name: 'Use a custom endpoint',
    });
    expect(opener.getAttribute('aria-expanded')).toBe('false');
    const controls = opener.getAttribute('aria-controls');
    expect(controls).toBeTruthy();

    fireEvent.click(opener);
    const field = within(dialog).getByRole('textbox', {
      name: /Base URL/,
    }) as HTMLInputElement;
    expect(field.id).toBe(controls);
    expect(field.value).toBe('');
    expect(document.activeElement).toBe(field);
    const fold = within(dialog).getByRole('button', {
      name: 'Use the default endpoint',
    });
    expect(fold.getAttribute('aria-expanded')).toBe('true');
    expect(
      document.getElementById(fold.getAttribute('aria-controls') ?? ''),
    ).toBe(field);

    // A rejection on the custom host, then a URL edit clears it.
    fireEvent.change(field, { target: { value: 'https://proxy.example.com' } });
    await paste(dialog, KEY);
    expect(preflightCalls()[0]?.body).toEqual({
      api_key_value: KEY,
      base_url: 'https://proxy.example.com',
    });
    const notice = await within(dialog).findByRole('alert');
    expect(notice.textContent).toBe('Check the key and Base URL → Check key.');
    fireEvent.change(field, {
      target: { value: 'https://proxy2.example.com' },
    });
    expect(alerts(dialog)).toHaveLength(0);

    fireEvent.click(fold);
    const reopened = within(dialog).getByRole('button', {
      name: 'Use a custom endpoint',
    });
    expect(document.activeElement).toBe(reopened);
    expect(
      within(dialog).queryByRole('textbox', { name: /Base URL/ }),
    ).toBeNull();
    cleanup();

    // Hidden in replace mode.
    const replace = renderDialog({ mode: 'replace', upstream: upstream() });
    expect(
      within(replace.dialog).queryByRole('button', {
        name: 'Use a custom endpoint',
      }),
    ).toBeNull();
  });

  test('UI-D-28: a custom endpoint skips the client gate; the server verdict renders the same', async () => {
    route = () => ({ status: 200, body: rejected('key_admin') });
    const { dialog } = openApiKeyStep();
    fireEvent.click(
      within(dialog).getByRole('button', { name: 'Use a custom endpoint' }),
    );
    fireEvent.change(
      within(dialog).getByRole('textbox', { name: /Base URL/ }),
      {
        target: { value: 'http://127.0.0.1:9' },
      },
    );
    await paste(dialog, ADMIN_KEY);
    expect(preflightCalls()).toHaveLength(1);
    await within(dialog).findByRole('alert');
    expect(describedError(keyInput(dialog))).toBe(
      "This is an Admin key — it manages the org and can't call models.",
    );
    expect(alerts(dialog)[0]?.textContent).toContain(
      'Use a standard API key → get inference.',
    );
  });

  test('an Anthropic near-miss URL is a custom endpoint the check sends', async () => {
    route = () => ({ status: 200, body: rejected('key_invalid') });
    for (const nearMiss of [
      'https://api.anthropic.com/v1/',
      'https://api.anthropic.com/?x=1',
      'https://api.anthropic.com/#frag',
    ]) {
      const { dialog } = openApiKeyStep();
      fireEvent.click(
        within(dialog).getByRole('button', { name: 'Use a custom endpoint' }),
      );
      fireEvent.change(
        within(dialog).getByRole('textbox', { name: /Base URL/ }),
        { target: { value: nearMiss } },
      );
      await paste(dialog, KEY);
      expect(preflightCalls().at(-1)?.body).toEqual({
        api_key_value: KEY,
        base_url: nearMiss,
      });
      cleanup();
    }
  });

  test('UI-D-31: a hidden character is a field error and never a request', async () => {
    for (const hidden of ['\u200B', '\u00A0']) {
      const { dialog } = openApiKeyStep();
      await paste(dialog, `sk-ant-api03-ab${hidden}cd0123456789`);
      expect(describedError(keyInput(dialog))).toBe(KEY_CHARACTERS_REASON);
      expect(alerts(dialog)).toHaveLength(0);
      expect(
        within(dialog).getByRole('button', { name: 'Check key' }),
      ).toHaveProperty('disabled', true);
      cleanup();
    }
    expect(preflightCalls()).toHaveLength(0);
  });

  test('UI-D-32: server invalid_input on the key renders as the Key field error', async () => {
    route = () => ({
      status: 400,
      body: {
        error: 'invalid_input',
        field: 'api_key_value',
        reason: 'API keys contain no spaces or line breaks',
      },
    });
    const { dialog } = openApiKeyStep();
    await paste(dialog, KEY);
    await within(dialog).findByText(
      'API keys contain no spaces or line breaks',
    );
    expect(describedError(keyInput(dialog))).toBe(
      'API keys contain no spaces or line breaks',
    );
    expect(alerts(dialog)).toHaveLength(0);
    expect(
      within(dialog).queryByText(/cc-lb couldn't check the key/),
    ).toBeNull();
    expect(
      within(dialog).queryByRole('button', { name: 'Try again' }),
    ).toBeNull();
    expect(toast.error).not.toHaveBeenCalled();
  });

  test('UI-D-23 / UI-AY-05: an unknown workspace is one warning alert in manual mode', async () => {
    const missing = 'wrkspc_01MissingWorkspace0000';
    route = (call) => ({
      status: 200,
      body: carriesWorkspace(call)
        ? wire({
            verdict: 'needs_workspace',
            keyClass: 'org_scoped',
            workspaceList: 'denied',
            remediation: ['workspace_not_found'],
          })
        : ORG_DENIED,
    });
    const { dialog } = openApiKeyStep();
    await paste(dialog, KEY);
    await within(dialog).findByText('Pick a workspace');
    const input = within(dialog).getByPlaceholderText('wrkspc_…');
    fireEvent.change(input, { target: { value: missing } });
    await act(async () => {
      fireEvent.click(within(dialog).getByRole('button', { name: 'Continue' }));
    });
    const alert = await within(dialog).findByRole('alert');
    expect(alerts(dialog)).toHaveLength(1);
    expect(alert.getAttribute('data-tone')).toBe('warning');
    expect(alert.textContent).toBe(
      `This key can't see workspace ${missing} — check the id, or the key's account lacks access.`,
    );
    expect(input.getAttribute('aria-invalid')).toBe('true');
    expect((input as HTMLInputElement).value).toBe(missing);
    expect(toast.error).not.toHaveBeenCalled();
  });

  test('UI-D-22: a malformed workspace id is caught without a request', async () => {
    route = () => ({ status: 200, body: ORG_DENIED });
    const { dialog } = openApiKeyStep();
    await paste(dialog, KEY);
    await within(dialog).findByText('Pick a workspace');
    const input = within(dialog).getByPlaceholderText('wrkspc_…');
    fireEvent.change(input, { target: { value: 'ws_123' } });
    fireEvent.click(within(dialog).getByRole('button', { name: 'Continue' }));
    expect(preflightCalls()).toHaveLength(1);
    expect((input as HTMLInputElement).value).toBe('ws_123');
    // UI-AY-06 on the workspace input.
    expect(describedError(input)).toBe('Workspace IDs start with wrkspc_.');
    // WorkspaceIdHelp line 2 is a nudge (UI-CP-02).
    const help = within(dialog).getByText(/Open Workspaces in Console/)
      .parentElement as HTMLElement;
    expectOneArrow(help);
    expect(
      within(dialog).getByText(
        "This key's account lacks the role to list workspaces.",
      ),
    ).toBeTruthy();
  });
});

// ─── Notices, alerts, Finish ─────────────────────────────────────────────────

describe('notices and Finish (UI-AY-05, UI-CP-01/02/05/07)', () => {
  test('UI-AY-05: forbidden and unreachable are one alert; request failure is one alert', async () => {
    route = () => ({ status: 200, body: FORBIDDEN });
    const forbidden = openApiKeyStep();
    await paste(forbidden.dialog, KEY);
    const f = await within(forbidden.dialog).findByRole('alert');
    expect(alerts(forbidden.dialog)).toHaveLength(1);
    expect(f.textContent).toBe(
      "Valid key, but Anthropic won't let it count tokens. Save anyway → add it unverified.",
    );
    expect(primaryButtons(forbidden.dialog).map((b) => b.textContent)).toEqual([
      'Check key',
    ]);
    fireEvent.click(
      within(forbidden.dialog).getByRole('button', { name: 'Save anyway' }),
    );
    expect(
      within(forbidden.dialog).getByText('Key not verified', {
        selector: 'h3',
      }),
    ).toBeTruthy();
    expect(
      within(forbidden.dialog).getByText(
        "Anthropic accepted the key but wouldn't let it count tokens. The upstream shows Key not verified until a re-check passes.",
      ),
    ).toBeTruthy();
    expectNoRetiredCopy(forbidden.dialog);
    cleanup();

    route = () => ({ status: 200, body: UNREACHABLE });
    const down = openApiKeyStep();
    await paste(down.dialog, KEY);
    const d = await within(down.dialog).findByRole('alert');
    expect(alerts(down.dialog)).toHaveLength(1);
    expect(d.textContent).toBe(
      "Couldn't reach Anthropic. Try again → check the key.",
    );
    expect(primaryButtons(down.dialog).map((b) => b.textContent)).toEqual([
      'Try again',
    ]);
    fireEvent.click(
      within(down.dialog).getByRole('button', {
        name: 'Save without checking',
      }),
    );
    // D-UX4: no promise that cc-lb checks it later.
    expect(
      within(down.dialog).getByText(
        'The upstream shows Key not verified until a Re-check passes.',
      ),
    ).toBeTruthy();
    expectNoRetiredCopy(down.dialog);
    cleanup();

    route = () => ({
      status: 503,
      body: { error: 'storage_unavailable', message: 'database is down' },
    });
    const broken = openApiKeyStep();
    await paste(broken.dialog, KEY);
    const b = await within(broken.dialog).findByRole('alert');
    expect(alerts(broken.dialog)).toHaveLength(1);
    expect(b.textContent).toContain(
      "cc-lb couldn't check the key: database is down",
    );
    expect(within(b).getByRole('button', { name: 'Try again' })).toBeTruthy();
    expect(toast.error).not.toHaveBeenCalled();
  });

  test('UI-D-15 copy: an unreachable custom endpoint names the endpoint', async () => {
    route = () => ({ status: 200, body: UNREACHABLE });
    const { dialog } = openApiKeyStep();
    fireEvent.click(
      within(dialog).getByRole('button', { name: 'Use a custom endpoint' }),
    );
    fireEvent.change(
      within(dialog).getByRole('textbox', { name: /Base URL/ }),
      {
        target: { value: 'http://127.0.0.1:9' },
      },
    );
    await paste(dialog, KEY);
    const alert = await within(dialog).findByRole('alert');
    expect(alert.textContent).toBe(
      "Couldn't reach your endpoint. Try again → check the key.",
    );
  });

  test('UI-CP-01 / UI-CP-07: Finish facts for a bound key', async () => {
    route = () => ({ status: 200, body: BOUND });
    const { dialog } = openApiKeyStep();
    await paste(dialog, KEY);
    await within(dialog).findByText('Key verified');
    expect(
      within(dialog).getByText('Check what it can do, then name the upstream.'),
    ).toBeTruthy();
    const inference = within(dialog).getByText('Key accepted');
    // A StatusBadge (dot + phrase), never plain text.
    expect(
      inference.parentElement?.querySelector('.status-dot.ok'),
    ).not.toBeNull();
    expect(within(dialog).getByText('Fixed by the key')).toBeTruthy();
    expect(within(dialog).getByText('Estimates only')).toBeTruthy();
    const fix = within(dialog).getByText(
      'Replace it with an All workspaces key → get reported usage.',
    );
    expectOneArrow(fix);
    // Steady-state limit: a status, not an alert.
    expect(alerts(dialog)).toHaveLength(0);
    expect(primaryButtons(dialog).map((b) => b.textContent)).toEqual([
      'Add upstream',
    ]);
    expectNoRetiredCopy(dialog);
  });

  test('UI-CP-01: Finish facts for an org key with a picked workspace', async () => {
    route = (call) => ({
      status: 200,
      body: carriesWorkspace(call) ? ORG_SELECTED : ORG_LIST,
    });
    const { dialog } = openApiKeyStep();
    await paste(dialog, KEY);
    await within(dialog).findByText('Pick a workspace');
    expect(
      within(dialog).getByText('You can change this later in Access.'),
    ).toBeTruthy();
    // No silent default: the first listed workspace is not preselected.
    expect(
      within(dialog).getByRole('button', { name: 'Continue' }),
    ).toHaveProperty('disabled', true);
    await pickWorkspace(dialog, /Production/);
    expect(primaryButtons(dialog).map((b) => b.textContent)).toEqual([
      'Continue',
    ]);
    await act(async () => {
      fireEvent.click(within(dialog).getByRole('button', { name: 'Continue' }));
    });
    await within(dialog).findByText('Key verified');
    expect(preflightCalls()[1]?.body).toEqual({
      api_key_value: KEY,
      workspace_id: PROD.id,
    });
    expect(within(dialog).getByText('Production')).toBeTruthy();
    expect(within(dialog).getByText('Change it later in Access')).toBeTruthy();
    expect(within(dialog).getByText('Reported by Anthropic')).toBeTruthy();
    expect(within(dialog).getByText('Service account')).toBeTruthy();
    expect(
      (
        within(dialog).getByRole('textbox', {
          name: /Upstream name/,
        }) as HTMLInputElement
      ).value,
    ).toBe('Production API key');
    expectNoRetiredCopy(dialog);
  });

  test('UI-CP-01: Finish on a custom endpoint keeps estimates', async () => {
    route = () => ({ status: 200, body: CUSTOM_OK });
    const { dialog } = openApiKeyStep();
    fireEvent.click(
      within(dialog).getByRole('button', { name: 'Use a custom endpoint' }),
    );
    fireEvent.change(
      within(dialog).getByRole('textbox', { name: /Base URL/ }),
      {
        target: { value: 'https://proxy.example.com' },
      },
    );
    await paste(dialog, KEY);
    await within(dialog).findByText('Key verified');
    expect(
      within(dialog).getByText(
        'Org checks (workspaces, usage reporting) need api.anthropic.com — usage stays estimated.',
      ),
    ).toBeTruthy();
  });

  test('UI-D-30: Cancel abandons without creating anything', async () => {
    route = () => ({ status: 200, body: BOUND });
    const { dialog, onClose } = openApiKeyStep();
    await paste(dialog, KEY);
    await within(dialog).findByText('Key verified');
    fireEvent.click(within(dialog).getByRole('button', { name: 'Cancel' }));
    expect(onClose).toHaveBeenCalledTimes(1);
    expect(calls.filter((c) => c.path === UPSTREAMS)).toHaveLength(0);
  });
});

// ─── Save ────────────────────────────────────────────────────────────────────

describe('save (UI-D-33, UI-R-03, D-UX1 inlineError)', () => {
  test('UI-D-33: a key revoked between Check and Add goes back to Key with one alert and no toast', async () => {
    route = (call) =>
      call.path === PREFLIGHT
        ? { status: 200, body: BOUND }
        : {
            status: 400,
            body: {
              error: {
                code: 'key_invalid',
                message: 'Anthropic answered 401 — invalid x-api-key.',
              },
            },
          };
    const { dialog, onCreated } = openApiKeyStep();
    await paste(dialog, KEY);
    await within(dialog).findByText('Key verified');
    await act(async () => {
      fireEvent.click(
        within(dialog).getByRole('button', { name: 'Add upstream' }),
      );
    });
    await within(dialog).findByText(
      "Anthropic rejected this key — check for a typo, or it's expired or deleted.",
    );
    expect(currentStep(dialog)).toBe('Key');
    expect(alerts(dialog)).toHaveLength(1);
    expect(toast.error).not.toHaveBeenCalled();
    expect(onCreated).not.toHaveBeenCalled();
    const create = calls.filter(
      (c) => c.method === 'POST' && c.path === UPSTREAMS,
    );
    expect(create).toHaveLength(1);
    expect(create[0]?.body).toEqual({
      name: 'Anthropic API key',
      kind: 'anthropic_api_key',
      base_url: null,
      api_key_value: KEY,
      workspace_id: null,
    });
  });

  test('UI-D-33: a non-§5 save failure is one danger alert on the step, no toast', async () => {
    route = (call) =>
      call.path === PREFLIGHT
        ? { status: 200, body: BOUND }
        : {
            status: 409,
            body: { error: 'stale_revision', message: 'stale revision' },
          };
    const { dialog } = renderDialog({ mode: 'replace', upstream: upstream() });
    await paste(dialog, KEY);
    await within(dialog).findByText('Key verified');
    await act(async () => {
      fireEvent.click(within(dialog).getByRole('button', { name: 'Save key' }));
    });
    const alert = await within(dialog).findByRole('alert');
    expect(alerts(dialog)).toHaveLength(1);
    expect(alert.getAttribute('data-tone')).toBe('danger');
    expect(alert.textContent).toBe(STALE_EDITOR_MESSAGE);
    expect(currentStep(dialog)).toBe('Save');
    expect(toast.error).not.toHaveBeenCalled();
  });

  test('a poll refresh to a newer revision does not let Save overwrite it', async () => {
    // The server's optimistic lock: only the current revision is accepted.
    let current = 7;
    route = (call) => {
      if (call.path === PREFLIGHT) return { status: 200, body: BOUND };
      return call.headers.get('if-match') === `W/"${current}"`
        ? { status: 200, body: upstream({ spec_revision: current + 1 }) }
        : {
            status: 409,
            body: { error: 'stale_revision', message: 'stale revision' },
          };
    };
    const { dialog, onClose, retarget } = renderDialog({
      mode: 'replace',
      upstream: upstream(),
    });
    await paste(dialog, KEY);
    await within(dialog).findByText('Key verified');

    // Another admin replaces the key; the pane's poll hands the open dialog
    // the newer upstream.
    current = 8;
    retarget({ mode: 'replace', upstream: upstream({ spec_revision: 8 }) });

    await act(async () => {
      fireEvent.click(within(dialog).getByRole('button', { name: 'Save key' }));
    });
    const put = calls.filter((c) => c.method === 'PUT');
    expect(put).toHaveLength(1);
    expect(put[0]?.headers.get('if-match')).toBe('W/"7"');
    const alert = await within(dialog).findByRole('alert');
    expect(alert.textContent).toBe(STALE_EDITOR_MESSAGE);
    // The draft stays on the Save step; nothing toasts or closes.
    expect(currentStep(dialog)).toBe('Save');
    expect(
      within(dialog).getByRole('button', { name: 'Save key' }),
    ).toBeTruthy();
    expect(toast.error).not.toHaveBeenCalled();
    expect(toast.success).not.toHaveBeenCalled();
    expect(onClose).not.toHaveBeenCalled();
  });

  test('UI-R-03: replacing with a bound key clears the stored workspace, under If-Match', async () => {
    route = (call) =>
      call.path === PREFLIGHT
        ? { status: 200, body: BOUND }
        : { status: 200, body: upstream({ spec_revision: 8 }) };
    const { dialog, onClose } = renderDialog({
      mode: 'replace',
      upstream: upstream(),
    });
    expect(within(dialog).getByText('Paste the new key')).toBeTruthy();
    await paste(dialog, KEY);
    await within(dialog).findByText('Key verified');
    expect(
      within(dialog).getByText('Check what it can do, then save the key.'),
    ).toBeTruthy();
    // Replacing is already the fix: no "Replace it with…" nudge in here.
    expect(within(dialog).getByText('Estimates only')).toBeTruthy();
    expect(within(dialog).queryByText(/All workspaces key/)).toBeNull();
    expect(primaryButtons(dialog).map((b) => b.textContent)).toEqual([
      'Save key',
    ]);
    await act(async () => {
      fireEvent.click(within(dialog).getByRole('button', { name: 'Save key' }));
    });
    const put = calls.filter((c) => c.method === 'PUT');
    expect(put).toHaveLength(1);
    expect(put[0]?.path).toBe('/admin/v1/upstreams/up-api-1');
    expect(put[0]?.headers.get('if-match')).toBe('W/"7"');
    expect(put[0]?.body).toEqual({
      api_key_value: KEY,
      clear_workspace_id: true,
    });
    expect(toast.success).toHaveBeenCalledWith('Key replaced on prod-key');
    expect(onClose).toHaveBeenCalledTimes(1);
    expect(toast.error).not.toHaveBeenCalled();
  });

  test('UI-R-03: an org key replace carries the picked workspace', async () => {
    route = (call) =>
      call.path === PREFLIGHT
        ? {
            status: 200,
            body: carriesWorkspace(call) ? ORG_SELECTED : ORG_LIST,
          }
        : { status: 200, body: upstream({ spec_revision: 8 }) };
    const { dialog } = renderDialog({
      mode: 'replace',
      upstream: upstream({ api_key_access: ORG_ACCESS }),
    });
    await paste(dialog, KEY);
    await within(dialog).findByText('Pick a workspace');
    // The upstream's current workspace is the starting choice.
    expect(
      within(dialog).getByRole('combobox', { name: 'Workspace' }).textContent,
    ).toContain('Production');
    await act(async () => {
      fireEvent.click(within(dialog).getByRole('button', { name: 'Continue' }));
    });
    await within(dialog).findByText('Key verified');
    await act(async () => {
      fireEvent.click(within(dialog).getByRole('button', { name: 'Save key' }));
    });
    const put = calls.filter((c) => c.method === 'PUT');
    expect(put[0]?.body).toEqual({ api_key_value: KEY, workspace_id: PROD.id });
  });

  test('UI-R-03: replacing a bound key with an org key asks for a workspace', async () => {
    const staging = { id: 'wrkspc_01StagingWorkspace0b0b', name: 'Staging' };
    route = (call) =>
      call.path === PREFLIGHT
        ? {
            status: 200,
            body: carriesWorkspace(call)
              ? wire({
                  verdict: 'ok',
                  keyClass: 'org_scoped',
                  workspace: { mode: 'selected', ...staging },
                  workspaceList: 'ok',
                  items: [PROD, staging],
                  usageReport: 'ok',
                })
              : wire({
                  verdict: 'needs_workspace',
                  keyClass: 'org_scoped',
                  workspaceList: 'ok',
                  items: [PROD, staging],
                  remediation: ['workspace_required'],
                }),
          }
        : { status: 200, body: upstream({ spec_revision: 8 }) };
    const { dialog } = renderDialog({
      mode: 'replace',
      upstream: upstream({
        api_key_access: {
          ...ORG_ACCESS,
          key_class: 'workspace_bound',
          workspace: { mode: 'bound', id: BOUND_ID },
        },
      }),
    });
    await paste(dialog, KEY);
    await within(dialog).findByText('Pick a workspace');
    const continueButton = within(dialog).getByRole('button', {
      name: 'Continue',
    });
    expect(continueButton).toHaveProperty('disabled', true);
    await pickWorkspace(dialog, /Staging/);
    await act(async () => {
      fireEvent.click(continueButton);
    });
    await within(dialog).findByText('Key verified');
    expect(preflightCalls()[1]?.body).toEqual({
      api_key_value: KEY,
      workspace_id: staging.id,
    });
  });

  test('Replace promises the current key keeps working only when it does', () => {
    const working = renderDialog({
      mode: 'replace',
      upstream: upstream({ api_key_access: ORG_ACCESS }),
    });
    expect(
      within(working.dialog).getByText(
        'The current key keeps working until you save.',
      ),
    ).toBeTruthy();
    cleanup();

    for (const access of [
      null,
      { ...ORG_ACCESS, verified: false, inference: 'denied' as const },
    ]) {
      const { dialog } = renderDialog({
        mode: 'replace',
        upstream: upstream({ api_key_access: access }),
      });
      expect(
        within(dialog).getByText('Requests use the new key once you save.'),
      ).toBeTruthy();
      expect(within(dialog).queryByText(/keeps working/)).toBeNull();
      cleanup();
    }
  });
});
