import type { UseMutationResult, UseQueryResult } from '@tanstack/react-query';
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { useState } from 'react';
import { toast } from 'sonner';
import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest';
import {
  ApiError,
  type ApiKeyAccess,
  type PreflightResponse,
} from '../../lib/api';
import type {
  UpdateUpstreamRequest,
  Upstream,
  UpstreamWorkspacesResult,
} from '../../lib/queries';
import * as queries from '../../lib/queries';
import {
  failedMutation,
  failedQuery,
  idleMutation,
  mutationContext,
  pendingMutation,
  pendingQuery,
  successQuery,
} from '../../lib/test-utils/hook-results';
import { ApiKeyAccessSection } from './ApiKeyAccessSection';

vi.mock('../../lib/queries', async () => {
  const actual = (await vi.importActual('../../lib/queries')) as typeof queries;
  return {
    ...actual,
    useRecheckApiKey: vi.fn(),
    useUpdateUpstream: vi.fn(),
    useUpstreamWorkspaces: vi.fn(),
  };
});

const CHECKED_AT = Math.floor(Date.now() / 1000) - 3 * 3600;

const FULL: ApiKeyAccess = {
  verified: true,
  checked_at_unix_secs: CHECKED_AT,
  key_class: 'org_scoped',
  inference: 'ok',
  workspace: {
    mode: 'selected',
    id: 'wrkspc_01PreviewProd7c1e',
    name: 'Production',
  },
  workspace_list: 'ok',
  usage_report: 'ok',
  key_type: 'service_account',
  last_error: null,
};

function upstreamWith(
  access: ApiKeyAccess | null,
  baseUrl: string | null = null,
): Upstream {
  return {
    id: 'upstream-api-key',
    name: 'API Key Primary',
    kind: 'anthropic_api_key',
    enabled: true,
    spec_revision: 7,
    base_url: baseUrl,
    api_key_access: access,
    warmup_enabled: false,
    warmup_dialect_plugin: null,
    status: {
      last_apply_error: null,
      last_apply_at_unix_secs: null,
      last_warmup_at_unix_secs: null,
    },
  };
}

// The hooks' result contracts; the mocks below must stay assignable to them.
type UpdateVariables = {
  id: string;
  body: UpdateUpstreamRequest;
  spec_revision: number;
};
type UpdateMutation = UseMutationResult<Upstream, Error, UpdateVariables>;
type RecheckMutation = UseMutationResult<PreflightResponse, Error, string>;

/** A `mutate` that settles with `updated` as the server's answer. */
const succeedWith =
  (updated: Upstream): UpdateMutation['mutate'] =>
  (vars, options) =>
    options?.onSuccess?.(updated, vars, undefined, mutationContext);

/** A `mutate` that fails with `error`. */
const failWith =
  (error: Error): UpdateMutation['mutate'] =>
  (vars, options) =>
    options?.onError?.(error, vars, undefined, mutationContext);

const recheckMutate = vi.fn<RecheckMutation['mutate']>();
const onReplace = vi.fn();

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(queries.useRecheckApiKey).mockReturnValue(
    idleMutation({ mutate: recheckMutate }),
  );
  vi.mocked(queries.useUpdateUpstream).mockReturnValue(idleMutation());
  vi.mocked(queries.useUpstreamWorkspaces).mockReturnValue(pendingQuery());
});

afterEach(cleanup);

function renderAccess(
  access: ApiKeyAccess | null,
  opts: { baseUrl?: string | null; stopped?: boolean } = {},
) {
  render(
    <ApiKeyAccessSection
      upstream={upstreamWith(access, opts.baseUrl ?? null)}
      onReplace={onReplace}
      usageReportsStopped={opts.stopped}
    />,
  );
  return screen.getByTestId('api-key-access');
}

describe('ApiKeyAccessSection header', () => {
  test('summarizes the key and offers Re-check and Replace key', () => {
    const section = renderAccess(FULL);

    expect(section.textContent).toContain('Service account key · checked');
    expect(within(section).getByRole('status').textContent).toBe(
      'Key accepted',
    );
    fireEvent.click(within(section).getByRole('button', { name: 'Re-check' }));
    expect(recheckMutate).toHaveBeenCalledWith('upstream-api-key');
    fireEvent.click(
      within(section).getByRole('button', { name: 'Replace key' }),
    );
    expect(onReplace).toHaveBeenCalledTimes(1);
  });

  test('falls back to the key class when the key type is unknown', () => {
    const section = renderAccess({ ...FULL, key_type: null });
    expect(section.textContent).toContain('All workspaces key · checked');
  });

  test('a never-checked key leaves Re-check to the pane notice', () => {
    const section = renderAccess(null);

    expect(section.textContent).toContain('Never checked');
    expect(within(section).getByRole('status').textContent).toBe(
      'Key not verified',
    );
    expect(
      within(section).queryByRole('button', { name: 'Re-check' }),
    ).toBeNull();
    expect(
      within(section).getByRole('button', { name: 'Replace key' }),
    ).toBeDefined();
    expect(
      within(section).getByText(
        'Workspace and usage reports appear after a successful check.',
      ),
    ).toBeDefined();
  });

  test('a failed check reads as a last attempt', () => {
    const section = renderAccess({
      ...FULL,
      verified: false,
      key_class: 'unknown',
      key_type: null,
      inference: 'unverified',
    });
    expect(section.textContent).toContain('Last attempt');
    expect(
      within(section).queryByRole('button', { name: 'Re-check' }),
    ).toBeNull();
  });

  test('a rejected key leaves Replace key to the pane notice', () => {
    const section = renderAccess({ ...FULL, inference: 'denied' });

    expect(within(section).getByRole('status').textContent).toBe(
      'Key rejected',
    );
    expect(
      within(section).getByRole('button', { name: 'Re-check' }),
    ).toBeDefined();
    expect(
      within(section).queryByRole('button', { name: 'Replace key' }),
    ).toBeNull();
  });
});

describe('ApiKeyAccessSection rows', () => {
  test('Endpoint, Workspace and Usage data rows; no stored-literal or list rows', () => {
    const section = renderAccess(FULL);

    expect(
      within(section).getByTestId('upstream-endpoint-row').textContent,
    ).toContain('Anthropic (default)');
    expect(within(section).getByText('Production')).toBeDefined();
    expect(within(section).getByText('wrkspc_…7c1e')).toBeDefined();
    const usage = within(section).getByTestId('api-key-usage-mode');
    expect(usage.textContent).toContain('Reported');
    expect(usage.textContent).toContain(
      "Full access · Anthropic's own figures, every 5 min",
    );
    expect(within(section).queryByText(/Literal value/)).toBeNull();
    expect(within(section).queryByText('Workspace list')).toBeNull();
    expect(within(section).queryByText('Inference')).toBeNull();
  });

  test('workspace attribution says the figures are the whole workspace', () => {
    render(
      <ApiKeyAccessSection
        upstream={upstreamWith(FULL)}
        onReplace={onReplace}
        usageAttribution="workspace"
      />,
    );
    expect(screen.getByTestId('api-key-usage-mode').textContent).toContain(
      'Full access · whole-workspace figures from Anthropic',
    );
  });

  test('a bound key shows its short id; the fix replaces the key', () => {
    const section = renderAccess({
      ...FULL,
      key_class: 'workspace_bound',
      key_type: 'workspace',
      workspace: { mode: 'bound', id: 'wrkspc_01PreviewBound4f2a' },
      workspace_list: 'not_applicable',
      usage_report: 'not_applicable',
    });

    expect(
      within(section).getByText('wrkspc_…4f2a').getAttribute('title'),
    ).toBe('wrkspc_01PreviewBound4f2a');
    expect(within(section).getByText('Set by the key')).toBeDefined();
    expect(
      within(section).queryByRole('button', { name: 'Change workspace' }),
    ).toBeNull();
    const usage = within(section).getByTestId('api-key-usage-mode');
    expect(usage.textContent).toContain('Estimated');
    expect(usage.textContent).toContain(
      'Replace it with an All workspaces key → get reported usage.',
    );
    fireEvent.click(
      within(usage).getByRole('button', {
        name: 'Replace it with an All workspaces key',
      }),
    );
    expect(onReplace).toHaveBeenCalledTimes(1);
  });

  test('a low org role gets the plain Console fix', () => {
    const section = renderAccess({ ...FULL, usage_report: 'denied' });
    const usage = within(section).getByTestId('api-key-usage-mode');
    expect(usage.textContent).toContain(
      "Raise the linked account's org role → get reported usage.",
    );
    expect(within(usage).queryByRole('button')).toBeNull();
  });

  test("a custom endpoint's fix opens the Endpoint editor", () => {
    const section = renderAccess(
      { ...FULL, usage_report: 'not_applicable' },
      { baseUrl: 'https://proxy.example.com' },
    );
    expect(
      within(section).getByText('https://proxy.example.com'),
    ).toBeDefined();
    const usage = within(section).getByTestId('api-key-usage-mode');
    expect(usage.textContent).toContain(
      "Use Anthropic's endpoint → get reported usage.",
    );
    fireEvent.click(
      within(usage).getByRole('button', { name: "Use Anthropic's endpoint" }),
    );
    expect(within(section).getByLabelText('Base URL')).toBeDefined();
  });

  test('a temporary report error offers Re-check inline', () => {
    const section = renderAccess({ ...FULL, usage_report: 'error' });
    const usage = within(section).getByTestId('api-key-usage-mode');
    expect(usage.textContent).toContain("Anthropic didn't answer");
    fireEvent.click(within(usage).getByRole('button', { name: 'Re-check' }));
    expect(recheckMutate).toHaveBeenCalledWith('upstream-api-key');
  });

  test('stopped reports read Stopped; the pane banner owns the fix', () => {
    const section = renderAccess(FULL, { stopped: true });
    const usage = within(section).getByTestId('api-key-usage-mode');
    expect(usage.textContent).toContain('Stopped');
    expect(usage.textContent).toContain('Showing the last report.');
    expect(usage.textContent).not.toContain('Reported');
  });

  test('a rejected key keeps Endpoint but hides Workspace and Usage data', () => {
    const section = renderAccess({ ...FULL, inference: 'denied' });
    expect(within(section).getByTestId('upstream-endpoint-row')).toBeDefined();
    expect(within(section).queryByTestId('api-key-usage-mode')).toBeNull();
    expect(within(section).queryByText('Production')).toBeNull();
    expect(
      within(section).getByText(
        'Workspace and usage reports appear after a successful check.',
      ),
    ).toBeDefined();
  });

  test('an org key with no workspace gets one notice that opens the editor', () => {
    const section = renderAccess({
      ...FULL,
      workspace: { mode: 'required' },
    });

    expect(within(section).getByText('No workspace chosen')).toBeDefined();
    expect(within(section).getByText('Not chosen')).toBeDefined();
    fireEvent.click(
      within(section).getByRole('button', { name: 'Choose workspace' }),
    );
    expect(within(section).queryByText('No workspace chosen')).toBeNull();
    expect(within(section).getByRole('button', { name: 'Save' })).toBeDefined();
  });

  test('Change opens the workspace editor', () => {
    const section = renderAccess(FULL);
    fireEvent.click(
      within(section).getByRole('button', { name: 'Change workspace' }),
    );
    expect(
      within(section).getByRole('button', { name: 'Cancel' }),
    ).toBeDefined();
  });

  test('a failed Undo still reports after the editor has closed', async () => {
    const success = vi.spyOn(toast, 'success');
    const error = vi.spyOn(toast, 'error');
    const mutateAsync = vi
      .fn<UpdateMutation['mutateAsync']>()
      .mockRejectedValue(new Error('conflict'));
    vi.mocked(queries.useUpdateUpstream).mockReturnValue(
      idleMutation({
        mutate: succeedWith({ ...upstreamWith(FULL), spec_revision: 8 }),
        mutateAsync,
      }),
    );
    // List denied: the editor takes a typed id.
    const section = renderAccess({ ...FULL, workspace_list: 'denied' });
    fireEvent.click(
      within(section).getByRole('button', { name: 'Change workspace' }),
    );
    fireEvent.change(within(section).getByLabelText('Workspace ID'), {
      target: { value: 'wrkspc_01Other' },
    });
    fireEvent.click(within(section).getByRole('button', { name: 'Save' }));

    // Saved: the editor is gone, the Undo toast remains.
    expect(within(section).queryByLabelText('Workspace ID')).toBeNull();
    const action = success.mock.calls[0]?.[1]?.action;
    if (!(typeof action === 'object' && action && 'onClick' in action)) {
      throw new Error('undo toast has no action button');
    }
    action.onClick({} as Parameters<typeof action.onClick>[0]);

    expect(mutateAsync).toHaveBeenCalledWith({
      id: 'upstream-api-key',
      body: { workspace_id: 'wrkspc_01PreviewProd7c1e' },
      spec_revision: 8,
    });
    await waitFor(() =>
      expect(error).toHaveBeenCalledWith(
        'Undo failed: conflict. Change → pick the previous workspace.',
      ),
    );
  });
});

const CHECK_FAILED: ApiKeyAccess = {
  ...FULL,
  verified: false,
  key_class: 'unknown',
  key_type: null,
  inference: 'unverified',
  workspace_list: 'not_applicable',
  usage_report: 'not_applicable',
  last_error: "Couldn't reach your endpoint — the key wasn't checked.",
};

function mockUpdate(mutate: UpdateMutation['mutate']) {
  vi.mocked(queries.useUpdateUpstream).mockReturnValue(
    idleMutation({ mutate }),
  );
}

function mockWorkspaces(result: UseQueryResult<UpstreamWorkspacesResult>) {
  vi.mocked(queries.useUpstreamWorkspaces).mockReturnValue(result);
}

describe('UI-AC-01 Endpoint row in every state', () => {
  test.each<[string, ApiKeyAccess | null, string | null]>([
    ['verified', FULL, null],
    ['never checked', null, null],
    ['rejected', { ...FULL, inference: 'denied' }, null],
    ['check failed', CHECK_FAILED, null],
    ['custom URL', FULL, 'http://127.0.0.1:9080'],
  ])('%s: row 1 is the effective endpoint', (_l, access, baseUrl) => {
    const section = renderAccess(access, { baseUrl });
    const row = within(section).getByTestId('upstream-endpoint-row');
    const rows = section.querySelectorAll('[data-testid]');
    // Endpoint is the first testid'd row in the body.
    expect(
      [...rows].find((el) =>
        (el.getAttribute('data-testid') ?? '').endsWith('-row'),
      ),
    ).toBe(row);
    expect(
      within(row).getByRole('button', { name: 'Change endpoint' }),
    ).toBeDefined();
    if (baseUrl === null) {
      expect(row.textContent).toContain('Anthropic (default)');
    } else {
      const url = within(row).getByText(baseUrl);
      expect(url.className).toContain('font-mono');
      expect(url.className).toContain('break-all');
      // Rendered as text, never a link.
      expect(url.closest('a')).toBeNull();
      expect(row.textContent).toContain(
        'Requests and key checks go here instead of Anthropic.',
      );
    }
  });
});

describe('UI-AC-07 Workspace row states', () => {
  test('id-only: mono short id with the full id as title, plus Change', () => {
    const section = renderAccess({
      ...FULL,
      workspace: { mode: 'selected', id: 'wrkspc_01OnlyAnId9z9z' },
    });
    const short = within(section).getByText('wrkspc_…9z9z');
    expect(short.getAttribute('title')).toBe('wrkspc_01OnlyAnId9z9z');
    expect(
      within(section).getByRole('button', { name: 'Change workspace' }),
    ).toBeDefined();
  });

  test('required: warn Not chosen, Change, and a warning notice above', () => {
    const section = renderAccess({ ...FULL, workspace: { mode: 'required' } });
    const badge = within(section).getByText('Not chosen');
    expect(badge.parentElement?.className).toContain('text-warn-text');
    expect(
      within(section).getByRole('button', { name: 'Change workspace' }),
    ).toBeDefined();
    expect(section.textContent).toContain(
      'Choose one → finish setting up this key.',
    );
  });
});

describe('UI-AC-08 Change workspace from the list', () => {
  test('pick another, Save re-probes, toast with Undo re-PUTs the previous id', async () => {
    const success = vi.spyOn(toast, 'success');
    const mutate = vi.fn(
      succeedWith({ ...upstreamWith(FULL), spec_revision: 8 }),
    );
    mockUpdate(mutate);
    mockWorkspaces(
      successQuery({
        status: 'ok',
        workspaces: [
          { id: 'wrkspc_01PreviewProd7c1e', name: 'Production' },
          { id: 'wrkspc_01Staging0b0b', name: 'Staging' },
        ],
      }),
    );
    const section = renderAccess(FULL);
    fireEvent.click(
      within(section).getByRole('button', { name: 'Change workspace' }),
    );
    const user = userEvent.setup();
    await user.click(
      within(section).getByRole('combobox', { name: 'Workspace' }),
    );
    await user.click(await screen.findByRole('option', { name: /Staging/ }));
    fireEvent.click(within(section).getByRole('button', { name: 'Save' }));

    expect(mutate).toHaveBeenCalledWith(
      {
        id: 'upstream-api-key',
        body: { workspace_id: 'wrkspc_01Staging0b0b' },
        spec_revision: 7,
      },
      expect.anything(),
    );
    expect(success).toHaveBeenCalledWith(
      'Workspace set to Staging',
      expect.objectContaining({
        action: expect.objectContaining({ label: 'Undo' }),
      }),
    );
  });

  test('Save reads Checking… while the server re-probes', () => {
    vi.mocked(queries.useUpdateUpstream).mockReturnValue(
      pendingMutation({
        id: 'upstream-api-key',
        body: { workspace_id: 'wrkspc_01Other' },
        spec_revision: 7,
      }),
    );
    const section = renderAccess({ ...FULL, workspace_list: 'denied' });
    fireEvent.click(
      within(section).getByRole('button', { name: 'Change workspace' }),
    );
    expect(
      within(section).getByRole('button', { name: 'Checking…' }),
    ).toBeDefined();
  });

  test('first set from required: a plain success toast, no Undo', () => {
    const success = vi.spyOn(toast, 'success');
    mockUpdate(vi.fn(succeedWith(upstreamWith(FULL))));
    const section = renderAccess({
      ...FULL,
      workspace: { mode: 'required' },
      workspace_list: 'denied',
    });
    fireEvent.click(
      within(section).getByRole('button', { name: 'Change workspace' }),
    );
    fireEvent.change(within(section).getByLabelText('Workspace ID'), {
      target: { value: 'wrkspc_01New00aa' },
    });
    fireEvent.click(within(section).getByRole('button', { name: 'Save' }));
    expect(success).toHaveBeenCalledWith('Workspace set to wrkspc_…00aa');
  });

  test('a refresh under an open editor keeps the opening revision: no silent overwrite', () => {
    const toastError = vi.spyOn(toast, 'error');
    const mutate = vi.fn(
      failWith(new ApiError(409, 'stale_revision', null, 'stale revision')),
    );
    mockUpdate(mutate);
    const a: ApiKeyAccess = {
      ...FULL,
      workspace: { mode: 'selected', id: 'wrkspc_01WorkspaceA0aa' },
      workspace_list: 'denied',
    };
    const view = render(
      <ApiKeyAccessSection
        upstream={{ ...upstreamWith(a), spec_revision: 1 }}
        onReplace={onReplace}
      />,
    );
    const section = screen.getByTestId('api-key-access');
    fireEvent.click(
      within(section).getByRole('button', { name: 'Change workspace' }),
    );
    // Another admin saved B; the upstream poll refreshes the pane's props.
    view.rerender(
      <ApiKeyAccessSection
        upstream={{
          ...upstreamWith({
            ...a,
            workspace: { mode: 'selected', id: 'wrkspc_01WorkspaceB0bb' },
          }),
          spec_revision: 2,
        }}
        onReplace={onReplace}
      />,
    );
    const input = within(section).getByLabelText('Workspace ID');
    expect((input as HTMLInputElement).value).toBe('wrkspc_01WorkspaceA0aa');
    fireEvent.click(within(section).getByRole('button', { name: 'Save' }));

    expect(mutate).toHaveBeenCalledTimes(1);
    expect(mutate).toHaveBeenCalledWith(
      {
        id: 'upstream-api-key',
        body: { workspace_id: 'wrkspc_01WorkspaceA0aa' },
        spec_revision: 1,
      },
      expect.anything(),
    );
    expect(within(section).getByRole('alert').textContent).toBe(
      'This upstream changed since you opened the editor. Cancel → open it again.',
    );
    // The draft stays for the admin to compare; nothing toasts.
    expect((input as HTMLInputElement).value).toBe('wrkspc_01WorkspaceA0aa');
    expect(toastError).not.toHaveBeenCalled();
  });
});

describe('UI-AC-09 manual workspace entry', () => {
  function openManual(baseUrl: string | null = null) {
    const section = renderAccess(
      { ...FULL, workspace_list: 'denied' },
      { baseUrl },
    );
    fireEvent.click(
      within(section).getByRole('button', { name: 'Change workspace' }),
    );
    const input = within(section).getByLabelText('Workspace ID');
    expect(document.activeElement).toBe(input);
    return { section, input };
  }

  test('a non-wrkspc_ id is refused locally with no request', () => {
    const mutate = vi.fn<UpdateMutation['mutate']>();
    mockUpdate(mutate);
    const { section, input } = openManual();
    fireEvent.change(input, { target: { value: 'ws_1' } });
    fireEvent.keyDown(input, { key: 'Enter' });
    expect(mutate).not.toHaveBeenCalled();
    expect(within(section).getByRole('alert').textContent).toBe(
      'Workspace IDs start with wrkspc_.',
    );
  });

  test('a save in flight holds the field: repeated Enter sends one PUT', () => {
    const mutate = vi.fn<UpdateMutation['mutate']>();
    // The hook goes pending once `mutate` runs and stays there (the server
    // has not answered), as the real mutation does between Enter presses.
    vi.mocked(queries.useUpdateUpstream).mockImplementation(() => {
      const [sent, setSent] = useState<UpdateVariables | null>(null);
      const send: UpdateMutation['mutate'] = (vars, options) => {
        mutate(vars, options);
        setSent(vars);
      };
      return sent
        ? pendingMutation(sent, { mutate: send })
        : idleMutation({ mutate: send });
    });
    const { section, input } = openManual();
    fireEvent.change(input, { target: { value: 'wrkspc_01New00aa' } });
    fireEvent.keyDown(input, { key: 'Enter' });
    fireEvent.keyDown(input, { key: 'Enter' });
    fireEvent.click(within(section).getByRole('button', { name: 'Checking…' }));
    expect(mutate).toHaveBeenCalledTimes(1);
    // Read-only, not disabled: the value and focus stay while it checks.
    const field = input as HTMLInputElement;
    expect(field.readOnly).toBe(true);
    expect(field.disabled).toBe(false);
    expect(field.getAttribute('aria-busy')).toBe('true');
    expect(field.value).toBe('wrkspc_01New00aa');
    expect(document.activeElement).toBe(input);
  });

  test.each<[string, number, string, string | null, string]>([
    [
      'not found',
      404,
      'workspace_not_found',
      null,
      "This key can't see workspace wrkspc_01Gone0000 — check the id, or the key's account lacks access.",
    ],
    [
      'stored key rejected',
      400,
      'key_invalid',
      null,
      'Anthropic rejected the stored key. Replace it → pick a workspace.',
    ],
    [
      'Anthropic unreachable',
      502,
      'probe_unreachable',
      null,
      "Couldn't reach Anthropic. Save again → check the workspace.",
    ],
    [
      'custom endpoint unreachable',
      504,
      'probe_unreachable',
      'https://proxy.example.com',
      "Couldn't reach your endpoint. Save again → check the workspace.",
    ],
  ])('%s: one inline alert, no toast', (_l, status, code, baseUrl, text) => {
    const toastError = vi.spyOn(toast, 'error');
    mockUpdate(
      vi.fn(failWith(new ApiError(status, code, null, 'server text'))),
    );
    const { section, input } = openManual(baseUrl);
    fireEvent.change(input, { target: { value: 'wrkspc_01Gone0000' } });
    fireEvent.click(within(section).getByRole('button', { name: 'Save' }));
    const alerts = within(section).getAllByRole('alert');
    expect(alerts).toHaveLength(1);
    expect(alerts[0]?.textContent).toBe(text);
    // The value stays for a retry.
    expect((input as HTMLInputElement).value).toBe('wrkspc_01Gone0000');
    expect(toastError).not.toHaveBeenCalled();
  });
});

describe('UI-AC-10 workspace list outcomes', () => {
  function openEditor(): HTMLElement {
    const section = renderAccess(FULL);
    fireEvent.click(
      within(section).getByRole('button', { name: 'Change workspace' }),
    );
    return section;
  }

  test('403 workspace_list_denied: manual mode with the denied line', () => {
    mockWorkspaces(successQuery({ status: 'denied' }));
    const section = openEditor();
    expect(within(section).getByLabelText('Workspace ID')).toBeDefined();
    expect(section.textContent).toContain(
      "This key's account lacks the role to list workspaces.",
    );
  });

  test('403 non_anthropic_base_url: manual mode with the not-applicable line', () => {
    mockWorkspaces(successQuery({ status: 'not_applicable' }));
    const section = openEditor();
    expect(within(section).getByLabelText('Workspace ID')).toBeDefined();
    expect(section.textContent).toContain(
      'Workspace lists need api.anthropic.com — paste the ID.',
    );
  });

  test('5xx: a warning alert with Retry and Enter ID, no toast', () => {
    const toastError = vi.spyOn(toast, 'error');
    const refetch =
      vi.fn<UseQueryResult<UpstreamWorkspacesResult>['refetch']>();
    mockWorkspaces(
      failedQuery(new ApiError(502, 'probe_unreachable', null, 'Bad gateway'), {
        refetch,
      }),
    );
    const section = openEditor();
    const alert = within(section).getByRole('alert');
    expect(alert.textContent).toContain("Couldn't load workspaces:");
    fireEvent.click(within(alert).getByRole('button', { name: 'Retry' }));
    expect(refetch).toHaveBeenCalledTimes(1);
    fireEvent.click(within(alert).getByRole('button', { name: 'Enter ID' }));
    expect(within(section).getByLabelText('Workspace ID')).toBeDefined();
    expect(toastError).not.toHaveBeenCalled();
  });
});

describe('UI-AC-11 an org-scoped key cannot clear its workspace', () => {
  test('the editor has no clear path and never sends clear_workspace_id', () => {
    const mutate = vi.fn<UpdateMutation['mutate']>();
    mockUpdate(mutate);
    const section = renderAccess({ ...FULL, workspace_list: 'denied' });
    fireEvent.click(
      within(section).getByRole('button', { name: 'Change workspace' }),
    );
    const input = within(section).getByLabelText('Workspace ID');
    fireEvent.change(input, { target: { value: '' } });
    expect(
      within(section)
        .getByRole('button', { name: 'Save' })
        .hasAttribute('disabled'),
    ).toBe(true);
    expect(
      within(section).queryByRole('button', { name: /clear/i }),
    ).toBeNull();
    fireEvent.keyDown(input, { key: 'Enter' });
    for (const [vars] of mutate.mock.calls) {
      expect(JSON.stringify(vars)).not.toContain('clear_workspace_id');
    }
  });
});

describe('UI-AC-12 / UI-AC-13 Usage data modes and tones', () => {
  function usageBadge(section: HTMLElement, label: string): HTMLElement {
    const usage = within(section).getByTestId('api-key-usage-mode');
    const text = within(usage).getByText(label);
    const badge = text.parentElement;
    if (!badge) throw new Error(`no ${label} badge`);
    return badge;
  }

  test('Reported is ok, Stopped is warn, Estimated is neutral (never amber)', () => {
    let section = renderAccess(FULL);
    expect(usageBadge(section, 'Reported').className).toContain(
      'text-traffic-success-text',
    );
    cleanup();
    section = renderAccess(FULL, { stopped: true });
    expect(usageBadge(section, 'Stopped').className).toContain(
      'text-warn-text',
    );
    for (const report of ['denied', 'error', 'not_applicable'] as const) {
      cleanup();
      section = renderAccess({ ...FULL, usage_report: report });
      const badge = usageBadge(section, 'Estimated');
      expect(badge.className).toContain('text-text-muted');
      expect(badge.className).not.toContain('text-warn-text');
    }
  });

  test('the error nudge hides its Re-check link while a re-check is pending', () => {
    vi.mocked(queries.useRecheckApiKey).mockReturnValue(
      pendingMutation('upstream-api-key', { mutate: recheckMutate }),
    );
    const section = renderAccess({ ...FULL, usage_report: 'error' });
    const usage = within(section).getByTestId('api-key-usage-mode');
    expect(usage.textContent).toContain("Anthropic didn't answer");
    expect(
      within(usage).queryByRole('button', { name: 'Re-check' }),
    ).toBeNull();
  });

  test('every degraded mode has exactly one "→" nudge', () => {
    for (const [access, baseUrl] of [
      [{ ...FULL, usage_report: 'denied' }, null],
      [{ ...FULL, usage_report: 'error' }, null],
      [
        {
          ...FULL,
          key_class: 'workspace_bound',
          workspace: { mode: 'bound', id: 'wrkspc_01b' },
          usage_report: 'not_applicable',
        },
        null,
      ],
      [
        { ...FULL, usage_report: 'not_applicable' },
        'https://proxy.example.com',
      ],
    ] as [ApiKeyAccess, string | null][]) {
      const section = renderAccess(access, { baseUrl });
      const line =
        within(section).getByTestId('api-key-usage-mode').textContent;
      expect(line?.split('→').length).toBe(2);
      cleanup();
    }
  });
});

describe('UI-AC-14 keyUsable gate', () => {
  test.each<[string, ApiKeyAccess | null]>([
    ['s19 rejected', { ...FULL, inference: 'denied' }],
    ['s11 never checked', null],
    ['s10 last check failed', CHECK_FAILED],
  ])('%s: only the Endpoint row and the muted caption', (_l, access) => {
    const section = renderAccess(access);
    expect(within(section).getByTestId('upstream-endpoint-row')).toBeDefined();
    expect(within(section).queryByText('Workspace')).toBeNull();
    expect(within(section).queryByTestId('api-key-usage-mode')).toBeNull();
    expect(
      within(section).getByText(
        'Workspace and usage reports appear after a successful check.',
      ).className,
    ).toContain('text-text-muted');
  });
});

describe('UI-AC-15 header', () => {
  test.each<[string, ApiKeyAccess | null, string, string]>([
    ['s06', FULL, 'Service account key · checked', 'Key accepted'],
    [
      'org without a key type',
      { ...FULL, key_type: null },
      'All workspaces key · checked',
      'Key accepted',
    ],
    [
      's05 bound',
      {
        ...FULL,
        key_class: 'workspace_bound',
        key_type: null,
        workspace: { mode: 'bound', id: 'wrkspc_01b' },
      },
      'Workspace key · checked',
      'Key accepted',
    ],
    ['s11', null, 'Never checked', 'Key not verified'],
    ['s10', CHECK_FAILED, 'Last attempt', 'Key not verified'],
    ['s19', { ...FULL, inference: 'denied' }, 'checked', 'Key rejected'],
  ])('%s: description and one status badge', (_l, access, summary, badge) => {
    const section = renderAccess(access);
    expect(section.textContent).toContain(summary);
    const statuses = within(section).getAllByRole('status');
    expect(statuses).toHaveLength(1);
    expect(statuses[0]?.textContent).toBe(badge);
  });

  test('K2/K3 hide the header Re-check; K1 hides the header Replace key', () => {
    for (const access of [null, CHECK_FAILED]) {
      const section = renderAccess(access);
      expect(
        within(section).queryByRole('button', { name: 'Re-check' }),
      ).toBeNull();
      expect(
        within(section).getByRole('button', { name: 'Replace key' }),
      ).toBeDefined();
      cleanup();
    }
    const k1 = renderAccess({ ...FULL, inference: 'denied' });
    expect(
      within(k1).queryByRole('button', { name: 'Replace key' }),
    ).toBeNull();
    expect(within(k1).getByRole('button', { name: 'Re-check' })).toBeDefined();
  });
});

describe('UI-AC-16 header Re-check failure', () => {
  test('one alert at the top of the Access body, no toast', () => {
    const toastError = vi.spyOn(toast, 'error');
    vi.mocked(queries.useRecheckApiKey).mockReturnValue(
      failedMutation(
        new ApiError(504, 'probe_unreachable', null, 'timed out'),
        'upstream-api-key',
        { mutate: recheckMutate },
      ),
    );
    const section = renderAccess(FULL);
    const alerts = within(section).getAllByRole('alert');
    expect(alerts).toHaveLength(1);
    expect(alerts[0]?.textContent).toBe('Re-check failed: timed out');
    // First in the body: before the Endpoint row.
    expect(
      alerts[0]?.compareDocumentPosition(
        within(section).getByTestId('upstream-endpoint-row'),
      ),
    ).toBe(Node.DOCUMENT_POSITION_FOLLOWING);
    expect(toastError).not.toHaveBeenCalled();
  });
});

describe('UI-AC-17 endpoint change outcomes', () => {
  test('(a) custom endpoint accepted: Key accepted and the Anthropic nudge', () => {
    const section = renderAccess(
      {
        ...FULL,
        workspace_list: 'not_applicable',
        usage_report: 'not_applicable',
      },
      { baseUrl: 'http://127.0.0.1:9080' },
    );
    expect(within(section).getByRole('status').textContent).toBe(
      'Key accepted',
    );
    expect(
      within(section).getByTestId('api-key-usage-mode').textContent,
    ).toContain("Use Anthropic's endpoint → get reported usage.");
  });

  test('(b) unreachable custom endpoint: Key not verified, K3 owns Re-check', () => {
    const section = renderAccess(CHECK_FAILED, {
      baseUrl: 'http://127.0.0.1:9',
    });
    expect(within(section).getByRole('status').textContent).toBe(
      'Key not verified',
    );
    expect(
      within(section).queryByRole('button', { name: 'Re-check' }),
    ).toBeNull();
  });

  test('(c) Use default: org probes ran again, Usage data reads Reported', () => {
    const view = render(
      <ApiKeyAccessSection
        upstream={upstreamWith(
          { ...FULL, usage_report: 'not_applicable' },
          'http://127.0.0.1:9080',
        )}
        onReplace={onReplace}
      />,
    );
    view.rerender(
      <ApiKeyAccessSection
        upstream={upstreamWith(FULL)}
        onReplace={onReplace}
      />,
    );
    const usage = screen.getByTestId('api-key-usage-mode');
    expect(usage.textContent).toContain('Reported');
    expect(screen.getByText('Production')).toBeDefined();
  });
});
