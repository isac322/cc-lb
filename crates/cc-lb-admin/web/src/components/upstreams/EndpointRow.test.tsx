import type { UseMutationResult } from '@tanstack/react-query';
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from '@testing-library/react';
import { toast } from 'sonner';
import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest';
import { ApiError } from '../../lib/api';
import type { UpdateUpstreamRequest, Upstream } from '../../lib/queries';
import * as queries from '../../lib/queries';
import {
  failedMutation,
  idleMutation,
  mutationContext,
  pendingMutation,
} from '../../lib/test-utils/hook-results';
import { EndpointRow } from './EndpointRow';

vi.mock('../../lib/queries', async () => {
  const actual = (await vi.importActual('../../lib/queries')) as typeof queries;
  return {
    ...actual,
    useUpdateUpstream: vi.fn(),
  };
});

const upstream: Upstream = {
  id: 'upstream-api-key',
  name: 'API Key Primary',
  kind: 'anthropic_api_key',
  enabled: true,
  spec_revision: 7,
  base_url: 'https://proxy.example.com',
  warmup_enabled: false,
  warmup_dialect_plugin: null,
  status: {
    last_apply_error: null,
    last_apply_at_unix_secs: null,
    last_warmup_at_unix_secs: null,
  },
};

// The hook's result contract; the mock below must stay assignable to it.
type UpdateVariables = {
  id: string;
  body: UpdateUpstreamRequest;
  spec_revision: number;
};
type UpdateMutation = UseMutationResult<Upstream, Error, UpdateVariables>;

const mutate = vi.fn<UpdateMutation['mutate']>();
const mutateAsync = vi.fn<UpdateMutation['mutateAsync']>();
const fns = { mutate, mutateAsync };
let updateMutation: UpdateMutation = idleMutation(fns);

/** A `mutate` that settles with `updated` as the server's answer. */
const succeedWith =
  (updated: Upstream): UpdateMutation['mutate'] =>
  (vars, options) =>
    options?.onSuccess?.(updated, vars, undefined, mutationContext);

beforeEach(() => {
  vi.clearAllMocks();
  updateMutation = idleMutation(fns);
  vi.mocked(queries.useUpdateUpstream).mockImplementation(() => updateMutation);
});

afterEach(cleanup);

const input = () =>
  screen.getByPlaceholderText('https://proxy.example.com') as HTMLInputElement;

describe('EndpointRow default', () => {
  test.each([
    ['unset', null],
    ['stored Anthropic URL', 'https://api.anthropic.com'],
  ])(
    'always shows the effective endpoint when %s, for both kinds',
    (_l, base) => {
      for (const kind of ['api_key', 'oauth'] as const) {
        const view = render(
          <EndpointRow
            upstream={{ ...upstream, base_url: base }}
            kind={kind}
          />,
        );
        const row = screen.getByTestId('upstream-endpoint-row');
        expect(row.textContent).toContain('Endpoint');
        expect(row.textContent).toContain('Anthropic (default)');
        expect(row.textContent).not.toContain('https://api.anthropic.com');
        expect(
          screen.getByRole('button', { name: 'Change endpoint' }),
        ).toBeDefined();
        expect(
          screen.queryByRole('button', { name: 'Use a custom endpoint' }),
        ).toBeNull();
        view.unmount();
      }
    },
  );

  test('Change opens an empty, focused editor; Cancel refocuses Change', () => {
    render(
      <EndpointRow upstream={{ ...upstream, base_url: null }} kind="oauth" />,
    );

    fireEvent.click(screen.getByRole('button', { name: 'Change endpoint' }));
    expect(input().value).toBe('');
    expect(document.activeElement).toBe(input());
    expect(
      screen.getByText('Requests go here instead of Anthropic.'),
    ).toBeDefined();
    expect(screen.queryByRole('button', { name: 'Use default' })).toBeNull();
    expect(
      screen.getByRole('button', { name: 'Save' }).hasAttribute('disabled'),
    ).toBe(true);

    fireEvent.click(screen.getByRole('button', { name: 'Cancel' }));
    expect(document.activeElement).toBe(
      screen.getByRole('button', { name: 'Change endpoint' }),
    );
    expect(mutate).not.toHaveBeenCalled();
  });
});

describe('EndpointRow custom', () => {
  test('shows the URL in mono with the kind hint', () => {
    render(<EndpointRow upstream={upstream} kind="api_key" />);
    expect(screen.getByText('https://proxy.example.com').className).toContain(
      'font-mono',
    );
    expect(
      screen.getByText('Requests and key checks go here instead of Anthropic.'),
    ).toBeDefined();
  });

  test.each([
    'https://api.anthropic.com/v1/',
    'https://api.anthropic.com/?x=1',
    'https://api.anthropic.com:8443',
  ])('a stored Anthropic near-miss %s stays custom', (base) => {
    render(
      <EndpointRow upstream={{ ...upstream, base_url: base }} kind="api_key" />,
    );
    const row = screen.getByTestId('upstream-endpoint-row');
    expect(row.textContent).toContain(base);
    expect(row.textContent).not.toContain('Anthropic (default)');
  });

  test('saving an Anthropic near-miss stores it instead of clearing', () => {
    render(<EndpointRow upstream={upstream} kind="api_key" />);
    fireEvent.click(screen.getByRole('button', { name: 'Change endpoint' }));
    fireEvent.change(input(), {
      target: { value: 'https://api.anthropic.com/v1/' },
    });
    fireEvent.keyDown(input(), { key: 'Enter' });
    expect(mutate).toHaveBeenCalledTimes(1);
    expect(mutate.mock.calls[0][0].body).toEqual({
      base_url: 'https://api.anthropic.com/v1/',
    });
  });

  test('locks actions, shows saving progress, and submits only once', () => {
    const view = render(<EndpointRow upstream={upstream} kind="api_key" />);

    fireEvent.click(screen.getByRole('button', { name: 'Change endpoint' }));
    expect(
      screen.getByTestId('upstream-endpoint-row').getAttribute('aria-busy'),
    ).toBe('false');
    fireEvent.change(input(), {
      target: { value: ' https://other.example.com ' },
    });
    fireEvent.keyDown(input(), { key: 'Enter' });

    expect(mutate).toHaveBeenCalledTimes(1);
    expect(mutate).toHaveBeenCalledWith(
      {
        id: upstream.id,
        body: { base_url: 'https://other.example.com' },
        spec_revision: upstream.spec_revision,
      },
      expect.anything(),
    );

    updateMutation = pendingMutation(mutate.mock.calls[0][0], fns);
    view.rerender(<EndpointRow upstream={upstream} kind="api_key" />);

    expect(
      screen.getByTestId('upstream-endpoint-row').getAttribute('aria-busy'),
    ).toBe('true');
    const saving = screen.getByRole('button', { name: 'Saving…' });
    expect(saving.hasAttribute('disabled')).toBe(true);
    expect(
      screen.getByRole('button', { name: 'Cancel' }).hasAttribute('disabled'),
    ).toBe(true);

    fireEvent.click(saving);
    expect(mutate).toHaveBeenCalledTimes(1);
  });

  test('Save stays disabled while the value is unchanged or empty', () => {
    render(<EndpointRow upstream={upstream} kind="api_key" />);

    fireEvent.click(screen.getByRole('button', { name: 'Change endpoint' }));
    expect(input().value).toBe('https://proxy.example.com');
    expect(
      screen.getByRole('button', { name: 'Save' }).hasAttribute('disabled'),
    ).toBe(true);
    fireEvent.change(input(), { target: { value: '  ' } });
    expect(
      screen.getByRole('button', { name: 'Save' }).hasAttribute('disabled'),
    ).toBe(true);
    expect(mutate).not.toHaveBeenCalled();
  });

  test('saving the Anthropic URL clears instead of storing it as custom', () => {
    render(<EndpointRow upstream={upstream} kind="api_key" />);

    fireEvent.click(screen.getByRole('button', { name: 'Change endpoint' }));
    fireEvent.change(input(), {
      target: { value: 'https://api.anthropic.com' },
    });
    fireEvent.click(screen.getByRole('button', { name: 'Save' }));

    expect(mutate).toHaveBeenCalledTimes(1);
    expect(mutate.mock.calls[0][0].body).toEqual({ clear_base_url: true });
  });

  test('does not change the URL when props refetch under an untouched edit', () => {
    const view = render(<EndpointRow upstream={upstream} kind="api_key" />);

    fireEvent.click(screen.getByRole('button', { name: 'Change endpoint' }));
    view.rerender(
      <EndpointRow
        upstream={{
          ...upstream,
          base_url: 'https://refetched.example.com',
          spec_revision: 8,
        }}
        kind="api_key"
      />,
    );

    expect(
      screen.getByRole('button', { name: 'Save' }).hasAttribute('disabled'),
    ).toBe(true);
    expect(mutate).not.toHaveBeenCalled();
  });

  test('a refresh under an open editor keeps the opening revision: no silent overwrite', () => {
    const toastError = vi.spyOn(toast, 'error');
    const a = { ...upstream, spec_revision: 1 };
    const view = render(<EndpointRow upstream={a} kind="api_key" />);
    fireEvent.click(screen.getByRole('button', { name: 'Change endpoint' }));
    // Another admin saved B; the upstream poll refreshes the row's props.
    const b = {
      ...upstream,
      base_url: 'https://b.example.com',
      spec_revision: 2,
    };
    view.rerender(<EndpointRow upstream={b} kind="api_key" />);
    fireEvent.change(input(), {
      target: { value: 'https://mine.example.com' },
    });
    fireEvent.click(screen.getByRole('button', { name: 'Save' }));

    const sent = {
      id: upstream.id,
      body: { base_url: 'https://mine.example.com' },
      spec_revision: 1,
    };
    expect(mutate).toHaveBeenCalledTimes(1);
    expect(mutate).toHaveBeenCalledWith(sent, expect.anything());

    // The server refuses the older revision; the draft stays with the reason.
    updateMutation = failedMutation(
      new ApiError(409, 'stale_revision', null, 'stale revision'),
      sent,
      fns,
    );
    view.rerender(<EndpointRow upstream={b} kind="api_key" />);
    expect(screen.getByRole('alert').textContent).toBe(
      'This upstream changed since you opened the editor. Cancel → open it again.',
    );
    expect(input().value).toBe('https://mine.example.com');
    expect(toastError).not.toHaveBeenCalled();
  });

  test('a save failure renders inline', () => {
    updateMutation = failedMutation<Upstream, UpdateVariables>(
      new Error('Request failed (409)'),
      { id: upstream.id, body: { base_url: 'x' }, spec_revision: 7 },
      fns,
    );
    render(<EndpointRow upstream={upstream} kind="api_key" editing />);
    expect(screen.getByRole('alert').textContent).toBe('Request failed (409)');
  });

  test('"Use default" clears and offers Undo; a failed Undo reports', async () => {
    const success = vi.spyOn(toast, 'success');
    const error = vi.spyOn(toast, 'error');
    mutate.mockImplementationOnce(
      succeedWith({ ...upstream, base_url: null, spec_revision: 8 }),
    );
    mutateAsync.mockRejectedValueOnce(new Error('conflict'));
    render(<EndpointRow upstream={upstream} kind="api_key" />);

    fireEvent.click(screen.getByRole('button', { name: 'Change endpoint' }));
    fireEvent.click(screen.getByRole('button', { name: 'Use default' }));

    expect(mutate).toHaveBeenCalledTimes(1);
    expect(mutate.mock.calls[0][0].body).toEqual({ clear_base_url: true });
    expect(success).toHaveBeenCalledWith(
      'Using the default endpoint',
      expect.objectContaining({
        action: expect.objectContaining({ label: 'Undo' }),
      }),
    );

    const action = success.mock.calls[0]?.[1]?.action;
    if (!(typeof action === 'object' && action && 'onClick' in action)) {
      throw new Error('undo toast has no action button');
    }
    action.onClick({} as Parameters<typeof action.onClick>[0]);
    expect(mutateAsync).toHaveBeenCalledWith({
      id: upstream.id,
      body: { base_url: 'https://proxy.example.com' },
      spec_revision: 8,
    });
    await waitFor(() =>
      expect(error).toHaveBeenCalledWith('Undo failed: conflict'),
    );
  });

  test('keeps editing when the server does not confirm the clear', () => {
    const success = vi.spyOn(toast, 'success');
    const error = vi.spyOn(toast, 'error');
    // An old backend ignores clear_base_url and returns the stored URL.
    mutate.mockImplementationOnce(
      succeedWith({ ...upstream, spec_revision: 8 }),
    );
    render(<EndpointRow upstream={upstream} kind="api_key" />);

    fireEvent.click(screen.getByRole('button', { name: 'Change endpoint' }));
    fireEvent.click(screen.getByRole('button', { name: 'Use default' }));

    expect(mutate.mock.calls[0][0].body.clear_base_url).toBe(true);
    expect(success).not.toHaveBeenCalled();
    expect(error).toHaveBeenCalledTimes(1);
    expect(screen.getByLabelText('Base URL')).toBeDefined();
  });

  test('treats a missing base_url field as an unconfirmed clear', () => {
    const success = vi.spyOn(toast, 'success');
    const error = vi.spyOn(toast, 'error');
    // An even older DTO omits base_url entirely from the response.
    const { base_url: _omitted, ...withoutBaseUrl } = upstream;
    mutate.mockImplementationOnce(succeedWith(withoutBaseUrl as Upstream));
    render(<EndpointRow upstream={upstream} kind="api_key" />);

    fireEvent.click(screen.getByRole('button', { name: 'Change endpoint' }));
    fireEvent.click(screen.getByRole('button', { name: 'Use default' }));

    expect(mutate).toHaveBeenCalledTimes(1);
    expect(success).not.toHaveBeenCalled();
    expect(error).toHaveBeenCalledTimes(1);
    expect(screen.getByLabelText('Base URL')).toBeDefined();
  });

  test('offers no inline key fields: the key is replaced from Access', () => {
    render(<EndpointRow upstream={upstream} kind="api_key" />);

    fireEvent.click(screen.getByRole('button', { name: 'Change endpoint' }));
    expect(
      screen.queryByPlaceholderText(
        'Enter new API key to replace stored value',
      ),
    ).toBeNull();
    expect(screen.queryByText('Literal value')).toBeNull();
  });
});

describe('UI-AC-02 / UI-O-02 endpoint edit to custom', () => {
  test.each(['api_key', 'oauth'] as const)(
    '%s: Save PUTs with the revision, toasts, and returns focus to Change',
    (kind) => {
      const success = vi.spyOn(toast, 'success');
      mutate.mockImplementationOnce(
        succeedWith({ ...upstream, base_url: 'http://127.0.0.1:9080' }),
      );
      render(
        <EndpointRow upstream={{ ...upstream, base_url: null }} kind={kind} />,
      );

      fireEvent.click(screen.getByRole('button', { name: 'Change endpoint' }));
      const field = screen.getByLabelText('Base URL') as HTMLInputElement;
      expect(field.value).toBe('');
      expect(field.getAttribute('placeholder')).toBe(
        'https://proxy.example.com',
      );
      expect(document.activeElement).toBe(field);
      fireEvent.change(field, { target: { value: 'http://127.0.0.1:9080' } });
      fireEvent.keyDown(field, { key: 'Enter' });

      expect(mutate).toHaveBeenCalledWith(
        {
          id: upstream.id,
          body: { base_url: 'http://127.0.0.1:9080' },
          spec_revision: upstream.spec_revision,
        },
        expect.anything(),
      );
      expect(success).toHaveBeenCalledWith('Custom endpoint saved');
      expect(document.activeElement).toBe(
        screen.getByRole('button', { name: 'Change endpoint' }),
      );
    },
  );

  test('the update opts out of the global toast: failures render inline', () => {
    render(<EndpointRow upstream={upstream} kind="oauth" />);
    expect(queries.useUpdateUpstream).toHaveBeenCalledWith({
      inlineError: true,
    });
  });

  test('OAuth custom URL reads with the OAuth hint and offers Use default', () => {
    render(<EndpointRow upstream={upstream} kind="oauth" />);
    expect(
      screen.getByText('Requests go here instead of Anthropic.'),
    ).toBeDefined();
    fireEvent.click(screen.getByRole('button', { name: 'Change endpoint' }));
    expect(screen.getByRole('button', { name: 'Use default' })).toBeDefined();
  });
});
