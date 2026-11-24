// UI-U-09 / UI-AC-10 (D-UX1, R2-6): the API-key pane renders usage and
// workspace-list failures itself, so the app's QueryCache never toasts them.

import { QueryClientProvider } from '@tanstack/react-query';
import { act, renderHook, waitFor } from '@testing-library/react';
import { createElement, type ReactNode } from 'react';
import { toast } from 'sonner';
import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest';
import { qk, useUpstreamKeyUsage, useUpstreamWorkspaces } from '../../queries';
import { queryClient } from '../../queryClient';

const ID = 'upstream-key';

function wrapper({ children }: { children: ReactNode }) {
  return createElement(QueryClientProvider, { client: queryClient }, children);
}

function failing(status: number, body: unknown) {
  return vi.fn(
    async () =>
      new Response(JSON.stringify(body), {
        status,
        headers: { 'Content-Type': 'application/json' },
      }),
  );
}

beforeEach(() => {
  queryClient.clear();
  // One attempt, so the failure lands without waiting out a retry delay.
  queryClient.setQueryDefaults(qk.upstreamKeyUsage(ID, '24h', 'hour'), {
    retry: false,
  });
});

afterEach(() => {
  queryClient.clear();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe('API-key pane queries report failures inline', () => {
  test('a usage load failure is an error with zero toasts', async () => {
    const toastError = vi.spyOn(toast, 'error');
    vi.stubGlobal('fetch', failing(500, { error: 'storage_unavailable' }));

    const { result } = renderHook(
      () => useUpstreamKeyUsage(ID, '24h', 'hour'),
      { wrapper },
    );

    await waitFor(() => expect(result.current.isError).toBe(true));
    expect(toastError).not.toHaveBeenCalled();
  });

  test('a failed refresh keeps the last answer and exposes the error, no toast', async () => {
    const toastError = vi.spyOn(toast, 'error');
    vi.stubGlobal(
      'fetch',
      vi
        .fn()
        .mockResolvedValueOnce(
          new Response(JSON.stringify({ month: { tokens: 500 } }), {
            status: 200,
            headers: { 'Content-Type': 'application/json' },
          }),
        )
        .mockImplementation(failing(500, { error: 'storage_unavailable' })),
    );

    const { result } = renderHook(
      () => useUpstreamKeyUsage(ID, '24h', 'hour'),
      { wrapper },
    );
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    await act(() => result.current.refetch());

    // The pane renders both: retained figures and the refresh error.
    await waitFor(() => expect(result.current.error).not.toBeNull());
    expect(result.current.data?.month.tokens).toBe(500);
    expect(toastError).not.toHaveBeenCalled();
  });

  test.each([
    [502, { error: { code: 'probe_unreachable', message: 'unreachable' } }],
    [500, { error: 'api_key_credential_decrypt_failed' }],
  ])(
    'a %i workspace list failure is an error with zero toasts',
    async (status, body) => {
      const toastError = vi.spyOn(toast, 'error');
      vi.stubGlobal('fetch', failing(status, body));

      const { result } = renderHook(() => useUpstreamWorkspaces(ID), {
        wrapper,
      });

      await waitFor(() => expect(result.current.isError).toBe(true));
      expect(toastError).not.toHaveBeenCalled();
    },
  );

  test('a 403 list refusal resolves as data, never an error', async () => {
    const toastError = vi.spyOn(toast, 'error');
    vi.stubGlobal(
      'fetch',
      failing(403, {
        error: { code: 'workspace_list_denied', message: 'denied' },
      }),
    );

    const { result } = renderHook(() => useUpstreamWorkspaces(ID), {
      wrapper,
    });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data).toEqual({ status: 'denied' });
    expect(toastError).not.toHaveBeenCalled();
  });
});
