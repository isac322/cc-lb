// A workspace, key or endpoint edit changes whose usage the API usage card
// reads, so saving refetches the upstream's usage and workspace list instead
// of leaving the previous subject's figures cached under the new metadata.

import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { act, renderHook, waitFor } from '@testing-library/react';
import { createElement, type ReactNode } from 'react';
import { afterEach, describe, expect, test, vi } from 'vitest';
import {
  qk,
  useUpdateUpstream,
  useUpstreamKeyUsage,
  useUpstreamWorkspaces,
} from '../../queries';

const ID = 'upstream-key';

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

function json(body: unknown) {
  return new Response(JSON.stringify(body), {
    status: 200,
    headers: { 'Content-Type': 'application/json' },
  });
}

describe('useUpdateUpstream', () => {
  test('a saved edit refetches the upstream usage and workspace list', async () => {
    const calls: string[] = [];
    vi.stubGlobal(
      'fetch',
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
        calls.push(`${init?.method ?? 'GET'} ${url}`);
        if (url.includes('/usage?')) {
          return json({ attribution_workspace_id: 'wrkspc_A', tokens: 500 });
        }
        if (url.endsWith('/workspaces')) return json({ workspaces: [] });
        return json({ id: ID, spec_revision: 8 });
      }),
    );
    const client = new QueryClient({
      defaultOptions: {
        queries: { retry: false },
        mutations: { retry: false },
      },
    });
    const invalidate = vi.spyOn(client, 'invalidateQueries');
    const wrapper = ({ children }: { children: ReactNode }) =>
      createElement(QueryClientProvider, { client }, children);
    const { result } = renderHook(
      () => ({
        usage: useUpstreamKeyUsage(ID, '24h', 'hour'),
        workspaces: useUpstreamWorkspaces(ID),
        update: useUpdateUpstream({ inlineError: true }),
      }),
      { wrapper },
    );
    await waitFor(() => expect(result.current.usage.isSuccess).toBe(true));
    await waitFor(() => expect(result.current.workspaces.isSuccess).toBe(true));
    const count = (part: string) =>
      calls.filter((c) => c.startsWith('GET') && c.includes(part)).length;
    expect(count('/usage?')).toBe(1);
    expect(count('/workspaces')).toBe(1);

    await act(() =>
      result.current.update.mutateAsync({
        id: ID,
        body: { workspace_id: 'wrkspc_B' },
        spec_revision: 7,
      }),
    );

    expect(invalidate.mock.calls.map(([arg]) => arg?.queryKey)).toEqual(
      expect.arrayContaining([
        qk.upstreamWorkspaces(ID),
        ['upstream-key-usage', ID],
      ]),
    );
    await waitFor(() => expect(count('/usage?')).toBe(2));
    await waitFor(() => expect(count('/workspaces')).toBe(2));
  });
});
