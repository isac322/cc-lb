import {
  defaultScheduler,
  notifyManager,
  QueryClient,
  QueryClientProvider,
} from '@tanstack/react-query';
import {
  act,
  render,
  renderHook,
  screen,
  waitFor,
} from '@testing-library/react';
import { createElement, type ReactNode } from 'react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  type PluginChainEntry,
  type Upstream,
  useDeletePlugin,
  usePluginChain,
  useUpstreams,
} from '../../queries';
import { usePolledData } from '../../usePolledData';

function setDocumentVisibility(state: DocumentVisibilityState) {
  Object.defineProperty(document, 'visibilityState', {
    value: state,
    configurable: true,
  });
  document.dispatchEvent(new Event('visibilitychange'));
}

function jsonResponse(body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status: 200,
    headers: { 'Content-Type': 'application/json' },
  });
}

afterEach(() => {
  act(() => setDocumentVisibility('visible'));
  notifyManager.setScheduler(defaultScheduler);
  vi.unstubAllGlobals();
  vi.clearAllMocks();
});

describe('query observer boundaries', () => {
  it('does not rerender a data-only consumer when a refetch returns the same payload', async () => {
    const client = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });
    const queryKey = ['same-data-refetch'] as const;
    const queryFn = vi.fn(async () => ({ value: 'stable' }));
    let renderCount = 0;
    notifyManager.setScheduler(queueMicrotask);

    function DataConsumer() {
      renderCount += 1;
      const { data } = usePolledData({ queryKey, queryFn }, false);
      return createElement(
        'span',
        { 'data-testid': 'stable-value' },
        data?.value ?? 'loading',
      );
    }

    render(
      createElement(
        QueryClientProvider,
        { client },
        createElement(DataConsumer),
      ),
    );

    await waitFor(() =>
      expect(screen.getByTestId('stable-value').textContent).toBe('stable'),
    );
    const settledRenderCount = renderCount;

    await act(async () => {
      await client.refetchQueries({ queryKey });
    });
    expect(queryFn).toHaveBeenCalledTimes(2);
    expect(renderCount).toBe(settledRenderCount);
  });

  it('keeps an explicitly disabled query disabled across visibility changes', async () => {
    const client = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });
    const queryFn = vi.fn(async () => 'unexpected');

    function DisabledConsumer() {
      const { data } = usePolledData(
        { queryKey: ['disabled-poll'], queryFn, enabled: false },
        1_000,
      );
      return createElement(
        'span',
        { 'data-testid': 'disabled-value' },
        data ?? 'disabled',
      );
    }

    render(
      createElement(
        QueryClientProvider,
        { client },
        createElement(DisabledConsumer),
      ),
    );
    expect(screen.getByTestId('disabled-value').textContent).toBe('disabled');
    expect(queryFn).not.toHaveBeenCalled();

    act(() => setDocumentVisibility('hidden'));
    await act(async () => {
      await Promise.resolve();
    });
    act(() => setDocumentVisibility('visible'));
    await act(async () => {
      await Promise.resolve();
    });

    expect(queryFn).not.toHaveBeenCalled();
    expect(screen.getByTestId('disabled-value').textContent).toBe('disabled');
  });

  it('refreshes observed plugin-chain and upstream state after a cascade delete', async () => {
    const principalId = 'principal-1';
    const pluginId = 'plugin-1';
    const oldChainEntry: PluginChainEntry = {
      id: 'chain-entry-1',
      principal_id: principalId,
      slot: 'shape',
      order: 0,
      wasm_registry_id: pluginId,
      config: {},
      sse_per_event: false,
      batched_events_per_flush: 100,
      batched_flush_ms: 1_000,
      revision: 3,
    };
    const oldUpstream: Upstream = {
      id: 'upstream-1',
      name: 'Upstream 1',
      kind: 'anthropic_oauth',
      enabled: true,
      spec_revision: 7,
      base_url: null,
      api_key_env: null,
      warmup_enabled: true,
      warmup_dialect_plugin: {
        wasm_registry_id: pluginId,
        config: { mode: 'anthropic' },
        wire_version: 1,
      },
      status: {
        last_apply_error: null,
        last_apply_at_unix_secs: null,
        last_warmup_at_unix_secs: null,
      },
    };
    const updatedUpstream: Upstream = {
      ...oldUpstream,
      spec_revision: 8,
      warmup_dialect_plugin: null,
    };
    let cascadeApplied = false;
    const fetchMock = vi.fn(
      async (
        input: RequestInfo | URL,
        init?: RequestInit,
      ): Promise<Response> => {
        const url = String(input);
        const method = init?.method ?? 'GET';

        if (method === 'GET' && url === '/admin/v1/upstreams') {
          return jsonResponse({
            upstreams: [cascadeApplied ? updatedUpstream : oldUpstream],
          });
        }
        if (
          method === 'GET' &&
          url === `/admin/v1/principals/${principalId}/plugin-chain`
        ) {
          return jsonResponse({
            entries: cascadeApplied ? [] : [oldChainEntry],
          });
        }
        if (
          method === 'DELETE' &&
          url === `/admin/v1/plugins/registry/${pluginId}?cascade=references`
        ) {
          cascadeApplied = true;
          return new Response(null, { status: 204 });
        }
        throw new Error(`Unexpected request: ${method} ${url}`);
      },
    );
    vi.stubGlobal('fetch', fetchMock);

    const client = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });
    const { result } = renderHook(
      () => {
        const upstreams = useUpstreams();
        const pluginChain = usePluginChain(principalId);
        const deletePlugin = useDeletePlugin();
        return {
          upstream: upstreams.data?.upstreams[0],
          chainEntries: pluginChain.data?.entries,
          deletePlugin: deletePlugin.mutateAsync,
        };
      },
      {
        wrapper: ({ children }: { readonly children: ReactNode }) =>
          createElement(QueryClientProvider, { client }, children),
      },
    );

    await waitFor(() => {
      expect(result.current.upstream?.spec_revision).toBe(7);
      expect(result.current.chainEntries?.[0]?.revision).toBe(3);
    });

    await act(async () => {
      await result.current.deletePlugin({
        id: pluginId,
        revision: 4,
        cascade: true,
        referenceFingerprint: 'references-v1',
      });
    });

    await waitFor(() => {
      expect(result.current.chainEntries).toEqual([]);
      expect(result.current.upstream?.spec_revision).toBe(8);
      expect(result.current.upstream?.warmup_dialect_plugin).toBeNull();
    });
  });
});
