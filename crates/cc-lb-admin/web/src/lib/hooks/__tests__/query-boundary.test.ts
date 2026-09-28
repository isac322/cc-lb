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
  type Principal,
  qk,
  type Upstream,
  useDeletePlugin,
  usePluginChain,
  usePluginReferences,
  usePrincipals,
  usePrincipalWritePending,
  useRouterTerminalStrategy,
  useSetAllowedModels,
  useUpdateRouterTerminalStrategy,
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

  it('refreshes cascade side effects without refetching deleted plugin references', async () => {
    const principalId = 'principal-1';
    const pluginId = 'plugin-1';
    const oldChainEntry: PluginChainEntry = {
      id: 'chain-entry-1',
      principal_id: principalId,
      slot: 'shape',
      order: 0,
      wasm_registry_id: pluginId,
      config: {},
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
    let referenceGetCount = 0;
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
          url === `/admin/v1/plugins/registry/${pluginId}/references`
        ) {
          referenceGetCount += 1;
          if (cascadeApplied) {
            return new Response(
              JSON.stringify({ error: 'unknown_registry_entry' }),
              {
                status: 404,
                headers: { 'Content-Type': 'application/json' },
              },
            );
          }
          return jsonResponse({
            registry: {
              id: pluginId,
              name: 'Plugin 1',
              refcount: 2,
              revision: 4,
            },
            refcount: 2,
            reference_fingerprint: 'references-v1',
            references: [],
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
    const { result, unmount } = renderHook(
      () => {
        const upstreams = useUpstreams();
        const pluginChain = usePluginChain(principalId);
        const deletePlugin = useDeletePlugin();
        const pluginReferences = usePluginReferences(pluginId);
        return {
          upstream: upstreams.data?.upstreams[0],
          chainEntries: pluginChain.data?.entries,
          referenceFingerprint: pluginReferences.data?.reference_fingerprint,
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
      expect(result.current.referenceFingerprint).toBe('references-v1');
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
      expect(referenceGetCount).toBe(1);
    });

    unmount();
    await waitFor(() => {
      expect(
        client.getQueryData(qk.pluginReferences(pluginId)),
      ).toBeUndefined();
    });
  });

  it('keeps principal writes pending until Router has the new server revision', async () => {
    const principalId = 'principal-1';
    const principalRecord = (revision: number): Principal => ({
      id: principalId,
      name: 'Principal 1',
      kind: 'machine',
      enabled: true,
      revision,
      allowed_models: revision > 7 ? ['claude-sonnet'] : [],
      allowed_upstreams: [],
      default_limits: [],
      cache_keepalive: null,
    });
    let revision = 7;
    let strategy = 'first-pick';
    let terminalGetCount = 0;
    let resolveTerminalRefresh: ((response: Response) => void) | undefined;
    let terminalUpdateIfMatch: string | null = null;

    const fetchMock = vi.fn(
      async (
        input: RequestInfo | URL,
        init?: RequestInit,
      ): Promise<Response> => {
        const url = String(input);
        const method = init?.method ?? 'GET';

        if (method === 'GET' && url === '/admin/v1/principals') {
          return jsonResponse({ principals: [principalRecord(revision)] });
        }
        if (
          method === 'GET' &&
          url === `/admin/v1/principals/${principalId}/router-terminal`
        ) {
          terminalGetCount += 1;
          if (terminalGetCount === 2) {
            return new Promise<Response>((resolve) => {
              resolveTerminalRefresh = resolve;
            });
          }
          return jsonResponse({ strategy, revision });
        }
        if (
          method === 'PUT' &&
          url === `/admin/v1/principals/${principalId}/allowed_models`
        ) {
          revision = 8;
          return jsonResponse(principalRecord(revision));
        }
        if (
          method === 'PUT' &&
          url === `/admin/v1/principals/${principalId}/router-terminal`
        ) {
          terminalUpdateIfMatch = new Headers(init?.headers).get('If-Match');
          if (terminalUpdateIfMatch !== 'W/"8"') {
            return new Response(JSON.stringify({ error: 'stale_revision' }), {
              status: 409,
              headers: { 'Content-Type': 'application/json' },
            });
          }
          strategy = 'random';
          revision = 9;
          return jsonResponse({ strategy, revision });
        }
        throw new Error(`Unexpected request: ${method} ${url}`);
      },
    );
    vi.stubGlobal('fetch', fetchMock);

    const client = new QueryClient({
      defaultOptions: {
        queries: { retry: false },
        mutations: { retry: false },
      },
    });
    const { result } = renderHook(
      () => {
        const principals = usePrincipals();
        const principalWritePending = usePrincipalWritePending(principalId);
        const terminal = useRouterTerminalStrategy(principalId);
        const setAllowed = useSetAllowedModels();
        const updateTerminal = useUpdateRouterTerminalStrategy();
        return {
          principalRevision: principals.data?.principals[0]?.revision,
          terminalRevision: terminal.data?.revision,
          terminalStrategy: terminal.data?.strategy,
          principalWritePending,
          setAllowed: setAllowed.mutateAsync,
          updateTerminal: updateTerminal.mutateAsync,
        };
      },
      {
        wrapper: ({ children }: { readonly children: ReactNode }) =>
          createElement(QueryClientProvider, { client }, children),
      },
    );

    await waitFor(() => {
      expect(result.current.principalRevision).toBe(7);
      expect(result.current.terminalRevision).toBe(7);
    });

    let allowedWrite!: Promise<Principal>;
    act(() => {
      allowedWrite = result.current.setAllowed({
        id: principalId,
        models: ['claude-sonnet'],
        expected_revision: 7,
      });
    });

    await waitFor(() => {
      expect(resolveTerminalRefresh).toBeDefined();
      expect(result.current.principalRevision).toBe(8);
      expect(result.current.terminalRevision).toBe(7);
      expect(result.current.principalWritePending).toBe(1);
    });

    await act(async () => {
      resolveTerminalRefresh?.(jsonResponse({ strategy, revision }));
      await allowedWrite;
    });

    await waitFor(() => {
      expect(result.current.terminalRevision).toBe(8);
      expect(result.current.principalWritePending).toBe(0);
    });

    await act(async () => {
      await result.current.updateTerminal({
        id: principalId,
        strategy: 'random',
        revision: result.current.terminalRevision!,
      });
    });

    expect(terminalUpdateIfMatch).toBe('W/"8"');
    await waitFor(() => {
      expect(result.current.terminalRevision).toBe(9);
      expect(result.current.terminalStrategy).toBe('random');
    });
  });
});
