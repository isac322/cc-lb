import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { act, renderHook, waitFor } from '@testing-library/react';
import { createElement, type ReactNode } from 'react';
import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest';
import {
  qk,
  useClearUpstreamWarmupDialectPlugin,
  useFireNowUpstreamWarmup,
  useUpdateUpstreamWarmupSettings,
  useWarmupAttempts,
  useWarmupSummary,
  warmupKeys,
} from '../../queries';
import {
  makeFireNowError,
  makeFireNowLeaseHeld,
  makeFireNowSuccess,
  makeOauthUpstream,
  makeWarmupLane,
} from '../../test-utils/warmup-fixtures';

const UPSTREAM_ID = 'oauth-1';
const SPEC_REVISION = 7;

function makeClient() {
  return new QueryClient({
    defaultOptions: {
      queries: { retry: false },
      mutations: { retry: false },
    },
  });
}

function makeWrapper(client: QueryClient) {
  return function Wrapper({ children }: { children: ReactNode }) {
    return createElement(QueryClientProvider, { client }, children);
  };
}

function jsonResponse(body: unknown, init?: ResponseInit) {
  return new Response(JSON.stringify(body), {
    status: 200,
    headers: { 'Content-Type': 'application/json' },
    ...init,
  });
}

function stubFetchOnce(body: unknown, init?: ResponseInit) {
  const fetchMock = vi.fn(async () => jsonResponse(body, init));
  vi.stubGlobal('fetch', fetchMock);
  return fetchMock;
}

function requestFrom(fetchMock: ReturnType<typeof vi.fn>) {
  const [input, init] = fetchMock.mock.calls[0] as [
    RequestInfo | URL,
    RequestInit | undefined,
  ];
  const url =
    typeof input === 'string'
      ? input
      : input instanceof URL
        ? input.toString()
        : input.url;
  return { url, init };
}

function headersFrom(init: RequestInit | undefined) {
  return new Headers(init?.headers);
}

beforeEach(() => {
  localStorage.clear();
});

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe('useWarmupAttempts', () => {
  test('does not fetch until its drawer is open', async () => {
    const fetchMock = stubFetchOnce({ attempts: [], next_cursor: null });
    const { result, rerender } = renderHook(
      ({ enabled }) => useWarmupAttempts(UPSTREAM_ID, { limit: 50 }, enabled),
      {
        initialProps: { enabled: false },
        wrapper: makeWrapper(makeClient()),
      },
    );

    expect(fetchMock).not.toHaveBeenCalled();
    expect(result.current.fetchStatus).toBe('idle');

    rerender({ enabled: true });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(requestFrom(fetchMock).url).toBe(
      `/admin/v1/upstreams/${UPSTREAM_ID}/warmup/attempts?limit=50`,
    );
  });

  test('sends the lane filter and keys the cache by lane', async () => {
    const fetchMock = stubFetchOnce({ attempts: [], next_cursor: null });
    const client = makeClient();
    const { result } = renderHook(
      () =>
        useWarmupAttempts(UPSTREAM_ID, {
          status: 'skipped',
          lane: 'third_party',
          limit: 50,
        }),
      { wrapper: makeWrapper(client) },
    );

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(requestFrom(fetchMock).url).toBe(
      `/admin/v1/upstreams/${UPSTREAM_ID}/warmup/attempts?status=skipped&lane=third_party&limit=50`,
    );
    expect(
      client.getQueryData(
        warmupKeys.attempts(UPSTREAM_ID, {
          status: 'skipped',
          lane: 'third_party',
          limit: 50,
        }),
      ),
    ).toBeDefined();
    expect(
      client.getQueryData(
        warmupKeys.attempts(UPSTREAM_ID, {
          status: 'skipped',
          lane: 'primary',
          limit: 50,
        }),
      ),
    ).toBeUndefined();
  });

  test.each([0, 1_799_913_600, 1_799_395_200])(
    'sends the inclusive time bound %s and keys the cache by it',
    async (sinceUnixSecs) => {
      const fetchMock = stubFetchOnce({ attempts: [], next_cursor: null });
      const client = makeClient();
      const { result } = renderHook(
        () =>
          useWarmupAttempts(UPSTREAM_ID, {
            status: 'success',
            lane: 'primary',
            limit: 50,
            sinceUnixSecs,
          }),
        { wrapper: makeWrapper(client) },
      );

      await waitFor(() => expect(result.current.isSuccess).toBe(true));
      const url = new URL(requestFrom(fetchMock).url, 'http://localhost');
      expect(Object.fromEntries(url.searchParams)).toEqual({
        status: 'success',
        lane: 'primary',
        limit: '50',
        since_unix_secs: String(sinceUnixSecs),
      });
      expect(
        client.getQueryData(
          warmupKeys.attempts(UPSTREAM_ID, {
            status: 'success',
            lane: 'primary',
            limit: 50,
            sinceUnixSecs,
          }),
        ),
      ).toBeDefined();
      expect(
        client.getQueryData(
          warmupKeys.attempts(UPSTREAM_ID, {
            status: 'success',
            lane: 'primary',
            limit: 50,
            sinceUnixSecs: sinceUnixSecs + 1,
          }),
        ),
      ).toBeUndefined();
    },
  );

  test('keeps the time bound on older pages and resets pagination for a new range', async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(
        jsonResponse({ attempts: [], next_cursor: 'older-page' }),
      )
      .mockResolvedValueOnce(jsonResponse({ attempts: [], next_cursor: null }))
      .mockResolvedValueOnce(jsonResponse({ attempts: [], next_cursor: null }))
      .mockResolvedValueOnce(jsonResponse({ attempts: [], next_cursor: null }));
    vi.stubGlobal('fetch', fetchMock);
    const since24h = 1_799_913_600;
    const since7d = 1_799_395_200;
    const initialProps: { sinceUnixSecs: number | undefined } = {
      sinceUnixSecs: since24h,
    };
    const { result, rerender } = renderHook(
      ({ sinceUnixSecs }) =>
        useWarmupAttempts(UPSTREAM_ID, { limit: 50, sinceUnixSecs }),
      {
        initialProps,
        wrapper: makeWrapper(makeClient()),
      },
    );
    await waitFor(() => expect(result.current.hasNextPage).toBe(true));

    await act(async () => {
      await result.current.fetchNextPage();
    });
    await waitFor(() => expect(result.current.data?.pages).toHaveLength(2));
    const olderUrl = new URL(
      String(fetchMock.mock.calls[1][0]),
      'http://localhost',
    );
    expect(olderUrl.searchParams.get('since_unix_secs')).toBe(String(since24h));
    expect(olderUrl.searchParams.get('before')).toBe('older-page');

    rerender({ sinceUnixSecs: since7d });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.pages).toHaveLength(1);
    expect(result.current.data?.pageParams).toEqual([undefined]);
    const weekUrl = new URL(
      String(fetchMock.mock.calls[2][0]),
      'http://localhost',
    );
    expect(weekUrl.searchParams.get('since_unix_secs')).toBe(String(since7d));
    expect(weekUrl.searchParams.has('before')).toBe(false);

    rerender({ sinceUnixSecs: undefined });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.pages).toHaveLength(1);
    expect(result.current.data?.pageParams).toEqual([undefined]);
    const allUrl = new URL(
      String(fetchMock.mock.calls[3][0]),
      'http://localhost',
    );
    expect(allUrl.searchParams.has('since_unix_secs')).toBe(false);
    expect(allUrl.searchParams.has('before')).toBe(false);
  });
});
describe('useFireNowUpstreamWarmup', () => {
  test('sends a no-body POST without If-Match', async () => {
    const fetchMock = stubFetchOnce(makeFireNowSuccess());
    const client = makeClient();
    const { result } = renderHook(() => useFireNowUpstreamWarmup(), {
      wrapper: makeWrapper(client),
    });

    await result.current.mutateAsync({ id: UPSTREAM_ID });

    expect(fetchMock).toHaveBeenCalledTimes(1);
    const { url, init } = requestFrom(fetchMock);
    expect(url).toBe(`/admin/v1/upstreams/${UPSTREAM_ID}/warmup/fire-now`);
    expect(init?.method).toBe('POST');
    expect(init?.body).toBeUndefined();
    expect(headersFrom(init).has('If-Match')).toBe(false);
  });

  test.each(['primary', 'third_party'] as const)(
    'names the %s lane in the fire-now query and keeps the body empty',
    async (lane) => {
      const fetchMock = stubFetchOnce({ ...makeFireNowSuccess(), lane });
      const { result } = renderHook(() => useFireNowUpstreamWarmup(), {
        wrapper: makeWrapper(makeClient()),
      });

      await result.current.mutateAsync({ id: UPSTREAM_ID, lane });

      const { url, init } = requestFrom(fetchMock);
      expect(url).toBe(
        `/admin/v1/upstreams/${UPSTREAM_ID}/warmup/fire-now?lane=${lane}`,
      );
      expect(init?.method).toBe('POST');
      expect(init?.body).toBeUndefined();
    },
  );

  test('returns fired success and invalidates upstream detail before list', async () => {
    const response = makeFireNowSuccess();
    stubFetchOnce(response);
    const client = makeClient();
    const invalidateSpy = vi.spyOn(client, 'invalidateQueries');
    const { result } = renderHook(() => useFireNowUpstreamWarmup(), {
      wrapper: makeWrapper(client),
    });

    await expect(
      result.current.mutateAsync({ id: UPSTREAM_ID }),
    ).resolves.toEqual({
      fired: true,
      cycle_key: 1718380800,
    });

    expect(invalidateSpy.mock.calls.map(([arg]) => arg?.queryKey)).toEqual([
      qk.upstream(UPSTREAM_ID),
      qk.upstreams,
      warmupKeys.summary(UPSTREAM_ID),
      warmupKeys.attemptsAll(UPSTREAM_ID),
    ]);
  });

  test('returns lease-held response and still invalidates queries', async () => {
    const response = makeFireNowLeaseHeld('replica-2');
    stubFetchOnce(response, { status: 202 });
    const client = makeClient();
    const invalidateSpy = vi.spyOn(client, 'invalidateQueries');
    const { result } = renderHook(() => useFireNowUpstreamWarmup(), {
      wrapper: makeWrapper(client),
    });

    await expect(
      result.current.mutateAsync({ id: UPSTREAM_ID }),
    ).resolves.toEqual({
      fired: false,
      reason: 'lease_held',
      held_by: 'replica-2',
    });

    expect(invalidateSpy.mock.calls.map(([arg]) => arg?.queryKey)).toEqual([
      qk.upstream(UPSTREAM_ID),
      qk.upstreams,
      warmupKeys.summary(UPSTREAM_ID),
      warmupKeys.attemptsAll(UPSTREAM_ID),
    ]);
  });

  test.each([
    [400, makeFireNowError('oauth_credentials_missing')],
    [502, makeFireNowError('auth_failed')],
    [503, makeFireNowError('seven_day_quota_exhausted')],
    [503, makeFireNowError('dialect_plugin_transient')],
  ])(
    'returns %i error body and still invalidates queries after fire-now attempt',
    async (status, response) => {
      stubFetchOnce(response, { status });
      const client = makeClient();
      const invalidateSpy = vi.spyOn(client, 'invalidateQueries');
      const { result } = renderHook(() => useFireNowUpstreamWarmup(), {
        wrapper: makeWrapper(client),
      });

      await expect(
        result.current.mutateAsync({ id: UPSTREAM_ID }),
      ).resolves.toEqual(response);

      expect(invalidateSpy.mock.calls.map(([arg]) => arg?.queryKey)).toEqual([
        qk.upstream(UPSTREAM_ID),
        qk.upstreams,
        warmupKeys.summary(UPSTREAM_ID),
        warmupKeys.attemptsAll(UPSTREAM_ID),
      ]);
    },
  );
});

describe('useFireNowUpstreamWarmup cache refresh', () => {
  test('refreshes the summary and every attempts filter of that upstream only', async () => {
    stubFetchOnce({ ...makeFireNowSuccess(), lane: 'third_party' });
    const client = makeClient();
    const summaryKey = warmupKeys.summary(UPSTREAM_ID);
    const allLanes = warmupKeys.attempts(UPSTREAM_ID, { limit: 50 });
    const oneLane = warmupKeys.attempts(UPSTREAM_ID, {
      lane: 'third_party',
      limit: 50,
    });
    const otherUpstream = warmupKeys.attempts('other', { limit: 50 });
    for (const key of [summaryKey, allLanes, oneLane, otherUpstream]) {
      client.setQueryData(key, { seeded: true });
    }
    const { result } = renderHook(() => useFireNowUpstreamWarmup(), {
      wrapper: makeWrapper(client),
    });

    await result.current.mutateAsync({ id: UPSTREAM_ID, lane: 'third_party' });

    expect(client.getQueryState(summaryKey)?.isInvalidated).toBe(true);
    expect(client.getQueryState(allLanes)?.isInvalidated).toBe(true);
    expect(client.getQueryState(oneLane)?.isInvalidated).toBe(true);
    expect(client.getQueryState(otherUpstream)?.isInvalidated).toBe(false);
  });

  test('refreshes the summary and attempts even when the request throws', async () => {
    stubFetchOnce({ error: 'boom' }, { status: 500 });
    const client = makeClient();
    const summaryKey = warmupKeys.summary(UPSTREAM_ID);
    const attemptsKey = warmupKeys.attempts(UPSTREAM_ID, { limit: 50 });
    client.setQueryData(summaryKey, { seeded: true });
    client.setQueryData(attemptsKey, { seeded: true });
    const { result } = renderHook(() => useFireNowUpstreamWarmup(), {
      wrapper: makeWrapper(client),
    });

    await expect(
      result.current.mutateAsync({ id: UPSTREAM_ID }),
    ).rejects.toBeTruthy();

    expect(client.getQueryState(summaryKey)?.isInvalidated).toBe(true);
    expect(client.getQueryState(attemptsKey)?.isInvalidated).toBe(true);
  });
});

describe('useClearUpstreamWarmupDialectPlugin', () => {
  test('sends DELETE with If-Match revision header', async () => {
    const updatedUpstream = makeOauthUpstream({
      id: UPSTREAM_ID,
      spec_revision: SPEC_REVISION + 1,
      warmup_dialect_plugin: null,
    });
    const fetchMock = stubFetchOnce(updatedUpstream);
    const client = makeClient();
    const { result } = renderHook(() => useClearUpstreamWarmupDialectPlugin(), {
      wrapper: makeWrapper(client),
    });

    await result.current.mutateAsync({
      id: UPSTREAM_ID,
      spec_revision: SPEC_REVISION,
    });

    expect(fetchMock).toHaveBeenCalledTimes(1);
    const { url, init } = requestFrom(fetchMock);
    expect(url).toBe(
      `/admin/v1/upstreams/${UPSTREAM_ID}/warmup-dialect-plugin`,
    );
    expect(init?.method).toBe('DELETE');
    expect(headersFrom(init).get('If-Match')).toBe(`W/"${SPEC_REVISION}"`);
  });
});

describe('useUpdateUpstreamWarmupSettings', () => {
  test('sends PATCH with If-Match revision header and patch body', async () => {
    const patch = {
      warmup_enabled: true,
      warmup_dialect_plugin: {
        wasm_registry_id: 'shape-plugin-1',
        config: { mode: 'anthropic' },
        wire_version: 1,
      },
    };
    const updatedUpstream = makeOauthUpstream({
      id: UPSTREAM_ID,
      spec_revision: SPEC_REVISION + 1,
      ...patch,
    });
    const fetchMock = stubFetchOnce(updatedUpstream);
    const client = makeClient();
    const { result } = renderHook(() => useUpdateUpstreamWarmupSettings(), {
      wrapper: makeWrapper(client),
    });

    await result.current.mutateAsync({
      id: UPSTREAM_ID,
      spec_revision: SPEC_REVISION,
      body: patch,
    });

    expect(fetchMock).toHaveBeenCalledTimes(1);
    const { url, init } = requestFrom(fetchMock);
    expect(url).toBe(`/admin/v1/upstreams/${UPSTREAM_ID}`);
    expect(init?.method).toBe('PATCH');
    expect(headersFrom(init).get('If-Match')).toBe(`W/"${SPEC_REVISION}"`);
    expect(init?.body).toBe(JSON.stringify(patch));
  });

  test('PATCH body can carry enabled and warmup_enabled atomically', async () => {
    const patch = { enabled: true, warmup_enabled: true };
    const updatedUpstream = makeOauthUpstream({
      id: UPSTREAM_ID,
      spec_revision: SPEC_REVISION + 1,
      ...patch,
    });
    const fetchMock = stubFetchOnce(updatedUpstream);
    const client = makeClient();
    const { result } = renderHook(() => useUpdateUpstreamWarmupSettings(), {
      wrapper: makeWrapper(client),
    });

    await result.current.mutateAsync({
      id: UPSTREAM_ID,
      spec_revision: SPEC_REVISION,
      body: patch,
    });

    expect(fetchMock).toHaveBeenCalledTimes(1);
    const { url, init } = requestFrom(fetchMock);
    expect(url).toBe(`/admin/v1/upstreams/${UPSTREAM_ID}`);
    expect(init?.method).toBe('PATCH');
    expect(init?.body).toBe(JSON.stringify(patch));
  });
});

describe('warm-up plugin snapshot after mutation', () => {
  test.each([
    ['save', false],
    ['clear', false],
    ['save', true],
    ['clear', true],
  ] as const)(
    '%s refreshes the effective snapshot with initial summary pending=%s',
    async (operation, initialSummaryPending) => {
      const previousPlugin = {
        wasm_registry_id: 'shape-before',
        wire_version: 1,
        config: { source: 'before' },
      };
      const nextPlugin = {
        wasm_registry_id: 'shape-after',
        wire_version: 2,
        config: { source: 'after' },
      };
      let upstream = makeOauthUpstream({
        id: UPSTREAM_ID,
        spec_revision: SPEC_REVISION,
        warmup_dialect_plugin: previousPlugin,
      });
      let initialSummaryStarted = false;
      const initialSummaryGate = Promise.withResolvers<void>();
      vi.stubGlobal(
        'fetch',
        vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
          const url = String(input);
          if (url.endsWith('/warmup')) {
            const response = jsonResponse({
              upstream_id: UPSTREAM_ID,
              dialect_plugin: upstream.warmup_dialect_plugin,
              lanes: [makeWarmupLane()],
            });
            if (initialSummaryPending && !initialSummaryStarted) {
              initialSummaryStarted = true;
              await initialSummaryGate.promise;
            }
            return response;
          }
          if (init?.method === 'PATCH') {
            const patch = JSON.parse(String(init.body));
            upstream = {
              ...upstream,
              ...patch,
              spec_revision: upstream.spec_revision + 1,
            };
          } else if (init?.method === 'DELETE') {
            upstream = {
              ...upstream,
              warmup_dialect_plugin: null,
              spec_revision: upstream.spec_revision + 1,
            };
          }
          return jsonResponse(upstream);
        }),
      );
      const client = makeClient();
      const { result, unmount } = renderHook(
        () => ({
          summary: useWarmupSummary(UPSTREAM_ID),
          save: useUpdateUpstreamWarmupSettings(),
          clear: useClearUpstreamWarmupDialectPlugin(),
        }),
        { wrapper: makeWrapper(client) },
      );
      try {
        if (initialSummaryPending) {
          await waitFor(() => expect(initialSummaryStarted).toBe(true));
        } else {
          await waitFor(() =>
            expect(result.current.summary.data?.dialect_plugin).toEqual(
              previousPlugin,
            ),
          );
        }
        const mutation =
          operation === 'save'
            ? result.current.save.mutateAsync({
                id: UPSTREAM_ID,
                spec_revision: SPEC_REVISION,
                body: { warmup_dialect_plugin: nextPlugin },
              })
            : result.current.clear.mutateAsync({
                id: UPSTREAM_ID,
                spec_revision: SPEC_REVISION,
              });
        if (initialSummaryPending) {
          await waitFor(() =>
            expect(upstream.warmup_dialect_plugin).toEqual(
              operation === 'save' ? nextPlugin : null,
            ),
          );
          initialSummaryGate.resolve();
        }
        await mutation;
        await waitFor(() =>
          expect(result.current.summary.data?.dialect_plugin).toEqual(
            operation === 'save' ? nextPlugin : null,
          ),
        );
      } finally {
        initialSummaryGate.resolve();
        unmount();
        client.clear();
      }
    },
  );
});
