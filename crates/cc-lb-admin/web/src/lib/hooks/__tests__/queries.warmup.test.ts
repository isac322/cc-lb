import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { renderHook, waitFor } from '@testing-library/react';
import { createElement, type ReactNode } from 'react';
import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest';
import {
  qk,
  useClearUpstreamWarmupDialectPlugin,
  useFireNowUpstreamWarmup,
  useUpdateUpstreamWarmupSettings,
  useWarmupAttempts,
} from '../../queries';
import {
  makeFireNowError,
  makeFireNowLeaseHeld,
  makeFireNowSuccess,
  makeOauthUpstream,
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
});
describe('useFireNowUpstreamWarmup', () => {
  test('sends a no-body POST without If-Match', async () => {
    const fetchMock = stubFetchOnce(makeFireNowSuccess());
    const client = makeClient();
    const { result } = renderHook(() => useFireNowUpstreamWarmup(), {
      wrapper: makeWrapper(client),
    });

    await result.current.mutateAsync(UPSTREAM_ID);

    expect(fetchMock).toHaveBeenCalledTimes(1);
    const { url, init } = requestFrom(fetchMock);
    expect(url).toBe(`/admin/v1/upstreams/${UPSTREAM_ID}/warmup/fire-now`);
    expect(init?.method).toBe('POST');
    expect(init?.body).toBeUndefined();
    expect(headersFrom(init).has('If-Match')).toBe(false);
  });

  test('returns fired success and invalidates upstream detail before list', async () => {
    const response = makeFireNowSuccess();
    stubFetchOnce(response);
    const client = makeClient();
    const invalidateSpy = vi.spyOn(client, 'invalidateQueries');
    const { result } = renderHook(() => useFireNowUpstreamWarmup(), {
      wrapper: makeWrapper(client),
    });

    await expect(result.current.mutateAsync(UPSTREAM_ID)).resolves.toEqual({
      fired: true,
      cycle_key: 1718380800,
    });

    expect(invalidateSpy.mock.calls.map(([arg]) => arg?.queryKey)).toEqual([
      qk.upstream(UPSTREAM_ID),
      qk.upstreams,
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

    await expect(result.current.mutateAsync(UPSTREAM_ID)).resolves.toEqual({
      fired: false,
      reason: 'lease_held',
      held_by: 'replica-2',
    });

    expect(invalidateSpy.mock.calls.map(([arg]) => arg?.queryKey)).toEqual([
      qk.upstream(UPSTREAM_ID),
      qk.upstreams,
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

      await expect(result.current.mutateAsync(UPSTREAM_ID)).resolves.toEqual(
        response,
      );

      expect(invalidateSpy.mock.calls.map(([arg]) => arg?.queryKey)).toEqual([
        qk.upstream(UPSTREAM_ID),
        qk.upstreams,
      ]);
    },
  );
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

  test('returns updated upstream and invalidates upstream detail before list', async () => {
    const updatedUpstream = makeOauthUpstream({
      id: UPSTREAM_ID,
      spec_revision: SPEC_REVISION + 1,
      warmup_dialect_plugin: null,
    });
    stubFetchOnce(updatedUpstream);
    const client = makeClient();
    const invalidateSpy = vi.spyOn(client, 'invalidateQueries');
    const { result } = renderHook(() => useClearUpstreamWarmupDialectPlugin(), {
      wrapper: makeWrapper(client),
    });

    await expect(
      result.current.mutateAsync({
        id: UPSTREAM_ID,
        spec_revision: SPEC_REVISION,
      }),
    ).resolves.toEqual(updatedUpstream);

    expect(invalidateSpy.mock.calls.map(([arg]) => arg?.queryKey)).toEqual([
      qk.upstream(UPSTREAM_ID),
      qk.upstreams,
    ]);
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

  test('returns updated upstream and invalidates upstream detail before list', async () => {
    const patch = { warmup_enabled: false };
    const updatedUpstream = makeOauthUpstream({
      id: UPSTREAM_ID,
      spec_revision: SPEC_REVISION + 1,
      ...patch,
    });
    stubFetchOnce(updatedUpstream);
    const client = makeClient();
    const invalidateSpy = vi.spyOn(client, 'invalidateQueries');
    const { result } = renderHook(() => useUpdateUpstreamWarmupSettings(), {
      wrapper: makeWrapper(client),
    });

    await expect(
      result.current.mutateAsync({
        id: UPSTREAM_ID,
        spec_revision: SPEC_REVISION,
        body: patch,
      }),
    ).resolves.toEqual(updatedUpstream);

    expect(invalidateSpy.mock.calls.map(([arg]) => arg?.queryKey)).toEqual([
      qk.upstream(UPSTREAM_ID),
      qk.upstreams,
    ]);
  });
});
