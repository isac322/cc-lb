import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { renderHook, waitFor } from '@testing-library/react';
import { createElement, type ReactNode } from 'react';
import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest';
import {
  qk,
  useCacheKeepaliveSessionDetail,
  useCacheKeepaliveSessions,
  useCacheKeepaliveSummary,
} from '../../queries';
import {
  cacheKeepaliveListResponseFixture,
  cacheKeepaliveRenewedDetailFixture,
  cacheKeepaliveSummaryFixture,
  cacheKeepaliveSummaryOnlyResponseFixture,
} from '../../test-utils/cacheKeepalive-fixtures';

const PRINCIPAL_ID = 'principal-1';
const SESSION_ID = 'session/a%2Bb+c&d?e#한글';
const SESSION_DETAIL_PATH = `/admin/v1/principals/${PRINCIPAL_ID}/cache-keepalive/detail`;

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

function urlFrom(fetchMock: ReturnType<typeof vi.fn>) {
  const [input] = fetchMock.mock.calls[0] as [RequestInfo | URL];
  if (typeof input === 'string') {
    return input;
  }
  return input instanceof URL ? input.toString() : input.url;
}

beforeEach(() => {
  localStorage.clear();
});

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe('useCacheKeepaliveSummary', () => {
  test('fetches limit=0 and exposes the decoded card summary', async () => {
    const fetchMock = stubFetchOnce(cacheKeepaliveSummaryOnlyResponseFixture);
    const { result } = renderHook(
      () => useCacheKeepaliveSummary(PRINCIPAL_ID),
      { wrapper: makeWrapper(makeClient()) },
    );

    await waitFor(() => expect(result.current.isSuccess).toBe(true));

    expect(urlFrom(fetchMock)).toBe(
      `/admin/v1/principals/${PRINCIPAL_ID}/cache-keepalive?limit=0&horizon=24h`,
    );
    expect(result.current.data).toEqual(cacheKeepaliveSummaryFixture);
  });

  test('does not fetch while keep-alive is disabled', async () => {
    const fetchMock = stubFetchOnce(cacheKeepaliveSummaryOnlyResponseFixture);
    const { result, rerender } = renderHook(
      ({ enabled }) => useCacheKeepaliveSummary(PRINCIPAL_ID, enabled),
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
  });
});

describe('useCacheKeepaliveSessions', () => {
  test('does not fetch until its drawer is open', async () => {
    const fetchMock = stubFetchOnce(cacheKeepaliveListResponseFixture);
    const { result, rerender } = renderHook(
      ({ enabled }) => useCacheKeepaliveSessions(PRINCIPAL_ID, {}, enabled),
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
    expect(urlFrom(fetchMock)).toBe(
      `/admin/v1/principals/${PRINCIPAL_ID}/cache-keepalive`,
    );
  });

  test('fetches the session list and decodes every frozen fixture row', async () => {
    const fetchMock = stubFetchOnce(cacheKeepaliveListResponseFixture);
    const { result } = renderHook(
      () => useCacheKeepaliveSessions(PRINCIPAL_ID),
      { wrapper: makeWrapper(makeClient()) },
    );

    await waitFor(() => expect(result.current.isSuccess).toBe(true));

    expect(urlFrom(fetchMock)).toBe(
      `/admin/v1/principals/${PRINCIPAL_ID}/cache-keepalive`,
    );
    const firstPage = result.current.data?.pages[0];
    expect(firstPage?.rows).toHaveLength(7);
    expect(firstPage?.rows.map((row) => row.state)).toEqual([
      'renewed',
      'scheduled',
      'capped',
      'capped',
      'expired',
      'not_tracked',
      'renewed',
    ]);
    expect(result.current.hasNextPage).toBe(true);
  });
});

describe('useCacheKeepaliveSessionDetail', () => {
  test('fetches one session detail and decodes its turns', async () => {
    const fetchMock = stubFetchOnce(cacheKeepaliveRenewedDetailFixture);
    const { result } = renderHook(
      () => useCacheKeepaliveSessionDetail(PRINCIPAL_ID, SESSION_ID, '24h'),
      { wrapper: makeWrapper(makeClient()) },
    );

    await waitFor(() => expect(result.current.isSuccess).toBe(true));

    const detailUrl = new URL(urlFrom(fetchMock), 'http://localhost');
    expect(detailUrl.pathname).toBe(SESSION_DETAIL_PATH);
    expect(detailUrl.searchParams.get('id')).toBe(SESSION_ID);
    expect(detailUrl.searchParams.get('horizon')).toBe('24h');
    expect([...detailUrl.searchParams.keys()]).toEqual(['id', 'horizon']);
    expect(result.current.data?.turns).toHaveLength(3);
    expect(result.current.data?.config_snapshot?.snapshot_bytes).toBe(524_288);
  });

  test('selecting a different session never shows the previous session while it loads', async () => {
    const { promise: secondResponse, resolve: resolveSecond } =
      Promise.withResolvers<Response>();
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(jsonResponse(cacheKeepaliveRenewedDetailFixture))
      .mockImplementationOnce(() => secondResponse);
    vi.stubGlobal('fetch', fetchMock);
    const { result, rerender } = renderHook(
      ({ sessionId }) =>
        useCacheKeepaliveSessionDetail(PRINCIPAL_ID, sessionId, '24h'),
      {
        initialProps: { sessionId: SESSION_ID },
        wrapper: makeWrapper(makeClient()),
      },
    );
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    rerender({ sessionId: 'other-session' });
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2));
    expect(result.current.data).toBeUndefined();
    expect(result.current.isLoading).toBe(true);
    resolveSecond(
      jsonResponse({
        ...cacheKeepaliveRenewedDetailFixture,
        id: 'other-session',
      }),
    );
    await waitFor(() => expect(result.current.data?.id).toBe('other-session'));
  });

  test('an advancing list fingerprint starts a new request while the first detail is in flight', async () => {
    const { promise: firstResponse, resolve: resolveFirst } =
      Promise.withResolvers<Response>();
    const updated = { ...cacheKeepaliveRenewedDetailFixture, generation: 8 };
    const fetchMock = vi
      .fn()
      .mockImplementationOnce(() => firstResponse)
      .mockResolvedValue(jsonResponse(updated));
    vi.stubGlobal('fetch', fetchMock);
    const client = makeClient();
    const { result, rerender } = renderHook(
      ({ fingerprint }) =>
        useCacheKeepaliveSessionDetail(
          PRINCIPAL_ID,
          SESSION_ID,
          '24h',
          fingerprint,
        ),
      {
        initialProps: { fingerprint: 'generation-7' },
        wrapper: makeWrapper(client),
      },
    );
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1));
    rerender({ fingerprint: 'generation-8' });
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2));
    await waitFor(() => expect(result.current.data?.generation).toBe(8));
    resolveFirst(jsonResponse(cacheKeepaliveRenewedDetailFixture));
    await waitFor(() =>
      expect(
        client.getQueryState(
          qk.cacheKeepaliveSessionDetail(
            PRINCIPAL_ID,
            SESSION_ID,
            '24h',
            'generation-7',
          ),
        )?.status,
      ).toBe('success'),
    );
    expect(result.current.data?.generation).toBe(8);
  });

  test('same-generation economics changes refetch, but an unchanged fingerprint does not', async () => {
    const original = {
      ...cacheKeepaliveRenewedDetailFixture,
      is_active: false,
    };
    const updated = { ...original, net_pnl: 9, total_avoided: 10 };
    const { promise: nextResponse, resolve: resolveNext } =
      Promise.withResolvers<Response>();
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(jsonResponse(original))
      .mockImplementationOnce(() => nextResponse);
    vi.stubGlobal('fetch', fetchMock);
    const { result, rerender } = renderHook(
      ({ fingerprint }) =>
        useCacheKeepaliveSessionDetail(
          PRINCIPAL_ID,
          SESSION_ID,
          '24h',
          fingerprint,
        ),
      {
        initialProps: { fingerprint: 'economics-before' },
        wrapper: makeWrapper(makeClient()),
      },
    );
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    rerender({ fingerprint: 'economics-before' });
    expect(fetchMock).toHaveBeenCalledTimes(1);
    rerender({ fingerprint: 'economics-after' });
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2));
    expect(result.current.data).toEqual(original);
    expect(result.current.isPlaceholderData).toBe(true);
    resolveNext(jsonResponse(updated));
    await waitFor(() => expect(result.current.data?.net_pnl).toBe(9));
    expect(result.current.data?.generation).toBe(original.generation);
    rerender({ fingerprint: 'economics-after' });
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  test('stays idle and issues no request when sessionId is null', () => {
    const fetchMock = stubFetchOnce(cacheKeepaliveRenewedDetailFixture);
    const { result } = renderHook(
      () => useCacheKeepaliveSessionDetail(PRINCIPAL_ID, null),
      { wrapper: makeWrapper(makeClient()) },
    );

    expect(fetchMock).not.toHaveBeenCalled();
    expect(result.current.fetchStatus).toBe('idle');
    expect(result.current.data).toBeUndefined();
  });
});
