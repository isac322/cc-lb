import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { renderHook, waitFor } from '@testing-library/react';
import { createElement, type ReactNode } from 'react';
import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest';
import {
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
const SESSION_ID = 'a1f39c2b7e04';

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
      `/admin/v1/principals/${PRINCIPAL_ID}/cache-keepalive?limit=0`,
    );
    expect(result.current.data).toEqual(cacheKeepaliveSummaryFixture);
  });
});

describe('useCacheKeepaliveSessions', () => {
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
      () => useCacheKeepaliveSessionDetail(PRINCIPAL_ID, SESSION_ID),
      { wrapper: makeWrapper(makeClient()) },
    );

    await waitFor(() => expect(result.current.isSuccess).toBe(true));

    expect(urlFrom(fetchMock)).toBe(
      `/admin/v1/principals/${PRINCIPAL_ID}/cache-keepalive/${SESSION_ID}`,
    );
    expect(result.current.data?.turns).toHaveLength(3);
    expect(result.current.data?.config_snapshot?.snapshot_bytes).toBe(524_288);
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
