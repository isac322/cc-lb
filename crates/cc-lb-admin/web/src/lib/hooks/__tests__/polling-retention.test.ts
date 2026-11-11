import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { act, renderHook, waitFor } from '@testing-library/react';
import { createElement, type ReactNode } from 'react';
import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest';
import {
  buildPoolQuotaChartData,
  POOL_QUOTA_HEARTBEAT_CARRY_SECS,
  recordedPoolQuotaChartData,
} from '../../../routes/-overviewPoolQuota';
import type { PoolHistoryResponse, SeriesResponse } from '../../api';
import {
  POLLING_INTERVALS,
  useSubscriptionQuotaPoolHistory,
  useSubscriptionQuotaSeries,
} from '../../queries';

type PendingRequest = {
  readonly url: URL;
  resolve(response: Response): void;
};

function makeClient() {
  return new QueryClient({
    defaultOptions: {
      queries: {
        retry: false,
        refetchOnWindowFocus: false,
      },
    },
  });
}

function makeWrapper(client: QueryClient) {
  return function Wrapper({ children }: { readonly children: ReactNode }) {
    return createElement(QueryClientProvider, { client }, children);
  };
}

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json' },
  });
}

function quotaSeries(
  upstreamId: string,
  upstreamName: string,
  sinceUnixSecs: number,
  untilUnixSecs: number,
  utilization: number,
): SeriesResponse {
  return {
    since_unix_secs: sinceUnixSecs,
    until_unix_secs: untilUnixSecs,
    bucket_secs: 60,
    source: 'merged',
    series: [
      {
        upstream_id: upstreamId,
        upstream_name: upstreamName,
        window: '5h',
        buckets: [
          {
            bucket_start_unix_secs: untilUnixSecs - 60,
            utilization_last: utilization,
          },
        ],
        markers: [],
      },
    ],
  };
}

function installDeferredFetch() {
  const requests: PendingRequest[] = [];
  const fetchMock = vi.fn((input: RequestInfo | URL) => {
    const url = new URL(
      typeof input === 'string'
        ? input
        : input instanceof URL
          ? input.toString()
          : input.url,
      'http://cc-lb.test',
    );
    return new Promise<Response>((resolve) => {
      requests.push({ url, resolve });
    });
  });
  vi.stubGlobal('fetch', fetchMock);
  return requests;
}

async function resolveRequest(request: PendingRequest, response: Response) {
  await act(async () => {
    request.resolve(response);
    await Promise.resolve();
  });
}

function requestBounds(request: PendingRequest) {
  return {
    since: Number(request.url.searchParams.get('since_unix_secs')),
    until: Number(request.url.searchParams.get('until_unix_secs')),
  };
}

beforeEach(() => {
  localStorage.clear();
  vi.useFakeTimers({ shouldAdvanceTime: true });
  vi.setSystemTime(new Date('2026-09-07T12:00:00.000Z'));
});

afterEach(() => {
  vi.unstubAllGlobals();
  vi.clearAllTimers();
  vi.useRealTimers();
});

describe('quota polling retention', () => {
  test('keeps successful data through a same-range refresh failure and replaces it only after recovery', async () => {
    const requests = installDeferredFetch();
    const client = makeClient();
    const wrapper = makeWrapper(client);
    const rangeSecs = 3_600;
    const upstreamId = 'upstream-alpha';
    const { result, unmount } = renderHook(
      () => {
        const query = useSubscriptionQuotaSeries({
          upstreamIds: upstreamId,
          windows: '5h',
          source: 'merged',
          rangeSecs,
          bucketSecs: 60,
        });
        return {
          data: query.data,
          isError: query.isError,
          isFetching: query.isFetching,
          isPlaceholderData: query.isPlaceholderData,
        };
      },
      { wrapper },
    );

    await waitFor(() => expect(requests).toHaveLength(1));
    const initialBounds = requestBounds(requests[0]);
    expect(initialBounds.until - initialBounds.since).toBe(rangeSecs);
    const initialPayload = quotaSeries(
      upstreamId,
      'Alpha',
      initialBounds.since,
      initialBounds.until,
      0.31,
    );
    await resolveRequest(requests[0], jsonResponse(initialPayload));
    await waitFor(() => expect(result.current.data).toEqual(initialPayload));
    const retainedData = result.current.data;

    vi.setSystemTime(new Date('2026-09-07T12:01:00.000Z'));
    await act(async () => {
      await vi.advanceTimersByTimeAsync(POLLING_INTERVALS.QUOTA_SERIES_MS);
    });
    await waitFor(() => expect(requests).toHaveLength(2));
    const refreshBounds = requestBounds(requests[1]);
    expect(refreshBounds.until).toBeGreaterThan(initialBounds.until);
    expect(refreshBounds.until - refreshBounds.since).toBe(rangeSecs);
    expect(result.current.isFetching).toBe(true);
    expect(result.current.data).toBe(retainedData);

    await resolveRequest(
      requests[1],
      jsonResponse({ error: 'temporary refresh failure' }, 500),
    );
    await waitFor(() => expect(result.current.isFetching).toBe(false));
    expect(result.current.isError).toBe(true);
    expect(result.current.data).toBe(retainedData);

    vi.setSystemTime(new Date('2026-09-07T12:02:00.000Z'));
    await act(async () => {
      await vi.advanceTimersByTimeAsync(POLLING_INTERVALS.QUOTA_SERIES_MS);
    });
    await waitFor(() => expect(requests).toHaveLength(3));
    expect(result.current.data).toBe(retainedData);

    const recoveryBounds = requestBounds(requests[2]);
    const recoveredPayload = quotaSeries(
      upstreamId,
      'Alpha recovered',
      recoveryBounds.since,
      recoveryBounds.until,
      0.42,
    );
    await resolveRequest(requests[2], jsonResponse(recoveredPayload));
    await waitFor(() => expect(result.current.data).toEqual(recoveredPayload));
    expect(result.current.isError).toBe(false);
    expect(result.current.isPlaceholderData).toBe(false);

    unmount();
    client.clear();
  });

  test('uses same-identity data as a range placeholder but never crosses upstream identity', async () => {
    const requests = installDeferredFetch();
    const client = makeClient();
    const wrapper = makeWrapper(client);
    const initialRangeSecs = 3_600;
    const expandedRangeSecs = 21_600;
    const { result, rerender, unmount } = renderHook(
      ({ upstreamId, rangeSecs }) => {
        const query = useSubscriptionQuotaSeries({
          upstreamIds: upstreamId,
          windows: '5h',
          source: 'merged',
          rangeSecs,
          bucketSecs: 60,
        });
        return {
          data: query.data,
          isPending: query.isPending,
          isPlaceholderData: query.isPlaceholderData,
        };
      },
      {
        initialProps: {
          upstreamId: 'upstream-alpha',
          rangeSecs: initialRangeSecs,
        },
        wrapper,
      },
    );

    await waitFor(() => expect(requests).toHaveLength(1));
    const initialBounds = requestBounds(requests[0]);
    const alphaPayload = quotaSeries(
      'upstream-alpha',
      'Alpha',
      initialBounds.since,
      initialBounds.until,
      0.31,
    );
    await resolveRequest(requests[0], jsonResponse(alphaPayload));
    await waitFor(() => expect(result.current.data).toEqual(alphaPayload));

    vi.setSystemTime(new Date('2026-09-07T12:03:00.000Z'));
    rerender({
      upstreamId: 'upstream-alpha',
      rangeSecs: expandedRangeSecs,
    });
    await waitFor(() => expect(requests).toHaveLength(2));
    const expandedBounds = requestBounds(requests[1]);
    expect(expandedBounds.until - expandedBounds.since).toBe(expandedRangeSecs);
    expect(result.current.isPlaceholderData).toBe(true);
    expect(result.current.data).toEqual(alphaPayload);

    const expandedPayload = quotaSeries(
      'upstream-alpha',
      'Alpha expanded',
      expandedBounds.since,
      expandedBounds.until,
      0.42,
    );
    await resolveRequest(requests[1], jsonResponse(expandedPayload));
    await waitFor(() => expect(result.current.data).toEqual(expandedPayload));
    expect(result.current.isPlaceholderData).toBe(false);

    rerender({
      upstreamId: 'upstream-beta',
      rangeSecs: expandedRangeSecs,
    });
    await waitFor(() => expect(requests).toHaveLength(3));
    expect(requests[2].url.searchParams.get('upstream_ids')).toBe(
      'upstream-beta',
    );
    expect(result.current.isPending).toBe(true);
    expect(result.current.isPlaceholderData).toBe(false);
    expect(result.current.data).toBeUndefined();

    const betaBounds = requestBounds(requests[2]);
    const betaPayload = quotaSeries(
      'upstream-beta',
      'Beta',
      betaBounds.since,
      betaBounds.until,
      0.18,
    );
    await resolveRequest(requests[2], jsonResponse(betaPayload));
    await waitFor(() => expect(result.current.data).toEqual(betaPayload));

    unmount();
    client.clear();
  });
});

describe('pool history persisted predecessor lookback', () => {
  const providers = [
    { provider: 'openai', window: '5h' },
    { provider: 'gemini', window: 'gemini_5h' },
  ] as const;

  test.each(['anthropic', 'openai', 'gemini'] as const)(
    'keeps %s query anchors outside the heartbeat or preserves Anthropic bounds',
    async (provider) => {
      vi.setSystemTime(new Date(4500 * 1000));
      const requests = installDeferredFetch();
      const client = makeClient();
      const { result, unmount } = renderHook(
        () => {
          const query = useSubscriptionQuotaPoolHistory({
            provider,
            rangeSecs: 3600,
            windowQuantumSecs: 1800,
            maxPointsPerSeries: 1000,
          });
          return { data: query.data };
        },
        { wrapper: makeWrapper(client) },
      );
      await waitFor(() => expect(requests).toHaveLength(1));
      const bounds = requestBounds(requests[0]);
      expect(bounds.until).toBe(5400);
      if (provider === 'anthropic') {
        expect(bounds.since).toBe(0);
      } else {
        expect(900 - bounds.since).toBeGreaterThan(
          POOL_QUOTA_HEARTBEAT_CARRY_SECS,
        );
      }
      await resolveRequest(
        requests[0],
        jsonResponse({ now_unix_secs: 4500, windows: [] }),
      );
      await waitFor(() => expect(result.current.data?.range_secs).toBe(3600));
      unmount();
      client.clear();
    },
  );

  describe.each(providers)('$provider $window', ({ provider, window }) => {
    test.each(['ancient', 'healthy', 'null'] as const)(
      'clips the real query response with a %s predecessor without live substitution',
      async (predecessor) => {
        vi.setSystemTime(new Date(4500 * 1000));
        const requests = installDeferredFetch();
        const client = makeClient();
        const { result, unmount } = renderHook(
          () => {
            const query = useSubscriptionQuotaPoolHistory({
              provider,
              windows: window,
              rangeSecs: 3600,
              windowQuantumSecs: 1800,
              maxPointsPerSeries: 1000,
            });
            return {
              data: query.data,
              rows: recordedPoolQuotaChartData(
                buildPoolQuotaChartData(query.data?.windows),
                900,
                4500,
              ),
            };
          },
          { wrapper: makeWrapper(client) },
        );
        await waitFor(() => expect(requests).toHaveLength(1));
        const bounds = requestBounds(requests[0]);
        // Both storage adapters replace a pre-query snapshot (here -86400)
        // with `since`, then floor chart points to the server-selected bucket.
        const bucketSecs = Math.ceil((bounds.until - bounds.since) / 999);
        const persistedUnix = predecessor === 'ancient' ? -86400 : 0;
        const anchorUnix =
          Math.floor(Math.max(persistedUnix, bounds.since) / bucketSecs) *
          bucketSecs;
        const recoveryUnix = Math.floor(3600 / bucketSecs) * bucketSecs;
        const payload: PoolHistoryResponse = {
          now_unix_secs: 4500,
          windows: [
            {
              window,
              latest: {
                snapshot_at_unix_secs: 4500,
                utilization: 0.64,
                utilization_percent: 64,
                contributing_upstreams: 1,
                eligible_upstreams: 1,
                stale_upstreams: 0,
                max_observed_at_unix_millis: 4500 * 1000,
              },
              series: [
                {
                  snapshot_at_unix_secs: anchorUnix,
                  utilization_percent: predecessor === 'null' ? null : 40,
                },
                {
                  snapshot_at_unix_secs: recoveryUnix,
                  utilization_percent: 40,
                },
              ],
            },
          ],
        };
        await resolveRequest(requests[0], jsonResponse(payload));
        await waitFor(() => expect(result.current.data?.range_secs).toBe(3600));
        expect(result.current.rows).toEqual(
          predecessor === 'ancient'
            ? [{ unix: 3600, [window]: 40 }]
            : [
                { unix: 900, [window]: predecessor === 'null' ? null : 40 },
                { unix: 3600, [window]: 40 },
              ],
        );
        unmount();
        client.clear();
      },
    );
  });
});
