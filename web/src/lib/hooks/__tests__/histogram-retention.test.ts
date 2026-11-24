import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { act, renderHook, waitFor } from '@testing-library/react';
import { createElement, type ReactNode } from 'react';
import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest';
import type { EventsHistogramPayload, EventsHistogramRange } from '../../api';
import { POLLING_INTERVALS, useEventsHistogram } from '../../queries';

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

function jsonResponse(body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status: 200,
    headers: { 'Content-Type': 'application/json' },
  });
}

function histogram(totalCount: number): EventsHistogramPayload {
  return {
    buckets: [
      {
        bucket_start_unix_secs: 100,
        total_count: totalCount,
        error_count: 0,
      },
    ],
    bucket_ms: 60_000,
    bucket_count: 1,
  };
}

function installDeferredFetch() {
  const requests: PendingRequest[] = [];
  vi.stubGlobal(
    'fetch',
    vi.fn((input: RequestInfo | URL) => {
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
    }),
  );
  return requests;
}

async function resolveRequest(request: PendingRequest, payload: unknown) {
  await act(async () => {
    request.resolve(jsonResponse(payload));
    await Promise.resolve();
  });
}

const INITIAL_RANGE: EventsHistogramRange = {
  sinceSecs: 100,
  untilSecs: 200,
  bucketMs: 60_000,
};
const PANNED_RANGE: EventsHistogramRange = {
  sinceSecs: 200,
  untilSecs: 300,
  bucketMs: 60_000,
};

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

describe('event histogram retention', () => {
  test('retains the same filter density through polling and a pending pan', async () => {
    const requests = installDeferredFetch();
    const client = makeClient();
    const wrapper = makeWrapper(client);
    const { result, rerender, unmount } = renderHook(
      ({ range }) =>
        useEventsHistogram({ principal_id: 'principal-alpha' }, range),
      {
        initialProps: { range: INITIAL_RANGE },
        wrapper,
      },
    );

    await waitFor(() => expect(requests).toHaveLength(1));
    const initialPayload = histogram(10);
    await resolveRequest(requests[0], initialPayload);
    await waitFor(() => expect(result.current.data).toEqual(initialPayload));

    await act(async () => {
      await vi.advanceTimersByTimeAsync(POLLING_INTERVALS.RECENT_EVENTS_MS);
    });
    await waitFor(() => expect(requests).toHaveLength(2));
    expect(result.current.data).toEqual(initialPayload);

    const polledPayload = histogram(11);
    await resolveRequest(requests[1], polledPayload);
    await waitFor(() => expect(result.current.data).toEqual(polledPayload));

    rerender({ range: PANNED_RANGE });
    await waitFor(() => {
      expect(requests).toHaveLength(3);
      expect(result.current.data).toEqual(polledPayload);
    });

    const pannedPayload = histogram(12);
    await resolveRequest(requests[2], pannedPayload);
    await waitFor(() => expect(result.current.data).toEqual(pannedPayload));

    unmount();
    client.clear();
  });

  test('drops prior filter density and ignores its delayed range result', async () => {
    const requests = installDeferredFetch();
    const client = makeClient();
    const wrapper = makeWrapper(client);
    const { result, rerender, unmount } = renderHook(
      ({ principalId, range }) =>
        useEventsHistogram({ principal_id: principalId }, range, {
          poll: false,
        }),
      {
        initialProps: {
          principalId: 'principal-alpha',
          range: INITIAL_RANGE,
        },
        wrapper,
      },
    );

    await waitFor(() => expect(requests).toHaveLength(1));
    const alphaPayload = histogram(40);
    await resolveRequest(requests[0], alphaPayload);
    await waitFor(() => expect(result.current.data).toEqual(alphaPayload));

    rerender({ principalId: 'principal-alpha', range: PANNED_RANGE });
    await waitFor(() => {
      expect(requests).toHaveLength(2);
      expect(result.current.data).toEqual(alphaPayload);
    });

    rerender({ principalId: 'principal-beta', range: PANNED_RANGE });
    await waitFor(() => {
      expect(requests).toHaveLength(3);
      expect(result.current.data).toBeUndefined();
    });

    await resolveRequest(requests[1], histogram(41));
    expect(result.current.data).toBeUndefined();

    const betaPayload = histogram(2);
    await resolveRequest(requests[2], betaPayload);
    await waitFor(() => expect(result.current.data).toEqual(betaPayload));

    unmount();
    client.clear();
  });
});
