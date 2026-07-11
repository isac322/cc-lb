import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { act, renderHook } from '@testing-library/react';
import {
  createEventSource,
  type EventSourceMessage,
  type EventSourceOptions,
} from 'eventsource-client';
import { createElement, type ReactNode } from 'react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import type { RequestEvent, RequestEventUpdate } from '../../api';
import { MAX_LIVE_PARTIAL_EVENTS } from '../../upsertReducer';
import { useLiveEventStream } from '../../useLiveEventStream';

vi.mock('eventsource-client', () => ({
  createEventSource: vi.fn(() => ({ close: vi.fn() })),
}));

vi.mock('../../auth', () => ({
  getAdminToken: vi.fn(() => 'test-token'),
}));

vi.mock('../../api', () => ({
  getJson: vi.fn(),
  RequestEventUpdateSchema: {
    safeParse: vi.fn((data: unknown) => ({ success: true, data })),
  },
}));

function makeWrapper(client: QueryClient) {
  return function Wrapper({ children }: { readonly children: ReactNode }) {
    return createElement(QueryClientProvider, { client }, children);
  };
}

function latestOptions(): EventSourceOptions {
  const input = vi.mocked(createEventSource).mock.calls.at(-1)?.[0];
  if (
    input === undefined ||
    typeof input === 'string' ||
    input instanceof URL
  ) {
    throw new Error('Expected EventSource options');
  }
  return input;
}

function makePartialMessage(index: number): EventSourceMessage {
  const update: RequestEventUpdate = {
    phase: 'partial',
    payload: {
      event_id: `event-${index}`,
      request_id: `request-${index}`,
      ts: index / 1000,
      ts_ms: index,
      last_update_ms: index,
    },
  };
  return { data: JSON.stringify(update) };
}

function makeFinalMessage(index: number): EventSourceMessage {
  const update: RequestEventUpdate = {
    phase: 'final',
    payload: {
      event: {
        event_id: `final-${index}`,
        request_id: `final-request-${index}`,
        ts: index / 1000,
        ts_ms: index,
        status: 200,
        duration_ms: 10,
      } satisfies RequestEvent,
      cursor: index,
    },
  };
  return { data: JSON.stringify(update) };
}

describe('useLiveEventStream partial cap', () => {
  afterEach(() => vi.clearAllMocks());

  it('keeps a burst of partial frames within the partial and total live caps', () => {
    const client = new QueryClient();
    const { result } = renderHook(() => useLiveEventStream({}), {
      wrapper: makeWrapper(client),
    });
    const options = latestOptions();

    act(() => {
      for (let index = 0; index < 500; index += 1) {
        options.onMessage?.(makeFinalMessage(index));
      }
      for (let index = 0; index < 2000; index += 1) {
        options.onMessage?.(makePartialMessage(index));
      }
    });

    expect(result.current.eventsMap.size).toBe(1000);
    expect(
      [...result.current.eventsMap.values()].filter(
        ({ phase }) => phase === 'partial',
      ),
    ).toHaveLength(MAX_LIVE_PARTIAL_EVENTS);
    expect(
      [...result.current.eventsMap.values()].filter(
        ({ phase }) => phase === 'final',
      ),
    ).toHaveLength(500);
    expect(result.current.eventsMap.has('event-1999')).toBe(true);
  });
});
