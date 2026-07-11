import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { act, renderHook } from '@testing-library/react';
import {
  createEventSource,
  type EventSourceMessage,
  type EventSourceOptions,
} from 'eventsource-client';
import { createElement, type ReactNode } from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { getJson, type RequestEvent, type RequestEventUpdate } from '../../api';
import { useLiveEventStream } from '../../useLiveEventStream';

const closeClient = vi.hoisted(() => vi.fn());
const visibilityState = vi.hoisted(() => ({
  online: true,
  visible: true,
  gracePeriodElapsed: false,
}));

vi.mock('eventsource-client', () => ({
  createEventSource: vi.fn(() => ({ close: closeClient })),
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

vi.mock('../../visibilityManager', () => ({
  useVisibility: vi.fn(() => visibilityState),
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

function makeFinalMessage(index: number): EventSourceMessage {
  const update: RequestEventUpdate = {
    phase: 'final',
    payload: {
      event: {
        event_id: `event-${index}`,
        request_id: `request-${index}`,
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

function makePartialMessage(index: number): EventSourceMessage {
  const update: RequestEventUpdate = {
    phase: 'partial',
    payload: {
      event_id: `event-${index}`,
      request_id: `request-${index}`,
      ts: index / 1000,
      ts_ms: index,
    },
  };
  return { data: JSON.stringify(update) };
}

describe('useLiveEventStream retention lifecycle', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    visibilityState.online = true;
    visibilityState.visible = true;
    visibilityState.gracePeriodElapsed = false;
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.clearAllMocks();
  });

  it('creates no EventSource while disabled and closes an existing one when disabled', () => {
    const client = new QueryClient();
    const { result, rerender } = renderHook(
      ({ enabled }: { readonly enabled: boolean }) =>
        useLiveEventStream({}, { enabled }),
      {
        initialProps: { enabled: false },
        wrapper: makeWrapper(client),
      },
    );

    expect(createEventSource).not.toHaveBeenCalled();
    expect(result.current.status).toBe('idle');

    rerender({ enabled: true });
    expect(createEventSource).toHaveBeenCalledTimes(1);

    rerender({ enabled: false });
    expect(closeClient).toHaveBeenCalledTimes(1);
    expect(result.current.status).toBe('idle');
  });

  it('clears a scheduled reconnect when disabled', () => {
    const client = new QueryClient();
    const { result, rerender } = renderHook(
      ({ enabled }: { readonly enabled: boolean }) =>
        useLiveEventStream({}, { enabled }),
      {
        initialProps: { enabled: true },
        wrapper: makeWrapper(client),
      },
    );
    const options = latestOptions();

    act(() => {
      options.onScheduleReconnect?.({ delay: 1000 });
      vi.advanceTimersByTime(300_000);
      options.onScheduleReconnect?.({ delay: 1000 });
    });
    rerender({ enabled: false });
    act(() => vi.advanceTimersByTime(60_000));

    expect(createEventSource).toHaveBeenCalledTimes(1);
    expect(result.current.status).toBe('idle');
  });

  it('ages partial orphans from their last update rather than request start', () => {
    vi.setSystemTime(1_000_000);
    const client = new QueryClient();
    const { result } = renderHook(() => useLiveEventStream({}), {
      wrapper: makeWrapper(client),
    });
    const update: RequestEventUpdate = {
      phase: 'partial',
      payload: {
        event_id: 'long-running-event',
        request_id: 'long-running-request',
        ts: 0,
        ts_ms: 0,
        last_update_ms: 760_000,
      },
    };

    act(() => {
      latestOptions().onMessage?.({ data: JSON.stringify(update) });
      vi.advanceTimersByTime(30_000);
    });

    expect(result.current.eventsMap.has('long-running-event')).toBe(true);
  });

  it('wires bounded tombstones and clears them on reset', () => {
    const client = new QueryClient();
    const { result } = renderHook(() => useLiveEventStream({}), {
      wrapper: makeWrapper(client),
    });
    const options = latestOptions();

    act(() => {
      for (let index = 0; index < 501; index += 1) {
        options.onMessage?.(makeFinalMessage(index));
      }
    });
    expect(result.current.eventsMap.size).toBe(500);
    expect(result.current.eventsMap.has('event-0')).toBe(false);

    act(() => options.onMessage?.(makePartialMessage(0)));
    expect(result.current.eventsMap.has('event-0')).toBe(false);

    act(() => options.onMessage?.({ event: 'reset', data: '' }));
    const resetOptions = latestOptions();
    act(() => resetOptions.onMessage?.(makePartialMessage(0)));

    expect(result.current.eventsMap.size).toBe(1);
    expect(result.current.eventsMap.get('event-0')?.phase).toBe('partial');
  });

  it('discards an asynchronous backfill that resolves after streaming is disabled', async () => {
    const client = new QueryClient();
    let resolveBackfill:
      | ((value: {
          events: RequestEvent[];
          next_cursor: number;
          exhausted: boolean;
        }) => void)
      | undefined;
    vi.mocked(getJson).mockReturnValue(
      new Promise((resolve) => {
        resolveBackfill = resolve;
      }),
    );
    const { result, rerender } = renderHook(
      ({ enabled }: { readonly enabled: boolean }) =>
        useLiveEventStream({}, { enabled }),
      {
        initialProps: { enabled: true },
        wrapper: makeWrapper(client),
      },
    );

    act(() => latestOptions().onMessage?.(makeFinalMessage(1)));
    visibilityState.visible = false;
    visibilityState.gracePeriodElapsed = true;
    rerender({ enabled: true });
    visibilityState.visible = true;
    visibilityState.gracePeriodElapsed = false;
    rerender({ enabled: true });
    rerender({ enabled: false });

    if (resolveBackfill === undefined) {
      throw new Error('Expected backfill request');
    }
    await act(async () => {
      resolveBackfill?.({
        events: [
          {
            event_id: 'event-2',
            request_id: 'request-2',
            ts: 0.002,
            ts_ms: 2,
            status: 200,
            duration_ms: 10,
          } satisfies RequestEvent,
        ],
        next_cursor: 2,
        exhausted: true,
      });
      await Promise.resolve();
    });

    expect(result.current.eventsMap.has('event-2')).toBe(false);
    expect(result.current.lastCursor).toBe('1');
    expect(createEventSource).toHaveBeenCalledTimes(1);
  });
});
