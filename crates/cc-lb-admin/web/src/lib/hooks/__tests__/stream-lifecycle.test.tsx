// @vitest-environment jsdom
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { act, renderHook } from '@testing-library/react';
import {
  createEventSource,
  type EventSourceMessage,
  type EventSourceOptions,
} from 'eventsource-client';
import { createElement, type ReactNode } from 'react';
import {
  afterEach,
  beforeEach,
  describe,
  expect,
  it,
  type Mock,
  vi,
} from 'vitest';
import { getJson, type RequestEventUpdate } from '../../api';
import { useLiveEventStream } from '../../useLiveEventStream';

const visibilityState = vi.hoisted(() => ({
  online: true,
  visible: true,
  gracePeriodElapsed: false,
}));
const eventSourceState = vi.hoisted(() => ({
  closeHandlers: [] as Mock[],
}));

vi.mock('eventsource-client', () => ({
  createEventSource: vi.fn(() => {
    const close = vi.fn();
    eventSourceState.closeHandlers.push(close);
    return { close };
  }),
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

function eventSourceOptionsAt(index: number): EventSourceOptions {
  const input = vi.mocked(createEventSource).mock.calls[index]?.[0];
  if (
    input === undefined ||
    typeof input === 'string' ||
    input instanceof URL
  ) {
    throw new Error(`Expected EventSource options at call ${index}`);
  }
  return input;
}

function makeFinalMessage(cursor: number): EventSourceMessage {
  const update: RequestEventUpdate = {
    phase: 'final',
    payload: {
      event: {
        event_id: `event-${cursor}`,
        request_id: `request-${cursor}`,
        ts: cursor / 1000,
        ts_ms: cursor,
        status: 200,
        duration_ms: 10,
      },
      cursor,
    },
  };
  return { data: JSON.stringify(update) };
}

describe('useLiveEventStream connection lifecycle', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(1_000);
    visibilityState.online = true;
    visibilityState.visible = true;
    visibilityState.gracePeriodElapsed = false;
    eventSourceState.closeHandlers.length = 0;
    vi.mocked(getJson).mockResolvedValue({
      events: [],
      next_cursor: 8,
      exhausted: true,
    });
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.clearAllMocks();
  });

  it('reconnects a stalled stream with the latest filters', () => {
    const client = new QueryClient();
    const { result, rerender } = renderHook(
      ({ principalId }: { readonly principalId: string }) =>
        useLiveEventStream({ principal_id: principalId }),
      {
        initialProps: { principalId: 'principal-a' },
        wrapper: makeWrapper(client),
      },
    );

    expect(eventSourceOptionsAt(0).url).toContain('principal_id=principal-a');

    rerender({ principalId: 'principal-b' });
    const currentOptions = eventSourceOptionsAt(1);
    expect(currentOptions.url).toContain('principal_id=principal-b');
    if (currentOptions.onMessage === undefined) {
      throw new Error('Expected an EventSource message handler');
    }

    act(() => {
      currentOptions.onMessage?.({
        event: 'heartbeat',
        id: 'b-1',
        data: '',
      });
    });
    expect(result.current.status).toBe('live');

    act(() => {
      vi.advanceTimersByTime(50_000);
    });
    expect(result.current.status).toBe('reconnecting');

    expect(createEventSource).toHaveBeenCalledTimes(3);
    expect(eventSourceOptionsAt(2).url).toContain('principal_id=principal-b');
    expect(eventSourceOptionsAt(2).url).not.toContain(
      'principal_id=principal-a',
    );
    expect(eventSourceState.closeHandlers[0]).toHaveBeenCalledTimes(1);
    expect(eventSourceState.closeHandlers[1]).toHaveBeenCalledTimes(1);
  });

  it('keeps one live connection across hide and show within the grace period', () => {
    const client = new QueryClient();
    const { result, rerender } = renderHook(() => useLiveEventStream({}), {
      wrapper: makeWrapper(client),
    });
    const options = eventSourceOptionsAt(0);
    if (options.onMessage === undefined) {
      throw new Error('Expected an EventSource message handler');
    }

    act(() => options.onMessage?.({ event: 'heartbeat', id: '1', data: '' }));
    visibilityState.visible = false;
    rerender();
    act(() => vi.advanceTimersByTime(50_000));
    visibilityState.visible = true;
    rerender();

    expect(result.current.status).toBe('live');
    expect(createEventSource).toHaveBeenCalledTimes(1);
    expect(eventSourceState.closeHandlers[0]).not.toHaveBeenCalled();
  });

  it('stops after the hidden grace period and backfills before resuming', async () => {
    const client = new QueryClient();
    const { result, rerender } = renderHook(
      () => useLiveEventStream({ principal_id: 'principal-a' }),
      { wrapper: makeWrapper(client) },
    );
    const initialOptions = eventSourceOptionsAt(0);
    if (initialOptions.onMessage === undefined) {
      throw new Error('Expected an EventSource message handler');
    }

    act(() => initialOptions.onMessage?.(makeFinalMessage(7)));
    visibilityState.visible = false;
    visibilityState.gracePeriodElapsed = true;
    rerender();

    expect(result.current.status).toBe('hidden');
    expect(eventSourceState.closeHandlers[0]).toHaveBeenCalledTimes(1);
    expect(createEventSource).toHaveBeenCalledTimes(1);

    visibilityState.visible = true;
    visibilityState.gracePeriodElapsed = false;
    await act(async () => {
      rerender();
      await Promise.resolve();
    });

    expect(getJson).toHaveBeenCalledTimes(1);
    const backfillUrl = vi.mocked(getJson).mock.calls[0]?.[0];
    expect(backfillUrl).toContain('principal_id=principal-a');
    expect(backfillUrl).toContain('since_cursor=7');
    expect(createEventSource).toHaveBeenCalledTimes(2);
    expect(eventSourceOptionsAt(1).initialLastEventId).toBe('8');
    expect(result.current.lastCursor).toBe('8');
  });

  it('accepts normal frames after a reset cursor is paused and resumed', async () => {
    const client = new QueryClient();
    const { result, rerender } = renderHook(() => useLiveEventStream({}), {
      wrapper: makeWrapper(client),
    });
    const initialOptions = eventSourceOptionsAt(0);
    if (initialOptions.onMessage === undefined) {
      throw new Error('Expected an EventSource message handler');
    }

    act(() => {
      initialOptions.onMessage?.({ event: 'reset', data: '' });
    });
    expect(result.current.lastCursor).toBe('');
    expect(createEventSource).toHaveBeenCalledTimes(2);

    visibilityState.visible = false;
    visibilityState.gracePeriodElapsed = true;
    rerender();
    expect(result.current.status).toBe('hidden');
    expect(eventSourceState.closeHandlers[1]).toHaveBeenCalledTimes(1);

    visibilityState.visible = true;
    visibilityState.gracePeriodElapsed = false;
    await act(async () => {
      rerender();
      await Promise.resolve();
    });

    expect(getJson).not.toHaveBeenCalled();
    expect(createEventSource).toHaveBeenCalledTimes(3);
    const resumedOptions = eventSourceOptionsAt(2);
    if (resumedOptions.onMessage === undefined) {
      throw new Error('Expected a resumed EventSource message handler');
    }

    act(() => resumedOptions.onMessage?.(makeFinalMessage(9)));

    expect(result.current.status).toBe('live');
    expect(result.current.lastCursor).toBe('9');
    expect(result.current.eventsMap.get('event-9')?.event.request_id).toBe(
      'request-9',
    );
  });

  it('stops offline and backfills before reconnecting online', async () => {
    const client = new QueryClient();
    const { result, rerender } = renderHook(() => useLiveEventStream({}), {
      wrapper: makeWrapper(client),
    });
    const initialOptions = eventSourceOptionsAt(0);
    if (
      initialOptions.onMessage === undefined ||
      initialOptions.onScheduleReconnect === undefined
    ) {
      throw new Error('Expected EventSource lifecycle handlers');
    }

    act(() => {
      initialOptions.onMessage?.(makeFinalMessage(5));
      initialOptions.onScheduleReconnect?.({ delay: 1_000 });
    });
    expect(result.current.reconnectAttempts).toBe(1);
    expect(result.current.permanentFailureSince).toBe(1_000);

    visibilityState.online = false;
    rerender();

    expect(result.current.status).toBe('error');
    expect(result.current.error?.message).toBe('Offline');
    expect(result.current.reconnectAttempts).toBe(0);
    expect(result.current.permanentFailureSince).toBeNull();
    expect(result.current.permanentFailure).toBe(false);
    expect(eventSourceState.closeHandlers[0]).toHaveBeenCalledTimes(1);

    visibilityState.online = true;
    await act(async () => {
      rerender();
      await Promise.resolve();
    });

    const backfillUrl = vi.mocked(getJson).mock.calls[0]?.[0];
    expect(backfillUrl).toContain('since_cursor=5');
    expect(createEventSource).toHaveBeenCalledTimes(2);
    expect(result.current.status).toBe('connecting');
  });

  it('does not count a hidden pause toward permanent failure', () => {
    const client = new QueryClient();
    const { result, rerender } = renderHook(() => useLiveEventStream({}), {
      wrapper: makeWrapper(client),
    });
    const initialReconnect = eventSourceOptionsAt(0).onScheduleReconnect;
    if (initialReconnect === undefined) {
      throw new Error('Expected an EventSource reconnect handler');
    }

    act(() => initialReconnect({ delay: 1_000 }));
    expect(result.current.reconnectAttempts).toBe(1);
    expect(result.current.permanentFailureSince).toBe(1_000);

    visibilityState.visible = false;
    visibilityState.gracePeriodElapsed = true;
    rerender();
    expect(result.current.status).toBe('hidden');
    expect(result.current.reconnectAttempts).toBe(0);
    expect(result.current.permanentFailureSince).toBeNull();

    act(() => vi.advanceTimersByTime(600_000));
    visibilityState.visible = true;
    visibilityState.gracePeriodElapsed = false;
    rerender();

    const resumedReconnect = eventSourceOptionsAt(1).onScheduleReconnect;
    if (resumedReconnect === undefined) {
      throw new Error('Expected a resumed EventSource reconnect handler');
    }
    act(() => resumedReconnect({ delay: 1_000 }));

    expect(result.current.reconnectAttempts).toBe(1);
    expect(result.current.permanentFailureSince).toBe(601_000);
    expect(result.current.permanentFailure).toBe(false);
    expect(eventSourceState.closeHandlers[1]).not.toHaveBeenCalled();
  });

  it('publishes status transitions without rendering for live heartbeat or cursor frames', () => {
    const client = new QueryClient();
    let renderCount = 0;
    const { result } = renderHook(
      () => {
        renderCount += 1;
        return useLiveEventStream({});
      },
      {
        wrapper: makeWrapper(client),
      },
    );
    const options = eventSourceOptionsAt(0);
    if (options.onMessage === undefined) {
      throw new Error('Expected an EventSource message handler');
    }

    const connectingRenderCount = renderCount;
    act(() => options.onMessage?.({ event: 'heartbeat', id: '1', data: '' }));
    expect(result.current.status).toBe('live');
    expect(renderCount).toBe(connectingRenderCount + 1);

    const liveRenderCount = renderCount;
    const version = result.current.version;
    const eventsMap = result.current.eventsMap;
    const previousActivity = result.current.lastActivityAt;

    act(() => {
      vi.advanceTimersByTime(1_000);
      options.onMessage?.({ event: 'heartbeat', id: '2', data: '' });
    });
    expect(renderCount).toBe(liveRenderCount);
    expect(result.current.version).toBe(version);
    expect(result.current.eventsMap).toBe(eventsMap);
    expect(result.current.lastCursor).toBe('2');
    expect(result.current.lastActivityAt).toBeGreaterThan(
      previousActivity ?? Number.NEGATIVE_INFINITY,
    );

    act(() => {
      vi.advanceTimersByTime(1_000);
      options.onMessage?.({ event: 'cursor', id: '3', data: '' });
    });

    expect(renderCount).toBe(liveRenderCount);
    expect(result.current.version).toBe(version);
    expect(result.current.eventsMap).toBe(eventsMap);
    expect(result.current.lastCursor).toBe('3');
  });
});
