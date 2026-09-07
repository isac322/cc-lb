// @vitest-environment jsdom
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import {
  act,
  cleanup,
  render,
  renderHook,
  screen,
} from '@testing-library/react';
import { createEventSource, type EventSourceOptions } from 'eventsource-client';
import type React from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { LiveTailFailureBanner } from '../components/LiveTailFailureBanner';
import { useLiveEventStream } from './useLiveEventStream';

const closeClient = vi.hoisted(() => vi.fn());

vi.mock('eventsource-client', () => ({
  createEventSource: vi.fn(() => ({
    close: closeClient,
  })),
}));

vi.mock('./auth', () => ({
  getAdminToken: vi.fn(() => 'test-token'),
}));

vi.mock('./api', () => ({
  getJson: vi.fn(),
  RequestEventUpdateSchema: {
    safeParse: vi.fn((data) => {
      if (
        data &&
        data.phase === 'partial' &&
        typeof data.payload?.event_id === 'number'
      ) {
        return { success: false, error: new Error('Invalid event_id') };
      }
      return { success: true, data };
    }),
  },
}));

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

function LiveTailConsumer({ enabled }: { readonly enabled: boolean }) {
  const live = useLiveEventStream({}, { enabled });
  return (
    <LiveTailFailureBanner
      permanentFailure={live.permanentFailure}
      permanentFailureSince={live.permanentFailureSince}
      reconnectAttempts={live.reconnectAttempts}
      onRetry={live.forceReconnect}
    />
  );
}

describe('useLiveEventStream', () => {
  let queryClient: QueryClient;

  beforeEach(() => {
    queryClient = new QueryClient();
    vi.useFakeTimers();
  });

  afterEach(() => {
    cleanup();
    vi.useRealTimers();
    vi.clearAllMocks();
  });

  it('starts connecting when enabled', () => {
    const wrapper = ({ children }: { children: React.ReactNode }) => (
      <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    );
    const { result } = renderHook(() => useLiveEventStream({}), { wrapper });

    expect(createEventSource).toHaveBeenCalledTimes(1);
    expect(result.current.status).toBe('connecting');
  });

  it('increments malformedFrameCount on corrupted SSE frame', () => {
    const wrapper = ({ children }: { children: React.ReactNode }) => (
      <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    );
    let renderCount = 0;
    const { result } = renderHook(
      () => {
        renderCount += 1;
        return useLiveEventStream({});
      },
      { wrapper },
    );

    const onMessage = eventSourceOptionsAt(0).onMessage;
    if (onMessage === undefined) {
      throw new Error('Expected an EventSource message handler');
    }

    act(() => {
      onMessage({
        data: JSON.stringify({
          phase: 'partial',
          payload: { event_id: 'evt_1', event: { event_id: 'evt_1' } },
        }),
      });
    });

    expect(result.current.malformedFrameCount).toBe(0);
    expect(result.current.eventsMap.size).toBe(1);
    const renderCountBeforeMalformedFrame = renderCount;

    act(() => {
      onMessage({
        data: JSON.stringify({ phase: 'partial', payload: { event_id: 42 } }),
      });
    });

    expect(result.current.malformedFrameCount).toBe(1);
    expect(result.current.eventsMap.size).toBe(1);
    expect(renderCount).toBe(renderCountBeforeMalformedFrame);
  });

  it('clears failure bookkeeping and ignores stale callbacks after auth failure', () => {
    const wrapper = ({ children }: { children: React.ReactNode }) => (
      <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    );
    const { result } = renderHook(() => useLiveEventStream({}), { wrapper });

    const { onConnect, onDisconnect, onScheduleReconnect } =
      eventSourceOptionsAt(0);
    if (
      onConnect === undefined ||
      onDisconnect === undefined ||
      onScheduleReconnect === undefined
    ) {
      throw new Error('Expected EventSource lifecycle handlers');
    }

    act(() => {
      onScheduleReconnect({ delay: 1_000 });
      vi.advanceTimersByTime(300_000);
      onScheduleReconnect({ delay: 1_000 });
    });
    expect(result.current.permanentFailure).toBe(true);

    act(() => {
      window.dispatchEvent(new Event('cclb:auth-required'));
      onConnect();
      onDisconnect();
      onScheduleReconnect({ delay: 1_000 });
    });

    expect(closeClient).toHaveBeenCalledTimes(1);
    expect(createEventSource).toHaveBeenCalledTimes(1);
    expect(result.current.status).toBe('error');
    expect(result.current.error?.message).toBe('Unauthorized');
    expect(result.current.permanentFailure).toBe(false);
    expect(result.current.permanentFailureSince).toBeNull();
    expect(result.current.reconnectAttempts).toBe(0);
  });

  it('tracks permanent failure after 5 minutes of reconnecting', () => {
    const wrapper = ({ children }: { children: React.ReactNode }) => (
      <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    );
    const { result } = renderHook(() => useLiveEventStream({}), { wrapper });

    const createEventSourceMock = vi.mocked(createEventSource);
    const { onConnect, onScheduleReconnect } = eventSourceOptionsAt(0);
    if (onScheduleReconnect === undefined || onConnect === undefined) {
      throw new Error('Expected EventSource lifecycle handlers');
    }

    expect(result.current.permanentFailure).toBe(false);
    expect(result.current.reconnectAttempts).toBe(0);

    act(() => {
      onScheduleReconnect({ delay: 1000 });
    });

    expect(result.current.status).toBe('reconnecting');
    expect(result.current.reconnectAttempts).toBe(1);
    expect(result.current.permanentFailure).toBe(false);

    act(() => {
      vi.advanceTimersByTime(300_000);
      onScheduleReconnect({ delay: 1000 });
    });

    expect(result.current.reconnectAttempts).toBe(2);
    expect(result.current.permanentFailure).toBe(true);
    expect(result.current.permanentFailureSince).not.toBeNull();
    expect(closeClient).toHaveBeenCalledTimes(1);

    act(() => {
      onConnect();
    });
    expect(result.current.permanentFailure).toBe(true);

    act(() => {
      vi.advanceTimersByTime(60_000);
    });
    expect(createEventSourceMock).toHaveBeenCalledTimes(2);

    const { onConnect: onReconnect } = eventSourceOptionsAt(1);
    if (onReconnect === undefined) {
      throw new Error('Expected a reconnected EventSource handler');
    }
    act(() => {
      onReconnect();
    });

    expect(result.current.permanentFailure).toBe(false);
    expect(result.current.permanentFailureSince).toBeNull();
    expect(result.current.reconnectAttempts).toBe(0);
  });

  it('removes the failure banner when the stream is disabled', () => {
    const view = render(
      <QueryClientProvider client={queryClient}>
        <LiveTailConsumer enabled />
      </QueryClientProvider>,
    );
    const { onScheduleReconnect } = eventSourceOptionsAt(0);
    if (onScheduleReconnect === undefined) {
      throw new Error('Expected an EventSource reconnect handler');
    }

    act(() => {
      onScheduleReconnect({ delay: 1_000 });
      vi.advanceTimersByTime(300_000);
      onScheduleReconnect({ delay: 1_000 });
    });
    expect(screen.getByText('Live tail disconnected')).toBeTruthy();

    view.rerender(
      <QueryClientProvider client={queryClient}>
        <LiveTailConsumer enabled={false} />
      </QueryClientProvider>,
    );

    expect(screen.queryByText('Live tail disconnected')).toBeNull();
    expect(screen.queryByText('Retry now')).toBeNull();
  });

  it('forceReconnect resets the retry timer and attempts to connect', () => {
    const wrapper = ({ children }: { children: React.ReactNode }) => (
      <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    );
    const { result } = renderHook(() => useLiveEventStream({}), { wrapper });

    const createEventSourceMock = vi.mocked(createEventSource);
    const { onScheduleReconnect } = eventSourceOptionsAt(0);
    if (onScheduleReconnect === undefined) {
      throw new Error('Expected an EventSource reconnect handler');
    }

    act(() => {
      onScheduleReconnect({ delay: 1000 });
      vi.advanceTimersByTime(300_000);
      onScheduleReconnect({ delay: 1000 });
    });

    expect(result.current.permanentFailure).toBe(true);
    const initialCallCount = createEventSourceMock.mock.calls.length;

    act(() => {
      result.current.forceReconnect();
    });

    expect(createEventSourceMock.mock.calls.length).toBe(initialCallCount + 1);
    expect(result.current.status).toBe('reconnecting');
  });
});
