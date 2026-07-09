// @vitest-environment jsdom
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { act, renderHook } from '@testing-library/react';
import { createEventSource } from 'eventsource-client';
import type React from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { useLiveEventStream } from './useLiveEventStream';

vi.mock('eventsource-client', () => ({
  createEventSource: vi.fn(() => ({
    close: vi.fn(),
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

describe('useLiveEventStream', () => {
  let queryClient: QueryClient;

  beforeEach(() => {
    queryClient = new QueryClient();
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.clearAllMocks();
  });

  it('should initialize with idle status', () => {
    const wrapper = ({ children }: { children: React.ReactNode }) => (
      <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    );
    const { result } = renderHook(() => useLiveEventStream({}), { wrapper });
    expect(result.current.status).toBe('idle');
  });

  it('increments malformedFrameCount on corrupted SSE frame', () => {
    const wrapper = ({ children }: { children: React.ReactNode }) => (
      <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    );
    const { result } = renderHook(() => useLiveEventStream({}), { wrapper });

    const createEventSourceMock = vi.mocked(createEventSource);
    const options = createEventSourceMock.mock.calls[0][0] as unknown as {
      onMessage: (msg: unknown) => void;
    };
    const onMessage = options.onMessage;

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

    act(() => {
      onMessage({
        data: JSON.stringify({ phase: 'partial', payload: { event_id: 42 } }),
      });
    });

    expect(result.current.malformedFrameCount).toBe(1);
    expect(result.current.eventsMap.size).toBe(1);
  });

  it('closes and recreates EventSource on auth token change', () => {
    const wrapper = ({ children }: { children: React.ReactNode }) => (
      <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    );
    const { result } = renderHook(() => useLiveEventStream({}), { wrapper });

    const createEventSourceMock = vi.mocked(createEventSource);
    expect(createEventSourceMock).toHaveBeenCalledTimes(1);

    const mockClose = (
      createEventSourceMock.mock.results[0].value as unknown as {
        close: () => void;
      }
    ).close;

    act(() => {
      window.dispatchEvent(new Event('cclb:auth-required'));
    });

    expect(mockClose).toHaveBeenCalledTimes(1);
    expect(result.current.status).toBe('error');
    expect(result.current.error?.message).toBe('Unauthorized');
  });

  it('tracks permanent failure after 5 minutes of reconnecting', () => {
    const wrapper = ({ children }: { children: React.ReactNode }) => (
      <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    );
    const { result } = renderHook(() => useLiveEventStream({}), { wrapper });

    const createEventSourceMock = vi.mocked(createEventSource);
    const options = createEventSourceMock.mock.calls[0][0] as unknown as {
      onScheduleReconnect: (info: { delay: number }) => void;
      onConnect: () => void;
    };

    expect(result.current.permanentFailure).toBe(false);
    expect(result.current.reconnectAttempts).toBe(0);

    act(() => {
      options.onScheduleReconnect({ delay: 1000 });
    });

    expect(result.current.status).toBe('reconnecting');
    expect(result.current.reconnectAttempts).toBe(1);
    expect(result.current.permanentFailure).toBe(false);

    act(() => {
      vi.advanceTimersByTime(300_000);
      options.onScheduleReconnect({ delay: 1000 });
    });

    expect(result.current.reconnectAttempts).toBe(2);
    expect(result.current.permanentFailure).toBe(true);
    expect(result.current.permanentFailureSince).not.toBeNull();

    act(() => {
      options.onConnect();
    });

    expect(result.current.permanentFailure).toBe(false);
    expect(result.current.permanentFailureSince).toBeNull();
    expect(result.current.reconnectAttempts).toBe(0);
  });

  it('forceReconnect resets the retry timer and attempts to connect', () => {
    const wrapper = ({ children }: { children: React.ReactNode }) => (
      <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    );
    const { result } = renderHook(() => useLiveEventStream({}), { wrapper });

    const createEventSourceMock = vi.mocked(createEventSource);
    const options = createEventSourceMock.mock.calls[0][0] as unknown as {
      onScheduleReconnect: (info: { delay: number }) => void;
    };

    act(() => {
      options.onScheduleReconnect({ delay: 1000 });
      vi.advanceTimersByTime(300_000);
      options.onScheduleReconnect({ delay: 1000 });
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
