// @vitest-environment jsdom

import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { act, renderHook } from '@testing-library/react';
import type React from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { usePolledData } from './usePolledData';

function setDocumentVisibility(state: DocumentVisibilityState) {
  Object.defineProperty(document, 'visibilityState', {
    value: state,
    configurable: true,
  });
  document.dispatchEvent(new Event('visibilitychange'));
}

describe('usePolledData', () => {
  let queryClient: QueryClient;

  beforeEach(() => {
    queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });
    setDocumentVisibility('visible');
    vi.useFakeTimers();
  });

  afterEach(() => {
    act(() => setDocumentVisibility('visible'));
    vi.useRealTimers();
    vi.clearAllMocks();
  });

  it('pauses polling while hidden, preserves native status, and refetches on return', async () => {
    const queryFn = vi.fn().mockResolvedValue({ value: 'stable' });
    const wrapper = ({ children }: { children: React.ReactNode }) => (
      <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    );

    const { result } = renderHook(
      () => {
        const query = usePolledData({ queryKey: ['test'], queryFn }, 5_000);
        return { status: query.status };
      },
      { wrapper },
    );

    expect(result.current.status).toBe('pending');
    await act(async () => {
      await vi.advanceTimersByTimeAsync(0);
    });
    expect(result.current.status).toBe('success');
    expect(queryFn).toHaveBeenCalledTimes(1);

    act(() => setDocumentVisibility('hidden'));
    await act(async () => {
      await vi.advanceTimersByTimeAsync(15_000);
    });

    expect(queryFn).toHaveBeenCalledTimes(1);
    expect(result.current.status).toBe('success');

    act(() => setDocumentVisibility('visible'));
    await act(async () => {
      await vi.advanceTimersByTimeAsync(0);
    });

    expect(queryFn).toHaveBeenCalledTimes(2);
    expect(result.current.status).toBe('success');
  });
});
