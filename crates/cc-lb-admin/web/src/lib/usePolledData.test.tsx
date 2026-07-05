// @vitest-environment jsdom

import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { renderHook } from '@testing-library/react';
import type React from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { usePolledData } from './usePolledData';

describe('usePolledData', () => {
  let queryClient: QueryClient;

  beforeEach(() => {
    queryClient = new QueryClient();
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.clearAllMocks();
  });

  it('should toggle refetchInterval based on visibility', () => {
    const wrapper = ({ children }: { children: React.ReactNode }) => (
      <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    );

    const { result, rerender } = renderHook(
      () =>
        usePolledData(
          { queryKey: ['test'], queryFn: () => Promise.resolve('test') },
          5000,
        ),
      { wrapper },
    );

    expect(result.current.status).toBe('live');

    // Mock visibility hidden
    Object.defineProperty(document, 'visibilityState', {
      value: 'hidden',
      configurable: true,
    });
    document.dispatchEvent(new Event('visibilitychange'));
    vi.advanceTimersByTime(120_000); // grace period

    rerender();
    expect(result.current.status).toBe('hidden');

    // Mock visibility visible
    Object.defineProperty(document, 'visibilityState', {
      value: 'visible',
      configurable: true,
    });
    document.dispatchEvent(new Event('visibilitychange'));

    rerender();
    expect(result.current.status).toBe('live');
  });
});
