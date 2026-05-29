import { act, renderHook } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { useStatus } from '../useStatusOverview';

describe('useStatusOverview', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.restoreAllMocks();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('useStatus fetches status and polls', async () => {
    const mockResponse1 = {
      generation: 1,
      upstreams: [],
      principals: [],
      killswitch: false,
      build: { rust_version: '', profile: '', target: '' },
      git_sha: '',
      uptime_secs: 0,
      version: '',
      plugin_chain_summary: { principal_count_with_chain: 0, total_entries: 0 },
    };
    const mockResponse2 = { ...mockResponse1, generation: 2 };

    const fetchMock = vi
      .spyOn(global, 'fetch')
      .mockResolvedValueOnce({
        ok: true,
        json: async () => mockResponse1,
      } as Response)
      .mockResolvedValueOnce({
        ok: true,
        json: async () => mockResponse2,
      } as Response);

    const { result, unmount } = renderHook(() => useStatus());

    // Initial fetch
    await act(async () => {
      await vi.advanceTimersByTimeAsync(5000);
    });

    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(result.current.generation).toBe(2);

    unmount();
  });
});
