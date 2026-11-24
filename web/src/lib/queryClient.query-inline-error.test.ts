// D-UX1 / R2-6: a query that renders its own error surface sets
// `meta.inlineError`, and the global QueryCache must not toast it too.

import { afterEach, describe, expect, test, vi } from 'vitest';

vi.mock('sonner', () => ({
  toast: {
    error: vi.fn(),
    success: vi.fn(),
  },
}));

import { toast } from 'sonner';
import { queryClient } from './queryClient';

describe('queryClient query error handling (D-UX1)', () => {
  afterEach(() => {
    queryClient.clear();
    vi.mocked(toast.error).mockReset();
  });

  test('suppresses the global toast for inline-error queries', async () => {
    await expect(
      queryClient.fetchQuery({
        queryKey: ['inline-error-query'],
        retry: false,
        meta: { inlineError: true },
        queryFn: async () => {
          throw new Error('usage load failed');
        },
      }),
    ).rejects.toThrow('usage load failed');
    expect(toast.error).not.toHaveBeenCalled();
  });

  test('keeps the global toast for queries without inline errors', async () => {
    await expect(
      queryClient.fetchQuery({
        queryKey: ['global-error-query'],
        retry: false,
        queryFn: async () => {
          throw new Error('list load failed');
        },
      }),
    ).rejects.toThrow('list load failed');
    expect(toast.error).toHaveBeenCalledWith('list load failed');
  });
});
