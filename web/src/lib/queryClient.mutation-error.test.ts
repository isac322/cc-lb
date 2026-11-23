import { afterEach, describe, expect, test, vi } from 'vitest';

vi.mock('sonner', () => ({
  toast: {
    error: vi.fn(),
    success: vi.fn(),
  },
}));

import { toast } from 'sonner';
import { queryClient } from './queryClient';

describe('queryClient mutation error handling', () => {
  afterEach(() => {
    vi.mocked(toast.error).mockReset();
  });

  test('suppresses the global toast for inline-error mutations', async () => {
    const mutation = queryClient.getMutationCache().build(queryClient, {
      mutationFn: async () => {
        throw new Error('inline failure');
      },
      meta: { inlineError: true },
    });

    await expect(mutation.execute(undefined)).rejects.toThrow('inline failure');
    expect(toast.error).not.toHaveBeenCalled();
  });

  test('keeps the global toast for mutations without inline errors', async () => {
    const mutation = queryClient.getMutationCache().build(queryClient, {
      mutationFn: async () => {
        throw new Error('global failure');
      },
    });

    await expect(mutation.execute(undefined)).rejects.toThrow('global failure');
    expect(toast.error).toHaveBeenCalledWith('global failure');
  });
});
