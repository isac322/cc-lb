import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { renderHook } from '@testing-library/react';
import type { ReactNode } from 'react';
import { expect, test, vi } from 'vitest';
import { useUploadWasm } from '../lib/queries';

const fetchWithAuthMock = vi.fn();

vi.mock('../lib/api', async () => {
  const actual =
    await vi.importActual<typeof import('../lib/api')>('../lib/api');
  return {
    ...actual,
    fetchWithAuth: (...args: unknown[]) => fetchWithAuthMock(...args),
  };
});

function wrapper({ children }: { children: ReactNode }) {
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: { retry: false },
      mutations: { retry: false },
    },
  });
  return (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  );
}

test('upload omits registry identity fields while preserving the file payload', async () => {
  fetchWithAuthMock.mockResolvedValue({
    json: async () => ({
      id: 'reg-1',
      sha256_hex: 'ab',
      size_bytes: 8,
      original_filename: 'cc_lb_plugin_subscription_launderer.wasm',
      revision: 0,
      idempotent: false,
      action: 'created',
    }),
  });

  const { result } = renderHook(() => useUploadWasm(), { wrapper });

  const file = new File(
    [new Uint8Array([0, 0x61, 0x73, 0x6d, 1, 0, 0, 0])],
    'cc_lb_plugin_subscription_launderer.wasm',
    { type: 'application/wasm' },
  );

  await result.current.mutateAsync({ file });

  expect(fetchWithAuthMock).toHaveBeenCalledTimes(1);
  const init = fetchWithAuthMock.mock.calls[0][1] as { body: FormData };
  const form = init.body;
  expect(form.has('name')).toBe(false);
  expect(form.has('slot_kind')).toBe(false);
  expect(form.get('original_filename')).toBe(
    'cc_lb_plugin_subscription_launderer.wasm',
  );
  expect(form.get('bytes')).toBe(file);
});
