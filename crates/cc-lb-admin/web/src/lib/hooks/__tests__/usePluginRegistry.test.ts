import { act, renderHook } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import {
  useChainDelete,
  useRegistryList,
  useUploadWasm,
} from '../usePluginRegistry';

describe('usePluginRegistry', () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });

  it('useRegistryList fetches registry', async () => {
    const mockResponse = { entries: [] };
    vi.spyOn(global, 'fetch').mockResolvedValueOnce({
      ok: true,
      json: async () => mockResponse,
    } as Response);

    const { result } = renderHook(() => useRegistryList());
    const data = await act(async () => result.current());

    expect(global.fetch).toHaveBeenCalledWith(
      '/admin/v1/plugins/registry',
      expect.objectContaining({ method: 'GET' }),
    );
    expect(data).toEqual(mockResponse);
  });

  it('useUploadWasm sends FormData without Content-Type', async () => {
    const mockResponse = { id: '1' };
    vi.spyOn(global, 'fetch').mockResolvedValueOnce({
      ok: true,
      json: async () => mockResponse,
    } as Response);

    const { result } = renderHook(() => useUploadWasm());
    const formData = new FormData();
    formData.append('name', 'test');
    const data = await act(async () => result.current(formData));

    expect(global.fetch).toHaveBeenCalledWith(
      '/admin/v1/plugins/wasm',
      expect.objectContaining({
        method: 'POST',
        body: formData,
      }),
    );
    const call = vi.mocked(global.fetch).mock.calls[0];
    const headers = new Headers(call[1]?.headers);
    expect(headers.has('Content-Type')).toBe(false);
    expect(data).toEqual(mockResponse);
  });

  it('useChainDelete sends If-Match', async () => {
    vi.spyOn(global, 'fetch').mockResolvedValueOnce({
      ok: true,
      status: 204,
      json: async () => ({}),
    } as Response);

    const { result } = renderHook(() => useChainDelete());
    await act(async () => result.current('1', 1));

    expect(global.fetch).toHaveBeenCalledWith(
      '/admin/v1/plugin-chain-entries/1',
      expect.objectContaining({
        method: 'DELETE',
        headers: expect.any(Headers),
      }),
    );
    const call = vi.mocked(global.fetch).mock.calls[0];
    const headers = new Headers(call[1]?.headers);
    expect(headers.get('If-Match')).toBe('W/"1"');
  });
});
