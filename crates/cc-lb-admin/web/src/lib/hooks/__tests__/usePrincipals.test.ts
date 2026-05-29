import { act, renderHook } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { ConflictError } from '../../types/v1';
import { useList, useUpdate } from '../usePrincipals';

describe('usePrincipals', () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });

  it('useList fetches principals', async () => {
    const mockResponse = { principals: [{ id: '1', name: 'test' }] };
    vi.spyOn(global, 'fetch').mockResolvedValueOnce({
      ok: true,
      json: async () => mockResponse,
    } as Response);

    const { result } = renderHook(() => useList());
    const data = await act(async () => result.current());

    expect(global.fetch).toHaveBeenCalledWith(
      '/admin/v1/principals',
      expect.objectContaining({ method: 'GET' }),
    );
    expect(data).toEqual(mockResponse);
  });

  it('useUpdate sends If-Match and handles success', async () => {
    const mockResponse = { id: '1', revision: 2 };
    vi.spyOn(global, 'fetch').mockResolvedValueOnce({
      ok: true,
      json: async () => mockResponse,
    } as Response);

    const { result } = renderHook(() => useUpdate());
    const data = await act(async () => result.current('1', 1, { name: 'new' }));

    expect(global.fetch).toHaveBeenCalledWith(
      '/admin/v1/principals/1',
      expect.objectContaining({
        method: 'PUT',
        headers: expect.any(Headers),
      }),
    );
    const call = vi.mocked(global.fetch).mock.calls[0];
    const headers = new Headers(call[1]?.headers);
    expect(headers.get('If-Match')).toBe('W/"1"');
    expect(data).toEqual(mockResponse);
  });

  it('useUpdate throws ConflictError on 409', async () => {
    const mockLatest = { id: '1', revision: 2 };
    vi.spyOn(global, 'fetch')
      .mockResolvedValueOnce({
        ok: false,
        status: 409,
        statusText: 'Conflict',
        json: async () => ({ code: 'stale_revision' }),
      } as Response)
      .mockResolvedValueOnce({
        ok: true,
        json: async () => mockLatest,
      } as Response);

    const { result } = renderHook(() => useUpdate());

    await expect(
      act(async () => result.current('1', 1, { name: 'new' })),
    ).rejects.toThrow(ConflictError);

    expect(global.fetch).toHaveBeenCalledTimes(2);
  });
});
