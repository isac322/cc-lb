import { act, renderHook } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { useExport } from '../useExport';

describe('useExport', () => {
  beforeEach(() => {
    vi.restoreAllMocks();
    global.URL.createObjectURL = vi.fn(() => 'blob:url');
    global.URL.revokeObjectURL = vi.fn();
  });

  it('useExport triggers download', async () => {
    const mockBlob = new Blob(['{}'], { type: 'application/json' });
    vi.spyOn(global, 'fetch').mockResolvedValueOnce({
      ok: true,
      blob: async () => mockBlob,
    } as Response);

    const createElementSpy = vi.spyOn(document, 'createElement');
    const appendChildSpy = vi.spyOn(document.body, 'appendChild');
    const removeChildSpy = vi.spyOn(document.body, 'removeChild');

    const { result } = renderHook(() => useExport());
    await act(async () => result.current());

    expect(global.fetch).toHaveBeenCalledWith(
      '/admin/v1/export',
      expect.objectContaining({ headers: expect.any(Headers) }),
    );
    expect(global.URL.createObjectURL).toHaveBeenCalledWith(mockBlob);
    expect(createElementSpy).toHaveBeenCalledWith('a');
    expect(appendChildSpy).toHaveBeenCalled();
    expect(removeChildSpy).toHaveBeenCalled();
    expect(global.URL.revokeObjectURL).toHaveBeenCalledWith('blob:url');
  });
});
