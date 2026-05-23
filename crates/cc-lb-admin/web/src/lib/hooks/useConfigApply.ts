import { useState, useCallback } from 'react';
import { postJson, ApplyConfigRequest, ApplyConfigResponse } from '../api';

export function useConfigApply() {
  const [applying, setApplying] = useState(false);
  const [error, setError] = useState<Error | null>(null);

  const apply = useCallback(async (expectedRevision: number): Promise<ApplyConfigResponse> => {
    const isMock = new URLSearchParams(window.location.search).get('mock') === '1';
    if (isMock) {
      return new Promise((resolve) => setTimeout(() => resolve({
        applied_revision: expectedRevision,
        applied_at_unix_secs: Math.floor(Date.now() / 1000),
      }), 500));
    }

    setApplying(true);
    setError(null);
    try {
      const req: ApplyConfigRequest = { expected_revision: expectedRevision };
      const res = await postJson<ApplyConfigResponse, ApplyConfigRequest>('/admin/config/apply', req);
      return res;
    } catch (err) {
      setError(err instanceof Error ? err : new Error(String(err)));
      throw err;
    } finally {
      setApplying(false);
    }
  }, []);

  return { apply, applying, error };
}
