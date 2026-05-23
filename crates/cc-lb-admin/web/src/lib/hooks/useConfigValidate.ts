import { useState, useCallback } from 'react';
import { postJson, ValidateConfigDraftRequest, ValidateConfigDraftResponse } from '../api';

export function useConfigValidate() {
  const [validating, setValidating] = useState(false);
  const [error, setError] = useState<Error | null>(null);

  const validate = useCallback(async (expectedRevision: number): Promise<ValidateConfigDraftResponse> => {
    const isMock = new URLSearchParams(window.location.search).get('mock') === '1';
    if (isMock) {
      const isError = new URLSearchParams(window.location.search).get('dialog') === 'validate-error';
      return new Promise((resolve) => setTimeout(() => resolve({
        valid: !isError,
        revision: expectedRevision,
        error: isError ? 'Validation failed at listener.port: must be >= 1' : undefined,
      }), 500));
    }

    setValidating(true);
    setError(null);
    try {
      const req: ValidateConfigDraftRequest = { expected_revision: expectedRevision };
      const res = await postJson<ValidateConfigDraftResponse, ValidateConfigDraftRequest>('/admin/config/draft/validate', req);
      return res;
    } catch (err) {
      setError(err instanceof Error ? err : new Error(String(err)));
      throw err;
    } finally {
      setValidating(false);
    }
  }, []);

  return { validate, validating, error };
}
