import { useState, useEffect } from 'react';
import { getJson, PrincipalLimitsResponse } from '../api';
import { MOCK_LIMITS } from './mockData';

export function usePrincipalLimits(principalId: string | null, mock?: boolean) {
  const [data, setData] = useState<PrincipalLimitsResponse | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  useEffect(() => {
    if (!principalId) {
      setData(null);
      setIsLoading(false);
      return;
    }

    if (mock) {
      setData(MOCK_LIMITS);
      setIsLoading(false);
      setError(null);
      return;
    }

    const controller = new AbortController();
    
    async function fetchLimits() {
      try {
        setIsLoading(true);
        const res = await getJson<PrincipalLimitsResponse>(`/admin/principals/${principalId}/limits`, { signal: controller.signal });
        setData(res);
        setError(null);
      } catch (err) {
        if (err instanceof Error && err.name === 'AbortError') return;
        setError(err instanceof Error ? err : new Error(String(err)));
      } finally {
        setIsLoading(false);
      }
    }

    fetchLimits();
    const interval = setInterval(fetchLimits, 60000);

    return () => {
      controller.abort();
      clearInterval(interval);
    };
  }, [principalId, mock]);

  return { data, isLoading, error };
}
