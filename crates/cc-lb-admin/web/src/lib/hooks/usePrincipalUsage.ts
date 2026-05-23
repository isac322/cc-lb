import { useState, useEffect } from 'react';
import { getJson, DashboardUsageResponse } from '../api';
import { MOCK_USAGE } from './mockData';

export function usePrincipalUsage(principalId: string | null, range: string, mock?: boolean) {
  const [data, setData] = useState<DashboardUsageResponse | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  useEffect(() => {
    if (!principalId) {
      setData(null);
      setIsLoading(false);
      return;
    }

    if (mock) {
      setData(MOCK_USAGE);
      setIsLoading(false);
      setError(null);
      return;
    }

    const controller = new AbortController();
    
    async function fetchUsage() {
      try {
        setIsLoading(true);
        const res = await getJson<DashboardUsageResponse>(`/admin/principals/${principalId}/usage?range=${range}`, { signal: controller.signal });
        setData(res);
        setError(null);
      } catch (err) {
        if (err instanceof Error && err.name === 'AbortError') return;
        setError(err instanceof Error ? err : new Error(String(err)));
      } finally {
        setIsLoading(false);
      }
    }

    fetchUsage();
    const interval = setInterval(fetchUsage, 60000);

    return () => {
      controller.abort();
      clearInterval(interval);
    };
  }, [principalId, range, mock]);

  return { data, isLoading, error };
}
