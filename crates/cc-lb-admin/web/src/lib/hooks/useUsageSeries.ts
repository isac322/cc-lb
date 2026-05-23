import { useState, useEffect, useCallback } from 'react';
import { getJson, DashboardUsageResponse } from '../api';

export function useUsageSeries(range: string, groupBy: string) {
  const [data, setData] = useState<DashboardUsageResponse | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  const refresh = useCallback(() => {
    const controller = new AbortController();
    setIsLoading(true);
    setError(null);

    getJson<DashboardUsageResponse>(`/admin/usage?range=${range}&group_by=${groupBy}`, { signal: controller.signal })
      .then((res) => {
        setData(res);
        setIsLoading(false);
      })
      .catch((err) => {
        if (err.name === 'AbortError') return;
        setError(err);
        setIsLoading(false);
      });

    return controller;
  }, [range, groupBy]);

  useEffect(() => {
    const controller = refresh();
    const interval = setInterval(() => {
      getJson<DashboardUsageResponse>(`/admin/usage?range=${range}&group_by=${groupBy}`)
        .then((res) => setData(res))
        .catch((err) => {
          if (err.name !== 'AbortError') setError(err);
        });
    }, 30000);

    return () => {
      controller.abort();
      clearInterval(interval);
    };
  }, [range, groupBy, refresh]);

  return { data, isLoading, error, refresh };
}
