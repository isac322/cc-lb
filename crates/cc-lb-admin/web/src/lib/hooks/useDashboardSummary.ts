import { useCallback, useEffect, useState } from 'react';
import { type DashboardSummaryResponse, getJson } from '../api';

export function useDashboardSummary(range: string) {
  const [data, setData] = useState<DashboardSummaryResponse | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  const refresh = useCallback(() => {
    const controller = new AbortController();
    setIsLoading(true);
    setError(null);

    getJson<DashboardSummaryResponse>(
      `/admin/dashboard/summary?range=${range}`,
      { signal: controller.signal },
    )
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
  }, [range]);

  useEffect(() => {
    const controller = refresh();
    const interval = setInterval(() => {
      getJson<DashboardSummaryResponse>(
        `/admin/dashboard/summary?range=${range}`,
      )
        .then((res) => setData(res))
        .catch((err) => {
          if (err.name !== 'AbortError') setError(err);
        });
    }, 30000);

    return () => {
      controller.abort();
      clearInterval(interval);
    };
  }, [range, refresh]);

  return { data, isLoading, error, refresh };
}
