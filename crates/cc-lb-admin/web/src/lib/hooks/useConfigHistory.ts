import { useState, useEffect, useCallback } from 'react';
import { getJson, ConfigHistoryResponse } from '../api';
import { MOCK_HISTORY } from './mockData';

export function useConfigHistory(limit: number = 20) {
  const [history, setHistory] = useState<ConfigHistoryResponse | null>(null);
  const [error, setError] = useState<Error | null>(null);
  const [loading, setLoading] = useState(true);

  const fetchHistory = useCallback(async () => {
    const isMock = new URLSearchParams(window.location.search).get('mock') === '1';
    if (isMock) {
      setHistory(MOCK_HISTORY);
      setLoading(false);
      return;
    }

    setLoading(true);
    try {
      const data = await getJson<ConfigHistoryResponse>(`/admin/config/history?limit=${limit}`);
      setHistory(data);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err : new Error(String(err)));
    } finally {
      setLoading(false);
    }
  }, [limit]);

  useEffect(() => {
    fetchHistory();
  }, [fetchHistory]);

  return { history, error, loading, fetchHistory };
}
