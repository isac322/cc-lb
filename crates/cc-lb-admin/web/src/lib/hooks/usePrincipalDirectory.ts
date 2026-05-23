import { useEffect, useState } from 'react';
import { getJson, type PrincipalListResponse } from '../api';
import { MOCK_DIRECTORY } from './mockData';

export function usePrincipalDirectory(mock?: boolean) {
  const [data, setData] = useState<PrincipalListResponse | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  useEffect(() => {
    if (mock) {
      setData(MOCK_DIRECTORY);
      setIsLoading(false);
      setError(null);
      return;
    }

    const controller = new AbortController();

    async function fetchDirectory() {
      try {
        setIsLoading(true);
        const res = await getJson<PrincipalListResponse>('/admin/principals', {
          signal: controller.signal,
        });
        setData(res);
        setError(null);
      } catch (err) {
        if (err instanceof Error && err.name === 'AbortError') return;
        setError(err instanceof Error ? err : new Error(String(err)));
      } finally {
        setIsLoading(false);
      }
    }

    fetchDirectory();

    return () => controller.abort();
  }, [mock]);

  return { data, isLoading, error };
}
