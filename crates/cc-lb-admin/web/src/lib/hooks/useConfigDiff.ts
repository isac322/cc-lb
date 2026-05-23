import { useState, useEffect } from 'react';
import { getJson, ConfigDiffResponse } from '../api';
import { MOCK_DIFF } from './mockData';

export function useConfigDiff(fromRevision: number | null, toRevision: number | null) {
  const [diff, setDiff] = useState<ConfigDiffResponse | null>(null);
  const [error, setError] = useState<Error | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (fromRevision === null || toRevision === null) {
      setDiff(null);
      return;
    }

    const isMock = new URLSearchParams(window.location.search).get('mock') === '1';
    if (isMock) {
      setLoading(true);
      setTimeout(() => {
        setDiff(MOCK_DIFF);
        setLoading(false);
      }, 500);
      return;
    }

    setLoading(true);
    getJson<ConfigDiffResponse>(`/admin/config/diff?from_revision=${fromRevision}&to_revision=${toRevision}`)
      .then((data) => {
        setDiff(data);
        setError(null);
      })
      .catch((err) => {
        setError(err instanceof Error ? err : new Error(String(err)));
      })
      .finally(() => {
        setLoading(false);
      });
  }, [fromRevision, toRevision]);

  return { diff, error, loading };
}
