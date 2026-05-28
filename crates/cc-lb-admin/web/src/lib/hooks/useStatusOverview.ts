import { useEffect, useState } from 'react';
import { getJson } from '../api';
import type { StatusResponse } from '../types/v1';

export function useStatus() {
  const [status, setStatus] = useState<StatusResponse | null>(null);
  const [error, setError] = useState<Error | null>(null);
  const [replicaHistory, setReplicaHistory] = useState<string[]>([]);

  useEffect(() => {
    let controller = new AbortController();
    let timeoutId: ReturnType<typeof setTimeout>;

    async function fetchStatus() {
      try {
        const data = await getJson<StatusResponse>('/admin/v1/status', {
          signal: controller.signal,
        });
        setStatus(data);
        setError(null);

        if (data.replica_id) {
          const newReplicaId = data.replica_id;
          setReplicaHistory((prev) => {
            if (prev[0] === newReplicaId) return prev;
            return [newReplicaId, ...prev].slice(0, 3);
          });
        }
      } catch (err) {
        if (err instanceof Error && err.name === 'AbortError') return;
        setError(err instanceof Error ? err : new Error(String(err)));
      } finally {
        timeoutId = setTimeout(() => {
          controller = new AbortController();
          fetchStatus();
        }, 5000);
      }
    }

    fetchStatus();

    return () => {
      controller.abort();
      clearTimeout(timeoutId);
    };
  }, []);

  return {
    upstreams: status?.upstreams ?? [],
    principals: status?.principals ?? [],
    generation: status?.generation ?? 0,
    replica_id: status?.replica_id,
    replicaHistory,
    error,
  };
}
