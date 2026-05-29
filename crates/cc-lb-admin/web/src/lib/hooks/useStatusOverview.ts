import { useCallback, useEffect, useState } from 'react';
import { getJson } from '../api';
import type { StatusResponse } from '../types/v1';

export function useStatus() {
  const [status, setStatus] = useState<StatusResponse | null>(null);
  const [error, setError] = useState<Error | null>(null);
  const [replicaHistory, setReplicaHistory] = useState<string[]>([]);

  const fetchStatus = useCallback(async (signal?: AbortSignal) => {
    const isMock =
      new URLSearchParams(window.location.search).get('mock') === '1';
    if (isMock) {
      const mockRestartRequired =
        new URLSearchParams(window.location.search).get('restartRequired') ===
        '1';
      const data: StatusResponse = {
        version: 'mock',
        git_sha: 'mock',
        uptime_secs: 60,
        build: { rust_version: 'mock', profile: 'debug', target: 'mock' },
        generation: 1,
        upstreams: [],
        principals: [],
        plugin_chain_summary: {
          principal_count_with_chain: 0,
          total_entries: 0,
        },
        killswitch: false,
        restart_required_changes: mockRestartRequired
          ? [
              {
                field: 'listener.proxy_addr',
                current: '127.0.0.1:8080',
                new: '127.0.0.1:8081',
                reason: 'socket binding changes require a process restart',
              },
            ]
          : [],
      };
      setStatus(data);
      setError(null);
      return data;
    }

    const data = await getJson<StatusResponse>('/admin/v1/status', { signal });
    setStatus(data);
    setError(null);

    if (data.replica_id) {
      const newReplicaId = data.replica_id;
      setReplicaHistory((prev) => {
        if (prev[0] === newReplicaId) return prev;
        return [newReplicaId, ...prev].slice(0, 3);
      });
    }
    return data;
  }, []);

  useEffect(() => {
    let controller = new AbortController();
    let timeoutId: ReturnType<typeof setTimeout>;

    async function pollStatus() {
      try {
        await fetchStatus(controller.signal);
      } catch (err) {
        if (err instanceof Error && err.name === 'AbortError') return;
        setError(err instanceof Error ? err : new Error(String(err)));
      } finally {
        timeoutId = setTimeout(() => {
          controller = new AbortController();
          pollStatus();
        }, 5000);
      }
    }

    pollStatus();

    return () => {
      controller.abort();
      clearTimeout(timeoutId);
    };
  }, [fetchStatus]);

  return {
    upstreams: status?.upstreams ?? [],
    principals: status?.principals ?? [],
    generation: status?.generation ?? 0,
    replica_id: status?.replica_id,
    restartRequiredChanges: status?.restart_required_changes ?? [],
    replicaHistory,
    error,
    refreshStatus: () => fetchStatus(),
  };
}
