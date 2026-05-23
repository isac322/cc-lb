import { useState, useEffect, useCallback } from 'react';
import { getJson, UpstreamHealthResponse, UpstreamListResponse } from '../api';

export function useUpstreamHealth() {
  const [healthByName, setHealthByName] = useState<Record<string, UpstreamHealthResponse>>({});
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  const refresh = useCallback(async () => {
    setIsLoading(true);
    setError(null);
    try {
      const isMock = new URLSearchParams(window.location.search).get('mock');
      if (isMock === '1') {
        setHealthByName({
          anthropic_direct: {
            name: 'anthropic_direct',
            kind: 'anthropic_direct',
            breaker: { state: 'closed', failure_count: 0, half_open_in_flight: 0, observed: true },
            bulkhead: { max_conns: 100, available_permits: 95, observed: true },
            drain: { draining: true, in_flight: 5 },
            killswitch: false,
            last_probe_unix_secs: Math.floor(Date.now() / 1000) - 10,
            error_count_recent: 0,
          },
          bedrock_runtime: {
            name: 'bedrock_runtime',
            kind: 'bedrock_runtime',
            breaker: { state: 'half_open', failure_count: 3, half_open_in_flight: 1, observed: true },
            bulkhead: { max_conns: 50, available_permits: 49, observed: true },
            drain: { draining: false, in_flight: 0 },
            killswitch: false,
            last_probe_unix_secs: Math.floor(Date.now() / 1000) - 60,
            error_count_recent: 3,
          },
          vertex: {
            name: 'vertex',
            kind: 'vertex',
            breaker: { state: 'open', failure_count: 12, half_open_in_flight: 0, observed: true },
            bulkhead: { max_conns: 20, available_permits: 20, observed: true },
            drain: { draining: false, in_flight: 0 },
            killswitch: false,
            last_probe_unix_secs: Math.floor(Date.now() / 1000) - 300,
            error_count_recent: 12,
          },
        });
        setIsLoading(false);
        return;
      } else if (isMock === 'empty') {
        setHealthByName({});
        setIsLoading(false);
        return;
      }

      const listRes = await getJson<UpstreamListResponse>('/admin/upstreams');
      const names = listRes.upstreams.map(u => u.name);
      
      const healthResults = await Promise.all(
        names.map(name => getJson<UpstreamHealthResponse>(`/admin/upstreams/${name}/health`).catch(e => {
          console.error(`Failed to fetch health for ${name}`, e);
          return null;
        }))
      );

      const newHealth: Record<string, UpstreamHealthResponse> = {};
      healthResults.forEach(res => {
        if (res) {
          newHealth[res.name] = res;
        }
      });
      setHealthByName(newHealth);
    } catch (err) {
      setError(err instanceof Error ? err : new Error(String(err)));
    } finally {
      setIsLoading(false);
    }
  }, []);

  useEffect(() => {
    refresh();
    const interval = setInterval(refresh, 30000);
    return () => clearInterval(interval);
  }, [refresh]);

  return { healthByName, isLoading, error, refresh };
}
