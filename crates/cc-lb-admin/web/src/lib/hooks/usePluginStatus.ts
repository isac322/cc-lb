import { useCallback, useEffect, useState } from 'react';
import {
  getJson,
  type PluginStatusEntry,
  type PluginsStatusResponse,
} from '../api';

export function usePluginStatus() {
  const [plugins, setPlugins] = useState<PluginStatusEntry[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  const refresh = useCallback(async () => {
    setIsLoading(true);
    setError(null);
    try {
      const isMock = new URLSearchParams(window.location.search).get('mock');
      if (isMock === '1') {
        setPlugins([
          {
            slot: 'authn',
            name: 'my-authn-plugin',
            wasm_path: '/path/to/authn.wasm',
            loaded: true,
            disabled: false,
            failure_count: 0,
            last_error: null,
            sse_per_event: null,
            batched_events_per_flush: null,
            batched_flush_ms: null,
          },
          {
            slot: 'router',
            name: 'my-router-plugin',
            wasm_path: '/path/to/router.wasm',
            loaded: true,
            disabled: true,
            failure_count: 0,
            last_error: null,
            sse_per_event: null,
            batched_events_per_flush: null,
            batched_flush_ms: null,
          },
          {
            slot: 'observability',
            name: 'my-obs-plugin',
            wasm_path: '/path/to/obs.wasm',
            loaded: true,
            disabled: false,
            failure_count: 2,
            last_error: 'Failed to connect to external service',
            sse_per_event: true,
            batched_events_per_flush: 100,
            batched_flush_ms: 1000,
          },
        ]);
        setIsLoading(false);
        return;
      } else if (isMock === 'empty') {
        setPlugins([]);
        setIsLoading(false);
        return;
      }

      const res = await getJson<PluginsStatusResponse>('/admin/plugins');
      setPlugins(res.plugins);
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

  return { plugins, isLoading, error, refresh };
}
