import { useEffect, useState } from 'react';
import { getJson, type RecentEventsPayload, type RequestEvent } from '../api';

export function useRecentEvents(
  filters: Record<string, string>,
  mock: boolean,
) {
  const [events, setEvents] = useState<RequestEvent[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  useEffect(() => {
    if (mock) {
      setLoading(false);
      return;
    }

    let isMounted = true;
    setLoading(true);
    setError(null);

    const params = new URLSearchParams();
    params.set('limit', '100');
    for (const [k, v] of Object.entries(filters)) {
      if (v) params.set(k, v);
    }

    getJson<RecentEventsPayload>(`/admin/events/recent?${params.toString()}`)
      .then((data) => {
        if (isMounted) {
          setEvents(data.events);
          setLoading(false);
        }
      })
      .catch((err) => {
        if (isMounted) {
          setError(err instanceof Error ? err : new Error(String(err)));
          setLoading(false);
        }
      });

    return () => {
      isMounted = false;
    };
  }, [filters, mock]);

  return { events, loading, error };
}
