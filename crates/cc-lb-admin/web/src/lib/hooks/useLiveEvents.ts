import { useState, useEffect, useRef } from 'react';
import { streamEventsFetch, RequestEvent } from '../api';

export type LiveEventStatus = 'connecting' | 'live' | 'reconnecting' | 'error' | 'closed';

export function useLiveEvents() {
  const [events, setEvents] = useState<RequestEvent[]>([]);
  const [status, setStatus] = useState<LiveEventStatus>('connecting');
  const [error, setError] = useState<Error | null>(null);
  const isPaused = useRef(false);

  useEffect(() => {
    let closeStream: (() => void) | null = null;
    let retryTimeout: ReturnType<typeof setTimeout>;

    function connect() {
      setStatus('connecting');
      closeStream = streamEventsFetch('/admin/events/stream', {
        onConnect: () => {
          setStatus('live');
          setError(null);
        },
        onEvent: (ev) => {
          if (isPaused.current) return;
          try {
            const data = JSON.parse(ev.data) as RequestEvent;
            setEvents((prev) => {
              const next = [data, ...prev];
              return next.slice(0, 10);
            });
          } catch {
            // ignore parse error
          }
        },
        onError: (err) => {
          setError(err);
          setStatus('reconnecting');
          retryTimeout = setTimeout(connect, 5000);
        }
      });
    }

    connect();

    return () => {
      if (closeStream) closeStream();
      clearTimeout(retryTimeout);
      setStatus('closed');
    };
  }, []);

  const pause = () => { isPaused.current = true; };
  const resume = () => { isPaused.current = false; };

  return { events, status, error, pause, resume };
}
