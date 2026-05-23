import { useState, useEffect, useRef, useCallback } from 'react';
import { streamEventsFetch, RequestEvent } from '../api';

let mockIdCounter = 1;
export function generateMockEvent(): RequestEvent {
  const statuses = [200, 200, 200, 200, 429, 500];
  const principals = ['p_123', 'p_456'];
  const models = ['claude-3-haiku', 'claude-3-opus'];
  const upstreams = ['anthropic_direct', 'bedrock_runtime'];
  
  return {
    request_id: `req_${mockIdCounter++}`,
    ts: Date.now() - Math.floor(Math.random() * 10000),
    principal_id: principals[Math.floor(Math.random() * principals.length)],
    model: models[Math.floor(Math.random() * models.length)],
    upstream: upstreams[Math.floor(Math.random() * upstreams.length)],
    status: statuses[Math.floor(Math.random() * statuses.length)],
    duration_ms: Math.floor(Math.random() * 2000) + 50,
    input_tokens: Math.floor(Math.random() * 1000),
    output_tokens: Math.floor(Math.random() * 500),
  };
}

export function useEventStream(filters: Record<string, string>, mock: boolean, initialEvents: RequestEvent[]) {
  const [events, setEvents] = useState<RequestEvent[]>(initialEvents);
  const [status, setStatus] = useState<'live' | 'reconnecting' | 'error'>('reconnecting');
  const [paused, setPaused] = useState(false);
  const [bufferedCount, setBufferedCount] = useState(0);
  const [lastError, setLastError] = useState<Error | null>(null);
  
  const bufferRef = useRef<RequestEvent[]>([]);
  const eventsRef = useRef<RequestEvent[]>(initialEvents);
  const pausedRef = useRef(paused);
  
  useEffect(() => {
    pausedRef.current = paused;
  }, [paused]);

  useEffect(() => {
    if (!mock && initialEvents.length > 0) {
      setEvents(initialEvents);
      eventsRef.current = initialEvents;
    }
  }, [initialEvents, mock]);

  const flushBuffer = useCallback(() => {
    if (bufferRef.current.length > 0) {
      setEvents(prev => {
        const next = [...bufferRef.current, ...prev].slice(0, 2000);
        eventsRef.current = next;
        return next;
      });
      bufferRef.current = [];
      setBufferedCount(0);
    }
  }, []);

  const clearEvents = useCallback(() => {
    setEvents([]);
    eventsRef.current = [];
    bufferRef.current = [];
    setBufferedCount(0);
  }, []);

  useEffect(() => {
    if (mock) {
      setStatus('live');
      const initialMockEvents = Array.from({ length: 15 }, generateMockEvent).sort((a, b) => b.ts - a.ts);
      setEvents(initialMockEvents);
      eventsRef.current = initialMockEvents;

      const interval = setInterval(() => {
        const newEvent = generateMockEvent();
        newEvent.ts = Date.now();
        
        if (filters.principal_id && newEvent.principal_id !== filters.principal_id) return;
        if (filters.model && newEvent.model !== filters.model) return;
        if (filters.upstream && newEvent.upstream !== filters.upstream) return;
        if (filters.status_class) {
          const cls = filters.status_class;
          const s = newEvent.status;
          if (cls === '2xx' && (s < 200 || s >= 300)) return;
          if (cls === '3xx' && (s < 300 || s >= 400)) return;
          if (cls === '4xx' && (s < 400 || s >= 500)) return;
          if (cls === '5xx' && (s < 500 || s >= 600)) return;
        }

        if (pausedRef.current) {
          bufferRef.current.unshift(newEvent);
          setBufferedCount(bufferRef.current.length);
        } else {
          setEvents(prev => {
            const next = [newEvent, ...prev].slice(0, 2000);
            eventsRef.current = next;
            return next;
          });
        }
      }, 1500);
      return () => clearInterval(interval);
    }

    let isMounted = true;
    let closeStream: (() => void) | null = null;
    let retryTimeout: ReturnType<typeof setTimeout>;
    let attempt = 0;

    function connect() {
      if (!isMounted) return;
      
      const params = new URLSearchParams();
      for (const [k, v] of Object.entries(filters)) {
        if (v) params.set(k, v);
      }
      
      closeStream = streamEventsFetch(`/admin/events/stream?${params.toString()}`, {
        onConnect: () => {
          if (!isMounted) return;
          setStatus('live');
          setLastError(null);
          attempt = 0;
        },
        onEvent: (ev) => {
          if (!isMounted) return;
          try {
            const newEvent = JSON.parse(ev.data) as RequestEvent;
            if (pausedRef.current) {
              bufferRef.current.unshift(newEvent);
              setBufferedCount(bufferRef.current.length);
            } else {
              setEvents(prev => {
                const next = [newEvent, ...prev].slice(0, 2000);
                eventsRef.current = next;
                return next;
              });
            }
          } catch (e) {
            console.error('Failed to parse event', e);
          }
        },
        onError: (err) => {
          if (!isMounted) return;
          setStatus('error');
          setLastError(err);
          
          const jitter = (Math.random() * 0.5) - 0.25; // [-0.25, 0.25]
          const delay = Math.min(30000, 500 * Math.pow(2, attempt)) * (1 + jitter);
          attempt++;
          
          retryTimeout = setTimeout(connect, delay);
        }
      });
    }

    connect();

    return () => {
      isMounted = false;
      if (closeStream) closeStream();
      clearTimeout(retryTimeout);
    };
  }, [filters, mock]);

  return { events, status, paused, setPaused, bufferedCount, flushBuffer, lastError, clearEvents };
}
