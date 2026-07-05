import { useQueryClient } from '@tanstack/react-query';
import { createEventSource, type EventSourceClient } from 'eventsource-client';
import { useEffect, useRef, useState } from 'react';
import {
  getJson,
  type RequestEvent,
  type RequestEventUpdate,
  RequestEventUpdateSchema,
} from './api';
import { getAdminToken } from './auth';
import { qk } from './queries';
import { type LiveEventMap, upsertLiveEvent } from './upsertReducer';
import { useVisibility } from './visibilityManager';

export type ConnectionStatus =
  | 'idle'
  | 'connecting'
  | 'live'
  | 'stale'
  | 'reconnecting'
  | 'hidden'
  | 'error';

export interface LiveEventStreamState {
  eventsMap: LiveEventMap;
  version: number;
  status: ConnectionStatus;
  lastActivityAt: number | null;
  lastCursor: string | null;
  error: Error | null;
  malformedFrameCount: number;
}

export function useLiveEventStream(
  filters: Record<string, string | undefined>,
): LiveEventStreamState {
  const visibility = useVisibility();
  const queryClient = useQueryClient();

  const [status, setStatus] = useState<ConnectionStatus>('idle');
  const [error, setError] = useState<Error | null>(null);
  const [lastActivityAt, setLastActivityAt] = useState<number | null>(null);
  const [lastCursor, setLastCursor] = useState<string | null>(null);
  const [malformedFrameCount, setMalformedFrameCount] = useState(0);

  // We use refs for mutable state that doesn't need to trigger re-renders immediately
  // or needs to be accessed in callbacks without stale closures.
  const eventsMapRef = useRef<LiveEventMap>(new Map());
  const finalizedIdsRef = useRef<Set<string>>(new Set());
  const clientRef = useRef<EventSourceClient | null>(null);
  const lastCursorRef = useRef<string | null>(null);
  const lastActivityAtRef = useRef<number | null>(null);
  const statusRef = useRef<ConnectionStatus>('idle');
  const malformedFrameCountRef = useRef(0);

  // Version counter published on every successful upsert so downstream
  // useMemo deps re-run when the mutated Map ref changes contents.
  const [version, setVersion] = useState(0);
  const forceUpdate = () => setVersion((t) => t + 1);

  const updateStatus = (newStatus: ConnectionStatus) => {
    statusRef.current = newStatus;
    setStatus(newStatus);
  };

  const updateActivity = () => {
    const now = Date.now();
    lastActivityAtRef.current = now;
    setLastActivityAt(now);
    if (
      statusRef.current === 'connecting' ||
      statusRef.current === 'stale' ||
      statusRef.current === 'reconnecting'
    ) {
      updateStatus('live');
    }
  };

  const updateCursor = (cursor: string) => {
    lastCursorRef.current = cursor;
    setLastCursor(cursor);
  };

  // Orphan cleanup
  // biome-ignore lint/correctness/useExhaustiveDependencies: intentional
  useEffect(() => {
    const interval = setInterval(() => {
      const now = Date.now();
      let changed = false;
      for (const [id, entry] of eventsMapRef.current.entries()) {
        if (entry.phase === 'partial') {
          const ts_ms =
            entry.event.ts_ms ??
            (entry.event.ts ? entry.event.ts * 1000 : null);
          if (ts_ms != null && ts_ms < now - 5 * 60 * 1000) {
            eventsMapRef.current.delete(id);
            changed = true;
          }
        }
      }
      if (changed) forceUpdate();
    }, 30_000);
    return () => clearInterval(interval);
  }, []);

  // Stale detector
  // biome-ignore lint/correctness/useExhaustiveDependencies: intentional
  useEffect(() => {
    const interval = setInterval(() => {
      if (statusRef.current !== 'live' && statusRef.current !== 'stale') return;

      const now = Date.now();
      const lastActivity = lastActivityAtRef.current;

      if (lastActivity) {
        const idleTime = now - lastActivity;
        if (idleTime > 45_000) {
          // Force reconnect
          if (clientRef.current) {
            clientRef.current.close();
            clientRef.current = null;
          }
          updateStatus('reconnecting');
          connect();
        } else if (idleTime > 30_000 && statusRef.current === 'live') {
          updateStatus('stale');
        }
      }
    }, 5_000);
    return () => clearInterval(interval);
  }, []);

  const connect = async (isBackfill = false) => {
    if (clientRef.current) {
      clientRef.current.close();
    }

    const token = getAdminToken();
    const params = new URLSearchParams();
    for (const [k, v] of Object.entries(filters)) {
      if (v) params.set(k, v);
    }

    let queuedEvents: { event?: string; id?: string; data?: string }[] = [];
    let isBackfilling = isBackfill;

    if (isBackfill && lastCursorRef.current) {
      // Hybrid backfill
      try {
        const deltaParams = new URLSearchParams(params);
        deltaParams.set('since_cursor', lastCursorRef.current);
        deltaParams.set('limit', '500');

        const res = await getJson<{
          events: RequestEvent[];
          next_cursor: number;
          exhausted: boolean;
        }>(`/admin/v1/events/delta?${deltaParams.toString()}`);

        let changed = false;
        for (const ev of res.events) {
          const update: RequestEventUpdate = {
            phase: 'final',
            payload: {
              event: ev as unknown as RequestEvent,
              cursor: res.next_cursor,
            },
          };
          if (
            upsertLiveEvent(
              eventsMapRef.current,
              finalizedIdsRef.current,
              update,
            )
          ) {
            changed = true;
          }
        }
        if (changed) forceUpdate();
        updateCursor(res.next_cursor.toString());
      } catch (err) {
        console.error('Backfill failed', err);
      } finally {
        isBackfilling = false;
        // Flush queued events
        let changed = false;
        for (const msg of queuedEvents) {
          if (handleMessage(msg)) changed = true;
        }
        if (changed) forceUpdate();
        queuedEvents = [];
      }
    }

    const url = `/admin/events/stream?${params.toString()}`;

    clientRef.current = createEventSource({
      url,
      headers: token ? { Authorization: `Bearer ${token}` } : {},
      initialLastEventId: lastCursorRef.current ?? undefined,
      onConnect: () => {
        updateStatus('connecting');
        setError(null);
      },
      onDisconnect: () => {
        updateStatus('reconnecting');
      },
      onScheduleReconnect: () => {
        updateStatus('reconnecting');
      },
      onMessage: (msg) => {
        if (isBackfilling) {
          queuedEvents.push(msg);
        } else {
          if (handleMessage(msg)) forceUpdate();
        }
      },
    });
  };

  const handleMessage = (msg: {
    event?: string;
    id?: string;
    data?: string;
  }): boolean => {
    updateActivity();

    if (msg.event === 'heartbeat') {
      if (msg.id) updateCursor(msg.id);
      return false;
    }

    if (msg.event === 'cursor') {
      if (msg.id) updateCursor(msg.id);
      return false;
    }

    if (msg.event === 'reset') {
      eventsMapRef.current.clear();
      finalizedIdsRef.current.clear();
      updateCursor('');
      queryClient.invalidateQueries({ queryKey: qk.events(filters) });

      if (clientRef.current) {
        clientRef.current.close();
      }
      connect();
      return true;
    }

    let raw: unknown;
    try {
      if (msg.data === undefined) return false;
      raw = JSON.parse(msg.data);
    } catch {
      malformedFrameCountRef.current += 1;
      setMalformedFrameCount(malformedFrameCountRef.current);
      return false;
    }

    const parsed = RequestEventUpdateSchema.safeParse(raw);
    if (!parsed.success) {
      malformedFrameCountRef.current += 1;
      setMalformedFrameCount(malformedFrameCountRef.current);
      return false;
    }

    const update = parsed.data;
    if (update.phase === 'final') {
      updateCursor(update.payload.cursor.toString());
    }

    return upsertLiveEvent(
      eventsMapRef.current,
      finalizedIdsRef.current,
      update,
    );
  };

  // Main effect for connection lifecycle and visibility
  // biome-ignore lint/correctness/useExhaustiveDependencies: intentional
  useEffect(() => {
    if (!visibility.online) {
      if (clientRef.current) {
        clientRef.current.close();
        clientRef.current = null;
      }
      updateStatus('error');
      setError(new Error('Offline'));
      return;
    }

    if (visibility.gracePeriodElapsed) {
      if (clientRef.current) {
        clientRef.current.close();
        clientRef.current = null;
      }
      updateStatus('hidden');
      return;
    }

    if (statusRef.current === 'hidden' && visibility.visible) {
      updateStatus('connecting');
      connect(true);
      return;
    }

    if (!clientRef.current) {
      connect();
    }

    return () => {
      if (clientRef.current) {
        clientRef.current.close();
        clientRef.current = null;
      }
    };
  }, [
    visibility.online,
    visibility.gracePeriodElapsed,
    visibility.visible,
    JSON.stringify(filters),
  ]);

  // Token refresh effect
  // biome-ignore lint/correctness/useExhaustiveDependencies: intentional
  useEffect(() => {
    const handleAuthRequired = () => {
      if (clientRef.current) {
        clientRef.current.close();
        clientRef.current = null;
      }
      updateStatus('error');
      setError(new Error('Unauthorized'));
    };

    window.addEventListener('cclb:auth-required', handleAuthRequired);

    return () => {
      window.removeEventListener('cclb:auth-required', handleAuthRequired);
    };
  }, []);

  return {
    eventsMap: eventsMapRef.current,
    version,
    status,
    lastActivityAt,
    lastCursor,
    error,
    malformedFrameCount,
  };
}
