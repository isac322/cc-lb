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
  permanentFailure: boolean;
  permanentFailureSince: number | null;
  reconnectAttempts: number;
  forceReconnect: () => void;
}

export type LiveEventStreamOptions = {
  readonly enabled?: boolean;
};

const PERMANENT_FAILURE_THRESHOLD_MS = 300_000;

export function useLiveEventStream(
  filters: Record<string, string | undefined>,
  options: LiveEventStreamOptions = {},
): LiveEventStreamState {
  const enabled = options.enabled ?? true;
  const visibility = useVisibility();
  const queryClient = useQueryClient();

  const [status, setStatus] = useState<ConnectionStatus>('idle');
  const [error, setError] = useState<Error | null>(null);
  const [lastActivityAt, setLastActivityAt] = useState<number | null>(null);
  const [lastCursor, setLastCursor] = useState<string | null>(null);
  const [malformedFrameCount, setMalformedFrameCount] = useState(0);
  const [permanentFailure, setPermanentFailure] = useState(false);
  const [permanentFailureSince, setPermanentFailureSince] = useState<
    number | null
  >(null);
  const [reconnectAttempts, setReconnectAttempts] = useState(0);

  // We use refs for mutable state that doesn't need to trigger re-renders immediately
  // or needs to be accessed in callbacks without stale closures.
  const eventsMapRef = useRef<LiveEventMap>(new Map());
  const finalizedIdsRef = useRef<Set<string>>(new Set());
  const tombstonesRef = useRef<Set<string>>(new Set());
  const enabledRef = useRef(enabled);
  enabledRef.current = enabled;
  const clientRef = useRef<EventSourceClient | null>(null);
  const lastCursorRef = useRef<string | null>(null);
  const lastActivityAtRef = useRef<number | null>(null);
  const statusRef = useRef<ConnectionStatus>('idle');
  const malformedFrameCountRef = useRef(0);
  const permanentFailureRef = useRef(false);
  const permanentFailureSinceRef = useRef<number | null>(null);
  const reconnectAttemptsRef = useRef(0);
  const reconnectTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(
    null,
  );
  const connectionGenerationRef = useRef(0);

  const clearReconnectTimeout = () => {
    if (reconnectTimeoutRef.current) {
      clearTimeout(reconnectTimeoutRef.current);
      reconnectTimeoutRef.current = null;
    }
  };

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
          const lastUpdateMs =
            entry.event.last_update_ms ??
            entry.event.ts_ms ??
            (entry.event.ts ? entry.event.ts * 1000 : null);
          if (lastUpdateMs != null && lastUpdateMs < now - 5 * 60 * 1000) {
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
    const connectionGeneration = connectionGenerationRef.current + 1;
    connectionGenerationRef.current = connectionGeneration;
    const isCurrentConnection = () =>
      enabledRef.current &&
      connectionGenerationRef.current === connectionGeneration;
    clearReconnectTimeout();
    if (!enabledRef.current) {
      if (clientRef.current) {
        clientRef.current.close();
        clientRef.current = null;
      }
      updateStatus('idle');
      return;
    }
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
        if (!isCurrentConnection()) return;

        let changed = false;
        for (const ev of res.events) {
          const update: RequestEventUpdate = {
            phase: 'final',
            payload: {
              event: ev,
              cursor: res.next_cursor,
            },
          };
          if (
            upsertLiveEvent(
              eventsMapRef.current,
              finalizedIdsRef.current,
              update,
              tombstonesRef.current,
            )
          ) {
            changed = true;
          }
        }
        if (changed) forceUpdate();
        updateCursor(res.next_cursor.toString());
      } catch (error) {
        if (!isCurrentConnection()) return;
        if (error instanceof Error) {
          console.error('Backfill failed', error);
        } else {
          throw error;
        }
      } finally {
        isBackfilling = false;
        if (isCurrentConnection()) {
          // Flush queued events
          let changed = false;
          for (const msg of queuedEvents) {
            if (handleMessage(msg)) changed = true;
          }
          if (changed) forceUpdate();
        }
        queuedEvents = [];
      }
    }

    if (!isCurrentConnection()) {
      if (!enabledRef.current) updateStatus('idle');
      return;
    }

    const url = `/admin/events/stream?${params.toString()}`;

    clientRef.current = createEventSource({
      url,
      headers: token ? { Authorization: `Bearer ${token}` } : {},
      initialLastEventId: lastCursorRef.current ?? undefined,
      onConnect: () => {
        if (!isCurrentConnection()) return;
        updateStatus('connecting');
        setError(null);
        permanentFailureRef.current = false;
        setPermanentFailure(false);
        permanentFailureSinceRef.current = null;
        setPermanentFailureSince(null);
        reconnectAttemptsRef.current = 0;
        setReconnectAttempts(0);
      },
      onDisconnect: () => {
        if (!isCurrentConnection()) {
          updateStatus('idle');
          return;
        }
        updateStatus('reconnecting');
      },
      onScheduleReconnect: () => {
        if (!isCurrentConnection()) return;
        updateStatus('reconnecting');
        reconnectAttemptsRef.current += 1;
        setReconnectAttempts(reconnectAttemptsRef.current);

        if (permanentFailureSinceRef.current === null) {
          permanentFailureSinceRef.current = Date.now();
          setPermanentFailureSince(permanentFailureSinceRef.current);
        }

        const duration = Date.now() - permanentFailureSinceRef.current;
        if (duration >= PERMANENT_FAILURE_THRESHOLD_MS) {
          permanentFailureRef.current = true;
          setPermanentFailure(true);

          if (clientRef.current) {
            clientRef.current.close();
            clientRef.current = null;
            reconnectTimeoutRef.current = setTimeout(() => {
              if (statusRef.current === 'reconnecting') {
                connect();
              }
            }, 60_000);
          }
        }
      },
      onMessage: (msg) => {
        if (!isCurrentConnection()) return;
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
    if (!enabledRef.current) return false;
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
      tombstonesRef.current.clear();
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
      tombstonesRef.current,
    );
  };

  // Main effect for connection lifecycle and visibility
  // biome-ignore lint/correctness/useExhaustiveDependencies: intentional
  useEffect(() => {
    if (!enabled) {
      connectionGenerationRef.current += 1;
      clearReconnectTimeout();
      if (clientRef.current) {
        clientRef.current.close();
        clientRef.current = null;
      }
      updateStatus('idle');
      setError(null);
      return;
    }

    if (!visibility.online) {
      connectionGenerationRef.current += 1;
      if (clientRef.current) {
        clientRef.current.close();
        clientRef.current = null;
      }
      updateStatus('error');
      setError(new Error('Offline'));
      return;
    }

    if (visibility.gracePeriodElapsed) {
      connectionGenerationRef.current += 1;
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
      connectionGenerationRef.current += 1;
      clearReconnectTimeout();
      if (clientRef.current) {
        clientRef.current.close();
        clientRef.current = null;
      }
    };
  }, [
    visibility.online,
    visibility.gracePeriodElapsed,
    visibility.visible,
    enabled,
    JSON.stringify(filters),
  ]);

  // Token refresh effect
  // biome-ignore lint/correctness/useExhaustiveDependencies: intentional
  useEffect(() => {
    const handleAuthRequired = () => {
      if (!enabledRef.current) return;
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

  const forceReconnect = () => {
    clearReconnectTimeout();
    if (!enabledRef.current) {
      updateStatus('idle');
      return;
    }
    updateStatus('reconnecting');
    connect();
  };

  return {
    eventsMap: eventsMapRef.current,
    version,
    status,
    lastActivityAt,
    lastCursor,
    error,
    malformedFrameCount,
    permanentFailure,
    permanentFailureSince,
    reconnectAttempts,
    forceReconnect,
  };
}
