import { useQueryClient } from '@tanstack/react-query';
import {
  createEventSource,
  type EventSourceClient,
  type EventSourceMessage,
} from 'eventsource-client';
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
  const filterKey = JSON.stringify(filters);
  const visibility = useVisibility();
  const pauseReason = !enabled
    ? 'disabled'
    : !visibility.online
      ? 'offline'
      : visibility.gracePeriodElapsed
        ? 'hidden'
        : 'active';
  const queryClient = useQueryClient();

  const [status, setStatus] = useState<ConnectionStatus>('idle');
  const [error, setError] = useState<Error | null>(null);
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
  const visibleRef = useRef(visibility.visible);
  visibleRef.current = visibility.visible;
  const clientRef = useRef<EventSourceClient | null>(null);
  const lastCursorRef = useRef<string | null>(null);
  const lastActivityAtRef = useRef<number | null>(null);
  const statusRef = useRef<ConnectionStatus>('idle');
  const malformedFrameCountRef = useRef(0);
  const permanentFailureSinceRef = useRef<number | null>(null);
  const reconnectAttemptsRef = useRef(0);
  const reconnectTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(
    null,
  );
  const connectionGenerationRef = useRef(0);
  const filterKeyRef = useRef(filterKey);
  const filtersRef = useRef(filters);
  filtersRef.current = filters;
  const pauseReasonRef = useRef(pauseReason);
  pauseReasonRef.current = pauseReason;
  const resumeNeedsBackfillRef = useRef(false);
  const connectRef = useRef<((isBackfill?: boolean) => Promise<void>) | null>(
    null,
  );

  const clearReconnectTimeout = () => {
    if (reconnectTimeoutRef.current) {
      clearTimeout(reconnectTimeoutRef.current);
      reconnectTimeoutRef.current = null;
    }
  };

  const closeCurrentConnection = () => {
    connectionGenerationRef.current += 1;
    const client = clientRef.current;
    clientRef.current = null;
    client?.close();
  };

  // Version counter published on every successful upsert so downstream
  // useMemo deps re-run when the mutated Map ref changes contents.
  const [version, setVersion] = useState(0);
  const forceUpdate = () => setVersion((t) => t + 1);

  const updateStatus = (newStatus: ConnectionStatus) => {
    if (statusRef.current === newStatus) return;
    statusRef.current = newStatus;
    setStatus(newStatus);
  };

  const updateActivity = () => {
    lastActivityAtRef.current = Date.now();
    if (
      statusRef.current === 'connecting' ||
      statusRef.current === 'stale' ||
      statusRef.current === 'reconnecting'
    ) {
      updateStatus('live');
    }
  };

  const resetFailureBookkeeping = () => {
    permanentFailureSinceRef.current = null;
    reconnectAttemptsRef.current = 0;
    setPermanentFailure(false);
    setPermanentFailureSince(null);
    setReconnectAttempts(0);
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
      if (
        !visibleRef.current ||
        (statusRef.current !== 'live' && statusRef.current !== 'stale')
      ) {
        return;
      }

      const now = Date.now();
      const lastActivity = lastActivityAtRef.current;

      if (lastActivity !== null) {
        const idleTime = now - lastActivity;
        if (idleTime > 45_000) {
          updateStatus('reconnecting');
          void connectRef.current?.();
        } else if (idleTime > 30_000 && statusRef.current === 'live') {
          updateStatus('stale');
        }
      }
    }, 5_000);
    return () => clearInterval(interval);
  }, []);

  const connect = async (isBackfill = false) => {
    clearReconnectTimeout();
    closeCurrentConnection();
    const connectionGeneration = connectionGenerationRef.current;
    const isCurrentConnection = () =>
      enabledRef.current &&
      pauseReasonRef.current === 'active' &&
      connectionGenerationRef.current === connectionGeneration;
    if (!enabledRef.current || pauseReasonRef.current !== 'active') {
      return;
    }

    const connectionFilters = filtersRef.current;
    const token = getAdminToken();
    const params = new URLSearchParams();
    for (const [k, v] of Object.entries(connectionFilters)) {
      if (v) params.set(k, v);
    }

    const backfillCursor =
      isBackfill && lastCursorRef.current ? lastCursorRef.current : null;

    if (backfillCursor !== null) {
      // Hybrid backfill
      try {
        const deltaParams = new URLSearchParams(params);
        deltaParams.set('since_cursor', backfillCursor);
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
        lastCursorRef.current = res.next_cursor.toString();
      } catch (error) {
        if (!isCurrentConnection()) return;
        if (error instanceof Error) {
          console.error('Backfill failed', error);
        } else {
          throw error;
        }
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
        resetFailureBookkeeping();
      },
      onDisconnect: () => {
        if (!isCurrentConnection()) return;
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
          setPermanentFailure(true);

          if (clientRef.current) {
            closeCurrentConnection();
            reconnectTimeoutRef.current = setTimeout(() => {
              if (statusRef.current === 'reconnecting') {
                void connectRef.current?.();
              }
            }, 60_000);
          }
        }
      },
      onMessage: (msg) => {
        if (!isCurrentConnection()) return;
        if (handleMessage(msg)) forceUpdate();
      },
    });
  };

  const handleMessage = (msg: EventSourceMessage): boolean => {
    if (!enabledRef.current) return false;
    updateActivity();

    if (msg.event === 'heartbeat') {
      if (msg.id) lastCursorRef.current = msg.id;
      return false;
    }

    if (msg.event === 'cursor') {
      if (msg.id) lastCursorRef.current = msg.id;
      return false;
    }

    if (msg.event === 'reset') {
      eventsMapRef.current.clear();
      finalizedIdsRef.current.clear();
      tombstonesRef.current.clear();
      lastCursorRef.current = '';
      queryClient.invalidateQueries({
        queryKey: qk.events(filtersRef.current),
      });

      updateStatus('reconnecting');
      void connectRef.current?.();
      return true;
    }

    let raw: unknown;
    try {
      raw = JSON.parse(msg.data);
    } catch {
      malformedFrameCountRef.current += 1;
      return false;
    }

    const parsed = RequestEventUpdateSchema.safeParse(raw);
    if (!parsed.success) {
      malformedFrameCountRef.current += 1;
      return false;
    }

    const update = parsed.data;
    if (update.phase === 'final') {
      lastCursorRef.current = update.payload.cursor.toString();
    }

    return upsertLiveEvent(
      eventsMapRef.current,
      finalizedIdsRef.current,
      update,
      tombstonesRef.current,
    );
  };

  connectRef.current = connect;

  // Reconcile only meaningful connection boundaries. Raw visibility changes
  // inside the grace period deliberately keep the current connection alive.
  // biome-ignore lint/correctness/useExhaustiveDependencies: refs hold callbacks
  useEffect(() => {
    const filtersChanged = filterKeyRef.current !== filterKey;
    if (filtersChanged) {
      filterKeyRef.current = filterKey;
      resumeNeedsBackfillRef.current = false;
      clearReconnectTimeout();
      closeCurrentConnection();
      eventsMapRef.current.clear();
      finalizedIdsRef.current.clear();
      tombstonesRef.current.clear();
      lastCursorRef.current = null;
      forceUpdate();
    }

    if (pauseReason !== 'active') {
      if (!filtersChanged && lastCursorRef.current !== null) {
        resumeNeedsBackfillRef.current = true;
      }
      clearReconnectTimeout();
      closeCurrentConnection();
      resetFailureBookkeeping();

      if (pauseReason === 'disabled') {
        updateStatus('idle');
        setError(null);
      } else if (pauseReason === 'offline') {
        updateStatus('error');
        setError(new Error('Offline'));
      } else {
        updateStatus('hidden');
        setError(null);
      }
      return;
    }

    if (!clientRef.current) {
      const shouldBackfill = resumeNeedsBackfillRef.current;
      resumeNeedsBackfillRef.current = false;
      updateStatus('connecting');
      setError(null);
      void connectRef.current?.(shouldBackfill);
    }
  }, [filterKey, pauseReason]);

  // Token refresh effect
  // biome-ignore lint/correctness/useExhaustiveDependencies: intentional
  useEffect(() => {
    const handleAuthRequired = () => {
      if (!enabledRef.current) return;
      clearReconnectTimeout();
      closeCurrentConnection();
      resetFailureBookkeeping();
      resumeNeedsBackfillRef.current = false;
      updateStatus('error');
      setError(new Error('Unauthorized'));
    };

    window.addEventListener('cclb:auth-required', handleAuthRequired);

    return () => {
      window.removeEventListener('cclb:auth-required', handleAuthRequired);
      clearReconnectTimeout();
      closeCurrentConnection();
    };
  }, []);

  const forceReconnect = () => {
    clearReconnectTimeout();
    if (pauseReasonRef.current !== 'active') {
      if (!enabledRef.current) updateStatus('idle');
      return;
    }
    updateStatus('reconnecting');
    void connectRef.current?.();
  };

  return {
    eventsMap: eventsMapRef.current,
    version,
    status,
    get lastActivityAt() {
      return lastActivityAtRef.current;
    },
    get lastCursor() {
      return lastCursorRef.current;
    },
    error,
    get malformedFrameCount() {
      return malformedFrameCountRef.current;
    },
    permanentFailure,
    permanentFailureSince,
    reconnectAttempts,
    forceReconnect,
  };
}
