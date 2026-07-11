import { describe, expect, it } from 'vitest';
import type { RequestEventUpdate } from './api';
import {
  type LiveEventMap,
  MAX_LIVE_PARTIAL_EVENTS,
  upsertLiveEvent,
} from './upsertReducer';

function makePartialUpdate({
  eventId,
  lastUpdateMs,
  tsMs = 0,
}: {
  readonly eventId: string;
  readonly lastUpdateMs?: number;
  readonly tsMs?: number;
}): RequestEventUpdate {
  return {
    phase: 'partial',
    payload: {
      event_id: eventId,
      request_id: `request-${eventId}`,
      ts: tsMs / 1000,
      ts_ms: tsMs,
      last_update_ms: lastUpdateMs,
    },
  };
}

function makeFinalUpdate(eventId: string): RequestEventUpdate {
  return {
    phase: 'final',
    payload: {
      event: {
        event_id: eventId,
        request_id: `request-${eventId}`,
        ts: 1,
        ts_ms: 1000,
        status: 200,
        duration_ms: 10,
      },
      cursor: 1000,
    },
  };
}

describe('upsertReducer partial retention', () => {
  it('evicts the least recently updated partial instead of insertion order or request start', () => {
    const eventsMap: LiveEventMap = new Map();
    const finalizedIds = new Set<string>();

    for (let index = 0; index < MAX_LIVE_PARTIAL_EVENTS; index += 1) {
      upsertLiveEvent(
        eventsMap,
        finalizedIds,
        makePartialUpdate({
          eventId: `partial-${index.toString().padStart(3, '0')}`,
          lastUpdateMs: 10_000 + index,
          tsMs: index,
        }),
      );
    }

    upsertLiveEvent(
      eventsMap,
      finalizedIds,
      makePartialUpdate({
        eventId: 'inserted-last-but-stale',
        lastUpdateMs: 1,
        tsMs: 1_000_000,
      }),
    );

    expect(eventsMap.size).toBe(MAX_LIVE_PARTIAL_EVENTS);
    expect(eventsMap.has('partial-000')).toBe(true);
    expect(eventsMap.has('inserted-last-but-stale')).toBe(false);
  });

  it('uses timestamp fallbacks and event identity to break equal freshness ties', () => {
    const eventsMap: LiveEventMap = new Map();
    const finalizedIds = new Set<string>();

    for (let index = 0; index < MAX_LIVE_PARTIAL_EVENTS; index += 1) {
      upsertLiveEvent(
        eventsMap,
        finalizedIds,
        makePartialUpdate({
          eventId: `partial-${index.toString().padStart(3, '0')}`,
          tsMs: 1000,
        }),
      );
    }

    upsertLiveEvent(
      eventsMap,
      finalizedIds,
      makePartialUpdate({ eventId: 'aaa', tsMs: 1000 }),
    );
    expect(eventsMap.has('aaa')).toBe(false);

    upsertLiveEvent(
      eventsMap,
      finalizedIds,
      makePartialUpdate({ eventId: 'zzz', tsMs: 1000 }),
    );
    expect(eventsMap.has('partial-000')).toBe(false);
    expect(eventsMap.has('zzz')).toBe(true);
  });

  it('allows a fresh partial to re-enter after a stale version is evicted', () => {
    const eventsMap: LiveEventMap = new Map();
    const finalizedIds = new Set<string>();

    for (let index = 0; index < MAX_LIVE_PARTIAL_EVENTS; index += 1) {
      upsertLiveEvent(
        eventsMap,
        finalizedIds,
        makePartialUpdate({
          eventId: `partial-${index}`,
          lastUpdateMs: 10_000 + index,
        }),
      );
    }

    upsertLiveEvent(
      eventsMap,
      finalizedIds,
      makePartialUpdate({ eventId: 'evicted', lastUpdateMs: 1 }),
    );
    expect(eventsMap.has('evicted')).toBe(false);

    upsertLiveEvent(
      eventsMap,
      finalizedIds,
      makePartialUpdate({ eventId: 'evicted', lastUpdateMs: 20_000 }),
    );
    expect(eventsMap.has('evicted')).toBe(true);
    expect(eventsMap.size).toBe(MAX_LIVE_PARTIAL_EVENTS);
  });

  it('admits a final after its partial was evicted', () => {
    const eventsMap: LiveEventMap = new Map();
    const finalizedIds = new Set<string>();

    for (let index = 0; index < MAX_LIVE_PARTIAL_EVENTS; index += 1) {
      upsertLiveEvent(
        eventsMap,
        finalizedIds,
        makePartialUpdate({
          eventId: `partial-${index}`,
          lastUpdateMs: 10_000 + index,
        }),
      );
    }

    upsertLiveEvent(
      eventsMap,
      finalizedIds,
      makePartialUpdate({ eventId: 'evicted', lastUpdateMs: 1 }),
    );
    expect(eventsMap.has('evicted')).toBe(false);

    upsertLiveEvent(eventsMap, finalizedIds, makeFinalUpdate('evicted'));
    expect(eventsMap.get('evicted')?.phase).toBe('final');
  });
});
