import { describe, expect, it } from 'vitest';
import type { RequestEvent, RequestEventUpdate } from './api';
import { type LiveEventMap, upsertLiveEvent } from './upsertReducer';

describe('upsertReducer', () => {
  it('Rule 1: incoming.phase == partial && finalizedIds.has(event_id) -> DROP', () => {
    const eventsMap: LiveEventMap = new Map();
    const finalizedIds = new Set(['evt-1']);
    const update: RequestEventUpdate = {
      phase: 'partial',
      payload: { event_id: 'evt-1', request_id: 'req-1', ts: 1, ts_ms: 1000 },
    } as unknown as RequestEventUpdate;

    const changed = upsertLiveEvent(eventsMap, finalizedIds, update);
    expect(changed).toBe(false);
    expect(eventsMap.size).toBe(0);
  });

  it('Rule 2: incoming.phase == final -> REPLACE entire, add to finalizedIds', () => {
    const eventsMap: LiveEventMap = new Map();
    const finalizedIds = new Set<string>();
    const update: RequestEventUpdate = {
      phase: 'final',
      payload: {
        event: {
          event_id: 'evt-2',
          request_id: 'req-2',
          status: 200,
          duration_ms: 100,
        },
        cursor: 1,
      },
    } as unknown as RequestEventUpdate;

    const changed = upsertLiveEvent(eventsMap, finalizedIds, update);
    expect(changed).toBe(true);
    expect(eventsMap.get('evt-2')).toEqual({
      phase: 'final',
      event: update.payload.event,
    });
    expect(finalizedIds.has('evt-2')).toBe(true);
  });

  it('Rule 3: incoming.phase == partial && !existing -> INSERT', () => {
    const eventsMap: LiveEventMap = new Map();
    const finalizedIds = new Set<string>();
    const update: RequestEventUpdate = {
      phase: 'partial',
      payload: { event_id: 'evt-3', request_id: 'req-3', ts: 1, ts_ms: 1000 },
    } as unknown as RequestEventUpdate;

    const changed = upsertLiveEvent(eventsMap, finalizedIds, update);
    expect(changed).toBe(true);
    expect(eventsMap.get('evt-3')).toEqual({
      phase: 'partial',
      event: update.payload,
    });
  });

  it('Rule 4: incoming.phase == partial && existing.phase == partial -> REPLACE entire', () => {
    const eventsMap: LiveEventMap = new Map();
    eventsMap.set('evt-4', {
      phase: 'partial',
      event: {
        event_id: 'evt-4',
        request_id: 'req-4',
        ts: 1,
        ts_ms: 1000,
      } as unknown as RequestEvent,
    });
    const finalizedIds = new Set<string>();
    const update: RequestEventUpdate = {
      phase: 'partial',
      payload: {
        event_id: 'evt-4',
        request_id: 'req-4',
        ts: 2,
        ts_ms: 2000,
        input_tokens: 10,
      },
    } as unknown as RequestEventUpdate;

    const changed = upsertLiveEvent(eventsMap, finalizedIds, update);
    expect(changed).toBe(true);
    expect(eventsMap.get('evt-4')).toEqual({
      phase: 'partial',
      event: update.payload,
    });
  });
});
