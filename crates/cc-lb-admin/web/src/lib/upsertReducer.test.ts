import { describe, expect, it } from 'vitest';
import type { RequestEvent, RequestEventUpdate } from './api';
import { type LiveEventMap, upsertLiveEvent } from './upsertReducer';

function makeFinalUpdate(eventId: string, tsMs: number): RequestEventUpdate {
  return {
    phase: 'final',
    payload: {
      event: {
        event_id: eventId,
        request_id: `request-${eventId}`,
        ts: tsMs / 1000,
        ts_ms: tsMs,
        status: 200,
        duration_ms: 10,
      } satisfies RequestEvent,
      cursor: tsMs,
    },
  };
}

function makePartialUpdate(
  eventId: string,
  tsMs: number,
  inputTokens?: number,
): RequestEventUpdate {
  return {
    phase: 'partial',
    payload: {
      event_id: eventId,
      request_id: `request-${eventId}`,
      ts: tsMs / 1000,
      ts_ms: tsMs,
      last_update_ms: tsMs,
      elapsed_ms: 0,
      stream: false,
      input_tokens: inputTokens,
    },
  };
}

describe('upsertReducer', () => {
  it('Rule 1: incoming.phase == partial && finalizedIds.has(event_id) -> DROP', () => {
    const eventsMap: LiveEventMap = new Map();
    const finalizedIds = new Set(['evt-1']);

    const changed = upsertLiveEvent(
      eventsMap,
      finalizedIds,
      makePartialUpdate('evt-1', 1000),
    );

    expect(changed).toBe(false);
    expect(eventsMap.size).toBe(0);
  });

  it('Rule 2: incoming.phase == final -> REPLACE entire, add to finalizedIds', () => {
    const partial = makePartialUpdate('evt-2', 500);
    if (partial.phase !== 'partial')
      throw new Error('Expected partial fixture');
    const eventsMap: LiveEventMap = new Map([
      ['evt-2', { phase: 'partial', event: partial.payload }],
    ]);
    const finalizedIds = new Set<string>();
    const update = makeFinalUpdate('evt-2', 1000);

    const changed = upsertLiveEvent(eventsMap, finalizedIds, update);

    expect(changed).toBe(true);
    if (update.phase !== 'final') throw new Error('Expected final fixture');
    expect(eventsMap.get('evt-2')).toEqual({
      phase: 'final',
      event: update.payload.event,
    });
    expect(finalizedIds.has('evt-2')).toBe(true);
  });

  it('Rule 3: incoming.phase == partial && !existing -> INSERT', () => {
    const eventsMap: LiveEventMap = new Map();
    const finalizedIds = new Set<string>();
    const update = makePartialUpdate('evt-3', 1000);

    const changed = upsertLiveEvent(eventsMap, finalizedIds, update);

    expect(changed).toBe(true);
    if (update.phase !== 'partial') throw new Error('Expected partial fixture');
    expect(eventsMap.get('evt-3')).toEqual({
      phase: 'partial',
      event: update.payload,
    });
  });

  it('Rule 4: incoming.phase == partial && existing.phase == partial -> REPLACE entire', () => {
    const initialUpdate = makePartialUpdate('evt-4', 1000);
    if (initialUpdate.phase !== 'partial') {
      throw new Error('Expected partial fixture');
    }
    const eventsMap: LiveEventMap = new Map([
      [
        'evt-4',
        {
          phase: 'partial',
          event: initialUpdate.payload,
        },
      ],
    ]);
    const finalizedIds = new Set<string>();
    const update = makePartialUpdate('evt-4', 2000, 10);
    if (update.phase !== 'partial') throw new Error('Expected partial fixture');

    const changed = upsertLiveEvent(eventsMap, finalizedIds, update);

    expect(changed).toBe(true);
    expect(eventsMap.get('evt-4')).toEqual({
      phase: 'partial',
      event: update.payload,
    });
  });

  it('bounds finalized entries and tombstones while preserving active partials', () => {
    const eventsMap: LiveEventMap = new Map();
    const finalizedIds = new Set<string>();
    const tombstones = new Set<string>();
    for (let index = 0; index < 3; index += 1) {
      upsertLiveEvent(
        eventsMap,
        finalizedIds,
        makePartialUpdate(`partial-${index}`, index),
        tombstones,
      );
    }

    for (let index = 0; index < 1001; index += 1) {
      upsertLiveEvent(
        eventsMap,
        finalizedIds,
        makeFinalUpdate(`final-${index}`, index),
        tombstones,
      );
    }

    expect(finalizedIds.size).toBe(500);
    expect(tombstones.size).toBe(500);
    expect(
      [...eventsMap.values()].filter(({ phase }) => phase === 'final'),
    ).toHaveLength(500);
    expect(eventsMap.has('partial-0')).toBe(true);
    expect(eventsMap.has('partial-1')).toBe(true);
    expect(eventsMap.has('partial-2')).toBe(true);
    expect(tombstones.has('final-0')).toBe(false);
    expect(tombstones.has('final-1')).toBe(true);
    expect(tombstones.has('final-500')).toBe(true);
  });

  it('retains the 500 freshest partials with deterministic event-id ties', () => {
    const eventsMap: LiveEventMap = new Map();
    const finalizedIds = new Set<string>();
    for (let index = 0; index < 500; index += 1) {
      upsertLiveEvent(
        eventsMap,
        finalizedIds,
        makePartialUpdate(`partial-${index}`, 1000 + index),
      );
    }

    upsertLiveEvent(
      eventsMap,
      finalizedIds,
      makePartialUpdate('partial-stale', 1),
    );
    upsertLiveEvent(
      eventsMap,
      finalizedIds,
      makePartialUpdate('partial-fresh', 2000),
    );
    upsertLiveEvent(
      eventsMap,
      finalizedIds,
      makePartialUpdate('a-equal-oldest', 1001),
    );

    expect(eventsMap.size).toBe(500);
    expect(eventsMap.has('partial-stale')).toBe(false);
    expect(eventsMap.has('partial-0')).toBe(false);
    expect(eventsMap.has('a-equal-oldest')).toBe(false);
    expect(eventsMap.has('partial-fresh')).toBe(true);
  });

  it('evicts the final with the oldest event timestamp rather than insertion order', () => {
    const eventsMap: LiveEventMap = new Map();
    const finalizedIds = new Set<string>();
    const tombstones = new Set<string>();
    upsertLiveEvent(
      eventsMap,
      finalizedIds,
      makeFinalUpdate('inserted-first', 10_000),
      tombstones,
    );
    for (let index = 1; index < 500; index += 1) {
      upsertLiveEvent(
        eventsMap,
        finalizedIds,
        makeFinalUpdate(`newer-${index}`, 10_000 + index),
        tombstones,
      );
    }

    upsertLiveEvent(
      eventsMap,
      finalizedIds,
      makeFinalUpdate('inserted-last-but-oldest', 1),
      tombstones,
    );

    expect(eventsMap.has('inserted-first')).toBe(true);
    expect(eventsMap.has('inserted-last-but-oldest')).toBe(false);
    expect(tombstones.has('inserted-last-but-oldest')).toBe(true);

    upsertLiveEvent(
      eventsMap,
      finalizedIds,
      makeFinalUpdate('a-equal-oldest', 10_000),
      tombstones,
    );

    expect(eventsMap.has('a-equal-oldest')).toBe(false);
    expect(eventsMap.has('inserted-first')).toBe(true);
    expect(tombstones.has('a-equal-oldest')).toBe(true);
  });

  it('blocks late partial resurrection until a later final clears the tombstone', () => {
    const eventsMap: LiveEventMap = new Map();
    const finalizedIds = new Set<string>();
    const tombstones = new Set<string>();
    for (let index = 0; index < 500; index += 1) {
      upsertLiveEvent(
        eventsMap,
        finalizedIds,
        makeFinalUpdate(`final-${index}`, index),
        tombstones,
      );
    }
    upsertLiveEvent(
      eventsMap,
      finalizedIds,
      makeFinalUpdate('new-final', 500),
      tombstones,
    );

    const partialChanged = upsertLiveEvent(
      eventsMap,
      finalizedIds,
      makePartialUpdate('final-0', 501),
      tombstones,
    );
    const finalChanged = upsertLiveEvent(
      eventsMap,
      finalizedIds,
      makeFinalUpdate('final-0', 502),
      tombstones,
    );

    expect(partialChanged).toBe(false);
    expect(finalChanged).toBe(true);
    expect(eventsMap.get('final-0')?.phase).toBe('final');
    expect(tombstones.has('final-0')).toBe(false);
  });
});
