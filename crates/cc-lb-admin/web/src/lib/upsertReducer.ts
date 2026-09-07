import type {
  RequestEvent,
  RequestEventPartial,
  RequestEventUpdate,
} from './api';

export const MAX_LIVE_FINAL_EVENTS = 500;
export const MAX_LIVE_PARTIAL_EVENTS = 500;
export const MAX_EVICTED_FINAL_TOMBSTONES = 500;

export type LiveEventEntry =
  | { readonly phase: 'partial'; readonly event: RequestEventPartial }
  | { readonly phase: 'final'; readonly event: RequestEvent };

export type LiveEventMap = Map<string, LiveEventEntry>;

function eventTimestamp(event: RequestEvent): number {
  if (event.ts_ms != null) return event.ts_ms;
  if (event.ts != null) return event.ts * 1000;
  return Number.NEGATIVE_INFINITY;
}

function partialFreshness(event: RequestEventPartial): number {
  if (event.last_update_ms != null) return event.last_update_ms;
  if (event.ts_ms != null) return event.ts_ms;
  if (event.ts != null) return event.ts * 1000;
  return Number.NEGATIVE_INFINITY;
}

function evictOldestPartial(eventsMap: LiveEventMap): void {
  let partialCount = 0;
  let oldestId: string | undefined;
  let oldestFreshness = Number.POSITIVE_INFINITY;

  for (const [eventId, entry] of eventsMap) {
    if (entry.phase !== 'partial') continue;
    partialCount += 1;
    const freshness = partialFreshness(entry.event);
    if (
      freshness < oldestFreshness ||
      (freshness === oldestFreshness &&
        (oldestId === undefined || eventId < oldestId))
    ) {
      oldestId = eventId;
      oldestFreshness = freshness;
    }
  }

  if (partialCount <= MAX_LIVE_PARTIAL_EVENTS || oldestId === undefined) {
    return;
  }
  eventsMap.delete(oldestId);
}

function rememberEvictedFinal(tombstones: Set<string>, eventId: string): void {
  tombstones.delete(eventId);
  tombstones.add(eventId);
  while (tombstones.size > MAX_EVICTED_FINAL_TOMBSTONES) {
    const oldest = tombstones.values().next();
    if (oldest.done) return;
    tombstones.delete(oldest.value);
  }
}

function evictOldestFinal(
  eventsMap: LiveEventMap,
  finalizedIds: Set<string>,
  tombstones: Set<string>,
): void {
  let finalCount = 0;
  let oldestId: string | undefined;
  let oldestTimestamp = Number.POSITIVE_INFINITY;

  for (const [eventId, entry] of eventsMap) {
    if (entry.phase !== 'final') continue;
    finalCount += 1;
    const timestamp = eventTimestamp(entry.event);
    if (
      timestamp < oldestTimestamp ||
      (timestamp === oldestTimestamp &&
        (oldestId === undefined || eventId < oldestId))
    ) {
      oldestId = eventId;
      oldestTimestamp = timestamp;
    }
  }

  if (finalCount <= MAX_LIVE_FINAL_EVENTS || oldestId === undefined) return;
  eventsMap.delete(oldestId);
  finalizedIds.delete(oldestId);
  rememberEvictedFinal(tombstones, oldestId);
}

export function upsertLiveEvent(
  eventsMap: LiveEventMap,
  finalizedIds: Set<string>,
  update: RequestEventUpdate,
  tombstones: Set<string> = new Set(),
): boolean {
  const eventId =
    update.phase === 'final'
      ? (update.payload.event.event_id ?? update.payload.event.request_id)
      : (update.payload.event_id ?? update.payload.request_id);

  if (!eventId) return false;

  if (update.phase === 'partial') {
    if (finalizedIds.has(eventId) || tombstones.has(eventId)) return false;

    const existing = eventsMap.get(eventId);
    if (!existing || existing.phase === 'partial') {
      eventsMap.set(eventId, { phase: 'partial', event: update.payload });
      if (eventsMap.size - finalizedIds.size > MAX_LIVE_PARTIAL_EVENTS) {
        evictOldestPartial(eventsMap);
      }
      return true;
    }
    return false;
  }

  const event: RequestEvent = update.payload.event;
  tombstones.delete(eventId);
  eventsMap.set(eventId, { phase: 'final', event });
  finalizedIds.add(eventId);
  if (finalizedIds.size > MAX_LIVE_FINAL_EVENTS) {
    evictOldestFinal(eventsMap, finalizedIds, tombstones);
  }
  return true;
}
