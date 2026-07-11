import type {
  RequestEvent,
  RequestEventPartial,
  RequestEventUpdate,
} from './api';

export type LiveEventMap = Map<
  string,
  | { phase: 'partial'; event: RequestEventPartial }
  | { phase: 'final'; event: RequestEvent }
>;

export function upsertLiveEvent(
  eventsMap: LiveEventMap,
  finalizedIds: Set<string>,
  update: RequestEventUpdate,
): boolean {
  const eventId =
    update.phase === 'final'
      ? (update.payload.event.event_id ?? update.payload.event.request_id)
      : (update.payload.event_id ?? update.payload.request_id);

  if (!eventId) return false;

  if (update.phase === 'partial') {
    // Rule 1: incoming.phase == 'partial' && finalizedIds.has(event_id) -> DROP
    if (finalizedIds.has(eventId)) {
      return false;
    }

    const existing = eventsMap.get(eventId);
    // Rule 3: incoming.phase == 'partial' && !existing -> INSERT
    // Rule 4: incoming.phase == 'partial' && existing.phase == 'partial' -> REPLACE entire
    if (!existing || existing.phase === 'partial') {
      eventsMap.set(eventId, {
        phase: 'partial',
        event: update.payload,
      });
      return true;
    }

    // If existing is final, we drop the partial (should be covered by finalizedIds, but just in case)
    return false;
  } else {
    // Rule 2: incoming.phase == 'final' -> REPLACE entire, add to finalizedIds
    eventsMap.set(eventId, {
      phase: 'final',
      event: update.payload.event,
    });
    finalizedIds.add(eventId);
    return true;
  }
}
