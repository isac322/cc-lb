import { describe, expect, it } from 'vitest';
import type { RequestEvent, RequestEventPartial } from './api';
import {
  filterLiveEventsByUnixSeconds,
  filterLogRowsByStatusClass,
  mergeLogRows,
} from './logRows';
import type { LiveEventMap } from './upsertReducer';

function makeFinal(
  eventId: string,
  requestId: string,
  tsMs: number,
): RequestEvent {
  return {
    event_id: eventId,
    request_id: requestId,
    ts: tsMs / 1000,
    ts_ms: tsMs,
    status: 200,
    duration_ms: 10,
  };
}

function makePartial(
  eventId: string,
  requestId: string,
  tsMs: number,
): RequestEventPartial {
  return {
    event_id: eventId,
    request_id: requestId,
    ts: tsMs / 1000,
    ts_ms: tsMs,
  };
}

describe('mergeLogRows', () => {
  it('deduplicates by event identity without collapsing retries sharing a request id', () => {
    const live: LiveEventMap = new Map([
      [
        'live-event',
        {
          phase: 'partial',
          event: makePartial('live-event', 'shared-request', 100),
        },
      ],
      [
        'shared-event',
        {
          phase: 'final',
          event: makeFinal('shared-event', 'live-request', 200),
        },
      ],
    ]);
    const historical = [
      makeFinal('historical-event', 'shared-request', 400),
      makeFinal('shared-event', 'historical-request', 300),
    ];

    const rows = mergeLogRows(live, historical);

    expect(rows).toHaveLength(3);
    expect(rows.map(({ event_id }) => event_id)).toEqual([
      'historical-event',
      'shared-event',
      'live-event',
    ]);
    expect(rows.map(({ _phase }) => _phase)).toEqual([
      'final',
      'final',
      'partial',
    ]);
  });

  it('reserves partials and fills the remaining 500-row budget with newest finals', () => {
    const live: LiveEventMap = new Map();
    for (let index = 0; index < 100; index += 1) {
      const eventId = `partial-${index}`;
      live.set(eventId, {
        phase: 'partial',
        event: makePartial(eventId, `partial-request-${index}`, index),
      });
    }
    const historical = Array.from({ length: 600 }, (_, index) =>
      makeFinal(`final-${index}`, `final-request-${index}`, 1000 + index),
    );

    const rows = mergeLogRows(live, historical);

    expect(rows).toHaveLength(500);
    expect(rows.filter(({ _phase }) => _phase === 'partial')).toHaveLength(100);
    expect(rows.filter(({ _phase }) => _phase === 'final')).toHaveLength(400);
    expect(rows[0]?.event_id).toBe('final-599');
    expect(rows.at(-1)?.event_id).toBe('partial-0');
  });

  it('caps an over-limit partial set at the newest 500 rows and omits finals', () => {
    const live: LiveEventMap = new Map();
    for (let index = 0; index < 503; index += 1) {
      const eventId = `partial-${index}`;
      live.set(eventId, {
        phase: 'partial',
        event: makePartial(eventId, `partial-request-${index}`, index),
      });
    }

    const rows = mergeLogRows(live, [
      makeFinal('final', 'final-request', 1000),
    ]);

    expect(rows).toHaveLength(500);
    expect(rows.every(({ _phase }) => _phase === 'partial')).toBe(true);
    expect(rows[0]?.event_id).toBe('partial-502');
    expect(rows.at(-1)?.event_id).toBe('partial-3');
  });

  it('preserves final row identity across merges for unchanged source events', () => {
    const historical = [
      makeFinal('a', 'req-a', 200),
      makeFinal('b', 'req-b', 100),
    ];

    const first = mergeLogRows(new Map(), historical);
    const second = mergeLogRows(new Map(), historical);

    expect(second[0]).toBe(first[0]);
    expect(second[1]).toBe(first[1]);
  });

  it('preserves live final row identity across version bumps', () => {
    const event = makeFinal('live', 'req-live', 300);
    const live: LiveEventMap = new Map([['live', { phase: 'final', event }]]);

    const first = mergeLogRows(live, []);
    const second = mergeLogRows(live, []);

    expect(second[0]).toBe(first[0]);
  });
});

describe('filterLiveEventsByUnixSeconds', () => {
  it('keeps inclusive preset and custom bounds for retained live rows', () => {
    const live: LiveEventMap = new Map();
    for (const seconds of [99, 100, 150, 200, 201]) {
      const eventId = `event-${seconds}`;
      const requestId = `request-${seconds}`;
      if (seconds === 150) {
        live.set(eventId, {
          phase: 'partial',
          event: makePartial(eventId, requestId, seconds * 1000),
        });
      } else {
        live.set(eventId, {
          phase: 'final',
          event: makeFinal(eventId, requestId, seconds * 1000),
        });
      }
    }

    const filtered = filterLiveEventsByUnixSeconds(live, {
      since: 100,
      until: 200,
    });

    expect([...filtered.keys()]).toEqual([
      'event-100',
      'event-150',
      'event-200',
    ]);
  });

  it('returns the original live map when no bounds are active', () => {
    const live: LiveEventMap = new Map();

    expect(filterLiveEventsByUnixSeconds(live, {})).toBe(live);
  });
});

describe('filterLogRowsByStatusClass', () => {
  it('filters final and partial rows by HTTP status class', () => {
    const live: LiveEventMap = new Map([
      [
        'partial-429',
        {
          phase: 'partial',
          event: {
            ...makePartial('partial-429', 'partial-request', 400),
            upstream_response_status: 429,
          },
        },
      ],
    ]);
    const historical = [
      makeFinal('final-200', 'request-200', 300),
      { ...makeFinal('final-404', 'request-404', 200), status: 404 },
      { ...makeFinal('final-503', 'request-503', 100), status: 503 },
    ];
    const rows = mergeLogRows(live, historical);

    const filtered = filterLogRowsByStatusClass(rows, '4xx');

    expect(filtered.map(({ event_id }) => event_id)).toEqual([
      'partial-429',
      'final-404',
    ]);
  });

  it('returns the original rows when no status class is active', () => {
    const rows = mergeLogRows(new Map(), [makeFinal('final', 'request', 100)]);

    expect(filterLogRowsByStatusClass(rows)).toBe(rows);
  });
});
