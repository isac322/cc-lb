import { describe, expect, it } from 'vitest';
import { MAX_FORMATTABLE_UNIX_SECONDS } from '../../lib/timezone';
import { parseBound } from './DateTimeField';

// These cases used to be exercised through the retired preset selector. The
// typed-bounds path in TimeRangeBounds still depends on every one of them: an
// unparseable draft must report why and yield no timestamp, so the caller can
// keep the previous selection instead of committing NaN.
describe('parseBound', () => {
  const TZ = 'America/New_York';

  it('rejects an ambiguous wall time in the DST fall-back hour', () => {
    expect(parseBound('2024-11-03 01:30', '', TZ, 'since')).toEqual({
      err: 'Ambiguous time (DST)',
    });
  });

  it('rejects a wall time that the DST spring-forward gap skips', () => {
    expect(parseBound('2024-03-10 02:30', '', TZ, 'since')).toEqual({
      err: 'Time does not exist (DST)',
    });
  });

  it('separates a calendar-invalid date from a DST gap', () => {
    expect(parseBound('2024-02-30 12:00', '', TZ, 'since')).toEqual({
      err: 'Invalid time',
    });
  });

  it('rejects a malformed draft without producing a timestamp', () => {
    for (const draft of ['2026-99-99 10:00', 'not a time', '2026-07-31']) {
      const parsed = parseBound(draft, '', TZ, 'since');
      expect(parsed.err).toBe('Invalid time');
      expect(parsed.ts).toBeUndefined();
    }
  });

  it('accepts a valid wall time on either bound', () => {
    const since = parseBound('2024-11-03 03:30', '', TZ, 'since');
    expect(since.err).toBeUndefined();
    expect(since.ts).toBe(1730622600);
    // An upper bound covers the whole minute it names.
    expect(parseBound('2024-11-03 03:30', '', TZ, 'until').ts).toBe(1730622659);
  });

  it('treats an empty draft as required only when the other bound is set', () => {
    expect(parseBound('', '2024-11-03 04:30', TZ, 'since')).toEqual({
      err: 'Required',
    });
    expect(parseBound('', '', TZ, 'since')).toEqual({ err: undefined });
  });

  it('clamps the furthest formattable upper bound', () => {
    expect(
      parseBound(
        '9999-12-31 09:59',
        '9999-12-31 09:59',
        'Pacific/Kiritimati',
        'until',
      ).ts,
    ).toBe(MAX_FORMATTABLE_UNIX_SECONDS);
  });
});
