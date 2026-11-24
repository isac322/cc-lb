import { describe, expect, it } from 'vitest';
import {
  formatDatePart,
  mergeDateIntoField,
  parseDatePart,
} from './calendarDate';

describe('parseDatePart', () => {
  it('parses a full "YYYY-MM-DD HH:mm" string into a local date', () => {
    const d = parseDatePart('2026-07-15 08:30');
    expect(d).toBeInstanceOf(Date);
    expect(d?.getFullYear()).toBe(2026);
    expect(d?.getMonth()).toBe(6); // July = 6
    expect(d?.getDate()).toBe(15);
  });

  it('parses a date-only string', () => {
    const d = parseDatePart('2026-01-05');
    expect(d?.getFullYear()).toBe(2026);
    expect(d?.getMonth()).toBe(0);
    expect(d?.getDate()).toBe(5);
  });

  it('returns undefined for empty or malformed input', () => {
    expect(parseDatePart('')).toBeUndefined();
    expect(parseDatePart('not-a-date')).toBeUndefined();
    expect(parseDatePart('2026-7-5')).toBeUndefined();
  });

  it('rejects overflow dates like Feb 30', () => {
    expect(parseDatePart('2026-02-30 00:00')).toBeUndefined();
    expect(parseDatePart('2026-13-01')).toBeUndefined();
  });
});

describe('formatDatePart', () => {
  it('formats a Date to zero-padded YYYY-MM-DD using local components', () => {
    expect(formatDatePart(new Date(2026, 6, 15, 12, 0, 0))).toBe('2026-07-15');
    expect(formatDatePart(new Date(2026, 0, 5, 12, 0, 0))).toBe('2026-01-05');
  });
});

describe('mergeDateIntoField', () => {
  it('defaults to 00:00 for an empty since field', () => {
    expect(mergeDateIntoField('', '2026-07-15', 'since')).toBe(
      '2026-07-15 00:00',
    );
  });

  it('defaults to 23:59 for an empty until field', () => {
    expect(mergeDateIntoField('', '2026-07-15', 'until')).toBe(
      '2026-07-15 23:59',
    );
  });

  it('preserves an existing typed time when only the date changes', () => {
    expect(mergeDateIntoField('2026-01-01 08:30', '2026-07-15', 'since')).toBe(
      '2026-07-15 08:30',
    );
    expect(mergeDateIntoField('2026-01-01 22:05', '2026-07-15', 'until')).toBe(
      '2026-07-15 22:05',
    );
  });
});
