import { describe, expect, it } from 'vitest';
import {
  formatInTimezone,
  MAX_FORMATTABLE_UNIX_SECONDS,
  parseInTimezone,
} from './timezone';

describe('timezone', () => {
  it('parses standard time', () => {
    const res = parseInTimezone('2023-10-25T14:30', 'America/New_York');
    expect(res.error).toBeUndefined();
    expect(res.ts).toBe(new Date('2023-10-25T18:30:00Z').getTime());
  });

  it('detects nonexistent time (spring forward)', () => {
    // US Eastern spring forward: 2024-03-10 02:00 to 03:00 doesn't exist
    const res = parseInTimezone('2024-03-10T02:30', 'America/New_York');
    expect(res.error).toBe('nonexistent');
    expect(res.ts).toBeNull();
  });

  it('detects ambiguous time (fall back)', () => {
    // US Eastern fall back: 2024-11-03 01:00 to 02:00 happens twice
    const res = parseInTimezone('2024-11-03T01:30', 'America/New_York');
    expect(res.error).toBe('ambiguous');
    expect(res.ts).toBeNull();
  });

  it('formats time correctly', () => {
    const ts = new Date('2023-10-25T18:30:00Z').getTime();
    expect(formatInTimezone(ts, 'America/New_York')).toBe('2023-10-25T14:30');
  });

  it('keeps the maximum supported minute within the four-digit year contract', () => {
    expect(
      formatInTimezone(
        MAX_FORMATTABLE_UNIX_SECONDS * 1000,
        'Pacific/Kiritimati',
      ),
    ).toBe('9999-12-31T09:59');

    const parsed = parseInTimezone('9999-12-31T09:59', 'Pacific/Kiritimati');
    expect(parsed.ts).toBe((MAX_FORMATTABLE_UNIX_SECONDS - 59) * 1000);
  });
});
