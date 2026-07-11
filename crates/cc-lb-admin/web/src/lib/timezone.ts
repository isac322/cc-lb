// Latest Unix second that remains a four-digit year in UTC+14.
export const MAX_FORMATTABLE_UNIX_SECONDS = 253_402_199_999;

export function formatInTimezone(ts: number, timeZone: string): string {
  const fmt = new Intl.DateTimeFormat('en-US', {
    timeZone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  });
  const parts = fmt.formatToParts(new Date(ts));
  const p: Record<string, string> = {};
  for (const part of parts) p[part.type] = part.value;
  const hour = p.hour === '24' ? '00' : p.hour;
  return `${p.year}-${p.month}-${p.day}T${hour}:${p.minute}`;
}

export function parseInTimezone(
  localString: string,
  timeZone: string,
): { ts: number | null; error?: 'nonexistent' | 'ambiguous' | 'invalid' } {
  const t0 = new Date(`${localString}Z`).getTime();
  if (Number.isNaN(t0)) return { ts: null, error: 'invalid' };

  const local0Str = formatInTimezone(t0, timeZone);
  const local0 = new Date(`${local0Str}Z`).getTime();
  const offset0 = local0 - t0;

  const guess1 = t0 - offset0;

  const matches = new Set<number>();

  for (let h = -2; h <= 2; h++) {
    for (let m = 0; m < 60; m += 15) {
      const testTs = guess1 + h * 3600000 + m * 60000;
      if (formatInTimezone(testTs, timeZone) === localString) {
        matches.add(testTs);
      }
    }
  }

  if (matches.size === 0) {
    return { ts: null, error: 'nonexistent' };
  }
  if (matches.size > 1) {
    return { ts: null, error: 'ambiguous' };
  }
  return { ts: Array.from(matches)[0], error: undefined };
}
