/** Pure date helpers for the request-log Custom time-range calendar helper. */

const DATE_RE = /^(\d{4})-(\d{2})-(\d{2})/;
const TIME_RE = /(\d{2}:\d{2})\s*$/;

/**
 * Parse the leading `YYYY-MM-DD` of a field string into a local noon Date
 * (noon avoids DST midnight edges). Returns undefined for empty, malformed,
 * or overflow dates (e.g. Feb 30).
 */
export function parseDatePart(value: string): Date | undefined {
  const m = DATE_RE.exec(value);
  if (m === null) return undefined;
  const year = Number(m[1]);
  const month = Number(m[2]);
  const day = Number(m[3]);
  if (month < 1 || month > 12 || day < 1 || day > 31) return undefined;
  const date = new Date(year, month - 1, day, 12, 0, 0, 0);
  if (
    date.getFullYear() !== year ||
    date.getMonth() !== month - 1 ||
    date.getDate() !== day
  ) {
    return undefined;
  }
  return date;
}

/** Format a Date to zero-padded `YYYY-MM-DD` using its local calendar components. */
export function formatDatePart(date: Date): string {
  const y = String(date.getFullYear()).padStart(4, '0');
  const m = String(date.getMonth() + 1).padStart(2, '0');
  const d = String(date.getDate()).padStart(2, '0');
  return `${y}-${m}-${d}`;
}

/**
 * Write a picked `YYYY-MM-DD` into a field string, preserving any already-typed
 * `HH:mm`. Empty fields default to the start (00:00) or end (23:59) of day so a
 * single click yields a complete, valid bound.
 */
export function mergeDateIntoField(
  current: string,
  isoDate: string,
  field: 'since' | 'until',
): string {
  const timeMatch = TIME_RE.exec(current);
  const time =
    timeMatch !== null ? timeMatch[1] : field === 'since' ? '00:00' : '23:59';
  return `${isoDate} ${time}`;
}
