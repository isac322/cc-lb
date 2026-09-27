/**
 * Quota severity is the only thing a quota figure's color communicates:
 * usage below 80% renders in the default ink, 80% and up is a warning, 95%
 * and up is danger. Window identity (5h / 7d / 7d_fable) belongs to legends
 * and chart series, never to the number or bar itself.
 */
export type QuotaSeverity = 'none' | 'ok' | 'warn' | 'danger';

export const QUOTA_WARN_PCT = 80;
export const QUOTA_DANGER_PCT = 95;

/** `pct` is 0-100; `null` or a non-finite value means no reading. */
export function quotaSeverity(pct: number | null): QuotaSeverity {
  if (pct === null || !Number.isFinite(pct)) return 'none';
  if (pct >= QUOTA_DANGER_PCT) return 'danger';
  if (pct >= QUOTA_WARN_PCT) return 'warn';
  return 'ok';
}

/**
 * The one display rule for a quota utilization figure (0-100): whole
 * percent everywhere, lists, cards and chart tooltips alike. A reading that
 * is not yet exhausted never rounds up to `100%`, and a non-zero reading
 * never rounds down to `0%`.
 */
export function formatQuotaPercent(pct: number | null | undefined): string {
  if (pct == null || !Number.isFinite(pct)) return '—';
  if (pct > 0 && pct < 1) return '<1%';
  if (pct >= 99 && pct < 100) return '99%';
  return `${Math.round(pct)}%`;
}

/**
 * Headroom is the instrument: how much of a window is LEFT. `usedPct` is the
 * utilization (0-100); the result is `100 - used` clamped to 0-100, or
 * `null` for no reading. Severity is still judged on used (`quotaSeverity`).
 */
export function headroomPct(usedPct: number | null | undefined): number | null {
  if (usedPct == null || !Number.isFinite(usedPct)) return null;
  return Math.min(100, Math.max(0, 100 - usedPct));
}

/**
 * The one display rule for a headroom figure: `N% left`, whole percent.
 * Mirrors `formatQuotaPercent` on the headroom side: it reads `0% left`
 * only once the window is exhausted (used ≥ 100), `<1% left` for any
 * sliver below one percent, and never `100% left` once anything is used.
 * Used, when shown at all, is secondary text via `formatQuotaPercent`.
 */
export function formatHeadroom(usedPct: number | null | undefined): string {
  const left = headroomPct(usedPct);
  if (left === null) return '—';
  return `${formatHeadroomNumber(left)}% left`;
}

/**
 * The bare number of `formatHeadroom` (`'59'`, `'<1'`, `'—'`) for layouts
 * that set the unit apart, like a gauge numeral with a small `% left`.
 */
export function formatHeadroomValue(
  usedPct: number | null | undefined,
): string {
  const left = headroomPct(usedPct);
  return left === null ? '—' : formatHeadroomNumber(left);
}

function formatHeadroomNumber(left: number): string {
  if (left <= 0) return '0';
  if (left < 1) return '<1';
  if (left > 99 && left < 100) return '99';
  return String(Math.round(left));
}

/** Text color for a quota percentage or label. Uses the AA-safe `*-text` tokens. */
export const QUOTA_SEVERITY_TEXT_CLASS: Record<QuotaSeverity, string> = {
  none: 'text-text-faint',
  ok: 'text-text',
  warn: 'text-warn-text',
  danger: 'text-danger-text',
};
