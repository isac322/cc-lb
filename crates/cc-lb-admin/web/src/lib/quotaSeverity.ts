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

/** Text color for a quota percentage or label. Uses the AA-safe `*-text` tokens. */
export const QUOTA_SEVERITY_TEXT_CLASS: Record<QuotaSeverity, string> = {
  none: 'text-text-faint',
  ok: 'text-text',
  warn: 'text-warn-text',
  danger: 'text-danger-text',
};

/** Background for a quota meter's filled segment. */
export const QUOTA_SEVERITY_FILL_CLASS: Record<QuotaSeverity, string> = {
  none: 'bg-overlay-4',
  ok: 'bg-text-muted',
  warn: 'bg-warn',
  danger: 'bg-danger',
};
