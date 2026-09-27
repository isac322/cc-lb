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

/** Text color for a quota percentage or label. Uses the AA-safe `*-text` tokens. */
export const QUOTA_SEVERITY_TEXT_CLASS: Record<QuotaSeverity, string> = {
  none: 'text-text-faint',
  ok: 'text-text',
  warn: 'text-warn-text',
  danger: 'text-danger-text',
};

/**
 * The one human label for a quota limit status: `allowed_warning` reads
 * "Allowed warning". Every surface that prints a status goes through this
 * (and `quotaStatusTone`) so the table caption and the detail pane's quota
 * rows can never drift.
 */
export function quotaStatusLabel(status: string): string {
  const text = status.replaceAll('_', ' ');
  return text.charAt(0).toUpperCase() + text.slice(1);
}

export type QuotaStatusTone = 'danger' | 'warn' | 'default';

/**
 * The severity a limit status earns: `rejected` is danger, any `*_warning`
 * is warn, anything else (including `allowed`) is plain text.
 */
export function quotaStatusTone(status: string): QuotaStatusTone {
  if (status === 'rejected') return 'danger';
  if (status.endsWith('warning')) return 'warn';
  return 'default';
}
