import { WINDOW_DURATION_SECS } from './colors';

/**
 * Quota severity is the only thing a quota figure's color communicates, and
 * it is pace-relative: usage far ahead of even pace reads worse than the
 * same figure on pace. `used` at 95%+ is always danger. Without a pace
 * reading (untimed windows, unstarted or expired resets) severity falls back
 * to the absolute rule: warn from 80% used. With pace, danger when used
 * runs 30+ points ahead of pace, warn when used is 90%+ or 10+ points
 * ahead, ok otherwise. Window identity (5h / 7d / 7d_fable) belongs to
 * legends and chart series, never to the number or bar itself.
 */
export type QuotaSeverity = 'none' | 'ok' | 'warn' | 'danger';

/** Absolute thresholds: warn below this, danger from this, pace or not. */
export const QUOTA_WARN_PCT = 80;
export const QUOTA_DANGER_PCT = 95;
/**
 * With an even-pace reading, warn starts at this utilization and the
 * used-minus-pace gaps below decide warn vs danger.
 */
export const QUOTA_PACE_WARN_PCT = 90;
export const QUOTA_PACE_WARN_GAP = 10;
export const QUOTA_PACE_DANGER_GAP = 30;

/**
 * `used` is 0-100; `null` or a non-finite value means no reading. `pace` is
 * the even-pace mark from `quotaPacePct`; `null` means pace is unavailable
 * and severity falls back to the absolute warn-at-80 rule.
 */
export function quotaSeverity(
  used: number | null,
  pace?: number | null,
): QuotaSeverity {
  if (used === null || !Number.isFinite(used)) return 'none';
  if (used >= QUOTA_DANGER_PCT) return 'danger';
  if (pace === null || pace === undefined || !Number.isFinite(pace)) {
    return used >= QUOTA_WARN_PCT ? 'warn' : 'ok';
  }
  const gap = used - pace;
  if (gap >= QUOTA_PACE_DANGER_GAP) return 'danger';
  if (used >= QUOTA_PACE_WARN_PCT || gap >= QUOTA_PACE_WARN_GAP) return 'warn';
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
 * Even pace for a timed quota window (0-100): where usage would sit if it
 * were spread evenly over the window, i.e. the share of the window already
 * elapsed. The window started at `resets_at − length` (5h or 7d). `null`
 * for windows without a fixed length (Extra usage, Unified), without a reset
 * time, or when the window is not running: a reset already passed, or a
 * reset so far ahead the window has not opened yet.
 */
export function quotaPacePct(
  window: string,
  resetsAtUnixSecs: number | null | undefined,
  nowUnixSecs: number,
): number | null {
  const length = WINDOW_DURATION_SECS[window];
  if (!length || resetsAtUnixSecs == null || !Number.isFinite(resetsAtUnixSecs))
    return null;
  const start = resetsAtUnixSecs - length;
  if (nowUnixSecs <= start || resetsAtUnixSecs <= nowUnixSecs) return null;
  return Math.min(100, ((nowUnixSecs - start) / length) * 100);
}

/** The fields of a `/latest` quota snapshot that decide its severity. */
export interface QuotaReading {
  window: string;
  state: string;
  /** 0-1 as reported; `null` without a reading. */
  utilization: number | null;
  resets_at_unix_secs: number | null;
}

/**
 * Even pace for a snapshot's window at `atUnixSecs`, from that window's own
 * reset. `null` when the snapshot was never observed or `atUnixSecs` lies
 * outside the window that ends at the reset (expired, not yet opened), so
 * severity falls back to the absolute rule. A past `atUnixSecs` inside the
 * same window gives that moment's pace, never the current one.
 */
export function snapshotQuotaPacePct(
  snap: QuotaReading,
  atUnixSecs: number,
): number | null {
  if (snap.state === 'unobserved') return null;
  return quotaPacePct(snap.window, snap.resets_at_unix_secs, atUnixSecs);
}

/** `quotaSeverity` of a snapshot's used% against its pace at `nowUnixSecs`. */
export function snapshotQuotaSeverity(
  snap: QuotaReading,
  nowUnixSecs: number,
): QuotaSeverity {
  const used =
    snap.utilization == null || !Number.isFinite(snap.utilization)
      ? null
      : snap.utilization * 100;
  return quotaSeverity(used, snapshotQuotaPacePct(snap, nowUnixSecs));
}

const SEVERITY_ORDER: Record<QuotaSeverity, number> = {
  none: 0,
  ok: 1,
  warn: 2,
  danger: 3,
};

/** The most severe of `severities`; `none` when empty. */
export function worstQuotaSeverity(
  severities: Iterable<QuotaSeverity>,
): QuotaSeverity {
  let worst: QuotaSeverity = 'none';
  for (const severity of severities) {
    if (SEVERITY_ORDER[severity] > SEVERITY_ORDER[worst]) worst = severity;
  }
  return worst;
}
