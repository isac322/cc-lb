import type {
  WarmupAttempt,
  WarmupAttemptStatus,
  WarmupSummary,
} from '../../../../lib/queries';
import { REASON_LABEL } from './copy';

export type Severity = 'ok' | 'warn' | 'danger' | 'neutral';

export const OUTCOME_SEVERITY: Record<WarmupAttemptStatus, Severity> = {
  success: 'ok',
  skipped: 'neutral',
  transient_failure: 'warn',
  permanent_failure: 'danger',
};

/** Text color for an explicit warm-up outcome; dots use `status-dot <severity>`. */
export const SEVERITY_TEXT_CLASS: Record<Severity, string> = {
  ok: 'text-traffic-success-text',
  warn: 'text-warn-text',
  danger: 'text-danger-text',
  neutral: 'text-text-muted',
};

/** Compact human duration: 12s, 5m, 5m 12s, 1h 47m, 3d 4h. */
export function formatDuration(secs: number | null | undefined): string {
  if (secs == null || !Number.isFinite(secs)) return '—';
  const n = Math.max(0, Math.floor(secs));
  if (n < 60) return `${n}s`;
  const m = Math.floor(n / 60);
  if (m < 60) {
    const s = n % 60;
    return s > 0 ? `${m}m ${s}s` : `${m}m`;
  }
  const h = Math.floor(m / 60);
  const mm = m % 60;
  if (h < 24) return mm > 0 ? `${h}h ${mm}m` : `${h}h`;
  const d = Math.floor(h / 24);
  const hh = h % 24;
  return hh > 0 ? `${d}d ${hh}h` : `${d}d`;
}

/** One-line narrative for an attempt. */
export function formatResultNarrative(
  attempt: WarmupAttempt | null | undefined,
): string {
  if (!attempt) return 'No warm-up attempts recorded yet';
  const reasonSuffix = attempt.reason
    ? ` · ${REASON_LABEL[attempt.reason]}`
    : '';
  switch (attempt.status) {
    case 'success':
      return attempt.reason === 'cycle_advanced'
        ? 'Fresh window started'
        : 'Window was already active';
    case 'transient_failure':
      return `Transient failure${reasonSuffix}`;
    case 'permanent_failure':
      return `Permanent failure${reasonSuffix}`;
    case 'skipped':
      return `Skipped${reasonSuffix}`;
  }
}

/** Did this warm-up actually start a fresh 5h window? + idle context. */
export function formatFreshnessLine(
  attempt: WarmupAttempt | null | undefined,
): string {
  if (!attempt) return '—';
  switch (attempt.status) {
    case 'success': {
      if (attempt.reason === 'cycle_advanced') {
        const idle = attempt.idle_secs_since_prev_window;
        if (idle != null && idle > 0)
          return `Started a new 5h window — idle ${formatDuration(idle)} before warm-up`;
        return 'Started a new 5h window';
      }
      return 'Window was already active — no new cycle started';
    }
    case 'transient_failure':
    case 'permanent_failure':
      return 'Not evaluated — the call failed';
    case 'skipped':
      return 'Not evaluated — warm-up was skipped';
  }
}

export interface ActiveIncident {
  lastFailure: WarmupAttempt;
  /** Consecutive failures from newest backward (skips are passed through). */
  consecutiveCount: number;
  /** Total failures within the recent attempts window. */
  recentFailures: number;
  /** Most-frequent failure reason in the recent attempts window. */
  dominantReason: string | null;
}

/**
 * Currently-ongoing incident, or null if the latest non-skipped result was a
 * success. Walks newest → older; `skipped` attempts are passed through (they
 * neither resolve nor extend the incident).
 */
export function detectActiveIncident(
  summary: WarmupSummary | undefined,
): ActiveIncident | null {
  if (!summary) return null;
  const attempts = summary.recent_attempts ?? [];
  if (attempts.length === 0) return null;

  let lastFailure: WarmupAttempt | null = null;
  let consecutive = 0;
  for (const a of attempts) {
    if (a.status === 'skipped') continue;
    if (a.status === 'success') {
      // A success was seen. If we already collected failures NEWER than this
      // success, those are the active streak — keep them. Otherwise no incident.
      break;
    }
    if (!lastFailure) {
      lastFailure = a;
      consecutive = 1;
    } else {
      consecutive++;
    }
  }
  if (!lastFailure) return null;

  // Failure totals across the window.
  const reasonCounts = new Map<string, number>();
  let total = 0;
  for (const a of attempts) {
    if (a.status === 'permanent_failure' || a.status === 'transient_failure') {
      total++;
      const key = a.reason ?? a.status;
      reasonCounts.set(key, (reasonCounts.get(key) ?? 0) + 1);
    }
  }
  let dominant: string | null = null;
  let max = 0;
  for (const [k, v] of reasonCounts) {
    if (v > max) {
      max = v;
      dominant = REASON_LABEL[k as keyof typeof REASON_LABEL] ?? k;
    }
  }
  return {
    lastFailure,
    consecutiveCount: consecutive,
    recentFailures: total,
    dominantReason: dominant,
  };
}
