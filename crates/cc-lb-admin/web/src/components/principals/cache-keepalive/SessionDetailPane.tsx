import { AlertTriangle } from 'lucide-react';
import { useState } from 'react';
import { ApiError } from '../../../lib/api';
import type {
  CacheKeepaliveDetail,
  CacheKeepaliveGap,
  CacheKeepaliveHorizon,
} from '../../../lib/cacheKeepaliveApi';
import { useTimezone } from '../../../lib/locale';
import { useCacheKeepaliveSessionDetail } from '../../../lib/queries';
import { Button, cx, Skeleton } from '../../ui/primitives';
import { RelativeTime } from '../../ui/RelativeTime';

interface Props {
  principalId: string;
  sessionId: string;
  horizon?: CacheKeepaliveHorizon;
  rowFingerprint?: string | null;
  onClose: () => void;
}

export function fmtUsdSigned(val: number | null): string {
  if (val === null) return '—';
  if (Math.abs(val) < 0.00005) return '$0';
  const sign = val > 0 ? '+' : '−';
  const abs = Math.abs(val);
  const precision = abs >= 0.1 ? 2 : 4;
  const fixed = abs.toFixed(precision).replace(/0+$/, '').replace(/\.$/, '');
  return `${sign}$${fixed}`;
}

export function fmtUsdPlain(val: number | null): string {
  if (val === null) return '—';
  const abs = Math.abs(val);
  if (abs < 0.00005) return '$0';
  const precision = abs >= 0.1 ? 2 : 4;
  const fixed = abs.toFixed(precision).replace(/0+$/, '').replace(/\.$/, '');
  return `$${fixed}`;
}

function pnlTone(value: number | null): string {
  if (value == null) return 'text-text-muted';
  if (value > 0) return 'text-traffic-success-text';
  if (value < 0) return 'text-warn-text';
  return 'text-text-muted';
}

function compactTokens(tokens: number): string {
  if (tokens >= 1_000_000) return `${(tokens / 1_000_000).toFixed(2)}M`;
  if (tokens >= 1_000) return `${(tokens / 1_000).toFixed(1)}k`;
  return `${tokens}`;
}

function deltaLabel(seconds: number): string {
  const clamped = Math.max(0, Math.round(seconds));
  if (clamped === 0) return '0s';
  const hours = Math.floor(clamped / 3600);
  const minutes = Math.floor((clamped % 3600) / 60);
  const secs = clamped % 60;
  if (hours > 0) {
    return minutes > 0 ? `${hours}h ${minutes}m` : `${hours}h`;
  }
  if (minutes === 0) return `${secs}s`;
  if (secs === 0) return `${minutes}m`;
  return `${minutes}m ${String(secs).padStart(2, '0')}s`;
}

function plusDeltaLabel(seconds: number): string {
  return `+${deltaLabel(seconds)}`;
}

function railTime(ms: number, timeZone: string): string {
  const parts = new Intl.DateTimeFormat('en-GB', {
    timeZone,
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  }).formatToParts(new Date(ms));
  const map: Record<string, string> = {};
  for (const part of parts) map[part.type] = part.value;
  const hh = map.hour === '24' ? '00' : (map.hour ?? '00');
  const mm = map.minute ?? '00';
  const ss = map.second ?? '00';
  return `${hh}:${mm}:${ss}`;
}

function shortThreadDisplay(id: string): string {
  const slashIdx = id.indexOf('/');
  const raw = slashIdx >= 0 ? id.slice(slashIdx + 1) : id;
  if (raw.length <= 28) return raw;
  return `${raw.slice(0, 15)}…${raw.slice(-12)}`;
}

interface RailPoint {
  id: string;
  pct: number;
  kind:
    | 'start'
    | 'pulse'
    | 'end_hit'
    | 'end_waiting'
    | 'end_unknown'
    | 'end_miss';
  title: string;
  badgeText: string;
  badgeTone: 'neutral' | 'accent' | 'ok' | 'warn';
  absTime: string;
  sinceUserReq: string;
  sinceLastRenewal: string;
}

function buildRailPoints(
  gap: CacheKeepaliveGap,
  maxAttempts: number,
  timeZone: string,
): RailPoint[] {
  const maxSec = Math.max(gap.total_duration_sec, 300);
  const points: RailPoint[] = [
    {
      id: 'start',
      pct: 0,
      kind: 'start',
      title: 'Message paused',
      badgeText: `${compactTokens(gap.cached_tokens)} cached`,
      badgeTone: 'neutral',
      absTime: railTime(gap.pause_at_ms, timeZone),
      sinceUserReq: '0s (paused)',
      sinceLastRenewal: '— (before renewal)',
    },
  ];

  gap.pulses.forEach((pulse, idx) => {
    const prevPulse = idx > 0 ? gap.pulses[idx - 1] : null;
    const ratio = Math.min(1, Math.max(0, pulse.offset_sec / maxSec));
    const pct = 7 + ratio * 86;
    const sincePrevSec =
      pulse.since_prev_renewal_sec ??
      (prevPulse ? Math.max(0, pulse.offset_sec - prevPulse.offset_sec) : null);
    points.push({
      id: `pulse-${pulse.pulse_number}`,
      pct,
      kind: 'pulse',
      title: `Renewal #${pulse.pulse_number}`,
      badgeText:
        pulse.spent_usd === null ? '—' : `−${fmtUsdPlain(pulse.spent_usd)}`,
      badgeTone: 'accent',
      absTime: railTime(pulse.time_ms, timeZone),
      sinceUserReq: plusDeltaLabel(pulse.offset_sec),
      sinceLastRenewal:
        prevPulse && sincePrevSec !== null
          ? `${plusDeltaLabel(sincePrevSec)} (after #${prevPulse.pulse_number})`
          : '— (first renewal)',
    });
  });

  const lastPulse =
    gap.pulses.length > 0 ? gap.pulses[gap.pulses.length - 1] : null;
  const sinceLastSec =
    gap.since_last_renewal_sec ??
    (lastPulse
      ? Math.max(0, gap.total_duration_sec - lastPulse.offset_sec)
      : null);
  const sinceLastRenewalForEnd =
    lastPulse && sinceLastSec !== null
      ? `${plusDeltaLabel(sinceLastSec)} (after #${lastPulse.pulse_number})`
      : '— (no renewals)';

  if (gap.ending_kind === 'followup_hit') {
    points.push({
      id: 'end',
      pct: 100,
      kind: 'end_hit',
      title: 'Next message (cache hit)',
      badgeText:
        gap.net_pnl_usd !== null && gap.net_pnl_usd > 0
          ? `${fmtUsdSigned(gap.net_pnl_usd)} saved`
          : fmtUsdSigned(gap.net_pnl_usd),
      badgeTone:
        gap.net_pnl_usd !== null && gap.net_pnl_usd > 0
          ? 'ok'
          : gap.net_pnl_usd !== null && gap.net_pnl_usd < 0
            ? 'warn'
            : 'neutral',
      absTime: railTime(gap.ending_at_ms, timeZone),
      sinceUserReq: plusDeltaLabel(gap.total_duration_sec),
      sinceLastRenewal: sinceLastRenewalForEnd,
    });
  } else if (gap.ending_kind === 'scheduled_waiting') {
    points.push({
      id: 'end',
      pct: 100,
      kind: 'end_waiting',
      title: 'Waiting for next message',
      badgeText: deltaLabel(gap.total_duration_sec),
      badgeTone: 'accent',
      absTime: railTime(gap.ending_at_ms, timeZone),
      sinceUserReq: plusDeltaLabel(gap.total_duration_sec),
      sinceLastRenewal: sinceLastRenewalForEnd,
    });
  } else if (gap.ending_kind === 'followup_unknown') {
    points.push({
      id: 'end',
      pct: 100,
      kind: 'end_unknown',
      title: 'Follow-up unknown',
      badgeText: fmtUsdSigned(gap.net_pnl_usd),
      badgeTone: 'neutral',
      absTime: railTime(gap.ending_at_ms, timeZone),
      sinceUserReq: plusDeltaLabel(gap.total_duration_sec),
      sinceLastRenewal: sinceLastRenewalForEnd,
    });
  } else {
    const capTitle =
      gap.ending_kind === 'capped_no_followup'
        ? gap.pulses.length >= maxAttempts && maxAttempts > 0
          ? `No follow-up (${gap.pulses.length}/${maxAttempts} cap)`
          : 'No follow-up (capped)'
        : 'No follow-up (TTL expired)';
    points.push({
      id: 'end',
      pct: 100,
      kind: 'end_miss',
      title: capTitle,
      badgeText: fmtUsdSigned(gap.net_pnl_usd),
      badgeTone:
        gap.net_pnl_usd !== null && gap.net_pnl_usd < 0
          ? 'warn'
          : gap.net_pnl_usd !== null && gap.net_pnl_usd > 0
            ? 'ok'
            : 'neutral',
      absTime: railTime(gap.ending_at_ms, timeZone),
      sinceUserReq: plusDeltaLabel(gap.total_duration_sec),
      sinceLastRenewal: sinceLastRenewalForEnd,
    });
  }

  return points;
}

function PointHoverPopover({
  point,
  placeBelow,
}: {
  point: RailPoint;
  placeBelow: boolean;
}) {
  const alignClass =
    point.pct <= 20
      ? 'left-0'
      : point.pct >= 80
        ? 'right-0'
        : 'left-1/2 -translate-x-1/2';

  const toneClass =
    point.badgeTone === 'ok'
      ? 'text-traffic-success-text'
      : point.badgeTone === 'warn'
        ? 'text-warn-text'
        : point.badgeTone === 'accent'
          ? 'text-accent-text'
          : 'text-text-muted';

  return (
    <div
      role="tooltip"
      style={
        point.pct > 20 && point.pct < 80 ? { left: `${point.pct}%` } : undefined
      }
      className={cx(
        'pointer-events-none absolute z-30 w-72 rounded-sm border border-subtle-strong bg-panel p-2.5 shadow-overlay tabular-nums',
        placeBelow ? 'top-full mt-2' : 'bottom-full mb-2',
        alignClass,
      )}
    >
      <div className="flex items-baseline justify-between gap-2 border-b border-row pb-1.5 text-caption">
        <span className="font-medium text-text">{point.title}</span>
        <span className={cx('font-mono text-data font-medium', toneClass)}>
          {point.badgeText}
        </span>
      </div>
      <dl className="mt-1.5 space-y-1 text-caption">
        <div className="flex items-baseline justify-between gap-2">
          <dt className="text-text-faint">Timestamp</dt>
          <dd className="font-mono text-data text-text">{point.absTime}</dd>
        </div>
        <div className="flex items-baseline justify-between gap-2">
          <dt className="text-text-faint">Since last request</dt>
          <dd className="font-mono text-data text-text">
            {point.sinceUserReq}
          </dd>
        </div>
        <div className="flex items-baseline justify-between gap-2">
          <dt className="text-text-faint">Since last renewal</dt>
          <dd className="font-mono text-data text-text">
            {point.sinceLastRenewal}
          </dd>
        </div>
      </dl>
    </div>
  );
}

function TimeRailGap({
  gap,
  maxAttempts,
  placeTooltipBelow,
  timeZone,
}: {
  gap: CacheKeepaliveGap;
  maxAttempts: number;
  placeTooltipBelow: boolean;
  timeZone: string;
}) {
  const [hoveredId, setHoveredId] = useState<string | null>(null);
  const count = gap.pulses.length;
  const points = buildRailPoints(gap, maxAttempts, timeZone);
  const activePoint = points.find((pt) => pt.id === hoveredId) ?? null;
  const netText = fmtUsdSigned(gap.net_pnl_usd);
  const netTone = pnlTone(gap.net_pnl_usd);
  const afterRenewalLabel =
    gap.since_last_renewal_sec != null
      ? `${plusDeltaLabel(gap.since_last_renewal_sec)} later`
      : plusDeltaLabel(gap.total_duration_sec);

  return (
    <div className="space-y-2 rounded-sm bg-overlay-2 px-3.5 py-3 tabular-nums">
      {/* Top Endpoint Labels: Start Message (Left) vs Next Message + Net (Right) */}
      <div className="flex flex-wrap items-baseline justify-between gap-2 text-body-sm">
        <div className="flex items-center gap-1.5">
          <span className="font-mono text-data text-text-muted">
            {railTime(gap.pause_at_ms, timeZone)}
          </span>
          <span className="font-medium text-text">Paused</span>
          {gap.cached_tokens > 0 ? (
            <span className="text-caption text-text-faint">
              ({compactTokens(gap.cached_tokens)})
            </span>
          ) : null}
        </div>

        <div className="flex items-center gap-2">
          {gap.ending_kind === 'followup_hit' ? (
            <>
              <span className="text-caption text-text-muted">
                Next msg{' '}
                <span className="font-mono">
                  {railTime(gap.ending_at_ms, timeZone)}
                </span>{' '}
                ({afterRenewalLabel})
              </span>
              <span
                className={cx(
                  'whitespace-nowrap font-mono text-data font-medium',
                  netTone,
                )}
              >
                {netText}
              </span>
            </>
          ) : gap.ending_kind === 'scheduled_waiting' ? (
            <span className="text-caption text-text-muted">
              Waiting · {deltaLabel(gap.total_duration_sec)}
            </span>
          ) : gap.ending_kind === 'followup_unknown' ? (
            <>
              <span className="text-caption text-text-faint">
                Follow-up unknown ({railTime(gap.ending_at_ms, timeZone)})
              </span>
              <span
                className={cx(
                  'whitespace-nowrap font-mono text-data font-medium',
                  netTone,
                )}
              >
                {netText}
              </span>
            </>
          ) : (
            <>
              <span className="text-caption text-text-faint">
                No follow-up ({railTime(gap.ending_at_ms, timeZone)})
              </span>
              <span
                className={cx(
                  'whitespace-nowrap font-mono text-data font-medium',
                  netTone,
                )}
              >
                {netText}
              </span>
            </>
          )}
        </div>
      </div>

      {/* Proportional Time Rail with interactive hover points */}
      <div className="relative flex h-6 items-center">
        {activePoint ? (
          <PointHoverPopover
            point={activePoint}
            placeBelow={placeTooltipBelow}
          />
        ) : null}

        {/* Track line */}
        <div className="h-1.5 w-full rounded-full bg-overlay-4" />

        {/* Interactive Points (Start, Renewal Ticks, End) */}
        {points.map((pt) => {
          const isHovered = hoveredId === pt.id;
          return (
            <button
              key={pt.id}
              type="button"
              data-rail-point={pt.id}
              aria-label={`${pt.title}: Timestamp ${pt.absTime}, Since last request ${pt.sinceUserReq}, Since last renewal ${pt.sinceLastRenewal}`}
              style={{ left: `${pt.pct}%` }}
              onMouseEnter={() => setHoveredId(pt.id)}
              onMouseLeave={() =>
                setHoveredId((prev) => (prev === pt.id ? null : prev))
              }
              onFocus={() => setHoveredId(pt.id)}
              onBlur={() =>
                setHoveredId((prev) => (prev === pt.id ? null : prev))
              }
              className="group absolute flex h-6 w-6 -translate-x-1/2 items-center justify-center rounded-full focus-visible:outline-2 focus-visible:outline-accent"
            >
              {pt.kind === 'pulse' ? (
                <span
                  className={cx(
                    'h-3.5 w-1.5 rounded-xs bg-accent transition-transform',
                    isHovered ? 'scale-125 ring-2 ring-accent/40' : undefined,
                  )}
                />
              ) : (
                <span
                  className={cx(
                    'h-3 w-3 rounded-full border-2 transition-transform',
                    pt.kind === 'start'
                      ? 'border-subtle-strong bg-panel'
                      : pt.kind === 'end_hit'
                        ? 'border-traffic-success-text bg-traffic-success-text'
                        : pt.kind === 'end_waiting'
                          ? 'border-accent bg-accent'
                          : pt.kind === 'end_unknown'
                            ? 'border-subtle-strong bg-panel'
                            : 'border-subtle-strong bg-bg',
                    isHovered
                      ? 'scale-125 ring-2 ring-subtle-strong'
                      : undefined,
                  )}
                />
              )}
            </button>
          );
        })}
      </div>

      {/* Bottom 1-line micro legend for the rail */}
      <div className="flex items-center justify-between gap-2 text-caption text-text-faint">
        <span className="shrink-0 whitespace-nowrap">0s (pause)</span>
        <span className="min-w-0 text-center">
          {count === 0 ? (
            `Next renewal at ${plusDeltaLabel(gap.total_duration_sec)}`
          ) : count === 1 ? (
            <>
              ↻ 1 renewal at {plusDeltaLabel(gap.pulses[0].offset_sec)}{' '}
              <span className="whitespace-nowrap">
                (−{fmtUsdPlain(gap.spent_usd)})
              </span>
            </>
          ) : (
            <>
              ↻ {count} renewals ({plusDeltaLabel(gap.pulses[0].offset_sec)} →{' '}
              {plusDeltaLabel(gap.pulses[count - 1].offset_sec)}){' '}
              <span className="whitespace-nowrap">
                · −{fmtUsdPlain(gap.spent_usd)}
              </span>
            </>
          )}
        </span>
        <span className="shrink-0 whitespace-nowrap">
          {deltaLabel(gap.total_duration_sec)}
        </span>
      </div>
    </div>
  );
}

function leadWindowSec(session: CacheKeepaliveDetail): number | null {
  if (session.ttl === '12h') return null;
  const ttlSec =
    session.ttl === '1h' ? 3600 : session.ttl === '30m' ? 30 * 60 : 300;
  const leadSec =
    session.ttl === '1h'
      ? (session.config_snapshot?.lead_1h ?? 300)
      : (session.config_snapshot?.lead_5m ?? 30);
  return Math.max(0, ttlSec - leadSec);
}

function synthesizeGapsFromTurns(
  session: CacheKeepaliveDetail,
): CacheKeepaliveGap[] {
  const leadSec = leadWindowSec(session);
  if (leadSec === null) return [];
  if (session.turns.length === 0) {
    if (session.state === 'scheduled') {
      return [
        {
          cycle_id: null,
          pause_at_ms: session.last_message_at_ms,
          cached_tokens: session.renewal_tokens,
          pulses: [],
          ending_kind: 'scheduled_waiting',
          ending_at_ms: session.last_message_at_ms + leadSec * 1000,
          total_duration_sec: leadSec,
          since_last_renewal_sec: null,
          spent_usd: 0,
          avoided_usd: null,
          net_pnl_usd: null,
        },
      ];
    }
    return [];
  }

  const ordered = [...session.turns].sort((a, b) => a.time_ms - b.time_ms);
  const firstPulseMs = ordered[0].time_ms;
  const pauseAtMs = Math.max(0, firstPulseMs - leadSec * 1000);
  const pulses = ordered.map((turn, idx) => {
    const prev = idx > 0 ? ordered[idx - 1] : null;
    const offsetSec = Math.max(
      0,
      Math.round((turn.time_ms - pauseAtMs) / 1000),
    );
    const prevOffsetSec = prev
      ? Math.max(0, Math.round((prev.time_ms - pauseAtMs) / 1000))
      : null;
    return {
      pulse_number: idx + 1,
      time_ms: turn.time_ms,
      offset_sec: offsetSec,
      since_prev_renewal_sec:
        prevOffsetSec !== null ? Math.max(0, offsetSec - prevOffsetSec) : null,
      spent_usd:
        ordered.length > 0 ? session.total_spent / ordered.length : null,
      hit: turn.renewals > 0,
    };
  });
  const lastPulseMs = ordered[ordered.length - 1].time_ms;
  const anyFollowedUp = ordered.some((t) => t.followed_up === true);
  const anyUnknown = ordered.some(
    (t) => t.followed_up === null || t.evidence_status === 'unknown',
  );
  const anyPending = ordered.some((t) => t.pending);
  const endingAtMs = Math.max(session.last_message_at_ms, lastPulseMs);
  const totalDurationSec = Math.max(
    0,
    Math.round((endingAtMs - pauseAtMs) / 1000),
  );
  const sinceLastSec = Math.max(
    0,
    Math.round((endingAtMs - lastPulseMs) / 1000),
  );

  return [
    {
      cycle_id: null,
      pause_at_ms: pauseAtMs,
      cached_tokens: session.renewal_tokens,
      pulses,
      ending_kind: anyPending
        ? 'scheduled_waiting'
        : anyUnknown
          ? 'followup_unknown'
          : anyFollowedUp
            ? 'followup_hit'
            : session.state === 'capped'
              ? 'capped_no_followup'
              : 'ttl_expired_no_followup',
      ending_at_ms: endingAtMs,
      total_duration_sec: totalDurationSec,
      since_last_renewal_sec: sinceLastSec,
      spent_usd: session.total_spent,
      avoided_usd: session.total_avoided,
      net_pnl_usd: session.net_pnl,
    },
  ];
}

export function SessionDetailPane({
  principalId,
  sessionId,
  horizon,
  rowFingerprint,
  onClose,
}: Props) {
  const query = useCacheKeepaliveSessionDetail(
    principalId,
    sessionId,
    horizon ?? 'all',
    rowFingerprint,
  );
  const { effective: timeZone } = useTimezone();

  if (query.isLoading) {
    return (
      <div
        className="flex-1 min-w-0"
        data-testid="session-detail-loading"
        role="region"
        aria-label="Session detail"
        aria-busy="true"
      >
        <span className="sr-only" role="status">
          Loading session detail
        </span>
        <button
          type="button"
          aria-label="Back to sessions"
          className="m-3 mb-0 min-h-9 rounded-sm px-2 text-body-sm text-text-muted hover:bg-overlay-5 hover:text-text focus-visible:outline-2 focus-visible:outline-accent md:hidden"
          onClick={onClose}
        >
          ← Back to sessions
        </button>
        <div className="flex items-center justify-between border-b border-row px-4 py-2.5">
          <Skeleton className="h-4 w-48" />
          <Skeleton className="h-4 w-36" />
        </div>
        <div className="space-y-2.5 p-4">
          {Array.from({ length: 2 }).map((_, idx) => (
            <div
              key={idx}
              className="space-y-2 rounded-sm bg-overlay-2 px-3.5 py-3"
            >
              <div className="flex justify-between">
                <Skeleton className="h-4 w-32" />
                <Skeleton className="h-4 w-40" />
              </div>
              <Skeleton className="h-2 w-full" />
              <div className="flex justify-between">
                <Skeleton className="h-3 w-16" />
                <Skeleton className="h-3 w-36" />
                <Skeleton className="h-3 w-12" />
              </div>
            </div>
          ))}
        </div>
      </div>
    );
  }

  const isNotFound =
    query.error instanceof ApiError && query.error.status === 404;

  if (isNotFound || !query.data) {
    return (
      <div
        className="flex-1 min-w-0"
        data-testid="session-detail-error"
        role="region"
        aria-label="Session detail"
        aria-busy="false"
      >
        <div
          className="flex flex-1 flex-col items-center justify-center gap-3 p-6 text-center"
          role="alert"
        >
          <p className="text-body-sm text-text-muted">
            {isNotFound
              ? 'This session is no longer available.'
              : 'Failed to load session detail.'}
          </p>
          <div className="flex items-center gap-2">
            <Button
              variant="secondary"
              size="sm"
              onClick={() => query.refetch()}
            >
              {isNotFound ? 'Retry session detail' : 'Retry'}
            </Button>
            <Button variant="ghost" size="sm" onClick={onClose}>
              Back to sessions
            </Button>
          </div>
        </div>
      </div>
    );
  }

  const session = query.data;
  const isError = session.error != null;
  const railGaps =
    session.ttl === '12h'
      ? []
      : session.gaps && session.gaps.length > 0
        ? session.gaps
        : synthesizeGapsFromTurns(session);

  return (
    <div
      className="flex-1 min-w-0"
      data-testid="session-detail-content"
      role="region"
      aria-label="Session detail"
      aria-busy="false"
    >
      <button
        type="button"
        aria-label="Back to sessions"
        className="m-3 mb-0 min-h-9 rounded-sm px-2 text-body-sm text-text-muted hover:bg-overlay-5 hover:text-text focus-visible:outline-2 focus-visible:outline-accent md:hidden"
        onClick={onClose}
      >
        ← Back to sessions
      </button>

      {/* Single-line session summary bar (zero duplicate trigger/upstream/headings) */}
      <div className="flex flex-wrap items-center justify-between gap-2 border-b border-row px-4 py-2.5 text-caption tabular-nums">
        <div className="flex flex-wrap items-center gap-x-2 gap-y-0.5 text-text-muted">
          <span className="font-mono text-data text-text" title={session.id}>
            {shortThreadDisplay(session.id)}
          </span>
          <span className="text-text-faint">·</span>
          <span className="text-text-faint">
            <RelativeTime ts={session.last_message_at_ms} compact />
          </span>
        </div>

        <div className="flex min-w-0 flex-wrap items-center justify-end gap-2">
          <span className="text-text-faint">
            Saved{' '}
            <span className="whitespace-nowrap">
              {fmtUsdPlain(session.total_avoided)}
            </span>{' '}
            · Spent{' '}
            <span className="whitespace-nowrap">
              {fmtUsdPlain(session.total_spent)}
            </span>
          </span>
          {session.evidence_status !== 'observed' ? (
            <span className="text-text-faint">
              Follow-up {session.evidence_status} ·
            </span>
          ) : null}
          <span
            className={cx(
              'whitespace-nowrap font-mono text-data font-medium',
              pnlTone(session.net_pnl),
            )}
          >
            Net {fmtUsdSigned(session.net_pnl)}
          </span>
        </div>
      </div>

      <div className="space-y-3 p-4">
        {session.state === 'cancelled' ? (
          <p className="rounded-sm bg-overlay-2 px-3.5 py-3 text-body-sm text-text-muted">
            {session.cancel_reason ? session.reason : 'Cancelled'}
          </p>
        ) : null}
        {session.ttl === '12h' ? (
          <p
            className="rounded-sm bg-overlay-2 px-3.5 py-3 text-body-sm text-text-muted"
            data-testid="cache-keepalive-12h-retention"
          >
            12h retention is recorded without read re-anchoring. This session is
            not periodically renewed.
          </p>
        ) : null}
        {query.isError ? (
          <div
            className="flex items-center justify-between gap-2 rounded-sm bg-danger/8 px-3 py-2 text-caption text-danger-text"
            role="alert"
          >
            <span>Refresh failed. Showing the last successful detail.</span>
            <Button variant="ghost" size="sm" onClick={() => query.refetch()}>
              Retry
            </Button>
          </div>
        ) : null}

        {isError ? (
          <div className="flex items-start gap-2 rounded-sm bg-danger/8 px-3 py-2 text-body-sm text-danger-text">
            <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" />
            <span>{session.error}</span>
          </div>
        ) : null}

        {railGaps.length > 0 ? (
          <div className="space-y-2.5" data-testid="session-detail-time-rail">
            {railGaps.map((gap, index) => (
              <TimeRailGap
                key={gap.cycle_id ?? `${gap.pause_at_ms}-${index}`}
                gap={gap}
                maxAttempts={session.max_attempts}
                placeTooltipBelow={index === 0}
                timeZone={timeZone}
              />
            ))}
          </div>
        ) : session.state !== 'cancelled' ? (
          <div className="rounded-sm bg-overlay-2 px-3.5 py-3 text-body-sm text-text-muted">
            {session.reason}
          </div>
        ) : null}
      </div>
    </div>
  );
}
