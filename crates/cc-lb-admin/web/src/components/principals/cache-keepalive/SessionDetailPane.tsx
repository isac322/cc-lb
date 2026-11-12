import { AlertTriangle, ArrowLeft } from 'lucide-react';
import { type CSSProperties, useEffect, useId, useRef, useState } from 'react';
import { ApiError } from '../../../lib/api';
import type {
  CacheKeepaliveDetail,
  CacheKeepaliveGap,
  CacheKeepaliveHorizon,
  CacheKeepaliveRow,
  CacheKeepaliveTurn,
} from '../../../lib/cacheKeepaliveApi';
import { useTimezone } from '../../../lib/locale';
import { useCacheKeepaliveSessionDetail } from '../../../lib/queries';
import { Button, cx, Skeleton } from '../../ui/primitives';
import { RelativeTime } from '../../ui/RelativeTime';
import {
  Table,
  TableCell,
  TableHead,
  TableHeadCell,
  TableRow,
} from '../../ui/Table';
import { sessionStateDisplay } from './sessionStateDisplay';

interface Props {
  principalId: string;
  sessionId: string;
  horizon?: CacheKeepaliveHorizon;
  rowFingerprint?: string | null;
  rowLifecycle?: Pick<CacheKeepaliveRow, 'generation' | 'state' | 'is_active'>;
  onClose: () => void;
}

export function fmtUsdSigned(val: number | null): string {
  if (val === null) return '—';
  if (Math.abs(val) < 0.00005) return '$0';
  const sign = val > 0 ? '+' : '−';
  const abs = Math.abs(val);
  const rounded = abs.toFixed(4);
  const fixed =
    Number(rounded) >= 0.1
      ? abs.toFixed(2)
      : rounded.replace(/0+$/, '').replace(/\.$/, '');
  return `${sign}$${fixed}`;
}

export function fmtUsdPlain(val: number | null): string {
  if (val === null) return '—';
  const abs = Math.abs(val);
  if (abs < 0.00005) return '$0';
  const rounded = abs.toFixed(4);
  const fixed =
    Number(rounded) >= 0.1
      ? abs.toFixed(2)
      : rounded.replace(/0+$/, '').replace(/\.$/, '');
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

function railTime(ms: number, timeZone: string, previousMs?: number): string {
  const formatter = new Intl.DateTimeFormat('en-GB', {
    timeZone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hourCycle: 'h23',
  });
  const values = (at: number) =>
    Object.fromEntries(
      formatter
        .formatToParts(new Date(at))
        .map((part) => [part.type, part.value]),
    );
  const parts = values(ms);
  const day = (parts: Record<string, string>) =>
    `${parts.year}-${parts.month}-${parts.day}`;
  const needsDate =
    day(parts) !== day(values(Date.now())) ||
    (previousMs !== undefined && day(parts) !== day(values(previousMs)));
  return `${needsDate ? `${parts.month}-${parts.day} ` : ''}${parts.hour}:${parts.minute}:${parts.second}`;
}

function shortThreadDisplay(id: string): string {
  const slashIdx = id.indexOf('/');
  const raw = slashIdx >= 0 ? id.slice(slashIdx + 1) : id;
  if (raw.length <= 28) return raw;
  return `${raw.slice(0, 15)}…${raw.slice(-12)}`;
}

function fullTimestamp(ms: number, timeZone: string): string {
  return new Date(ms).toLocaleString('en-GB', {
    timeZone,
    year: 'numeric',
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hourCycle: 'h23',
    timeZoneName: 'short',
  });
}

function followUpLabel(turn?: CacheKeepaliveTurn): string {
  if (!turn) return 'Unknown';
  if (turn.pending) return 'Pending';
  if (turn.followed_up === true) return 'Observed';
  if (turn.followed_up === false) return 'None';
  return 'Unknown';
}

function matchGapTurns(
  gaps: CacheKeepaliveGap[],
  turns: CacheKeepaliveTurn[],
  synthesized: boolean,
): Array<Array<CacheKeepaliveTurn | undefined>> {
  const available = [...turns].sort((a, b) => a.time_ms - b.time_ms);

  // Claim each turn at most once across the session. Reserve cycle ordinals
  // and exact timestamps before nearby pulses can consume their evidence.
  // Null-cycle turn ordinals span multiple gaps; only their timestamps identify
  // gap-local pulses, whose numbering restarts in each gap.
  const matched = gaps.map((gap) =>
    gap.pulses.map((pulse) => {
      const matchIndex = available.findIndex((turn) => {
        if (synthesized) return turn.time_ms === pulse.time_ms;
        if (turn.cycle_id !== gap.cycle_id) return false;
        return gap.cycle_id === null
          ? turn.time_ms === pulse.time_ms
          : turn.turn_number === pulse.pulse_number;
      });
      if (matchIndex >= 0) return available.splice(matchIndex, 1)[0];
      // The legacy rail is synthesized from this ordered turn list.
      return synthesized ? available.shift() : undefined;
    }),
  );

  return matched.map((gapTurns, gapIndex) =>
    gapTurns.map((turn, pulseIndex) => {
      if (turn) return turn;
      const gap = gaps[gapIndex];
      let nearestIndex = -1;
      let nearestDelta = 3001;
      available.forEach((candidate, candidateIndex) => {
        if (candidate.cycle_id !== gap.cycle_id) return;
        const delta = Math.abs(
          candidate.time_ms - gap.pulses[pulseIndex].time_ms,
        );
        if (delta <= 3000 && delta < nearestDelta) {
          nearestIndex = candidateIndex;
          nearestDelta = delta;
        }
      });
      return nearestIndex >= 0
        ? available.splice(nearestIndex, 1)[0]
        : undefined;
    }),
  );
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
  timeMs: number;
  hit?: boolean;
  spent?: number | null;
  spendEstimated?: boolean;
  net?: number | null;
  followUp?: string;
}

function buildRailPoints(
  gap: CacheKeepaliveGap,
  maxAttempts: number,
  timeZone: string,
  turns: Array<CacheKeepaliveTurn | undefined>,
  synthesized: boolean,
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
      timeMs: gap.pause_at_ms,
      sinceUserReq: '0s (paused)',
      sinceLastRenewal: '— (before renewal)',
    },
  ];

  gap.pulses.forEach((pulse, idx) => {
    const prevPulse = idx > 0 ? gap.pulses[idx - 1] : null;
    const turn = turns[idx];
    const ratio = Math.min(1, Math.max(0, pulse.offset_sec / maxSec));
    const pct = 7 + ratio * 86;
    const sincePrevSec =
      pulse.since_prev_renewal_sec ??
      (prevPulse ? Math.max(0, pulse.offset_sec - prevPulse.offset_sec) : null);
    points.push({
      id: `pulse-${pulse.pulse_number}`,
      pct,
      kind: 'pulse',
      title: `Renewal #${turn?.turn_number ?? pulse.pulse_number}`,
      badgeText: pulse.hit ? 'Hit' : 'Miss',
      badgeTone: pulse.hit ? 'ok' : 'warn',
      absTime: fullTimestamp(pulse.time_ms, timeZone),
      timeMs: pulse.time_ms,
      hit: pulse.hit,
      spent: pulse.spent_usd,
      spendEstimated: synthesized,
      net: turn?.pnl ?? null,
      followUp: followUpLabel(turn),
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
      absTime: railTime(gap.ending_at_ms, timeZone, gap.pause_at_ms),
      timeMs: gap.ending_at_ms,
      sinceUserReq: plusDeltaLabel(gap.total_duration_sec),
      sinceLastRenewal: sinceLastRenewalForEnd,
    });
  } else if (
    gap.ending_kind === 'scheduled_waiting' ||
    gap.ending_kind === 'followup_pending'
  ) {
    points.push({
      id: 'end',
      pct: 100,
      kind: 'end_waiting',
      title:
        gap.ending_kind === 'followup_pending'
          ? 'Waiting for follow-up evidence'
          : 'Waiting for next message',
      badgeText: deltaLabel(gap.total_duration_sec),
      badgeTone: gap.ending_kind === 'followup_pending' ? 'neutral' : 'accent',
      absTime: railTime(gap.ending_at_ms, timeZone, gap.pause_at_ms),
      timeMs: gap.ending_at_ms,
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
      absTime: railTime(gap.ending_at_ms, timeZone, gap.pause_at_ms),
      timeMs: gap.ending_at_ms,
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
      absTime: railTime(gap.ending_at_ms, timeZone, gap.pause_at_ms),
      timeMs: gap.ending_at_ms,
      sinceUserReq: plusDeltaLabel(gap.total_duration_sec),
      sinceLastRenewal: sinceLastRenewalForEnd,
    });
  }

  return points;
}

function PointHoverPopover({
  point,
  placeBelow,
  id,
}: {
  point: RailPoint;
  placeBelow: boolean;
  id: string;
}) {
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
      id={id}
      role="tooltip"
      style={{
        left: `clamp(0px, calc(${point.pct}% - 9rem), max(0px, 100% - 18rem))`,
      }}
      className={cx(
        'absolute z-30 w-72 max-w-full rounded-md border border-subtle-strong bg-bg-sub p-3 shadow-overlay tabular-nums',
        placeBelow ? 'top-full' : 'bottom-full',
      )}
    >
      <div className="flex items-baseline justify-between gap-2 border-b border-row pb-2 text-caption">
        <span className="font-medium text-text">{point.title}</span>
        <span
          className={cx(
            'font-medium',
            point.kind === 'pulse' && 'rounded-sm bg-overlay-5 px-1.5 py-0.5',
            toneClass,
          )}
        >
          {point.badgeText}
        </span>
      </div>
      <dl className="mt-2 space-y-1 text-caption">
        <div className="flex flex-wrap items-baseline justify-between gap-x-2">
          <dt className="text-text-faint">Timestamp</dt>
          <dd className="text-text">
            <time dateTime={new Date(point.timeMs).toISOString()}>
              {point.absTime}
            </time>
          </dd>
        </div>
        <div className="flex items-baseline justify-between gap-2">
          <dt className="text-text-faint">Since last request</dt>
          <dd className="text-text">{point.sinceUserReq}</dd>
        </div>
        <div className="flex items-baseline justify-between gap-2">
          <dt className="text-text-faint">Since last renewal</dt>
          <dd className="text-right text-text">{point.sinceLastRenewal}</dd>
        </div>
        {point.kind === 'pulse' ? (
          <>
            <div className="flex items-baseline justify-between gap-2">
              <dt className="text-text-faint">Spent</dt>
              <dd
                className="text-text"
                title={
                  point.spendEstimated
                    ? 'Estimated per-renewal spend'
                    : undefined
                }
              >
                {point.spendEstimated && point.spent != null ? '≈ ' : ''}
                {fmtUsdPlain(point.spent ?? null)}
              </dd>
            </div>
            <div className="flex items-baseline justify-between gap-2">
              <dt className="text-text-faint">Net</dt>
              <dd className={pnlTone(point.net ?? null)}>
                {fmtUsdSigned(point.net ?? null)}
              </dd>
            </div>
            <div className="flex items-baseline justify-between gap-2">
              <dt className="text-text-faint">Follow-up</dt>
              <dd className="text-text">{point.followUp}</dd>
            </div>
          </>
        ) : null}
      </dl>
    </div>
  );
}

function TimeRailGap({
  gap,
  maxAttempts,
  renewalScheduled,
  placeTooltipBelow,
  timeZone,
  turns,
  synthesized,
}: {
  gap: CacheKeepaliveGap;
  maxAttempts: number;
  renewalScheduled: boolean;
  placeTooltipBelow: boolean;
  timeZone: string;
  turns: Array<CacheKeepaliveTurn | undefined>;
  synthesized: boolean;
}) {
  const [hoveredId, setHoveredId] = useState<string | null>(null);
  const [focusedId, setFocusedId] = useState<string | null>(null);
  const [pinnedId, setPinnedId] = useState<string | null>(null);
  const railRef = useRef<HTMLDivElement>(null);
  const tooltipId = useId();
  const count = gap.pulses.length;
  const points = buildRailPoints(
    gap,
    maxAttempts,
    timeZone,
    turns,
    synthesized,
  );
  const activeId = pinnedId ?? hoveredId ?? focusedId;
  const activePoint = points.find((point) => point.id === activeId) ?? null;

  useEffect(() => {
    if (!activeId) return;
    const dismiss = () => {
      setPinnedId(null);
      setHoveredId(null);
      setFocusedId(null);
    };
    const onPointerDown = (event: PointerEvent) => {
      const target = event.target;
      if (
        !(target instanceof Element) ||
        !railRef.current?.contains(target) ||
        !target.closest('[data-rail-point], [role="tooltip"]')
      ) {
        dismiss();
      }
    };
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key !== 'Escape') return;
      event.preventDefault();
      event.stopPropagation();
      dismiss();
    };
    document.addEventListener('pointerdown', onPointerDown, true);
    document.addEventListener('keydown', onKeyDown, true);
    return () => {
      document.removeEventListener('pointerdown', onPointerDown, true);
      document.removeEventListener('keydown', onKeyDown, true);
    };
  }, [activeId]);

  const outcome =
    gap.ending_kind === 'followup_hit'
      ? `Next message ${railTime(gap.ending_at_ms, timeZone, gap.pause_at_ms)}`
      : gap.ending_kind === 'followup_pending'
        ? 'Follow-up pending'
        : gap.ending_kind === 'scheduled_waiting'
          ? 'Waiting'
          : gap.ending_kind === 'followup_unknown'
            ? `Follow-up unknown (${railTime(gap.ending_at_ms, timeZone, gap.pause_at_ms)})`
            : `No follow-up (${railTime(gap.ending_at_ms, timeZone, gap.pause_at_ms)})`;

  return (
    <div className="space-y-2 rounded-sm bg-overlay-2 px-3 py-3 tabular-nums">
      <div className="flex flex-wrap items-baseline justify-between gap-x-3 gap-y-1 text-body-sm">
        <div className="flex flex-wrap items-baseline gap-x-1.5">
          <span className="text-caption text-text-muted">
            {railTime(gap.pause_at_ms, timeZone)}
          </span>
          <span className="font-medium text-text">Paused</span>
          {gap.cached_tokens > 0 ? (
            <span className="text-caption text-text-faint">
              {compactTokens(gap.cached_tokens)} cached
            </span>
          ) : null}
        </div>
        <div className="flex flex-wrap items-baseline gap-x-2">
          <span className="text-caption text-text-muted">{outcome}</span>
          <span
            className={cx(
              'whitespace-nowrap font-medium',
              pnlTone(gap.net_pnl_usd),
            )}
          >
            {fmtUsdSigned(gap.net_pnl_usd)}
          </span>
        </div>
      </div>

      <div
        ref={railRef}
        className="relative flex h-6 items-center max-md:h-8"
        onMouseLeave={() => setHoveredId(null)}
      >
        <div className="h-1.5 w-full rounded-xs bg-overlay-4" />
        {points.map((point, index) => {
          const active = activeId === point.id;
          const nearestDistance = Math.min(
            index > 0 ? point.pct - points[index - 1].pct : 100,
            index < points.length - 1 ? points[index + 1].pct - point.pct : 100,
          );
          return (
            <button
              key={point.id}
              type="button"
              data-rail-point={point.id}
              aria-label={`${point.title}: ${point.badgeText}, Timestamp ${point.absTime}, Since last request ${point.sinceUserReq}, Since last renewal ${point.sinceLastRenewal}${point.kind === 'pulse' ? `, Spent ${point.spendEstimated ? 'approximately ' : ''}${fmtUsdPlain(point.spent ?? null)}, Net ${fmtUsdSigned(point.net ?? null)}, Follow-up ${point.followUp}` : ''}`}
              aria-pressed={pinnedId === point.id}
              aria-describedby={active ? tooltipId : undefined}
              style={
                {
                  left: `${point.pct}%`,
                  '--rail-hit-width': `${nearestDistance}%`,
                } as CSSProperties
              }
              onPointerEnter={(event) => {
                if (event.pointerType === 'mouse') setHoveredId(point.id);
              }}
              onFocus={() => setFocusedId(point.id)}
              onBlur={() =>
                setFocusedId((prev) => (prev === point.id ? null : prev))
              }
              onClick={() => {
                setPinnedId((prev) => (prev === point.id ? null : point.id));
                setHoveredId(null);
                setFocusedId(null);
              }}
              className="absolute flex h-6 w-6 -translate-x-1/2 items-center justify-center rounded-sm focus-visible:outline-2 focus-visible:outline-accent max-md:h-8 max-md:w-[clamp(1.5rem,var(--rail-hit-width),2rem)]"
            >
              {point.kind === 'pulse' ? (
                <span
                  className={cx(
                    'h-3.5 w-1.5 rounded-xs transition-transform',
                    point.hit === false ? 'bg-warn' : 'bg-accent',
                    active && 'scale-125',
                    pinnedId === point.id &&
                      'ring-2 ring-subtle-strong ring-offset-2 ring-offset-bg-sub',
                  )}
                />
              ) : (
                <span
                  className={cx(
                    'h-3 w-3 rounded-full border-2 transition-transform',
                    point.kind === 'start'
                      ? 'border-subtle-strong bg-panel'
                      : point.kind === 'end_hit'
                        ? 'border-traffic-success-text bg-traffic-success-text'
                        : point.kind === 'end_waiting'
                          ? point.badgeTone === 'neutral'
                            ? 'border-subtle-strong bg-panel'
                            : 'border-accent bg-accent'
                          : point.kind === 'end_unknown'
                            ? 'border-subtle-strong bg-panel'
                            : 'border-subtle-strong bg-bg',
                    active && 'scale-125',
                    pinnedId === point.id &&
                      'ring-2 ring-subtle-strong ring-offset-2 ring-offset-bg-sub',
                  )}
                />
              )}
            </button>
          );
        })}
        {activePoint ? (
          <PointHoverPopover
            point={activePoint}
            placeBelow={placeTooltipBelow}
            id={tooltipId}
          />
        ) : null}
      </div>

      <p className="text-caption text-text-faint">
        {count > 0
          ? `${count} ${count === 1 ? 'renewal' : 'renewals'} · Spent ${fmtUsdPlain(gap.spent_usd)} · ${deltaLabel(gap.total_duration_sec)}`
          : renewalScheduled
            ? `Next renewal at ${plusDeltaLabel(gap.total_duration_sec)}`
            : `No renewals · ${deltaLabel(gap.total_duration_sec)}`}
      </p>
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
        ? session.is_active
          ? 'scheduled_waiting'
          : 'followup_pending'
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

function RenewalTable({
  label,
  turns,
  timeZone,
  cycleStartMs,
}: {
  label: string;
  turns: CacheKeepaliveTurn[];
  timeZone: string;
  cycleStartMs?: number;
}) {
  const ordered = [...turns].sort((a, b) => a.time_ms - b.time_ms);
  return (
    <div className="min-w-0 overflow-x-auto tabular-nums">
      <Table aria-label={`${label} renewals`}>
        <caption className="pb-2 text-left text-label text-text-muted">
          {label}
        </caption>
        <TableHead sticky={false}>
          <tr>
            <TableHeadCell>Renewal</TableHeadCell>
            <TableHeadCell>Time</TableHeadCell>
            <TableHeadCell>Follow-up</TableHeadCell>
            <TableHeadCell numeric>Net</TableHeadCell>
          </tr>
        </TableHead>
        <tbody>
          {ordered.map((turn) => (
            <TableRow key={`${turn.turn_number}-${turn.time_ms}`} dense>
              <TableCell>
                <span className="block">#{turn.turn_number}</span>
                <span
                  className={cx(
                    'text-caption',
                    turn.renewals > 0
                      ? 'text-traffic-success-text'
                      : 'text-warn-text',
                  )}
                >
                  {turn.renewals > 0 ? 'Hit' : 'Miss'}
                </span>
              </TableCell>
              <TableCell>
                <time
                  dateTime={new Date(turn.time_ms).toISOString()}
                  title={fullTimestamp(turn.time_ms, timeZone)}
                >
                  {railTime(
                    turn.time_ms,
                    timeZone,
                    cycleStartMs ?? ordered[0]?.time_ms,
                  )}
                </time>
              </TableCell>
              <TableCell>{followUpLabel(turn)}</TableCell>
              <TableCell numeric className={pnlTone(turn.pnl)}>
                {fmtUsdSigned(turn.pnl)}
              </TableCell>
            </TableRow>
          ))}
        </tbody>
      </Table>
    </div>
  );
}

export function SessionDetailPane({
  principalId,
  sessionId,
  horizon,
  rowFingerprint,
  rowLifecycle,
  onClose,
}: Props) {
  const query = useCacheKeepaliveSessionDetail(
    principalId,
    sessionId,
    horizon ?? 'all',
    rowFingerprint,
    rowLifecycle,
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
        <Button
          variant="ghost"
          size="sm"
          className="m-3 mb-0 max-md:min-h-11 md:hidden"
          iconLeft={<ArrowLeft strokeWidth={1.75} />}
          onClick={onClose}
        >
          Back to sessions
        </Button>
        <div className="z-10 flex items-center justify-between border-b border-row bg-bg-sub px-4 py-3 md:sticky md:top-0">
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
            <Button
              variant="ghost"
              size="sm"
              className="max-md:min-h-11"
              iconLeft={<ArrowLeft strokeWidth={1.75} />}
              onClick={onClose}
            >
              Back to sessions
            </Button>
          </div>
        </div>
      </div>
    );
  }

  const session = query.data;
  const isError = session.error != null;
  const synthesized = !session.gaps?.length;
  const railGaps =
    session.ttl === '12h'
      ? []
      : session.gaps && session.gaps.length > 0
        ? session.gaps
        : synthesizeGapsFromTurns(session);
  const railTurns = matchGapTurns(railGaps, session.turns, synthesized);
  const mappedTurns = new Set(
    railTurns.flat().filter((turn) => turn !== undefined),
  );
  const unmappedTurns = session.turns.filter((turn) => !mappedTurns.has(turn));
  const cycleLabels = new Map<string, string>();
  for (const gap of railGaps) {
    if (gap.cycle_id !== null && !cycleLabels.has(gap.cycle_id)) {
      cycleLabels.set(gap.cycle_id, `Cycle ${cycleLabels.size + 1}`);
    }
  }
  const cycleTurns = new Map<string | null, CacheKeepaliveTurn[]>();
  for (const turn of [...session.turns].sort((a, b) => a.time_ms - b.time_ms)) {
    const cycleId = turn.cycle_id ?? null;
    if (cycleId !== null && !cycleLabels.has(cycleId)) {
      cycleLabels.set(cycleId, `Cycle ${cycleLabels.size + 1}`);
    }
    const turns = cycleTurns.get(cycleId) ?? [];
    turns.push(turn);
    cycleTurns.set(cycleId, turns);
  }
  const tableCycles = [...cycleLabels.keys(), null].filter((id) =>
    cycleTurns.has(id),
  );
  const lifecycleLabel = sessionStateDisplay(session.state).label;
  const outcomes = session.follow_up_outcomes;
  const followUpSummary = [
    outcomes.observed > 0 ? `${outcomes.observed} observed` : null,
    outcomes.no_follow_up > 0 ? `${outcomes.no_follow_up} no follow-up` : null,
    outcomes.unknown > 0 ? `${outcomes.unknown} unknown` : null,
    outcomes.pending > 0 ? `${outcomes.pending} pending` : null,
  ]
    .filter(Boolean)
    .join(' · ');
  const followUpAllCounts = `${outcomes.observed} observed · ${outcomes.no_follow_up} no follow-up · ${outcomes.unknown} unknown · ${outcomes.pending} pending`;

  return (
    <div
      className="flex-1 min-w-0"
      data-testid="session-detail-content"
      role="region"
      aria-label="Session detail"
      aria-busy="false"
    >
      <Button
        variant="ghost"
        size="sm"
        className="m-3 mb-0 max-md:min-h-11 md:hidden"
        iconLeft={<ArrowLeft strokeWidth={1.75} />}
        onClick={onClose}
      >
        Back to sessions
      </Button>

      <div className="z-10 space-y-2 border-b border-row bg-bg-sub px-4 py-3 text-caption tabular-nums md:sticky md:top-0">
        <div className="flex flex-wrap items-baseline gap-x-2 gap-y-1">
          <span
            className="break-all font-mono text-data text-text"
            title={session.id}
          >
            {shortThreadDisplay(session.id)}
          </span>
          <span className="text-text-muted">{lifecycleLabel}</span>
          <span className="text-text-faint">
            Last request{' '}
            <RelativeTime ts={session.last_message_at_ms} compact />
          </span>
        </div>
        <div className="flex flex-wrap items-baseline gap-x-3 gap-y-1">
          <span
            className={cx(
              'text-title-section font-medium',
              pnlTone(session.net_pnl),
            )}
          >
            Net {fmtUsdSigned(session.net_pnl)}
          </span>
          <span className="text-text-muted">
            Saved {fmtUsdPlain(session.total_avoided)} · Spent{' '}
            {fmtUsdPlain(session.total_spent)}
          </span>
        </div>
        <p
          className="text-text-faint"
          aria-label="Follow-up outcomes"
          title={followUpAllCounts}
        >
          <span aria-hidden="true">
            {followUpSummary
              ? `Follow-up: ${followUpSummary}`
              : 'No follow-up evidence yet'}
          </span>
          <span className="sr-only">{followUpAllCounts}</span>
        </p>
      </div>

      <div className="space-y-3 p-4">
        {session.state === 'in_request' ? (
          <p className="rounded-sm bg-overlay-2 px-3.5 py-3 text-body-sm text-text-muted">
            A real request is in progress. Keepalive renewals are paused until
            it completes.
          </p>
        ) : null}
        {session.state === 'cancelled' && session.cancel_reason ? (
          <p className="rounded-sm bg-overlay-2 px-3.5 py-3 text-body-sm text-text-muted">
            {session.reason}
          </p>
        ) : null}
        {session.state === 'expired' ? (
          <p className="rounded-sm bg-overlay-2 px-3.5 py-3 text-body-sm text-text-muted">
            {session.reason}
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
            <AlertTriangle
              className="mt-0.5 h-4 w-4 shrink-0"
              strokeWidth={1.75}
            />
            <span>{session.error}</span>
          </div>
        ) : null}

        {railGaps.length > 0 ? (
          <div
            key={sessionId}
            className="space-y-2.5"
            data-testid="session-detail-time-rail"
          >
            {railGaps.map((gap, index) => (
              <section
                key={gap.cycle_id ?? `${gap.pause_at_ms}-${index}`}
                aria-label={`${gap.cycle_id ? cycleLabels.get(gap.cycle_id) : 'Unknown cycle'} timeline`}
              >
                <h3 className="mb-2 text-label text-text-muted">
                  {gap.cycle_id
                    ? cycleLabels.get(gap.cycle_id)
                    : 'Unknown cycle'}
                </h3>
                <TimeRailGap
                  gap={gap}
                  turns={railTurns[index]}
                  synthesized={synthesized}
                  maxAttempts={session.max_attempts}
                  renewalScheduled={
                    session.is_active &&
                    (session.state === 'scheduled' ||
                      session.state === 'renewed') &&
                    gap.ending_kind === 'scheduled_waiting'
                  }
                  placeTooltipBelow={index === 0}
                  timeZone={timeZone}
                />
              </section>
            ))}
          </div>
        ) : session.state !== 'cancelled' &&
          session.state !== 'in_request' &&
          session.state !== 'expired' ? (
          <div className="rounded-sm bg-overlay-2 px-3.5 py-3 text-body-sm text-text-muted">
            {session.reason}
          </div>
        ) : null}
        {railGaps.length === 0 && tableCycles.length > 0 ? (
          <section aria-label="Renewals by cycle" className="space-y-4">
            {tableCycles.length > 1 ? (
              <h3 className="text-title-card text-text">Renewals</h3>
            ) : null}
            {tableCycles.map((cycleId) => (
              <RenewalTable
                key={cycleId ?? 'unknown-cycle'}
                label={
                  cycleId === null
                    ? 'Unknown cycle'
                    : (cycleLabels.get(cycleId) ?? 'Unknown cycle')
                }
                turns={cycleTurns.get(cycleId) ?? []}
                timeZone={timeZone}
                cycleStartMs={
                  cycleId === null
                    ? undefined
                    : railGaps.find((gap) => gap.cycle_id === cycleId)
                        ?.pause_at_ms
                }
              />
            ))}
          </section>
        ) : null}
        {railGaps.length > 0 && unmappedTurns.length > 0 ? (
          <section
            aria-label="Additional renewal evidence"
            className="space-y-2 tabular-nums"
          >
            <h3 className="text-label text-text-muted">Additional evidence</h3>
            <p className="text-caption text-text-faint">
              These records could not be matched to a timeline renewal.
            </p>
            <ul className="divide-y divide-row text-caption">
              {unmappedTurns.map((turn) => (
                <li
                  key={`${turn.cycle_id}-${turn.turn_number}-${turn.time_ms}`}
                  className="flex flex-wrap items-baseline gap-x-3 gap-y-1 py-2"
                >
                  <span className="text-text">Renewal #{turn.turn_number}</span>
                  <span
                    className={
                      turn.renewals > 0
                        ? 'text-traffic-success-text'
                        : 'text-warn-text'
                    }
                  >
                    {turn.renewals > 0 ? 'Hit' : 'Miss'}
                  </span>
                  <time
                    dateTime={new Date(turn.time_ms).toISOString()}
                    className="text-text-muted"
                  >
                    {fullTimestamp(turn.time_ms, timeZone)}
                  </time>
                  <span className="text-text-muted">
                    Follow-up: {followUpLabel(turn)}
                  </span>
                  <span className={pnlTone(turn.pnl)}>
                    Net {fmtUsdSigned(turn.pnl)}
                  </span>
                </li>
              ))}
            </ul>
          </section>
        ) : null}
      </div>
    </div>
  );
}
