import { AlertTriangle, ChevronLeft, ChevronRight, Lock } from 'lucide-react';
import { type ReactNode, useEffect, useState } from 'react';
import { ApiError } from '../../../lib/api';
import type { CacheKeepaliveGap } from '../../../lib/cacheKeepaliveApi';
import { useCacheKeepaliveSessionDetail } from '../../../lib/queries';
import { Badge, Button, cx, Skeleton } from '../../ui/primitives';
import { RelativeTime } from '../../ui/RelativeTime';

interface Props {
  principalId: string;
  sessionId: string;
  onClose: () => void;
}

const METADATA_SKELETON_ROWS = [
  ['Session ID', 'w-full max-w-40'],
  ['Upstream', 'w-24'],
  ['TTL', 'w-12'],
  ['Generation', 'w-8'],
  ['First seen', 'w-24'],
  [null, 'w-24'],
  ['Current-cycle renewals', 'w-8'],
] as const;

interface SessionDetailFrameProps extends Pick<Props, 'onClose'> {
  children: ReactNode;
  isLoading?: boolean;
  testId: string;
  contentTestId?: string;
}

function SessionDetailFrame({
  children,
  onClose,
  isLoading = false,
  testId,
  contentTestId,
}: SessionDetailFrameProps) {
  return (
    <div
      className="flex-1 min-w-0 overflow-y-auto"
      data-testid={testId}
      role="region"
      aria-label="Session detail"
      aria-busy={isLoading}
    >
      <div className="flex flex-col gap-3 p-3" data-testid={contentTestId}>
        {isLoading && (
          <span className="sr-only" role="status">
            Loading session detail
          </span>
        )}
        <SessionDetailHeader onClose={onClose} />
        {children}
      </div>
    </div>
  );
}

function SessionDetailHeader({ onClose }: Pick<Props, 'onClose'>) {
  return (
    <div className="flex items-start justify-between gap-2 sticky top-0 bg-bg-sub -m-3 mb-0 px-3 py-2 border-b border-subtle z-10">
      <h3 className="text-title-card text-text">Session detail</h3>
      <Button
        variant="ghost"
        size="sm"
        onClick={onClose}
        aria-label="Back to sessions"
        className="-my-0.5"
      >
        <span className="inline-flex items-center gap-1 max-[960px]:hidden">
          Close
          <ChevronRight strokeWidth={1.75} aria-hidden="true" />
        </span>
        <span className="hidden items-center gap-1 max-[960px]:inline-flex">
          <ChevronLeft strokeWidth={1.75} aria-hidden="true" />
          Back
        </span>
      </Button>
    </div>
  );
}

function SessionDetailSkeleton({ onClose }: Pick<Props, 'onClose'>) {
  return (
    <SessionDetailFrame
      onClose={onClose}
      isLoading
      testId="session-detail-loading"
      contentTestId="session-detail-loading-content"
    >
      <div
        className="flex flex-col gap-3"
        data-testid="session-detail-overview-skeleton"
      >
        <div className="flex justify-between items-start">
          <div className="flex flex-col gap-1">
            <span className="text-label text-text-muted">Net P&amp;L</span>
            <div className="w-28" aria-hidden="true">
              <Skeleton className="h-8" />
            </div>
          </div>
          <div className="w-20" aria-hidden="true">
            <Skeleton className="h-5" />
          </div>
        </div>
        <Skeleton className="h-4" />

        <div className="h-px bg-row w-full my-1"></div>

        <dl
          className="grid grid-cols-[100px_minmax(0,1fr)] gap-x-3 gap-y-1.5 text-body-sm"
          data-testid="session-detail-metadata-skeleton"
        >
          {METADATA_SKELETON_ROWS.map(([label, valueClassName]) => (
            <div key={label ?? 'state-time'} className="contents">
              <dt className="text-text-muted">
                {label ?? (
                  <span className="block w-14" aria-hidden="true">
                    <Skeleton className="h-3" />
                  </span>
                )}
              </dt>
              <dd className={valueClassName} aria-hidden="true">
                <Skeleton className="h-3" />
              </dd>
            </div>
          ))}
        </dl>
      </div>

      <div className="mt-2" data-testid="session-detail-turns-skeleton">
        <h4 className="text-title-card text-text mb-2">Message-by-message</h4>
        <div className="flex flex-col divide-y divide-row">
          {Array.from({ length: 3 }).map((_, index) => (
            <div
              key={index}
              className="py-2.5"
              data-testid="session-detail-turn-skeleton"
              aria-hidden="true"
            >
              <div className="flex items-center justify-between mb-1.5">
                <div className="w-32">
                  <Skeleton className="h-3" />
                </div>
                <div className="w-16">
                  <Skeleton className="h-3" />
                </div>
              </div>
              <div className="flex justify-between items-end mt-2">
                <div className="flex flex-col gap-0.5">
                  <div className="w-20">
                    <Skeleton className="h-3" />
                  </div>
                  <div className="w-36">
                    <Skeleton className="h-3" />
                  </div>
                </div>
                <div className="w-16">
                  <Skeleton className={index === 0 ? 'h-4' : 'h-3'} />
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>

      <div className="mt-4 w-48" aria-hidden="true">
        <Skeleton className="h-4" />
      </div>
      <div className="w-32" aria-hidden="true">
        <Skeleton className="h-4" />
      </div>
    </SessionDetailFrame>
  );
}
function railTime(ms: number): string {
  const date = new Date(ms);
  return [date.getHours(), date.getMinutes(), date.getSeconds()]
    .map((value) => String(value).padStart(2, '0'))
    .join(':');
}

function deltaLabel(sec: number): string {
  const value = Math.max(0, Math.round(sec));
  if (value < 60) return `+${value}s`;
  const minutes = Math.floor(value / 60);
  const seconds = value % 60;
  return seconds === 0
    ? `+${minutes}m`
    : `+${minutes}m ${String(seconds).padStart(2, '0')}s`;
}

function compactTokens(tokens: number): string {
  if (tokens >= 1_000_000) return `${(tokens / 1_000_000).toFixed(1)}M`;
  if (tokens >= 1_000) return `${(tokens / 1_000).toFixed(1)}k`;
  return String(tokens);
}

interface RailPoint {
  id: string;
  pct: number;
  kind: 'start' | 'pulse' | 'end_hit' | 'end_waiting' | 'end_miss';
  title: string;
  badge: string;
  timestamp: string;
  sinceRequest: string;
  sinceRenewal: string;
}

function buildRailPoints(
  gap: CacheKeepaliveGap,
  maxAttempts: number,
): RailPoint[] {
  const maxSec = Math.max(gap.total_duration_sec, 300);
  const points: RailPoint[] = [
    {
      id: 'start',
      pct: 0,
      kind: 'start',
      title: 'Message paused',
      badge: `${compactTokens(gap.cached_tokens)} cached`,
      timestamp: railTime(gap.pause_at_ms),
      sinceRequest: '0s (paused)',
      sinceRenewal: '— (before renewal)',
    },
  ];
  for (const [index, pulse] of gap.pulses.entries()) {
    const previous = gap.pulses[index - 1];
    points.push({
      id: `pulse-${pulse.pulse_number}`,
      pct: Math.min(93, Math.max(7, (pulse.offset_sec / maxSec) * 100)),
      kind: 'pulse',
      title: `Renewal #${pulse.pulse_number}`,
      badge:
        pulse.spent_usd === null
          ? '−$—'
          : `−$${pulse.spent_usd.toFixed(4).replace(/0+$/, '').replace(/\.$/, '')}`,
      timestamp: railTime(pulse.time_ms),
      sinceRequest: deltaLabel(pulse.offset_sec),
      sinceRenewal: previous
        ? `${deltaLabel(pulse.offset_sec - previous.offset_sec)} (after #${previous.pulse_number})`
        : '— (first renewal)',
    });
  }
  const lastPulse = gap.pulses[gap.pulses.length - 1];
  const sinceLastRenewal = lastPulse
    ? `${deltaLabel(gap.total_duration_sec - lastPulse.offset_sec)} (after #${lastPulse.pulse_number})`
    : '— (no renewals)';
  const kind =
    gap.ending_kind === 'followup_hit'
      ? 'end_hit'
      : gap.ending_kind === 'scheduled_waiting'
        ? 'end_waiting'
        : 'end_miss';
  const title =
    gap.ending_kind === 'followup_hit'
      ? 'Next message (cache hit)'
      : gap.ending_kind === 'scheduled_waiting'
        ? 'Waiting for next message'
        : gap.ending_kind === 'capped_no_followup'
          ? gap.pulses.length >= maxAttempts && maxAttempts > 0
            ? `No follow-up (${gap.pulses.length}/${maxAttempts} cap)`
            : 'No follow-up (capped)'
          : 'No follow-up (TTL expired)';
  const net = gap.net_pnl_usd;
  points.push({
    id: 'end',
    pct: 100,
    kind,
    title,
    badge:
      gap.ending_kind === 'scheduled_waiting'
        ? deltaLabel(gap.total_duration_sec)
        : net === null
          ? '$—'
          : `${net >= 0 ? '+' : '−'}$${Math.abs(net).toFixed(4).replace(/0+$/, '').replace(/\.$/, '')}`,
    timestamp: railTime(gap.ending_at_ms),
    sinceRequest: deltaLabel(gap.total_duration_sec),
    sinceRenewal: sinceLastRenewal,
  });
  return points;
}

function RailTooltip({ point, below }: { point: RailPoint; below: boolean }) {
  return (
    <div
      role="tooltip"
      className={cx(
        'pointer-events-none absolute z-30 w-72 rounded-sm border border-subtle-strong bg-panel p-2.5 shadow-overlay tabular-nums',
        below ? 'top-full mt-2' : 'bottom-full mb-2',
        point.pct <= 20
          ? 'left-0'
          : point.pct >= 80
            ? 'right-0'
            : 'left-1/2 -translate-x-1/2',
      )}
      style={
        point.pct > 20 && point.pct < 80 ? { left: `${point.pct}%` } : undefined
      }
    >
      <div className="flex items-baseline justify-between gap-2 border-b border-row pb-1.5 text-caption">
        <span className="font-medium text-text">{point.title}</span>
        <span className="font-mono text-data text-text-muted">
          {point.badge}
        </span>
      </div>
      <dl className="mt-1.5 space-y-1 text-caption">
        <div className="flex items-baseline justify-between gap-2">
          <dt className="text-text-faint">Timestamp</dt>
          <dd className="font-mono text-data text-text">{point.timestamp}</dd>
        </div>
        <div className="flex items-baseline justify-between gap-2">
          <dt className="text-text-faint">Since last request</dt>
          <dd className="font-mono text-data text-text">
            {point.sinceRequest}
          </dd>
        </div>
        <div className="flex items-baseline justify-between gap-2">
          <dt className="text-text-faint">Since last renewal</dt>
          <dd className="font-mono text-data text-text">
            {point.sinceRenewal}
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
}: {
  gap: CacheKeepaliveGap;
  maxAttempts: number;
  placeTooltipBelow: boolean;
}) {
  const [activeId, setActiveId] = useState<string | null>(null);
  const points = buildRailPoints(gap, maxAttempts);
  const activePoint = points.find((point) => point.id === activeId);
  const net = gap.net_pnl_usd;
  const netText =
    net === null
      ? '—'
      : Math.abs(net) < 0.00005
        ? '$0'
        : `${net > 0 ? '+' : '−'}$${Math.abs(net).toFixed(4).replace(/0+$/, '').replace(/\.$/, '')}`;
  const netTone =
    net === null
      ? 'text-text-muted'
      : net > 0
        ? 'text-success-text'
        : net < 0
          ? 'text-warn-text'
          : 'text-text-muted';
  return (
    <div className="space-y-2 rounded-sm bg-overlay-2 px-3.5 py-3 tabular-nums">
      <div className="flex flex-wrap items-baseline justify-between gap-2 text-body-sm">
        <div className="flex items-center gap-1.5">
          <span className="font-mono text-data text-text-muted">
            {railTime(gap.pause_at_ms)}
          </span>
          <span className="font-medium text-text">Paused</span>
          <span className="text-caption text-text-faint">
            ({compactTokens(gap.cached_tokens)})
          </span>
        </div>
        <div className="flex items-center gap-2">
          {gap.ending_kind === 'followup_hit' ? (
            <>
              <span className="text-caption text-text-muted">
                Next msg {railTime(gap.ending_at_ms)} (
                {deltaLabel(gap.total_duration_sec)})
              </span>
              <span className={cx('font-mono text-data font-medium', netTone)}>
                {netText}
              </span>
            </>
          ) : gap.ending_kind === 'scheduled_waiting' ? (
            <span className="text-caption text-text-muted">
              Waiting · {deltaLabel(gap.total_duration_sec)}
            </span>
          ) : (
            <>
              <span className="text-caption text-text-faint">
                No follow-up ({railTime(gap.ending_at_ms)})
              </span>
              <span className={cx('font-mono text-data font-medium', netTone)}>
                {netText}
              </span>
            </>
          )}
        </div>
      </div>
      <div className="relative flex h-6 items-center">
        {activePoint ? (
          <RailTooltip point={activePoint} below={placeTooltipBelow} />
        ) : null}
        <div className="h-1.5 w-full rounded-full bg-overlay-4" />
        {points.map((point) => (
          <button
            key={point.id}
            type="button"
            data-rail-point={point.id}
            aria-label={`${point.title}: Timestamp ${point.timestamp}, Since last request ${point.sinceRequest}, Since last renewal ${point.sinceRenewal}`}
            style={{ left: `${point.pct}%` }}
            onMouseEnter={() => setActiveId(point.id)}
            onMouseLeave={() =>
              setActiveId((current) => (current === point.id ? null : current))
            }
            onFocus={() => setActiveId(point.id)}
            onBlur={() =>
              setActiveId((current) => (current === point.id ? null : current))
            }
            className="group absolute flex h-6 w-6 -translate-x-1/2 items-center justify-center rounded-full focus-visible:outline-2 focus-visible:outline-accent"
          >
            <span
              className={cx(
                point.kind === 'pulse'
                  ? 'h-3.5 w-1.5 rounded-xs bg-accent'
                  : 'h-3 w-3 rounded-full border-2',
                point.kind === 'start'
                  ? 'border-subtle-strong bg-panel'
                  : point.kind === 'end_hit'
                    ? 'border-success-text bg-success-text'
                    : point.kind === 'end_waiting'
                      ? 'border-accent bg-accent'
                      : 'border-subtle-strong bg-bg',
              )}
            />
          </button>
        ))}
      </div>
      <div className="flex items-center justify-between text-caption text-text-faint">
        <span>0s (pause)</span>
        <span>
          {gap.pulses.length} renewal{gap.pulses.length === 1 ? '' : 's'} · −$
          {gap.spent_usd.toFixed(4)}
        </span>
        <span>{deltaLabel(gap.total_duration_sec)}</span>
      </div>
    </div>
  );
}

export function SessionDetailPane({ principalId, sessionId, onClose }: Props) {
  const query = useCacheKeepaliveSessionDetail(principalId, sessionId);
  const [showConfig, setShowConfig] = useState(false);
  const [showRaw, setShowRaw] = useState(false);

  // biome-ignore lint/correctness/useExhaustiveDependencies: reset local disclosure state when the session identity changes
  useEffect(() => {
    setShowConfig(false);
    setShowRaw(false);
  }, [principalId, sessionId]);

  if (query.isLoading) {
    return <SessionDetailSkeleton onClose={onClose} />;
  }

  const isNotFound =
    query.error instanceof ApiError && query.error.status === 404;

  if (isNotFound || !query.data) {
    return (
      <SessionDetailFrame onClose={onClose} testId="session-detail-error">
        <div
          className="flex flex-1 flex-col items-center justify-center gap-3 p-4 text-center"
          role="alert"
        >
          <p className="text-body text-text-muted">
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
      </SessionDetailFrame>
    );
  }

  const session = query.data;
  const isError = session.error != null;

  let pnlBreakdown = '';
  if (session.state !== 'not_tracked') {
    const renewalCountLabel = `${session.total_renewals_current_cycle} current-cycle renewal${session.total_renewals_current_cycle === 1 ? '' : 's'}`;
    const retainedSpend = `spent $${session.total_spent.toFixed(3)} across retained history`;
    pnlBreakdown =
      session.total_avoided === null
        ? `evidence ${session.evidence_status} · ${retainedSpend} · ${renewalCountLabel}`
        : `saved $${session.total_avoided.toFixed(3)} in avoided cache re-creation · ${retainedSpend} · ${renewalCountLabel}`;
    if (session.is_active && session.is_last_pending) {
      pnlBreakdown += ' · current turn pending';
    }
  } else {
    pnlBreakdown = 'no renewals fired';
  }

  let netPnlColor = 'text-text';
  let netPnlSign = '';
  let netPnlFormatted = '0.00';
  if (session.net_pnl !== null) {
    netPnlColor =
      session.net_pnl >= 0 ? 'text-success-text' : 'text-danger-text';
    netPnlSign = session.net_pnl >= 0 ? '+' : '−';
    const abs = Math.abs(session.net_pnl);
    const str = abs.toFixed(4);
    netPnlFormatted = str.replace(/0$/, '');
  }

  const isActiveSession = session.is_active;
  const latestTagLabel = isActiveSession ? 'Current turn · Live' : 'Final turn';
  const latestTagTone: 'ok' | 'neutral' | 'warn' = isActiveSession
    ? 'ok'
    : session.state === 'expired'
      ? 'warn'
      : 'neutral';

  const STATE_TONE: Record<
    string,
    'ok' | 'warn' | 'danger' | 'neutral' | 'accent'
  > = {
    renewed: 'ok',
    scheduled: 'accent',
    capped: 'warn',
    expired: 'warn',
    not_tracked: 'neutral',
  };

  const STATE_LABEL: Record<string, string> = {
    renewed: 'Renewed',
    scheduled: 'Scheduled',
    capped: 'Capped',
    expired: 'Expired',
    not_tracked: 'Not tracked',
  };
  const railGaps = session.gaps ?? [];

  return (
    <SessionDetailFrame onClose={onClose} testId="session-detail-content">
      {query.isError && (
        <div
          className="rounded-sm bg-danger/8 px-3 py-2 text-danger-text"
          role="alert"
        >
          <span className="text-body">
            Refresh failed. Showing the last successful detail.
          </span>{' '}
          <Button variant="ghost" size="sm" onClick={() => query.refetch()}>
            Retry
          </Button>
        </div>
      )}
      {isError && (
        <div className="rounded-sm bg-danger/8 text-danger-text px-3 py-2 flex items-start gap-2 mb-3">
          <AlertTriangle className="w-4 h-4 shrink-0 mt-0.5" />
          <span className="text-body leading-snug">
            {session.error} ·{' '}
            <RelativeTime ts={session.last_message_at_ms} compact />
          </span>
        </div>
      )}
      {railGaps.length > 0 ? (
        <section
          className="mt-3 space-y-2.5"
          data-testid="session-detail-time-rail"
        >
          {railGaps.map((gap, index) => (
            <TimeRailGap
              key={gap.cycle_id ?? `${gap.pause_at_ms}-${index}`}
              gap={gap}
              maxAttempts={session.max_attempts}
              placeTooltipBelow={index === 0}
            />
          ))}
        </section>
      ) : null}

      {/* Overview: readout, state and metadata on the pane ground. */}
      <div className="flex flex-col gap-3">
        <div className="flex justify-between items-start">
          <div className="flex flex-col gap-1">
            <span className="text-label text-text-muted">Net P&amp;L</span>
            <div className={cx('text-display tabular-nums', netPnlColor)}>
              {session.net_pnl === null
                ? '—'
                : `${netPnlSign}$${netPnlFormatted}`}
            </div>
          </div>
          <Badge tone={STATE_TONE[session.state]}>
            {STATE_LABEL[session.state]}
          </Badge>
        </div>
        <p className="text-caption text-text-muted">{pnlBreakdown}</p>

        <div className="h-px bg-row w-full my-1"></div>

        <dl className="grid grid-cols-1 gap-x-6 gap-y-2 text-body-sm sm:grid-cols-2">
          <div className="grid grid-cols-[100px_minmax(0,1fr)] gap-x-3 gap-y-1.5">
            <dt className="text-text-muted">Session ID</dt>
            <dd className="text-text font-mono text-data break-all">
              {session.id}
            </dd>
          </div>
          <div className="grid grid-cols-[100px_minmax(0,1fr)] gap-x-3 gap-y-1.5">
            <dt className="text-text-muted">Upstream</dt>
            <dd className="text-text break-all">{session.upstream ?? '—'}</dd>
          </div>
          <div className="grid grid-cols-[100px_minmax(0,1fr)] gap-x-3 gap-y-1.5">
            <dt className="text-text-muted">TTL</dt>
            <dd className="text-text">{session.ttl ?? '—'}</dd>
          </div>
          <div className="grid grid-cols-[100px_minmax(0,1fr)] gap-x-3 gap-y-1.5">
            <dt className="text-text-muted">Generation</dt>
            <dd className="text-text tabular-nums">{session.generation}</dd>
          </div>
          <div className="grid grid-cols-[100px_minmax(0,1fr)] gap-x-3 gap-y-1.5">
            <dt className="text-text-muted">First seen</dt>
            <dd className="text-text">
              {session.turns.length > 0 ? (
                <RelativeTime
                  ts={session.turns[session.turns.length - 1].time_ms}
                />
              ) : (
                <RelativeTime ts={session.last_message_at_ms} />
              )}
            </dd>
          </div>
          <div className="grid grid-cols-[100px_minmax(0,1fr)] gap-x-3 gap-y-1.5">
            <dt className="text-text-muted">Last message</dt>
            <dd className="text-text">
              <RelativeTime ts={session.last_message_at_ms} />
            </dd>
          </div>
          <div className="grid grid-cols-[100px_minmax(0,1fr)] gap-x-3 gap-y-1.5">
            <dt className="text-text-muted">Current-cycle renewals</dt>
            <dd className="text-text tabular-nums">
              {session.total_renewals_current_cycle}
            </dd>
          </div>
        </dl>
      </div>

      {/* Turns: flat rows on the ground, one 1px line between. */}
      <div className="mt-2">
        <h4 className="text-title-card text-text mb-2">Message-by-message</h4>
        <div className="flex flex-col divide-y divide-row">
          {session.turns.length === 0 ? (
            <div className="py-2.5">
              <div className="flex items-center justify-between mb-1.5">
                <span className="text-body-sm font-medium text-text flex items-center gap-1.5">
                  <Lock className="w-3 h-3 text-text-faint" />
                  <span className="text-text-muted font-normal">
                    · no renewals
                  </span>
                </span>
              </div>
              <div className="flex justify-between items-end mt-2">
                <div className="flex flex-col gap-0.5">
                  <span className="text-caption text-text-muted">
                    Renewals: <span className="text-text">-</span>
                  </span>
                  <span className="text-caption text-text-muted">
                    Status: <span className="text-text">-</span>
                  </span>
                </div>
                <div className="text-body-sm font-medium tabular-nums">
                  <span className="text-text-muted">-</span>
                </div>
              </div>
            </div>
          ) : (
            session.turns.map((turn, idx) => {
              const isNewest = idx === 0;
              const turnNum = turn.turn_number;

              let turnPnlHtml: ReactNode;
              if (turn.pending) {
                const pendingPnl =
                  turn.pnl === null
                    ? '— pending'
                    : `${turn.pnl < 0 ? '−' : '+'}$${Math.abs(turn.pnl).toFixed(3)} pending`;
                turnPnlHtml = (
                  <span className="text-warn-text">{pendingPnl}</span>
                );
              } else if (turn.pnl === null) {
                turnPnlHtml = <span className="text-text-muted">-</span>;
              } else if (turn.pnl >= 0) {
                turnPnlHtml = (
                  <span className="text-success-text">
                    +${turn.pnl.toFixed(3)}
                  </span>
                );
              } else {
                turnPnlHtml = (
                  <span className="text-danger-text">
                    −${Math.abs(turn.pnl).toFixed(3)}
                  </span>
                );
              }

              let followUpText = '';
              if (turn.pending) {
                followUpText = 'waiting for follow-up';
              } else if (
                turn.followed_up === null ||
                turn.evidence_status === 'unknown'
              ) {
                followUpText = 'follow-up unavailable (evidence unknown)';
              } else if (turn.followed_up === true) {
                followUpText = 'cache used by follow-up';
              } else {
                followUpText = 'no follow-up (loss)';
              }

              return (
                <div key={turnNum} className="py-2.5">
                  <div className="flex items-center justify-between gap-2 mb-1.5">
                    <span className="text-body-sm font-medium text-text flex items-center gap-1.5 min-w-0">
                      {isNewest ? null : (
                        <Lock className="w-3 h-3 shrink-0 text-text-faint" />
                      )}
                      <span className="truncate">
                        Turn {turnNum}{' '}
                        <span className="text-text-muted font-normal">
                          · {turn.label}
                        </span>
                      </span>
                      {isNewest ? (
                        <Badge tone={latestTagTone}>{latestTagLabel}</Badge>
                      ) : null}
                    </span>
                    <span className="shrink-0 text-caption tabular-nums text-text-muted">
                      <RelativeTime ts={turn.time_ms} compact />
                    </span>
                  </div>
                  <div className="flex justify-between items-end mt-2">
                    <div className="flex flex-col gap-0.5">
                      <span className="text-caption text-text-muted">
                        Renewals:{' '}
                        <span className="text-text">{turn.renewals}</span>
                      </span>
                      <span className="text-caption text-text-muted">
                        Status:{' '}
                        <span className="text-text">{followUpText}</span>
                      </span>
                    </div>
                    <div className="text-body-sm font-medium tabular-nums">
                      {turnPnlHtml}
                    </div>
                  </div>
                </div>
              );
            })
          )}
        </div>
      </div>

      <div className="mt-4">
        <button
          type="button"
          onClick={() => setShowConfig(!showConfig)}
          className="text-caption text-text-muted hover:text-text inline-flex items-center gap-1"
        >
          <ChevronRight
            className={cx(
              'w-3 h-3 transition-transform',
              showConfig && 'rotate-90',
            )}
          />
          Config in effect at schedule time
        </button>
        {showConfig && session.config_snapshot && (
          <div className="mt-2 pl-4">
            <dl className="grid grid-cols-[110px_minmax(0,1fr)] gap-x-3 gap-y-1.5 text-body-sm">
              <dt className="text-text-muted">Lead time · 5m</dt>
              <dd className="text-text tabular-nums">
                {session.config_snapshot.lead_5m}s
              </dd>
              <dt className="text-text-muted">Lead time · 1h</dt>
              <dd className="text-text tabular-nums">
                {session.config_snapshot.lead_1h}s
              </dd>
              <dt className="text-text-muted">Max renewals</dt>
              <dd className="text-text tabular-nums">
                {session.config_snapshot.max_renewals}
              </dd>
              <dt className="text-text-muted">Max duration</dt>
              <dd className="text-text tabular-nums">
                {session.config_snapshot.max_duration}s
              </dd>
              <dt className="text-text-muted">Snapshot</dt>
              <dd className="text-text tabular-nums">
                {session.config_snapshot.snapshot_bytes} bytes
              </dd>
            </dl>
          </div>
        )}
        {showConfig && !session.config_snapshot && (
          <div className="mt-2 pl-4 text-body-sm text-text-muted">
            No config snapshot available for this session.
          </div>
        )}
      </div>

      <div>
        <button
          type="button"
          onClick={() => setShowRaw(!showRaw)}
          className="text-caption text-text-muted hover:text-text inline-flex items-center gap-1"
        >
          <ChevronRight
            className={cx(
              'w-3 h-3 transition-transform',
              showRaw && 'rotate-90',
            )}
          />
          Raw session record
        </button>
        {showRaw && (
          <pre className="well mt-1.5 p-3 font-mono text-data whitespace-pre-wrap break-all max-h-72 overflow-y-auto">
            {JSON.stringify(session.raw_record, null, 2)}
          </pre>
        )}
      </div>
    </SessionDetailFrame>
  );
}
