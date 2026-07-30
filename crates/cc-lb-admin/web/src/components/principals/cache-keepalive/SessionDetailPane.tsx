import { AlertTriangle, ChevronRight, Lock } from 'lucide-react';
import { type ReactNode, useState } from 'react';
import { useCacheKeepaliveSessionDetail } from '../../../lib/queries';
import { Badge, cx, Skeleton } from '../../ui/primitives';
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
  ['Total renewals', 'w-8'],
] as const;

function SessionDetailHeader({ onClose }: Pick<Props, 'onClose'>) {
  return (
    <div className="flex items-start justify-between gap-2 sticky top-0 bg-bg-sub -m-3 mb-0 px-3 py-2 border-b border-subtle z-10">
      <h4 className="text-sm font-medium text-text">Session detail</h4>
      <button
        type="button"
        onClick={onClose}
        className="text-text-faint hover:text-text text-xs inline-flex items-center gap-1"
      >
        <span className="max-[960px]:hidden">Close ▶</span>
        <span className="hidden max-[960px]:inline">◀ Back</span>
      </button>
    </div>
  );
}

function SessionDetailSkeleton({ onClose }: Pick<Props, 'onClose'>) {
  return (
    <div
      className="flex-1 min-w-0 overflow-y-auto"
      data-testid="session-detail-loading"
      role="status"
      aria-busy="true"
      aria-label="Loading session detail"
    >
      <div
        className="flex flex-col gap-3 p-3"
        data-testid="session-detail-loading-content"
      >
        <SessionDetailHeader onClose={onClose} />

        <div
          className="bg-overlay-1 border border-subtle rounded-sm p-3 flex flex-col gap-3"
          data-testid="session-detail-overview-skeleton"
        >
          <div className="flex justify-between items-start">
            <div className="flex flex-col gap-1">
              <span className="text-[10px] uppercase text-text-faint tracking-wider">
                Net P&amp;L
              </span>
              <div className="w-28" aria-hidden="true">
                <Skeleton className="h-8" />
              </div>
            </div>
            <div className="w-20" aria-hidden="true">
              <Skeleton className="h-5" />
            </div>
          </div>
          <Skeleton className="h-4" />

          <div className="h-px bg-subtle w-full my-1"></div>

          <dl
            className="grid grid-cols-[100px_minmax(0,1fr)] gap-x-3 gap-y-1.5 text-[11px]"
            data-testid="session-detail-metadata-skeleton"
          >
            {METADATA_SKELETON_ROWS.map(([label, valueClassName]) => (
              <div key={label ?? 'state-time'} className="contents">
                <dt className="text-text-faint">
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
          <h4 className="text-sm font-medium text-text mb-3">
            Message-by-message
          </h4>
          <div className="flex flex-col gap-2">
            {Array.from({ length: 3 }).map((_, index) => (
              <div
                key={index}
                className={cx(
                  'rounded-sm border border-subtle bg-overlay-1 px-3',
                  index === 0 ? 'py-2.5 relative' : 'py-2 opacity-70',
                )}
                data-testid="session-detail-turn-skeleton"
                aria-hidden="true"
              >
                {index === 0 && (
                  <div className="absolute -top-2.5 right-2 w-20">
                    <Skeleton className="h-4" />
                  </div>
                )}
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
      </div>
    </div>
  );
}

export function SessionDetailPane({ principalId, sessionId, onClose }: Props) {
  const query = useCacheKeepaliveSessionDetail(principalId, sessionId);
  const [showConfig, setShowConfig] = useState(false);
  const [showRaw, setShowRaw] = useState(false);

  if (query.isLoading) {
    return <SessionDetailSkeleton onClose={onClose} />;
  }

  if (query.isError || !query.data) {
    return (
      <div className="flex-1 min-w-0 overflow-y-auto p-4 flex items-center justify-center text-text-muted text-sm">
        Failed to load session detail.
      </div>
    );
  }

  const session = query.data;
  const isError = session.error != null;

  let pnlBreakdown = '';
  if (session.state !== 'not_tracked') {
    pnlBreakdown = `saved $${session.total_avoided.toFixed(3)} in avoided cache re-creation · spent $${session.total_spent.toFixed(3)} on ${session.total_renewals} renewals`;
    if (session.is_last_pending) {
      pnlBreakdown += ` · current turn pending`;
    }
  } else {
    pnlBreakdown = `no renewals fired`;
  }

  let netPnlColor = 'text-text';
  let netPnlSign = '';
  let netPnlFormatted = '0.00';
  if (session.state !== 'not_tracked') {
    netPnlColor =
      session.net_pnl >= 0
        ? 'text-[var(--color-ok)]'
        : 'text-[var(--color-danger)]';
    netPnlSign = session.net_pnl >= 0 ? '+' : '−';
    const abs = Math.abs(session.net_pnl);
    const str = abs.toFixed(4);
    netPnlFormatted = str.replace(/0$/, '');
  }

  const isActiveSession =
    session.state === 'renewed' || session.state === 'scheduled';
  const latestTagLabel = isActiveSession ? 'Current turn · Live' : 'Final turn';
  const latestCardCls = isActiveSession
    ? 'border-accent/40 bg-accent/5'
    : session.state === 'expired'
      ? 'border-[var(--color-warn)]/40 bg-[var(--color-warn)]/5'
      : 'border-text-faint/30 bg-overlay-2';
  const latestTagCls = isActiveSession
    ? 'border-accent/40 text-accent'
    : session.state === 'expired'
      ? 'border-[var(--color-warn)]/40 text-[var(--color-warn)]'
      : 'border-text-faint/40 text-text-muted';

  const STATE_TONE: Record<
    string,
    'ok' | 'warn' | 'danger' | 'neutral' | 'accent' | 'mono'
  > = {
    renewed: 'ok',
    scheduled: 'accent',
    capped: 'neutral',
    expired: 'warn',
    not_tracked: 'mono',
  };

  const STATE_LABEL: Record<string, string> = {
    renewed: 'Renewed',
    scheduled: 'Scheduled',
    capped: 'Capped',
    expired: 'Expired',
    not_tracked: 'Not tracked',
  };

  return (
    <div className="flex-1 min-w-0 overflow-y-auto">
      <div className="flex flex-col gap-3 p-3">
        <SessionDetailHeader onClose={onClose} />

        {isError && (
          <div className="border border-[var(--color-danger)]/30 bg-red-500/10 text-[var(--color-danger)] rounded-sm px-2 py-1.5 flex items-start gap-2 mb-3">
            <AlertTriangle className="w-4 h-4 shrink-0 mt-0.5" />
            <span className="text-sm leading-snug">
              {session.error} ·{' '}
              <RelativeTime ts={session.last_message_at_ms} compact />
            </span>
          </div>
        )}

        {/* Overview Section */}
        <div className="bg-overlay-1 border border-subtle rounded-sm p-3 flex flex-col gap-3">
          <div className="flex justify-between items-start">
            <div className="flex flex-col gap-1">
              <span className="text-[10px] uppercase text-text-faint tracking-wider">
                Net P&L
              </span>
              <div
                className={cx('text-2xl font-medium tabular-nums', netPnlColor)}
              >
                {netPnlSign}${netPnlFormatted}
              </div>
            </div>
            <Badge tone={STATE_TONE[session.state]}>
              {STATE_LABEL[session.state]}
            </Badge>
          </div>
          <p className="text-[11px] text-text-muted leading-relaxed">
            {pnlBreakdown}
          </p>

          <div className="h-px bg-subtle w-full my-1"></div>

          <dl className="grid grid-cols-[100px_minmax(0,1fr)] gap-x-3 gap-y-1.5 text-[11px]">
            <dt className="text-text-faint">Session ID</dt>
            <dd className="text-text font-mono break-all">{session.id}</dd>

            <dt className="text-text-faint">Upstream</dt>
            <dd className="text-text font-mono break-all">
              {session.upstream ?? '-'}
            </dd>

            <dt className="text-text-faint">TTL</dt>
            <dd className="text-text">{session.ttl ?? '-'}</dd>

            <dt className="text-text-faint">Generation</dt>
            <dd className="text-text">{session.generation}</dd>

            <dt className="text-text-faint">First seen</dt>
            <dd className="text-text">
              {session.turns.length > 0 ? (
                <RelativeTime
                  ts={session.turns[session.turns.length - 1].time_ms}
                />
              ) : (
                <RelativeTime ts={session.last_message_at_ms} />
              )}
            </dd>

            <dt className="text-text-faint">
              {session.state === 'not_tracked'
                ? 'Last seen'
                : session.state === 'expired' || session.state === 'capped'
                  ? 'Expired'
                  : 'Expires'}
            </dt>
            <dd className="text-text">
              <RelativeTime ts={session.last_message_at_ms} />
            </dd>

            <dt className="text-text-faint">Total renewals</dt>
            <dd className="text-text">{session.total_renewals}</dd>
          </dl>
        </div>

        {/* Turns Section */}
        <div className="mt-2">
          <h4 className="text-sm font-medium text-text mb-3">
            Message-by-message
          </h4>
          <div className="flex flex-col gap-2">
            {session.turns.length === 0 ? (
              <div className="rounded-sm border border-subtle bg-overlay-1 px-3 py-2 opacity-70">
                <div className="flex items-center justify-between mb-1.5">
                  <span className="text-xs font-medium text-text flex items-center gap-1.5">
                    <Lock className="w-3 h-3 text-text-faint" />
                    <span className="text-text-muted font-normal">
                      · no renewals
                    </span>
                  </span>
                </div>
                <div className="flex justify-between items-end mt-2">
                  <div className="flex flex-col gap-0.5">
                    <span className="text-[11px] text-text-muted">
                      Renewals: <span className="text-text">-</span>
                    </span>
                    <span className="text-[11px] text-text-muted">
                      Status: <span className="text-text">-</span>
                    </span>
                  </div>
                  <div className="text-xs font-medium tabular-nums">
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
                  turnPnlHtml = (
                    <span className="text-amber-400">
                      −${Math.abs(turn.pnl ?? 0).toFixed(3)} pending
                    </span>
                  );
                } else if (turn.pnl === null) {
                  turnPnlHtml = <span className="text-text-muted">-</span>;
                } else if (turn.pnl >= 0) {
                  turnPnlHtml = (
                    <span className="text-[var(--color-ok)]">
                      +${turn.pnl.toFixed(3)}
                    </span>
                  );
                } else {
                  turnPnlHtml = (
                    <span className="text-[var(--color-danger)]">
                      −${Math.abs(turn.pnl).toFixed(3)}
                    </span>
                  );
                }

                let followUpText = '';
                if (turn.pending) {
                  followUpText = 'waiting for follow-up';
                } else if (turn.followed_up) {
                  followUpText = 'cache used by follow-up';
                } else {
                  followUpText = 'no follow-up (loss)';
                }

                if (isNewest) {
                  return (
                    <div
                      key={turnNum}
                      className={cx(
                        'rounded-sm border px-3 py-2.5 relative',
                        latestCardCls,
                      )}
                    >
                      <div
                        className={cx(
                          'absolute -top-2.5 right-2 bg-bg-sub border text-[9px] uppercase tracking-wider px-1.5 py-0.5 rounded-sm font-medium',
                          latestTagCls,
                        )}
                      >
                        {latestTagLabel}
                      </div>
                      <div className="flex items-center justify-between mb-1.5">
                        <span className="text-xs font-medium text-text">
                          Turn {turnNum}{' '}
                          <span className="text-text-muted font-normal">
                            · {turn.label}
                          </span>
                        </span>
                        <span className="text-[11px] font-mono text-text-muted">
                          <RelativeTime ts={turn.time_ms} compact />
                        </span>
                      </div>
                      <div className="flex justify-between items-end mt-2">
                        <div className="flex flex-col gap-0.5">
                          <span className="text-[11px] text-text-muted">
                            Renewals:{' '}
                            <span className="text-text">{turn.renewals}</span>
                          </span>
                          <span className="text-[11px] text-text-muted">
                            Status:{' '}
                            <span className="text-text">{followUpText}</span>
                          </span>
                        </div>
                        <div className="text-sm font-medium tabular-nums">
                          {turnPnlHtml}
                        </div>
                      </div>
                    </div>
                  );
                }

                return (
                  <div
                    key={turnNum}
                    className="rounded-sm border border-subtle bg-overlay-1 px-3 py-2 opacity-70"
                  >
                    <div className="flex items-center justify-between mb-1.5">
                      <span className="text-xs font-medium text-text flex items-center gap-1.5">
                        <Lock className="w-3 h-3 text-text-faint" />
                        Turn {turnNum}{' '}
                        <span className="text-text-muted font-normal">
                          · {turn.label}
                        </span>
                      </span>
                      <span className="text-[11px] font-mono text-text-muted">
                        <RelativeTime ts={turn.time_ms} compact />
                      </span>
                    </div>
                    <div className="flex justify-between items-end mt-2">
                      <div className="flex flex-col gap-0.5">
                        <span className="text-[11px] text-text-muted">
                          Renewals:{' '}
                          <span className="text-text">{turn.renewals}</span>
                        </span>
                        <span className="text-[11px] text-text-muted">
                          Status:{' '}
                          <span className="text-text">{followUpText}</span>
                        </span>
                      </div>
                      <div className="text-xs font-medium tabular-nums">
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
            className="text-[11px] text-text-faint hover:text-text inline-flex items-center gap-1"
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
            <div className="mt-1 bg-bg border border-subtle rounded-sm p-2">
              <dl className="grid grid-cols-[110px_minmax(0,1fr)] gap-x-3 gap-y-1.5 text-[11px]">
                <dt className="text-text-faint">lead 5m</dt>
                <dd className="text-text font-mono">
                  {session.config_snapshot.lead_5m}s
                </dd>
                <dt className="text-text-faint">lead 1h</dt>
                <dd className="text-text font-mono">
                  {session.config_snapshot.lead_1h}s
                </dd>
                <dt className="text-text-faint">max renewals</dt>
                <dd className="text-text font-mono">
                  {session.config_snapshot.max_renewals}
                </dd>
                <dt className="text-text-faint">max duration</dt>
                <dd className="text-text font-mono">
                  {session.config_snapshot.max_duration}s
                </dd>
                <dt className="text-text-faint">snapshot</dt>
                <dd className="text-text font-mono">
                  {session.config_snapshot.snapshot_bytes} bytes
                </dd>
              </dl>
            </div>
          )}
          {showConfig && !session.config_snapshot && (
            <div className="mt-1 bg-bg border border-subtle rounded-sm p-2 text-[11px] text-text-muted">
              No config snapshot available for this session.
            </div>
          )}
        </div>

        <div>
          <button
            type="button"
            onClick={() => setShowRaw(!showRaw)}
            className="text-[11px] text-text-faint hover:text-text inline-flex items-center gap-1"
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
            <pre className="mt-1 text-[11px] font-mono whitespace-pre-wrap break-all bg-bg border border-subtle rounded-sm p-2 max-h-72 overflow-y-auto">
              {JSON.stringify(session.raw_record, null, 2)}
            </pre>
          )}
        </div>
      </div>
    </div>
  );
}
