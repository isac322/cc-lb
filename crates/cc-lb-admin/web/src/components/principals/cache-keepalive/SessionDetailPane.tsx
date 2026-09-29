import { AlertTriangle, ChevronLeft, ChevronRight, Lock } from 'lucide-react';
import { type ReactNode, useState } from 'react';
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
  ['Total renewals', 'w-8'],
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

export function SessionDetailPane({ principalId, sessionId, onClose }: Props) {
  const query = useCacheKeepaliveSessionDetail(principalId, sessionId);
  const [showConfig, setShowConfig] = useState(false);
  const [showRaw, setShowRaw] = useState(false);

  if (query.isLoading) {
    return <SessionDetailSkeleton onClose={onClose} />;
  }

  if (query.isError || !query.data) {
    return (
      <SessionDetailFrame onClose={onClose} testId="session-detail-error">
        <div
          className="flex flex-1 items-center justify-center p-4 text-sm text-text-muted"
          role="alert"
        >
          Failed to load session detail.
        </div>
      </SessionDetailFrame>
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
      session.net_pnl >= 0 ? 'text-success-text' : 'text-danger-text';
    netPnlSign = session.net_pnl >= 0 ? '+' : '−';
    const abs = Math.abs(session.net_pnl);
    const str = abs.toFixed(4);
    netPnlFormatted = str.replace(/0$/, '');
  }

  const isActiveSession =
    session.state === 'renewed' || session.state === 'scheduled';
  const latestTagLabel = isActiveSession ? 'Current turn · Live' : 'Final turn';
  const latestTagTone: 'neutral' | 'warn' =
    session.state === 'expired' ? 'warn' : 'neutral';

  const STATE_TONE: Record<
    string,
    'ok' | 'warn' | 'danger' | 'neutral' | 'accent'
  > = {
    renewed: 'ok',
    scheduled: 'accent',
    capped: 'neutral',
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

  return (
    <SessionDetailFrame onClose={onClose} testId="session-detail-content">
      {isError && (
        <div className="rounded-sm bg-danger/8 text-danger-text px-3 py-2 flex items-start gap-2 mb-3">
          <AlertTriangle className="w-4 h-4 shrink-0 mt-0.5" />
          <span className="text-body leading-snug">
            {session.error} ·{' '}
            <RelativeTime ts={session.last_message_at_ms} compact />
          </span>
        </div>
      )}

      {/* Overview: readout, state and metadata on the pane ground. */}
      <div className="flex flex-col gap-3">
        <div className="flex justify-between items-start">
          <div className="flex flex-col gap-1">
            <span className="text-label text-text-muted">Net P&L</span>
            <div className={cx('text-display tabular-nums', netPnlColor)}>
              {netPnlSign}${netPnlFormatted}
            </div>
          </div>
          <Badge tone={STATE_TONE[session.state]}>
            {STATE_LABEL[session.state]}
          </Badge>
        </div>
        <p className="text-caption text-text-muted">{pnlBreakdown}</p>

        <div className="h-px bg-row w-full my-1"></div>

        <dl className="grid grid-cols-[100px_minmax(0,1fr)] gap-x-3 gap-y-1.5 text-body-sm">
          <dt className="text-text-muted">Session ID</dt>
          <dd className="text-text font-mono text-data break-all">
            {session.id}
          </dd>

          <dt className="text-text-muted">Upstream</dt>
          <dd className="text-text break-all">{session.upstream ?? '—'}</dd>

          <dt className="text-text-muted">TTL</dt>
          <dd className="text-text">{session.ttl ?? '—'}</dd>

          <dt className="text-text-muted">Generation</dt>
          <dd className="text-text tabular-nums">{session.generation}</dd>

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

          <dt className="text-text-muted">
            {session.state === 'not_tracked'
              ? 'Last seen'
              : session.state === 'expired' || session.state === 'capped'
                ? 'Expired'
                : 'Expires'}
          </dt>
          <dd className="text-text">
            <RelativeTime ts={session.last_message_at_ms} />
          </dd>

          <dt className="text-text-muted">Total renewals</dt>
          <dd className="text-text tabular-nums">{session.total_renewals}</dd>
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
                turnPnlHtml = (
                  <span className="text-warn-text">
                    −${Math.abs(turn.pnl ?? 0).toFixed(3)} pending
                  </span>
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
              } else if (turn.followed_up) {
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
