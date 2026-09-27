import { ChevronRight, X } from 'lucide-react';
import { useMemo, useState } from 'react';
import { formatRelativeUnixSeconds } from '../../../lib/format';
import {
  type Upstream,
  useWarmupAttempts,
  type WarmupAttempt,
  type WarmupAttemptStatus,
} from '../../../lib/queries';
import {
  Button,
  cx,
  Drawer,
  EmptyState,
  IconButton,
  SegmentedControl,
  Skeleton,
  Spinner,
} from '../../ui/primitives';
import { RelativeTime } from '../../ui/RelativeTime';
import { OUTCOME_LABEL, REASON_LABEL } from './parts/copy';
import {
  formatDuration,
  formatFreshnessLine,
  formatResultNarrative,
  OUTCOME_SEVERITY,
  SEVERITY_DOT_CLASS,
} from './parts/warmupViewModel';

const SEVERITY_TEXT_CLASS = {
  ok: 'text-text',
  neutral: 'text-text-muted',
  warn: 'text-warn-text',
  danger: 'text-danger-text',
} as const;
interface Props {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  upstream: Upstream;
}

type Horizon = '24h' | '7d' | 'all';
const HORIZON_SECS: Record<Exclude<Horizon, 'all'>, number> = {
  '24h': 86400,
  '7d': 7 * 86400,
};

const FILTERS: { key: WarmupAttemptStatus | 'all'; label: string }[] = [
  { key: 'all', label: 'All' },
  { key: 'success', label: OUTCOME_LABEL.success },
  { key: 'transient_failure', label: OUTCOME_LABEL.transient_failure },
  { key: 'permanent_failure', label: OUTCOME_LABEL.permanent_failure },
  { key: 'skipped', label: OUTCOME_LABEL.skipped },
];

const ATTEMPT_SKELETON_ROWS = [0, 1, 2, 3, 4] as const;

function OverviewStrip({
  attempts,
  horizon,
  isPending,
}: {
  attempts: WarmupAttempt[];
  horizon: Horizon;
  isPending: boolean;
}) {
  const now = Math.floor(Date.now() / 1000);
  const cutoff = horizon === 'all' ? 0 : now - HORIZON_SECS[horizon];
  const inWindow = attempts.filter((a) => a.attempted_at_unix_secs >= cutoff);
  const counts = {
    success: 0,
    transient_failure: 0,
    permanent_failure: 0,
    skipped: 0,
  };
  for (const a of inWindow) counts[a.status]++;
  const total = inWindow.length;
  const failed = counts.transient_failure + counts.permanent_failure;

  // Dominant failure reason.
  const reasonCounts = new Map<string, number>();
  for (const a of inWindow) {
    if (a.status === 'permanent_failure' || a.status === 'transient_failure') {
      const r = a.reason ?? 'unknown';
      reasonCounts.set(r, (reasonCounts.get(r) ?? 0) + 1);
    }
  }
  let dominantReason: string | null = null;
  let maxCount = 0;
  for (const [k, v] of reasonCounts) {
    if (v > maxCount) {
      maxCount = v;
      dominantReason = REASON_LABEL[k as keyof typeof REASON_LABEL] ?? k;
    }
  }

  return (
    <div className="px-4 py-3 border-b border-subtle shrink-0">
      <div className="flex min-h-5 items-baseline gap-2 mb-1 flex-wrap">
        <span className="text-body-sm text-text-muted">
          Last {horizon === 'all' ? 'all-time' : horizon}:
        </span>
        {isPending ? (
          <>
            <Skeleton className="h-4 w-20 self-center" />
            <Skeleton className="h-3 w-40 self-center" />
          </>
        ) : (
          <>
            <span className="text-body-sm font-medium tabular-nums text-text">
              {total} attempts
            </span>
            <span className="text-caption tabular-nums text-text-faint">
              · {counts.success} success · {failed} failed · {counts.skipped}{' '}
              skipped
            </span>
          </>
        )}
      </div>
      <div
        className="min-h-4"
        data-testid="warmup-history-dominant-failure-slot"
      >
        {isPending ? (
          <Skeleton className="h-3 w-48" />
        ) : dominantReason && failed > 0 ? (
          <p className="text-caption text-text-muted">
            Most common failure:{' '}
            <span className="text-text">{dominantReason}</span> ({maxCount}×)
          </p>
        ) : null}
      </div>
    </div>
  );
}

const HORIZON_OPTIONS: { value: Horizon; label: string }[] = [
  { value: '24h', label: '24h' },
  { value: '7d', label: '7d' },
  { value: 'all', label: 'All' },
];

function AttemptListSkeleton() {
  return (
    <ul
      aria-hidden="true"
      className="flex flex-col gap-1"
      data-testid="warmup-attempt-skeleton-list"
    >
      {ATTEMPT_SKELETON_ROWS.map((row) => (
        <li data-testid="warmup-attempt-skeleton-row" key={row}>
          <div className="w-full rounded-sm px-2.5 py-1.5">
            <div className="flex min-h-5 items-center gap-2 flex-wrap leading-tight">
              <Skeleton className="h-1.5 w-1.5 shrink-0 rounded-full" />
              <Skeleton className="h-3 w-16" />
              <Skeleton className="h-5 w-16 rounded-sm" />
              <Skeleton className="h-3 w-12" />
            </div>
            <Skeleton className="mt-0.5 h-4 w-3/4" />
          </div>
        </li>
      ))}
    </ul>
  );
}

export function WarmupHistoryDrawer({ open, onOpenChange, upstream }: Props) {
  const [filter, setFilter] = useState<WarmupAttemptStatus | 'all'>('all');
  const [horizon, setHorizon] = useState<Horizon>('24h');
  const [selected, setSelected] = useState<WarmupAttempt | null>(null);

  const query = useWarmupAttempts(
    upstream.id,
    {
      status: filter === 'all' ? null : filter,
      limit: 50,
    },
    open,
  );
  const attemptsPending = query.isPending;

  const allAttempts = useMemo(() => {
    return query.data?.pages.flatMap((p) => p.attempts ?? []) ?? [];
  }, [query.data]);

  const handleOpenChange = (o: boolean) => {
    if (!o) setSelected(null);
    onOpenChange(o);
  };

  return (
    <Drawer
      open={open}
      onOpenChange={handleOpenChange}
      title="Warm-up history"
      description={upstream.name}
      width="xl"
    >
      <div data-testid="warmup-history-drawer" className="flex h-full flex-col">
        <div className="flex shrink-0 items-center justify-between gap-3 border-b border-subtle px-4 py-2">
          <span className="text-body-sm text-text-muted">Range</span>
          <SegmentedControl
            ariaLabel="History range"
            size="sm"
            value={horizon}
            onChange={setHorizon}
            options={HORIZON_OPTIONS}
          />
        </div>

        <OverviewStrip
          attempts={allAttempts}
          horizon={horizon}
          isPending={attemptsPending}
        />

        <div className="px-4 py-2 border-b border-subtle shrink-0 flex flex-wrap gap-1">
          {FILTERS.map((f) => (
            <button
              key={f.key}
              type="button"
              aria-pressed={filter === f.key}
              onClick={() => setFilter(f.key)}
              className={cx(
                'inline-flex items-center gap-1.5 px-2 h-8 md:h-7 rounded-sm text-xs font-medium transition-colors',
                'focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-1',
                filter === f.key
                  ? 'bg-selected text-text'
                  : 'text-text-muted hover:bg-hover-bg hover:text-text',
              )}
            >
              {f.key !== 'all' && (
                <span
                  className={cx(
                    'status-dot',
                    SEVERITY_DOT_CLASS[OUTCOME_SEVERITY[f.key]],
                  )}
                />
              )}
              {f.label}
            </button>
          ))}
        </div>

        <div className="flex-1 flex min-h-0">
          <div
            className={cx(
              'overflow-y-auto p-2',
              selected ? 'w-[44%] shrink-0 border-r border-subtle' : 'flex-1',
            )}
          >
            {attemptsPending ? (
              <AttemptListSkeleton />
            ) : allAttempts.length === 0 ? (
              <EmptyState
                title="No matching attempts"
                description="Try a different filter."
              />
            ) : (
              <ul className="flex flex-col gap-1">
                {allAttempts.map((a) => (
                  <AttemptListRow
                    key={a.id}
                    attempt={a}
                    isSelected={selected?.id === a.id}
                    onSelect={() => setSelected(a)}
                  />
                ))}
              </ul>
            )}
            {query.hasNextPage && (
              <div className="flex justify-center pt-3 pb-4">
                <Button
                  variant="secondary"
                  size="sm"
                  onClick={() => query.fetchNextPage()}
                  disabled={query.isFetchingNextPage}
                  iconLeft={query.isFetchingNextPage ? <Spinner /> : undefined}
                >
                  Load older
                </Button>
              </div>
            )}
          </div>

          {selected && (
            <div className="flex-1 min-w-0 overflow-y-auto">
              <AttemptDetail
                key={selected.id}
                attempt={selected}
                onClose={() => setSelected(null)}
              />
            </div>
          )}
        </div>
      </div>
    </Drawer>
  );
}

function AttemptListRow({
  attempt,
  isSelected,
  onSelect,
}: {
  attempt: WarmupAttempt;
  isSelected: boolean;
  onSelect: () => void;
}) {
  return (
    <li>
      <button
        type="button"
        onClick={onSelect}
        className={cx(
          'w-full text-left rounded-sm px-2.5 py-1.5 transition-colors focus-visible:outline-2 focus-visible:outline-accent focus-visible:-outline-offset-2',
          isSelected ? 'bg-selected' : 'hover:bg-overlay-2',
        )}
        aria-current={isSelected ? 'true' : undefined}
      >
        <div className="flex items-center gap-2 flex-wrap leading-tight">
          <span
            className={cx(
              'status-dot shrink-0',
              SEVERITY_DOT_CLASS[OUTCOME_SEVERITY[attempt.status]],
            )}
            aria-hidden
          />
          <span
            className={cx(
              'text-body-sm font-medium',
              SEVERITY_TEXT_CLASS[OUTCOME_SEVERITY[attempt.status]],
            )}
          >
            {OUTCOME_LABEL[attempt.status]}
          </span>
          <span className="text-caption text-text-faint">
            <RelativeTime
              ts={formatRelativeUnixSeconds(attempt.attempted_at_unix_secs)}
            />
          </span>
          {attempt.trigger === 'manual' && (
            <span className="text-caption text-text-faint">· Manual</span>
          )}
          {attempt.http_status != null && (
            <span className="text-caption tabular-nums text-text-faint">
              · HTTP {attempt.http_status}
            </span>
          )}
        </div>
        <p className="text-caption text-text-muted mt-0.5 truncate pl-3.5">
          {attempt.reason
            ? REASON_LABEL[attempt.reason]
            : attempt.status === 'success'
              ? 'Cycle key advanced — fresh 5h window'
              : '—'}
        </p>
      </button>
    </li>
  );
}

function AttemptDetail({
  attempt,
  onClose,
}: {
  attempt: WarmupAttempt;
  onClose: () => void;
}) {
  const [rawOpen, setRawOpen] = useState(false);
  const [pluginOpen, setPluginOpen] = useState(false);
  const rawJson = useMemo(() => JSON.stringify(attempt, null, 2), [attempt]);
  const pluginJson = useMemo(
    () =>
      attempt.dialect_plugin_snapshot
        ? JSON.stringify(attempt.dialect_plugin_snapshot, null, 2)
        : null,
    [attempt],
  );

  const completed = attempt.completed_at_unix_secs;
  const duration =
    completed != null ? completed - attempt.attempted_at_unix_secs : null;
  const sched = attempt.scheduled_for_unix_secs;
  const lateBy = sched ? attempt.attempted_at_unix_secs - sched : null;

  return (
    <div className="flex flex-col gap-4 p-4">
      <div className="flex items-center justify-between gap-2 sticky top-0 bg-bg-sub -m-4 mb-0 px-4 py-2 border-b border-subtle z-10">
        <h3 className="text-title-card text-text">Attempt detail</h3>
        <IconButton label="Close attempt detail" onClick={onClose}>
          <X aria-hidden="true" strokeWidth={1.75} />
        </IconButton>
      </div>

      {/* Tier 1: narrative */}
      <div className="space-y-1">
        <p className="text-body text-text">{formatResultNarrative(attempt)}.</p>
        <p className="text-body-sm text-text-muted">
          {formatFreshnessLine(attempt)}
        </p>
        {attempt.error_detail && (
          <p className="mt-2 rounded-sm bg-overlay-2 px-2.5 py-1.5 font-mono text-data break-words text-text-muted">
            {attempt.error_detail}
          </p>
        )}
      </div>

      {/* Tier 2: 5 secondary fields */}
      <dl className="grid grid-cols-[96px_minmax(0,1fr)] items-baseline gap-x-3 gap-y-2 text-body-sm">
        <dt className="text-label text-text-faint">Trigger</dt>
        <dd className="text-text">
          {attempt.trigger === 'manual' ? 'Manual fire-now' : 'Scheduled'}
          {attempt.trigger === 'scheduled' && lateBy != null && lateBy > 60 && (
            <span className="text-text-faint">
              {' '}
              · late by {formatDuration(lateBy)}
            </span>
          )}
        </dd>

        <dt className="text-label text-text-faint">Duration</dt>
        <dd className="text-text">
          {duration != null && duration > 0 ? formatDuration(duration) : '—'}
        </dd>

        <dt className="text-label text-text-faint">Dispatch</dt>
        <dd className="font-mono text-data text-text">
          {attempt.dispatch_kind ?? '—'}
        </dd>

        <dt className="text-label text-text-faint">HTTP</dt>
        <dd className="tabular-nums text-text">{attempt.http_status ?? '—'}</dd>

        <dt className="text-label text-text-faint">Cycle</dt>
        <dd className="text-text font-mono text-data break-all">
          {attempt.cycle_key != null ? attempt.cycle_key : '—'}
          {attempt.expected_cycle_key != null &&
            attempt.cycle_key !== attempt.expected_cycle_key && (
              <span className="text-text-faint">
                {' '}
                (expected {attempt.expected_cycle_key})
              </span>
            )}
        </dd>

        <dt className="text-label text-text-faint">Replica</dt>
        <dd className="text-text font-mono text-data break-all">
          {attempt.replica_id ?? '—'}
          {attempt.lease_holder &&
            attempt.lease_holder !== attempt.replica_id && (
              <span className="text-text-faint">
                {' '}
                · lease held by {attempt.lease_holder}
              </span>
            )}
        </dd>

        <dt className="text-label text-text-faint">Spec revision</dt>
        <dd className="tabular-nums text-text">
          {attempt.upstream_spec_revision ?? '—'}
        </dd>
      </dl>

      {/* Plugin snapshot collapsible */}
      {pluginJson && (
        <div>
          <button
            type="button"
            onClick={() => setPluginOpen((v) => !v)}
            aria-expanded={pluginOpen}
            className="inline-flex items-center gap-1 rounded-sm text-caption text-text-muted transition-colors hover:text-text focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-1"
          >
            <ChevronRight
              className={cx(
                'size-3 transition-transform',
                pluginOpen && 'rotate-90',
              )}
            />
            Shape plugin snapshot at attempt time
          </button>
          {pluginOpen && (
            <pre className="mt-2 max-h-56 overflow-y-auto whitespace-pre-wrap break-all rounded-sm bg-overlay-2 p-2.5 font-mono text-data">
              {pluginJson}
            </pre>
          )}
        </div>
      )}

      {/* Tier 3: raw */}
      <div>
        <button
          type="button"
          onClick={() => setRawOpen((v) => !v)}
          aria-expanded={rawOpen}
          className="inline-flex items-center gap-1 rounded-sm text-caption text-text-muted transition-colors hover:text-text focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-1"
        >
          <ChevronRight
            className={cx(
              'size-3 transition-transform',
              rawOpen && 'rotate-90',
            )}
          />
          Raw attempt record
        </button>
        {rawOpen && (
          <pre className="mt-2 max-h-72 overflow-y-auto whitespace-pre-wrap break-all rounded-sm bg-overlay-2 p-2.5 font-mono text-data">
            {rawJson}
          </pre>
        )}
      </div>
    </div>
  );
}
