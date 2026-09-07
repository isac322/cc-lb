import { Dialog as BaseDialog } from '@base-ui/react/dialog';
import { ChevronRight, X } from 'lucide-react';
import { useMemo, useState } from 'react';
import { formatRelativeUnixSeconds } from '../../../lib/format';
import {
  type Upstream,
  useWarmupAttempts,
  type WarmupAttempt,
  type WarmupAttemptStatus,
} from '../../../lib/queries';
import { Button, cx, EmptyState, Skeleton, Spinner } from '../../ui/primitives';
import { RelativeTime } from '../../ui/RelativeTime';
import { OUTCOME_LABEL, REASON_LABEL } from './parts/copy';
import { WarmupOutcomeBadge } from './parts/WarmupOutcomeBadge';
import {
  formatDuration,
  formatFreshnessLine,
  formatResultNarrative,
  OUTCOME_SEVERITY,
  SEVERITY_DOT_CLASS,
} from './parts/warmupViewModel';

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
    <div className="px-3 py-2.5 border-b border-subtle shrink-0">
      <div className="flex min-h-5 items-baseline gap-2 mb-1 flex-wrap">
        <span className="text-xs text-text-muted">
          Last {horizon === 'all' ? 'all-time' : horizon}:
        </span>
        {isPending ? (
          <>
            <Skeleton className="h-4 w-20 self-center" />
            <Skeleton className="h-3 w-40 self-center" />
          </>
        ) : (
          <>
            <span className="text-sm font-medium text-text">
              {total} attempts
            </span>
            <span className="text-[11px] text-text-faint">
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
          <p className="text-[11px] text-text-muted">
            Most common failure:{' '}
            <span className="text-text">{dominantReason}</span> ({maxCount}×)
          </p>
        ) : null}
      </div>
    </div>
  );
}

function HorizonToggle({
  value,
  onChange,
}: {
  value: Horizon;
  onChange: (h: Horizon) => void;
}) {
  const opts: Horizon[] = ['24h', '7d', 'all'];
  return (
    <div className="flex items-center gap-1 shrink-0">
      {opts.map((o) => (
        <button
          key={o}
          type="button"
          onClick={() => onChange(o)}
          className={cx(
            'h-6 px-2 text-[11px] rounded-sm border',
            value === o
              ? 'bg-accent/15 border-accent/40 text-accent'
              : 'bg-overlay-2 border-subtle text-text-muted hover:bg-overlay-4',
          )}
        >
          {o === 'all' ? 'All' : o}
        </button>
      ))}
    </div>
  );
}

function AttemptListSkeleton() {
  return (
    <ul
      aria-hidden="true"
      className="flex flex-col gap-1"
      data-testid="warmup-attempt-skeleton-list"
    >
      {ATTEMPT_SKELETON_ROWS.map((row) => (
        <li data-testid="warmup-attempt-skeleton-row" key={row}>
          <div className="w-full rounded-sm border border-subtle bg-overlay-1 px-2.5 py-1.5">
            <div className="flex min-h-5 items-center gap-2 flex-wrap leading-tight">
              <Skeleton className="h-2 w-2 shrink-0 rounded-full" />
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
    <BaseDialog.Root onOpenChange={handleOpenChange} open={open}>
      <BaseDialog.Portal>
        <BaseDialog.Backdrop className="fixed inset-0 z-40 bg-drawer-backdrop transition-opacity duration-200 ease-out data-[ending-style]:opacity-0 data-[starting-style]:opacity-0" />
        <BaseDialog.Popup
          className="fixed right-0 top-0 bottom-0 w-full max-w-3xl bg-bg-sub border-l border-subtle z-50 flex flex-col outline-none transition-transform duration-200 ease-out data-[ending-style]:translate-x-full data-[starting-style]:translate-x-full"
          data-testid="warmup-history-drawer"
        >
          <BaseDialog.Title className="sr-only">
            Warm-up history
          </BaseDialog.Title>
          <BaseDialog.Description className="sr-only">
            Full list of warm-up attempts for {upstream.name}.
          </BaseDialog.Description>

          <header className="flex items-center justify-between gap-3 px-3 py-2.5 border-b border-subtle shrink-0">
            <div className="min-w-0 flex items-center gap-3">
              <div className="min-w-0">
                <h3 className="text-sm font-medium text-text">
                  Warm-up history
                </h3>
                <p className="text-[11px] text-text-faint font-mono truncate">
                  {upstream.name}
                </p>
              </div>
              <HorizonToggle value={horizon} onChange={setHorizon} />
            </div>
            <button
              type="button"
              onClick={() => handleOpenChange(false)}
              aria-label="Close history"
              className="inline-flex items-center justify-center w-7 h-7 rounded-sm text-text-muted hover:text-text hover:bg-overlay-5 shrink-0"
            >
              <X className="w-4 h-4" />
            </button>
          </header>

          <OverviewStrip
            attempts={allAttempts}
            horizon={horizon}
            isPending={attemptsPending}
          />

          <div className="px-3 py-2 border-b border-subtle shrink-0 flex flex-wrap gap-1">
            {FILTERS.map((f) => (
              <button
                key={f.key}
                type="button"
                onClick={() => setFilter(f.key)}
                className={cx(
                  'inline-flex items-center gap-1.5 px-2 h-6 rounded-sm text-[11px] transition-colors border',
                  filter === f.key
                    ? 'bg-accent/15 border-accent/40 text-accent'
                    : 'bg-overlay-2 border-subtle text-text-muted hover:bg-overlay-4',
                )}
              >
                {f.key !== 'all' && (
                  <span
                    className={cx(
                      'w-1.5 h-1.5 rounded-full',
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
                    iconLeft={
                      query.isFetchingNextPage ? <Spinner /> : undefined
                    }
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
        </BaseDialog.Popup>
      </BaseDialog.Portal>
    </BaseDialog.Root>
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
          'w-full text-left rounded-sm border px-2.5 py-1.5 transition-colors',
          isSelected
            ? 'bg-accent/10 border-accent/40'
            : 'bg-overlay-1 border-subtle hover:bg-overlay-3',
        )}
      >
        <div className="flex items-center gap-2 flex-wrap leading-tight">
          <span
            className={cx(
              'w-2 h-2 rounded-full shrink-0',
              SEVERITY_DOT_CLASS[OUTCOME_SEVERITY[attempt.status]],
            )}
            aria-hidden
          />
          <span className="text-xs text-text">
            <RelativeTime
              ts={formatRelativeUnixSeconds(attempt.attempted_at_unix_secs)}
            />
          </span>
          <WarmupOutcomeBadge outcome={attempt.status} />
          {attempt.trigger === 'manual' && (
            <span className="text-[10px] uppercase tracking-wider text-accent font-mono">
              manual
            </span>
          )}
          {attempt.http_status != null && (
            <span className="text-[10px] text-text-faint font-mono">
              HTTP {attempt.http_status}
            </span>
          )}
        </div>
        <p className="text-[11px] text-text-muted mt-0.5 truncate">
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
    <div className="flex flex-col gap-3 p-3">
      <div className="flex items-start justify-between gap-2 sticky top-0 bg-bg-sub -m-3 mb-0 px-3 py-2 border-b border-subtle z-10">
        <h4 className="text-sm font-medium text-text">Attempt detail</h4>
        <button
          type="button"
          onClick={onClose}
          className="text-text-faint hover:text-text text-xs inline-flex items-center gap-1"
        >
          Close ▶
        </button>
      </div>

      {/* Tier 1: narrative */}
      <div className="space-y-1">
        <p className="text-sm text-text leading-snug">
          {formatResultNarrative(attempt)}.
        </p>
        <p className="text-[12px] text-text-muted leading-snug">
          {formatFreshnessLine(attempt)}
        </p>
        {attempt.error_detail && (
          <p className="text-[11px] text-text-faint mt-1 font-mono break-words leading-snug bg-bg/40 border border-subtle rounded-sm px-2 py-1">
            {attempt.error_detail}
          </p>
        )}
      </div>

      {/* Tier 2: 5 secondary fields */}
      <dl className="grid grid-cols-[96px_minmax(0,1fr)] gap-x-3 gap-y-1.5 text-[12px]">
        <dt className="text-[10px] uppercase text-text-faint pt-0.5">
          Trigger
        </dt>
        <dd className="text-text">
          {attempt.trigger === 'manual' ? 'Manual fire-now' : 'Scheduled'}
          {attempt.trigger === 'scheduled' && lateBy != null && lateBy > 60 && (
            <span className="text-text-faint">
              {' '}
              · late by {formatDuration(lateBy)}
            </span>
          )}
        </dd>

        <dt className="text-[10px] uppercase text-text-faint pt-0.5">
          Duration
        </dt>
        <dd className="text-text">
          {duration != null && duration > 0 ? formatDuration(duration) : '—'}
        </dd>

        <dt className="text-[10px] uppercase text-text-faint pt-0.5">
          Dispatch
        </dt>
        <dd className="text-text">{attempt.dispatch_kind ?? '—'}</dd>

        <dt className="text-[10px] uppercase text-text-faint pt-0.5">HTTP</dt>
        <dd className="text-text">{attempt.http_status ?? '—'}</dd>

        <dt className="text-[10px] uppercase text-text-faint pt-0.5">Cycle</dt>
        <dd className="text-text font-mono text-[11px] break-all">
          {attempt.cycle_key != null ? attempt.cycle_key : '—'}
          {attempt.expected_cycle_key != null &&
            attempt.cycle_key !== attempt.expected_cycle_key && (
              <span className="text-text-faint">
                {' '}
                (expected {attempt.expected_cycle_key})
              </span>
            )}
        </dd>

        <dt className="text-[10px] uppercase text-text-faint pt-0.5">
          Replica
        </dt>
        <dd className="text-text font-mono text-[11px] break-all">
          {attempt.replica_id ?? '—'}
          {attempt.lease_holder &&
            attempt.lease_holder !== attempt.replica_id && (
              <span className="text-text-faint">
                {' '}
                · lease held by {attempt.lease_holder}
              </span>
            )}
        </dd>

        <dt className="text-[10px] uppercase text-text-faint pt-0.5">
          Spec rev
        </dt>
        <dd className="text-text font-mono text-[11px]">
          {attempt.upstream_spec_revision ?? '—'}
        </dd>
      </dl>

      {/* Plugin snapshot collapsible */}
      {pluginJson && (
        <div>
          <button
            type="button"
            onClick={() => setPluginOpen((v) => !v)}
            className="text-[11px] text-text-faint hover:text-text inline-flex items-center gap-1"
          >
            <ChevronRight
              className={cx(
                'w-3 h-3 transition-transform',
                pluginOpen && 'rotate-90',
              )}
            />
            Shape plugin snapshot at attempt time
          </button>
          {pluginOpen && (
            <pre className="mt-1 text-[11px] font-mono whitespace-pre-wrap break-all bg-bg border border-subtle rounded-sm p-2 max-h-56 overflow-y-auto">
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
          className="text-[11px] text-text-faint hover:text-text inline-flex items-center gap-1"
        >
          <ChevronRight
            className={cx(
              'w-3 h-3 transition-transform',
              rawOpen && 'rotate-90',
            )}
          />
          Raw attempt record
        </button>
        {rawOpen && (
          <pre className="mt-1 text-[11px] font-mono whitespace-pre-wrap break-all bg-bg border border-subtle rounded-sm p-2 max-h-72 overflow-y-auto">
            {rawJson}
          </pre>
        )}
      </div>
    </div>
  );
}
