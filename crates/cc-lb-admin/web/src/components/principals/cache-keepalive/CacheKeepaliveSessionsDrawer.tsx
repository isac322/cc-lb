import { AlertCircle } from 'lucide-react';
import React, { useMemo, useState } from 'react';
import type {
  CacheKeepaliveHorizon,
  CacheKeepaliveRow,
  CacheKeepaliveState,
  CacheKeepaliveStatusFilter,
  CacheKeepaliveSummary,
} from '../../../lib/cacheKeepaliveApi';
import {
  type Principal,
  useCacheKeepaliveSessions,
} from '../../../lib/queries';
import {
  Badge,
  Button,
  cx,
  Drawer,
  EmptyState,
  SegmentedControl,
  type SegmentedOption,
  Skeleton,
  Spinner,
} from '../../ui/primitives';
import { RelativeTime } from '../../ui/RelativeTime';
import { Select, type SelectOption } from '../../ui/Select';
import { cacheKeepaliveAnimationContract } from './__fixtures__/cacheKeepaliveContract';
import { mergeLiveSessions } from './liveMergeSessions';

import { SessionDetailPane } from './SessionDetailPane';

declare global {
  interface Window {
    PAUSE_ANIMATIONS?: boolean;
  }
}

interface Props {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  principal: Principal;
}

const HORIZON_OPTIONS: SegmentedOption<CacheKeepaliveHorizon>[] = [
  { value: '24h', label: '24h' },
  { value: '7d', label: '7d' },
  { value: 'all', label: 'All' },
];

/** Status filter items; the `''` "All statuses" item comes from `allLabel`. */
const STATUS_OPTIONS: SelectOption[] = [
  { value: 'renewed', label: 'Renewed' },
  { value: 'scheduled', label: 'Scheduled' },
  { value: 'capped', label: 'Capped' },
  { value: 'expired', label: 'Expired' },
  { value: 'not_tracked', label: 'Not tracked' },
  { value: 'error', label: 'Error' },
];

function isStatusFilter(value: string): value is CacheKeepaliveStatusFilter {
  return (
    value === 'all' || STATUS_OPTIONS.some((option) => option.value === value)
  );
}

const STATE_TONE: Record<
  CacheKeepaliveState,
  'ok' | 'warn' | 'danger' | 'neutral' | 'accent'
> = {
  renewed: 'ok',
  scheduled: 'accent',
  capped: 'neutral',
  expired: 'warn',
  not_tracked: 'neutral',
};

const STATE_LABEL: Record<CacheKeepaliveState, string> = {
  renewed: 'Renewed',
  scheduled: 'Scheduled',
  capped: 'Capped',
  expired: 'Expired',
  not_tracked: 'Not tracked',
};

function formatMoney(val: number) {
  return `$${val.toFixed(2)}`;
}

function formatNetPnl(val: number) {
  if (val === 0) return '$0.00';
  const sign = val > 0 ? '+' : '−';
  const abs = Math.abs(val);
  const str = abs.toFixed(4);
  return `${sign}$${str.replace(/0$/, '')}`;
}

function OverviewStrip({
  horizon,
  summary,
  isLoading,
  onHorizonChange,
  showHorizon,
}: {
  horizon: CacheKeepaliveHorizon;
  summary: CacheKeepaliveSummary | undefined;
  isLoading: boolean;
  onHorizonChange: (h: CacheKeepaliveHorizon) => void;
  showHorizon: boolean;
}) {
  const label = horizon === 'all' ? 'All time:' : `Last ${horizon}:`;
  return (
    <div className="px-4 py-3 border-b border-subtle shrink-0 flex items-center justify-between gap-3 flex-wrap">
      <div className="flex items-baseline gap-2 flex-wrap min-w-0">
        <span className="text-caption text-text-muted">{label}</span>
        {isLoading ? (
          <>
            <Skeleton className="h-5 w-32" />
            <Skeleton className="h-4 w-52" />
          </>
        ) : (
          <>
            <span className="text-body-sm font-medium text-text tabular-nums">
              {(summary?.renewals_fired ?? 0).toLocaleString('en-US')} renewals
              fired
            </span>
            <span className="text-caption text-text-faint tabular-nums">
              · {summary ? formatMoney(summary.cost_saved) : '$0.00'} saved ·{' '}
              {(summary?.sessions_last_5m ?? 0).toLocaleString('en-US')}{' '}
              sessions tracked
            </span>
          </>
        )}
      </div>
      {showHorizon ? (
        <SegmentedControl
          ariaLabel="Time range"
          size="sm"
          value={horizon}
          onChange={onHorizonChange}
          options={HORIZON_OPTIONS}
        />
      ) : null}
    </div>
  );
}

function SessionListRow({
  row,
  isSelected,
  onSelect,
}: {
  row: CacheKeepaliveRow;
  isSelected: boolean;
  onSelect: () => void;
}) {
  const isError = row.error != null;
  const showTicks = row.state !== 'scheduled' && row.state !== 'not_tracked';
  const tickColor = row.state === 'renewed' ? 'bg-ok' : 'bg-text-muted';

  const reasonText =
    isError && row.error === row.reason
      ? row.error
      : isError
        ? `${row.error} · ${row.reason}`
        : row.reason;

  return (
    <li data-key={row.id}>
      <button
        type="button"
        onClick={onSelect}
        aria-current={isSelected || undefined}
        className={cx(
          'w-full min-h-[68px] text-left rounded-sm border-b px-4 py-3 transition-colors',
          'focus-visible:outline-2 focus-visible:outline-accent focus-visible:-outline-offset-2',
          isSelected
            ? 'bg-accent-dim border-subtle'
            : isError
              ? 'bg-danger/5 border-subtle hover:bg-danger/10'
              : 'border-subtle hover:bg-overlay-2',
        )}
      >
        <div className="flex items-center justify-between gap-2 flex-wrap leading-tight">
          <div className="flex items-center gap-2 min-w-0">
            {isError && (
              <AlertCircle
                className="w-3.5 h-3.5 shrink-0 text-danger-text"
                aria-hidden="true"
              />
            )}
            <span className="font-mono text-data text-text">{row.id}</span>
            <Badge tone={STATE_TONE[row.state]}>{STATE_LABEL[row.state]}</Badge>
            {isError && <Badge tone="danger">Error</Badge>}
          </div>
          <div className="flex items-center gap-2 text-caption tabular-nums">
            <span
              className={cx(
                row.net_pnl > 0
                  ? 'text-success-text'
                  : row.net_pnl < 0
                    ? 'text-danger-text'
                    : 'text-text-muted',
              )}
            >
              {row.state === 'not_tracked'
                ? '$0.00'
                : formatNetPnl(row.net_pnl)}
            </span>
            <span className="text-text-muted">
              {row.relative_time ? (
                row.relative_time
              ) : (
                <RelativeTime ts={row.last_message_at_ms} compact />
              )}
            </span>
          </div>
        </div>
        <div className="flex items-center justify-between mt-2">
          <p
            className={cx(
              'text-caption truncate',
              isError ? 'text-danger-text' : 'text-text-muted',
            )}
          >
            {reasonText}
          </p>
          {showTicks && row.attempts != null && (
            <div className="flex items-center gap-px shrink-0 ml-2">
              {Array.from({ length: row.max_attempts }).map((_, i) => (
                <div
                  key={i}
                  className={cx(
                    'w-1 h-2 rounded-xs',
                    i < row.attempts! ? tickColor : 'bg-overlay-4',
                  )}
                />
              ))}
              <span className="text-caption text-text-faint ml-1.5 tabular-nums">
                {row.attempts}/{row.max_attempts}
              </span>
            </div>
          )}
        </div>
      </button>
    </li>
  );
}

function SessionListSkeleton() {
  return (
    <ul className="flex flex-col" aria-hidden="true">
      {Array.from({ length: 5 }).map((_, i) => (
        <li
          key={i}
          data-testid="cache-keepalive-session-skeleton-row"
          className="w-full min-h-[68px] rounded-sm border-b border-subtle px-4 py-3"
        >
          <div className="flex items-center justify-between gap-2 flex-wrap leading-tight">
            <div className="flex items-center gap-2">
              <Skeleton className="h-5 w-36" />
              <Skeleton className="h-5 w-16" />
            </div>
            <div className="flex items-center gap-2">
              <Skeleton className="h-4 w-14" />
              <Skeleton className="h-4 w-12" />
            </div>
          </div>
          <div className="flex items-center justify-between mt-2">
            <Skeleton className="h-4 w-48" />
            <Skeleton className="h-2 w-24 shrink-0 ml-2" />
          </div>
        </li>
      ))}
    </ul>
  );
}

function useFlipReorder(
  listElement: HTMLUListElement | null,
  items: unknown[],
) {
  const oldRects = React.useRef<Record<string, DOMRect>>({});
  const seenIds = React.useRef<Set<string>>(new Set());

  // biome-ignore lint/correctness/useExhaustiveDependencies: items is the trigger
  React.useLayoutEffect(() => {
    if (!listElement) return;
    if (window.PAUSE_ANIMATIONS) return;

    const children = Array.from(listElement.children) as HTMLElement[];
    const previousRects = oldRects.current;
    const nextRects: Record<string, DOMRect> = {};
    const measurements: {
      child: HTMLElement;
      key: string;
      rect: DOMRect;
    }[] = [];

    // FLIP's geometry phase must finish before any transform/style write.
    for (const child of children) {
      const key = child.dataset.key;
      if (!key) continue;

      const rect = child.getBoundingClientRect();
      nextRects[key] = rect;
      measurements.push({ child, key, rect });
    }
    oldRects.current = nextRects;

    for (const { child, key, rect } of measurements) {
      const oldRect = previousRects[key];

      if (oldRect) {
        const deltaY = oldRect.top - rect.top;
        if (deltaY !== 0) {
          child.style.transform = `translateY(${deltaY}px)`;
          child.style.transition = 'none';
          child.style.position = 'relative';
          child.style.zIndex = '10';
          child.style.backgroundColor = 'var(--color-bg-sub)';
          child.style.boxShadow = '0 6px 16px rgba(0, 0, 0, 0.45)';

          requestAnimationFrame(() => {
            child.style.transform = '';
            child.style.transition =
              cacheKeepaliveAnimationContract.riseTransition;
          });

          const clearLift = () => {
            child.style.position = '';
            child.style.zIndex = '';
            child.style.backgroundColor = '';
            child.style.boxShadow = '';
            child.removeEventListener('transitionend', clearLift);
          };
          child.addEventListener('transitionend', clearLift);
        }
      } else if (!seenIds.current.has(key)) {
        child.style.opacity = '0';
        child.style.transform = 'translateY(10px)';
        child.style.transition = 'none';
        requestAnimationFrame(() => {
          child.style.opacity = '1';
          child.style.transform = '';
          child.style.transition =
            cacheKeepaliveAnimationContract.newSessionTransition;
        });
      }
    }

    for (const { key } of measurements) {
      seenIds.current.add(key);
    }
  }, [items, listElement]);
}

export function CacheKeepaliveSessionsDrawer({
  open,
  onOpenChange,
  principal,
}: Props) {
  const [filterChoice, setFilter] = useState<CacheKeepaliveStatusFilter>('all');
  const [horizon, setHorizon] = useState<CacheKeepaliveHorizon>('24h');
  const [selected, setSelected] = useState<CacheKeepaliveRow | null>(null);
  const [listElement, setListElement] = useState<HTMLUListElement | null>(null);

  const enabled = principal.cache_keepalive?.enabled ?? false;
  // The status filter is hidden while keepalive is off, so a stale choice
  // must not keep narrowing the list the operator can no longer see filtered.
  const filter: CacheKeepaliveStatusFilter = enabled ? filterChoice : 'all';

  const query = useCacheKeepaliveSessions(
    principal.id,
    {
      horizon,
      status:
        filter === 'all' ? undefined : filter === 'error' ? undefined : filter,
      error: filter === 'error' ? true : undefined,
    },
    open,
  );

  const allRows = useMemo(() => {
    const rawRows = query.data?.pages.flatMap((p) => p.rows ?? []) ?? [];
    const maxRows = Math.max(
      100,
      query.data?.pages.length ? query.data.pages.length * 100 : 100,
    );
    return mergeLiveSessions(rawRows, maxRows);
  }, [query.data]);

  useFlipReorder(listElement, allRows);

  const summary = query.data?.pages[0]?.summary;

  const handleOpenChange = (o: boolean) => {
    if (!o) setSelected(null);
    onOpenChange(o);
  };

  return (
    <Drawer
      open={open}
      onOpenChange={handleOpenChange}
      title="Cache keepalive sessions"
      description={principal.name}
      width="xl"
    >
      <div
        className="h-full flex flex-col min-h-0"
        data-testid="cache-keepalive-sessions-body"
      >
        <OverviewStrip
          horizon={horizon}
          summary={summary}
          isLoading={query.isLoading}
          onHorizonChange={setHorizon}
          showHorizon={enabled}
        />

        {enabled ? (
          <div className="px-4 py-2.5 border-b border-subtle shrink-0">
            <Select
              aria-label="Session status"
              size="sm"
              className="w-44"
              allLabel="All statuses"
              value={filter === 'all' ? '' : filter}
              options={STATUS_OPTIONS}
              onChange={(next) => {
                const value = next === '' ? 'all' : next;
                if (isStatusFilter(value)) setFilter(value);
              }}
            />
          </div>
        ) : (
          <p
            className="px-4 py-2.5 border-b border-subtle shrink-0 text-body-sm text-text-muted"
            data-testid="cache-keepalive-sessions-off"
          >
            Cache keepalive is off for this principal, so no new sessions are
            tracked. Turn it on from the Cache keepalive card.
          </p>
        )}

        <div className="flex-1 flex min-h-0">
          <div
            className={cx(
              'overflow-y-auto p-2',
              selected
                ? 'max-[960px]:hidden w-[440px] shrink-0 border-r border-subtle'
                : 'flex-1',
            )}
          >
            <div
              data-testid="cache-keepalive-session-list-region"
              aria-busy={query.isLoading}
              className="min-h-[340px]"
            >
              {query.isLoading ? (
                <SessionListSkeleton />
              ) : !enabled && allRows.length === 0 ? (
                <EmptyState title="No sessions recorded" />
              ) : allRows.length === 0 ? (
                <EmptyState
                  title={
                    filter === 'all' ? 'No sessions' : 'No matching sessions'
                  }
                  description={
                    filter === 'all'
                      ? 'No sessions recorded yet.'
                      : 'Try a different filter.'
                  }
                />
              ) : (
                <ul ref={setListElement} className="flex flex-col">
                  {allRows.map((r) => (
                    <SessionListRow
                      key={r.id}
                      row={r}
                      isSelected={selected?.id === r.id}
                      onSelect={() => setSelected(r)}
                    />
                  ))}
                </ul>
              )}
            </div>
            {query.hasNextPage && (
              <div className="flex justify-center pt-3 pb-4">
                <Button
                  variant="secondary"
                  size="sm"
                  onClick={() => query.fetchNextPage()}
                  disabled={query.isFetchingNextPage}
                  iconLeft={query.isFetchingNextPage ? <Spinner /> : undefined}
                >
                  Loading older sessions...
                </Button>
              </div>
            )}
            {!query.hasNextPage && allRows.length > 0 && (
              <div className="text-center py-4 text-caption text-text-faint">
                · No more sessions ·
              </div>
            )}
          </div>

          {selected && (
            <SessionDetailPane
              principalId={principal.id}
              sessionId={selected.id}
              onClose={() => setSelected(null)}
            />
          )}
        </div>
      </div>
    </Drawer>
  );
}
