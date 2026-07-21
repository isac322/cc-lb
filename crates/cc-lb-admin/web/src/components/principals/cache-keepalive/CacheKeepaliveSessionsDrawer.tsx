import { Dialog as BaseDialog } from '@base-ui/react/dialog';
import { X } from 'lucide-react';
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
import { Badge, Button, cx, EmptyState, Spinner } from '../../ui/primitives';
import { RelativeTime } from '../../ui/RelativeTime';
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

const HORIZONS: CacheKeepaliveHorizon[] = ['24h', '7d', 'all'];

const FILTERS: { key: CacheKeepaliveStatusFilter; label: string }[] = [
  { key: 'all', label: 'All' },
  { key: 'renewed', label: 'Renewed' },
  { key: 'scheduled', label: 'Scheduled' },
  { key: 'capped', label: 'Capped' },
  { key: 'expired', label: 'Expired' },
  { key: 'not_tracked', label: 'Not tracked' },
  { key: 'error', label: 'Error' },
];

const FILTER_TONE: Record<CacheKeepaliveStatusFilter, string> = {
  all: 'bg-accent/15 text-accent border-accent/40',
  renewed:
    'bg-[color:var(--color-ok)]/15 text-[color:var(--color-ok)] border-[color:var(--color-ok)]/40',
  scheduled: 'bg-accent/15 text-accent border-accent/40',
  capped: 'bg-overlay-4 text-text border-subtle',
  expired:
    'bg-[color:var(--color-warn)]/15 text-[color:var(--color-warn)] border-[color:var(--color-warn)]/40',
  not_tracked: 'bg-overlay-3 text-text-faint border-subtle',
  error:
    'bg-[color:var(--color-danger)]/15 text-[color:var(--color-danger)] border-[color:var(--color-danger)]/40',
};

const FILTER_ACTIVE_RING: Record<CacheKeepaliveStatusFilter, string> = {
  all: 'ring-[color:var(--color-accent)]/50',
  renewed: 'ring-[color:var(--color-ok)]/50',
  scheduled: 'ring-[color:var(--color-accent)]/50',
  capped: 'ring-[color:var(--color-border-strong)]/70',
  expired: 'ring-[color:var(--color-warn)]/50',
  not_tracked: 'ring-[color:var(--color-border-strong)]/70',
  error: 'ring-[color:var(--color-danger)]/50',
};

const STATE_TONE: Record<
  CacheKeepaliveState,
  'ok' | 'warn' | 'danger' | 'neutral' | 'accent' | 'mono'
> = {
  renewed: 'ok',
  scheduled: 'accent',
  capped: 'neutral',
  expired: 'warn',
  not_tracked: 'mono',
};

const STATE_LABEL: Record<CacheKeepaliveState, string> = {
  renewed: 'Renewed',
  scheduled: 'Scheduled',
  capped: 'Capped',
  expired: 'Expired',
  not_tracked: 'Not tracked',
};

function HorizonToggle({
  value,
  onChange,
}: {
  value: CacheKeepaliveHorizon;
  onChange: (h: CacheKeepaliveHorizon) => void;
}) {
  return (
    <div className="flex items-center gap-1 shrink-0">
      {HORIZONS.map((o) => (
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
}: {
  horizon: CacheKeepaliveHorizon;
  summary: CacheKeepaliveSummary | undefined;
}) {
  const label = horizon === 'all' ? 'All time:' : `Last ${horizon}:`;
  return (
    <div className="px-4 py-3 border-b border-subtle shrink-0">
      <div className="flex items-baseline gap-2 flex-wrap">
        <span className="text-xs text-text-muted">{label}</span>
        <span className="text-sm font-medium text-text">
          {(summary?.renewals_fired ?? 0).toLocaleString('en-US')} renewals
          fired
        </span>
        <span className="text-[11px] text-text-faint">
          · {summary ? formatMoney(summary.cost_saved) : '$0.00'} saved ·{' '}
          {(summary?.sessions_last_5m ?? 0).toLocaleString('en-US')} sessions
          tracked
        </span>
      </div>
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
  const tickColor =
    row.state === 'renewed' ? 'bg-[var(--color-ok)]' : 'bg-text-muted';

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
        className={cx(
          'w-full text-left rounded-sm border-b border-subtle px-4 py-3 transition-colors',
          isSelected ? 'bg-accent/10 border-accent/40' : 'hover:bg-overlay-2',
          isError && 'border-l-2 border-l-red-500',
          isError && !isSelected && 'bg-red-500/5',
        )}
      >
        <div className="flex items-center justify-between gap-2 flex-wrap leading-tight">
          <div className="flex items-center gap-2">
            <span className="text-sm font-mono text-text">{row.id}</span>
            <Badge tone={STATE_TONE[row.state]}>{STATE_LABEL[row.state]}</Badge>
            {isError && <Badge tone="danger">Error</Badge>}
          </div>
          <div className="flex items-center gap-2 text-xs">
            <span
              className={cx(
                row.net_pnl > 0
                  ? 'text-[var(--color-ok)]'
                  : row.net_pnl < 0
                    ? 'text-[var(--color-danger)]'
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
              'text-[12px] truncate',
              isError ? 'text-red-400' : 'text-text-muted',
            )}
          >
            {reasonText}
          </p>
          {showTicks && row.attempts != null && (
            <div className="flex items-center gap-0.5 shrink-0 ml-2">
              {Array.from({ length: row.max_attempts }).map((_, i) => (
                <div
                  key={i}
                  className={cx(
                    'w-1 h-2 rounded-sm',
                    i < row.attempts! ? tickColor : 'bg-overlay-4',
                  )}
                />
              ))}
              <span className="text-[10px] text-text-faint ml-1 tabular-nums">
                {row.attempts}/{row.max_attempts}
              </span>
            </div>
          )}
        </div>
      </button>
    </li>
  );
}

function useFlipReorder(
  listRef: React.RefObject<HTMLUListElement | null>,
  items: unknown[],
) {
  const oldRects = React.useRef<Record<string, DOMRect>>({});
  const seenIds = React.useRef<Set<string>>(new Set());

  // biome-ignore lint/correctness/useExhaustiveDependencies: items is the trigger
  React.useLayoutEffect(() => {
    if (!listRef.current) return;
    if (window.PAUSE_ANIMATIONS) return;

    const children = Array.from(listRef.current.children) as HTMLElement[];

    children.forEach((child) => {
      const key = child.dataset.key;
      if (!key) return;

      const oldRect = oldRects.current[key];
      const newRect = child.getBoundingClientRect();

      if (oldRect) {
        const deltaY = oldRect.top - newRect.top;
        if (deltaY !== 0) {
          child.style.transform = `translateY(${deltaY}px)`;
          child.style.transition = 'none';
          child.style.position = 'relative';
          child.style.zIndex = '10';
          child.style.backgroundColor = '#161616';
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
    });

    oldRects.current = {};
    children.forEach((child) => {
      const key = child.dataset.key;
      if (key) {
        oldRects.current[key] = child.getBoundingClientRect();
        seenIds.current.add(key);
      }
    });
  }, [items]);
}

export function CacheKeepaliveSessionsDrawer({
  open,
  onOpenChange,
  principal,
}: Props) {
  const [filter, setFilter] = useState<CacheKeepaliveStatusFilter>('all');
  const [horizon, setHorizon] = useState<CacheKeepaliveHorizon>('24h');
  const [selected, setSelected] = useState<CacheKeepaliveRow | null>(null);
  const listRef = React.useRef<HTMLUListElement>(null);

  const query = useCacheKeepaliveSessions(principal.id, {
    horizon,
    status:
      filter === 'all' ? undefined : filter === 'error' ? undefined : filter,
    error: filter === 'error' ? true : undefined,
  });

  const allRows = useMemo(() => {
    const rawRows = query.data?.pages.flatMap((p) => p.rows ?? []) ?? [];
    const maxRows = Math.max(
      100,
      query.data?.pages.length ? query.data.pages.length * 100 : 100,
    );
    return mergeLiveSessions(rawRows, maxRows);
  }, [query.data]);

  useFlipReorder(listRef, allRows);

  const summary = query.data?.pages[0]?.summary;

  const handleOpenChange = (o: boolean) => {
    if (!o) setSelected(null);
    onOpenChange(o);
  };

  const enabled = principal.cache_keepalive?.enabled ?? false;

  return (
    <BaseDialog.Root onOpenChange={handleOpenChange} open={open}>
      <BaseDialog.Portal>
        <BaseDialog.Backdrop className="fixed inset-0 z-40 bg-drawer-backdrop transition-opacity duration-200 ease-out data-[ending-style]:opacity-0 data-[starting-style]:opacity-0" />
        <BaseDialog.Popup
          className="fixed right-0 top-0 bottom-0 w-full max-w-[960px] bg-bg-sub border-l border-subtle z-50 flex flex-col outline-none transition-transform duration-200 ease-out data-[ending-style]:translate-x-full data-[starting-style]:translate-x-full"
          data-testid="cache-keepalive-sessions-drawer"
        >
          <BaseDialog.Title className="sr-only">
            Cache keepalive sessions
          </BaseDialog.Title>
          <BaseDialog.Description className="sr-only">
            Full list of cache keepalive sessions for {principal.name}.
          </BaseDialog.Description>

          <header className="flex items-center justify-between gap-3 px-4 py-3 border-b border-subtle shrink-0">
            <div className="min-w-0 flex items-center gap-3">
              <div className="min-w-0">
                <h3 className="text-sm font-medium text-text">
                  Cache keepalive sessions
                </h3>
                <p className="text-[11px] text-text-faint font-mono truncate">
                  {principal.name}
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

          <OverviewStrip horizon={horizon} summary={summary} />

          <div className="px-4 py-2.5 border-b border-subtle shrink-0 flex gap-1.5 overflow-x-auto no-scrollbar">
            {FILTERS.map((f) => (
              <button
                key={f.key}
                type="button"
                aria-pressed={filter === f.key}
                onClick={() => setFilter(f.key)}
                className={cx(
                  'inline-flex items-center gap-1.5 px-2 h-6 rounded-sm text-[11px] transition-opacity border whitespace-nowrap shrink-0',
                  FILTER_TONE[f.key],
                  filter === f.key
                    ? cx(
                        'opacity-100 ring-1 ring-inset',
                        FILTER_ACTIVE_RING[f.key],
                      )
                    : 'opacity-60 hover:opacity-100',
                )}
              >
                {f.label}
              </button>
            ))}
          </div>

          <div className="flex-1 flex min-h-0">
            <div
              className={cx(
                'overflow-y-auto p-2',
                selected
                  ? 'max-[960px]:hidden w-[440px] shrink-0 border-r border-subtle'
                  : 'flex-1',
              )}
            >
              {query.isLoading ? (
                <div className="flex items-center justify-center py-12">
                  <Spinner />
                </div>
              ) : !enabled && allRows.length === 0 ? (
                <EmptyState
                  title="Cache keepalive disabled"
                  description="Enable cache keepalive to start tracking sessions."
                />
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
                <ul ref={listRef} className="flex flex-col">
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
                    Loading older sessions...
                  </Button>
                </div>
              )}
              {!query.hasNextPage && allRows.length > 0 && (
                <div className="text-center py-4 text-xs text-text-faint">
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
        </BaseDialog.Popup>
      </BaseDialog.Portal>
    </BaseDialog.Root>
  );
}
