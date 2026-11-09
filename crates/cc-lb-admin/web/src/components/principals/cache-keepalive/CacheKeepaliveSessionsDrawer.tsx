import { AlertCircle, ChevronRight } from 'lucide-react';
import React, { useEffect, useMemo, useState } from 'react';
import type {
  CacheKeepaliveHorizon,
  CacheKeepaliveRow,
  CacheKeepaliveState,
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
  { value: '1h', label: '1h' },
  { value: '6h', label: '6h' },
  { value: '24h', label: '24h' },
  { value: '7d', label: '7d' },
  { value: 'all', label: 'All' },
];

const TRACKED_STATUS_OPTIONS = [
  { value: 'scheduled', label: 'Scheduled' },
  { value: 'renewed', label: 'Renewed' },
  { value: 'capped', label: 'Capped' },
  { value: 'expired', label: 'Expired' },
  { value: 'error', label: 'Error' },
] as const;

type TrackedStatus = (typeof TRACKED_STATUS_OPTIONS)[number]['value'];
type GroupMode = 'flat' | 'trigger';

const TRACKED_STATUS_KEYS = new Set<TrackedStatus>(
  TRACKED_STATUS_OPTIONS.map((option) => option.value),
);

function extractTriggerLabel(row: CacheKeepaliveRow): string {
  const sourceText =
    row.display_reason && row.display_reason !== row.reason
      ? `${row.display_reason} · ${row.reason}`
      : (row.display_reason ?? row.reason);
  const backtickToolMatch = sourceText.match(/tool_use:\s*`([^`]+)`/i);
  if (backtickToolMatch?.[1]) return backtickToolMatch[1];
  const toolMatch = sourceText.match(/tool_use:\s*([A-Za-z0-9_.:/-]+)/i);
  if (toolMatch?.[1]) return toolMatch[1];
  const stopMatch = sourceText.match(/stop_reason=([A-Za-z0-9_:-]+)/i);
  if (stopMatch?.[1]) return `stop_reason=${stopMatch[1]}`;
  const unsupportedMatch = sourceText.match(
    /zero-token renewal unsupported:\s*([^—·\n]+)/i,
  );
  if (unsupportedMatch?.[1])
    return `Unsupported: ${unsupportedMatch[1].trim()}`;
  if (/cached prefix too small/i.test(sourceText)) return 'Prefix < min tokens';
  if (/snapshot too large/i.test(sourceText)) return 'Snapshot too large';
  if (/ambiguous turn/i.test(sourceText)) return 'Ambiguous turn';
  if (/agent-in-turn/i.test(sourceText)) return 'Agent in-turn';
  if (/max renewals reached/i.test(sourceText)) return 'Max renewals reached';
  if (/max duration reached/i.test(sourceText)) return 'Max duration reached';
  if (/TTL expired before follow-up/i.test(sourceText)) return 'TTL expired';
  if (/renewal dispatch unavailable/i.test(sourceText)) return 'Dispatch error';
  return 'Other';
}

function extractActorLabel(
  row: CacheKeepaliveRow,
): 'subagent' | 'advisor' | null {
  const match = row.id.match(/(?:^|\/)agent:(subagent|advisor)(?:$|[/:])/i);
  return (
    (match?.[1]?.toLowerCase() as 'subagent' | 'advisor' | undefined) ?? null
  );
}

function formatMoney(val: number) {
  if (val < 0) return `−$${Math.abs(val).toFixed(2)}`;
  return `$${val.toFixed(2)}`;
}

function formatSignedMoney(val: number) {
  if (val === 0) return '$0.00';
  return val > 0 ? `+${formatMoney(val)}` : formatMoney(val);
}

function formatNetPnl(val: number) {
  if (val === 0) return '$0.00';
  const sign = val > 0 ? '+' : '−';
  const abs = Math.abs(val);
  const str = abs.toFixed(4);
  return `${sign}$${str.replace(/0$/, '')}`;
}

const STATE_TONE: Record<
  CacheKeepaliveState,
  'ok' | 'warn' | 'danger' | 'neutral' | 'accent'
> = {
  renewed: 'ok',
  scheduled: 'accent',
  capped: 'warn',
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
  const effectiveCostSaved =
    summary?.cost_saved ?? summary?.resolved_cost_saved ?? null;
  const effectiveTotalAvoided =
    summary?.total_avoided ?? summary?.resolved_total_avoided ?? null;
  const avoidedKnown =
    effectiveTotalAvoided != null &&
    (summary?.evidence_status === 'observed' ||
      (summary?.evidence_status === 'pending' &&
        summary?.resolved_total_avoided != null));
  const avoided = effectiveTotalAvoided ?? 0;
  const spent = summary?.total_spent ?? 0;
  const total = avoidedKnown ? avoided + spent : 0;
  const avoidedPercent = total > 0 ? (avoided / total) * 100 : 0;
  const spentPercent = total > 0 ? (spent / total) * 100 : 0;

  let verdict: React.ReactNode = null;
  let netReadout = '—';
  let netClassName = 'text-text-muted';
  if (
    summary &&
    effectiveCostSaved !== null &&
    summary.evidence_status === 'observed'
  ) {
    if (effectiveCostSaved > 0) {
      verdict = <Badge tone="ok">Saving</Badge>;
      netReadout = formatSignedMoney(effectiveCostSaved);
      netClassName = 'text-success-text';
    } else if (effectiveCostSaved < 0) {
      verdict = <Badge tone="danger">Losing</Badge>;
      netReadout = formatMoney(effectiveCostSaved);
      netClassName = 'text-danger-text';
    } else {
      verdict = <Badge tone="neutral">Break-even</Badge>;
      netReadout = '$0.00';
    }
  } else if (summary) {
    verdict = <Badge tone="neutral">Evidence {summary.evidence_status}</Badge>;
  }

  return (
    <div className="shrink-0 px-4 pt-3 pb-2">
      <div className="flex items-center justify-between gap-3 flex-wrap">
        <div className="flex items-baseline gap-x-2 gap-y-0.5 flex-wrap min-w-0">
          <span className="text-caption text-text-muted">
            Retained history:
          </span>
          {isLoading ? (
            <>
              <Skeleton className="h-5 w-32" />
              <Skeleton className="h-4 w-52" />
            </>
          ) : summary ? (
            <>
              <span className="text-body font-medium text-text tabular-nums">
                {summary.retained_successful_hits.toLocaleString('en-US')}{' '}
                renewal
                {summary.retained_successful_hits === 1 ? '' : 's'} fired
              </span>
              <span className="text-caption text-text-faint tabular-nums">
                · {summary.sessions_last_5m.toLocaleString('en-US')} session
                {summary.sessions_last_5m === 1 ? '' : 's'} (last 5m)
              </span>
            </>
          ) : (
            <span className="text-caption text-text-faint">
              No summary available
            </span>
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
      {summary && !isLoading ? (
        <div
          className="well mt-3 grid gap-3 p-3 sm:grid-cols-[auto_minmax(0,1fr)]"
          data-testid="cache-keepalive-pnl-equation"
        >
          <div className="flex items-center gap-2">
            {verdict}
            <span
              className={cx('text-body font-medium tabular-nums', netClassName)}
            >
              {netReadout}
            </span>
          </div>
          <div className="min-w-0">
            <div
              className="h-1 w-full overflow-hidden rounded-xs bg-overlay-4"
              aria-label="Avoided versus spent"
            >
              <div className="flex h-full">
                <span
                  className="h-full bg-[var(--color-series-cache-read)]"
                  style={{ width: `${avoidedPercent}%` }}
                />
                <span
                  className="h-full bg-[var(--color-series-cache-create-5m)]"
                  style={{ width: `${spentPercent}%` }}
                />
              </div>
            </div>
            <p className="mt-2 text-caption text-text-muted tabular-nums">
              Avoided{' '}
              {effectiveTotalAvoided === null
                ? '—'
                : formatMoney(effectiveTotalAvoided)}
              {' − '}Spent {formatMoney(summary.total_spent)}
              {' = '}Net{' '}
              {effectiveCostSaved === null
                ? '—'
                : formatSignedMoney(effectiveCostSaved)}
            </p>
          </div>
        </div>
      ) : null}
    </div>
  );
}

function SessionListRow({
  row,
  isSelected,
  onSelect,
  detail,
}: {
  row: CacheKeepaliveRow;
  isSelected: boolean;
  onSelect: () => void;
  detail?: React.ReactNode;
}) {
  const isError = row.error != null;
  const triggerLabel = extractTriggerLabel(row);
  const actorLabel = extractActorLabel(row);
  const showTicks =
    row.is_active && row.state !== 'scheduled' && row.state !== 'not_tracked';
  // Attempt ticks are a small meter: brand fill for a renewed session, ink
  // for any other state (the state badge carries the meaning).
  const tickColor = row.state === 'renewed' ? 'bg-accent' : 'bg-text-muted';

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
            ? 'bg-selected border-subtle'
            : isError
              ? 'bg-danger/5 border-subtle hover:bg-danger/10'
              : 'border-subtle hover:bg-overlay-2',
        )}
      >
        <div className="grid grid-cols-[minmax(0,1fr)_auto] items-center gap-x-3 gap-y-1 leading-tight sm:grid-cols-[minmax(0,1fr)_auto_auto_auto]">
          <div className="col-start-1 row-start-1 flex min-w-0 items-center gap-2">
            {isError && (
              <AlertCircle
                className="w-3.5 h-3.5 shrink-0 text-danger-text"
                aria-hidden="true"
              />
            )}
            <span
              className={cx(
                'truncate font-mono text-data text-text',
                isSelected && 'font-medium',
              )}
            >
              {row.id}
            </span>
            {triggerLabel !== 'Other' && (
              <span className="shrink-0 rounded-xs bg-overlay-4 px-1.5 py-0.5 font-mono text-caption text-text-muted">
                {triggerLabel}
              </span>
            )}
            {actorLabel && (
              <span className="shrink-0 rounded-xs bg-overlay-4 px-1.5 py-0.5 text-caption text-text-faint">
                {actorLabel}
              </span>
            )}
          </div>
          <div className="col-start-1 row-start-2 flex min-w-0 items-center gap-2 sm:col-start-2 sm:row-start-1">
            <Badge tone={STATE_TONE[row.state]}>{STATE_LABEL[row.state]}</Badge>
            {isError && <Badge tone="danger">Error</Badge>}
          </div>
          <span
            className={cx(
              'col-start-2 row-start-1 justify-self-end text-caption tabular-nums sm:col-start-3',
              row.net_pnl === null
                ? 'text-text-faint'
                : row.net_pnl > 0
                  ? 'text-success-text'
                  : row.net_pnl < 0
                    ? 'text-danger-text'
                    : 'text-text-muted',
            )}
          >
            {row.net_pnl === null ? '—' : formatNetPnl(row.net_pnl)}
          </span>
          <span className="col-start-2 row-start-2 justify-self-end text-caption tabular-nums text-text-muted sm:col-start-4 sm:row-start-1">
            {row.relative_time ? (
              row.relative_time
            ) : (
              <RelativeTime ts={row.last_message_at_ms} compact />
            )}
          </span>
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
      {detail}
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
  layoutKey: string,
) {
  const oldRects = React.useRef<Record<string, DOMRect>>({});
  const oldOrder = React.useRef<Record<string, number>>({});
  const seenIds = React.useRef<Set<string>>(new Set());
  const prevLayoutKey = React.useRef<string>(layoutKey);

  // biome-ignore lint/correctness/useExhaustiveDependencies: items and layoutKey are the triggers
  React.useLayoutEffect(() => {
    if (!listElement) return;
    if (window.PAUSE_ANIMATIONS) return;

    const layoutChanged = prevLayoutKey.current !== layoutKey;
    prevLayoutKey.current = layoutKey;

    const children = Array.from(listElement.children) as HTMLElement[];
    const previousRects = oldRects.current;
    const previousOrder = oldOrder.current;
    const nextRects: Record<string, DOMRect> = {};
    const nextOrder: Record<string, number> = {};
    const measurements: {
      child: HTMLElement;
      key: string;
      rect: DOMRect;
      index: number;
    }[] = [];

    // FLIP's geometry phase must finish before any transform/style write.
    for (const child of children) {
      const key = child.dataset.key;
      if (!key) continue;

      const rect = child.getBoundingClientRect();
      const index = measurements.length;
      nextRects[key] = rect;
      nextOrder[key] = index;
      measurements.push({ child, key, rect, index });
    }
    oldRects.current = nextRects;
    oldOrder.current = nextOrder;

    if (layoutChanged) {
      for (const { key } of measurements) {
        seenIds.current.add(key);
      }
      return;
    }

    for (const { child, key, rect, index } of measurements) {
      const oldRect = previousRects[key];
      const oldIndex = previousOrder[key];

      if (oldRect) {
        const deltaY = oldRect.top - rect.top;
        if (oldIndex !== undefined && oldIndex !== index && deltaY !== 0) {
          child.style.transform = `translateY(${deltaY}px)`;
          child.style.transition = 'none';
          child.style.position = 'relative';
          child.style.zIndex = '10';
          child.style.backgroundColor = 'var(--color-bg-sub)';

          requestAnimationFrame(() => {
            child.style.transform = '';
            child.style.transition =
              cacheKeepaliveAnimationContract.riseTransition;
          });

          const clearLift = () => {
            child.style.position = '';
            child.style.zIndex = '';
            child.style.backgroundColor = '';
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
  }, [items, layoutKey, listElement]);
}

export function CacheKeepaliveSessionsDrawer({
  open,
  onOpenChange,
  principal,
}: Props) {
  const [selectedStatuses, setSelectedStatuses] = useState<Set<TrackedStatus>>(
    () => new Set(TRACKED_STATUS_KEYS),
  );
  const [showNotTracked, setShowNotTracked] = useState(false);
  const [groupMode, setGroupMode] = useState<GroupMode>('trigger');
  const [collapsedGroups, setCollapsedGroups] = useState<Set<string>>(
    () => new Set(),
  );
  const [horizon, setHorizon] = useState<CacheKeepaliveHorizon>('24h');
  const [selected, setSelected] = useState<CacheKeepaliveRow | null>(null);
  const [listElement, setListElement] = useState<HTMLUListElement | null>(null);

  // biome-ignore lint/correctness/useExhaustiveDependencies: reset local drawer state when the principal identity changes
  useEffect(() => {
    setSelected(null);
    setSelectedStatuses(new Set(TRACKED_STATUS_KEYS));
    setShowNotTracked(false);
    setGroupMode('trigger');
    setCollapsedGroups(new Set());
    setHorizon('24h');
  }, [principal.id]);

  const enabled = principal.cache_keepalive?.enabled ?? false;
  const selectedQueryStatus =
    enabled && !showNotTracked
      ? selectedStatuses.size === TRACKED_STATUS_OPTIONS.length
        ? 'tracked'
        : selectedStatuses.size === 1
          ? [...selectedStatuses][0]
          : undefined
      : undefined;

  const query = useCacheKeepaliveSessions(
    principal.id,
    {
      horizon,
      status: selectedQueryStatus === 'error' ? undefined : selectedQueryStatus,
      error: selectedQueryStatus === 'error' ? true : undefined,
    },
    open,
  );

  const unfilteredQuery = useCacheKeepaliveSessions(
    principal.id,
    {
      horizon,
      status: undefined,
      error: undefined,
    },
    open && enabled,
  );

  const allRows = useMemo(() => {
    const rawRows = query.data?.pages.flatMap((p) => p.rows ?? []) ?? [];
    const maxRows = Math.max(
      100,
      query.data?.pages.length ? query.data.pages.length * 100 : 100,
    );
    return mergeLiveSessions(rawRows, maxRows);
  }, [query.data]);

  const unfilteredRows = useMemo(() => {
    const sourcePages = unfilteredQuery.data?.pages ?? query.data?.pages;
    const rawRows = sourcePages?.flatMap((p) => p.rows ?? []) ?? [];
    const maxRows = Math.max(
      100,
      sourcePages?.length ? sourcePages.length * 100 : 100,
    );
    return mergeLiveSessions(rawRows, maxRows);
  }, [query.data, unfilteredQuery.data]);

  const visibleRows = useMemo(
    () =>
      allRows.filter((row) => {
        if (!enabled) return true;
        const rowStatus = row.error != null ? 'error' : row.state;
        if (rowStatus === 'not_tracked') return showNotTracked;
        return selectedStatuses.has(rowStatus as TrackedStatus);
      }),
    [allRows, enabled, selectedStatuses, showNotTracked],
  );

  const groupedRows = useMemo(() => {
    const groups = new Map<string, CacheKeepaliveRow[]>();
    for (const row of visibleRows) {
      const trigger = extractTriggerLabel(row);
      const rows = groups.get(trigger);
      if (rows) rows.push(row);
      else groups.set(trigger, [row]);
    }
    return [...groups.entries()];
  }, [visibleRows]);

  const flipLayoutKey = `${groupMode}:${selected?.id ?? ''}:${showNotTracked}:${[...selectedStatuses].sort().join(',')}:${[...collapsedGroups].sort().join(',')}`;
  useFlipReorder(listElement, visibleRows, flipLayoutKey);

  // Pagination remains strictly user-driven via the "Loading older sessions..."
  // control so opening the drawer never auto-drains multi-page retention history.

  const liveCounts = useMemo(() => {
    const computed: Record<TrackedStatus | 'not_tracked', number> = {
      scheduled: 0,
      renewed: 0,
      capped: 0,
      expired: 0,
      error: 0,
      not_tracked: 0,
    };
    for (const row of unfilteredRows) {
      const key = row.error != null ? 'error' : row.state;
      computed[key] = (computed[key] ?? 0) + 1;
    }
    if (
      selectedQueryStatus !== undefined &&
      selectedQueryStatus !== 'tracked'
    ) {
      computed[selectedQueryStatus] = Math.max(
        computed[selectedQueryStatus],
        allRows.filter(
          (row) =>
            (row.error != null ? 'error' : row.state) === selectedQueryStatus,
        ).length,
      );
    }
    return computed;
  }, [allRows, selectedQueryStatus, unfilteredRows]);
  const summary =
    query.data?.pages[0]?.summary ?? unfilteredQuery.data?.pages[0]?.summary;
  const notTrackedCount = liveCounts.not_tracked;
  const handleStatusClick = (key: TrackedStatus) => {
    if (selectedStatuses.size === TRACKED_STATUS_OPTIONS.length) {
      setShowNotTracked(false);
    }
    setSelectedStatuses((current) => {
      if (current.size === TRACKED_STATUS_OPTIONS.length) {
        return new Set([key]);
      }
      const next = new Set(current);
      if (next.has(key)) {
        next.delete(key);
        return next.size === 0 ? new Set(TRACKED_STATUS_KEYS) : next;
      }
      next.add(key);
      return next;
    });
  };

  const resetStatusFilter = () => {
    setSelectedStatuses(new Set(TRACKED_STATUS_KEYS));
    setShowNotTracked(false);
  };

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
        {query.isError && query.data ? (
          <div
            className="flex items-center justify-between gap-3 border-b border-subtle bg-danger/8 px-4 py-2 text-caption"
            role="alert"
          >
            <span className="text-danger-text">
              Could not refresh sessions. Showing the last successful read.
            </span>
            <Button variant="ghost" size="sm" onClick={() => query.refetch()}>
              Retry
            </Button>
          </div>
        ) : null}
        {enabled ? (
          <div className="flex items-center gap-2 flex-wrap px-4 pt-1 pb-3 border-b border-subtle shrink-0">
            <div
              role="group"
              aria-label="Session status"
              className="flex items-center gap-1.5 flex-wrap"
            >
              {TRACKED_STATUS_OPTIONS.map((option) => {
                const selectedStatus = selectedStatuses.has(option.value);
                const dotClassName =
                  option.value === 'renewed'
                    ? 'bg-traffic-success-text'
                    : option.value === 'scheduled'
                      ? 'bg-accent'
                      : option.value === 'error'
                        ? 'bg-danger-text'
                        : 'bg-warn-text';
                const count = liveCounts[option.value];
                return (
                  <button
                    key={option.value}
                    type="button"
                    data-testid={`status-chip-${option.value}`}
                    aria-pressed={selectedStatus}
                    onClick={() => handleStatusClick(option.value)}
                    className={cx(
                      'inline-flex min-h-7 max-md:min-h-10 items-center gap-1.5 rounded-sm border px-2.5 text-caption transition-colors',
                      selectedStatus
                        ? 'border-accent bg-accent/10 text-text'
                        : 'border-subtle text-text-muted hover:bg-overlay-2',
                    )}
                  >
                    <span
                      className={cx('h-1.5 w-1.5 rounded-full', dotClassName)}
                      aria-hidden="true"
                    />
                    <span>{option.label}</span>
                    <span className="tabular-nums text-text-faint">
                      {count}
                    </span>
                  </button>
                );
              })}
            </div>
            <button
              type="button"
              data-testid="toggle-not-tracked"
              aria-pressed={showNotTracked}
              onClick={() => setShowNotTracked((current) => !current)}
              className={cx(
                'inline-flex min-h-7 max-md:min-h-10 items-center rounded-sm border px-2.5 text-caption transition-colors',
                showNotTracked
                  ? 'border-accent bg-accent/10 text-text'
                  : 'border-subtle text-text-muted hover:bg-overlay-2',
              )}
            >
              {showNotTracked ? 'Hide not tracked' : '+ Show not tracked'} (
              {notTrackedCount})
            </button>
            {selectedStatuses.size < TRACKED_STATUS_OPTIONS.length ||
            showNotTracked ? (
              <Button
                variant="ghost"
                size="sm"
                data-testid="status-filter-reset"
                onClick={resetStatusFilter}
              >
                Reset
              </Button>
            ) : null}
            <SegmentedControl
              ariaLabel="List grouping"
              size="sm"
              value={groupMode}
              onChange={setGroupMode}
              options={[
                { value: 'flat', label: 'Flat list' },
                { value: 'trigger', label: 'Group by trigger' },
              ]}
            />
          </div>
        ) : (
          <p
            className="px-4 pt-1 pb-3 border-b border-subtle shrink-0 text-body text-text-muted"
            data-testid="cache-keepalive-sessions-off"
          >
            Cache keepalive is off for this principal, so no new sessions are
            tracked. Turn it on from the Cache keepalive section.
          </p>
        )}

        <div className="flex-1 min-h-0 overflow-y-auto p-2">
          <div
            data-testid="cache-keepalive-session-list-region"
            aria-busy={query.isLoading}
            className="min-h-[340px]"
          >
            {query.isLoading ? (
              <SessionListSkeleton />
            ) : query.isError && !query.data ? (
              <div
                className="flex min-h-[340px] flex-col items-center justify-center gap-3 px-4 text-center"
                role="alert"
              >
                <p className="text-body text-danger-text">
                  Unable to load cache keepalive sessions.
                </p>
                <Button
                  variant="secondary"
                  size="sm"
                  onClick={() => query.refetch()}
                >
                  Retry sessions
                </Button>
              </div>
            ) : !enabled && allRows.length === 0 ? (
              <EmptyState title="No sessions recorded" />
            ) : visibleRows.length === 0 ? (
              allRows.length === 0 &&
              selectedStatuses.size === TRACKED_STATUS_OPTIONS.length &&
              !showNotTracked ? (
                <EmptyState
                  title="No sessions"
                  description="No sessions recorded yet."
                />
              ) : (
                <EmptyState
                  title="No matching sessions"
                  description="Try a different filter."
                />
              )
            ) : (
              <ul ref={setListElement} className="flex flex-col">
                {groupMode === 'flat'
                  ? visibleRows.map((r) => (
                      <SessionListRow
                        key={r.id}
                        row={r}
                        isSelected={selected?.id === r.id}
                        onSelect={() =>
                          setSelected((current) =>
                            current?.id === r.id ? null : r,
                          )
                        }
                        detail={
                          selected?.id === r.id ? (
                            <div className="w-full border-t border-subtle bg-panel/50">
                              <SessionDetailPane
                                principalId={principal.id}
                                sessionId={r.id}
                                onClose={() => setSelected(null)}
                              />
                            </div>
                          ) : null
                        }
                      />
                    ))
                  : groupedRows.map(([trigger, rows]) => {
                      const groupPnlValues = rows
                        .map((row) => row.net_pnl)
                        .filter((value): value is number => value !== null);
                      const groupPnl =
                        groupPnlValues.length > 0
                          ? groupPnlValues.reduce(
                              (sum, value) => sum + value,
                              0,
                            )
                          : null;
                      const isCollapsed = collapsedGroups.has(trigger);
                      return (
                        <React.Fragment key={`group:${trigger}`}>
                          <li key={`group:${trigger}`}>
                            <button
                              type="button"
                              aria-expanded={!isCollapsed}
                              onClick={() =>
                                setCollapsedGroups((current) => {
                                  const next = new Set(current);
                                  if (next.has(trigger)) next.delete(trigger);
                                  else next.add(trigger);
                                  return next;
                                })
                              }
                              className="flex w-full min-h-9 max-md:min-h-10 items-center justify-between gap-3 border-b border-subtle bg-overlay-2 px-4 py-2 text-left"
                            >
                              <span className="flex min-w-0 items-center gap-2">
                                <ChevronRight
                                  className={cx(
                                    'w-3 h-3 shrink-0 text-text-faint transition-transform',
                                    !isCollapsed && 'rotate-90',
                                  )}
                                  strokeWidth={1.75}
                                  aria-hidden="true"
                                />
                                <span className="truncate text-caption font-medium text-text">
                                  {trigger}
                                </span>
                                <span className="text-caption text-text-muted tabular-nums">
                                  {' '}
                                  {rows.length} session
                                  {rows.length === 1 ? '' : 's'}
                                </span>
                              </span>
                              <span className="shrink-0 text-caption tabular-nums text-text-muted">
                                {groupPnl === null
                                  ? '—'
                                  : formatNetPnl(groupPnl)}
                              </span>
                            </button>
                          </li>
                          {!isCollapsed
                            ? rows.map((r) => (
                                <SessionListRow
                                  key={r.id}
                                  row={r}
                                  isSelected={selected?.id === r.id}
                                  onSelect={() =>
                                    setSelected((current) =>
                                      current?.id === r.id ? null : r,
                                    )
                                  }
                                  detail={
                                    selected?.id === r.id ? (
                                      <div className="w-full border-t border-subtle bg-panel/50">
                                        <SessionDetailPane
                                          principalId={principal.id}
                                          sessionId={r.id}
                                          onClose={() => setSelected(null)}
                                        />
                                      </div>
                                    ) : null
                                  }
                                />
                              ))
                            : null}
                        </React.Fragment>
                      );
                    })}
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
          {!query.hasNextPage && visibleRows.length > 0 && (
            <div className="text-center py-4 text-caption text-text-faint">
              · No more sessions ·
            </div>
          )}
        </div>
      </div>
    </Drawer>
  );
}
