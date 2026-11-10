import { useQueryClient } from '@tanstack/react-query';
import {
  ChevronDown,
  ChevronRight,
  CornerDownRight,
  Terminal,
} from 'lucide-react';
import { useEffect, useMemo, useState } from 'react';
import type {
  CacheKeepaliveHorizon,
  CacheKeepaliveRow,
  CacheKeepaliveSummary,
} from '../../../lib/cacheKeepaliveApi';
import {
  type Principal,
  qk,
  useCacheKeepaliveSessions,
} from '../../../lib/queries';
import {
  Button,
  cx,
  Drawer,
  EmptyState,
  SegmentedControl,
  type SegmentedOption,
  Skeleton,
} from '../../ui/primitives';
import { mergeLiveSessions } from './liveMergeSessions';
import {
  fmtUsdPlain,
  fmtUsdSigned,
  SessionDetailPane,
} from './SessionDetailPane';

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

export interface SplitUsdParts {
  intPart: string;
  fracDigits: string;
  hasDot: boolean;
}

/**
 * Formats a USD amount without trailing zero padding, splitting at the decimal point `.`
 * so all rows in the same alignment scope align their `.` at the exact same X coordinate.
 */
export function splitUsdNoTrailingZeros(
  val: number | null,
  signed: boolean,
): SplitUsdParts {
  if (val === null) {
    return { intPart: '—', fracDigits: '', hasDot: false };
  }
  if (Math.abs(val) < 0.00005) {
    return { intPart: '$0', fracDigits: '', hasDot: false };
  }
  const sign = val > 0 ? (signed ? '+' : '') : '−';
  const abs = Math.abs(val);
  const precision = abs >= 0.1 ? 2 : 4;
  const fixed = abs.toFixed(precision).replace(/0+$/, '').replace(/\.$/, '');
  const dotIdx = fixed.indexOf('.');
  if (dotIdx === -1) {
    return { intPart: `${sign}$${fixed}`, fracDigits: '', hasDot: false };
  }
  return {
    intPart: `${sign}$${fixed.slice(0, dotIdx)}`,
    fracDigits: fixed.slice(dotIdx + 1),
    hasDot: true,
  };
}

function pnlTone(value: number | null): string {
  if (value == null) return 'text-text-muted';
  if (value > 0) return 'text-traffic-success-text';
  if (value < 0) return 'text-warn-text';
  return 'text-text-muted';
}

export function DecimalAlignedUsd({
  value,
  signed = true,
  maxFracDigits,
  scope,
  className,
}: {
  value: number | null;
  signed?: boolean;
  maxFracDigits: number;
  scope: 'group' | 'item';
  className?: string;
}) {
  const { intPart, fracDigits, hasDot } = splitUsdNoTrailingZeros(
    value,
    signed,
  );
  const fullText = hasDot ? `${intPart}.${fracDigits}` : intPart;
  const padRightCh =
    maxFracDigits <= 0
      ? 0
      : hasDot
        ? Math.max(0, maxFracDigits - fracDigits.length)
        : maxFracDigits + 1;

  return (
    <span
      data-decimal-scope={scope}
      style={padRightCh > 0 ? { paddingRight: `${padRightCh}ch` } : undefined}
      className={cx(
        'inline-flex shrink-0 whitespace-nowrap items-baseline justify-end font-mono text-data tabular-nums',
        pnlTone(value),
        className,
      )}
    >
      {fullText}
      {hasDot ? (
        <span data-decimal-dot={scope} aria-hidden="true" className="hidden" />
      ) : null}
    </span>
  );
}

function sessionDotTone(row: CacheKeepaliveRow): string {
  if (row.error != null) return 'danger';
  if (row.state === 'scheduled') return 'live';
  if (row.state === 'renewed' || (row.net_pnl != null && row.net_pnl > 0)) {
    return 'ok';
  }
  return 'neutral';
}

function shortId8(id: string): string {
  const threadIdx = id.indexOf('thread:');
  if (threadIdx >= 0) {
    return id.slice(threadIdx + 'thread:'.length).slice(0, 8);
  }
  const slashIdx = id.lastIndexOf('/');
  const tail = slashIdx >= 0 ? id.slice(slashIdx + 1) : id;
  return tail.slice(0, 8);
}

function SessionMasterRow({
  row,
  selected,
  maxItemFracDigits,
  onSelect,
}: {
  row: CacheKeepaliveRow;
  selected: boolean;
  maxItemFracDigits: number;
  onSelect: () => void;
}) {
  const renewals = row.retained_renewals ?? row.attempts ?? 0;
  const renewalLabel =
    renewals === 0
      ? row.state === 'scheduled'
        ? 'scheduled'
        : '0×'
      : `${renewals}×`;

  return (
    <button
      type="button"
      data-session-id={row.id}
      title={row.id}
      className={cx(
        'flex w-full items-center justify-between gap-2 rounded-sm pl-5 pr-3.5 py-1.5 text-left transition-colors',
        selected
          ? 'bg-selected text-text'
          : 'text-text-muted hover:bg-overlay-2 hover:text-text',
        'focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-[-2px]',
      )}
      aria-current={selected ? 'true' : undefined}
      onClick={onSelect}
    >
      <span className="flex min-w-0 flex-1 items-center gap-2 overflow-hidden">
        <span
          aria-hidden="true"
          className={cx('status-dot shrink-0', sessionDotTone(row))}
        />
        <span className="truncate font-mono text-data text-text">
          {shortId8(row.id)}
        </span>
        <span className="shrink-0 text-caption text-text-faint tabular-nums">
          {renewalLabel}
        </span>
        {row.state === 'cancelled' ? (
          <span className="shrink-0 rounded-sm bg-overlay-5 px-1.5 py-0.5 text-caption text-text-muted">
            Cancelled
          </span>
        ) : row.state === 'failed' ? (
          <span className="shrink-0 rounded-sm bg-danger/12 px-1.5 py-0.5 text-caption text-danger-text">
            Failed
          </span>
        ) : null}
      </span>
      <DecimalAlignedUsd
        value={row.net_pnl}
        signed={row.net_pnl !== null && Math.abs(row.net_pnl) >= 0.00005}
        maxFracDigits={maxItemFracDigits}
        scope="item"
        className="font-medium"
      />
    </button>
  );
}
function DeletedSessionsRow({
  summary,
  maxFracDigits,
}: {
  summary: CacheKeepaliveSummary;
  maxFracDigits: number;
}) {
  return (
    <div
      className="flex w-full items-start justify-between gap-2 rounded-sm bg-overlay-2 pl-5 pr-3.5 py-1.5 text-left"
      data-testid="cache-keepalive-deleted-sessions"
    >
      <span className="min-w-0">
        <span className="block truncate text-label text-text">
          Deleted sessions ({summary.unlisted_sessions.toLocaleString('en-US')})
        </span>
        <span className="block text-caption text-text-faint">
          {summary.unlisted_renewals.toLocaleString('en-US')} renewals · Spent{' '}
          <span className="whitespace-nowrap">
            {fmtUsdPlain(summary.unlisted_spent)}
          </span>
        </span>
        <span className="block text-caption text-text-faint">
          Session records were removed after retention, but their renewal cost
          stays in the totals.
        </span>
      </span>
      <span className="flex shrink-0 items-baseline gap-1 text-caption text-text-muted">
        Net
        <DecimalAlignedUsd
          value={summary.unlisted_net}
          maxFracDigits={maxFracDigits}
          scope="group"
        />
      </span>
    </div>
  );
}

function SheetSummaryHeader({
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
  const effectiveCostSaved = summary?.cost_saved ?? null;
  const effectiveTotalAvoided = summary?.total_avoided ?? null;

  return (
    <header
      className="flex shrink-0 flex-wrap items-center justify-between gap-3 border-b border-subtle px-4 py-2.5"
      data-testid="cache-keepalive-pnl-equation"
    >
      <div className="flex min-w-0 flex-wrap items-baseline gap-x-2 gap-y-0.5 text-body-sm tabular-nums">
        {isLoading ? (
          <Skeleton className="h-5 w-52" />
        ) : summary ? (
          <>
            <span
              className={cx(
                'font-medium whitespace-nowrap',
                pnlTone(effectiveCostSaved),
              )}
            >
              Net {fmtUsdSigned(effectiveCostSaved)}
            </span>
            <span className="text-caption text-text-faint">
              Saved{' '}
              <span className="whitespace-nowrap">
                {fmtUsdPlain(effectiveTotalAvoided)}
              </span>{' '}
              · Spent{' '}
              <span className="whitespace-nowrap">
                {fmtUsdPlain(summary.total_spent)}
              </span>
            </span>
            {effectiveCostSaved === null ? (
              <span className="text-caption text-text-faint">
                · Price unknown
              </span>
            ) : summary.evidence_status !== 'observed' ? (
              <span className="text-caption text-text-faint">
                · Follow-up {summary.evidence_status}
              </span>
            ) : null}
          </>
        ) : (
          <span className="text-caption text-text-faint">
            No summary available
          </span>
        )}
      </div>

      {showHorizon ? (
        <div className="flex min-w-0 items-center gap-2">
          <span className="shrink-0 text-caption text-text-muted">
            Showing {horizon === 'all' ? 'All time' : `Last ${horizon}`}
          </span>
          <SegmentedControl
            ariaLabel="Time range"
            size="sm"
            value={horizon}
            onChange={onHorizonChange}
            options={HORIZON_OPTIONS}
          />
        </div>
      ) : null}
    </header>
  );
}

function SessionListSkeleton() {
  return (
    <div className="divide-y divide-row px-2 py-2" aria-hidden="true">
      {Array.from({ length: 5 }).map((_, i) => (
        <div
          key={i}
          data-testid="cache-keepalive-session-skeleton-row"
          className="flex items-center justify-between gap-2 px-2 py-2"
        >
          <Skeleton className="h-4 w-28" />
          <Skeleton className="h-4 w-14" />
        </div>
      ))}
    </div>
  );
}

const horizonByPrincipal = new Map<string, CacheKeepaliveHorizon>();

export function CacheKeepaliveSessionsDrawer({
  open,
  onOpenChange,
  principal,
}: Props) {
  const queryClient = useQueryClient();
  const [collapsedTriggers, setCollapsedTriggers] = useState<
    Record<string, boolean>
  >({});
  const [horizon, setHorizon] = useState<CacheKeepaliveHorizon>(
    () => horizonByPrincipal.get(principal.id) ?? '24h',
  );
  const [selectedSessionId, setSelectedSessionId] = useState<string | null>(
    null,
  );

  useEffect(() => {
    setSelectedSessionId(null);
    setCollapsedTriggers({});
    setHorizon(horizonByPrincipal.get(principal.id) ?? '24h');
  }, [principal.id]);

  const handleHorizonChange = (next: CacheKeepaliveHorizon) => {
    horizonByPrincipal.set(principal.id, next);
    setHorizon(next);
  };

  const enabled = principal.cache_keepalive?.enabled ?? false;
  const query = useCacheKeepaliveSessions(
    principal.id,
    {
      horizon,
      status: enabled ? 'visible' : 'all',
    },
    open,
  );

  const groups = query.data?.pages[0]?.groups ?? [];
  const loadedRows = useMemo(
    () =>
      mergeLiveSessions(
        query.data?.pages.flatMap((page) => page.rows ?? []) ?? [],
      ),
    [query.data],
  );
  const loadedRowsByGroup = useMemo(() => {
    const byGroup = new Map<string, CacheKeepaliveRow[]>();
    for (const row of loadedRows) {
      const rows = byGroup.get(row.group_key) ?? [];
      rows.push(row);
      byGroup.set(row.group_key, rows);
    }
    return byGroup;
  }, [loadedRows]);

  const pagesComplete =
    !query.isLoading && !query.hasNextPage && !query.isFetchingNextPage;

  useEffect(() => {
    const overviewSummary = query.data?.pages[0]?.summary;
    if (open && horizon === '24h' && overviewSummary) {
      // The summary query stores the list response and selects `.summary`.
      queryClient.setQueryData(qk.cacheKeepaliveSummary(principal.id), {
        summary: overviewSummary,
        groups: null,
        rows: [],
        next_cursor: null,
      });
    }
  }, [horizon, open, principal.id, query.data, queryClient]);

  // Group totals come from the overview response, not from the loaded page subset.
  const maxGroupFracDigits = useMemo(
    () =>
      groups.reduce((max, group) => {
        const { fracDigits } = splitUsdNoTrailingZeros(
          group.net_pnl,
          group.net_pnl !== null && Math.abs(group.net_pnl) >= 0.00005,
        );
        return Math.max(max, fracDigits.length);
      }, 0),
    [groups],
  );

  const maxItemFracDigits = useMemo(
    () =>
      loadedRows.reduce((max, row) => {
        const { fracDigits } = splitUsdNoTrailingZeros(
          row.net_pnl,
          row.net_pnl !== null && Math.abs(row.net_pnl) >= 0.00005,
        );
        return Math.max(max, fracDigits.length);
      }, 0),
    [loadedRows],
  );

  const hasSelection = selectedSessionId !== null;
  // A selected row can sit on a page that is not loaded. Only claim it left
  // the list once every page is loaded and it is still absent.
  const selectedSessionHidden =
    hasSelection &&
    pagesComplete &&
    !loadedRows.some((row) => row.id === selectedSessionId);
  const desktopSessionId = hasSelection
    ? selectedSessionHidden
      ? null
      : selectedSessionId
    : (loadedRows[0]?.id ?? null);
  const selectedDetailRow = loadedRows.find(
    (row) => row.id === desktopSessionId,
  );
  useEffect(() => {
    if (!desktopSessionId || !selectedDetailRow) return;
    const queryKey = qk.cacheKeepaliveSessionDetail(
      principal.id,
      desktopSessionId,
      horizon,
    );
    const cachedDetail = queryClient.getQueryData<{
      generation: number;
      is_active: boolean;
    }>(queryKey);
    if (
      cachedDetail &&
      (cachedDetail.generation !== selectedDetailRow.generation ||
        cachedDetail.is_active !== selectedDetailRow.is_active)
    ) {
      void queryClient.invalidateQueries({ queryKey });
    }
  }, [desktopSessionId, horizon, principal.id, queryClient, selectedDetailRow]);
  const showMobileDetail = hasSelection;
  const summary = query.data?.pages[0]?.summary ?? undefined;
  const totalSessions = groups.reduce(
    (total, group) => total + group.sessions,
    0,
  );

  const handleOpenChange = (nextOpen: boolean) => {
    if (!nextOpen) setSelectedSessionId(null);
    onOpenChange(nextOpen);
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
        className="flex h-full min-h-0 flex-col"
        data-testid="cache-keepalive-sessions-body"
      >
        <p
          className="shrink-0 border-b border-subtle px-4 py-2 text-caption text-text-muted"
          data-testid="cache-keepalive-scope"
        >
          Principal-wide history. The current provider lens does not filter
          these sessions.
        </p>
        <SheetSummaryHeader
          horizon={horizon}
          summary={summary}
          isLoading={query.isLoading}
          onHorizonChange={handleHorizonChange}
          showHorizon
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

        {!enabled ? (
          <p
            className="shrink-0 border-b border-subtle px-4 py-2.5 text-body-sm text-text-muted"
            data-testid="cache-keepalive-sessions-off"
          >
            Cache keepalive is off for this principal, so no new sessions are
            tracked. Turn it on from the Cache keepalive section.
          </p>
        ) : null}

        <div
          className="min-h-0 flex-1 overflow-y-auto"
          data-testid="cache-keepalive-session-list-region"
          aria-busy={query.isLoading}
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
          ) : !enabled &&
            groups.length === 0 &&
            (summary?.unlisted_sessions ?? 0) === 0 ? (
            <EmptyState title="No sessions recorded" />
          ) : groups.length === 0 && (summary?.unlisted_sessions ?? 0) === 0 ? (
            <EmptyState
              title="No sessions"
              description="No sessions recorded in this horizon."
            />
          ) : (
            <div className="min-h-full md:grid md:grid-cols-[248px_minmax(0,1fr)] md:divide-x md:divide-subtle lg:grid-cols-[272px_minmax(0,1fr)]">
              {/* Left Pane: Collapsible Trigger groups + Decimal-aligned USD columns */}
              <div
                className={cx(
                  showMobileDetail ? 'hidden md:block' : 'block',
                  'divide-y divide-row px-2 py-2',
                )}
              >
                {groups.map((group) => {
                  const loadedGroupRows =
                    loadedRowsByGroup.get(group.key) ?? [];
                  const isCollapsed = collapsedTriggers[group.key] ?? false;
                  return (
                    <div
                      key={group.key}
                      data-trigger-group={group.key}
                      className="py-1.5 first:pt-0 last:pb-0"
                    >
                      <button
                        type="button"
                        aria-expanded={!isCollapsed}
                        onClick={() =>
                          setCollapsedTriggers((prev) => ({
                            ...prev,
                            [group.key]: !prev[group.key],
                          }))
                        }
                        className="flex w-full items-center justify-between gap-2 rounded-sm px-2 py-1 text-left transition-colors hover:bg-overlay-2 focus-visible:outline-2 focus-visible:outline-accent"
                      >
                        <span className="flex min-w-0 flex-1 items-center gap-1.5 overflow-hidden">
                          {isCollapsed ? (
                            <ChevronRight
                              aria-hidden="true"
                              strokeWidth={1.75}
                              className="h-3.5 w-3.5 shrink-0 text-text-faint"
                            />
                          ) : (
                            <ChevronDown
                              aria-hidden="true"
                              strokeWidth={1.75}
                              className="h-3.5 w-3.5 shrink-0 text-text-faint"
                            />
                          )}
                          {group.kind === 'tool' ? (
                            <>
                              <Terminal
                                aria-hidden="true"
                                strokeWidth={1.75}
                                className="h-3.5 w-3.5 shrink-0 text-text-faint"
                              />
                              <span
                                title={group.label}
                                className="min-w-0 truncate rounded-sm bg-overlay-4 px-1.5 py-0.5 font-mono text-data text-text"
                              >
                                {group.label}
                              </span>
                            </>
                          ) : (
                            <>
                              <CornerDownRight
                                aria-hidden="true"
                                strokeWidth={1.75}
                                className="h-3.5 w-3.5 shrink-0 text-text-faint"
                              />
                              <span
                                title={group.label}
                                className="min-w-0 truncate text-label text-text-muted"
                              >
                                {group.label}
                              </span>
                            </>
                          )}
                          <span className="shrink-0 text-caption text-text-faint tabular-nums">
                            ({group.sessions})
                          </span>
                        </span>
                        <DecimalAlignedUsd
                          value={group.net_pnl}
                          signed={
                            group.net_pnl !== null &&
                            Math.abs(group.net_pnl) >= 0.00005
                          }
                          maxFracDigits={maxGroupFracDigits}
                          scope="group"
                        />
                      </button>

                      {!isCollapsed ? (
                        <div className="mt-0.5 space-y-0.5">
                          {loadedGroupRows.map((row) => (
                            <SessionMasterRow
                              key={row.id}
                              row={row}
                              selected={row.id === desktopSessionId}
                              maxItemFracDigits={maxItemFracDigits}
                              onSelect={() => setSelectedSessionId(row.id)}
                            />
                          ))}
                        </div>
                      ) : null}
                    </div>
                  );
                })}
                {summary && summary.unlisted_sessions > 0 ? (
                  <DeletedSessionsRow
                    summary={summary}
                    maxFracDigits={maxGroupFracDigits}
                  />
                ) : null}
                {query.isFetchingNextPage ? (
                  <p
                    className="px-2 py-2 text-caption text-text-muted"
                    data-testid="cache-keepalive-loading-sessions"
                  >
                    Loading sessions… ({loadedRows.length} loaded)
                  </p>
                ) : query.hasNextPage ? (
                  <div className="flex flex-wrap items-center justify-between gap-2 px-2 py-2">
                    <p
                      className="text-caption text-text-muted"
                      data-testid="cache-keepalive-session-count"
                    >
                      {loadedRows.length} of {totalSessions} sessions shown
                    </p>
                    <Button
                      variant="secondary"
                      size="sm"
                      onClick={() => void query.fetchNextPage()}
                    >
                      Load more sessions
                    </Button>
                  </div>
                ) : pagesComplete ? (
                  <p
                    className="px-2 py-2 text-caption text-text-muted"
                    data-testid="cache-keepalive-all-sessions"
                  >
                    All {totalSessions} sessions shown
                  </p>
                ) : null}
              </div>

              {/* Right Pane: Single compact context bar + D3 Proportional Time-Rail */}
              <div
                className={cx(
                  showMobileDetail ? 'block' : 'hidden',
                  'min-w-0 md:block',
                )}
              >
                {selectedSessionHidden ? (
                  <div
                    className="flex h-full min-h-[340px] flex-col items-center justify-center gap-3 p-6 text-center"
                    data-testid="cache-keepalive-session-no-longer-listed"
                  >
                    <p className="text-body-sm text-text-muted">
                      This session is no longer in this list.
                    </p>
                    <Button
                      variant="ghost"
                      size="sm"
                      onClick={() => setSelectedSessionId(null)}
                    >
                      Back to sessions
                    </Button>
                  </div>
                ) : desktopSessionId ? (
                  <SessionDetailPane
                    principalId={principal.id}
                    sessionId={desktopSessionId}
                    horizon={horizon}
                    onClose={() => setSelectedSessionId(null)}
                  />
                ) : null}
              </div>
            </div>
          )}
        </div>
      </div>
    </Drawer>
  );
}
