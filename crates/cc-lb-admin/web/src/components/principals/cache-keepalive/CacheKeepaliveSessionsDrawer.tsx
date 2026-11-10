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
  Spinner,
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

const TRIGGER_ORDER = [
  'Bash',
  'Write',
  'Edit',
  'Read',
  '_advise',
  'Agent in-turn',
] as const;

function normalizeToolTrigger(raw: string): string {
  const trimmed = raw.trim();
  const lower = trimmed.toLowerCase();
  if (lower === 'bash') return 'Bash';
  if (lower === 'write' || lower === 'write_file') return 'Write';
  if (lower === 'edit' || lower === 'edit_file') return 'Edit';
  if (lower === 'read' || lower === 'read_file') return 'Read';
  const match = TRIGGER_ORDER.find((known) => known.toLowerCase() === lower);
  return match ?? trimmed;
}

type TriggerKind = 'tool' | 'state';

function extractTriggerDescriptor(row: CacheKeepaliveRow): {
  label: string;
  kind: TriggerKind;
} {
  // Rows written before display reasons existed store '' rather than null.
  const displayReason = row.display_reason?.trim() ? row.display_reason : null;
  const sourceText =
    displayReason && displayReason !== row.reason
      ? `${displayReason} · ${row.reason}`
      : (displayReason ?? row.reason);
  const backtickToolMatch = sourceText.match(/tool_use:\s*`([^`]+)`/i);
  if (backtickToolMatch?.[1]) {
    return {
      label: normalizeToolTrigger(backtickToolMatch[1]),
      kind: 'tool',
    };
  }
  const toolMatch = sourceText.match(/tool_use:\s*([A-Za-z0-9_.:/-]+)/i);
  if (toolMatch?.[1]) {
    return {
      label: normalizeToolTrigger(toolMatch[1]),
      kind: 'tool',
    };
  }
  const stopMatch = sourceText.match(/stop_reason=([A-Za-z0-9_:-]+)/i);
  if (stopMatch?.[1]) {
    return { label: `stop_reason=${stopMatch[1]}`, kind: 'state' };
  }
  const unsupportedMatch = sourceText.match(
    /zero-token renewal unsupported:\s*([^—·\n]+)/i,
  );
  if (unsupportedMatch?.[1]) {
    return {
      label: `Unsupported: ${unsupportedMatch[1].trim()}`,
      kind: 'state',
    };
  }
  if (/cached prefix too small/i.test(sourceText)) {
    return { label: 'Prefix < min tokens', kind: 'state' };
  }
  if (/snapshot too large/i.test(sourceText)) {
    return { label: 'Snapshot too large', kind: 'state' };
  }
  if (/ambiguous turn/i.test(sourceText)) {
    return { label: 'Ambiguous turn', kind: 'state' };
  }
  if (/agent-in-turn/i.test(sourceText)) {
    return { label: 'Agent in-turn', kind: 'state' };
  }
  if (row.state !== 'not_tracked') {
    return { label: 'Agent in-turn', kind: 'state' };
  }
  return { label: 'Other', kind: 'state' };
}

export function extractTriggerLabel(row: CacheKeepaliveRow): string {
  return extractTriggerDescriptor(row).label;
}

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
  const effectiveCostSaved =
    summary?.cost_saved ?? summary?.resolved_cost_saved ?? null;
  const effectiveTotalAvoided =
    summary?.total_avoided ?? summary?.resolved_total_avoided ?? null;

  return (
    <header
      className="flex shrink-0 flex-wrap items-center justify-between gap-3 border-b border-subtle px-4 py-2.5"
      data-testid="cache-keepalive-pnl-equation"
    >
      <div className="flex flex-wrap items-baseline gap-x-2 gap-y-0.5 text-body-sm tabular-nums">
        {isLoading ? (
          <Skeleton className="h-5 w-52" />
        ) : summary ? (
          <>
            <span className={cx('font-medium', pnlTone(effectiveCostSaved))}>
              Net {fmtUsdSigned(effectiveCostSaved)}
            </span>
            <span className="text-caption text-text-faint">
              (Saved {fmtUsdPlain(effectiveTotalAvoided)} − Spent{' '}
              {fmtUsdPlain(summary.total_spent)})
            </span>
            {effectiveCostSaved === null ||
            summary.evidence_status !== 'observed' ? (
              <span className="text-caption text-text-faint">
                · Evidence {summary.evidence_status}
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
        <SegmentedControl
          ariaLabel="Time range"
          size="sm"
          value={horizon}
          onChange={onHorizonChange}
          options={HORIZON_OPTIONS}
        />
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

export function CacheKeepaliveSessionsDrawer({
  open,
  onOpenChange,
  principal,
}: Props) {
  const [collapsedTriggers, setCollapsedTriggers] = useState<
    Record<string, boolean>
  >({});
  const [horizon, setHorizon] = useState<CacheKeepaliveHorizon>('24h');
  const [selectedSessionId, setSelectedSessionId] = useState<string | null>(
    null,
  );

  // biome-ignore lint/correctness/useExhaustiveDependencies: reset local drawer state when the principal identity changes
  useEffect(() => {
    setSelectedSessionId(null);
    setCollapsedTriggers({});
    setHorizon('24h');
  }, [principal.id]);

  const enabled = principal.cache_keepalive?.enabled ?? false;
  const query = useCacheKeepaliveSessions(
    principal.id,
    {
      horizon,
      status: enabled ? 'all' : undefined,
    },
    open,
  );

  const visibleRows = useMemo(() => {
    const rawRows = query.data?.pages.flatMap((p) => p.rows ?? []) ?? [];
    const maxRows = Math.max(
      100,
      query.data?.pages.length ? query.data.pages.length * 100 : 100,
    );
    return mergeLiveSessions(rawRows, maxRows).filter(
      (row) =>
        !enabled ||
        row.state !== 'not_tracked' ||
        row.error != null ||
        row.ttl === '12h',
    );
  }, [enabled, query.data]);

  const groups = useMemo(() => {
    const grouped = new Map<
      string,
      { sessions: CacheKeepaliveRow[]; kind: TriggerKind }
    >();
    for (const row of visibleRows) {
      const { label: trigger, kind } = extractTriggerDescriptor(row);
      const current = grouped.get(trigger) ?? { sessions: [], kind: 'state' };
      current.sessions.push(row);
      if (kind === 'tool') current.kind = 'tool';
      grouped.set(trigger, current);
    }
    const orderedTriggers = [
      ...TRIGGER_ORDER,
      ...Array.from(grouped.keys()).filter(
        (t) =>
          !TRIGGER_ORDER.some(
            (known) => known.toLowerCase() === t.toLowerCase(),
          ),
      ),
    ];
    return orderedTriggers
      .map((trigger) => {
        const group = grouped.get(trigger);
        const sessions = group?.sessions ?? [];
        const pnlValues = sessions
          .map((s) => s.net_pnl)
          .filter((v): v is number => v !== null);
        const groupNet =
          pnlValues.length > 0
            ? pnlValues.reduce((acc, v) => acc + v, 0)
            : null;
        return { trigger, kind: group?.kind ?? 'state', sessions, groupNet };
      })
      .filter((group) => group.sessions.length > 0);
  }, [visibleRows]);

  // 1. Group titles align their '.' with each other across all groups.
  const maxGroupFracDigits = useMemo(
    () =>
      groups.reduce((max, g) => {
        const { fracDigits } = splitUsdNoTrailingZeros(
          g.groupNet,
          g.groupNet !== null && Math.abs(g.groupNet) >= 0.00005,
        );
        return Math.max(max, fracDigits.length);
      }, 0),
    [groups],
  );

  // 2. Session items align their '.' with each other across all groups.
  const maxItemFracDigits = useMemo(
    () =>
      visibleRows.reduce((max, s) => {
        const { fracDigits } = splitUsdNoTrailingZeros(
          s.net_pnl,
          s.net_pnl !== null && Math.abs(s.net_pnl) >= 0.00005,
        );
        return Math.max(max, fracDigits.length);
      }, 0),
    [visibleRows],
  );

  const selectedSession = visibleRows.find((s) => s.id === selectedSessionId);
  const desktopSession = selectedSession ?? visibleRows[0] ?? null;
  const showMobileDetail =
    selectedSession !== undefined && selectedSession !== null;
  const summary = query.data?.pages[0]?.summary;

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
          ) : !enabled && visibleRows.length === 0 ? (
            <EmptyState title="No sessions recorded" />
          ) : visibleRows.length === 0 ? (
            query.hasNextPage ? (
              <div className="flex min-h-[340px] items-center justify-center">
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
            ) : (
              <EmptyState
                title="No sessions"
                description="No sessions recorded in this horizon."
              />
            )
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
                  const isCollapsed = collapsedTriggers[group.trigger] ?? false;
                  return (
                    <div
                      key={group.trigger}
                      data-trigger-group={group.trigger}
                      className="py-1.5 first:pt-0 last:pb-0"
                    >
                      <button
                        type="button"
                        aria-expanded={!isCollapsed}
                        onClick={() =>
                          setCollapsedTriggers((prev) => ({
                            ...prev,
                            [group.trigger]: !prev[group.trigger],
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
                                title={group.trigger}
                                className="min-w-0 truncate rounded-sm bg-overlay-4 px-1.5 py-0.5 font-mono text-data text-text"
                              >
                                {group.trigger}
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
                                title={group.trigger}
                                className="min-w-0 truncate text-label text-text-muted"
                              >
                                {group.trigger}
                              </span>
                            </>
                          )}
                          <span className="shrink-0 text-caption text-text-faint tabular-nums">
                            ({group.sessions.length})
                          </span>
                        </span>
                        <DecimalAlignedUsd
                          value={group.groupNet}
                          signed={
                            group.groupNet !== null &&
                            Math.abs(group.groupNet) >= 0.00005
                          }
                          maxFracDigits={maxGroupFracDigits}
                          scope="group"
                        />
                      </button>

                      {!isCollapsed ? (
                        <div className="mt-0.5 space-y-0.5">
                          {group.sessions.map((row) => (
                            <SessionMasterRow
                              key={row.id}
                              row={row}
                              selected={row.id === desktopSession?.id}
                              maxItemFracDigits={maxItemFracDigits}
                              onSelect={() => setSelectedSessionId(row.id)}
                            />
                          ))}
                        </div>
                      ) : null}
                    </div>
                  );
                })}

                {query.hasNextPage ? (
                  <div className="flex justify-center pt-3 pb-2">
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
                ) : null}
              </div>

              {/* Right Pane: Single compact context bar + D3 Proportional Time-Rail */}
              <div
                className={cx(
                  showMobileDetail ? 'block' : 'hidden',
                  'min-w-0 md:block',
                )}
              >
                {desktopSession ? (
                  <SessionDetailPane
                    principalId={principal.id}
                    sessionId={desktopSession.id}
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
