import { Select as BaseSelect } from '@base-ui/react/select';
import { Toggle as BaseToggle } from '@base-ui/react/toggle';
import { createFileRoute, useNavigate } from '@tanstack/react-router';
import {
  AlertTriangle,
  Check,
  ChevronDown,
  Download,
  RefreshCw,
  X,
  Zap,
} from 'lucide-react';
import { type ReactNode, useEffect, useMemo, useRef, useState } from 'react';
import { z } from 'zod';
import { LiveTailFailureBanner } from '../components/LiveTailFailureBanner';
import {
  Button,
  Card,
  cx,
  Field,
  FullPage,
  INPUT_CLASS,
  Section,
} from '../components/ui/primitives';
import {
  RequestEventsTable,
  SessionChip,
} from '../components/ui/RequestEventsTable';
import { eventTime, type RequestEvent } from '../lib/api';
import {
  usePrincipalNameMap,
  useRecentEventsInfinite,
  useUpstreamNameMap,
  useUpstreams,
} from '../lib/queries';
import { useLiveEventStream } from '../lib/useLiveEventStream';

const logsSearchSchema = z.object({
  principal_id: z.string().optional(),
  upstream: z.string().optional(),
  session: z.string().optional(),
  model: z.string().optional(),
  status: z.string().optional(),
  since: z.string().optional(),
  until: z.string().optional(),
});

export const Route = createFileRoute('/logs')({
  validateSearch: logsSearchSchema,
  component: LogsPage,
});

function LogsPage() {
  const filters = Route.useSearch();
  const navigate = useNavigate({ from: Route.fullPath });
  const upstreams = useUpstreams();
  // Session filter is applied client-side (the backend has no thread_id filter
  // yet), so strip it before we key server queries or the live stream by it.
  const { session: sessionFilter, ...serverFilters } = filters;
  const recent = useRecentEventsInfinite(serverFilters);
  const principalNameMap = usePrincipalNameMap();
  const upstreamNameMap = useUpstreamNameMap();

  const [tailing, setTailing] = useState(true);
  const scrollContainerRef = useRef<HTMLDivElement>(null);
  const sentinelRef = useRef<HTMLTableRowElement>(null);

  const live = useLiveEventStream(
    tailing ? serverFilters : { __disabled: '1' },
  );
  const liveRows = Array.from(live.eventsMap.values()).map((v) => v.event);
  const tailStatus = tailing
    ? live.permanentFailure
      ? 'failed'
      : live.status
    : 'idle';

  const statusLabel = {
    idle: 'Off',
    connecting: 'Connecting…',
    live: 'Live',
    stale: 'Stale',
    reconnecting: 'Reconnecting…',
    hidden: 'Paused',
    error: 'Offline',
    failed: 'Failed',
  }[tailStatus];

  const statusColor = {
    idle: 'neutral',
    connecting: 'info',
    live: 'ok',
    stale: 'warn',
    reconnecting: 'warn',
    hidden: 'neutral',
    error: 'danger',
    failed: 'danger',
  }[tailStatus];

  useEffect(() => {
    const el = sentinelRef.current;
    if (!el || !recent.hasNextPage || recent.isFetchingNextPage) return;
    const obs = new IntersectionObserver(
      (entries) =>
        entries.forEach((e) => {
          if (e.isIntersecting) recent.fetchNextPage();
        }),
      { root: scrollContainerRef.current, threshold: 0.1 },
    );
    obs.observe(el);
    return () => obs.disconnect();
  }, [recent.hasNextPage, recent.isFetchingNextPage, recent.fetchNextPage]);

  // biome-ignore lint/correctness/useExhaustiveDependencies: live.eventsMap is a stable Map ref mutated in place by useLiveEventStream; live.version is bumped on every upsert so it is the real re-run trigger.
  const rows = useMemo(() => {
    // While filters change, `recent.data` still holds the previous filter's
    // pages (queryClient default `placeholderData: keepPreviousData`). Treat
    // that as empty so we don't show the old filter's rows under the new
    // filter's subtitle.
    const historical = recent.isPlaceholderData
      ? []
      : (recent.data?.pages.flatMap((p) => p.events) ?? []);
    const seen = new Set<string>();
    const out: (RequestEvent & { _phase?: 'partial' | 'final' })[] = [];
    for (const { phase, event: ev } of live.eventsMap.values()) {
      const key = ev.event_id ?? ev.request_id;
      if (!seen.has(key)) {
        seen.add(key);
        out.push({ ...(ev as RequestEvent), _phase: phase });
      }
    }
    for (const ev of historical) {
      const key = ev.event_id ?? ev.request_id;
      if (!seen.has(key)) {
        seen.add(key);
        out.push({ ...ev, _phase: 'final' });
      }
    }
    return out.sort(
      (a, b) => (eventTime(b)?.getTime() ?? 0) - (eventTime(a)?.getTime() ?? 0),
    );
  }, [live.eventsMap, live.version, recent.data, recent.isPlaceholderData]);

  const sessionOptions = useMemo(() => {
    const seen = new Set<string>();
    const out: string[] = [];
    for (const r of rows) {
      const id = r.thread_id;
      if (id && !seen.has(id)) {
        seen.add(id);
        out.push(id);
      }
    }
    return out;
  }, [rows]);

  const visibleRows = useMemo(
    () =>
      sessionFilter ? rows.filter((r) => r.thread_id === sessionFilter) : rows,
    [rows, sessionFilter],
  );

  const principalSelectOptions = useMemo<FilterOption[]>(
    () =>
      Array.from(principalNameMap.entries()).map(([id, name]) => ({
        value: id,
        label: <span className="truncate">{name}</span>,
      })),
    [principalNameMap],
  );

  const upstreamSelectOptions = useMemo<FilterOption[]>(
    () =>
      (upstreams.data?.upstreams ?? []).map((u) => ({
        value: u.name,
        label: <span className="font-mono truncate">{u.name}</span>,
      })),
    [upstreams.data],
  );

  const sessionSelectOptions = useMemo<FilterOption[]>(() => {
    const list: FilterOption[] = [];
    if (sessionFilter && !sessionOptions.includes(sessionFilter)) {
      list.push({
        value: sessionFilter,
        label: <SessionChip sessionId={sessionFilter} />,
        hint: 'not in view',
      });
    }
    for (const id of sessionOptions) {
      list.push({
        value: id,
        label: <SessionChip sessionId={id} />,
      });
    }
    return list;
  }, [sessionOptions, sessionFilter]);

  const statusSelectOptions = useMemo<FilterOption[]>(
    () => [
      { value: '200', label: <span className="font-mono">2xx</span> },
      { value: '429', label: <span className="font-mono">429</span> },
      { value: '500', label: <span className="font-mono">5xx</span> },
    ],
    [],
  );

  // biome-ignore lint/correctness/useExhaustiveDependencies: same rationale — live.version is the mutation counter for the stable eventsMap ref.
  const recentLiveIds = useMemo(
    () =>
      new Set(
        Array.from(live.eventsMap.values())
          .slice(0, 20)
          .map((e) => e.event.event_id ?? e.event.request_id),
      ),
    [live.eventsMap, live.version],
  );

  const setFilter = (key: keyof typeof filters, value: string) => {
    navigate({ search: { ...filters, [key]: value || undefined } });
  };

  const downloadJson = () => {
    const blob = new Blob([JSON.stringify(visibleRows, null, 2)], {
      type: 'application/json',
    });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `cc-lb-events-${new Date().toISOString().slice(0, 16)}.json`;
    a.click();
    URL.revokeObjectURL(url);
  };

  return (
    <FullPage>
      <LiveTailFailureBanner
        permanentFailure={live.permanentFailure}
        permanentFailureSince={live.permanentFailureSince}
        reconnectAttempts={live.reconnectAttempts}
        onRetry={live.forceReconnect}
      />
      <Section
        title="Live Logs"
        className="flex-1 min-h-0"
        subtitle={
          <span className="flex items-center gap-2">
            <span>
              {visibleRows.length} requests —{' '}
              {tailing ? 'live tailing' : 'paged'}
            </span>
            {tailing ? (
              <span className="inline-flex items-center gap-1 px-1.5 py-0.5 text-[10px] uppercase tracking-wider border border-subtle rounded-sm">
                {tailStatus === 'failed' ? (
                  <AlertTriangle className="w-3 h-3 text-[color:var(--color-danger)]" />
                ) : (
                  <span
                    className={cx(
                      'status-dot',
                      statusColor,
                      tailStatus === 'connecting' ||
                        tailStatus === 'reconnecting'
                        ? 'animate-pulse'
                        : '',
                    )}
                  />
                )}
                <span
                  className={
                    tailStatus === 'failed'
                      ? 'text-[color:var(--color-danger)] font-bold'
                      : ''
                  }
                >
                  {statusLabel}
                </span>
              </span>
            ) : null}
          </span>
        }
        action={
          <div className="flex items-center gap-2 flex-wrap justify-end">
            <BaseToggle
              aria-label="Live tail logs"
              className="inline-flex items-center justify-center rounded-sm font-medium transition-colors select-none disabled:cursor-not-allowed disabled:opacity-50 focus-visible:outline focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-2 h-7 px-2.5 text-xs gap-1.5 bg-[color:var(--color-panel-strong)] border border-[color:var(--color-border)] text-[color:var(--color-text)] hover:bg-[color:var(--color-hover-bg)] data-[pressed]:bg-[color:var(--color-accent-dim)] data-[pressed]:text-[color:var(--color-accent)] data-[pressed]:border-[color:var(--color-accent)]"
              onPressedChange={setTailing}
              pressed={tailing}
            >
              <Zap className="w-3 h-3" />
              {tailing ? 'Stop tail' : 'Live tail'}
            </BaseToggle>
            <Button
              size="sm"
              iconLeft={<RefreshCw className="w-3 h-3" />}
              onClick={() => recent.refetch()}
            >
              Refresh
            </Button>
            <Button
              size="sm"
              iconLeft={<Download className="w-3 h-3" />}
              onClick={downloadJson}
            >
              Export
            </Button>
          </div>
        }
      >
        <Card className="flex-1 flex flex-col min-h-0">
          <div className="p-3 border-b border-subtle flex flex-wrap gap-3 items-end shrink-0">
            <Field label="Principal">
              <LogSelect
                value={filters.principal_id ?? ''}
                options={principalSelectOptions}
                onChange={(v) => setFilter('principal_id', v)}
                allLabel="All principals"
                widthClass="w-44"
              />
            </Field>
            <Field label="Upstream">
              <LogSelect
                value={filters.upstream ?? ''}
                options={upstreamSelectOptions}
                onChange={(v) => setFilter('upstream', v)}
                allLabel="All upstreams"
                widthClass="w-44"
              />
            </Field>
            <Field label="Session">
              <LogSelect
                value={sessionFilter ?? ''}
                options={sessionSelectOptions}
                onChange={(v) => setFilter('session', v)}
                allLabel="All sessions"
                widthClass="w-48"
              />
            </Field>
            <Field label="Model">
              <input
                className={`${INPUT_CLASS} w-44 font-mono`}
                value={filters.model ?? ''}
                onChange={(e) => setFilter('model', e.target.value)}
                placeholder="claude-*"
              />
            </Field>
            <Field label="Status">
              <LogSelect
                value={filters.status ?? ''}
                options={statusSelectOptions}
                onChange={(v) => setFilter('status', v)}
                allLabel="All statuses"
                widthClass="w-32"
              />
            </Field>
            <Field label="Since">
              <input
                type="datetime-local"
                lang="en"
                className={`${INPUT_CLASS} w-48`}
                value={filters.since ?? ''}
                onChange={(e) => setFilter('since', e.target.value)}
              />
            </Field>
            <Field label="Until">
              <input
                type="datetime-local"
                lang="en"
                className={`${INPUT_CLASS} w-48`}
                value={filters.until ?? ''}
                onChange={(e) => setFilter('until', e.target.value)}
              />
            </Field>
            {filters.principal_id ||
            filters.upstream ||
            filters.session ||
            filters.model ||
            filters.status ||
            filters.since ||
            filters.until ? (
              <Button
                size="sm"
                iconLeft={<X className="w-3 h-3" />}
                onClick={() => navigate({ search: {} })}
              >
                Clear
              </Button>
            ) : null}
          </div>

          <div
            ref={scrollContainerRef}
            className="flex-1 overflow-auto min-h-0"
          >
            <RequestEventsTable
              events={visibleRows}
              principalNameMap={principalNameMap}
              upstreamNameMap={upstreamNameMap}
              loading={
                (recent.isPending || recent.isPlaceholderData) &&
                liveRows.length === 0
              }
              liveFlashIds={tailing ? recentLiveIds : undefined}
              columns={{ cost: true, tokens: true }}
              sentinelRef={sentinelRef}
              loadingMore={recent.isFetchingNextPage}
              hasMore={recent.hasNextPage}
              minWidthClass="min-w-[1080px]"
              emptyTitle="No requests"
              emptyDescription="Adjust filters or enable live tail."
            />
          </div>
        </Card>
      </Section>
    </FullPage>
  );
}

type FilterOption = {
  value: string;
  label: ReactNode;
  hint?: ReactNode;
};

const LOG_SELECT_ITEM_CLASS =
  'flex items-center gap-2 px-2 py-1.5 text-xs rounded-sm cursor-pointer outline-none data-[highlighted]:bg-[color:var(--color-overlay-5)]';

function LogSelect({
  value,
  options,
  onChange,
  allLabel,
  widthClass,
}: {
  value: string;
  options: FilterOption[];
  onChange: (value: string) => void;
  allLabel: string;
  widthClass: string;
}) {
  return (
    <BaseSelect.Root value={value} onValueChange={(v) => onChange(v ?? '')}>
      <BaseSelect.Trigger
        className={cx(
          INPUT_CLASS,
          widthClass,
          'flex items-center justify-between gap-2 cursor-pointer',
        )}
      >
        <BaseSelect.Value>
          {(selected: unknown) => {
            const key = typeof selected === 'string' ? selected : '';
            if (!key)
              return (
                <span className="text-text-faint text-sm">{allLabel}</span>
              );
            const opt = options.find((o) => o.value === key);
            return (
              <span className="truncate text-sm">{opt?.label ?? key}</span>
            );
          }}
        </BaseSelect.Value>
        <BaseSelect.Icon className="shrink-0 text-text-faint">
          <ChevronDown className="w-3.5 h-3.5" />
        </BaseSelect.Icon>
      </BaseSelect.Trigger>
      <BaseSelect.Portal>
        <BaseSelect.Positioner sideOffset={4} alignItemWithTrigger={false}>
          <BaseSelect.Popup className="z-50 min-w-[220px] max-h-[320px] overflow-auto glass-strong rounded-sm border border-subtle p-1 shadow-2xl">
            <BaseSelect.List>
              <BaseSelect.Item value="" className={LOG_SELECT_ITEM_CLASS}>
                <BaseSelect.ItemIndicator className="w-3.5 shrink-0 text-accent">
                  <Check className="w-3 h-3" />
                </BaseSelect.ItemIndicator>
                <span className="text-text">{allLabel}</span>
              </BaseSelect.Item>
              {options.map((opt) => (
                <BaseSelect.Item
                  key={opt.value}
                  value={opt.value}
                  className={LOG_SELECT_ITEM_CLASS}
                >
                  <BaseSelect.ItemIndicator className="w-3.5 shrink-0 text-accent">
                    <Check className="w-3 h-3" />
                  </BaseSelect.ItemIndicator>
                  <BaseSelect.ItemText className="flex-1 min-w-0">
                    {opt.label}
                  </BaseSelect.ItemText>
                  {opt.hint && (
                    <span className="text-[10px] text-text-faint shrink-0">
                      {opt.hint}
                    </span>
                  )}
                </BaseSelect.Item>
              ))}
            </BaseSelect.List>
          </BaseSelect.Popup>
        </BaseSelect.Positioner>
      </BaseSelect.Portal>
    </BaseSelect.Root>
  );
}
