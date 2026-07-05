import { Toggle as BaseToggle } from '@base-ui/react/toggle';
import { createFileRoute, useNavigate } from '@tanstack/react-router';
import { Download, RefreshCw, X, Zap } from 'lucide-react';
import { useEffect, useMemo, useRef, useState } from 'react';
import { z } from 'zod';
import {
  Button,
  Card,
  cx,
  Field,
  FullPage,
  INPUT_CLASS,
  Section,
} from '../components/ui/primitives';
import { RequestEventsTable } from '../components/ui/RequestEventsTable';
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
  const recent = useRecentEventsInfinite(filters);
  const principalNameMap = usePrincipalNameMap();
  const upstreamNameMap = useUpstreamNameMap();

  const [tailing, setTailing] = useState(true);
  const scrollContainerRef = useRef<HTMLDivElement>(null);
  const sentinelRef = useRef<HTMLTableRowElement>(null);

  const live = useLiveEventStream(tailing ? filters : { __disabled: '1' });
  const liveRows = Array.from(live.eventsMap.values()).map((v) => v.event);
  const tailStatus = tailing ? live.status : 'idle';

  const statusLabel = {
    idle: 'Off',
    connecting: 'Connecting…',
    live: 'Live',
    stale: 'Stale',
    reconnecting: 'Reconnecting…',
    hidden: 'Paused',
    error: 'Offline',
  }[tailStatus];

  const statusColor = {
    idle: 'neutral',
    connecting: 'info',
    live: 'ok',
    stale: 'warn',
    reconnecting: 'warn',
    hidden: 'neutral',
    error: 'danger',
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
    const blob = new Blob([JSON.stringify(rows, null, 2)], {
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
      <Section
        title="Live Logs"
        className="flex-1 min-h-0"
        subtitle={
          <span className="flex items-center gap-2">
            <span>
              {rows.length} requests — {tailing ? 'live tailing' : 'paged'}
            </span>
            {tailing ? (
              <span className="inline-flex items-center gap-1 px-1.5 py-0.5 text-[10px] uppercase tracking-wider border border-subtle rounded-sm">
                <span
                  className={cx(
                    'status-dot',
                    statusColor,
                    tailStatus === 'connecting' || tailStatus === 'reconnecting'
                      ? 'animate-pulse'
                      : '',
                  )}
                />
                <span>{statusLabel}</span>
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
              <select
                className={`${INPUT_CLASS} w-44`}
                value={filters.principal_id ?? ''}
                onChange={(e) => setFilter('principal_id', e.target.value)}
              >
                <option value="">All principals</option>
                {Array.from(principalNameMap.entries()).map(([id, name]) => (
                  <option key={id} value={id}>
                    {name}
                  </option>
                ))}
              </select>
            </Field>
            <Field label="Upstream">
              <select
                className={`${INPUT_CLASS} w-44`}
                value={filters.upstream ?? ''}
                onChange={(e) => setFilter('upstream', e.target.value)}
              >
                <option value="">All upstreams</option>
                {upstreams.data?.upstreams.map((u) => (
                  <option key={u.id} value={u.name}>
                    {u.name}
                  </option>
                ))}
              </select>
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
              <select
                className={`${INPUT_CLASS} w-32`}
                value={filters.status ?? ''}
                onChange={(e) => setFilter('status', e.target.value)}
              >
                <option value="">All statuses</option>
                <option value="200">2xx</option>
                <option value="429">429</option>
                <option value="500">5xx</option>
              </select>
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
              events={rows}
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
              minWidthClass="min-w-[980px]"
              emptyTitle="No requests"
              emptyDescription="Adjust filters or enable live tail."
            />
          </div>
        </Card>
      </Section>
    </FullPage>
  );
}
