import { Link } from '@tanstack/react-router';
import { memo, useMemo } from 'react';
import { ProviderLogo } from '../components/ui/ProviderLogo';
import { Badge, cx, Skeleton } from '../components/ui/primitives';
import {
  PoolQuotaLegend,
  PoolQuotaThemedChart,
} from '../designs/default/OverviewPage';
import type { AggregateResponse, PoolHistoryWindowResponse } from '../lib/api';
import { useTimezone } from '../lib/locale';
import { PROVIDER_LABELS, type ProviderFamily } from '../lib/providers';
import {
  useSubscriptionQuotaAggregate,
  useSubscriptionQuotaPoolHistory,
} from '../lib/queries';
import { formatQuotaPercent } from '../lib/quotaSeverity';
import { formatInTimezone } from '../lib/timezone';
import {
  buildPoolQuotaChartData,
  formatAgo,
  freshProviderPoolQuotaChartData,
  latestPoolQuotaSnapshotUnix,
  type PoolQuotaChartRow,
  type PoolQuotaLatest,
  type PoolQuotaWindow,
  poolQuotaResponseLatest,
  poolQuotaResponseObservation,
  poolQuotaWindowsPresent,
  poolWindowResets,
  visiblePoolQuotaChartData,
} from '../routes/-overviewPoolQuota';

const PROVIDER_WINDOW_KEYS: Record<ProviderFamily, readonly PoolQuotaWindow[]> =
  {
    anthropic: ['5h', '7d', '7d_fable'],
    openai: ['5h', '7d'],
    gemini: ['gemini_5h', 'gemini_7d', 'third_party_5h', 'third_party_7d'],
  };

/**
 * Keep only pool windows with current aggregate or history data. An explicit
 * absent provider lot is not a missing reading: it removes that window even if
 * the history endpoint still carries older buckets.
 */
export function presentProviderWindows(
  defaults: readonly PoolQuotaWindow[],
  aggregate: AggregateResponse | undefined,
  history: readonly PoolHistoryWindowResponse[] | undefined,
  loading: boolean,
): PoolQuotaWindow[] {
  if (loading) return [...defaults];
  return poolQuotaWindowsPresent(history, aggregate, defaults, true);
}

function providerMetricHeadline(
  latest: PoolQuotaLatest | undefined,
  visibleWindows: readonly PoolQuotaWindow[],
): string | null {
  const allowed = new Set(visibleWindows);
  if (!latest) return null;
  const values = Object.entries(latest)
    .filter(([key]) => allowed.has(key))
    .map(([, value]) => value)
    .filter((value): value is number => value != null);
  return values.length > 0 ? formatQuotaPercent(Math.max(...values)) : null;
}

export interface ProviderPoolCardProps {
  provider: ProviderFamily;
  hasSubscription: boolean;
  rangeSecs: number;
  compact?: boolean;
}

export const ProviderPoolCard = memo(function ProviderPoolCard({
  provider,
  hasSubscription,
  rangeSecs,
  compact = false,
}: ProviderPoolCardProps) {
  const { effective: timeZone } = useTimezone();
  const providerWindowKeys = PROVIDER_WINDOW_KEYS[provider];
  const windows = providerWindowKeys.join(',');
  const aggregate = useSubscriptionQuotaAggregate({
    provider,
    source: 'merged',
    windows,
    enabled: hasSubscription,
  });
  const history = useSubscriptionQuotaPoolHistory({
    provider,
    windows,
    rangeSecs,
    windowQuantumSecs: 1800,
    maxPointsPerSeries: 1000,
    enabled: hasSubscription,
  });
  const normalizedHistory = history.data?.windows;
  const chartData = useMemo(
    () => buildPoolQuotaChartData(normalizedHistory),
    [normalizedHistory],
  );
  const loading = hasSubscription && (aggregate.isPending || history.isPending);
  const normalizedAggregate = aggregate.data;
  const visibleWindows = useMemo(
    () =>
      presentProviderWindows(
        providerWindowKeys,
        normalizedAggregate,
        normalizedHistory,
        loading,
      ),
    [loading, normalizedAggregate, normalizedHistory, providerWindowKeys],
  );
  const latestSnapshotUnixSecs = useMemo(
    () => latestPoolQuotaSnapshotUnix(normalizedHistory),
    [normalizedHistory],
  );
  const rangeEndUnix =
    history.data?.now_unix_secs ?? Math.floor(Date.now() / 1000);
  const rangeStartUnix = rangeEndUnix - (history.data?.range_secs ?? rangeSecs);
  const visibleChartData = useMemo(() => {
    // Provider history is retained evidence, not a replay of the live reading.
    // Recovery must not paint over a recorded stale interval.
    const rows =
      provider === 'anthropic'
        ? visiblePoolQuotaChartData(
            chartData,
            rangeStartUnix,
            rangeEndUnix,
            latestSnapshotUnixSecs,
            normalizedHistory,
          )
        : freshProviderPoolQuotaChartData(
            chartData,
            rangeStartUnix,
            rangeEndUnix,
            normalizedHistory,
          );
    return rows.map((row) => {
      const sparseRow: PoolQuotaChartRow = { unix: row.unix };
      for (const window of visibleWindows) {
        if (window in row) sparseRow[window] = row[window];
      }
      return sparseRow;
    });
  }, [
    chartData,
    latestSnapshotUnixSecs,
    normalizedHistory,
    provider,
    rangeEndUnix,
    rangeStartUnix,
    visibleWindows,
  ]);
  const latest = useMemo(
    () =>
      poolQuotaResponseLatest(normalizedHistory, visibleChartData, {
        requireVisibleHistory: provider === 'anthropic',
      }),
    [normalizedHistory, provider, visibleChartData],
  );
  const headline = providerMetricHeadline(
    provider === 'anthropic' ? visibleChartData.at(-1) : latest,
    visibleWindows,
  );
  const resets = poolWindowResets(normalizedAggregate);
  const error = hasSubscription && (aggregate.isError || history.isError);
  const weighting =
    normalizedAggregate && 'weighting' in normalizedAggregate
      ? normalizedAggregate.weighting
      : undefined;
  const nowUnixSecs =
    normalizedAggregate?.now_unix_secs ?? Math.floor(Date.now() / 1000);
  const observation = useMemo(
    () =>
      provider === 'anthropic'
        ? { observedAtUnixSecs: null, stale: false }
        : poolQuotaResponseObservation(normalizedHistory, visibleWindows),
    [normalizedHistory, provider, visibleWindows],
  );
  const observationNow =
    provider === 'anthropic'
      ? nowUnixSecs
      : (history.data?.now_unix_secs ?? nowUnixSecs);
  const showHeadline =
    compact &&
    (provider === 'anthropic'
      ? headline != null
      : hasSubscription && !loading && !error && visibleWindows.length > 0);

  return (
    <article
      className="min-w-0 rounded-md border border-subtle bg-panel"
      data-testid={`provider-pool-${provider}`}
      title={
        weighting === 'equal'
          ? 'Pool quota is equal-weighted across upstreams'
          : undefined
      }
    >
      <header className="flex items-start justify-between gap-3 border-b border-subtle px-4 py-3">
        <div className="min-w-0">
          <Link
            className="flex min-h-10 min-w-0 items-center gap-2 py-2.5 -my-2.5 text-title-card text-text hover:underline focus-visible:outline-2 focus-visible:outline-accent"
            params={{ provider }}
            to="/providers/$provider"
          >
            <ProviderLogo
              className="shrink-0 text-text-muted"
              provider={provider}
            />
            <span className="truncate">{PROVIDER_LABELS[provider]}</span>
          </Link>
        </div>
        {showHeadline ? (
          <span
            className={cx(
              'inline-flex shrink-0 items-baseline gap-1 text-display tabular-nums',
              headline == null ? 'text-text-faint' : 'text-text',
            )}
            data-slot="provider-pool-headline"
            aria-label={
              headline == null ? 'Current pool usage unavailable' : undefined
            }
          >
            {headline ?? '—'}
            {headline != null ? (
              <span className="text-caption text-text-muted">used</span>
            ) : null}
          </span>
        ) : null}
      </header>
      <div className="p-4">
        {!hasSubscription ? (
          <p className="py-8 text-body-sm text-text-muted">
            No subscription quota - API keys only
          </p>
        ) : error ? (
          <p className="py-8 text-body-sm text-danger-text" role="alert">
            Quota data could not be loaded. Refresh to try again.
          </p>
        ) : (
          <>
            <div className="mb-3">
              <PoolQuotaLegend
                latest={latest}
                loading={loading}
                nowUnixSecs={nowUnixSecs}
                resets={resets}
                timeZone={timeZone}
                windows={visibleWindows}
              />
              {weighting === 'equal' ? (
                <span className="sr-only">
                  Pool quota is equal-weighted across upstreams.
                </span>
              ) : null}
            </div>
            <div
              className={cx(
                'relative w-full min-w-0',
                compact ? 'h-40' : 'h-56',
              )}
            >
              {loading ? (
                <Skeleton className="size-full" />
              ) : (
                <PoolQuotaThemedChart
                  rangeEndUnix={rangeEndUnix}
                  rangeStartUnix={rangeStartUnix}
                  seriesData={visibleChartData}
                  windows={visibleWindows}
                />
              )}
            </div>
            {!loading &&
            (observation.observedAtUnixSecs != null || observation.stale) ? (
              <div className="mt-3 flex flex-wrap items-center gap-x-2 gap-y-1.5 text-caption tabular-nums text-text-muted">
                {observation.observedAtUnixSecs != null ? (
                  <span
                    title={formatInTimezone(
                      observation.observedAtUnixSecs * 1000,
                      timeZone,
                    )}
                  >
                    {`Provider data ${formatAgo(observation.observedAtUnixSecs, observationNow)}`}
                  </span>
                ) : null}
                {observation.stale ? (
                  <Badge tone="warn">Stale reading</Badge>
                ) : null}
              </div>
            ) : null}
          </>
        )}
      </div>
    </article>
  );
});
