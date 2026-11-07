import { Link } from '@tanstack/react-router';
import { ArrowDown, ArrowUp } from 'lucide-react';
import { memo, useMemo, useState } from 'react';
import { cx, INPUT_SM_CLASS, Skeleton } from '../components/ui/primitives';
import {
  Table,
  TableCell,
  TableEmptyRow,
  TableHead,
  TableHeadCell,
  TableRow,
} from '../components/ui/Table';
import type { ProviderFamily } from '../lib/providers';
import { PrincipalUsageRow } from './PrincipalUsageRow';
import type {
  MergedPrincipalUsage,
  PrincipalProviderMetric,
} from './principalMerge';

const PROVIDER_ORDER: readonly ProviderFamily[] = [
  'anthropic',
  'openai',
  'gemini',
];
const providerLabel: Record<ProviderFamily, string> = {
  anthropic: 'Anthropic',
  openai: 'OpenAI',
  gemini: 'Gemini',
};
// Inherit the row's opaque, transitioning fill so sticky and scrolling cells
// paint the same surface at every point in the hover transition.
const PRINCIPAL_BODY_CELL_CLASS = 'bg-inherit';

function fmtCost(micros: number): string {
  return `$${(micros / 1_000_000).toFixed(2)}`;
}
function fmtCache(value: number | null): string {
  return value == null ? '—' : `${Math.floor(value * 100)}%`;
}
function metricValue(
  metric: PrincipalProviderMetric | undefined,
  metricKey: 'cost' | 'cache',
): string {
  if (!metric) return '—';
  return metricKey === 'cost'
    ? fmtCost(metric.costMicros)
    : fmtCache(metric.cacheHitRatio);
}
function sortValue(row: MergedPrincipalUsage): number {
  return row.totalCostMicros;
}
function aggregateProviderMetrics(
  rows: readonly MergedPrincipalUsage[],
  provider: ProviderFamily,
): PrincipalProviderMetric {
  const metrics = rows
    .map((row) => row.providers[provider])
    .filter((metric): metric is PrincipalProviderMetric => metric != null);
  const promptTokens = metrics.reduce(
    (sum, metric) => sum + metric.promptTokens,
    0,
  );
  return {
    costMicros: metrics.reduce((sum, metric) => sum + metric.costMicros, 0),
    cacheHitRatio:
      promptTokens > 0
        ? metrics.reduce(
            (sum, metric) =>
              sum + (metric.cacheHitRatio ?? 0) * metric.promptTokens,
            0,
          ) / promptTokens
        : null,
    requests: metrics.reduce((sum, metric) => sum + metric.requests, 0),
    tokens: metrics.reduce((sum, metric) => sum + metric.tokens, 0),
    promptTokens,
  };
}

export const PrincipalProviderTable = memo(function PrincipalProviderTable({
  providers,
  rows,
  loading,
  error,
}: {
  providers: readonly ProviderFamily[];
  rows: readonly MergedPrincipalUsage[];
  loading: boolean;
  error: boolean;
}) {
  const [descending, setDescending] = useState(true);
  const [filter, setFilter] = useState('');
  const activeProviders = useMemo(
    () => PROVIDER_ORDER.filter((provider) => providers.includes(provider)),
    [providers],
  );
  const sortedRows = useMemo(() => {
    const needle = filter.trim().toLowerCase();
    return [...rows]
      .filter((row) => !needle || row.name.toLowerCase().includes(needle))
      .sort((left, right) => {
        const difference = sortValue(left) - sortValue(right);
        return (
          (descending ? -1 : 1) * difference ||
          left.name.localeCompare(right.name)
        );
      });
  }, [descending, filter, rows]);
  const providerTotals = useMemo(
    () =>
      Object.fromEntries(
        activeProviders.map((provider) => [
          provider,
          aggregateProviderMetrics(rows, provider),
        ]),
      ) as Record<ProviderFamily, PrincipalProviderMetric>,
    [activeProviders, rows],
  );
  const totalCostMicros = activeProviders.reduce(
    (sum, provider) => sum + providerTotals[provider].costMicros,
    0,
  );
  const sort = () => setDescending((value) => !value);
  const header = (label: string, className?: string) => {
    const Arrow = descending ? ArrowDown : ArrowUp;
    return (
      <TableHeadCell
        aria-sort={descending ? 'descending' : 'ascending'}
        className={className}
        numeric
      >
        <button
          className="inline-flex min-h-10 items-center gap-1 whitespace-nowrap hover:text-text focus-visible:outline-2 focus-visible:outline-accent"
          onClick={sort}
          type="button"
        >
          <Arrow aria-hidden="true" className="size-3" strokeWidth={1.75} />
          {label}
        </button>
      </TableHeadCell>
    );
  };

  if (error)
    return (
      <p className="p-4 text-body-sm text-danger-text" role="alert">
        Principal usage could not be loaded. Refresh to try again.
      </p>
    );
  if (loading)
    return (
      <div className="space-y-3 p-4" aria-busy="true">
        {Array.from({ length: 5 }, (_, index) => (
          <Skeleton className="h-8 w-full" key={index} />
        ))}
      </div>
    );

  return (
    <div className="min-w-0">
      {rows.length > 8 ? (
        <div className="flex flex-wrap items-center gap-3 border-b border-row p-3">
          <input
            aria-label="Filter principals by name"
            className={cx(INPUT_SM_CLASS, 'w-full sm:w-64')}
            onChange={(event) => setFilter(event.target.value)}
            placeholder="Filter by name"
            type="search"
            value={filter}
          />
        </div>
      ) : null}
      <div className="min-w-0 overflow-x-auto overscroll-x-contain">
        <Table className="w-full table-auto">
          <TableHead className="bg-bg">
            <tr>
              <TableHeadCell className="sticky left-0 z-20 min-w-24 bg-bg">
                Principal
              </TableHeadCell>
              {header('Total cost')}
              <TableHeadCell>Provider share</TableHeadCell>
              {activeProviders.map((provider) => (
                <TableHeadCell key={provider}>
                  {providerLabel[provider]}
                </TableHeadCell>
              ))}
            </tr>
          </TableHead>
          <tbody>
            {sortedRows.length === 0 ? (
              <TableEmptyRow colSpan={3 + activeProviders.length}>
                No principals in this range
              </TableEmptyRow>
            ) : (
              sortedRows.map((row) => (
                <PrincipalUsageRow
                  // Opaque fill instead of the shared translucent `interactive`
                  // hover: the inheriting cells would stack the translucency
                  // and the sticky cell would show scrolled content through it.
                  className="group cursor-pointer bg-bg hover:bg-row-hover-solid"
                  data-testid="multi-provider-principal-row"
                  key={row.id}
                  principalId={row.id}
                >
                  <TableCell
                    className={`sticky left-0 z-10 min-w-24 max-w-0 ${PRINCIPAL_BODY_CELL_CLASS}`}
                  >
                    <Link
                      className="relative -my-2 flex min-h-10 min-w-0 items-center truncate text-text underline decoration-transparent underline-offset-4 after:absolute after:-inset-2 after:content-[''] hover:decoration-border-strong focus-visible:outline-2 focus-visible:outline-accent"
                      data-principal-link
                      search={{ selectedId: row.id }}
                      title={row.name}
                      to="/principals"
                    >
                      {row.name}
                    </Link>
                  </TableCell>
                  <TableCell className={PRINCIPAL_BODY_CELL_CLASS} numeric>
                    {fmtCost(row.totalCostMicros)}
                  </TableCell>
                  <TableCell className={PRINCIPAL_BODY_CELL_CLASS}>
                    <div className="flex min-w-24 items-center gap-2">
                      <span className="h-1.5 min-w-0 flex-1 bg-progress-track">
                        <span
                          className="block h-full bg-text-muted"
                          style={{
                            width: `${Math.min(100, Math.max(0, row.sharePct))}%`,
                          }}
                        />
                      </span>
                      <span className="text-caption tabular-nums text-text-muted">
                        {row.sharePct.toFixed(0)}%
                      </span>
                    </div>
                  </TableCell>
                  {activeProviders.map((provider) => (
                    <TableCell
                      className={`${PRINCIPAL_BODY_CELL_CLASS} whitespace-nowrap`}
                      key={provider}
                    >
                      <span className="tabular-nums">
                        {metricValue(row.providers[provider], 'cost')}
                      </span>
                      <span className="ml-1 text-caption text-text-muted">
                        / {metricValue(row.providers[provider], 'cache')}
                      </span>
                    </TableCell>
                  ))}
                </PrincipalUsageRow>
              ))
            )}
          </tbody>
          <tfoot>
            <TableRow className="border-t border-subtle bg-bg">
              <TableCell
                className={`${PRINCIPAL_BODY_CELL_CLASS} sticky left-0 z-10 min-w-24 text-label text-text`}
              >
                Total
              </TableCell>
              <TableCell className={PRINCIPAL_BODY_CELL_CLASS} numeric>
                {fmtCost(totalCostMicros)}
              </TableCell>
              <TableCell className={PRINCIPAL_BODY_CELL_CLASS} numeric>
                100%
              </TableCell>
              {activeProviders.map((provider) => (
                <TableCell
                  className={`${PRINCIPAL_BODY_CELL_CLASS} whitespace-nowrap`}
                  key={provider}
                >
                  {fmtCost(providerTotals[provider].costMicros)} /{' '}
                  {fmtCache(providerTotals[provider].cacheHitRatio)}
                </TableCell>
              ))}
            </TableRow>
          </tfoot>
        </Table>
      </div>
    </div>
  );
});
