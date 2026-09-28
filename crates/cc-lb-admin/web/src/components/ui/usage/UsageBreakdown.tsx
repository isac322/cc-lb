import { fmtN, formatCostMicros } from '../../../lib/format';
import type { RequestEventWithPhase } from '../../../lib/RequestEventTypes';
import { StackedBar } from '../StackedBar';
import { USAGE_CATEGORIES, type UsageCategoryKey } from './sliceColors';

interface UsageRow {
  key: string;
  label: string;
  value: number;
  color: string;
  formatted: string;
}

/**
 * Request drawer breakdown: an 8px stacked bar over a value table (swatch,
 * label, value, share) with the total as the last row. Replaces the donut
 * charts, which spent 190px on a two-slice request.
 */
function UsageBreakdown({
  rows,
  total,
  ariaLabel,
  emptyMessage,
}: {
  rows: UsageRow[];
  total: string;
  ariaLabel: string;
  emptyMessage: string;
}) {
  const sum = rows.reduce((acc, row) => acc + row.value, 0);
  if (rows.length === 0 || sum <= 0) {
    return <p className="text-body-sm text-text-muted">{emptyMessage}</p>;
  }
  return (
    <div className="min-w-0 space-y-3">
      <StackedBar
        segments={rows}
        ariaLabel={`${ariaLabel}: ${rows
          .map((row) => `${row.label} ${row.formatted}`)
          .join(', ')}`}
      />
      <table className="w-full text-body-sm tabular-nums">
        <tbody>
          {rows.map((row) => (
            <tr key={row.key}>
              <td className="py-0.5 pr-2">
                <span className="flex min-w-0 items-center gap-2 text-text-muted">
                  <span
                    aria-hidden="true"
                    className="h-2 w-2 shrink-0 rounded-xs"
                    style={{ backgroundColor: row.color }}
                  />
                  <span className="truncate">{row.label}</span>
                </span>
              </td>
              <td className="py-0.5 text-right text-text whitespace-nowrap">
                {row.formatted}
              </td>
              <td className="w-10 py-0.5 text-right text-text-faint">
                {Math.round((row.value / sum) * 100)}%
              </td>
            </tr>
          ))}
          <tr className="border-t border-row">
            <td className="pt-1.5 pr-2 text-text-muted">Total</td>
            <td className="pt-1.5 text-right font-medium text-text whitespace-nowrap">
              {total}
            </td>
            <td className="w-10" />
          </tr>
        </tbody>
      </table>
    </div>
  );
}

export function TokenBreakdown({ event }: { event: RequestEventWithPhase }) {
  const cache5m = event.cache_creation_input_tokens_5m;
  const cache1h = event.cache_creation_input_tokens_1h;
  const hasSplit = cache5m != null || cache1h != null;
  const tokens: Record<UsageCategoryKey, number> = {
    cache_read: event.cache_read_input_tokens ?? 0,
    cache_create_5m: hasSplit
      ? (cache5m ?? 0)
      : (event.cache_creation_input_tokens ?? 0),
    cache_create_1h: hasSplit ? (cache1h ?? 0) : 0,
    input: event.input_tokens ?? 0,
    output: event.output_tokens ?? 0,
  };
  const rows = USAGE_CATEGORIES.map((category) => ({
    key: category.key,
    // A row recorded before the 5m / 1h split carries one unsplit total.
    label:
      category.key === 'cache_create_5m' && !hasSplit
        ? 'Cache creation'
        : category.label,
    value: tokens[category.key],
    color: category.color,
  }))
    .filter((row) => row.value > 0)
    .map((row) => ({ ...row, formatted: fmtN(row.value) }));
  const totalTokens = rows.reduce((sum, row) => sum + row.value, 0);

  return (
    <UsageBreakdown
      rows={rows}
      total={fmtN(totalTokens)}
      ariaLabel="Token usage"
      emptyMessage="No token usage"
    />
  );
}

export function CostBreakdown({ event }: { event: RequestEventWithPhase }) {
  const micros: Record<UsageCategoryKey, number> = {
    cache_read: event.cost_cache_read_micros ?? 0,
    cache_create_5m: event.cost_cache_creation_5m_micros ?? 0,
    cache_create_1h: event.cost_cache_creation_1h_micros ?? 0,
    input: event.cost_input_micros ?? 0,
    output: event.cost_output_micros ?? 0,
  };
  const rows = USAGE_CATEGORIES.map((category) => ({
    key: category.key,
    label: category.label,
    value: micros[category.key],
    color: category.color,
  }))
    .filter((row) => row.value > 0)
    .map((row) => ({ ...row, formatted: formatCostMicros(row.value) }));
  const totalMicros =
    event.cost_usd_micros ?? rows.reduce((sum, row) => sum + row.value, 0);

  return (
    <UsageBreakdown
      rows={rows}
      total={formatCostMicros(totalMicros)}
      ariaLabel="Cost breakdown"
      emptyMessage="No cost"
    />
  );
}
