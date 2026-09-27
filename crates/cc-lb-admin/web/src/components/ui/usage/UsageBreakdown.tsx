import { fmtN, formatCostMicros } from '../../../lib/format';
import type { RequestEventWithPhase } from '../../../lib/RequestEventTypes';
import { StackedBar } from '../StackedBar';
import { SLICE_COLORS } from './sliceColors';

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
  const rows = [
    {
      key: 'input',
      label: 'Input',
      value: event.input_tokens ?? 0,
      color: SLICE_COLORS.input,
    },
    {
      key: 'output',
      label: 'Output',
      value: event.output_tokens ?? 0,
      color: SLICE_COLORS.output,
    },
    ...(hasSplit
      ? [
          {
            key: 'cache_create_5m',
            label: 'Cache create 5m',
            value: cache5m ?? 0,
            color: SLICE_COLORS.cache_create_5m,
          },
          {
            key: 'cache_create_1h',
            label: 'Cache create 1h',
            value: cache1h ?? 0,
            color: SLICE_COLORS.cache_create_1h,
          },
        ]
      : [
          {
            key: 'cache_creation',
            label: 'Cache creation',
            value: event.cache_creation_input_tokens ?? 0,
            color: SLICE_COLORS.cache_create_5m,
          },
        ]),
    {
      key: 'cache_read',
      label: 'Cache read',
      value: event.cache_read_input_tokens ?? 0,
      color: SLICE_COLORS.cache_read,
    },
  ]
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
  const rows = [
    {
      key: 'input',
      label: 'Input',
      value: event.cost_input_micros ?? 0,
      color: SLICE_COLORS.input,
    },
    {
      key: 'output',
      label: 'Output',
      value: event.cost_output_micros ?? 0,
      color: SLICE_COLORS.output,
    },
    {
      key: 'cache_create_5m',
      label: 'Cache create 5m',
      value: event.cost_cache_creation_5m_micros ?? 0,
      color: SLICE_COLORS.cache_create_5m,
    },
    {
      key: 'cache_create_1h',
      label: 'Cache create 1h',
      value: event.cost_cache_creation_1h_micros ?? 0,
      color: SLICE_COLORS.cache_create_1h,
    },
    {
      key: 'cache_read',
      label: 'Cache read',
      value: event.cost_cache_read_micros ?? 0,
      color: SLICE_COLORS.cache_read,
    },
  ]
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
