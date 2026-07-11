import { formatCostMicros } from '../../lib/format';
import type { RequestEventWithPhase } from '../../lib/RequestEventTypes';
import { BreakdownPopover } from './BreakdownPopover';
import { cx, Hint } from './primitives';
import { Sparkline } from './Sparkline';
import { SLICE_COLORS } from './usage/sliceColors';

const DASH = '—';

type CostBreakdownT = {
  input: number;
  output: number;
  cc_5m: number;
  cc_1h: number;
  cr: number;
  total: number;
  hasComponents: boolean;
};

function costBreakdown(e: RequestEventWithPhase): CostBreakdownT {
  const input = e.cost_input_micros ?? 0;
  const output = e.cost_output_micros ?? 0;
  const cc_5m = e.cost_cache_creation_5m_micros ?? 0;
  const cc_1h = e.cost_cache_creation_1h_micros ?? 0;
  const cr = e.cost_cache_read_micros ?? 0;
  const hasComponents =
    e.cost_input_micros != null ||
    e.cost_output_micros != null ||
    e.cost_cache_creation_5m_micros != null ||
    e.cost_cache_creation_1h_micros != null ||
    e.cost_cache_read_micros != null;
  const total = e.cost_usd_micros ?? input + output + cc_5m + cc_1h + cr;
  return { input, output, cc_5m, cc_1h, cr, total, hasComponents };
}

export function CostCell({
  event,
  isPartial,
}: {
  event: RequestEventWithPhase;
  isPartial?: boolean;
}) {
  const c = costBreakdown(event);

  if (!isPartial && event.cost_usd_micros == null && !c.hasComponents) {
    return (
      <td className="px-3 py-2 text-right tabular-nums whitespace-nowrap text-text-faint">
        {DASH}
      </td>
    );
  }

  const popover = (
    <BreakdownPopover
      title={isPartial ? 'Estimated Cost' : 'Cost'}
      showZeroRows={true}
      isPartial={isPartial}
      rows={[
        {
          label: 'Input',
          value: c.input,
          color: SLICE_COLORS.input,
          fmt: formatCostMicros,
        },
        {
          label: 'Output',
          value: c.output,
          color: SLICE_COLORS.output,
          fmt: formatCostMicros,
        },
        {
          label: 'Cache create 5m',
          value: c.cc_5m,
          color: SLICE_COLORS.cache_create_5m,
          fmt: formatCostMicros,
        },
        {
          label: 'Cache create 1h',
          value: c.cc_1h,
          color: SLICE_COLORS.cache_create_1h,
          fmt: formatCostMicros,
        },
        {
          label: 'Cache read',
          value: c.cr,
          color: SLICE_COLORS.cache_read,
          fmt: formatCostMicros,
        },
      ]}
      footer={{ label: 'Total', value: c.total, fmt: formatCostMicros }}
    />
  );

  return (
    <td
      className="p-0 text-right whitespace-nowrap"
      onClick={(e) => e.stopPropagation()}
    >
      <Hint label={popover}>
        <div
          className={cx(
            'px-3 py-2 cursor-help block',
            isPartial ? 'animate-pulse' : '',
          )}
        >
          <div className="text-right tabular-nums leading-tight">
            {isPartial
              ? `Est. ${c.total > 0 ? formatCostMicros(c.total) : '—'}`
              : formatCostMicros(c.total)}
          </div>
          {c.hasComponents ? (
            <Sparkline
              segments={[
                { value: c.input, color: SLICE_COLORS.input },
                { value: c.output, color: SLICE_COLORS.output },
                { value: c.cc_5m, color: SLICE_COLORS.cache_create_5m },
                { value: c.cc_1h, color: SLICE_COLORS.cache_create_1h },
                { value: c.cr, color: SLICE_COLORS.cache_read },
              ]}
            />
          ) : (
            <div className="mt-1 h-1" />
          )}
        </div>
      </Hint>
    </td>
  );
}
