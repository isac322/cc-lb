import { formatCostMicros } from '../../lib/format';
import type { RequestEventWithPhase } from '../../lib/RequestEventTypes';
import { BreakdownPopover } from './BreakdownPopover';
import { cx, Hint } from './primitives';
import {
  type CostComponentMicros,
  costCategorySegments,
  sumCostMicros,
} from './usage/costCategories';

const DASH = '—';

type CostBreakdownT = {
  components: CostComponentMicros;
  total: number;
  hasComponents: boolean;
};

function costBreakdown(e: RequestEventWithPhase): CostBreakdownT {
  const components: CostComponentMicros = {
    input: e.cost_input_micros ?? 0,
    output: e.cost_output_micros ?? 0,
    cache_create_5m: e.cost_cache_creation_5m_micros ?? 0,
    cache_create_1h: e.cost_cache_creation_1h_micros ?? 0,
    cache_read: e.cost_cache_read_micros ?? 0,
  };
  const hasComponents =
    e.cost_input_micros != null ||
    e.cost_output_micros != null ||
    e.cost_cache_creation_5m_micros != null ||
    e.cost_cache_creation_1h_micros != null ||
    e.cost_cache_read_micros != null;
  const total = e.cost_usd_micros ?? sumCostMicros(components);
  return { components, total, hasComponents };
}

export function CostCell({
  event,
  isPartial,
  className,
}: {
  event: RequestEventWithPhase;
  isPartial?: boolean;
  className?: string;
}) {
  const c = costBreakdown(event);
  const segments = costCategorySegments(c.components);

  if (!isPartial && event.cost_usd_micros == null && !c.hasComponents) {
    return (
      <td
        className={cx(
          'px-3 py-2 text-right tabular-nums whitespace-nowrap',
          className,
        )}
      >
        <span className="text-text-faint">{DASH}</span>
      </td>
    );
  }

  const popover = (
    <BreakdownPopover
      title={isPartial ? 'Estimated cost' : 'Cost'}
      showZeroRows={true}
      isPartial={isPartial}
      rows={segments.map((segment) => ({
        label: segment.label,
        value: segment.value,
        color: segment.color,
        fmt: formatCostMicros,
      }))}
      footer={{ label: 'Total', value: c.total, fmt: formatCostMicros }}
    />
  );

  return (
    <td
      className={cx(
        'px-3 py-2 text-right tabular-nums whitespace-nowrap',
        className,
      )}
      onClick={(e) => e.stopPropagation()}
    >
      <Hint label={popover}>
        <span
          className={cx(
            'cursor-help text-text',
            isPartial ? 'animate-pulse' : '',
          )}
        >
          {isPartial
            ? `Est. ${c.total > 0 ? formatCostMicros(c.total) : '—'}`
            : formatCostMicros(c.total)}
        </span>
      </Hint>
    </td>
  );
}
