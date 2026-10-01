import { formatCostMicros } from '../../lib/format';
import type { RequestEventWithPhase } from '../../lib/RequestEventTypes';
import { BreakdownPopover } from './BreakdownPopover';
import { CostFigure } from './CostFigure';
import { EmptyMetricCell, MetricCell } from './MetricCell';
import { cx } from './primitives';
import {
  type CostComponentMicros,
  costCategorySegments,
  sumCostMicros,
} from './usage/costCategories';

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

/*
 * Cost is the request table's last column, so the cell already carries the
 * table's right inset; the trigger's own right padding would push the figure
 * a further 12px in from the right-aligned "Cost" header. Dropping it ends
 * the last digit (and the bar) on the same edge as the header in the full,
 * scrolling and pane-band layouts. The trigger keeps its left padding, so it
 * stays a full-size button.
 */
const COST_EDGE_CLASS = '*:pr-0!';

/**
 * Request cost over a bar of what it paid for, in the token bar's categories
 * and order (`USAGE_CATEGORIES`). Cost no category accounts for is left as
 * bare track. The figure is right-aligned on the column's edge, so every
 * row's cost ends where the header does.
 */
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

  const cellClassName = cx(className, COST_EDGE_CLASS);

  if (!isPartial && event.cost_usd_micros == null && !c.hasComponents) {
    return <EmptyMetricCell className={cellClassName} />;
  }

  const popover = (
    <BreakdownPopover
      title={isPartial ? 'Estimated cost' : 'Cost'}
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
  const text = isPartial
    ? `Est. ${c.total > 0 ? formatCostMicros(c.total) : '—'}`
    : formatCostMicros(c.total);

  return (
    <MetricCell
      className={cellClassName}
      label={`Cost ${text}, show breakdown`}
      popover={popover}
      segments={segments}
      total={c.total}
      pulse={isPartial}
    >
      <CostFigure text={text} />
    </MetricCell>
  );
}
