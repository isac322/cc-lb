import { useMemo } from 'react';
import type { RequestEvent } from '../../../lib/api';
import { fmtN, fmtUsd } from '../../../lib/format';
import { type ActiveSliceControl, PieChart, type PieSlice } from './PieChart';
import { SLICE_COLORS } from './sliceColors';

export function buildCostSlices(event: RequestEvent): PieSlice[] {
  const rows: {
    key: string;
    label: string;
    costMicros: number;
    tokens: number | null;
    color: string;
  }[] = [
    {
      key: 'input',
      label: 'Input',
      costMicros: event.cost_input_micros ?? 0,
      tokens: event.input_tokens ?? null,
      color: SLICE_COLORS.input,
    },
    {
      key: 'output',
      label: 'Output',
      costMicros: event.cost_output_micros ?? 0,
      tokens: event.output_tokens ?? null,
      color: SLICE_COLORS.output,
    },
    {
      key: 'cache_create_5m',
      label: 'Cache create 5m',
      costMicros: event.cost_cache_creation_5m_micros ?? 0,
      tokens: event.cache_creation_input_tokens_5m ?? null,
      color: SLICE_COLORS.cache_create_5m,
    },
    {
      key: 'cache_create_1h',
      label: 'Cache create 1h',
      costMicros: event.cost_cache_creation_1h_micros ?? 0,
      tokens: event.cache_creation_input_tokens_1h ?? null,
      color: SLICE_COLORS.cache_create_1h,
    },
    {
      key: 'cache_read',
      label: 'Cache read',
      costMicros: event.cost_cache_read_micros ?? 0,
      tokens: event.cache_read_input_tokens ?? null,
      color: SLICE_COLORS.cache_read,
    },
  ];

  return rows
    .filter((r) => r.costMicros > 0)
    .map<PieSlice>((r) => ({
      key: r.key,
      label: r.label,
      value: r.costMicros,
      color: r.color,
      primary: fmtUsd(r.costMicros),
      secondary:
        r.tokens != null && r.tokens > 0 ? `${fmtN(r.tokens)} tk` : undefined,
      aria: `${r.label} · ${fmtUsd(r.costMicros)}${r.tokens != null && r.tokens > 0 ? ` · ${fmtN(r.tokens)} tokens` : ''}`,
    }));
}

export function CostPie({
  event,
  control,
}: {
  event: RequestEvent;
  control?: ActiveSliceControl;
}) {
  const slices = useMemo(() => buildCostSlices(event), [event]);
  const totalMicros = slices.reduce((sum, s) => sum + s.value, 0);
  const totalUsd = event.cost_usd_micros ?? totalMicros;
  const totalTokens =
    (event.input_tokens ?? 0) +
    (event.output_tokens ?? 0) +
    (event.cache_creation_input_tokens ?? 0) +
    (event.cache_read_input_tokens ?? 0);
  const totalSecondary =
    totalTokens > 0 ? `${fmtN(totalTokens)} tk` : undefined;

  return (
    <PieChart
      slices={slices}
      totalPrimary={fmtUsd(totalUsd)}
      totalSecondary={totalSecondary}
      ariaLabel="Cost breakdown"
      emptyMessage="No cost"
      control={control}
    />
  );
}
