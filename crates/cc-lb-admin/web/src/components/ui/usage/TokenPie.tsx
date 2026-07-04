import { useMemo } from 'react';
import type { RequestEvent } from '../../../lib/api';
import { fmtN, fmtUsd } from '../../../lib/format';
import { type ActiveSliceControl, PieChart, type PieSlice } from './PieChart';
import { SLICE_COLORS } from './sliceColors';

export function buildTokenSlices(event: RequestEvent): PieSlice[] {
  const cache5m = event.cache_creation_input_tokens_5m;
  const cache1h = event.cache_creation_input_tokens_1h;
  const hasSplit = cache5m != null || cache1h != null;
  const cacheCombined = event.cache_creation_input_tokens;

  const rows: {
    key: string;
    label: string;
    tokens: number;
    costMicros: number | null;
    color: string;
  }[] = [
    {
      key: 'input',
      label: 'Input',
      tokens: event.input_tokens ?? 0,
      costMicros: event.cost_input_micros ?? null,
      color: SLICE_COLORS.input,
    },
    {
      key: 'output',
      label: 'Output',
      tokens: event.output_tokens ?? 0,
      costMicros: event.cost_output_micros ?? null,
      color: SLICE_COLORS.output,
    },
    ...(hasSplit
      ? [
          {
            key: 'cache_create_5m',
            label: 'Cache create 5m',
            tokens: cache5m ?? 0,
            costMicros: event.cost_cache_creation_5m_micros ?? null,
            color: SLICE_COLORS.cache_create_5m,
          },
          {
            key: 'cache_create_1h',
            label: 'Cache create 1h',
            tokens: cache1h ?? 0,
            costMicros: event.cost_cache_creation_1h_micros ?? null,
            color: SLICE_COLORS.cache_create_1h,
          },
        ]
      : [
          {
            key: 'cache_creation',
            label: 'Cache creation',
            tokens: cacheCombined ?? 0,
            costMicros:
              (event.cost_cache_creation_5m_micros ?? 0) +
                (event.cost_cache_creation_1h_micros ?? 0) || null,
            color: SLICE_COLORS.cache_create_5m,
          },
        ]),
    {
      key: 'cache_read',
      label: 'Cache read',
      tokens: event.cache_read_input_tokens ?? 0,
      costMicros: event.cost_cache_read_micros ?? null,
      color: SLICE_COLORS.cache_read,
    },
  ];

  return rows
    .filter((r) => r.tokens > 0)
    .map<PieSlice>((r) => ({
      key: r.key,
      label: r.label,
      value: r.tokens,
      color: r.color,
      primary: fmtN(r.tokens),
      secondary: r.costMicros != null ? fmtUsd(r.costMicros) : undefined,
      aria: `${r.label} · ${fmtN(r.tokens)} tokens${r.costMicros != null ? ` · ${fmtUsd(r.costMicros)}` : ''}`,
    }));
}

export function TokenPie({
  event,
  control,
}: {
  event: RequestEvent;
  control?: ActiveSliceControl;
}) {
  const slices = useMemo(() => buildTokenSlices(event), [event]);
  const totalTokens = slices.reduce((sum, s) => sum + s.value, 0);
  const totalCost =
    event.cost_usd_micros != null && event.cost_usd_micros > 0
      ? fmtUsd(event.cost_usd_micros)
      : undefined;

  return (
    <PieChart
      slices={slices}
      totalPrimary={fmtN(totalTokens)}
      totalSecondary={totalCost}
      ariaLabel="Token usage breakdown"
      emptyMessage="No token usage"
      control={control}
    />
  );
}
