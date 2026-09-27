// Recharts-backed mini charts. Kept out of `primitives.tsx` so importing a
// Button or Card never drags Recharts (and d3) into a route's chunk.
import type { ReactNode } from 'react';
import { Area, AreaChart, ResponsiveContainer, YAxis } from 'recharts';

import { getWindowColor, SERIES_FILL_OPACITY } from '../../lib/colors';

// ─── Shared chart chrome ─────────────────────────────────────────────────────
// Spread into Recharts parts so every chart shares the instrument-cluster
// chrome: 1px crisp horizontal gridlines in the line color (index.css
// enforces the stroke too), 12px sans tick labels in `text-faint`, a 1px
// strong-line hover cursor, and 1px dashed warn / danger threshold rules.
export const CHART_GRID = {
  vertical: false,
  stroke: 'var(--color-border)',
} as const;
export const CHART_AXIS = {
  axisLine: false,
  tickLine: false,
  tick: { fill: 'var(--color-text-faint)', fontSize: 12 },
} as const;
export const CHART_CURSOR = {
  stroke: 'var(--color-border-strong)',
  strokeWidth: 1,
} as const;
export const CHART_THRESHOLD = {
  warn: { stroke: 'var(--color-warn)', strokeWidth: 1, strokeDasharray: '3 3' },
  danger: {
    stroke: 'var(--color-danger)',
    strokeWidth: 1,
    strokeDasharray: '3 3',
  },
} as const;

/**
 * Accessible wrapper shared by the mini charts. Recharts' own keyboard layer
 * is disabled, so the chart is one static image: named by `ariaLabel`, or
 * hidden from assistive tech when the caller already labels the surrounding
 * element.
 */
function ChartFrame({
  ariaLabel,
  children,
}: {
  ariaLabel: string | undefined;
  children: ReactNode;
}) {
  return (
    <div
      className="w-full"
      style={{ minWidth: 60, height: 28 }}
      {...(ariaLabel
        ? { role: 'img', 'aria-label': ariaLabel }
        : { 'aria-hidden': true })}
    >
      {children}
    </div>
  );
}

// ─── Sparkline ───────────────────────────────────────────────────────────────
/** Dash of a sparkline's second series, so it reads apart from the first. */
export const SPARKLINE_SECONDARY_DASH = '3 2';

/**
 * 1.25px line over a flat series-opacity fill, neutral by default (the
 * accent is reserved for pool quota). Renders nothing when every value is
 * zero (or there is no data): a flat line on the tile's bottom edge reads
 * as a second border, not as "no traffic".
 *
 * An optional `secondary` series draws as a 1.5px dashed line without fill
 * on its own scale: the pair shows shape, so the caller must name both
 * series and carry their figures in a legend.
 */
export function Sparkline({
  data,
  color = 'var(--color-text-muted)',
  secondary,
  ariaLabel,
}: {
  data: number[];
  color?: string;
  secondary?: { data: number[]; color: string };
  /** Summary read by screen readers; omit when an ancestor already names the chart. */
  ariaLabel?: string;
}) {
  if (!data.some((value) => value !== 0)) return null;
  const chartData = data.map((value, i) => ({
    i,
    value,
    secondary: secondary?.data[i] ?? 0,
  }));
  return (
    <ChartFrame ariaLabel={ariaLabel}>
      <ResponsiveContainer
        width="100%"
        height="100%"
        minWidth={60}
        minHeight={28}
      >
        <AreaChart
          data={chartData}
          margin={{ top: 1, right: 0, bottom: 1, left: 0 }}
          accessibilityLayer={false}
        >
          <YAxis yAxisId="primary" hide />
          <Area
            yAxisId="primary"
            type="monotone"
            dataKey="value"
            stroke={color}
            strokeWidth={1.25}
            fill={color}
            fillOpacity={SERIES_FILL_OPACITY}
            isAnimationActive={false}
            activeDot={false}
          />
          {secondary ? (
            <>
              <YAxis yAxisId="secondary" hide />
              <Area
                yAxisId="secondary"
                type="monotone"
                dataKey="secondary"
                stroke={secondary.color}
                strokeWidth={1.5}
                strokeDasharray={SPARKLINE_SECONDARY_DASH}
                fill="none"
                isAnimationActive={false}
                activeDot={false}
              />
            </>
          ) : null}
        </AreaChart>
      </ResponsiveContainer>
    </ChartFrame>
  );
}

// ─── QuotaMiniChart ──────────────────────────────────────────────────────────
export function QuotaMiniChart({
  data,
  ariaLabel,
}: {
  data: { i: number; val5h: number | null; val7d: number | null }[];
  /** Summary read by screen readers; omit when an ancestor already names the chart. */
  ariaLabel?: string;
}) {
  const c5h = getWindowColor('5h');
  const c7d = getWindowColor('7d');

  return (
    <ChartFrame ariaLabel={ariaLabel}>
      <ResponsiveContainer
        width="100%"
        height="100%"
        minWidth={60}
        minHeight={28}
      >
        <AreaChart
          data={data}
          margin={{ top: 1, right: 0, bottom: 1, left: 0 }}
          accessibilityLayer={false}
        >
          <Area
            type="stepAfter"
            dataKey="val7d"
            stroke={c7d.stroke}
            strokeWidth={1.5}
            fill="none"
            isAnimationActive={false}
            connectNulls={false}
          />
          <Area
            type="stepAfter"
            dataKey="val5h"
            stroke={c5h.stroke}
            strokeWidth={1.5}
            fill="none"
            isAnimationActive={false}
            connectNulls={false}
          />
        </AreaChart>
      </ResponsiveContainer>
    </ChartFrame>
  );
}
