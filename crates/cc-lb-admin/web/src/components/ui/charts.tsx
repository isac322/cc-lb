// Recharts-backed mini charts. Kept out of `primitives.tsx` so importing a
// Button or Card never drags Recharts (and d3) into a route's chunk.
import { type ReactNode, useId } from 'react';
import { Area, AreaChart, ResponsiveContainer, YAxis } from 'recharts';

import { SERIES_FILL_OPACITY } from '../../lib/colors';

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
 * series and carry their figures in a legend. A null secondary value is a
 * gap (the figure is undefined for that bucket), not a zero.
 */
export function Sparkline({
  data,
  color = 'var(--color-text-muted)',
  secondary,
  ariaLabel,
}: {
  data: number[];
  color?: string;
  secondary?: {
    data: readonly (number | null)[];
    color: string;
    /** Fixed scale for the second series (e.g. `[0, 100]` for a percent). */
    domain?: [number, number];
  };
  /** Summary read by screen readers; omit when an ancestor already names the chart. */
  ariaLabel?: string;
}) {
  if (!data.some((value) => value !== 0)) return null;
  const chartData = data.map((value, i) => ({
    i,
    value,
    secondary: secondary?.data[i] ?? null,
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
              <YAxis
                yAxisId="secondary"
                hide
                domain={secondary.domain ?? [0, 'auto']}
              />
              <Area
                yAxisId="secondary"
                type="monotone"
                dataKey="secondary"
                stroke={secondary.color}
                strokeWidth={1.5}
                strokeDasharray={SPARKLINE_SECONDARY_DASH}
                fill="none"
                connectNulls={false}
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

// ─── Quota series fills ──────────────────────────────────────────────────────
/**
 * `useId` made safe for SVG `url(#…)` references (React ids carry
 * punctuation), so every chart instance owns its gradient ids.
 */
export function useChartId(): string {
  return useId().replace(/[^a-zA-Z0-9_-]/g, '');
}

/**
 * Vertical gradient for a filled series: `color` at `strength` times the
 * theme's series fill opacity along the line, fading to nothing at the
 * baseline, so overlapping areas stay legible under each other's 1.5px
 * strokes. Charts that overlap many series pass a lower `strength`. Render
 * it inside the chart's `<defs>` and fill the `Area` with `url(#id)`.
 */
export function SeriesFillGradient({
  id,
  color,
  strength = 2,
}: {
  id: string;
  color: string;
  strength?: number;
}) {
  return (
    <linearGradient id={id} x1="0" y1="0" x2="0" y2="1">
      <stop
        offset="0%"
        style={{
          stopColor: color,
          stopOpacity: `calc(${SERIES_FILL_OPACITY} * ${strength})`,
        }}
      />
      <stop offset="100%" style={{ stopColor: color, stopOpacity: 0 }} />
    </linearGradient>
  );
}
