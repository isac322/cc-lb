// Recharts-backed mini charts. Kept out of `primitives.tsx` so importing a
// Button or Card never drags Recharts (and d3) into a route's chunk.
import type { ReactNode } from 'react';
import { Area, AreaChart, ResponsiveContainer } from 'recharts';

import { getWindowColor, SERIES_FILL_OPACITY } from '../../lib/colors';

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
/**
 * 1.5px line over a flat series-opacity fill. Renders nothing when every
 * value is zero (or there is no data): a flat line on the tile's bottom edge
 * reads as a second border, not as "no traffic".
 */
export function Sparkline({
  data,
  color = 'var(--color-accent)',
  ariaLabel,
}: {
  data: number[];
  color?: string;
  /** Summary read by screen readers; omit when an ancestor already names the chart. */
  ariaLabel?: string;
}) {
  if (!data.some((value) => value !== 0)) return null;
  const chartData = data.map((value, i) => ({ i, value }));
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
          <Area
            type="monotone"
            dataKey="value"
            stroke={color}
            strokeWidth={1.5}
            fill={color}
            fillOpacity={SERIES_FILL_OPACITY}
            isAnimationActive={false}
          />
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
