import { useMemo, useId } from 'react';
import {
  Area,
  AreaChart,
  CartesianGrid,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import { getWindowColor, WINDOW_DURATION_SECS } from '../../lib/colors';
import { WINDOW_LABELS } from '../../lib/api';

export function QuotaUpstreamMiniChart({
  upstreamId,
  series,
  latest,
  rangeStart,
  rangeEnd,
  range,
}: {
  upstreamId: string;
  series: any[];
  latest?: any;
  rangeStart: number;
  rangeEnd: number;
  range: string;
}) {
  const chartId = useId();
  const chartData = useMemo(() => {
    const bucketsByTime = new Map<number, Record<string, any>>();
    const markers: { ts: number; kind: string; window: string }[] = [];

    for (const s of series) {
      if (s.upstream_id !== upstreamId) continue;
      const w = s.window;

      for (const b of s.buckets) {
        const ts = b.bucket_start_unix_secs;
        if (!bucketsByTime.has(ts)) {
          const date = new Date(ts * 1000);
          const label =
            range === '7d' || range === '24h'
              ? `${date.getMonth() + 1}/${date.getDate()} ${date.getHours()}h`
              : date.toTimeString().slice(0, 5);
          bucketsByTime.set(ts, { ts: label, unix: ts });
        }
        const row = bucketsByTime.get(ts)!;
        row[w] = b.utilization_last != null ? b.utilization_last * 100 : null;
      }

      for (const m of s.markers) {
        if (m.at_unix_secs && m.at_unix_secs >= rangeStart && m.at_unix_secs <= rangeEnd) {
          markers.push({ ts: m.at_unix_secs, kind: m.kind, window: w });
        }
      }
    }

    if (latest) {
      const latestWindows = latest.windows;
      if (latestWindows) {
        for (const w of latestWindows) {
          if (w.resets_at_unix_secs && WINDOW_DURATION_SECS[w.window]) {
            const startTs = w.resets_at_unix_secs - WINDOW_DURATION_SECS[w.window];
            if (startTs >= rangeStart && startTs <= rangeEnd) {
              markers.push({
                ts: startTs,
                kind: 'start',
                window: w.window,
              });
            }
          }
        }
      }
    }

    const rows = Array.from(bucketsByTime.values()).sort(
      (a, b) => a.unix - b.unix,
    );
    return { rows, markers };
  }, [series, upstreamId, range, rangeStart, rangeEnd, latest]);

  const chartWindows = ['5h', '7d', '7d_sonnet'];
  if (latest) {
    const opus = latest.windows.find((w: any) => w.window === '7d_opus');
    if (opus && opus.state !== 'missing') chartWindows.push('7d_opus');
    const overage = latest.windows.find((w: any) => w.window === 'overage');
    if (overage && (overage.extra_usage_enabled || overage.extra_usage_monthly_limit != null)) chartWindows.push('overage');
  }

  return (
    <div className="w-full h-full" style={{ minWidth: 0 }}>
      {chartData.rows.length === 0 ? (
        <div className="h-full flex items-center justify-center text-text-faint text-xs">
          No data in this range
        </div>
      ) : (
        <ResponsiveContainer width="100%" height="100%" debounce={150}>
          <AreaChart
            data={chartData.rows}
            margin={{ top: 8, right: 8, bottom: 4, left: -20 }}
          >
            <defs>
              {['5h', '7d', '7d_sonnet', '7d_opus', 'overage', 'unified'].map((w) => {
                const color = getWindowColor(w);
                return (
                  <linearGradient key={w} id={`${chartId}-grad-${w}`} x1="0" y1="0" x2="0" y2="1">
                    <stop offset="0%" stopColor={color.stroke} stopOpacity={0.55} />
                    <stop offset="100%" stopColor={color.stroke} stopOpacity={0} />
                  </linearGradient>
                );
              })}
            </defs>
            <CartesianGrid stroke="var(--color-border)" />
            <XAxis
              dataKey="unix"
              type="number"
              domain={[rangeStart, rangeEnd]}
              tick={{
                fill: 'var(--color-text-faint)',
                fontSize: 10,
                fontFamily: 'Geist Mono',
              }}
              tickFormatter={(val) => {
                const d = new Date(val * 1000);
                return range === '7d' || range === '24h'
                  ? `${d.getMonth() + 1}/${d.getDate()} ${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}`
                  : `${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}`;
              }}
              axisLine={false}
              tickLine={false}
              minTickGap={40}
            />
            <YAxis
              tick={{
                fill: 'var(--color-text-faint)',
                fontSize: 10,
                fontFamily: 'Geist Mono',
              }}
              tickFormatter={(val) => `${val}%`}
              axisLine={false}
              tickLine={false}
              width={40}
              domain={[0, 100]}
              allowDataOverflow={false}
            />
            <Tooltip
              cursor={{
                stroke: 'var(--color-accent)',
                strokeWidth: 1,
                strokeOpacity: 0.3,
              }}
              content={({ active, payload, label }) => {
                if (!active || !payload?.length) return null;
                return (
                  <div
                    style={{
                      background: 'var(--color-bg-sub)',
                      border: '1px solid var(--color-border)',
                      borderRadius: 2,
                      color: 'var(--color-text)',
                      fontSize: 11,
                      fontFamily: 'Geist Mono Variable, monospace',
                      padding: '6px 10px',
                      boxShadow: '0 4px 12px rgba(0,0,0,0.18)',
                      minWidth: 80,
                    }}
                  >
                    <div
                      style={{
                        color: 'var(--color-text-faint)',
                        marginBottom: 4,
                      }}
                    >
                      {label}
                    </div>
                    {payload.map((p, i) => {
                      const w = String(p.dataKey);
                      const wLabel = WINDOW_LABELS[w as keyof typeof WINDOW_LABELS] || w;
                      return (
                        <div
                          key={i}
                          style={{
                            color: 'var(--color-text)',
                            padding: '1px 0',
                            display: 'flex',
                            justifyContent: 'space-between',
                            gap: 8,
                          }}
                        >
                          <span
                            style={{
                              color:
                                typeof p.color === 'string'
                                  ? p.color
                                  : 'var(--color-text)',
                            }}
                          >
                            {wLabel}
                          </span>
                          <span
                            style={{
                              fontVariantNumeric: 'tabular-nums',
                            }}
                          >
                            {typeof p.value === 'number'
                              ? `${p.value.toFixed(1)}%`
                              : '—'}
                          </span>
                        </div>
                      );
                    })}
                  </div>
                );
              }}
            />
            {(() => {
              const chartRangeSecs = 2 * (rangeEnd - rangeStart);
              const visibleMarkers: typeof chartData.markers = [];
              const markersByWindow = new Map<string, { start?: typeof chartData.markers[0], reset?: typeof chartData.markers[0] }>();
              for (const m of chartData.markers) {
                if (!chartWindows.includes(m.window)) continue;
                if (!markersByWindow.has(m.window)) markersByWindow.set(m.window, {});
                if (m.kind === 'start') markersByWindow.get(m.window)!.start = m;
                else if (m.kind === 'reset') markersByWindow.get(m.window)!.reset = m;
                else visibleMarkers.push(m);
              }
              for (const m of markersByWindow.values()) {
                if (m.start && m.reset && Math.abs(m.reset.ts - m.start.ts) / chartRangeSecs < 0.25) {
                  visibleMarkers.push(m.reset);
                } else {
                  if (m.start) visibleMarkers.push(m.start);
                  if (m.reset) visibleMarkers.push(m.reset);
                }
              }
              return visibleMarkers.map((m, i) => {
                const color = getWindowColor(m.window);
                return (
                  <ReferenceLine
                    key={`marker-${i}`}
                    x={m.ts}
                    stroke={color.stroke}
                    strokeOpacity={0.6}
                    strokeDasharray={m.kind === 'start' ? "4 6" : "2 4"}
                  />
                );
              });
            })()}
            {chartWindows.map((w) => {
              const color = getWindowColor(w);
              return (
                <Area
                  key={w}
                  type="monotone"
                  dataKey={w}
                  stroke={color.stroke}
                  strokeWidth={1.4}
                  fill={`url(#${chartId}-grad-${w})`}
                  fillOpacity={1}
                  isAnimationActive={false}
                  connectNulls={false}
                />
              );
            })}
          </AreaChart>
        </ResponsiveContainer>
      )}
    </div>
  );
}
