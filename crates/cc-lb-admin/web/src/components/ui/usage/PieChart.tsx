import { useState } from 'react';
import { cx } from '../primitives';

export interface PieSlice {
  key: string;
  label: string;
  value: number;
  color: string;
  primary: string;
  secondary?: string;
  aria?: string;
}

export interface ActiveSliceControl {
  activeKey: string | null;
  stickyKey: string | null;
  setHovered: (key: string | null) => void;
  setSticky: (key: string | null) => void;
}

export function useActiveSlice(): ActiveSliceControl {
  const [hoveredKey, setHovered] = useState<string | null>(null);
  const [stickyKey, setSticky] = useState<string | null>(null);
  return {
    activeKey: hoveredKey ?? stickyKey,
    stickyKey,
    setHovered,
    setSticky,
  };
}

function polarToXy(cx0: number, cy0: number, r: number, angle: number) {
  return { x: cx0 + r * Math.cos(angle), y: cy0 + r * Math.sin(angle) };
}

function donutSlicePath(
  centerX: number,
  centerY: number,
  innerR: number,
  outerR: number,
  startAngle: number,
  endAngle: number,
): string {
  const p1 = polarToXy(centerX, centerY, outerR, startAngle);
  const p2 = polarToXy(centerX, centerY, outerR, endAngle);
  const p3 = polarToXy(centerX, centerY, innerR, endAngle);
  const p4 = polarToXy(centerX, centerY, innerR, startAngle);
  const largeArc = endAngle - startAngle > Math.PI ? 1 : 0;
  return [
    `M ${p1.x} ${p1.y}`,
    `A ${outerR} ${outerR} 0 ${largeArc} 1 ${p2.x} ${p2.y}`,
    `L ${p3.x} ${p3.y}`,
    `A ${innerR} ${innerR} 0 ${largeArc} 0 ${p4.x} ${p4.y}`,
    'Z',
  ].join(' ');
}

export function PieChart({
  slices,
  totalPrimary,
  totalSecondary,
  ariaLabel,
  emptyMessage = 'No data',
  control: externalControl,
}: {
  slices: PieSlice[];
  totalPrimary: string;
  totalSecondary?: string;
  ariaLabel: string;
  emptyMessage?: string;
  control?: ActiveSliceControl;
}) {
  const internalControl = useActiveSlice();
  const control = externalControl ?? internalControl;
  const { activeKey, stickyKey, setHovered, setSticky } = control;
  const total = slices.reduce((sum, s) => sum + s.value, 0);

  if (slices.length === 0 || total === 0) {
    return (
      <div className="text-text-faint text-xs text-center py-2">
        {emptyMessage}
      </div>
    );
  }

  const size = 200;
  const cx0 = size / 2;
  const cy0 = size / 2;
  const outerR = 94;
  const outerActiveR = 100;
  const innerR = 60;

  const startFromTop = -Math.PI / 2;
  let cursor = startFromTop;
  const arcs = slices.map((slice) => {
    const angle = (slice.value / total) * Math.PI * 2;
    const startAngle = cursor;
    const endAngle = cursor + angle;
    cursor = endAngle;
    return { slice, startAngle, endAngle };
  });

  const activeSlice =
    arcs.find((a) => a.slice.key === activeKey)?.slice ?? null;
  const centerLabel = activeSlice ? activeSlice.label : 'Total';
  const centerPrimary = activeSlice ? activeSlice.primary : totalPrimary;
  const centerPct = activeSlice
    ? `${Math.round((activeSlice.value / total) * 100)}%`
    : null;
  const centerSecondaryParts = activeSlice
    ? [activeSlice.secondary, centerPct].filter(
        (x): x is string => x != null && x !== '',
      )
    : totalSecondary
      ? [totalSecondary]
      : [];
  const centerSecondary = centerSecondaryParts.join(' · ');

  return (
    <div className="px-4 py-3 w-full min-w-0">
      <svg
        viewBox={`0 0 ${size} ${size}`}
        className="w-full aspect-square block"
        role="img"
        aria-label={ariaLabel}
      >
        <title>{ariaLabel}</title>
        {arcs.map(({ slice, startAngle, endAngle }) => {
          const isSticky = slice.key === stickyKey;
          const isActive = slice.key === activeKey;
          const r = isActive ? outerActiveR : outerR;
          const dimOthers = activeKey != null && !isActive;
          return (
            <path
              key={slice.key}
              d={donutSlicePath(cx0, cy0, innerR, r, startAngle, endAngle)}
              fill={slice.color}
              opacity={dimOthers ? 0.32 : 1}
              stroke={isSticky ? 'rgba(255,255,255,0.6)' : 'transparent'}
              strokeWidth={isSticky ? 1.5 : 0}
              className={cx(
                'cursor-pointer transition-all duration-150 ease-out',
                isActive ? 'brightness-110' : '',
              )}
              onMouseEnter={() => setHovered(slice.key)}
              onMouseLeave={() => setHovered(null)}
              onFocus={() => setHovered(slice.key)}
              onBlur={() => setHovered(null)}
              onClick={() => setSticky(isSticky ? null : slice.key)}
              tabIndex={0}
            >
              <title>{slice.aria ?? slice.label}</title>
            </path>
          );
        })}
        <text
          x={cx0}
          y={cy0 - 18}
          textAnchor="middle"
          className="fill-[color:var(--color-text-faint)] uppercase tracking-wider"
          style={{ fontSize: 10 }}
        >
          {centerLabel}
        </text>
        <text
          x={cx0}
          y={cy0 + 2}
          textAnchor="middle"
          className="fill-[color:var(--color-text)] tabular-nums"
          style={{ fontSize: 16, fontWeight: 500 }}
        >
          {centerPrimary}
        </text>
        {centerSecondary ? (
          <text
            x={cx0}
            y={cy0 + 20}
            textAnchor="middle"
            className="fill-[color:var(--color-text-muted)] tabular-nums"
            style={{ fontSize: 11 }}
          >
            {centerSecondary}
          </text>
        ) : null}
      </svg>
    </div>
  );
}
