// Headroom instruments: the arc gauge (cluster band, per-window cards) and
// its linear sibling (lists, phones). Both draw what is LEFT of a quota
// window in the accent, switching to warn / danger by used (≥80 / ≥95).
// Static SVG: nothing animates, so reduced motion needs no special case.
import type { ReactNode } from 'react';
import {
  formatHeadroom,
  formatHeadroomValue,
  formatQuotaPercent,
  headroomPct,
  QUOTA_DANGER_PCT,
  QUOTA_SEVERITY_TEXT_CLASS,
  QUOTA_WARN_PCT,
  quotaSeverity,
} from '../../lib/quotaSeverity';
import { cx } from './primitives';

/** The arc opens at the bottom: 240° clockwise from 150° (SVG angles). */
const ARC_START = 150;
const ARC_SWEEP = 240;
/** Headroom where the warn / danger zones begin (100 − used threshold). */
const WARN_LEFT = 100 - QUOTA_WARN_PCT;
const DANGER_LEFT = 100 - QUOTA_DANGER_PCT;

const GEOMETRY = {
  hero: { box: 264, stroke: 6, tick: 4, major: 8, gap: 6 },
  sub: { box: 128, stroke: 4, tick: 3, major: 5, gap: 4 },
} as const;

type Geometry = (typeof GEOMETRY)[keyof typeof GEOMETRY];

function polar(c: number, r: number, deg: number): [number, number] {
  const rad = (deg * Math.PI) / 180;
  return [c + r * Math.cos(rad), c + r * Math.sin(rad)];
}

const f = (n: number) => n.toFixed(2);

/** Clockwise arc path between two headroom fractions (0-100) of the dial. */
function arcPath(c: number, r: number, fromPct: number, toPct: number) {
  const a0 = ARC_START + (ARC_SWEEP * fromPct) / 100;
  const a1 = ARC_START + (ARC_SWEEP * toPct) / 100;
  const [x0, y0] = polar(c, r, a0);
  const [x1, y1] = polar(c, r, a1);
  const large = a1 - a0 > 180 ? 1 : 0;
  return `M${f(x0)} ${f(y0)}A${f(r)} ${f(r)} 0 ${large} 1 ${f(x1)} ${f(y1)}`;
}

function radial(c: number, r0: number, r1: number, pct: number) {
  const a = ARC_START + (ARC_SWEEP * pct) / 100;
  const [x0, y0] = polar(c, r0, a);
  const [x1, y1] = polar(c, r1, a);
  return { x1: f(x0), y1: f(y0), x2: f(x1), y2: f(y1) };
}

function Dial({ g, left }: { g: Geometry; left: number | null }) {
  const c = g.box / 2;
  // Ticks and zones sit outside the sweep; keep them inside the box.
  const outer = g.stroke / 2 + g.gap + g.major;
  const r = c - outer - 1;
  const ringOut = r + g.stroke / 2 + g.gap;
  const severity = left === null ? 'none' : quotaSeverity(100 - left);
  const ticks = Array.from({ length: 11 }, (_, i) => i * 10);
  return (
    <svg
      aria-hidden="true"
      className="gauge"
      height={g.box}
      viewBox={`0 0 ${g.box} ${g.box}`}
      width={g.box}
    >
      <path
        className="gauge-track"
        d={arcPath(c, r, 0, 100)}
        style={{ strokeWidth: g.stroke }}
      />
      {/* Low-headroom zones hug the start of the dial, just outside it. */}
      <path
        className="gauge-zone"
        d={arcPath(c, ringOut, 0, DANGER_LEFT)}
        data-zone="danger"
      />
      <path
        className="gauge-zone"
        d={arcPath(c, ringOut, DANGER_LEFT, WARN_LEFT)}
        data-zone="warn"
      />
      {ticks.map((pct) => {
        const major = pct % 50 === 0;
        return (
          <line
            className="gauge-tick"
            data-major={major || undefined}
            key={pct}
            {...radial(
              c,
              ringOut + 2,
              ringOut + 2 + (major ? g.major : g.tick),
              pct,
            )}
          />
        );
      })}
      {left !== null && left > 0 ? (
        <path
          className="gauge-sweep"
          d={arcPath(c, r, 0, left)}
          data-severity={severity}
          style={{ strokeWidth: g.stroke }}
        />
      ) : null}
      {left !== null ? (
        <line
          className="gauge-marker"
          data-severity={severity}
          {...radial(c, r - g.stroke / 2 - 3, r + g.stroke / 2 + 3, left)}
        />
      ) : null}
    </svg>
  );
}

function Numeral({
  usedPct,
  size,
}: {
  usedPct: number | null | undefined;
  size: 'hero' | 'sub';
}) {
  const left = headroomPct(usedPct);
  const severity = quotaSeverity(usedPct ?? null);
  const hero = size === 'hero';
  return (
    <span
      className={cx(
        'inline-flex items-baseline whitespace-nowrap tabular-nums',
        severity === 'ok' || severity === 'none'
          ? 'text-text'
          : QUOTA_SEVERITY_TEXT_CLASS[severity],
      )}
    >
      <span className={hero ? 'text-display-hero' : 'text-display'}>
        {formatHeadroomValue(usedPct)}
      </span>
      {left === null ? null : (
        <span
          className={cx(
            'ml-0.5 font-normal',
            hero ? 'text-title-section' : 'text-caption',
            severity === 'ok' || severity === 'none'
              ? 'text-text-muted'
              : undefined,
          )}
        >
          % left
        </span>
      )}
    </span>
  );
}

export interface ArcGaugeProps {
  /** Window utilization 0-100; `null`/`undefined` renders an empty dial. */
  usedPct: number | null | undefined;
  /** `hero`: 264px pool dial, label and caption inside. `sub`: 128px window dial, label and caption below. */
  size?: 'hero' | 'sub';
  /** Short window or pool name ("5h", "Pool headroom"); leads the accessible name. */
  label: string;
  /** Secondary line: binding window, "41% used", reset time. Non-interactive. */
  caption?: ReactNode;
  /**
   * Accessible name. Defaults to `"<label>: <N% left>"`, plus the caption
   * when it is a string. Pass it when the caption is not plain text, e.g.
   * `"5h: 59% left, resets Thu 10-01 09:00"`.
   */
  ariaLabel?: string;
  className?: string;
}

/**
 * 240° headroom dial: 1px ticks every 10%, the sweep = what is left in the
 * accent (warn / danger once used ≥ 80 / 95), warn and danger zones at the
 * low end, and a big light numeral `N% left`. One `role="img"`.
 */
export function ArcGauge({
  usedPct,
  size = 'sub',
  label,
  caption,
  ariaLabel,
  className,
}: ArcGaugeProps) {
  const g = GEOMETRY[size];
  const left = headroomPct(usedPct);
  const name =
    ariaLabel ??
    `${label}: ${left === null ? 'no reading' : formatHeadroom(usedPct)}${
      typeof caption === 'string' ? `, ${caption}` : ''
    }`;

  if (size === 'hero') {
    return (
      <div
        aria-label={name}
        className={cx('relative inline-grid place-items-center', className)}
        role="img"
        style={{ width: g.box, height: g.box }}
      >
        <Dial g={g} left={left} />
        <div className="absolute inset-x-8 top-[26%] flex flex-col items-center text-center">
          <span className="text-label text-text-muted">{label}</span>
          <Numeral size="hero" usedPct={usedPct} />
          {caption ? (
            <span className="mt-1 text-body-sm text-text-muted">{caption}</span>
          ) : null}
        </div>
      </div>
    );
  }

  return (
    <div
      aria-label={name}
      className={cx('inline-flex flex-col items-start gap-2', className)}
      role="img"
    >
      <div
        className="relative grid place-items-center"
        style={{ width: g.box, height: g.box }}
      >
        <Dial g={g} left={left} />
        <span className="absolute inset-0 grid place-items-center pt-1">
          <Numeral size="sub" usedPct={usedPct} />
        </span>
      </div>
      <div className="flex min-w-0 flex-col gap-0.5">
        <span className="text-title-section text-text">{label}</span>
        {caption ? (
          <span className="text-body-sm text-text-muted">{caption}</span>
        ) : null}
      </div>
    </div>
  );
}

export interface HeadroomMeterProps {
  /** Window utilization 0-100; `null`/`undefined` renders an empty track. */
  usedPct: number | null | undefined;
  /** `sm`: 4px bar for dense lists. `md`: 6px bar with `N% left` / `M% used` above. */
  size?: 'sm' | 'md';
  /** Accessible name of the meter, e.g. the window ("7d"). */
  label?: string;
  className?: string;
}

/**
 * Linear headroom: fill = what is left, in the accent (warn / danger by
 * used), with a 1px mark where the warn zone starts. `role="meter"` valued
 * as headroom so assistive tech hears "59% left".
 */
export function HeadroomMeter({
  usedPct,
  size = 'sm',
  label = 'Headroom',
  className,
}: HeadroomMeterProps) {
  const left = headroomPct(usedPct);
  const severity = quotaSeverity(usedPct ?? null);
  const fill =
    severity === 'danger'
      ? 'bg-danger'
      : severity === 'warn'
        ? 'bg-warn'
        : 'bg-accent';
  const bar = (
    <span
      className={cx(
        'relative block w-full bg-progress-track',
        size === 'md' ? 'h-1.5' : 'h-1',
      )}
    >
      {left !== null && left > 0 ? (
        <span
          className={cx('absolute inset-y-0 left-0', fill)}
          style={{ width: `${Math.max(left, 1)}%` }}
        />
      ) : null}
      <span
        aria-hidden="true"
        className="absolute -inset-y-0.5 w-px bg-text-faint"
        style={{ left: `${WARN_LEFT}%` }}
      />
    </span>
  );
  return (
    <div
      aria-label={label}
      aria-valuemax={100}
      aria-valuemin={0}
      aria-valuenow={left ?? undefined}
      aria-valuetext={left === null ? 'No reading' : formatHeadroom(usedPct)}
      className={cx('flex min-w-0 flex-col gap-1.5', className)}
      role="meter"
    >
      {size === 'md' ? (
        <span className="flex items-baseline justify-between gap-2 tabular-nums">
          <span
            className={cx(
              'text-title-card',
              severity === 'ok' || severity === 'none'
                ? 'text-text'
                : QUOTA_SEVERITY_TEXT_CLASS[severity],
            )}
          >
            {formatHeadroom(usedPct)}
          </span>
          {left === null ? null : (
            <span className="text-caption text-text-muted">
              {formatQuotaPercent(usedPct)} used
            </span>
          )}
        </span>
      ) : null}
      {bar}
    </div>
  );
}
