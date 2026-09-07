import { useCallback, useEffect, useRef, useState } from 'react';
import type { HistogramBucket } from '../../lib/api';
import { useTimezone } from '../../lib/locale';
import { formatInTimezone } from '../../lib/timezone';

export interface TimeRangeStripProps {
  readonly buckets: HistogramBucket[];
  readonly bucketMs: number;
  /** Rendered domain in epoch milliseconds. */
  readonly view: { a: number; b: number };
  /** Committed absolute selection in epoch milliseconds. */
  readonly selection: { a: number; b: number } | null;
  readonly loading: boolean;
  readonly failed: boolean;
  readonly onViewChange: (view: { a: number; b: number }) => void;
  readonly onSelectionCommit: (sel: { a: number; b: number } | null) => void;
}

const TOTAL_BASELINE_Y = 62;
const ERROR_LANE_Y = 66;
const ERROR_LANE_H = 14;
const AXIS_TEXT_Y = 94;
const STRIP_H = 104;
const EDGE_PX = 6;
/** Below this width a drag is treated as a click that clears the selection. */
const MIN_DRAG_MS = 2000;
const MIN_VIEW_MS = 60_000;

const TICK_LADDER_MS = [
  60_000,
  2 * 60_000,
  5 * 60_000,
  10 * 60_000,
  15 * 60_000,
  30 * 60_000,
  3_600_000,
  2 * 3_600_000,
  3 * 3_600_000,
  6 * 3_600_000,
  12 * 3_600_000,
  24 * 3_600_000,
];

type DragKind = 'new' | 'edge-a' | 'edge-b' | 'move' | 'pan';

interface DragState {
  kind: DragKind;
  origin: number;
  selA: number;
  selB: number;
  viewA: number;
  viewB: number;
}

interface Palette {
  grid: string;
  gridStrong: string;
  total: string;
  totalMuted: string;
  danger: string;
  dangerMuted: string;
  dangerWash: string;
  accent: string;
  accentWash: string;
  text: string;
}

function cssVar(styles: CSSStyleDeclaration, name: string, fallback: string) {
  const raw = styles.getPropertyValue(name).trim();
  return raw.length > 0 ? raw : fallback;
}

/**
 * Canvas cannot resolve CSS custom properties, so the palette is sampled from
 * the document element and re-sampled whenever the theme attribute flips.
 */
function readPalette(): Palette {
  const styles = getComputedStyle(document.documentElement);
  const accent = cssVar(styles, '--color-accent', '#00d4ff');
  const danger = cssVar(styles, '--color-danger', '#ef4444');
  return {
    grid: cssVar(styles, '--color-border-row', 'rgba(127,127,127,0.06)'),
    gridStrong: cssVar(styles, '--color-border', 'rgba(127,127,127,0.12)'),
    total: accent,
    totalMuted: cssVar(
      styles,
      '--color-border-strong',
      'rgba(127,127,127,0.2)',
    ),
    danger,
    dangerMuted: cssVar(
      styles,
      '--color-border-strong',
      'rgba(127,127,127,0.2)',
    ),
    dangerWash: cssVar(styles, '--color-overlay-2', 'rgba(127,127,127,0.03)'),
    accent,
    accentWash: cssVar(styles, '--color-accent-dim', 'rgba(0,212,255,0.10)'),
    text: cssVar(styles, '--color-text-faint', '#6b7280'),
  };
}

function formatDuration(ms: number): string {
  const mins = Math.round(ms / 60_000);
  if (mins < 60) return `${Math.max(1, mins)}m`;
  const hours = ms / 3_600_000;
  if (hours < 48) return `${hours.toFixed(hours < 10 ? 1 : 0)}h`;
  return `${(ms / 86_400_000).toFixed(1)}d`;
}

export function TimeRangeStrip({
  buckets,
  bucketMs,
  view,
  selection,
  loading,
  failed,
  onViewChange,
  onSelectionCommit,
}: TimeRangeStripProps) {
  const { effective: tz } = useTimezone();
  const shellRef = useRef<HTMLDivElement | null>(null);
  const canvasRef = useRef<HTMLCanvasElement | null>(null);
  const dragRef = useRef<DragState | null>(null);
  const widthRef = useRef(0);
  const lastPointerXRef = useRef(0);
  const [palette, setPalette] = useState<Palette | null>(null);
  const [width, setWidth] = useState(0);
  /** Non-null only while dragging; the committed selection stays authoritative. */
  const [draft, setDraft] = useState<{ a: number; b: number } | null>(null);
  const [hint, setHint] = useState<{ x: number; text: string } | null>(null);

  // Latest values for listeners that are attached once.
  const viewRef = useRef(view);
  viewRef.current = view;
  const selectionRef = useRef(selection);
  selectionRef.current = selection;
  const draftRef = useRef(draft);
  draftRef.current = draft;

  useEffect(() => {
    setPalette(readPalette());
    const observer = new MutationObserver(() => setPalette(readPalette()));
    observer.observe(document.documentElement, {
      attributes: true,
      attributeFilter: ['data-theme'],
    });
    return () => observer.disconnect();
  }, []);

  useEffect(() => {
    const shell = shellRef.current;
    if (shell == null) return;
    const apply = () => {
      const next = shell.clientWidth;
      widthRef.current = next;
      setWidth(next);
    };
    apply();
    const observer = new ResizeObserver(apply);
    observer.observe(shell);
    return () => observer.disconnect();
  }, []);

  const active = draft ?? selection;

  const timeToX = useCallback(
    (t: number, w: number, domain: { a: number; b: number }) =>
      ((t - domain.a) / Math.max(1, domain.b - domain.a)) * w,
    [],
  );

  // Draw
  useEffect(() => {
    const canvas = canvasRef.current;
    if (canvas == null || palette == null || width <= 0) return;
    const ctx = canvas.getContext('2d');
    if (ctx == null) return;

    const dpr = window.devicePixelRatio || 1;
    canvas.width = Math.floor(width * dpr);
    canvas.height = Math.floor(STRIP_H * dpr);
    canvas.style.height = `${STRIP_H}px`;
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, width, STRIP_H);

    const span = Math.max(1, view.b - view.a);
    const t2x = (t: number) => timeToX(t, width, view);

    ctx.strokeStyle = palette.grid;
    ctx.lineWidth = 1;
    ctx.beginPath();
    ctx.moveTo(0, TOTAL_BASELINE_Y + 0.5);
    ctx.lineTo(width, TOTAL_BASELINE_Y + 0.5);
    ctx.stroke();
    ctx.fillStyle = palette.dangerWash;
    ctx.fillRect(0, ERROR_LANE_Y, width, ERROR_LANE_H);

    // Ticks: widest ladder step that keeps labels at least 96px apart.
    const tickMs =
      TICK_LADDER_MS.find((ms) => (ms / span) * width >= 96) ??
      TICK_LADDER_MS[TICK_LADDER_MS.length - 1];
    const showDateOnly = tickMs >= 6 * 3_600_000;
    ctx.font = '10px ui-monospace, monospace';
    for (let t = Math.ceil(view.a / tickMs) * tickMs; t < view.b; t += tickMs) {
      const x = Math.round(t2x(t)) + 0.5;
      ctx.strokeStyle = palette.grid;
      ctx.beginPath();
      ctx.moveTo(x, 0);
      ctx.lineTo(x, TOTAL_BASELINE_Y);
      ctx.stroke();
      ctx.fillStyle = palette.text;
      const label = formatInTimezone(t, tz);
      ctx.fillText(
        showDateOnly ? label.slice(5, 10) : label.slice(11),
        x + 3,
        AXIS_TEXT_Y,
      );
    }

    if (buckets.length > 0) {
      let maxTotal = 1;
      let maxError = 1;
      for (const bucket of buckets) {
        if (bucket.total_count > maxTotal) maxTotal = bucket.total_count;
        if (bucket.error_count > maxError) maxError = bucket.error_count;
      }
      const slot = width / buckets.length;
      const barW = Math.max(2, slot - 0.6);
      const inSelection = (startMs: number) =>
        active == null ||
        (startMs + bucketMs / 2 >= active.a &&
          startMs + bucketMs / 2 < active.b);

      // Total lane uses a sqrt scale so short spikes survive next to a peak.
      for (let i = 0; i < buckets.length; i++) {
        const bucket = buckets[i];
        if (bucket.total_count === 0) continue;
        const startMs = bucket.bucket_start_unix_secs * 1000;
        const h = Math.max(
          1,
          Math.sqrt(bucket.total_count / maxTotal) * (TOTAL_BASELINE_Y - 6),
        );
        ctx.fillStyle = inSelection(startMs)
          ? palette.total
          : palette.totalMuted;
        ctx.globalAlpha = inSelection(startMs) ? 0.5 : 1;
        ctx.fillRect(t2x(startMs), TOTAL_BASELINE_Y - h, barW, h);
      }
      ctx.globalAlpha = 1;

      // Error lane is separate and guarantees 3px so a single failure is visible.
      for (let i = 0; i < buckets.length; i++) {
        const bucket = buckets[i];
        if (bucket.error_count === 0) continue;
        const startMs = bucket.bucket_start_unix_secs * 1000;
        const h = Math.max(
          3,
          Math.sqrt(bucket.error_count / maxError) * ERROR_LANE_H,
        );
        ctx.fillStyle = inSelection(startMs)
          ? palette.danger
          : palette.dangerMuted;
        ctx.globalAlpha = inSelection(startMs) ? 0.95 : 0.35;
        ctx.fillRect(t2x(startMs), ERROR_LANE_Y + ERROR_LANE_H - h, barW, h);
      }
      ctx.globalAlpha = 1;
    }

    ctx.fillStyle = palette.danger;
    ctx.globalAlpha = 0.55;
    ctx.font = '9px ui-monospace, monospace';
    ctx.fillText('err', 2, ERROR_LANE_Y + ERROR_LANE_H - 3);
    ctx.globalAlpha = 1;

    if (active != null) {
      const xa = t2x(active.a);
      const xb = t2x(active.b);
      ctx.fillStyle = palette.accentWash;
      ctx.fillRect(xa, 0, xb - xa, ERROR_LANE_Y + ERROR_LANE_H);
      for (const x of [xa, xb]) {
        const px = Math.round(x) + 0.5;
        ctx.strokeStyle = palette.accent;
        ctx.beginPath();
        ctx.moveTo(px, 0);
        ctx.lineTo(px, ERROR_LANE_Y + ERROR_LANE_H);
        ctx.stroke();
        ctx.fillStyle = palette.accent;
        ctx.fillRect(Math.round(x) - 2.5, TOTAL_BASELINE_Y / 2 - 11, 5, 22);
      }
    }
  }, [buckets, bucketMs, view, active, palette, width, tz, timeToX]);

  const hitKind = useCallback(
    (x: number, y: number): DragKind => {
      if (y > ERROR_LANE_Y + ERROR_LANE_H) return 'pan';
      const sel = draftRef.current ?? selectionRef.current;
      if (sel != null) {
        const w = widthRef.current;
        const xa = timeToX(sel.a, w, viewRef.current);
        const xb = timeToX(sel.b, w, viewRef.current);
        if (Math.abs(x - xa) <= EDGE_PX) return 'edge-a';
        if (Math.abs(x - xb) <= EDGE_PX) return 'edge-b';
        if (x > xa && x < xb) return 'move';
      }
      return 'new';
    },
    [timeToX],
  );

  /**
   * Approximate count for the in-flight drag. Whole buckets only, so it is
   * labelled with a tilde: the committed range is what the table counts.
   */
  const approximateCount = useCallback(
    (a: number, b: number) => {
      let total = 0;
      let errors = 0;
      for (const bucket of buckets) {
        const mid = bucket.bucket_start_unix_secs * 1000 + bucketMs / 2;
        if (mid >= a && mid < b) {
          total += bucket.total_count;
          errors += bucket.error_count;
        }
      }
      return { total, errors };
    },
    [buckets, bucketMs],
  );

  useEffect(() => {
    const canvas = canvasRef.current;
    if (canvas == null) return;

    const xToTime = (x: number) => {
      const domain = viewRef.current;
      return (
        domain.a + (x / Math.max(1, widthRef.current)) * (domain.b - domain.a)
      );
    };

    const onMouseDown = (event: MouseEvent) => {
      const rect = canvas.getBoundingClientRect();
      const x = event.clientX - rect.left;
      const y = event.clientY - rect.top;
      const kind = hitKind(x, y);
      const t = xToTime(x);
      const sel = draftRef.current ?? selectionRef.current;
      dragRef.current = {
        kind,
        origin: t,
        selA: sel?.a ?? t,
        selB: sel?.b ?? t,
        viewA: viewRef.current.a,
        viewB: viewRef.current.b,
      };
      if (kind === 'new') setDraft({ a: t, b: t });
      canvas.style.cursor = kind === 'new' ? 'crosshair' : 'grabbing';
      event.preventDefault();
    };

    const onMouseMove = (event: MouseEvent) => {
      const rect = canvas.getBoundingClientRect();
      const x = event.clientX - rect.left;
      const y = event.clientY - rect.top;
      const drag = dragRef.current;
      if (drag == null) {
        if (x >= 0 && x <= widthRef.current && y >= 0 && y <= STRIP_H) {
          const kind = hitKind(x, y);
          canvas.style.cursor =
            kind === 'edge-a' || kind === 'edge-b'
              ? 'ew-resize'
              : kind === 'move' || kind === 'pan'
                ? 'grab'
                : 'crosshair';
        }
        return;
      }
      const t = xToTime(x);
      if (drag.kind === 'pan') {
        const delta = drag.origin - t;
        const nextA = drag.viewA + delta;
        const nextB = drag.viewB + delta;
        if (nextA >= 0) onViewChange({ a: nextA, b: nextB });
        return;
      }
      let next: { a: number; b: number };
      if (drag.kind === 'new') {
        next = { a: Math.min(drag.origin, t), b: Math.max(drag.origin, t) };
      } else if (drag.kind === 'edge-a') {
        next = { a: Math.min(t, drag.selB - 1000), b: drag.selB };
      } else if (drag.kind === 'edge-b') {
        next = { a: drag.selA, b: Math.max(t, drag.selA + 1000) };
      } else {
        const delta = t - drag.origin;
        next = { a: drag.selA + delta, b: drag.selB + delta };
      }
      lastPointerXRef.current = x;
      setDraft(next);
      const stats = approximateCount(next.a, next.b);
      setHint({
        x: Math.max(52, Math.min(Math.max(52, widthRef.current - 52), x)),
        text: `${formatDuration(next.b - next.a)} · ~${stats.total} requests${
          stats.errors > 0 ? ` / ~${stats.errors} err` : ''
        }`,
      });
    };

    const onMouseUp = () => {
      const drag = dragRef.current;
      if (drag == null) return;
      dragRef.current = null;
      canvas.style.cursor = 'crosshair';
      setHint(null);
      if (drag.kind === 'pan') return;
      const next = draftRef.current;
      setDraft(null);
      if (next == null) return;
      if (drag.kind === 'new' && next.b - next.a < MIN_DRAG_MS) {
        onSelectionCommit(null);
        return;
      }
      // Releasing against the right edge of the strip means "up to the end of
      // what I can see". Snapping to the domain end lets the page recognise a
      // right edge that is still pinned to now, which a pixel short of the
      // edge would not express at coarse bucket widths.
      const releasedAtRightEdge =
        lastPointerXRef.current >= widthRef.current - EDGE_PX;
      const end = releasedAtRightEdge ? viewRef.current.b : next.b;
      onSelectionCommit({ a: Math.round(next.a), b: Math.round(end) });
    };

    const onWheel = (event: WheelEvent) => {
      event.preventDefault();
      const rect = canvas.getBoundingClientRect();
      const t = xToTime(event.clientX - rect.left);
      const factor = event.deltaY < 0 ? 1 / 1.25 : 1.25;
      const domain = viewRef.current;
      let a = t - (t - domain.a) * factor;
      let b = t + (domain.b - t) * factor;
      if (b - a < MIN_VIEW_MS) {
        const center = (a + b) / 2;
        a = center - MIN_VIEW_MS / 2;
        b = center + MIN_VIEW_MS / 2;
      }
      onViewChange({ a: Math.max(0, a), b });
    };

    const onDoubleClick = (event: MouseEvent) => {
      const rect = canvas.getBoundingClientRect();
      const x = event.clientX - rect.left;
      const y = event.clientY - rect.top;
      const sel = selectionRef.current;
      // The error lane counts as inside the selection; zooming from a red
      // spike is the whole point of having the lane.
      if (
        sel != null &&
        y <= ERROR_LANE_Y + ERROR_LANE_H &&
        x > timeToX(sel.a, widthRef.current, viewRef.current) &&
        x < timeToX(sel.b, widthRef.current, viewRef.current)
      ) {
        const pad = (sel.b - sel.a) * 0.3;
        onViewChange({ a: Math.max(0, sel.a - pad), b: sel.b + pad });
      }
    };

    const onMouseLeave = () => {
      if (dragRef.current == null) setHint(null);
    };

    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape' && selectionRef.current != null) {
        setDraft(null);
        onSelectionCommit(null);
      }
    };

    canvas.addEventListener('mousedown', onMouseDown);
    window.addEventListener('mousemove', onMouseMove);
    window.addEventListener('mouseup', onMouseUp);
    canvas.addEventListener('wheel', onWheel, { passive: false });
    canvas.addEventListener('dblclick', onDoubleClick);
    canvas.addEventListener('mouseleave', onMouseLeave);
    window.addEventListener('keydown', onKeyDown);
    return () => {
      canvas.removeEventListener('mousedown', onMouseDown);
      window.removeEventListener('mousemove', onMouseMove);
      window.removeEventListener('mouseup', onMouseUp);
      canvas.removeEventListener('wheel', onWheel);
      canvas.removeEventListener('dblclick', onDoubleClick);
      canvas.removeEventListener('mouseleave', onMouseLeave);
      window.removeEventListener('keydown', onKeyDown);
    };
  }, [hitKind, approximateCount, onViewChange, onSelectionCommit, timeToX]);

  const ariaLabel =
    active != null
      ? `Request density from ${formatInTimezone(view.a, tz)} to ${formatInTimezone(view.b, tz)}, selection ${formatInTimezone(active.a, tz)} to ${formatInTimezone(active.b, tz)}`
      : `Request density from ${formatInTimezone(view.a, tz)} to ${formatInTimezone(view.b, tz)}, no selection`;

  return (
    <div
      ref={shellRef}
      className="relative select-none"
      style={{ height: STRIP_H }}
    >
      <canvas
        ref={canvasRef}
        role="img"
        aria-label={ariaLabel}
        className="block w-full cursor-crosshair"
      />
      {hint != null ? (
        <div
          className="pointer-events-none absolute top-2 -translate-x-1/2 whitespace-nowrap rounded-sm border border-subtle bg-panel-strong px-1.5 py-0.5 font-mono text-[10px] text-muted"
          style={{ left: hint.x }}
        >
          {hint.text}
        </div>
      ) : null}
      {failed ? (
        <div
          className="pointer-events-none absolute right-2 top-2 z-10"
          data-testid="time-range-strip-error"
        >
          <span className="rounded-sm border border-subtle bg-panel-strong px-2 py-1 text-[11px] text-[color:var(--color-danger)]">
            Failed to load request density
          </span>
        </div>
      ) : null}
      {loading && !failed ? (
        <div
          className="pointer-events-none absolute inset-0 bg-[color:var(--color-overlay-1)]"
          data-testid="time-range-strip-loading"
        />
      ) : null}
    </div>
  );
}
