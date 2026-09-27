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

/** Bars grow up from this line; below it is the time axis (the pan handle). */
const BASELINE_Y = 64;
const AXIS_TEXT_Y = 80;
const STRIP_H = 88;
/** A bucket with any error keeps at least this much red so one failure shows. */
const MIN_ERROR_PX = 2;
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
  total: string;
  totalMuted: string;
  danger: string;
  accent: string;
  accentWash: string;
  text: string;
  /** Canvas text cannot inherit the CSS font stack. */
  font: string;
}

/**
 * Canvas cannot resolve CSS custom properties, so the palette is sampled from
 * the document element and re-sampled whenever the theme attribute flips.
 * index.css defines every token in both themes.
 */
function readPalette(): Palette {
  const styles = getComputedStyle(document.documentElement);
  const token = (name: string) => styles.getPropertyValue(name).trim();
  return {
    grid: token('--color-border-row'),
    total: token('--color-text-faint'),
    totalMuted: token('--color-border-strong'),
    danger: token('--color-danger'),
    accent: token('--color-accent'),
    accentWash: token('--color-accent-dim'),
    text: token('--color-text-faint'),
    font: getComputedStyle(document.body).fontFamily || 'sans-serif',
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
  const bucketsRef = useRef(buckets);
  const bucketMsRef = useRef(bucketMs);
  const viewRef = useRef(view);
  const selectionRef = useRef(selection);
  const draftRef = useRef<{ a: number; b: number } | null>(null);
  const onViewChangeRef = useRef(onViewChange);
  const onSelectionCommitRef = useRef(onSelectionCommit);
  const [palette, setPalette] = useState<Palette | null>(null);
  const [width, setWidth] = useState(0);
  const [dpr, setDpr] = useState(1);
  /** Non-null only while dragging; the committed selection stays authoritative. */
  const [draft, setDraft] = useState<{ a: number; b: number } | null>(null);
  const [hint, setHint] = useState<{ x: number; text: string } | null>(null);

  // Latest values for listeners that are attached once.
  bucketsRef.current = buckets;
  bucketMsRef.current = bucketMs;
  viewRef.current = view;
  selectionRef.current = selection;
  draftRef.current = draft;
  onViewChangeRef.current = onViewChange;
  onSelectionCommitRef.current = onSelectionCommit;
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
      const nextWidth = shell.clientWidth;
      const nextDpr = window.devicePixelRatio || 1;
      widthRef.current = nextWidth;
      setWidth((current) => (current === nextWidth ? current : nextWidth));
      setDpr((current) => (current === nextDpr ? current : nextDpr));
    };
    apply();
    const observer = new ResizeObserver(apply);
    observer.observe(shell);
    window.addEventListener('resize', apply);
    return () => {
      observer.disconnect();
      window.removeEventListener('resize', apply);
    };
  }, []);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (canvas == null || width <= 0) return;
    const pixelWidth = Math.floor(width * dpr);
    const pixelHeight = Math.floor(STRIP_H * dpr);
    if (canvas.width !== pixelWidth) canvas.width = pixelWidth;
    if (canvas.height !== pixelHeight) canvas.height = pixelHeight;
    canvas.style.height = `${STRIP_H}px`;
  }, [dpr, width]);

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

    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, width, STRIP_H);

    const span = Math.max(1, view.b - view.a);
    const t2x = (t: number) => timeToX(t, width, view);

    ctx.strokeStyle = palette.grid;
    ctx.lineWidth = 1;
    ctx.beginPath();
    ctx.moveTo(0, BASELINE_Y + 0.5);
    ctx.lineTo(width, BASELINE_Y + 0.5);
    ctx.stroke();

    // Ticks: widest ladder step that keeps labels at least 96px apart. Only
    // the labels are drawn; the chart keeps horizontal rules only.
    const tickMs =
      TICK_LADDER_MS.find((ms) => (ms / span) * width >= 96) ??
      TICK_LADDER_MS[TICK_LADDER_MS.length - 1];
    const showDateOnly = tickMs >= 6 * 3_600_000;
    ctx.font = `12px ${palette.font}`;
    ctx.fillStyle = palette.text;
    for (let t = Math.ceil(view.a / tickMs) * tickMs; t < view.b; t += tickMs) {
      const x = Math.round(t2x(t));
      const label = formatInTimezone(t, tz);
      const text = showDateOnly ? label.slice(5, 10) : label.slice(11);
      // A label that would run past the right edge is dropped, not clipped.
      if (x + 2 + ctx.measureText(text).width > width) continue;
      ctx.fillText(text, x + 2, AXIS_TEXT_Y);
    }

    if (buckets.length > 0) {
      let maxTotal = 1;
      for (const bucket of buckets) {
        if (bucket.total_count > maxTotal) maxTotal = bucket.total_count;
      }
      const slot = width / buckets.length;
      const barW = Math.max(2, slot - 0.6);
      const inSelection = (startMs: number) =>
        active == null ||
        (startMs + bucketMs / 2 >= active.a &&
          startMs + bucketMs / 2 < active.b);

      // Neutral bars on a sqrt scale so short spikes survive next to a peak;
      // the bucket's errors are stacked at the bottom of its bar in danger.
      for (let i = 0; i < buckets.length; i++) {
        const bucket = buckets[i];
        if (bucket.total_count === 0) continue;
        const startMs = bucket.bucket_start_unix_secs * 1000;
        const selected = inSelection(startMs);
        const x = t2x(startMs);
        const h = Math.max(
          MIN_ERROR_PX,
          Math.sqrt(bucket.total_count / maxTotal) * (BASELINE_Y - 4),
        );
        ctx.fillStyle = selected ? palette.total : palette.totalMuted;
        ctx.globalAlpha = selected ? 0.6 : 1;
        ctx.fillRect(x, BASELINE_Y - h, barW, h);
        if (bucket.error_count > 0) {
          const errorH = Math.min(
            h,
            Math.max(
              MIN_ERROR_PX,
              (bucket.error_count / bucket.total_count) * h,
            ),
          );
          ctx.fillStyle = palette.danger;
          ctx.globalAlpha = selected ? 0.9 : 0.4;
          ctx.fillRect(x, BASELINE_Y - errorH, barW, errorH);
        }
      }
      ctx.globalAlpha = 1;
    }

    if (active != null) {
      const xa = t2x(active.a);
      const xb = t2x(active.b);
      ctx.fillStyle = palette.accentWash;
      ctx.fillRect(xa, 0, xb - xa, BASELINE_Y);
      for (const x of [xa, xb]) {
        const px = Math.round(x) + 0.5;
        ctx.strokeStyle = palette.accent;
        ctx.beginPath();
        ctx.moveTo(px, 0);
        ctx.lineTo(px, BASELINE_Y);
        ctx.stroke();
        ctx.fillStyle = palette.accent;
        ctx.fillRect(Math.round(x) - 2, BASELINE_Y / 2 - 10, 4, 20);
      }
    }
  }, [buckets, bucketMs, view, active, palette, width, dpr, tz, timeToX]);

  const hitKind = useCallback(
    (x: number, y: number): DragKind => {
      if (y > BASELINE_Y) return 'pan';
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
  const approximateCount = useCallback((a: number, b: number) => {
    let total = 0;
    let errors = 0;
    const currentBucketMs = bucketMsRef.current;
    for (const bucket of bucketsRef.current) {
      const mid = bucket.bucket_start_unix_secs * 1000 + currentBucketMs / 2;
      if (mid >= a && mid < b) {
        total += bucket.total_count;
        errors += bucket.error_count;
      }
    }
    return { total, errors };
  }, []);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (canvas == null) return;
    const canvasElement = canvas;

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
      lastPointerXRef.current = x;
      attachDragListeners();
      if (kind === 'new') setDraft({ a: t, b: t });
      canvas.style.cursor = kind === 'new' ? 'crosshair' : 'grabbing';
      event.preventDefault();
    };

    const onDragMouseMove = (event: MouseEvent) => {
      const drag = dragRef.current;
      if (drag == null) return;
      const rect = canvas.getBoundingClientRect();
      const x = event.clientX - rect.left;
      const t = xToTime(x);
      if (drag.kind === 'pan') {
        const delta = drag.origin - t;
        const nextA = drag.viewA + delta;
        const nextB = drag.viewB + delta;
        if (nextA >= 0) onViewChangeRef.current({ a: nextA, b: nextB });
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
          stats.errors > 0 ? ` · ~${stats.errors} errors` : ''
        }`,
      });
    };

    const onCanvasMouseMove = (event: MouseEvent) => {
      if (dragRef.current != null) return;
      const rect = canvas.getBoundingClientRect();
      const x = event.clientX - rect.left;
      const y = event.clientY - rect.top;
      if (x >= 0 && x <= widthRef.current && y >= 0 && y <= STRIP_H) {
        const kind = hitKind(x, y);
        canvas.style.cursor =
          kind === 'edge-a' || kind === 'edge-b'
            ? 'ew-resize'
            : kind === 'move' || kind === 'pan'
              ? 'grab'
              : 'crosshair';
      }
    };

    let dragListenersAttached = false;
    const detachDragListeners = () => {
      if (!dragListenersAttached) return;
      window.removeEventListener('mousemove', onDragMouseMove);
      window.removeEventListener('mouseup', onMouseUp);
      dragListenersAttached = false;
    };

    const attachDragListeners = () => {
      if (dragListenersAttached) return;
      window.addEventListener('mousemove', onDragMouseMove);
      window.addEventListener('mouseup', onMouseUp);
      dragListenersAttached = true;
    };

    function onMouseUp() {
      detachDragListeners();
      const drag = dragRef.current;
      if (drag == null) return;
      dragRef.current = null;
      canvasElement.style.cursor = 'crosshair';
      setHint(null);
      if (drag.kind === 'pan') return;
      const next = draftRef.current;
      setDraft(null);
      if (next == null) return;
      if (drag.kind === 'new' && next.b - next.a < MIN_DRAG_MS) {
        onSelectionCommitRef.current(null);
        return;
      }
      // Releasing against the right edge of the strip means "up to the end of
      // what I can see". Snapping to the domain end lets the page recognise a
      // right edge that is still pinned to now, which a pixel short of the
      // edge would not express at coarse bucket widths.
      const releasedAtRightEdge =
        lastPointerXRef.current >= widthRef.current - EDGE_PX;
      const end = releasedAtRightEdge ? viewRef.current.b : next.b;
      onSelectionCommitRef.current({
        a: Math.round(next.a),
        b: Math.round(end),
      });
    }

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
      onViewChangeRef.current({ a: Math.max(0, a), b });
    };

    const onDoubleClick = (event: MouseEvent) => {
      const rect = canvas.getBoundingClientRect();
      const x = event.clientX - rect.left;
      const y = event.clientY - rect.top;
      const sel = selectionRef.current;
      if (
        sel != null &&
        y <= BASELINE_Y &&
        x > timeToX(sel.a, widthRef.current, viewRef.current) &&
        x < timeToX(sel.b, widthRef.current, viewRef.current)
      ) {
        const pad = (sel.b - sel.a) * 0.3;
        onViewChangeRef.current({
          a: Math.max(0, sel.a - pad),
          b: sel.b + pad,
        });
      }
    };

    const onMouseLeave = () => {
      if (dragRef.current == null) setHint(null);
    };

    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape' && selectionRef.current != null) {
        setDraft(null);
        onSelectionCommitRef.current(null);
      }
    };

    canvas.addEventListener('mousedown', onMouseDown);
    canvas.addEventListener('mousemove', onCanvasMouseMove);
    canvas.addEventListener('wheel', onWheel, { passive: false });
    canvas.addEventListener('dblclick', onDoubleClick);
    canvas.addEventListener('mouseleave', onMouseLeave);
    window.addEventListener('keydown', onKeyDown);
    return () => {
      dragRef.current = null;
      detachDragListeners();
      canvas.removeEventListener('mousedown', onMouseDown);
      canvas.removeEventListener('mousemove', onCanvasMouseMove);
      canvas.removeEventListener('wheel', onWheel);
      canvas.removeEventListener('dblclick', onDoubleClick);
      canvas.removeEventListener('mouseleave', onMouseLeave);
      window.removeEventListener('keydown', onKeyDown);
    };
  }, [hitKind, approximateCount, timeToX]);

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
          className="pointer-events-none absolute top-2 -translate-x-1/2 whitespace-nowrap rounded-sm border border-subtle-strong bg-bg-sub px-1.5 py-0.5 text-caption tabular-nums text-text-muted shadow-overlay"
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
          <span className="rounded-sm bg-danger/8 px-2 py-1 text-caption text-danger-text">
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
