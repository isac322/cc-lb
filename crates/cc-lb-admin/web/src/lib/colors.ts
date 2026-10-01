export const WINDOW_DURATION_SECS: Record<string, number> = {
  '5h': 18000,
  '7d': 604800,
  '7d_sonnet': 604800,
  '7d_opus': 604800,
  '7d_fable': 604800,
};

/**
 * Quota window → series token (`--color-series-*` in `index.css`, tuned per
 * theme). Window series never use amber, orange or red: on quota charts those
 * hues mean severity (the warn/danger thresholds).
 */
const WINDOW_SERIES_TOKEN: Record<string, string> = {
  '5h': 'var(--color-series-5h)',
  '7d': 'var(--color-series-7d)',
  '7d_fable': 'var(--color-series-fable)',
  '7d_sonnet': 'var(--color-series-sonnet)',
  '7d_opus': 'var(--color-series-opus)',
  overage: 'var(--color-series-overage)',
};

/** Flat area fill opacity for chart series: 12% dark, 10% light. */
export const SERIES_FILL_OPACITY = 'var(--chart-fill-opacity)';

/**
 * Stroke and fill for a quota window series, as CSS `var()` references so
 * they follow the theme. Unknown windows fall back to the neutral gray.
 */
export function getWindowColor(window: string): {
  stroke: string;
  fill: string;
} {
  const token = WINDOW_SERIES_TOKEN[window] ?? 'var(--color-neutral)';
  return { stroke: token, fill: token };
}

// FNV-1a: better hue distribution than djb2 for short opaque ids.
export function fnv1aHash(input: string): number {
  let hash = 0x811c9dc5;
  for (let i = 0; i < input.length; i++) {
    hash ^= input.charCodeAt(i);
    hash = Math.imul(hash, 0x01000193);
  }
  return hash >>> 0;
}

/**
 * A categorical chip color (session chips, request-kind badges) for an OKLCH
 * hue, tuned per theme through `light-dark()`:
 * - `text` at night L 0.83 / by day L 0.42 holds ≥ 4.5:1 on the chip's own
 *   tint over every table surface, including the selected-row fill;
 * - `mark` is the series-strength dot;
 * - `bg` is the chip's tint.
 */
export interface CategoricalColor {
  text: string;
  mark: string;
  bg: string;
}

export function categoricalColor(hue: number): CategoricalColor {
  return {
    text: `light-dark(oklch(0.42 0.12 ${hue}), oklch(0.83 0.11 ${hue}))`,
    mark: `light-dark(oklch(0.55 0.15 ${hue}), oklch(0.75 0.14 ${hue}))`,
    bg: `light-dark(oklch(0.55 0.15 ${hue} / 0.14), oklch(0.75 0.14 ${hue} / 0.18))`,
  };
}

/**
 * Session chip hues: nine hues ≥ 21° apart. The accent is near-achromatic
 * (a faint blue whisper), so chips can use most of the wheel. The palette
 * skips the warm band (~5–100: danger, warn
 * and the cache-write series; below ~105 a chip tint reads khaki next to
 * warn) so a chip never reads as severity, and any two palette hues are
 * visibly different. 355 is the pink end nearest danger, still ≥ 20° off.
 */
const SESSION_HUES: readonly number[] = [
  110, 135, 200, 230, 255, 280, 305, 330, 355,
];

/** Coprime with the palette size (9), so the probe visits every slot. */
const SESSION_PROBE_STRIDE = 4;

const sessionSlots = new Map<string, number>();
const sessionSlotLoad: number[] = SESSION_HUES.map(() => 0);

/**
 * A session keeps the palette slot it was first given. A new session takes
 * its hashed slot unless another session already holds it, then the
 * least-used slot along the probe order — so the first nine sessions seen
 * never share a hue, and past that each hue carries an even share.
 */
function sessionSlot(sessionId: string): number {
  const known = sessionSlots.get(sessionId);
  if (known !== undefined) return known;
  const preferred = fnv1aHash(sessionId) % SESSION_HUES.length;
  let slot = preferred;
  for (let step = 1; step < SESSION_HUES.length; step++) {
    const candidate =
      (preferred + step * SESSION_PROBE_STRIDE) % SESSION_HUES.length;
    if (sessionSlotLoad[candidate]! < sessionSlotLoad[slot]!) slot = candidate;
  }
  sessionSlots.set(sessionId, slot);
  sessionSlotLoad[slot]! += 1;
  return slot;
}

/**
 * The chip color of a session: rows of one session share it everywhere in
 * the app, and different sessions get different hues.
 */
export function getSessionColor(sessionId: string): CategoricalColor {
  return categoricalColor(SESSION_HUES[sessionSlot(sessionId)]!);
}
