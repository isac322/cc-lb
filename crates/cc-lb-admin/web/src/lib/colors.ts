export const WINDOW_DURATION_SECS: Record<string, number> = {
  '5h': 18000,
  '7d': 604800,
  '7d_sonnet': 604800,
  '7d_opus': 604800,
  '7d_fable': 604800,
};

/**
 * Quota window → series token (`--color-series-*` in `index.css`, tuned per
 * theme). Series never use amber, orange or red: those hues mean severity.
 */
const WINDOW_SERIES_TOKEN: Record<string, string> = {
  '5h': 'var(--color-series-5h)',
  '7d': 'var(--color-series-7d)',
  '7d_fable': 'var(--color-series-fable)',
  '7d_sonnet': 'var(--color-series-sonnet)',
  '7d_opus': 'var(--color-series-opus)',
  overage: 'var(--color-series-overage)',
  unified: 'var(--color-series-unified)',
};

/** Flat area fill opacity for chart series: 12% dark, 10% light. */
export const SERIES_FILL_OPACITY = 'var(--chart-fill-opacity)';

/**
 * Stroke and fill for a quota window series, as CSS `var()` references so
 * they follow the theme. Unknown windows fall back to the neutral `unified`
 * series.
 */
export function getWindowColor(window: string): {
  stroke: string;
  fill: string;
} {
  const token = WINDOW_SERIES_TOKEN[window] ?? WINDOW_SERIES_TOKEN.unified!;
  return { stroke: token, fill: token };
}

// FNV-1a: better hue distribution than djb2 for short opaque session ids.
function fnv1aHash(input: string): number {
  let hash = 0x811c9dc5;
  for (let i = 0; i < input.length; i++) {
    hash ^= input.charCodeAt(i);
    hash = Math.imul(hash, 0x01000193);
  }
  return hash >>> 0;
}

export interface SessionColor {
  fg: string;
  bg: string;
  border: string;
  hue: number;
}

export function getSessionColor(sessionId: string): SessionColor {
  const hue = fnv1aHash(sessionId) % 360;
  return {
    hue,
    fg: `hsl(${hue} 55% 62%)`,
    bg: `hsl(${hue} 55% 50% / 0.12)`,
    border: `hsl(${hue} 45% 50% / 0.35)`,
  };
}
