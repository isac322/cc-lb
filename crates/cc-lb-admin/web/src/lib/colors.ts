export const UPSTREAM_COLORS = [
  { line5h: '#3b82f6', line7d: '#93c5fd' }, // blue
  { line5h: '#8b5cf6', line7d: '#c4b5fd' }, // violet
  { line5h: '#10b981', line7d: '#6ee7b7' }, // emerald
  { line5h: '#f59e0b', line7d: '#fcd34d' }, // amber
  { line5h: '#ec4899', line7d: '#f9a8d4' }, // pink
  { line5h: '#06b6d4', line7d: '#67e8f9' }, // cyan
  { line5h: '#f97316', line7d: '#fdba74' }, // orange
  { line5h: '#14b8a6', line7d: '#5eead4' }, // teal
  { line5h: '#6366f1', line7d: '#a5b4fc' }, // indigo
  { line5h: '#84cc16', line7d: '#bef264' }, // lime
  { line5h: '#ef4444', line7d: '#fca5a5' }, // red
  { line5h: '#64748b', line7d: '#cbd5e1' }, // slate
];

export const WINDOW_GRADIENT_IDS = {
  '5h': 'quota-grad-5h',
  '7d': 'quota-grad-7d',
  '7d_sonnet': 'quota-grad-sonnet',
  '7d_opus': 'quota-grad-opus',
  '7d_fable': 'quota-grad-fable',
  overage: 'quota-grad-overage',
  unified: 'quota-grad-unified',
} as const;

export const WINDOW_DURATION_SECS: Record<string, number> = {
  '5h': 18000,
  '7d': 604800,
  '7d_sonnet': 604800,
  '7d_opus': 604800,
  '7d_fable': 604800,
};

export const WINDOW_COLORS: Record<string, { stroke: string; fill: string }> = {
  '5h': { stroke: '#3b82f6', fill: '#3b82f6' }, // blue
  '7d': { stroke: '#8b5cf6', fill: '#8b5cf6' }, // violet
  '7d_sonnet': { stroke: '#14b8a6', fill: '#14b8a6' }, // teal
  '7d_opus': { stroke: '#f59e0b', fill: '#f59e0b' }, // amber
  '7d_fable': { stroke: '#65a30d', fill: '#84cc16' }, // lime
  overage: { stroke: '#f97316', fill: '#f97316' }, // orange
  unified: { stroke: '#64748b', fill: '#64748b' }, // slate
};

export function getWindowColor(window: string): {
  stroke: string;
  fill: string;
} {
  return WINDOW_COLORS[window] || { stroke: '#64748b', fill: '#64748b' };
}

export function getUpstreamColor(upstreamId: string): {
  line5h: string;
  line7d: string;
} {
  let hash = 0;
  for (let i = 0; i < upstreamId.length; i++) {
    hash = upstreamId.charCodeAt(i) + ((hash << 5) - hash);
  }
  const index = Math.abs(hash) % UPSTREAM_COLORS.length;
  return UPSTREAM_COLORS[index]!;
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
