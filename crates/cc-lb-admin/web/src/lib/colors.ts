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
