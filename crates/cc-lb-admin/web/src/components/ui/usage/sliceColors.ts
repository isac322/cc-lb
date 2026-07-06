// Palette is anchored on the request-log table (Tailwind *-400 tones):
// the detail-drawer pie charts consume the same values so both views agree.
// cache_create_5m/1h share the amber hue at different shades — same category
// (cache creation), two pricing tiers (5m vs 1h TTL).
export const SLICE_COLORS = {
  input: '#38bdf8', // sky-400
  output: '#a78bfa', // violet-400
  cache_create_5m: '#fbbf24', // amber-400
  cache_create_1h: '#b45309', // amber-700
  cache_read: '#34d399', // emerald-400
} as const;
