// cache_create_5m/1h share the `purple` hue with different shades on purpose:
// same category (cache creation), two pricing tiers (5m vs 1h TTL).
export const SLICE_COLORS = {
  input: '#0ea5e9', // sky-500
  output: '#10b981', // emerald-500
  cache_create_5m: '#c084fc', // purple-400
  cache_create_1h: '#7e22ce', // purple-700
  cache_read: '#f59e0b', // amber-500
} as const;
