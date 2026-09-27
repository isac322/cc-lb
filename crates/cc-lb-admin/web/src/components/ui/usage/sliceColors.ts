// Series colors are theme tokens (index.css `--color-series-*`), shared by the
// request-log table, its popovers and the detail-drawer pie charts so every
// view agrees. cache_create_5m/1h share the amber hue at different shades:
// same category (cache creation), two pricing tiers (5m vs 1h TTL).
export const SLICE_COLORS = {
  input: 'var(--color-series-input)',
  output: 'var(--color-series-output)',
  cache_create_5m: 'var(--color-series-cache-create-5m)',
  cache_create_1h: 'var(--color-series-cache-create-1h)',
  cache_read: 'var(--color-series-cache-read)',
} as const;
