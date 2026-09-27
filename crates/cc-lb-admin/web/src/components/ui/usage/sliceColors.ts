/**
 * The five kinds of token a request is billed for, in the one order every
 * token and cost composition draws them: request-table bars and popovers,
 * the request drawer breakdowns and the Overview cost meters.
 *
 * Cache read leads, so the green run from the bar's left edge reads directly
 * as the cache hit; cache writes (amber 5m, orange 1h) sit right next to it,
 * so the two outcomes of the cache are compared side by side; uncached input
 * follows and output closes the bar on the right.
 *
 * Colors are the theme's series tokens (`--color-series-*` in `index.css`):
 * cache read green (served from cache, the good outcome), cache writes amber
 * and orange (the costly miss, never red), input quiet slate, output violet.
 */
export const USAGE_CATEGORIES = [
  {
    key: 'cache_read',
    label: 'Cache read',
    color: 'var(--color-series-cache-read)',
  },
  {
    key: 'cache_create_5m',
    label: 'Cache create 5m',
    color: 'var(--color-series-cache-create-5m)',
  },
  {
    key: 'cache_create_1h',
    label: 'Cache create 1h',
    color: 'var(--color-series-cache-create-1h)',
  },
  { key: 'input', label: 'Input', color: 'var(--color-series-input)' },
  { key: 'output', label: 'Output', color: 'var(--color-series-output)' },
] as const;

export type UsageCategoryKey = (typeof USAGE_CATEGORIES)[number]['key'];
