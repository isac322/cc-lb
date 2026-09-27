import type { UsageBucket } from '../../../lib/api';
import { SLICE_COLORS } from './sliceColors';

/**
 * Cost categories in request-log order. The log table, its cost popover and the
 * Overview principal meters all read this list, so a bar drawn in one view
 * cannot disagree with a breakdown shown in another.
 */
export const COST_CATEGORIES = [
  { key: 'input', label: 'Input', color: SLICE_COLORS.input },
  { key: 'output', label: 'Output', color: SLICE_COLORS.output },
  {
    key: 'cache_create_5m',
    label: 'Cache create 5m',
    color: SLICE_COLORS.cache_create_5m,
  },
  {
    key: 'cache_create_1h',
    label: 'Cache create 1h',
    color: SLICE_COLORS.cache_create_1h,
  },
  { key: 'cache_read', label: 'Cache read', color: SLICE_COLORS.cache_read },
] as const;

export type CostCategoryKey = (typeof COST_CATEGORIES)[number]['key'];

/**
 * Cost an authoritative total reports that no category accounts for — windows
 * rolled up before per-category cost was persisted. Neutral on purpose: it is
 * missing bookkeeping, not a sixth kind of token.
 */
export const UNATTRIBUTED_CATEGORY = {
  key: 'unattributed',
  label: 'Unattributed',
  color: 'var(--color-neutral)',
} as const;

/** Per-category cost in integer micros. */
export type CostComponentMicros = Record<CostCategoryKey, number>;

/** One ordered slice of a cost bar or breakdown popover. */
export interface CostSegment {
  key: CostCategoryKey | typeof UNATTRIBUTED_CATEGORY.key;
  label: string;
  color: string;
  value: number;
}

/** Field carrying each category on a `/admin/usage` bucket. */
const USAGE_COST_FIELD = {
  input: 'cost_input_micros',
  output: 'cost_output_micros',
  cache_create_5m: 'cost_cache_creation_5m_micros',
  cache_create_1h: 'cost_cache_creation_1h_micros',
  cache_read: 'cost_cache_read_micros',
} as const satisfies Record<CostCategoryKey, keyof UsageBucket>;

export function emptyCostComponents(): CostComponentMicros {
  return {
    input: 0,
    output: 0,
    cache_create_5m: 0,
    cache_create_1h: 0,
    cache_read: 0,
  };
}

/**
 * Adds one usage bucket's recorded cost components into `into`, and reports
 * whether the bucket carried any. Buckets that recorded none leave `into`
 * alone: a missing field is unknown cost, never zero cost.
 */
export function addBucketCostMicros(
  bucket: UsageBucket,
  into: CostComponentMicros,
): boolean {
  let recorded = false;
  for (const category of COST_CATEGORIES) {
    const micros = bucket[USAGE_COST_FIELD[category.key]];
    if (micros == null) continue;
    into[category.key] += micros;
    recorded = true;
  }
  return recorded;
}

export function sumCostMicros(components: CostComponentMicros): number {
  let total = 0;
  for (const category of COST_CATEGORIES) total += components[category.key];
  return total;
}

/**
 * Ordered segments for a cost bar or breakdown popover: the five categories,
 * then whatever an authoritative total reports on top of them. A remainder of
 * zero or less drops the tail, so a fully attributed window shows five rows.
 */
export function costCategorySegments(
  components: CostComponentMicros,
  unattributedMicros = 0,
): CostSegment[] {
  const segments: CostSegment[] = COST_CATEGORIES.map((category) => ({
    key: category.key,
    label: category.label,
    color: category.color,
    value: components[category.key],
  }));
  if (unattributedMicros > 0) {
    segments.push({ ...UNATTRIBUTED_CATEGORY, value: unattributedMicros });
  }
  return segments;
}
