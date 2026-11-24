export const BUCKET_LADDER_MS = [
  60_000,
  5 * 60_000,
  15 * 60_000,
  60 * 60_000,
  6 * 60 * 60_000,
  24 * 60 * 60_000,
] as const;

export const MAX_HISTOGRAM_BUCKETS = 240;

/** Return the smallest ladder step that keeps the span within the bucket cap. */
export function chooseBucketMs(spanMs: number): number {
  for (const bucketMs of BUCKET_LADDER_MS) {
    if (Math.ceil(spanMs / bucketMs) <= MAX_HISTOGRAM_BUCKETS) {
      return bucketMs;
    }
  }

  return BUCKET_LADDER_MS[BUCKET_LADDER_MS.length - 1];
}

/** Return the number of inclusive, second-aligned buckets for the range. */
export function bucketCountFor(
  sinceSecs: number,
  untilSecs: number,
  bucketMs: number,
): number {
  return Math.ceil(((untilSecs - sinceSecs + 1) * 1000) / bucketMs);
}
