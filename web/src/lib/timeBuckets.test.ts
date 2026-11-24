import { describe, expect, it } from 'vitest';
import {
  BUCKET_LADDER_MS,
  bucketCountFor,
  chooseBucketMs,
  MAX_HISTOGRAM_BUCKETS,
} from './timeBuckets';

const MINUTE = 60_000;
const DAY = 24 * 60 * MINUTE;

describe('chooseBucketMs', () => {
  it('keeps the finest step while the span still fits the cap', () => {
    expect(chooseBucketMs(MAX_HISTOGRAM_BUCKETS * MINUTE)).toBe(MINUTE);
  });

  it('steps up as soon as one more bucket would exceed the cap', () => {
    expect(chooseBucketMs(MAX_HISTOGRAM_BUCKETS * MINUTE + 1)).toBe(5 * MINUTE);
  });

  it('walks the whole ladder rather than jumping to the coarsest step', () => {
    const chosen = BUCKET_LADDER_MS.map((step) =>
      chooseBucketMs(MAX_HISTOGRAM_BUCKETS * step),
    );
    expect(chosen).toEqual([...BUCKET_LADDER_MS]);
  });

  it('saturates at the coarsest step for spans past the ladder', () => {
    expect(chooseBucketMs(10_000 * DAY)).toBe(DAY);
  });

  it('handles a degenerate span without dividing by zero', () => {
    expect(chooseBucketMs(0)).toBe(MINUTE);
  });
});

describe('bucketCountFor', () => {
  it('counts the range inclusively, matching the server formula', () => {
    // The server computes ((until - since + 1) * 1000).div_ceil(bucket_ms);
    // an off-by-one here means the request is rejected for exceeding the cap.
    expect(bucketCountFor(0, 59, MINUTE)).toBe(1);
    expect(bucketCountFor(0, 60, MINUTE)).toBe(2);
  });

  it('reports the overflow that a bucket-aligned snap can introduce', () => {
    // A domain sized exactly at the cap widens past it once both edges snap,
    // which is why the caller trims the oldest buckets before requesting.
    const span = MAX_HISTOGRAM_BUCKETS * DAY;
    const sinceSecs = 0;
    const untilSecs = span / 1000;
    expect(bucketCountFor(sinceSecs, untilSecs, DAY)).toBeGreaterThan(
      MAX_HISTOGRAM_BUCKETS,
    );
    const overflow =
      bucketCountFor(sinceSecs, untilSecs, DAY) - MAX_HISTOGRAM_BUCKETS;
    expect(
      bucketCountFor(sinceSecs + (overflow * DAY) / 1000, untilSecs, DAY),
    ).toBe(MAX_HISTOGRAM_BUCKETS);
  });
});
