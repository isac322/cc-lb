import { describe, expect, it } from 'vitest';
import {
  budgetToTier,
  REASONING_TIER_THRESHOLDS,
  reasoningBadgeText,
  serviceTierToFast,
} from './reasoningTier';

describe('budgetToTier', () => {
  it('returns none for null', () => {
    expect(budgetToTier(null)).toBe('none');
  });

  it('returns none for undefined', () => {
    expect(budgetToTier(undefined)).toBe('none');
  });

  it('returns none for 0', () => {
    expect(budgetToTier(0)).toBe('none');
  });

  it('returns none for 1023 (just below low threshold)', () => {
    expect(budgetToTier(1023)).toBe('none');
  });

  it('returns low for 1024 (low threshold)', () => {
    expect(budgetToTier(1024)).toBe('low');
  });

  it('returns low for 4000 (high end of low range)', () => {
    expect(budgetToTier(4000)).toBe('low');
  });

  it('returns medium for 4001 (just above low threshold)', () => {
    expect(budgetToTier(4001)).toBe('medium');
  });

  it('returns medium for 12000 (high end of medium range)', () => {
    expect(budgetToTier(12000)).toBe('medium');
  });

  it('returns high for 12001 (just above medium threshold)', () => {
    expect(budgetToTier(12001)).toBe('high');
  });

  it('returns high for 24000 (high end of high range)', () => {
    expect(budgetToTier(24000)).toBe('high');
  });

  it('returns xhigh for 24001 (just above high threshold)', () => {
    expect(budgetToTier(24001)).toBe('xhigh');
  });

  it('returns xhigh for 32000 (high end of xhigh range)', () => {
    expect(budgetToTier(32000)).toBe('xhigh');
  });

  it('returns max for 32001 (just above xhigh threshold)', () => {
    expect(budgetToTier(32001)).toBe('max');
  });

  it('returns max for very large budget', () => {
    expect(budgetToTier(1_000_000)).toBe('max');
  });
});

describe('serviceTierToFast', () => {
  it('returns true for priority', () => {
    expect(serviceTierToFast('priority')).toBe(true);
  });

  it('returns false for standard', () => {
    expect(serviceTierToFast('standard')).toBe(false);
  });

  it('returns false for batch', () => {
    expect(serviceTierToFast('batch')).toBe(false);
  });

  it('returns false for null', () => {
    expect(serviceTierToFast(null)).toBe(false);
  });

  it('returns false for undefined', () => {
    expect(serviceTierToFast(undefined)).toBe(false);
  });

  it('returns false for empty string', () => {
    expect(serviceTierToFast('')).toBe(false);
  });

  it('returns false for other arbitrary string', () => {
    expect(serviceTierToFast('unknown')).toBe(false);
  });
});

describe('reasoningBadgeText', () => {
  it('returns effort only', () => {
    expect(reasoningBadgeText('max', null, null)).toBe('max');
  });

  it('returns effort + tokens', () => {
    expect(reasoningBadgeText('max', null, 8200)).toBe('max · 8.2k');
  });

  it('returns legacy budget + tokens', () => {
    expect(reasoningBadgeText(null, 18000, 8200)).toBe('high · 8.2k');
  });

  it('returns legacy budget only', () => {
    expect(reasoningBadgeText(null, 18000, null)).toBe('high');
  });

  it('returns tokens only', () => {
    expect(reasoningBadgeText(null, null, 8200)).toBe('8.2k');
  });

  it('returns nothing', () => {
    expect(reasoningBadgeText(null, null, null)).toBeNull();
  });

  it('effort takes precedence over budget', () => {
    expect(reasoningBadgeText('max', 1024, null)).toBe('max');
  });

  it('zero/absent tokens not shown', () => {
    expect(reasoningBadgeText('max', null, 0)).toBe('max');
  });
});

describe('REASONING_TIER_THRESHOLDS', () => {
  it('exports a threshold table with all tiers', () => {
    expect(REASONING_TIER_THRESHOLDS).toHaveProperty('none');
    expect(REASONING_TIER_THRESHOLDS).toHaveProperty('low');
    expect(REASONING_TIER_THRESHOLDS).toHaveProperty('medium');
    expect(REASONING_TIER_THRESHOLDS).toHaveProperty('high');
    expect(REASONING_TIER_THRESHOLDS).toHaveProperty('xhigh');
    expect(REASONING_TIER_THRESHOLDS).toHaveProperty('max');
  });

  it('has correct min/max bounds for none', () => {
    expect(REASONING_TIER_THRESHOLDS.none.min).toBe(0);
    expect(REASONING_TIER_THRESHOLDS.none.max).toBe(1023);
  });

  it('has correct min/max bounds for low', () => {
    expect(REASONING_TIER_THRESHOLDS.low.min).toBe(1024);
    expect(REASONING_TIER_THRESHOLDS.low.max).toBe(4000);
  });

  it('has correct min/max bounds for medium', () => {
    expect(REASONING_TIER_THRESHOLDS.medium.min).toBe(4001);
    expect(REASONING_TIER_THRESHOLDS.medium.max).toBe(12000);
  });

  it('has correct min/max bounds for high', () => {
    expect(REASONING_TIER_THRESHOLDS.high.min).toBe(12001);
    expect(REASONING_TIER_THRESHOLDS.high.max).toBe(24000);
  });

  it('has correct min/max bounds for xhigh', () => {
    expect(REASONING_TIER_THRESHOLDS.xhigh.min).toBe(24001);
    expect(REASONING_TIER_THRESHOLDS.xhigh.max).toBe(32000);
  });

  it('has correct min/max bounds for max', () => {
    expect(REASONING_TIER_THRESHOLDS.max.min).toBe(32001);
    expect(REASONING_TIER_THRESHOLDS.max.max).toBe(Infinity);
  });
});
