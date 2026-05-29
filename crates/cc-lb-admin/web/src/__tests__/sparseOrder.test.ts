import { describe, expect, it } from 'vitest';
import {
  between,
  needsRebalance,
  nextAfter,
  rebalance,
  STEP,
} from '../lib/sparseOrder';

describe('sparseOrder', () => {
  describe('nextAfter', () => {
    it('returns STEP for empty array', () => {
      expect(nextAfter([])).toBe(STEP);
    });

    it('grows by step', () => {
      expect(nextAfter([1000])).toBe(2000);
      expect(nextAfter([1000, 2000, 3000])).toBe(4000);
      expect(nextAfter([3000, 1000, 2000])).toBe(4000);
    });
  });

  describe('between', () => {
    it('returns midpoint', () => {
      expect(between(1000, 3000)).toBe(2000);
      expect(between(0, 10)).toBe(5);
      expect(between(100, 200)).toBe(150);
    });

    it('throws if gap is too tight', () => {
      expect(() => between(1000, 1001)).toThrow('needs_rebalance');
    });
  });

  describe('rebalance', () => {
    it('is idempotent', () => {
      const input = [1000, 2000, 3000, 4000];
      const rebalanced = rebalance(input);
      expect(rebalanced).toEqual([1000, 2000, 3000, 4000]);
      expect(rebalance(rebalanced)).toEqual([1000, 2000, 3000, 4000]);
    });

    it('handles unsorted input by mapping to index', () => {
      // Note: the Rust implementation maps by index, not by sorted order.
      // Wait, let's check the Rust implementation:
      // existing.iter().enumerate().map(|(idx, _)| STEP * (idx as i64 + 1)).collect()
      // So it just replaces the elements with 1000, 2000, 3000...
      const input = [3000, 1000, 2000];
      const rebalanced = rebalance(input);
      expect(rebalanced).toEqual([1000, 2000, 3000]);
    });
  });

  describe('needsRebalance', () => {
    it('detects tight gap', () => {
      expect(needsRebalance([1000, 1001])).toBe(true);
      expect(needsRebalance([1000, 1002])).toBe(false);
      expect(needsRebalance([1000, 2000, 2001, 3000])).toBe(true);
      expect(needsRebalance([])).toBe(false);
      expect(needsRebalance([1000])).toBe(false);
      expect(needsRebalance([1001, 1000])).toBe(true);
    });

    it('returns false for healthy gaps', () => {
      expect(needsRebalance([1000, 2000, 3000, 4000])).toBe(false);
      expect(needsRebalance([1000, 1500, 2000, 2500, 3000])).toBe(false);
    });
  });
});
