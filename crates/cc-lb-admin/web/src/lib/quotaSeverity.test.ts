import { describe, expect, it } from 'vitest';
import { WINDOW_DURATION_SECS } from './colors';
import {
  formatQuotaPercent,
  quotaPacePct,
  quotaSeverity,
  snapshotQuotaPacePct,
  snapshotQuotaSeverity,
  worstQuotaSeverity,
} from './quotaSeverity';

const NOW = 1_800_000_000;
const H5 = WINDOW_DURATION_SECS['5h']!; // 18000
const D7 = WINDOW_DURATION_SECS['7d']!; // 604800

describe('quotaSeverity', () => {
  it.each([null, Number.NaN, Number.POSITIVE_INFINITY])(
    'has no reading for %s',
    (used) => {
      expect(quotaSeverity(used)).toBe('none');
      expect(quotaSeverity(used, 50)).toBe('none');
    },
  );
  it('is neutral however high when pace is unavailable', () => {
    expect(quotaSeverity(80, null)).toBe('ok');
    expect(quotaSeverity(95, null)).toBe('ok');
    expect(quotaSeverity(99)).toBe('ok');
    expect(quotaSeverity(99, undefined)).toBe('ok');
    expect(quotaSeverity(100, null)).toBe('ok');
    expect(quotaSeverity(100)).toBe('ok');
  });

  it('stays ok on even or behind pace, however high the reading', () => {
    expect(quotaSeverity(99, 99)).toBe('ok');
    expect(quotaSeverity(100, 100)).toBe('ok');
    expect(quotaSeverity(80, 80)).toBe('ok');
    expect(quotaSeverity(50, 80)).toBe('ok');
    expect(quotaSeverity(0, 0)).toBe('ok');
    expect(quotaSeverity(89, 85)).toBe('ok'); // gap under 10
  });

  it('is danger when used runs 30+ points ahead of pace', () => {
    expect(quotaSeverity(80, 50)).toBe('danger');
    expect(quotaSeverity(100, 70)).toBe('danger');
    expect(quotaSeverity(30, 0)).toBe('danger');
    expect(quotaSeverity(99, 69)).toBe('danger');
  });

  it('warns only when used runs 10-29 points ahead of pace', () => {
    expect(quotaSeverity(10, 0)).toBe('warn');
    expect(quotaSeverity(89, 79)).toBe('warn');
    expect(quotaSeverity(94, 84)).toBe('warn');
    expect(quotaSeverity(39, 29)).toBe('warn');
    expect(quotaSeverity(20, 10)).toBe('warn');
    expect(quotaSeverity(39, 30)).toBe('ok'); // gap under 10
  });

  it('treats a non-finite pace as no pace', () => {
    expect(quotaSeverity(100, Number.NaN)).toBe('ok');
    expect(quotaSeverity(80, Number.NaN)).toBe('ok');
    expect(quotaSeverity(80, Number.POSITIVE_INFINITY)).toBe('ok');
  });
});

describe('quotaPacePct', () => {
  it('is the share of the window elapsed', () => {
    // 5h window resetting in 1h: 4h of 5h elapsed → 80%.
    expect(quotaPacePct('5h', NOW + 3600, NOW)).toBeCloseTo(80, 10);
    // 7d window resetting in 5d: 2d of 7d elapsed.
    expect(quotaPacePct('7d', NOW + 5 * 86400, NOW)).toBeCloseTo(
      (2 / 7) * 100,
      10,
    );
    expect(quotaPacePct('7d_fable', NOW + 86400, NOW)).toBeCloseTo(
      ((D7 - 86400) / D7) * 100,
      10,
    );
  });

  it.each([
    ['missing reset', '5h', null, NOW],
    ['undefined reset', '5h', undefined, NOW],
    ['non-finite reset', '5h', Number.NaN, NOW],
    ['untimed window (extra usage)', 'overage', NOW + 3600, NOW],
    ['untimed window (unified)', 'unified', NOW + 3600, NOW],
    ['unknown window', 'bogus', NOW + 3600, NOW],
    ['expired reset', '5h', NOW - 1, NOW],
    ['reset exactly now', '5h', NOW, NOW],
    [
      'window not yet open (reset more than one length ahead)',
      '5h',
      NOW + H5 + 3600,
      NOW,
    ],
    ['window opens exactly now', '5h', NOW + H5, NOW],
  ])('is null for %s', (_label, window, reset, now) => {
    expect(quotaPacePct(window, reset, now)).toBeNull();
  });

  it('caps at 100 until the reset passes', () => {
    // 1s before reset the pace is effectively 100, never past it.
    expect(quotaPacePct('5h', NOW + 1, NOW)).toBeLessThanOrEqual(100);
    expect(quotaPacePct('5h', NOW + 1, NOW)).toBeGreaterThan(99);
  });
});

describe('snapshotQuotaSeverity', () => {
  const reading = (
    utilization: number | null,
    resetsAt: number | null,
    state = 'fresh',
  ) => ({ window: '5h', state, utilization, resets_at_unix_secs: resetsAt });

  it('is ok for 80% used exactly on pace', () => {
    // 5h window resetting in 1h: pace 80.
    expect(snapshotQuotaSeverity(reading(0.8, NOW + 3600), NOW)).toBe('ok');
  });

  it('is danger for 30%+ used right after the window opens', () => {
    // One minute into the window: pace ~0.3, used 31 runs 30+ points ahead.
    expect(snapshotQuotaSeverity(reading(0.31, NOW + H5 - 60), NOW)).toBe(
      'danger',
    );
  });

  it.each([
    ['an unobserved snapshot', reading(0.8, NOW + 3600, 'unobserved')],
    ['an expired reset', reading(0.8, NOW - 1)],
    ['no reset', reading(0.8, null)],
  ])('stays neutral for %s', (_label, snap) => {
    expect(snapshotQuotaSeverity(snap, NOW)).toBe('ok');
    expect(snapshotQuotaSeverity({ ...snap, utilization: 0.99 }, NOW)).toBe(
      'ok',
    );
  });

  it('has no reading without utilization', () => {
    expect(snapshotQuotaSeverity(reading(null, NOW + 3600), NOW)).toBe('none');
  });
});

describe('snapshotQuotaPacePct', () => {
  it("reads a past moment's pace inside the snapshot's window, not today's", () => {
    const snap = {
      window: '5h',
      state: 'fresh',
      utilization: 0.5,
      resets_at_unix_secs: NOW + 3600,
    };
    // Window opened 4h ago; 2h ago it was 2h of 5h in → 40.
    expect(snapshotQuotaPacePct(snap, NOW - 7200)).toBeCloseTo(40, 10);
    // Before that window opened the reset is unknown: no pace.
    expect(snapshotQuotaPacePct(snap, NOW - 5 * 3600)).toBeNull();
  });
});

describe('worstQuotaSeverity', () => {
  it('picks the most severe reading', () => {
    expect(worstQuotaSeverity(['ok', 'danger', 'warn'])).toBe('danger');
    expect(worstQuotaSeverity(['none', 'ok'])).toBe('ok');
    expect(worstQuotaSeverity([])).toBe('none');
  });
});

describe('formatQuotaPercent', () => {
  it('never rounds a live reading to 0% or 100%', () => {
    expect(formatQuotaPercent(0.4)).toBe('<1%');
    expect(formatQuotaPercent(99.6)).toBe('99%');
    expect(formatQuotaPercent(null)).toBe('—');
    expect(formatQuotaPercent(Number.NaN)).toBe('—');
  });
});
