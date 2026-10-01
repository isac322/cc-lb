import { describe, expect, it } from 'vitest';
import { WINDOW_DURATION_SECS } from './colors';
import {
  formatQuotaPercent,
  quotaPacePct,
  quotaSeverity,
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

  it('is danger from 95% used regardless of pace', () => {
    expect(quotaSeverity(99, 99)).toBe('danger');
    expect(quotaSeverity(95, 0)).toBe('danger');
  });

  it.each([null, 0, 50, 99])('100 used is danger at pace %s', (pace) => {
    expect(quotaSeverity(100, pace)).toBe('danger');
  });

  it('warns from 80% when pace is unavailable', () => {
    expect(quotaSeverity(79.9, null)).toBe('ok');
    expect(quotaSeverity(80, null)).toBe('warn');
    expect(quotaSeverity(80)).toBe('warn');
    expect(quotaSeverity(94, null)).toBe('warn');
    expect(quotaSeverity(95, null)).toBe('danger');
  });

  it('is danger when used runs 30+ points ahead of pace', () => {
    expect(quotaSeverity(80, 50)).toBe('danger');
    expect(quotaSeverity(30, 0)).toBe('danger');
    expect(quotaSeverity(79, 50)).toBe('warn');
  });

  it('warns at 90%+ used or 10+ points ahead of pace', () => {
    expect(quotaSeverity(94, 99)).toBe('warn');
    expect(quotaSeverity(90, 99)).toBe('warn');
    expect(quotaSeverity(10, 0)).toBe('warn');
    expect(quotaSeverity(89, 79)).toBe('warn');
    expect(quotaSeverity(20, 10)).toBe('warn');
  });

  it('is ok on or behind pace below the warn bar', () => {
    expect(quotaSeverity(80, 80)).toBe('ok');
    expect(quotaSeverity(50, 80)).toBe('ok');
    expect(quotaSeverity(0, 0)).toBe('ok');
    expect(quotaSeverity(89, 85)).toBe('ok'); // below 90, gap under 10
  });

  it('treats a non-finite pace as no pace', () => {
    expect(quotaSeverity(80, Number.NaN)).toBe('warn');
    expect(quotaSeverity(79, Number.NaN)).toBe('ok');
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

describe('formatQuotaPercent', () => {
  it('never rounds a live reading to 0% or 100%', () => {
    expect(formatQuotaPercent(0.4)).toBe('<1%');
    expect(formatQuotaPercent(99.6)).toBe('99%');
    expect(formatQuotaPercent(null)).toBe('—');
    expect(formatQuotaPercent(Number.NaN)).toBe('—');
  });
});
