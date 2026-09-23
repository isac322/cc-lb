import { describe, expect, it } from 'vitest';
import { deriveCouponNudge, type NudgeQuotaWindow } from './couponNudge';
import type { CedarEmberLimitResets, LimitResetGrant } from './limitResets';

const NOW = Date.parse('2026-09-24T12:00:00Z');
const HOUR = 3_600_000;

function grant(overrides: Partial<LimitResetGrant>): LimitResetGrant {
  return {
    id: 'g1',
    label: 'Coupon',
    resets_total: 1,
    resets_left: 1,
    starts_at: '2026-09-01T00:00:00Z',
    ends_at: '2026-10-01T00:00:00Z',
    clears: ['5h'],
    paused: false,
    usable_now: true,
    use_requires_limit: false,
    percent_used: {},
    blocking: [],
    ...overrides,
  };
}

function ember(
  grants: LimitResetGrant[],
  overrides: Partial<CedarEmberLimitResets> = {},
): CedarEmberLimitResets {
  return {
    eligible: true,
    ineligible_reason: null,
    at_limit: false,
    grants,
    next_grant_id: null,
    weekly_resets_at: null,
    cooldown_until: null,
    ...overrides,
  };
}

function window(overrides: Partial<NudgeQuotaWindow>): NudgeQuotaWindow {
  return {
    window: '5h',
    utilization: 1,
    state: 'fresh',
    resets_at_unix_secs: (NOW + 4 * HOUR) / 1000,
    ...overrides,
  };
}

describe('deriveCouponNudge', () => {
  it('counts a grant with no provider start date as active', () => {
    const nudge = deriveCouponNudge(
      ember([grant({ starts_at: null })]),
      [],
      NOW,
    );
    expect(nudge.activeCount).toBe(1);
    expect(nudge.usableCount).toBe(1);
  });

  it('treats a grant with no expiry as active but never expiring soon', () => {
    const nudge = deriveCouponNudge(ember([grant({ ends_at: null })]), [], NOW);
    expect(nudge.activeCount).toBe(1);
    expect(nudge.earliestExpiry).toBeNull();
    expect(nudge.expiresSoonAt).toBeNull();
    expect(nudge.kind).toBe('quiet');
  });

  it('excludes expired and depleted grants from the active count', () => {
    const nudge = deriveCouponNudge(
      ember([
        grant({ id: 'gone', ends_at: '2026-09-20T00:00:00Z' }),
        grant({ id: 'spent', resets_left: 0 }),
        grant({ id: 'live' }),
      ]),
      [],
      NOW,
    );
    expect(nudge.activeGrants.map((g) => g.id)).toEqual(['live']);
  });

  it('picks the soonest-expiring usable grant for the expiry hint', () => {
    const soon = '2026-09-24T20:00:00Z';
    const nudge = deriveCouponNudge(
      ember([
        grant({ id: 'later', ends_at: '2026-09-26T00:00:00Z' }),
        grant({ id: 'sooner', ends_at: soon }),
      ]),
      [],
      NOW,
    );
    expect(nudge.earliestExpiry).toBe(soon);
    expect(nudge.expiresSoonAt).toBe(Date.parse(soon));
    expect(nudge.kind).toBe('expiry');
  });

  it('recommends a reset only when a cleared window is full with a long wait', () => {
    const coupons = ember([grant({})]);
    const limited = deriveCouponNudge(coupons, [window({})], NOW);
    expect(limited.kind).toBe('limit');
    // A natural reset inside the hour is left to happen on its own.
    const soon = deriveCouponNudge(
      coupons,
      [window({ resets_at_unix_secs: (NOW + 30 * 60_000) / 1000 })],
      NOW,
    );
    expect(soon.kind).not.toBe('limit');
    // Stale or partial utilization never produces usage advice.
    const stale = deriveCouponNudge(coupons, [window({ state: 'stale' })], NOW);
    expect(stale.kind).not.toBe('limit');
  });

  it('suppresses usability while the account is on cooldown', () => {
    const nudge = deriveCouponNudge(
      ember([grant({})], {
        cooldown_until: '2026-09-24T13:00:00Z',
      }),
      [window({})],
      NOW,
    );
    expect(nudge.usableCount).toBe(0);
    expect(nudge.kind).toBe('quiet');
  });
});
