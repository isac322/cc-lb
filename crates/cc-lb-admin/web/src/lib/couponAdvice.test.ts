import { describe, expect, it } from 'vitest';
import { type CouponAdviceInput, deriveCouponAdvice } from './couponAdvice';
import {
  type CouponQuotaWindow,
  presentCouponEffects,
} from './couponPresentation';
import type { LimitResetGrant } from './limitResets';

const NOW = Date.parse('2026-11-20T12:00:00Z');
const HOUR = 3600;
const DAY = 24 * HOUR;

function grant(overrides: Partial<LimitResetGrant> = {}): LimitResetGrant {
  return {
    id: 'selected-credit',
    label: 'Reset',
    type: null,
    gives_back_weekly: false,
    unavailable_reason: null,
    resets_total: 1,
    resets_left: 1,
    starts_at: null,
    ends_at: new Date(NOW + 7 * DAY * 1000).toISOString(),
    clears: ['5h'],
    paused: false,
    usable_now: true,
    use_requires_limit: false,
    percent_used: {},
    blocking: [],
    ...overrides,
  };
}

function quota(
  window: string,
  used: number | null,
  resetInSeconds: number,
  state: CouponQuotaWindow['state'] = 'fresh',
): CouponQuotaWindow {
  return {
    window,
    utilization: used == null ? null : used / 100,
    state,
    resets_at_unix_secs: NOW / 1000 + resetInSeconds,
  };
}

function scenario({
  used = 25,
  resetIn = 2 * HOUR,
  expiresIn = 7 * DAY,
  state = 'fresh',
  extra = [],
  coupon = {},
}: {
  used?: number;
  resetIn?: number;
  expiresIn?: number | null;
  state?: CouponQuotaWindow['state'];
  extra?: CouponQuotaWindow[];
  coupon?: Partial<LimitResetGrant>;
} = {}): CouponAdviceInput {
  const selected = grant({
    ends_at:
      expiresIn == null ? null : new Date(NOW + expiresIn * 1000).toISOString(),
    ...coupon,
  });
  return {
    grant: selected,
    effects: presentCouponEffects(
      selected,
      [quota('5h', used, resetIn, state), ...extra],
      {
        provider: 'anthropic',
        providerFresh: true,
      },
    ),
    nowMs: NOW,
  };
}

function fable({
  fiveHour = false,
  weeklyUsed = fiveHour ? 80 : 100,
  fableUsed = 0,
  resetIn = 4 * DAY,
  expiresIn = 30 * DAY,
  weeklyState = 'fresh',
  fableState = 'fresh',
}: {
  fiveHour?: boolean;
  weeklyUsed?: number;
  fableUsed?: number;
  resetIn?: number;
  expiresIn?: number | null;
  weeklyState?: CouponQuotaWindow['state'];
  fableState?: CouponQuotaWindow['state'];
} = {}): CouponAdviceInput {
  return scenario({
    used: 100,
    resetIn: 3 * HOUR,
    expiresIn,
    state: fiveHour ? 'fresh' : 'absent',
    extra: [
      quota('7d', weeklyUsed, resetIn, weeklyState),
      quota('7d_fable', fableUsed, resetIn, fableState),
    ],
    coupon: {
      type: 'full',
      gives_back_weekly: true,
      clears: ['7d_overage_included'],
    },
  });
}

function monthly(used: number): CouponAdviceInput {
  const selected = grant({
    clears: [],
    ends_at: new Date(NOW + 60 * DAY * 1000).toISOString(),
    openai_credit: {
      id: 'selected-credit',
      reset_type: 'codex_rate_limits',
      status: 'available',
      is_supported_by_plan: true,
      granted_at: null,
      expires_at: null,
      title: null,
      description: null,
    },
    openai_manual_redemption: {
      eligible: used > 20,
      reason: used > 20 ? null : 'below_redemption_threshold',
      windows: [
        {
          window: 'monthly',
          used_percent: used,
          resets_at_unix_secs: NOW / 1000 + 10 * DAY,
          duration_seconds: 30 * DAY,
        },
      ],
    },
  });
  return {
    grant: selected,
    effects: presentCouponEffects(selected, [], {
      provider: 'openai',
      providerFresh: true,
    }),
    nowMs: NOW,
    blocked: used <= 20,
  };
}

const matrix: [string, () => CouponAdviceInput, string][] = [
  ['empty', () => scenario({ used: 0 }), 'nothing_to_restore'],
  ['reset-soon', () => scenario({ used: 100, resetIn: 8 * 60 }), 'wait'],
  ['below-pace', () => scenario(), 'save'],
  [
    'useful-now',
    () => scenario({ used: 100, resetIn: 3 * HOUR }),
    'useful_now',
  ],
  ['expires-first', () => scenario({ expiresIn: 20 * 60 }), 'expiry'],
  ['stale', () => scenario({ state: 'stale' }), 'unknown'],
  [
    'mixed-windows',
    () =>
      scenario({
        used: 100,
        resetIn: 8 * 60,
        expiresIn: 14 * DAY,
        coupon: { type: 'full', clears: ['5h', '7d'] },
        extra: [quota('7d', 90, 4 * DAY)],
      }),
    'useful_now',
  ],
  [
    'expiry-before-soon',
    () => scenario({ used: 100, resetIn: 8 * 60, expiresIn: 4 * 60 }),
    'useful_now',
  ],
  [
    'empty-expiring',
    () => scenario({ used: 0, expiresIn: 4 * 60 }),
    'nothing_to_restore',
  ],
  ['expiry-unknown', () => scenario({ expiresIn: null }), 'unknown'],
  [
    'wrong-scope',
    () => scenario({ expiresIn: 14 * DAY, extra: [quota('7d', 100, 4 * DAY)] }),
    'scope',
  ],
  ['fable-zero-bucket-on-full-weekly', () => fable(), 'scope'],
  ['fable-five-hour', () => fable({ fiveHour: true }), 'useful_now'],
  [
    'fable-unblocks-weekly',
    () => fable({ weeklyUsed: 100, fableUsed: 41 }),
    'useful_now',
  ],
  [
    'fable-nothing-to-restore',
    () => fable({ weeklyUsed: 74, fableUsed: 0 }),
    'nothing_to_restore',
  ],
  [
    'fable-intermediate-quiet',
    () => fable({ weeklyUsed: 80, fableUsed: 56 }),
    'none',
  ],
  [
    'fable-wait',
    () => fable({ weeklyUsed: 100, fableUsed: 40, resetIn: 20 * 60 }),
    'wait',
  ],
  [
    'fable-save',
    () => fable({ weeklyUsed: 15, fableUsed: 10, resetIn: 3 * DAY }),
    'save',
  ],
  [
    'fable-stale',
    () => fable({ weeklyUsed: 100, fableUsed: 41, fableState: 'stale' }),
    'unknown',
  ],
  ['openai-blocked', () => monthly(20), 'provider_blocked'],
  ['openai-below-pace', () => monthly(25), 'save'],
  ['neutral', () => scenario({ used: 55 }), 'none'],
];

describe('coupon advice scenario matrix', () => {
  it.each(matrix)(
    '%s is computed from quota and the selected grant',
    (_name, input, expected) => {
      const value = input();
      const advice = deriveCouponAdvice(value);
      expect(
        advice?.kind ?? (value.blocked ? 'provider_blocked' : 'none'),
      ).toBe(expected);
    },
  );

  it('keeps scope guidance limited to fresh exhausted common windows known unchanged', () => {
    const fresh = scenario({ extra: [quota('7d', 100, 4 * DAY)] });
    expect(deriveCouponAdvice(fresh)?.kind).toBe('scope');
    for (const state of ['stale', 'unobserved', 'absent'] as const) {
      expect(
        deriveCouponAdvice(
          scenario({ extra: [quota('7d', 100, 4 * DAY, state)] }),
        )?.kind,
      ).not.toBe('scope');
    }
    expect(
      deriveCouponAdvice(
        scenario({ extra: [quota('7d_sonnet', 100, 4 * DAY)] }),
      )?.kind,
    ).not.toBe('scope');
    const resetInsteadOfUnchanged = {
      ...fresh,
      effects: {
        ...fresh.effects,
        rows: fresh.effects.rows.map((row) =>
          row.window === '7d' ? { ...row, effect: 'reset' as const } : row,
        ),
      },
    };
    expect(deriveCouponAdvice(resetInsteadOfUnchanged)?.kind).not.toBe('scope');
  });

  it('includes one hour but not one hour plus a second in conditional wait guidance', () => {
    expect(
      deriveCouponAdvice(scenario({ used: 100, resetIn: HOUR }))?.kind,
    ).toBe('wait');
    expect(
      deriveCouponAdvice(scenario({ used: 100, resetIn: HOUR + 1 }))?.kind,
    ).toBe('useful_now');
    expect(deriveCouponAdvice(scenario({ used: 100, resetIn: 0 }))?.kind).toBe(
      'useful_now',
    );
  });

  it('uses the inclusive 10pp save margin without rounding observations first', () => {
    expect(deriveCouponAdvice(scenario({ used: 50 }))?.kind).toBe('save');
    expect(deriveCouponAdvice(scenario({ used: 50.001 }))).toBeNull();
  });

  it('does not recommend waiting when expiry is equal to the natural reset', () => {
    const advice = deriveCouponAdvice(
      scenario({ used: 100, resetIn: 8 * 60, expiresIn: 8 * 60 }),
    );
    expect(advice?.kind).toBe('useful_now');
    expect(advice?.message).toContain('expires');
    expect(deriveCouponAdvice(scenario({ expiresIn: 2 * HOUR }))?.kind).toBe(
      'expiry',
    );
  });

  it('uses only the selected credit expiry, never a later sibling expiry', () => {
    const early = scenario({ expiresIn: 20 * 60 });
    const later = scenario({ expiresIn: 7 * DAY });
    expect(deriveCouponAdvice(early)?.kind).toBe('expiry');
    expect(deriveCouponAdvice(later)?.kind).toBe('save');
    expect(deriveCouponAdvice(scenario({ expiresIn: null }))?.kind).toBe(
      'unknown',
    );
  });

  it('requires all positive restoration targets to support wait/save and ignores unchanged zero windows', () => {
    const mixed = scenario({
      used: 100,
      resetIn: 8 * 60,
      coupon: { type: 'full', clears: ['5h', '7d'] },
      extra: [quota('7d', 90, 4 * DAY)],
    });
    expect(deriveCouponAdvice(mixed)?.kind).toBe('useful_now');
    const uncertain = scenario({
      used: 25,
      coupon: { type: 'full', clears: ['5h', '7d'] },
      extra: [quota('7d', 50, 4 * DAY, 'stale')],
    });
    expect(deriveCouponAdvice(uncertain)?.kind).toBe('unknown');
  });

  it('gives determinate Fable advice for all observed weekly and bucket combinations', () => {
    const unblocks = deriveCouponAdvice(
      fable({ weeklyUsed: 100, fableUsed: 41 }),
    );
    expect(unblocks?.kind).toBe('useful_now');
    expect(unblocks?.message).toContain('Fable portion');
    expect(unblocks?.message).not.toContain('expires');

    const unblocksExpiring = deriveCouponAdvice(
      fable({
        weeklyUsed: 100,
        fableUsed: 41,
        resetIn: 4 * DAY,
        expiresIn: 2 * DAY,
      }),
    );
    expect(unblocksExpiring?.kind).toBe('useful_now');
    expect(unblocksExpiring?.message).toContain('Fable portion');
    expect(unblocksExpiring?.message).toContain('expires');

    expect(
      deriveCouponAdvice(fable({ weeklyUsed: 80, fableUsed: 56 })),
    ).toBeNull();
    expect(
      deriveCouponAdvice(fable({ weeklyUsed: 74, fableUsed: 0 }))?.kind,
    ).toBe('nothing_to_restore');

    const zeroOnFull = deriveCouponAdvice(
      fable({ weeklyUsed: 100, fableUsed: 0 }),
    );
    expect(zeroOnFull?.kind).toBe('scope');
    expect(zeroOnFull?.message).toContain('Weekly (Fable) is at 0%');

    expect(
      deriveCouponAdvice(
        fable({ weeklyUsed: 15, fableUsed: 10, resetIn: 3 * DAY }),
      )?.kind,
    ).toBe('save');
    expect(
      deriveCouponAdvice(
        fable({ weeklyUsed: 100, fableUsed: 40, resetIn: 20 * 60 }),
      )?.kind,
    ).toBe('wait');

    expect(
      deriveCouponAdvice(
        fable({ weeklyUsed: 100, fableUsed: 41, weeklyState: 'stale' }),
      )?.message,
    ).toContain('stale');
    expect(
      deriveCouponAdvice(
        fable({ weeklyUsed: 100, fableUsed: 41, fableState: 'stale' }),
      )?.message,
    ).toContain('stale');
    // Fresh 0% Weekly (Fable) must not emit nothing_to_restore when Weekly is stale or unobserved.
    const staleWeeklyZeroBucket = deriveCouponAdvice(
      fable({ weeklyUsed: 100, fableUsed: 0, weeklyState: 'stale' }),
    );
    expect(staleWeeklyZeroBucket?.kind).toBe('unknown');
    expect(staleWeeklyZeroBucket?.message).toContain('stale');
    const unobservedWeeklyZeroBucket = deriveCouponAdvice(
      fable({ weeklyUsed: 74, fableUsed: 0, weeklyState: 'unobserved' }),
    );
    expect(unobservedWeeklyZeroBucket?.kind).toBe('unknown');
    expect(deriveCouponAdvice(fable({ fiveHour: true }))?.kind).toBe(
      'useful_now',
    );
  });

  it('distinguishes stale usage from missing expiry in neutral guidance', () => {
    expect(deriveCouponAdvice(scenario({ state: 'stale' }))?.message).toContain(
      'stale',
    );
    expect(
      deriveCouponAdvice(scenario({ expiresIn: null }))?.message,
    ).toContain('expiry');
  });

  it('uses native monthly duration rather than assuming a 30-day or weekly period', () => {
    const input = monthly(25);
    expect(deriveCouponAdvice(input)?.kind).toBe('save');
    if (!input.grant.openai_manual_redemption)
      throw new Error('Missing native fixture');
    input.grant.openai_manual_redemption.windows[0].duration_seconds = 12 * DAY;
    expect(deriveCouponAdvice(input)).toBeNull();
    input.grant.openai_manual_redemption.windows[0].duration_seconds = null;
    expect(deriveCouponAdvice(input)?.kind).toBe('unknown');
  });

  it('never stacks advice on a provider block or an unresolved operation', () => {
    expect(
      deriveCouponAdvice({ ...scenario({ used: 100 }), blocked: true }),
    ).toBeNull();
  });

  it('gives determinate advice for weekly coupons clearing 7d and 7d_overage_included without false unknown status', () => {
    const weeklyFull = deriveCouponAdvice(
      scenario({
        used: 20,
        resetIn: 2 * HOUR,
        expiresIn: 30 * DAY,
        extra: [quota('7d', 100, 4 * DAY), quota('7d_fable', 80, 4 * DAY)],
        coupon: {
          type: 'weekly',
          gives_back_weekly: false,
          clears: ['7d', '7d_overage_included'],
          percent_used: { '7d': 100, '7d_overage_included': 80 },
        },
      }),
    );
    expect(weeklyFull?.kind).toBe('useful_now');

    const weeklyEmpty = deriveCouponAdvice(
      scenario({
        used: 20,
        resetIn: 2 * HOUR,
        expiresIn: 30 * DAY,
        extra: [quota('7d', 0, 4 * DAY), quota('7d_fable', 0, 4 * DAY)],
        coupon: {
          type: 'weekly',
          gives_back_weekly: false,
          clears: ['7d', '7d_overage_included'],
          percent_used: { '7d': 0, '7d_overage_included': 0 },
        },
      }),
    );
    expect(weeklyEmpty?.kind).toBe('nothing_to_restore');

    const fableFull = deriveCouponAdvice(
      scenario({
        used: 20,
        resetIn: 2 * HOUR,
        expiresIn: 30 * DAY,
        extra: [quota('7d', 80, 4 * DAY), quota('7d_fable', 100, 4 * DAY)],
        coupon: {
          type: 'full',
          gives_back_weekly: true,
          clears: ['7d_overage_included'],
          percent_used: { '5h': 20, '7d': 80, '7d_overage_included': 100 },
        },
      }),
    );
    expect(fableFull?.kind).toBe('useful_now');
  });
});
