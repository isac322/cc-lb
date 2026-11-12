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

function fable(fiveHour: boolean): CouponAdviceInput {
  return scenario({
    used: 100,
    resetIn: 3 * HOUR,
    expiresIn: 30 * DAY,
    state: fiveHour ? 'fresh' : 'absent',
    extra: [
      quota('7d', fiveHour ? 80 : 100, 4 * DAY),
      quota('7d_fable', 0, 4 * DAY),
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
  ['fable-unknown', () => fable(false), 'unknown'],
  ['fable-five-hour', () => fable(true), 'useful_now'],
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
    const unknown = {
      ...fresh,
      effects: {
        ...fresh.effects,
        rows: fresh.effects.rows.map((row) =>
          row.window === '7d' ? { ...row, effect: 'unknown' as const } : row,
        ),
      },
    };
    expect(deriveCouponAdvice(unknown)?.kind).not.toBe('scope');
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

  it('never treats the Fable percentage bar as the refundable weekly portion', () => {
    expect(deriveCouponAdvice(fable(false))?.kind).toBe('unknown');
    expect(deriveCouponAdvice(fable(false))?.message).toContain('Fable');
    expect(deriveCouponAdvice(fable(true))?.kind).toBe('useful_now');
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
});
