import { describe, expect, it } from 'vitest';
import { deriveCouponNudge } from './couponNudge';
import {
  type CouponQuotaWindow,
  chooseCouponGrant,
  couponClaimMessage,
  couponEffectDescription,
  couponInventory,
  couponOptionLabel,
  couponProductLabel,
  couponScopeLabel,
  couponUsageLabel,
  couponWindowLabel,
  isFableRebateGrant,
  presentCoupon,
} from './couponPresentation';
import type {
  CedarEmberLimitResets,
  LimitResetClaimResponse,
  LimitResetGrant,
  LimitResetsResponse,
  OpenAILimitResetCredit,
} from './limitResets';

const NOW = Date.parse('2026-11-12T12:00:00Z');
const FRESH = { providerFresh: true };

function grant(overrides: Partial<LimitResetGrant> = {}): LimitResetGrant {
  return {
    id: 'weekly',
    label: 'Limit reset',
    type: null,
    gives_back_weekly: null,
    unavailable_reason: null,
    resets_total: 2,
    resets_left: 1,
    starts_at: null,
    ends_at: '2026-12-01T00:00:00Z',
    clears: ['7d'],
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
  utilization: number | null,
  state: CouponQuotaWindow['state'] = 'fresh',
): CouponQuotaWindow {
  return {
    window,
    utilization,
    state,
    resets_at_unix_secs: NOW / 1000 + 14_400,
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

describe('exact coupon scope and usage', () => {
  it('shows a full weekly target even when the provider account gate is false', () => {
    const coupon = grant({ percent_used: { '7d': 100 } });
    const status = ember([coupon]);
    const presentation = presentCoupon(coupon, [quota('7d', 1)], FRESH);
    expect(presentation.hasFullTarget).toBe(true);
    expect(couponUsageLabel(presentation.targets[0])).toBe(
      '100% used · coupon snapshot',
    );
    expect(status.at_limit).toBe(false);
    expect(deriveCouponNudge(status, [quota('7d', 1)], NOW).kind).toBe('limit');
  });

  it('keeps each mixed target reading instead of deriving account-wide advice', () => {
    const presentation = presentCoupon(
      grant({
        clears: ['7d', '7d_overage_included'],
        percent_used: { '7d': 100, '7d_overage_included': 0 },
      }),
      [],
      FRESH,
    );
    expect(presentation.targets.map((reading) => reading.percentUsed)).toEqual([
      100, 0,
    ]);
    expect(presentation.hasFullTarget).toBe(true);
    expect(presentation.fullNonTargets).toEqual([]);
  });

  it('lists full windows outside declared clears without predicting indirect effects', () => {
    const presentation = presentCoupon(
      grant({
        label: 'Fable limit reset',
        clears: ['7d_overage_included'],
        percent_used: { '7d_overage_included': 0 },
      }),
      [quota('7d', 1), quota('7d_fable', 0)],
      FRESH,
    );
    expect(presentation.hasFullTarget).toBe(false);
    expect(presentation.targets.map((reading) => reading.window)).toEqual([
      '7d_overage_included',
    ]);
    expect(presentation.fullNonTargets.map((reading) => reading.label)).toEqual(
      ['Weekly'],
    );
    expect(couponUsageLabel(presentation.targets[0])).toBe(
      '0% used · coupon snapshot',
    );
  });

  it('never aliases an unconfirmed key to the Fable quota window', () => {
    const presentation = presentCoupon(
      grant({ clears: ['7d_overage_included'] }),
      [quota('7d_fable', 1)],
      FRESH,
    );
    expect(couponWindowLabel('7d_overage_included')).toBe(
      'Weekly (included usage)',
    );
    expect(presentation.targets[0].state).toBe('unknown');
    expect(presentation.hasFullTarget).toBe(false);
  });

  it('falls back only to a matching fresh quota snapshot', () => {
    const presentation = presentCoupon(grant(), [quota('7d', 0.42)], FRESH);
    expect(presentation.targets[0]).toMatchObject({
      percentUsed: 42,
      state: 'fresh',
      source: 'quota',
    });
    expect(couponUsageLabel(presentation.targets[0])).toBe(
      '42% used · quota snapshot',
    );
  });

  it('does not overwrite a valid provider zero with a different quota reading', () => {
    const presentation = presentCoupon(
      grant({ percent_used: { '7d': 0 } }),
      [quota('7d', 1)],
      FRESH,
    );
    expect(presentation.targets[0].percentUsed).toBe(0);
    expect(presentation.hasFullTarget).toBe(false);
  });

  it('marks stale quota explicitly and never treats it as a full current target', () => {
    const presentation = presentCoupon(
      grant(),
      [quota('7d', 1, 'stale')],
      FRESH,
    );
    expect(couponUsageLabel(presentation.targets[0])).toBe(
      '100% used · stale quota snapshot',
    );
    expect(presentation.hasFullTarget).toBe(false);
    expect(presentation.fullNonTargets).toEqual([]);
  });

  it('marks cached provider usage stale after a failed status refresh', () => {
    const presentation = presentCoupon(
      grant({ percent_used: { '7d': 100 } }),
      [],
      { providerFresh: false },
    );
    expect(couponUsageLabel(presentation.targets[0])).toBe(
      '100% used · stale coupon snapshot',
    );
    expect(presentation.hasFullTarget).toBe(false);
  });

  it.each(['absent', 'unobserved'] as const)(
    'never shows %s quota as zero or a current usage reading',
    (state) => {
      const presentation = presentCoupon(
        grant(),
        [quota('7d', 0, state)],
        FRESH,
      );
      expect(couponUsageLabel(presentation.targets[0])).toBe('Usage unknown');
      expect(presentation.targets[0].percentUsed).toBeNull();
    },
  );

  it('keeps missing and non-finite readings unknown', () => {
    const presentation = presentCoupon(
      grant({ percent_used: { '7d': Number.NaN } }),
      [quota('7d', Number.POSITIVE_INFINITY)],
      FRESH,
    );
    expect(couponUsageLabel(presentation.targets[0])).toBe('Usage unknown');
    expect(presentation.targets[0].percentUsed).toBeNull();
  });

  it('distinguishes repeated provider labels by declared scope and remaining count', () => {
    const full = grant({ clears: ['7d', '7d_overage_included'] });
    const narrow = grant({
      id: 'narrow',
      clears: ['7d_overage_included'],
      resets_total: 3,
      resets_left: 2,
    });
    expect(couponOptionLabel(full)).toBe(
      'Reset coupon · Weekly, Weekly (included usage) · 1/2 left',
    );
    expect(couponOptionLabel(narrow)).toBe(
      'Reset coupon · Weekly (included usage) · 2/3 left',
    );
    expect(couponScopeLabel(full)).not.toContain('All');
    expect(couponScopeLabel(grant({ clears: [] }))).toBe('Scope not reported');
  });

  it('does not infer eligibility from a full usage reading or a missing gate', () => {
    const coupon = grant({
      usable_now: false,
      percent_used: { '7d': 100 },
      blocking: ['surface_restricted'],
    });
    const nudge = deriveCouponNudge(
      ember([coupon], { at_limit: null }),
      [quota('7d', 1)],
      NOW,
    );
    expect(nudge.usableCount).toBe(0);
    expect(nudge.kind).toBe('quiet');
    expect(coupon.usable_now).toBe(false);
  });

  it('captures usage values independently of subsequent provider data changes', () => {
    const coupon = grant({ percent_used: { '7d': 100 } });
    const frozen = presentCoupon(coupon, [], FRESH);
    coupon.percent_used['7d'] = 0;
    expect(frozen.targets[0].percentUsed).toBe(100);
    expect(presentCoupon(coupon, [], FRESH).targets[0].percentUsed).toBe(0);
  });
});

describe('coupon selection', () => {
  const early = grant({
    id: 'early',
    clears: ['7d_overage_included'],
    percent_used: { '7d_overage_included': 0 },
    ends_at: '2026-11-13T00:00:00Z',
  });
  const full = grant({ id: 'full', percent_used: { '7d': 100 } });

  it('defaults to a usable coupon clearing an exhausted target before expiry', () => {
    expect(
      chooseCouponGrant([early, full], [early, full], [], null, FRESH)?.id,
    ).toBe('full');
  });

  it('uses earliest expiry when no usable target is full', () => {
    const unknown = grant({ id: 'unknown', percent_used: {} });
    expect(
      chooseCouponGrant([unknown, early], [unknown, early], [], null, FRESH)
        ?.id,
    ).toBe('early');
  });

  it('does not prioritize an exhausted coupon that the provider blocks', () => {
    expect(chooseCouponGrant([full, early], [early], [], null, FRESH)?.id).toBe(
      'early',
    );
  });

  it('preserves an explicit low-usage choice despite a full alternative', () => {
    expect(
      chooseCouponGrant([early, full], [early, full], [], early.id, FRESH),
    ).toBe(early);
  });

  it('preserves an explicit choice when it becomes blocked', () => {
    expect(chooseCouponGrant([early, full], [full], [], early.id, FRESH)).toBe(
      early,
    );
  });

  it('returns no submission target when an explicit coupon disappears', () => {
    expect(chooseCouponGrant([full], [full], [], early.id, FRESH)).toBeNull();
  });
});

function openAIStatus(
  creditOverrides: Partial<OpenAILimitResetCredit> = {},
): LimitResetsResponse {
  return {
    provider: 'openai',
    account_id: 'account',
    organization_id: null,
    cedar_ember: null,
    openai: {
      available_count: 2,
      applicable_available_count: null,
      credits: [
        {
          id: 'opaque-credit',
          reset_type: 'codex_rate_limits',
          status: 'available',
          is_supported_by_plan: true,
          granted_at: '2026-10-31T20:20:26Z',
          expires_at: '2026-11-30T20:20:26Z',
          title: 'Full reset',
          description: 'One free rate limit reset.',
          ...creditOverrides,
        },
      ],
    },
  };
}

describe('provider-specific reset semantics', () => {
  it('presents the observed Fable giveback without aliasing its scope or projecting a rebate', () => {
    const live = grant({
      id: 'opaque-fable',
      label:
        'New model launch: one Fable reset for Max, Team Premium and Enterprise Premium',
      type: 'full',
      gives_back_weekly: true,
      clears: ['7d_overage_included'],
      percent_used: { '7d_overage_included': 0, '7d': 100 },
    });
    expect(isFableRebateGrant(live)).toBe(true);
    expect(couponProductLabel(live)).toBe('Fable usage reset');
    expect(couponScopeLabel(live)).toBe('Weekly (included usage)');
    const observed = presentCoupon(live, [quota('7d_fable', 0)], FRESH);
    expect(observed.targets[0].window).toBe('7d_overage_included');
    expect(observed.fullNonTargets[0].window).toBe('7d');
    const copy = couponEffectDescription(live, 'anthropic');
    expect(copy).toContain('return');
    expect(copy).toContain('numeric weekly rebate');
    expect(copy).not.toContain('untouched');
    expect(copy).not.toContain('unaffected');
    expect(copy).not.toContain('100%');
  });

  it('does not infer a full-account reset from unknown taxonomy or unknown scopes', () => {
    const unknown = grant({ type: 'future', clears: ['future_window'] });
    expect(couponProductLabel(unknown)).toBe('Reset coupon');
    expect(couponScopeLabel(unknown)).toBe('Provider-defined scope');
    expect(
      presentCoupon(unknown, [quota('7d', 1)], FRESH).targets[0].state,
    ).toBe('unknown');
    expect(couponOptionLabel(unknown)).not.toContain('All');
    expect(couponProductLabel(grant({ type: 'weekly' }))).toBe('Weekly reset');
    expect(isFableRebateGrant(grant({ type: 'full' }))).toBe(false);
  });

  it.each([
    [true, true],
    [false, false],
    [null, false],
  ])(
    'keeps reported plan support %s distinct from availability',
    (support, usable) => {
      const data = openAIStatus({ is_supported_by_plan: support });
      const inventory = couponInventory(data);
      expect(inventory?.grants[0].usable_now).toBe(usable);
      expect(inventory?.grants[0].openai_credit?.is_supported_by_plan).toBe(
        support,
      );
      expect(inventory?.grants[0].blocking).toEqual(
        support === false
          ? ['unsupported_plan']
          : support == null
            ? ['plan_support_unreported']
            : [],
      );
      expect(inventory?.grants[0].id).toBe('opaque-credit');
      expect(inventory?.grants[0].clears).toEqual([]);
      expect(data.openai?.available_count).toBe(2);
      expect(data.openai?.applicable_available_count).toBeNull();
    },
  );

  it.each(['redeemed', 'expired', 'future_status'])(
    'never offers OpenAI status %s for consumption',
    (status) => {
      const inventory = couponInventory(openAIStatus({ status }));
      expect(inventory?.grants[0].usable_now).toBe(false);
      expect(
        deriveCouponNudge(inventory ?? undefined, [], NOW).usableCount,
      ).toBe(0);
    },
  );

  it('keeps native inventory counts and history independent of the selectable subset', () => {
    const data = openAIStatus({ status: 'redeemed' });
    const inventory = couponInventory(data);
    expect(inventory?.grants).toHaveLength(1);
    expect(deriveCouponNudge(inventory ?? undefined, [], NOW).activeCount).toBe(
      0,
    );
    expect(data.openai?.available_count).toBe(2);
  });

  it('warns about OpenAI schedule changes without inventing plan-specific windows', () => {
    const coupon = couponInventory(openAIStatus())?.grants[0];
    expect(coupon).toBeDefined();
    if (!coupon) throw new Error('Missing coupon fixture');
    const copy = couponEffectDescription(coupon, 'openai');
    expect(copy).toContain('main Codex allowance');
    expect(copy).toContain('may change the reset schedule');
    expect(copy).not.toContain('5-hour');
    expect(copy).not.toContain('weekly reset schedule stays');
  });

  it('freezes the confirmed native credit and usage independently of later poll mutations', () => {
    const data = openAIStatus();
    const live = couponInventory(data)?.grants[0];
    if (!live || !data.openai) throw new Error('Missing coupon fixture');
    const confirmed = structuredClone(live);
    data.openai.credits[0].status = 'redeemed';
    data.openai.credits[0].title = 'Changed';
    expect(confirmed.openai_credit?.status).toBe('available');
    expect(confirmed.label).toBe('Full reset');
  });
});

describe('reset completion and quota refresh copy', () => {
  const response: LimitResetClaimResponse = {
    result: 'reset',
    reason: null,
    cleared: [],
    resets_left: null,
    weekly_resets_at: null,
    cooldown_until: null,
    windows_reset: 1,
  };

  it('reports a reloaded provider sample without promising all routing is ready', () => {
    const copy = couponClaimMessage({
      ...response,
      usage_refresh: 'refreshed',
    });
    expect(copy).toContain('Usage reloaded from the provider');
    expect(copy).not.toContain('routing');
  });

  it('preserves successful reset when quota refresh fails without urging another coupon', () => {
    const copy = couponClaimMessage({ ...response, usage_refresh: 'failed' });
    expect(copy).toContain('Reset completed');
    expect(copy).toContain('quota refresh failed');
    expect(copy).not.toContain('another coupon');
  });

  it('treats AlreadyRedeemed as success and NothingToReset as no consumption', () => {
    expect(
      couponClaimMessage({
        ...response,
        result: 'already_used',
        usage_refresh: 'refreshed',
      }),
    ).toContain('already applied');
    const data = openAIStatus();
    expect(
      couponClaimMessage({ ...response, result: 'not_limited' }),
    ).toContain('No coupon was used');
    expect(data.openai?.available_count).toBe(2);
  });
});
