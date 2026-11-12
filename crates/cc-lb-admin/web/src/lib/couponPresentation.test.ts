import { describe, expect, it } from 'vitest';
import { deriveCouponNudge } from './couponNudge';
import {
  type CouponQuotaWindow,
  chooseCouponGrant,
  chooseCouponGroupGrant,
  couponClaimMessage,
  couponGroupKey,
  couponInventory,
  couponOptionLabel,
  couponProductLabel,
  couponScopeLabel,
  couponUsageLabel,
  couponWindowLabel,
  groupCouponGrants,
  isFableRebateGrant,
  presentCoupon,
  presentCouponEffects,
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

  it('keeps the option label compact while scope remains available separately', () => {
    const full = grant({ clears: ['7d', '7d_overage_included'] });
    const narrow = grant({
      id: 'narrow',
      clears: ['7d_overage_included'],
      resets_total: 3,
      resets_left: 2,
    });
    expect(couponOptionLabel(full)).toBe('Reset ×1');
    expect(couponOptionLabel(narrow)).toBe('Reset ×2');
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
      manual_redemption: {
        eligible: true,
        reason: null,
        windows: [
          {
            window: '5h',
            used_percent: 80,
            resets_at_unix_secs: NOW / 1000 + 3600,
            duration_seconds: 18000,
          },
          {
            window: '7d',
            used_percent: 40,
            resets_at_unix_secs: NOW / 1000 + 86400,
            duration_seconds: 604800,
          },
        ],
      },
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

  it('uses native covered OpenAI windows without calculating their new schedule', () => {
    const coupon = couponInventory(openAIStatus())?.grants[0];
    if (!coupon) throw new Error('Missing coupon fixture');
    const effects = presentCouponEffects(coupon, [], {
      ...FRESH,
      provider: 'openai',
    });
    expect(effects.rows.map((row) => row.window)).toEqual(['5h', '7d']);
    expect(
      effects.rows.every(
        (row) => row.effect === 'reset' && row.schedule === 'updated_after_use',
      ),
    ).toBe(true);
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

describe('coupon type groups', () => {
  it('sums equivalent active grants without merging differing exact scopes', () => {
    const first = grant({
      id: 'first',
      type: 'weekly',
      clears: ['7d', '5h'],
      resets_left: 2,
    });
    const second = grant({
      id: 'second',
      type: 'weekly',
      clears: ['5h', '7d'],
      resets_left: 3,
      paused: true,
      usable_now: false,
    });
    const narrow = grant({ id: 'narrow', type: 'weekly', clears: ['7d'] });
    const groups = groupCouponGrants([first, second, narrow], 'anthropic');
    expect(groups).toHaveLength(2);
    expect(groups[0].remaining).toBe(5);
    expect(groups[0].grants.map((item) => item.id)).toEqual([
      'first',
      'second',
    ]);
    expect(groups[1].remaining).toBe(1);
  });

  it('keeps provider, raw taxonomy, unknown scope and giveback tri-state distinct', () => {
    const base = grant({ type: 'full', gives_back_weekly: true });
    const variants = [
      base,
      grant({ type: 'full', gives_back_weekly: false }),
      grant({ type: 'full', gives_back_weekly: null }),
      grant({ type: null, gives_back_weekly: true }),
      grant({ type: 'full', gives_back_weekly: true, clears: [] }),
      grant({ type: 'full', gives_back_weekly: true, clears: ['future'] }),
    ];
    expect(groupCouponGrants(variants, 'anthropic')).toHaveLength(
      variants.length,
    );
    expect(couponGroupKey(base, 'anthropic')).not.toBe(
      couponGroupKey(base, 'openai'),
    );
    expect(groupCouponGrants([base], 'anthropic')[0].fable).toBe(true);
  });

  it('keeps OpenAI reset taxonomy and plan-support states separate', () => {
    const credits = [true, false, null].map((support, index) => {
      const adapted = couponInventory(
        openAIStatus({ id: `credit-${index}`, is_supported_by_plan: support }),
      )?.grants[0];
      if (!adapted) throw new Error('Missing credit fixture');
      return adapted;
    });
    const future = couponInventory(openAIStatus({ reset_type: 'future_reset' }))
      ?.grants[0];
    if (!future) throw new Error('Missing future credit');
    expect(groupCouponGrants([...credits, future], 'openai')).toHaveLength(4);
    // Native aggregate 2 is not added to the one listed credit's type count.
    expect(groupCouponGrants([credits[0]], 'openai')[0].remaining).toBe(1);
  });

  it('chooses the earliest usable exact grant, not an earlier blocked grant', () => {
    const blocked = grant({
      id: 'blocked',
      usable_now: false,
      paused: true,
      ends_at: '2026-11-13T00:00:00Z',
    });
    const later = grant({ id: 'later', ends_at: '2026-11-20T00:00:00Z' });
    const earliest = grant({ id: 'earliest', ends_at: '2026-11-15T00:00:00Z' });
    const group = groupCouponGrants([blocked, later, earliest], 'anthropic')[0];
    expect(chooseCouponGroupGrant(group, [later, earliest])?.id).toBe(
      'earliest',
    );
    expect(group.remaining).toBe(3);
    expect(chooseCouponGroupGrant(group, [])?.id).toBe('blocked');
  });

  it('preserves a selected exact credit rather than falling back within its group', () => {
    const chosen = grant({ id: 'chosen' });
    const sibling = grant({ id: 'sibling' });
    expect(couponGroupKey(chosen, 'anthropic')).toBe(
      couponGroupKey(sibling, 'anthropic'),
    );
    expect(
      chooseCouponGrant([sibling], [sibling], [], chosen.id, FRESH),
    ).toBeNull();
  });
});

describe('expected quota effects', () => {
  const anthropic = { ...FRESH, provider: 'anthropic' as const };
  const fable = grant({
    type: 'full',
    gives_back_weekly: true,
    clears: ['7d_overage_included'],
    percent_used: { '7d_overage_included': 0 },
  });

  it('restores existing 5h and Fable weekly usage without subtracting percentage windows', () => {
    const effects = presentCouponEffects(
      fable,
      [quota('5h', 0.6), quota('7d', 0.8), quota('7d_fable', 0.5)],
      anthropic,
    );
    expect(effects.rows.map((row) => [row.window, row.effect])).toEqual([
      ['5h', 'reset'],
      ['7d', 'fable_weekly'],
      ['7d_fable', 'fable_scoped'],
    ]);
    expect(effects.rows[0].schedule).toBe('confirmed_after_use');
    expect(effects.rows[1].schedule).toBe('unchanged');
    expect(effects.rows[1].current.percentUsed).toBe(80);
    expect(
      effects.rows.some((row) => row.window === '7d_overage_included'),
    ).toBe(false);
    // Effects are qualitative, not an invented 80% - 50% = 30% prediction.
    expect(effects.rows[1]).not.toHaveProperty('afterPercent');
  });

  it('does not turn fresh zero Fable-bar usage into no-benefit advice', () => {
    const effects = presentCouponEffects(
      fable,
      [quota('7d', 1), quota('7d_fable', 0)],
      anthropic,
    );
    expect(effects.rows.find((row) => row.window === '7d')?.effect).toBe(
      'fable_weekly',
    );
    expect(effects.rows.find((row) => row.window === '7d_fable')?.effect).toBe(
      'fable_scoped',
    );
  });

  it('distinguishes absent 5h from unobserved existence and stale known existence', () => {
    expect(
      presentCouponEffects(
        fable,
        [quota('5h', null, 'absent')],
        anthropic,
      ).rows.some((row) => row.window === '5h'),
    ).toBe(false);
    expect(
      presentCouponEffects(
        fable,
        [quota('5h', null, 'unobserved')],
        anthropic,
      ).rows.find((row) => row.window === '5h')?.effect,
    ).toBe('conditional_5h');
    expect(
      presentCouponEffects(
        fable,
        [quota('5h', 0.8, 'stale')],
        anthropic,
      ).rows.find((row) => row.window === '5h'),
    ).toMatchObject({
      effect: 'reset',
      current: { state: 'stale' },
    });
  });

  it('keeps every relevant window and distinguishes undeclared Fable-bar effects', () => {
    const effects = presentCouponEffects(
      grant({ type: 'weekly', clears: ['7d'] }),
      [
        quota('5h', 0.3),
        quota('7d', 1),
        quota('7d_fable', 0.4),
        quota('7d_sonnet', 0.2),
        quota('7d_opus', 0.1),
      ],
      anthropic,
    );
    expect(effects.rows).toHaveLength(5);
    expect(effects.rows.find((row) => row.window === '5h')?.effect).toBe(
      'unchanged',
    );
    expect(effects.rows.find((row) => row.window === '7d')?.effect).toBe(
      'reset',
    );
    expect(effects.rows.find((row) => row.window === '7d_fable')?.effect).toBe(
      'unknown',
    );
  });

  it.each([
    '7d_fable',
    '7d_sonnet',
    '7d_opus',
    '7d_oauth_apps',
    '7d_cowork',
    '7d_omelette',
  ])(
    'does not infer an undeclared effect on %s, but honors an explicit reset',
    (scope) => {
      for (const coupon of [
        grant({ type: 'weekly', clears: ['7d'] }),
        grant({ type: 'full', clears: ['7d'] }),
        grant({ clears: ['5h'] }),
      ]) {
        const observed = [quota(scope, 0.4)];
        const undeclared = presentCouponEffects(coupon, observed, anthropic);
        expect(
          undeclared.rows.find((row) => row.window === scope)?.effect,
        ).toBe('unknown');
        const declared = presentCouponEffects(
          { ...coupon, clears: [...coupon.clears, scope] },
          observed,
          anthropic,
        );
        expect(declared.rows.find((row) => row.window === scope)?.effect).toBe(
          'reset',
        );
      }
    },
  );

  it('keeps overall weekly usage unchanged but leaves undeclared Fable effects unknown for a five-hour-only reset', () => {
    const effects = presentCouponEffects(
      grant({ clears: ['5h'] }),
      [quota('5h', 1), quota('7d', 0.7), quota('7d_fable', 0.2)],
      anthropic,
    );
    expect(effects.rows.map((row) => row.effect)).toEqual([
      'reset',
      'unchanged',
      'unknown',
    ]);
  });

  it('preserves actual weekly reset times and accepts the observed weekly fallback', () => {
    const observed = quota('7d', 0.8);
    const weeklyReset = '2026-11-20T00:00:00Z';
    const effects = presentCouponEffects(fable, [observed], {
      ...anthropic,
      weeklyResetsAt: weeklyReset,
    });
    expect(
      effects.rows.find((row) => row.window === '7d')?.resetsAtUnixSecs,
    ).toBe(observed.resets_at_unix_secs);
    const fallback = presentCouponEffects(fable, [], {
      ...anthropic,
      weeklyResetsAt: weeklyReset,
    });
    expect(
      fallback.rows.find((row) => row.window === '7d')?.resetsAtUnixSecs,
    ).toBe(Date.parse(weeklyReset) / 1000);
  });

  it('keeps unknown provider scope in diagnostics instead of inventing a visible alias', () => {
    const effects = presentCouponEffects(
      grant({ type: 'future', clears: ['future_window'] }),
      [quota('7d', 0.8)],
      anthropic,
    );
    expect(effects.rows).toHaveLength(1);
    expect(effects.rows[0]).toMatchObject({ window: '7d', effect: 'unknown' });
  });

  it('freezes expected effects independently of later native observations', () => {
    const status = openAIStatus();
    const coupon = couponInventory(status)?.grants[0];
    if (!coupon || !status.openai?.manual_redemption)
      throw new Error('Missing native fixture');
    const effects = presentCouponEffects(coupon, [], {
      ...FRESH,
      provider: 'openai',
    });
    status.openai.manual_redemption.windows[0].used_percent = 0;
    expect(effects.rows[0].current.percentUsed).toBe(80);
    expect(effects.rows[0].effect).toBe('reset');
  });
});

describe('manual OpenAI redemption', () => {
  it.each([0, 19.999, 20, 20.001, 100])(
    'uses strict >20%% used eligibility at %s',
    (used) => {
      const status = openAIStatus();
      if (!status.openai) throw new Error('Missing native fixture');
      status.openai.manual_redemption = {
        eligible: used > 20,
        reason: used > 20 ? null : 'below_redemption_threshold',
        windows: [
          {
            window: '7d',
            used_percent: used,
            resets_at_unix_secs: NOW / 1000 + 86400,
            duration_seconds: 604800,
          },
        ],
      };
      const coupon = couponInventory(status)?.grants[0];
      expect(coupon?.usable_now).toBe(used > 20);
      expect(coupon?.blocking).toEqual(
        used > 20 ? [] : ['manual_usage_not_needed'],
      );
    },
  );

  it('does not promote cached credit availability without a manual eligibility observation', () => {
    const status = openAIStatus();
    if (!status.openai) throw new Error('Missing native fixture');
    delete status.openai.manual_redemption;
    const coupon = couponInventory(status)?.grants[0];
    expect(coupon?.resets_left).toBe(1);
    expect(coupon?.usable_now).toBe(false);
    expect(coupon?.blocking).toEqual(['manual_usage_unknown']);
  });

  it('allows a qualifying covered allowance despite an unknown sibling and preserves backend denials', () => {
    const status = openAIStatus();
    if (!status.openai?.manual_redemption)
      throw new Error('Missing native fixture');
    status.openai.manual_redemption.windows[1].used_percent = null;
    expect(couponInventory(status)?.grants[0].usable_now).toBe(true);
    status.openai.manual_redemption.windows[1].used_percent = 60;
    status.openai.manual_redemption.eligible = false;
    expect(couponInventory(status)?.grants[0].usable_now).toBe(false);
  });

  it('preserves a zero-usage covered allowance and its actual reset time', () => {
    const status = openAIStatus();
    if (!status.openai?.manual_redemption)
      throw new Error('Missing native fixture');
    status.openai.manual_redemption.windows[1].used_percent = 0;
    const coupon = couponInventory(status)?.grants[0];
    if (!coupon) throw new Error('Missing native credit');
    const effects = presentCouponEffects(coupon, [quota('7d_fable', 1)], {
      ...FRESH,
      provider: 'openai',
    });
    expect(effects.rows.map((row) => row.window)).toEqual(['5h', '7d']);
    expect(effects.rows[0]).toMatchObject({
      effect: 'reset',
      schedule: 'updated_after_use',
    });
    expect(effects.rows[1]).toMatchObject({
      effect: 'unchanged',
      schedule: 'unchanged',
      resetsAtUnixSecs:
        status.openai.manual_redemption.windows[1].resets_at_unix_secs,
    });
  });

  it('uses the actual monthly window without manufacturing a weekly allowance', () => {
    const status = openAIStatus();
    if (!status.openai) throw new Error('Missing native fixture');
    status.openai.manual_redemption = {
      eligible: false,
      reason: 'below_redemption_threshold',
      windows: [
        {
          window: 'monthly',
          used_percent: 20,
          resets_at_unix_secs: NOW / 1000 + 2592000,
          duration_seconds: 2592000,
        },
      ],
    };
    const coupon = couponInventory(status)?.grants[0];
    if (!coupon) throw new Error('Missing native credit');
    expect(coupon.usable_now).toBe(false);
    expect(
      presentCouponEffects(coupon, [], {
        ...FRESH,
        provider: 'openai',
      }).rows.map((row) => row.window),
    ).toEqual(['monthly']);
  });

  it('does not assign known full-reset scope to an unsupported reset type', () => {
    const coupon = couponInventory(openAIStatus({ reset_type: 'future_reset' }))
      ?.grants[0];
    if (!coupon) throw new Error('Missing native credit');
    expect(coupon.usable_now).toBe(false);
    expect(coupon.blocking).toEqual(['unsupported_reset_type']);
    expect(
      presentCouponEffects(coupon, [], { ...FRESH, provider: 'openai' }).rows,
    ).toEqual([]);
  });

  it('preserves provider plan support while failing closed on unknown local coverage', () => {
    const status = openAIStatus();
    if (!status.openai) throw new Error('Missing native fixture');
    status.openai.manual_redemption = {
      eligible: false,
      reason: 'coverage_unreported',
      windows: [],
    };
    const coupon = couponInventory(status)?.grants[0];
    expect(coupon?.openai_credit?.is_supported_by_plan).toBe(true);
    expect(coupon?.usable_now).toBe(false);
    expect(coupon?.blocking).toEqual(['coverage_unreported']);
  });
});
