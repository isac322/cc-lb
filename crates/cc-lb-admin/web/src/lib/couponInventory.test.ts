import { describe, expect, it } from 'vitest';
import {
  aggregateCouponInventory,
  type CouponInventoryResponse,
  type CouponInventoryUpstream,
} from './couponInventory';
import type { LimitResetGrant, OpenAILimitResetCredit } from './limitResets';

const NOW = Date.parse('2026-11-12T12:00:00Z');

function grant(overrides: Partial<LimitResetGrant> = {}): LimitResetGrant {
  return {
    id: 'grant-1',
    label: 'Provider weekly reset',
    type: 'weekly',
    gives_back_weekly: false,
    unavailable_reason: null,
    resets_total: 4,
    resets_left: 3,
    starts_at: null,
    ends_at: null,
    clears: ['7d', '7d_overage_included'],
    paused: false,
    usable_now: true,
    use_requires_limit: true,
    percent_used: {},
    blocking: [],
    ...overrides,
  };
}

function upstream(
  id: string,
  grants: LimitResetGrant[] = [grant()],
  overrides: Partial<CouponInventoryUpstream> = {},
): CouponInventoryUpstream {
  return {
    upstream_id: id,
    provider: 'anthropic',
    account_id: `account-${id}`,
    organization_id: 'organization-1',
    openai: null,
    cedar_ember: {
      eligible: true,
      ineligible_reason: null,
      at_limit: null,
      grants,
      next_grant_id: null,
      weekly_resets_at: null,
      cooldown_until: null,
    },
    ...overrides,
  };
}

function credit(
  id: string,
  overrides: Partial<OpenAILimitResetCredit> = {},
): OpenAILimitResetCredit {
  return {
    id,
    reset_type: 'codex_rate_limits',
    status: 'available',
    is_supported_by_plan: true,
    granted_at: null,
    expires_at: null,
    title: 'Full reset',
    description: null,
    ...overrides,
  };
}

function openAIUpstream(
  id: string,
  availableCount: number,
  credits: OpenAILimitResetCredit[],
  overrides: Partial<CouponInventoryUpstream> = {},
): CouponInventoryUpstream {
  return {
    upstream_id: id,
    provider: 'openai',
    account_id: `account-${id}`,
    organization_id: null,
    cedar_ember: null,
    openai: { available_count: availableCount, credits },
    ...overrides,
  };
}

function inventory(
  upstreams: CouponInventoryUpstream[],
  allowedUpstreams: readonly string[] = [],
) {
  return aggregateCouponInventory({
    fleet: { upstreams },
    allowedUpstreams,
    nowMs: NOW,
  });
}

describe('aggregateCouponInventory', () => {
  it('sums remaining resets, not grant count; an empty allowlist allows all', () => {
    const result = inventory([
      upstream('first'),
      upstream('second', [grant({ resets_left: 2 })]),
    ]);
    expect(result.total).toBe(5);
    expect(result.complete).toBe(true);
    expect(result.groups).toHaveLength(1);
    expect(result.groups[0]?.resets).toBe(5);
  });

  it('intersects a nonempty principal allowlist with returned eligible upstreams', () => {
    const result = inventory(
      [upstream('first'), upstream('second')],
      ['second', 'not-enabled-or-credentialed'],
    );
    expect(result.total).toBe(3);
    expect(result.upstreamIds).toEqual(['second']);
    expect(inventory([upstream('first')], ['absent']).total).toBe(0);
  });

  it('deduplicates exact provider/account/organization/grant identities', () => {
    const first = upstream('first');
    const duplicate = upstream('duplicate', [grant()], {
      account_id: first.account_id,
    });
    const otherOrganization = upstream('other-org', [grant()], {
      account_id: first.account_id,
      organization_id: 'organization-2',
    });
    const otherGrant = upstream('other-grant', [grant({ id: 'grant-2' })], {
      account_id: first.account_id,
      organization_id: 'organization-2',
    });
    expect(inventory([first, duplicate, otherOrganization]).total).toBe(6);
    // Separate observations of one account must agree on its inventory.
    const accountWithBoth = upstream(
      'other-org',
      [grant(), grant({ id: 'grant-2' })],
      {
        account_id: first.account_id,
        organization_id: 'organization-2',
      },
    );
    const duplicateWithBoth = {
      ...otherGrant,
      cedar_ember: accountWithBoth.cedar_ember,
    };
    expect(
      inventory([first, duplicate, accountWithBoth, duplicateWithBoth]).total,
    ).toBe(9);
  });

  it('chooses the lower duplicate count without claiming complete freshness', () => {
    const first = upstream('first');
    const duplicate = upstream('duplicate', [grant({ resets_left: 1 })], {
      account_id: first.account_id,
    });
    for (const observations of [
      [first, duplicate],
      [duplicate, first],
    ]) {
      const result = inventory(observations);
      expect(result.total).toBe(1);
      expect(result.complete).toBe(false);
      expect(result.groups[0]?.upstreamIds).toEqual(['duplicate', 'first']);
    }
  });

  it('does not revive a grant missing from another snapshot of the same account', () => {
    const first = upstream('first');
    const duplicate = upstream('duplicate', [], {
      account_id: first.account_id,
    });
    const result = inventory([first, duplicate]);
    expect(result.total).toBeNull();
    expect(result.complete).toBe(false);
  });

  it('retains active inventory while paused, blocked, ineligible, or on cooldown', () => {
    const paused = upstream('paused', [
      grant({
        paused: true,
        usable_now: false,
        blocking: ['cooldown', 'not_limited'],
      }),
    ]);
    if (paused.cedar_ember) {
      paused.cedar_ember.eligible = false;
      paused.cedar_ember.cooldown_until = '2026-11-13T12:00:00Z';
    }
    expect(inventory([paused]).total).toBe(3);
  });

  it('honors start/end boundaries, expired blocking, and depletion', () => {
    const result = inventory([
      upstream('first', [
        grant({
          id: 'start-now',
          starts_at: new Date(NOW).toISOString(),
          resets_left: 2,
        }),
        grant({ id: 'end-now', ends_at: new Date(NOW).toISOString() }),
        grant({ id: 'future', starts_at: new Date(NOW + 1).toISOString() }),
        grant({ id: 'provider-expired', blocking: ['expired'] }),
        grant({ id: 'depleted', resets_left: 0 }),
        grant({ id: 'past', ends_at: new Date(NOW - 1).toISOString() }),
      ]),
    ]);
    expect(result.total).toBe(2);
    expect(result.complete).toBe(true);
  });

  it('treats null snapshots and identities as unknown, never known empty', () => {
    for (const unknown of [
      upstream('stale', [], { cedar_ember: null }),
      upstream('no-account', [], { account_id: null }),
      upstream('no-org', [], { organization_id: null }),
    ]) {
      const result = inventory([unknown]);
      expect(result.total).toBeNull();
      expect(result.complete).toBe(false);
      expect(result.unknownUpstreamIds).toEqual([unknown.upstream_id]);
    }
    expect(inventory([upstream('known-empty', [])]).total).toBe(0);
  });

  it('marks partial known counts incomplete and suppresses stale duplicate accounts', () => {
    const known = upstream('known');
    expect(
      inventory([known, upstream('stale', [], { cedar_ember: null })]),
    ).toMatchObject({
      total: 3,
      complete: false,
    });
    expect(
      inventory([
        known,
        upstream('stale', [], {
          account_id: known.account_id,
          cedar_ember: null,
        }),
      ]).total,
    ).toBeNull();
  });

  it('does not infer a Fable scope or combine grants with distinct provider scopes', () => {
    const result = inventory([
      upstream('first', [
        grant({
          id: 'overage',
          label: 'Fable provider title',
          clears: ['7d_overage_included'],
        }),
        grant({
          id: 'fable',
          label: 'Fable provider title',
          clears: ['7d_fable'],
        }),
      ]),
    ]);
    expect(result.groups).toHaveLength(2);
    expect(result.groups.map((group) => group.grant?.clears)).toContainEqual([
      '7d_overage_included',
    ]);
    expect(result.total).toBe(6);
  });

  it('rejects conflicting scopes and invalid counts rather than inventing totals', () => {
    const first = upstream('first');
    const conflicting = upstream(
      'duplicate',
      [grant({ clears: ['7d_fable'] })],
      {
        account_id: first.account_id,
      },
    );
    expect(inventory([first, conflicting]).total).toBeNull();
    expect(
      inventory([upstream('bad', [grant({ resets_left: Number.NaN })])]).total,
    ).toBeNull();
  });

  it('does not count Anthropic inventory under an excluded provider scope', () => {
    const fleet: CouponInventoryResponse = { upstreams: [upstream('first')] };
    expect(
      aggregateCouponInventory({
        fleet,
        allowedUpstreams: [],
        providers: ['openai'],
        nowMs: NOW,
      }),
    ).toMatchObject({ total: 0, supported: true });
    expect(
      aggregateCouponInventory({
        fleet,
        allowedUpstreams: [],
        providers: [],
        nowMs: NOW,
      }).total,
    ).toBeNull();
  });

  it('keeps loading, missing principal config, and fetch failures distinct from zero', () => {
    expect(
      aggregateCouponInventory({
        fleet: undefined,
        allowedUpstreams: [],
        nowMs: NOW,
      }).total,
    ).toBeNull();
    expect(
      aggregateCouponInventory({
        fleet: { upstreams: [upstream('first')] },
        allowedUpstreams: undefined,
        nowMs: NOW,
      }).total,
    ).toBeNull();
    expect(
      aggregateCouponInventory({
        fleet: { upstreams: [] },
        allowedUpstreams: [],
        nowMs: NOW,
        incomplete: true,
      }).total,
    ).toBeNull();
    expect(
      aggregateCouponInventory({
        fleet: { upstreams: [upstream('first')] },
        allowedUpstreams: [],
        nowMs: NOW,
        incomplete: true,
      }),
    ).toMatchObject({ total: 3, complete: false });
  });

  it('groups Anthropic product type and weekly rebate separately from exact scopes', () => {
    const result = inventory([
      upstream('types', [
        grant({ id: 'weekly', clears: ['7d_overage_included'] }),
        grant({
          id: 'fable',
          type: 'full',
          gives_back_weekly: true,
          clears: ['7d_overage_included'],
        }),
        grant({
          id: 'unknown',
          type: null,
          gives_back_weekly: null,
          clears: ['7d_overage_included'],
        }),
      ]),
    ]);
    expect(result.total).toBe(9);
    expect(result.groups).toHaveLength(3);
    expect(result.groups.filter((group) => group.fable)).toMatchObject([
      { label: 'Fable usage reset', resets: 3 },
    ]);
    expect(
      result.groups.every(
        (group) => group.grant?.clears[0] === '7d_overage_included',
      ),
    ).toBe(true);
  });

  it('rejects duplicate Anthropic product disagreements even with identical scope', () => {
    const first = upstream('first');
    const duplicate = upstream(
      'duplicate',
      [grant({ type: 'full', gives_back_weekly: true })],
      { account_id: first.account_id },
    );
    expect(inventory([first, duplicate])).toMatchObject({
      total: null,
      complete: false,
    });
  });

  it('includes native OpenAI inventory with nullable organization and both providers', () => {
    const openai = openAIUpstream('openai', 2, [credit('one'), credit('two')]);
    const result = inventory([upstream('anthropic'), openai]);
    expect(result).toMatchObject({ total: 5, complete: true, supported: true });
    expect(result.groups.map((group) => group.provider).sort()).toEqual([
      'anthropic',
      'openai',
    ]);
    expect(inventory([openai], ['other']).total).toBe(0);
    expect(
      aggregateCouponInventory({
        fleet: { upstreams: [upstream('anthropic'), openai] },
        allowedUpstreams: [],
        providers: ['openai'],
        nowMs: NOW,
      }).total,
    ).toBe(2);
  });

  it('uses native available_count when coupon details are partial or absent', () => {
    const result = inventory([openAIUpstream('partial', 5, [credit('known')])]);
    expect(result).toMatchObject({ total: 5, complete: true });
    expect(result.groups).toMatchObject([
      { label: 'Codex allowance reset', resets: 1, unknownType: false },
      { label: 'Type not reported', resets: 4, unknownType: true },
    ]);
    const summaryOnly = inventory([openAIUpstream('summary', 3, [])]);
    expect(summaryOnly.total).toBe(3);
    expect(summaryOnly.groups).toMatchObject([
      { resets: 3, unknownType: true },
    ]);
    expect(
      inventory([
        openAIUpstream('partial-expired', 4, [
          credit('expired', { expires_at: new Date(NOW).toISOString() }),
          credit('used', { status: 'redeemed' }),
        ]),
      ]),
    ).toMatchObject({
      total: 4,
      complete: true,
      groups: [{ resets: 4, unknownType: true }],
    });
  });

  it('excludes redeemed, expired and future credits when available details reconcile', () => {
    const result = inventory([
      openAIUpstream('complete', 4, [
        credit('active'),
        credit('redeemed', { status: 'redeemed' }),
        credit('expired', { status: 'expired' }),
        credit('ended', { expires_at: new Date(NOW).toISOString() }),
        credit('future', { granted_at: new Date(NOW + 1).toISOString() }),
        credit('past', { expires_at: new Date(NOW - 1).toISOString() }),
      ]),
    ]);
    expect(result).toMatchObject({ total: 1, complete: true });
    expect(result.groups).toMatchObject([{ resets: 1 }]);
    expect(
      inventory([
        openAIUpstream('redeemed-only', 0, [
          credit('used', { status: 'redeemed' }),
        ]),
      ]),
    ).toMatchObject({ total: 0, complete: true, groups: [] });
  });

  it('keeps unsupported plans in active inventory without claiming usability', () => {
    const result = inventory([
      openAIUpstream('plan', 3, [
        credit('supported'),
        credit('unsupported', { is_supported_by_plan: false }),
        credit('unreported', { is_supported_by_plan: null }),
      ]),
    ]);
    expect(result.total).toBe(3);
    expect(
      result.groups.filter((group) => group.planUnsupported),
    ).toMatchObject([{ resets: 1, label: 'Codex allowance reset' }]);
    expect(
      result.groups.filter((group) => group.applicabilityUnknown),
    ).toHaveLength(1);
  });

  it('deduplicates native account counts and conservatively reconciles duplicate details', () => {
    const first = openAIUpstream('first', 3, [
      credit('common'),
      credit('first-only'),
    ]);
    const duplicate = openAIUpstream(
      'duplicate',
      2,
      [credit('common'), credit('second-only')],
      { account_id: first.account_id },
    );
    for (const observations of [
      [first, duplicate],
      [duplicate, first],
    ]) {
      const result = inventory(observations);
      expect(result).toMatchObject({ total: 2, complete: false });
      expect(result.groups).toMatchObject([
        { resets: 1, unknownType: false, upstreamIds: ['duplicate', 'first'] },
        { resets: 1, unknownType: true },
      ]);
    }
    const matching = openAIUpstream(
      'same',
      3,
      [credit('common'), credit('first-only')],
      { account_id: first.account_id },
    );
    expect(inventory([first, matching])).toMatchObject({
      total: 3,
      complete: true,
    });
  });

  it('does not fabricate zero for disagreeing, missing or invalid native snapshots', () => {
    const first = openAIUpstream('first', 2, [credit('one'), credit('two')]);
    const emptied = openAIUpstream('empty', 0, [], {
      account_id: first.account_id,
    });
    expect(inventory([first, emptied])).toMatchObject({
      total: null,
      complete: false,
    });
    for (const unknown of [
      openAIUpstream('missing', 0, [], { openai: null }),
      openAIUpstream('identity', 0, [], { account_id: null }),
      openAIUpstream('invalid', -1, []),
      openAIUpstream('fractional', 0.5, []),
      openAIUpstream('nonfinite', Number.NaN, []),
    ]) {
      expect(inventory([unknown])).toMatchObject({
        total: null,
        complete: false,
      });
    }
    expect(
      inventory([
        first,
        openAIUpstream('stale', 0, [], {
          account_id: first.account_id,
          openai: null,
        }),
      ]).total,
    ).toBeNull();
  });

  it('keeps unrecognized reset types generic and does not infer quota scopes', () => {
    const result = inventory([
      openAIUpstream('unknown-type', 1, [
        credit('unknown', { reset_type: 'future_provider_internal_type' }),
      ]),
    ]);
    expect(result.groups).toMatchObject([
      { label: 'Reset coupon', scope: null, fable: false, resets: 1 },
    ]);
  });

  it('does not assign conflicting native details or duplicate IDs to a product type', () => {
    const first = openAIUpstream('first', 1, [credit('same')]);
    const conflicting = openAIUpstream(
      'second',
      1,
      [credit('same', { reset_type: 'future_type' })],
      { account_id: first.account_id },
    );
    for (const observations of [
      [first, conflicting],
      [conflicting, first],
    ]) {
      expect(inventory(observations)).toMatchObject({
        total: 1,
        complete: false,
        groups: [{ unknownType: true, resets: 1 }],
      });
    }
    expect(
      inventory([
        openAIUpstream('duplicate-id', 1, [credit('same'), credit('same')]),
      ]),
    ).toMatchObject({
      total: 1,
      complete: false,
      groups: [{ unknownType: true }],
    });
  });

  it('retains summary truth when native expiry details are malformed', () => {
    expect(
      inventory([
        openAIUpstream('bad-date', 1, [
          credit('unknown-expiry', { expires_at: 'not-a-date' }),
        ]),
      ]),
    ).toMatchObject({
      total: 1,
      complete: false,
      groups: [{ resets: 1, unknownType: true }],
    });
  });

  it('deduplicates OpenAI by provider and account independently of optional organization', () => {
    const first = openAIUpstream('first', 1, [credit('same')]);
    const duplicate = openAIUpstream('duplicate', 1, [credit('same')], {
      account_id: first.account_id,
      organization_id: 'optional-organization',
    });
    const anthropic = upstream('anthropic', [grant()], {
      account_id: first.account_id,
    });
    expect(inventory([first, duplicate, anthropic])).toMatchObject({
      total: 4,
      complete: true,
    });
  });

  it('counts banked inventory when the native applicable summary is zero', () => {
    const unsupported = credit('unsupported', { is_supported_by_plan: false });
    const account = openAIUpstream('banked', 1, [unsupported], {
      openai: {
        available_count: 1,
        applicable_available_count: 0,
        credits: [unsupported],
      },
    });
    expect(inventory([account])).toMatchObject({
      total: 1,
      complete: true,
      groups: [{ resets: 1, planUnsupported: true }],
    });
  });
});
