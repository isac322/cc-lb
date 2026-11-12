import { describe, expect, it } from 'vitest';
import {
  type CouponInventoryResponse,
  type CouponInventoryUpstream,
  couponInventoryForUpstream,
} from './couponInventory';
import type { LimitResetGrant, OpenAILimitResetCredit } from './limitResets';
import type { Upstream } from './queries';

const NOW = Date.parse('2026-11-12T12:00:00Z');

function grant(overrides: Partial<LimitResetGrant> = {}): LimitResetGrant {
  return {
    id: 'grant-1',
    label: 'Weekly reset',
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

function observation(id: string, grants = [grant()]): CouponInventoryUpstream {
  return {
    upstream_id: id,
    provider: 'anthropic',
    account_id: 'shared-account',
    organization_id: 'shared-organization',
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
  };
}

function credit(
  overrides: Partial<OpenAILimitResetCredit> = {},
): OpenAILimitResetCredit {
  return {
    id: 'credit',
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

function inventory(
  id: string,
  upstreams?: CouponInventoryResponse['upstreams'],
  kind: Upstream['kind'] = 'anthropic_oauth',
) {
  return couponInventoryForUpstream({
    upstream: { id, kind },
    fleet: upstreams ? { upstreams } : undefined,
    nowMs: NOW,
  });
}

function openai(
  available_count: number,
  credits: OpenAILimitResetCredit[],
): CouponInventoryUpstream {
  return {
    upstream_id: 'codex',
    provider: 'openai',
    account_id: 'codex-account',
    organization_id: null,
    cedar_ember: null,
    openai: { available_count, credits },
  };
}

describe('couponInventoryForUpstream', () => {
  it('keeps each row observation without deduplicating shared accounts', () => {
    const fleet = [
      observation('first'),
      observation('second', [grant({ resets_left: 2 })]),
    ];
    expect(inventory('first', fleet).total).toBe(3);
    expect(inventory('second', fleet).total).toBe(2);
  });

  it('counts active owned coupons even when unusable, paused or cooling down', () => {
    const result = inventory('first', [
      observation('first', [
        grant({ paused: true, usable_now: false, blocking: ['cooldown'] }),
        grant({
          id: 'other',
          resets_left: 2,
          usable_now: false,
          blocking: ['requires_limit'],
        }),
      ]),
    ]);
    expect(result.total).toBe(5);
    expect(result.complete).toBe(true);
  });

  it('excludes expired, future and depleted grants', () => {
    expect(
      inventory('first', [
        observation('first', [
          grant({ ends_at: '2026-11-12T11:59:00Z' }),
          grant({ id: 'future', starts_at: '2026-11-13T12:00:00Z' }),
          grant({ id: 'empty', resets_left: 0 }),
        ]),
      ]).total,
    ).toBe(0);
  });

  it('distinguishes missing inventory from a known zero and unsupported kinds', () => {
    expect(inventory('first').total).toBeNull();
    expect(inventory('missing', [observation('first')]).total).toBeNull();
    expect(inventory('first', [observation('first', [])]).total).toBe(0);
    expect(inventory('key', [], 'anthropic_api_key').supported).toBe(false);
    expect(inventory('gemini', [], 'gemini_oauth').supported).toBe(false);
  });

  it('never aliases overage-included scope to Fable', () => {
    const result = inventory('first', [
      observation('first', [
        grant({
          type: 'full',
          gives_back_weekly: true,
          clears: ['7d_overage_included'],
        }),
      ]),
    ]);
    expect(result.groups[0]?.scope).not.toContain('Fable');
  });

  it('uses OpenAI native summary counts when credit details are partial', () => {
    const result = inventory('codex', [openai(4, [credit()])], 'openai_oauth');
    expect(result.total).toBe(4);
    expect(result.groups.map((group) => group.resets)).toEqual([1, 3]);
  });

  it('includes active OpenAI coupons unsupported by the current plan', () => {
    const result = inventory(
      'codex',
      [openai(1, [credit({ is_supported_by_plan: false })])],
      'openai_oauth',
    );
    expect(result.total).toBe(1);
  });

  it('ages complete OpenAI credit lists without inventing a zero for missing data', () => {
    const expired = credit({ expires_at: '2026-11-12T11:59:00Z' });
    expect(
      inventory('codex', [openai(1, [expired])], 'openai_oauth').total,
    ).toBe(0);
    expect(
      inventory('codex', [openai(3, [expired])], 'openai_oauth').total,
    ).toBe(3);
    expect(
      inventory('codex', [observation('codex')], 'openai_oauth').total,
    ).toBeNull();
  });
});
