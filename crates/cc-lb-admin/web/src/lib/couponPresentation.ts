import type { QuotaSnapshot } from './api';
import type {
  CedarEmberLimitResets,
  LimitResetClaimResponse,
  LimitResetGrant,
  LimitResetsResponse,
} from './limitResets';
import { formatQuotaPercent } from './quotaSeverity';

/** The quota data already loaded by the page; never fetch a second reading. */
export type CouponQuotaWindow = Pick<
  QuotaSnapshot,
  'utilization' | 'state' | 'resets_at_unix_secs'
> & { window: string };

const WINDOW_LABELS: Record<string, string> = {
  '5h': '5-hour',
  '7d': 'Weekly',
  '7d_sonnet': 'Weekly (Sonnet)',
  '7d_opus': 'Weekly (Opus)',
  '7d_fable': 'Weekly (Fable)',
  '7d_cowork': 'Weekly (Cowork)',
  '7d_omelette': 'Weekly (Omelette)',
  '7d_oauth_apps': 'Weekly (OAuth apps)',
  '7d_overage_included': 'Weekly (included usage)',
  overage: 'Extra usage',
};

export function couponWindowLabel(window: string): string {
  // Labels never alias provider keys to a different quota observation.
  return WINDOW_LABELS[window] ?? 'Provider-defined scope';
}
export function isFableRebateGrant(grant: LimitResetGrant): boolean {
  return grant.type === 'full' && grant.gives_back_weekly === true;
}

export function openAIResetLabel(resetType: string): string {
  return resetType === 'codex_rate_limits'
    ? 'Codex allowance reset'
    : 'Reset coupon';
}

export function couponProductLabel(grant: LimitResetGrant): string {
  if (grant.openai_credit)
    return openAIResetLabel(grant.openai_credit.reset_type);
  if (isFableRebateGrant(grant)) return 'Fable usage reset';
  if (grant.type === 'weekly') return 'Weekly reset';
  return 'Reset coupon';
}

/** Adapt presentation only. Native provider inventory and claim IDs stay intact. */
export function couponInventory(
  data: LimitResetsResponse | undefined,
): CedarEmberLimitResets | null {
  if (!data) return null;
  if (data.provider !== 'openai') return data.cedar_ember;
  if (!data.openai) return null;
  return {
    eligible: true,
    ineligible_reason: null,
    at_limit: null,
    next_grant_id: null,
    weekly_resets_at: null,
    cooldown_until: null,
    grants: data.openai.credits.map((credit) => ({
      id: credit.id,
      label: credit.title ?? openAIResetLabel(credit.reset_type),
      type: null,
      gives_back_weekly: null,
      unavailable_reason: null,
      openai_credit: credit,
      resets_total: null,
      resets_left: credit.status === 'available' ? 1 : 0,
      starts_at: null,
      ends_at: credit.expires_at,
      clears: [],
      paused: false,
      usable_now:
        credit.status === 'available' && credit.is_supported_by_plan === true,
      use_requires_limit: false,
      percent_used: {},
      blocking:
        credit.is_supported_by_plan === false
          ? ['unsupported_plan']
          : credit.is_supported_by_plan == null
            ? ['plan_support_unreported']
            : [],
    })),
  };
}

export function couponEffectDescription(
  grant: LimitResetGrant,
  provider: LimitResetsResponse['provider'],
): string {
  if (provider === 'openai') {
    return grant.openai_credit?.reset_type === 'codex_rate_limits'
      ? 'Resets the Codex quota; OpenAI may change the reset schedule.'
      : 'Resets the provider-reported allowance; OpenAI may change the reset schedule.';
  }
  if (isFableRebateGrant(grant)) {
    return grant.clears.includes('5h')
      ? 'Returns Fable usage to the weekly allowance and resets 5-hour usage.'
      : 'Returns Fable usage to the weekly allowance.';
  }
  if (grant.gives_back_weekly === true) {
    return 'Returns the weekly usage covered by this coupon.';
  }
  if (grant.type === 'weekly' && grant.clears.includes('7d'))
    return 'Resets weekly usage.';
  return grant.clears.length > 0
    ? `Resets ${couponScopeLabel(grant).toLowerCase()}.`
    : 'Uses the reset allowance reported by the provider; scope is not reported.';
}

export function couponClaimMessage(response: LimitResetClaimResponse): string {
  if (response.result === 'not_limited') {
    return 'No reset was needed. No coupon was used.';
  }
  const applied =
    response.result === 'already_used'
      ? 'This reset was already applied. No additional coupon was used.'
      : 'Reset completed.';
  if (response.usage_refresh === 'refreshed') {
    return `${applied} Usage reloaded from the provider.`;
  }
  if (response.usage_refresh === 'failed') {
    return `${applied} The quota refresh failed. Refresh usage to check the current quota.`;
  }
  return `${applied} Check the current usage for the updated quota.`;
}

export function couponScopeLabel(
  grant: Pick<LimitResetGrant, 'clears'>,
): string {
  return grant.clears.length > 0
    ? [...new Set(grant.clears.map(couponWindowLabel))].join(', ')
    : 'Scope not reported';
}

export function couponTypeLabel(grant: LimitResetGrant): string {
  if (grant.openai_credit)
    return grant.openai_credit.reset_type === 'codex_rate_limits'
      ? 'Codex'
      : 'Reset';
  if (isFableRebateGrant(grant)) return 'Fable';
  return grant.type === 'weekly' ? 'Weekly' : 'Reset';
}

export function couponOptionLabel(grant: LimitResetGrant): string {
  return `${couponTypeLabel(grant)} ×${grant.resets_left}`;
}

export interface CouponGroup {
  key: string;
  label: string;
  fable: boolean;
  remaining: number;
  grants: LimitResetGrant[];
}

/** Identity is provider semantics, never the marketing title or expiry. */
export function couponGroupKey(
  grant: LimitResetGrant,
  provider: LimitResetsResponse['provider'],
): string {
  return JSON.stringify([
    provider,
    grant.type,
    [...grant.clears].sort(),
    grant.gives_back_weekly,
    grant.openai_credit?.reset_type ?? null,
    grant.openai_credit?.is_supported_by_plan ?? null,
  ]);
}

export function groupCouponGrants(
  grants: readonly LimitResetGrant[],
  provider: LimitResetsResponse['provider'],
): CouponGroup[] {
  const groups = new Map<string, CouponGroup>();
  for (const grant of grants) {
    const key = couponGroupKey(grant, provider);
    const existing = groups.get(key);
    if (existing) {
      existing.remaining += grant.resets_left;
      existing.grants.push(grant);
    } else {
      groups.set(key, {
        key,
        label: couponTypeLabel(grant),
        fable: isFableRebateGrant(grant),
        remaining: grant.resets_left,
        grants: [grant],
      });
    }
  }
  return [...groups.values()];
}

/** Only the initial type choice uses exhausted-target preference. Within a
 * type, spend the earliest-expiring usable exact grant. */
export function chooseCouponGroupGrant(
  group: CouponGroup,
  usable: readonly LimitResetGrant[],
): LimitResetGrant | null {
  const ready = group.grants.filter((grant) => usable.includes(grant));
  const candidates = ready.length > 0 ? ready : group.grants;
  let selected: LimitResetGrant | null = null;
  let earliest = Infinity;
  for (const grant of candidates) {
    const expiry = grant.ends_at == null ? Infinity : Date.parse(grant.ends_at);
    if (!selected || expiry < earliest) {
      selected = grant;
      earliest = expiry;
    }
  }
  return selected;
}

export interface CouponWindowUsage {
  window: string;
  label: string;
  percentUsed: number | null;
  state: 'fresh' | 'stale' | 'unknown';
  source: 'coupon' | 'quota' | null;
}

export interface CouponPresentation {
  targets: CouponWindowUsage[];
  /** Fresh full readings outside the declared clears, not proof that the
   * provider leaves them unchanged: a grant may report additional effects. */
  fullNonTargets: CouponWindowUsage[];
  hasFullTarget: boolean;
}

export interface CouponObservationOptions {
  /** The coupon endpoint returns cedar_ember only for a fresh, identity-bound
   * observation. A failed status refresh must not promote cached usage to fresh. */
  providerFresh: boolean;
}

function validPercent(value: number | undefined | null): value is number {
  return value != null && Number.isFinite(value) && value >= 0;
}

export function presentCoupon(
  grant: LimitResetGrant,
  windows: readonly CouponQuotaWindow[],
  { providerFresh }: CouponObservationOptions,
): CouponPresentation {
  const byWindow = new Map(windows.map((window) => [window.window, window]));
  const usage = (name: string): CouponWindowUsage => {
    const providerPercent = grant.percent_used[name];
    const quota = byWindow.get(name);
    const quotaPercent =
      quota?.utilization == null ? null : quota.utilization * 100;
    const base = { window: name, label: couponWindowLabel(name) };
    // percent_used and clears share the provider's exact namespace. A valid
    // zero is authoritative even when this key is absent from /quota/latest.
    if (providerFresh && validPercent(providerPercent)) {
      return {
        ...base,
        percentUsed: providerPercent,
        state: 'fresh',
        source: 'coupon',
      };
    }
    if (quota?.state === 'fresh' && validPercent(quotaPercent)) {
      return {
        ...base,
        percentUsed: quotaPercent,
        state: 'fresh',
        source: 'quota',
      };
    }
    if (!providerFresh && validPercent(providerPercent)) {
      return {
        ...base,
        percentUsed: providerPercent,
        state: 'stale',
        source: 'coupon',
      };
    }
    if (quota?.state === 'stale' && validPercent(quotaPercent)) {
      return {
        ...base,
        percentUsed: quotaPercent,
        state: 'stale',
        source: 'quota',
      };
    }
    return { ...base, percentUsed: null, state: 'unknown', source: null };
  };
  const targets = grant.clears.map(usage);
  const targetNames = new Set(grant.clears);
  const observedNames = new Set([
    ...Object.keys(grant.percent_used),
    ...byWindow.keys(),
  ]);
  const fullNonTargets = [...observedNames]
    .filter((name) => !targetNames.has(name))
    .map(usage)
    .filter((reading) => isFullCouponWindow(reading));
  return {
    targets,
    fullNonTargets,
    hasFullTarget: targets.some(isFullCouponWindow),
  };
}

export function isFullCouponWindow(reading: CouponWindowUsage): boolean {
  return (
    reading.state === 'fresh' &&
    reading.percentUsed != null &&
    reading.percentUsed >= 100
  );
}

export function couponUsageLabel(reading: CouponWindowUsage): string {
  if (reading.state === 'unknown') return 'Usage unknown';
  const percent = `${formatQuotaPercent(reading.percentUsed)} used`;
  const source =
    reading.source === 'coupon' ? 'coupon snapshot' : 'quota snapshot';
  return reading.state === 'stale'
    ? `${percent} · stale ${source}`
    : `${percent} · ${source}`;
}

/** Explicit choices never fall back to another grant, even if they disappear.
 * Only an untouched selection prefers a usable coupon clearing a full target,
 * then the earliest expiry; eligibility remains the caller's provider truth. */
export function chooseCouponGrant(
  active: readonly LimitResetGrant[],
  usable: readonly LimitResetGrant[],
  windows: readonly CouponQuotaWindow[],
  explicitId: string | null,
  options: CouponObservationOptions,
): LimitResetGrant | null {
  if (explicitId != null) {
    return active.find((grant) => grant.id === explicitId) ?? null;
  }
  let best: LimitResetGrant | null = null;
  let bestFull = false;
  let bestExpiry = Infinity;
  for (const grant of usable) {
    const full = presentCoupon(grant, windows, options).hasFullTarget;
    const expiry = grant.ends_at == null ? Infinity : Date.parse(grant.ends_at);
    if (
      best == null ||
      (full && !bestFull) ||
      (full === bestFull && expiry < bestExpiry)
    ) {
      best = grant;
      bestFull = full;
      bestExpiry = expiry;
    }
  }
  return best ?? active.at(0) ?? null;
}
