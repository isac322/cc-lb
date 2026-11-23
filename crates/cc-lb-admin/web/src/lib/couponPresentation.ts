import type { QuotaSnapshot } from './api';
import type {
  CedarEmberLimitResets,
  LimitResetClaimResponse,
  LimitResetGrant,
  LimitResetsResponse,
  OpenAILimitResets,
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
  // Anthropic's cedar_ember reports the Fable/overage-included weekly bucket under this key.
  '7d_overage_included': 'Weekly (Fable)',
  overage: 'Extra usage',
  monthly: 'Monthly',
};

function relatedQuotaKeys(window: string): readonly string[] {
  if (window === '7d_fable') return ['7d_fable', '7d_overage_included'];
  if (window === '7d_overage_included')
    return ['7d_overage_included', '7d_fable'];
  return [window];
}

function grantClearsWindow(
  grant: Pick<LimitResetGrant, 'clears'>,
  window: string,
): boolean {
  return relatedQuotaKeys(window).some((key) => grant.clears.includes(key));
}

export function couponWindowLabel(window: string): string {
  // Keep provider keys unaliased unless a 1:1 equivalence is confirmed.
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

function openAIManualRedemptionBlock(
  inventory: OpenAILimitResets | null | undefined,
): string | null {
  const manual = inventory?.manual_redemption;
  if (manual?.reason === 'coverage_unreported') return 'coverage_unreported';
  if (!manual || manual.windows.length === 0) return 'manual_usage_unknown';
  const readings = manual.windows.map((window) => window.used_percent);
  const allKnown = readings.every(
    (used) => used != null && Number.isFinite(used) && used >= 0,
  );
  const anyQualifying = readings.some(
    (used) => used != null && Number.isFinite(used) && used > 20,
  );
  if (manual.eligible && manual.reason == null && anyQualifying) return null;
  if (allKnown && !anyQualifying) return 'manual_usage_not_needed';
  return 'manual_usage_unknown';
}

/** Adapt presentation only. Native provider inventory and claim IDs stay intact. */
export function couponInventory(
  data: LimitResetsResponse | undefined,
): CedarEmberLimitResets | null {
  if (!data) return null;
  if (data.provider !== 'openai') return data.cedar_ember;
  if (!data.openai) return null;
  const manualBlock = openAIManualRedemptionBlock(data.openai);
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
      openai_manual_redemption: data.openai?.manual_redemption,
      resets_total: null,
      resets_left: credit.status === 'available' ? 1 : 0,
      starts_at: null,
      ends_at: credit.expires_at,
      clears: [],
      paused: false,
      usable_now:
        credit.status === 'available' &&
        credit.is_supported_by_plan === true &&
        credit.reset_type === 'codex_rate_limits' &&
        manualBlock == null,
      use_requires_limit: false,
      percent_used: {},
      blocking:
        credit.is_supported_by_plan === false
          ? ['unsupported_plan']
          : credit.is_supported_by_plan == null
            ? ['plan_support_unreported']
            : credit.reset_type !== 'codex_rate_limits'
              ? ['unsupported_reset_type']
              : manualBlock != null
                ? [manualBlock]
                : [],
    })),
  };
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
  grant: Pick<LimitResetGrant, 'clears'> &
    Partial<Pick<LimitResetGrant, 'type' | 'gives_back_weekly'>>,
): string {
  const fable = grant.type === 'full' && grant.gives_back_weekly === true;
  const parts = grant.clears.map(couponWindowLabel);
  if (fable) {
    const fiveIdx = parts.indexOf(WINDOW_LABELS['5h']!);
    if (fiveIdx >= 0) {
      parts.splice(fiveIdx + 1, 0, 'Weekly (Fable portion)');
    } else {
      parts.unshift('Weekly (Fable portion)');
    }
    if (!parts.includes(WINDOW_LABELS['7d_fable']!)) {
      parts.push(WINDOW_LABELS['7d_fable']!);
    }
  }
  return parts.length > 0
    ? [...new Set(parts)].join(', ')
    : 'Scope not reported';
}

function couponTypeLabel(grant: LimitResetGrant): string {
  if (grant.openai_credit)
    return grant.openai_credit.reset_type === 'codex_rate_limits'
      ? 'Codex'
      : 'Reset';
  if (isFableRebateGrant(grant)) return 'Fable';
  return grant.type === 'weekly' ? 'Weekly' : 'Reset';
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

function readWindowUsage(
  grant: LimitResetGrant,
  byWindow: ReadonlyMap<string, CouponQuotaWindow>,
  name: string,
  { providerFresh }: CouponObservationOptions,
): CouponWindowUsage {
  const keys = relatedQuotaKeys(name);
  const providerPercent = keys
    .map((key) => grant.percent_used[key])
    .find((value) => value !== undefined);
  const quota = keys
    .map((key) => byWindow.get(key))
    .find((entry) => entry !== undefined);
  const quotaPercent =
    quota?.utilization == null ? null : quota.utilization * 100;
  const base = { window: name, label: couponWindowLabel(name) };
  // percent_used and clears share the provider's exact namespace, with a
  // confirmed 1:1 equivalence between 7d_overage_included and 7d_fable. A
  // valid zero is authoritative even when the key is absent from /quota/latest.
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
}

export function fableCanRestoreFullFiveHour(
  grant: LimitResetGrant,
  windows: readonly CouponQuotaWindow[],
  options: CouponObservationOptions,
): boolean {
  if (!isFableRebateGrant(grant)) return false;
  const byWindow = new Map(windows.map((window) => [window.window, window]));
  if (byWindow.get('5h')?.state === 'absent') return false;
  return isFullCouponWindow(readWindowUsage(grant, byWindow, '5h', options));
}

export function fableCanRestoreFullWeekly(
  grant: LimitResetGrant,
  windows: readonly CouponQuotaWindow[],
  options: CouponObservationOptions,
): boolean {
  if (!isFableRebateGrant(grant)) return false;
  const byWindow = new Map(windows.map((window) => [window.window, window]));
  if (byWindow.get('7d')?.state === 'absent') return false;
  const weekly = readWindowUsage(grant, byWindow, '7d', options);
  if (!isFullCouponWindow(weekly)) return false;
  const fableBucket = readWindowUsage(grant, byWindow, '7d_fable', options);
  return (
    fableBucket.state === 'fresh' &&
    fableBucket.percentUsed != null &&
    fableBucket.percentUsed > 0
  );
}

export function presentCoupon(
  grant: LimitResetGrant,
  windows: readonly CouponQuotaWindow[],
  options: CouponObservationOptions,
): CouponPresentation {
  const byWindow = new Map(windows.map((window) => [window.window, window]));
  const usage = (name: string): CouponWindowUsage =>
    readWindowUsage(grant, byWindow, name, options);
  const targets = grant.clears.map(usage);
  const targetNames = new Set(
    grant.clears.flatMap((name) => [...relatedQuotaKeys(name)]),
  );
  const observedNames = new Set(
    [...Object.keys(grant.percent_used), ...byWindow.keys()].map((name) =>
      name === '7d_overage_included' ? '7d_fable' : name,
    ),
  );
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

export type CouponQuotaEffect = 'reset' | 'unchanged' | 'fable_weekly';

export interface CouponEffectRow {
  window: string;
  label: string;
  current: CouponWindowUsage;
  effect: CouponQuotaEffect;
  schedule: 'unchanged' | 'confirmed_after_use' | 'updated_after_use';
  resetsAtUnixSecs: number | null;
}

export interface CouponEffects {
  rows: CouponEffectRow[];
  otherLimitsUnchanged: boolean;
}

/** Expected future effects for known coupon scopes, without inventing a
 * numeric rebate for the shared Fable-weighted weekly pool. */
export function presentCouponEffects(
  grant: LimitResetGrant,
  windows: readonly CouponQuotaWindow[],
  options: CouponObservationOptions & {
    provider: LimitResetsResponse['provider'];
    weeklyResetsAt?: string | null;
  },
): CouponEffects {
  if (options.provider === 'openai') {
    const full = grant.openai_credit?.reset_type === 'codex_rate_limits';
    if (!full) return { rows: [], otherLimitsUnchanged: false };
    return {
      otherLimitsUnchanged: full,
      rows: (
        grant.openai_manual_redemption?.windows ?? []
      ).map<CouponEffectRow>((window) => {
        const currentKnown =
          options.providerFresh && validPercent(window.used_percent);
        const unchanged = currentKnown && window.used_percent === 0;
        return {
          window: window.window,
          label: couponWindowLabel(window.window),
          current: {
            window: window.window,
            label: couponWindowLabel(window.window),
            percentUsed: currentKnown ? window.used_percent : null,
            state: currentKnown ? 'fresh' : 'unknown',
            source: currentKnown ? 'coupon' : null,
          },
          effect: unchanged ? 'unchanged' : 'reset',
          schedule: unchanged ? 'unchanged' : 'updated_after_use',
          resetsAtUnixSecs:
            window.resets_at_unix_secs != null &&
            Number.isFinite(window.resets_at_unix_secs) &&
            window.resets_at_unix_secs > 0
              ? window.resets_at_unix_secs
              : null,
        };
      }),
    };
  }

  const fable = isFableRebateGrant(grant);
  if (
    !fable &&
    !grant.clears.some((name) => Object.hasOwn(WINDOW_LABELS, name))
  ) {
    return { rows: [], otherLimitsUnchanged: false };
  }

  const byWindow = new Map(windows.map((window) => [window.window, window]));
  const clearsFable =
    grantClearsWindow(grant, '7d_fable') ||
    Object.hasOwn(grant.percent_used, '7d_overage_included');
  const names = new Set([
    ...windows
      .filter(
        (window) => window.state !== 'absent' && window.state !== 'unobserved',
      )
      .map((window) => window.window),
    ...grant.clears,
    ...Object.keys(grant.percent_used),
    ...(clearsFable ? ['7d_fable'] : []),
    ...(fable ? ['7d', '7d_fable'] : []),
  ]);
  const visibleNames = [...names].filter((name) => {
    if (!Object.hasOwn(WINDOW_LABELS, name) || name === '7d_overage_included') {
      return false;
    }
    if (byWindow.get(name)?.state !== 'absent') return true;
    return (
      name === '7d_fable' &&
      options.providerFresh &&
      relatedQuotaKeys(name).some((key) =>
        validPercent(grant.percent_used[key]),
      )
    );
  });
  const order = ['5h', '7d', '7d_fable', '7d_sonnet', '7d_opus', 'overage'];
  visibleNames.sort((a, b) => {
    const aIndex = order.indexOf(a);
    const bIndex = order.indexOf(b);
    return (
      (aIndex < 0 ? order.length : aIndex) -
      (bIndex < 0 ? order.length : bIndex)
    );
  });
  const readings = presentCoupon(
    { ...grant, clears: visibleNames },
    windows,
    options,
  ).targets;
  const weeklyFallback =
    options.weeklyResetsAt == null
      ? null
      : Date.parse(options.weeklyResetsAt) / 1000;
  return {
    otherLimitsUnchanged: false,
    rows: readings.map<CouponEffectRow>((current) => {
      const window = byWindow.get(current.window);
      let effect: CouponQuotaEffect;
      if (current.window === 'overage') effect = 'unchanged';
      else if (fable && current.window === '5h') effect = 'reset';
      else if (fable && current.window === '7d') effect = 'fable_weekly';
      else if (fable && current.window === '7d_fable') effect = 'reset';
      else if (grantClearsWindow(grant, current.window)) effect = 'reset';
      else effect = 'unchanged';
      const weekly = current.window.startsWith('7d');
      const reportedReset =
        window?.resets_at_unix_secs ?? (weekly ? weeklyFallback : null);
      const schedule: CouponEffectRow['schedule'] =
        weekly || effect === 'unchanged' ? 'unchanged' : 'confirmed_after_use';
      return {
        window: current.window,
        label: current.label,
        current,
        effect,
        schedule,
        resetsAtUnixSecs:
          reportedReset != null &&
          Number.isFinite(reportedReset) &&
          reportedReset > 0
            ? reportedReset
            : null,
      };
    }),
  };
}

/** Explicit choices never fall back to another grant, even if they disappear.
 * Only an untouched selection prefers a usable coupon clearing a full target
 * (direct resets first, then Fable weekly rebates), then the earliest expiry;
 * eligibility remains the caller's provider truth. */
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
  let bestPriority = -1;
  let bestExpiry = Infinity;
  for (const grant of usable) {
    const directFull = isFableRebateGrant(grant)
      ? fableCanRestoreFullFiveHour(grant, windows, options)
      : presentCoupon(grant, windows, options).hasFullTarget;
    const priority = directFull
      ? 2
      : fableCanRestoreFullWeekly(grant, windows, options)
        ? 1
        : 0;
    const expiry = grant.ends_at == null ? Infinity : Date.parse(grant.ends_at);
    if (
      best == null ||
      priority > bestPriority ||
      (priority === bestPriority && expiry < bestExpiry)
    ) {
      best = grant;
      bestPriority = priority;
      bestExpiry = expiry;
    }
  }
  return best ?? active.at(0) ?? null;
}
