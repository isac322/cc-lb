import { ApiError, getJson } from './api';
import { isActiveGrant } from './couponNudge';
import {
  couponProductLabel,
  couponScopeLabel,
  isFableRebateGrant,
  openAIResetLabel,
} from './couponPresentation';
import type {
  LimitResetGrant,
  LimitResetsResponse,
  OpenAILimitResetCredit,
  OpenAILimitResets,
} from './limitResets';
import type { ProviderFamily } from './providers';
import { usePolledData } from './usePolledData';

export interface CouponInventoryUpstream extends LimitResetsResponse {
  upstream_id: string;
}

export interface CouponInventoryResponse {
  upstreams: CouponInventoryUpstream[];
}

export interface CouponInventoryGroup {
  key: string;
  provider: LimitResetsResponse['provider'];
  grant: LimitResetGrant | null;
  label: string;
  scope: string | null;
  fable: boolean;
  planUnsupported: boolean;
  applicabilityUnknown: boolean;
  unknownType: boolean;
  resets: number;
  upstreamIds: string[];
}

export interface CouponInventory {
  /** Null is unknown, never a fabricated zero for an incomplete inventory. */
  total: number | null;
  complete: boolean;
  groups: CouponInventoryGroup[];
  upstreamIds: string[];
  unknownUpstreamIds: string[];
  supported: boolean;
}

/** One cached fleet read, shared by all principal rows and provider sections. */
export function useCouponInventory() {
  return usePolledData(
    {
      queryKey: ['limit-resets', 'inventory'] as const,
      queryFn: ({ signal }) =>
        getJson<CouponInventoryResponse>('/admin/v1/upstreams/limit-resets', {
          signal,
        }),
      retry: (failureCount, error) =>
        !(error instanceof ApiError && error.status === 404) &&
        failureCount < 3,
    },
    60_000,
  );
}

/** Inventory describes access, not ownership or a model-routing guarantee.
 * Duplicate account observations have no ordering timestamp: use the lower
 * count and only their common, consistent details. Native summary counts stay
 * authoritative when OpenAI returns only part of its credit list. */
export function aggregateCouponInventory({
  fleet,
  allowedUpstreams,
  providers,
  nowMs,
  incomplete = false,
}: {
  fleet: CouponInventoryResponse | undefined;
  allowedUpstreams: readonly string[] | undefined;
  providers?: readonly ProviderFamily[];
  nowMs: number;
  incomplete?: boolean;
}): CouponInventory {
  const supported =
    providers === undefined ||
    providers.some(
      (provider) => provider === 'anthropic' || provider === 'openai',
    );
  const unknown: CouponInventory = {
    total: null,
    complete: false,
    groups: [],
    upstreamIds: [],
    unknownUpstreamIds: [],
    supported,
  };
  if (!supported || !fleet || !allowedUpstreams) return unknown;

  const allowed = new Set(allowedUpstreams);
  const upstreams = fleet.upstreams.filter(
    (upstream) =>
      (providers === undefined || providers.includes(upstream.provider)) &&
      (allowed.size === 0 || allowed.has(upstream.upstream_id)),
  );
  const accounts = new Map<string, CouponInventoryUpstream[]>();
  const unknownIds = new Set<string>();
  let complete = !incomplete;
  for (const upstream of upstreams) {
    if (
      !upstream.account_id ||
      (upstream.provider === 'anthropic' && !upstream.organization_id)
    ) {
      unknownIds.add(upstream.upstream_id);
      complete = false;
      continue;
    }
    const identity = JSON.stringify([
      upstream.provider,
      upstream.account_id,
      upstream.provider === 'anthropic' ? upstream.organization_id : null,
    ]);
    const observations = accounts.get(identity) ?? [];
    observations.push(upstream);
    accounts.set(identity, observations);
  }

  const groups = new Map<string, CouponInventoryGroup>();
  let total = 0;
  for (const observations of accounts.values()) {
    const sourceIds = observations
      .map((upstream) => upstream.upstream_id)
      .sort();
    if (observations[0]?.provider === 'openai') {
      const native = reconcileOpenAI(observations, nowMs);
      if (!native.complete) complete = false;
      if (native.total === null) {
        for (const id of sourceIds) unknownIds.add(id);
        continue;
      }
      total += native.total;
      let classified = 0;
      for (const credit of native.credits) {
        const key = JSON.stringify([
          'openai',
          credit.reset_type,
          credit.is_supported_by_plan,
        ]);
        addGroup(groups, {
          key,
          provider: 'openai',
          grant: null,
          label: openAIResetLabel(credit.reset_type),
          scope: null,
          fable: false,
          planUnsupported: credit.is_supported_by_plan === false,
          applicabilityUnknown: credit.is_supported_by_plan === null,
          unknownType: false,
          resets: 1,
          upstreamIds: sourceIds,
        });
        classified += 1;
      }
      const remaining = native.total - classified;
      if (remaining > 0) {
        addGroup(groups, {
          key: 'openai:unreported-types',
          provider: 'openai',
          grant: null,
          label: 'Type not reported',
          scope: null,
          fable: false,
          planUnsupported: false,
          applicabilityUnknown: true,
          unknownType: true,
          resets: remaining,
          upstreamIds: sourceIds,
        });
      }
      continue;
    }
    if (observations.some((upstream) => upstream.cedar_ember === null)) {
      for (const upstream of observations) unknownIds.add(upstream.upstream_id);
      complete = false;
      continue;
    }
    const snapshots = observations.map(
      (upstream) => upstream.cedar_ember?.grants ?? [],
    );
    const ids = new Set(snapshots.flatMap((grants) => grants.map((g) => g.id)));
    for (const id of ids) {
      const grants = snapshots.flatMap((snapshot) =>
        snapshot.filter((grant) => grant.id === id),
      );
      const first = grants[0];
      if (!first) continue;
      const scope = JSON.stringify([...new Set(first.clears)].sort());
      const missing = snapshots.some(
        (snapshot) => !snapshot.some((grant) => grant.id === id),
      );
      const inconsistent = grants.some(
        (grant) =>
          grant.label !== first.label ||
          grant.type !== first.type ||
          grant.gives_back_weekly !== first.gives_back_weekly ||
          JSON.stringify([...new Set(grant.clears)].sort()) !== scope ||
          !Number.isSafeInteger(grant.resets_left) ||
          grant.resets_left < 0,
      );
      if (missing || inconsistent) {
        complete = false;
        continue;
      }
      const resets = Math.min(...grants.map((grant) => grant.resets_left));
      const active = grants.map((grant) => isActiveGrant(grant, nowMs));
      if (
        grants.some((grant) => grant.resets_left !== first.resets_left) ||
        active.some((value) => value !== active[0])
      ) {
        complete = false;
      }
      if (!active.every(Boolean)) continue;

      const key = JSON.stringify([
        observations[0]?.provider,
        first.type,
        first.gives_back_weekly,
        first.label,
        scope,
      ]);
      addGroup(groups, {
        key,
        provider: 'anthropic',
        grant: first,
        label: couponProductLabel(first),
        scope: couponScopeLabel(first),
        fable: isFableRebateGrant(first),
        planUnsupported: false,
        applicabilityUnknown: false,
        unknownType: false,
        resets,
        upstreamIds: sourceIds,
      });
      total += resets;
    }
  }

  return {
    total: complete || total > 0 ? total : null,
    complete,
    groups: [...groups.values()].sort((a, b) => a.key.localeCompare(b.key)),
    upstreamIds: [...new Set(upstreams.map((u) => u.upstream_id))].sort(),
    unknownUpstreamIds: [...unknownIds].sort(),
    supported,
  };
}

function addGroup(
  groups: Map<string, CouponInventoryGroup>,
  incoming: CouponInventoryGroup,
) {
  const existing = groups.get(incoming.key);
  if (!existing) {
    groups.set(incoming.key, incoming);
    return;
  }
  existing.resets += incoming.resets;
  existing.upstreamIds = [
    ...new Set([...existing.upstreamIds, ...incoming.upstreamIds]),
  ].sort();
}

function creditActive(credit: OpenAILimitResetCredit, nowMs: number): boolean {
  if (credit.status !== 'available') return false;
  const starts =
    credit.granted_at === null ? null : Date.parse(credit.granted_at);
  const ends =
    credit.expires_at === null ? null : Date.parse(credit.expires_at);
  return (
    (starts === null || (Number.isFinite(starts) && starts <= nowMs)) &&
    (ends === null || (Number.isFinite(ends) && ends > nowMs))
  );
}

function reconcileOpenAI(
  observations: CouponInventoryUpstream[],
  nowMs: number,
): {
  total: number | null;
  complete: boolean;
  credits: OpenAILimitResetCredit[];
} {
  const snapshots: OpenAILimitResets[] = [];
  for (const observation of observations) {
    if (
      !observation.openai ||
      !Number.isSafeInteger(observation.openai.available_count) ||
      observation.openai.available_count < 0
    ) {
      return { total: null, complete: false, credits: [] };
    }
    snapshots.push(observation.openai);
  }
  let complete = snapshots.every(
    (snapshot) => snapshot.available_count === snapshots[0]?.available_count,
  );
  const maps = snapshots.map((snapshot) => {
    const credits = new Map<string, OpenAILimitResetCredit>();
    const duplicated = new Set<string>();
    for (const credit of snapshot.credits) {
      if (credits.has(credit.id) || duplicated.has(credit.id)) {
        complete = false;
        duplicated.add(credit.id);
        credits.delete(credit.id);
      } else {
        credits.set(credit.id, credit);
      }
    }
    return credits;
  });
  const counts = snapshots.map((snapshot, index) => {
    const details = [...(maps[index]?.values() ?? [])];
    const available = details.filter((credit) => credit.status === 'available');
    const invalidTime = available.some(
      (credit) =>
        (credit.granted_at !== null &&
          !Number.isFinite(Date.parse(credit.granted_at))) ||
        (credit.expires_at !== null &&
          !Number.isFinite(Date.parse(credit.expires_at))),
    );
    if (invalidTime) {
      complete = false;
      return snapshot.available_count;
    }
    // A complete available list can age past expiry between provider polls.
    // Partial lists cannot redefine the provider's authoritative summary.
    return available.length === snapshot.available_count
      ? available.filter((credit) => creditActive(credit, nowMs)).length
      : snapshot.available_count;
  });
  const total = Math.min(...counts);
  if (counts.some((count) => count !== counts[0])) complete = false;
  const credits: OpenAILimitResetCredit[] = [];
  const firstMap = maps[0];
  if (firstMap) {
    for (const credit of firstMap.values()) {
      if (!creditActive(credit, nowMs)) continue;
      const consistent = maps.every((map) => {
        const other = map.get(credit.id);
        return (
          other !== undefined &&
          creditActive(other, nowMs) &&
          other.reset_type === credit.reset_type &&
          other.is_supported_by_plan === credit.is_supported_by_plan
        );
      });
      if (consistent) credits.push(credit);
      else complete = false;
    }
    if (
      maps.some((map) =>
        [...map.values()].some(
          (credit) =>
            creditActive(credit, nowMs) &&
            !credits.some((common) => common.id === credit.id),
        ),
      )
    ) {
      complete = false;
    }
  }
  // Conflicting detailed counts cannot establish a type allocation.
  if (credits.length > total) {
    return { total, complete: false, credits: [] };
  }
  return { total, complete, credits };
}
