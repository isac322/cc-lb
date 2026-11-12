import { useMemo } from 'react';
import { ApiError, getJson } from './api';
import { isActiveGrant, useCouponNow } from './couponNudge';
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
} from './limitResets';
import type { Upstream } from './queries';
import { usePolledData } from './usePolledData';

export interface CouponInventoryUpstream extends LimitResetsResponse {
  upstream_id: string;
}

export interface CouponInventoryResponse {
  upstreams: CouponInventoryUpstream[];
}

export interface CouponInventoryGroup {
  key: string;
  grant: LimitResetGrant | null;
  label: string;
  scope: string | null;
  fable: boolean;
  resets: number;
}

export interface CouponInventory {
  /** Null is unknown, never a fabricated zero for missing inventory. */
  total: number | null;
  complete: boolean;
  supported: boolean;
  groups: CouponInventoryGroup[];
  credentialsStored: boolean | undefined;
}

/** One cached fleet read; each upstream keeps its own provider observation. */
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

/** Inventory belongs to a row, even when two upstreams bind the same account.
 * Active coupons remain owned while paused, on cooldown or unsupported by a plan. */
export function couponInventoryForUpstream({
  upstream,
  fleet,
  nowMs,
}: {
  upstream: Pick<Upstream, 'id' | 'kind'>;
  fleet: CouponInventoryResponse | undefined;
  nowMs: number;
}): CouponInventory {
  const supported =
    upstream.kind === 'anthropic_oauth' || upstream.kind === 'openai_oauth';
  const unknown: CouponInventory = {
    total: null,
    complete: false,
    supported,
    groups: [],
    credentialsStored: undefined,
  };
  if (!supported) return { ...unknown, complete: true };
  const observation = fleet?.upstreams.find(
    (item) => item.upstream_id === upstream.id,
  );
  if (!observation) return unknown;
  const credentialsStored = observation.account_id ? true : undefined;
  if (upstream.kind === 'openai_oauth') {
    const native = observation.openai;
    if (
      !native ||
      !Number.isSafeInteger(native.available_count) ||
      native.available_count < 0
    )
      return { ...unknown, credentialsStored };
    const available = native.credits.filter(
      (credit) => credit.status === 'available',
    );
    const active = available.filter((credit) => creditActive(credit, nowMs));
    // Only a complete detailed list can age the provider's summary between polls.
    const datesKnown = available.every(
      (credit) =>
        (credit.granted_at === null ||
          Number.isFinite(Date.parse(credit.granted_at))) &&
        (credit.expires_at === null ||
          Number.isFinite(Date.parse(credit.expires_at))),
    );
    const total =
      datesKnown && available.length === native.available_count
        ? active.length
        : native.available_count;
    const groups: CouponInventoryGroup[] = active
      .slice(0, total)
      .map((credit) => ({
        key: credit.id,
        grant: null,
        label: openAIResetLabel(credit.reset_type),
        scope: null,
        fable: false,
        resets: 1,
      }));
    if (groups.length < total) {
      groups.push({
        key: 'unreported',
        grant: null,
        label: 'Type not reported',
        scope: null,
        fable: false,
        resets: total - groups.length,
      });
    }
    return { total, complete: true, supported, groups, credentialsStored };
  }
  const native = observation.cedar_ember;
  if (!native) return { ...unknown, credentialsStored };
  const active = native.grants.filter((grant) => isActiveGrant(grant, nowMs));
  if (
    active.some(
      (grant) =>
        !Number.isSafeInteger(grant.resets_left) || grant.resets_left < 0,
    )
  )
    return { ...unknown, credentialsStored };
  const groups = active.map((grant) => ({
    key: grant.id,
    grant,
    label: couponProductLabel(grant),
    scope: couponScopeLabel(grant),
    fable: isFableRebateGrant(grant),
    resets: grant.resets_left,
  }));
  return {
    total: groups.reduce((sum, group) => sum + group.resets, 0),
    complete: true,
    supported,
    groups,
    credentialsStored,
  };
}

export function useUpstreamCouponInventory(upstreams: readonly Upstream[]) {
  const query = useCouponInventory();
  const nowMs = useCouponNow();
  const fleet =
    query.isError || query.isPlaceholderData ? undefined : query.data;
  const inventories = useMemo(
    () =>
      new Map(
        upstreams.map((upstream) => [
          upstream.id,
          couponInventoryForUpstream({ upstream, fleet, nowMs }),
        ]),
      ),
    [upstreams, fleet, nowMs],
  );
  return { inventories, loading: query.isPending, error: query.isError };
}
