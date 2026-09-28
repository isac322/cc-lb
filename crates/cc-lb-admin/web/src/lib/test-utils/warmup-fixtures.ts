import type { FireNowErrorReason, FireNowResponse, Upstream } from '../queries';

type UpstreamFixtureOverrides = Partial<Upstream> & {
  revision?: number;
  last_warmup_at_unix_secs?: number | null;
  status?: Partial<Upstream['status']>;
};

function mergeUpstreamOverrides(
  base: Upstream,
  overrides: UpstreamFixtureOverrides = {},
): Upstream {
  const { revision, last_warmup_at_unix_secs, status, ...rest } = overrides;
  const mergedStatus: Upstream['status'] = {
    ...base.status,
    ...status,
  };
  if ('last_warmup_at_unix_secs' in overrides) {
    mergedStatus.last_warmup_at_unix_secs = last_warmup_at_unix_secs ?? null;
  }
  return {
    ...base,
    ...rest,
    spec_revision: rest.spec_revision ?? revision ?? base.spec_revision,
    status: mergedStatus,
  };
}

export function makeOauthUpstream(
  overrides?: UpstreamFixtureOverrides,
): Upstream {
  return mergeUpstreamOverrides(
    {
      id: 'oauth-1',
      name: 'my-prod-oauth',
      kind: 'anthropic_oauth',
      enabled: true,
      base_url: null,
      warmup_enabled: true,
      warmup_dialect_plugin: null,
      spec_revision: 1,
      status: {
        last_apply_error: null,
        last_apply_at_unix_secs: null,
        last_warmup_at_unix_secs: 1718380800,
      },
    },
    overrides,
  );
}

export function makeFireNowSuccess(): FireNowResponse {
  return { fired: true, cycle_key: 1718380800 };
}

export function makeFireNowLeaseHeld(heldBy = 'replica-2'): FireNowResponse {
  return { fired: false, reason: 'lease_held', held_by: heldBy };
}

export function makeFireNowError(reason: FireNowErrorReason): FireNowResponse {
  return { fired: false, reason };
}
