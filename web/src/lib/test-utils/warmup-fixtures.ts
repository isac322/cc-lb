import type {
  FireNowErrorReason,
  FireNowResponse,
  Upstream,
  WarmupAttempt,
  WarmupDialectPluginSnapshot,
  WarmupLane,
  WarmupLaneSummary,
  WarmupSummary,
} from '../queries';

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

export function makeWarmupAttempt(
  overrides: Partial<WarmupAttempt> = {},
): WarmupAttempt {
  return {
    id: 'attempt-1',
    upstream_id: 'oauth-1',
    lane: 'primary',
    attempted_at_unix_secs: 1_718_380_800,
    completed_at_unix_secs: 1_718_380_801,
    scheduled_for_unix_secs: 1_718_380_800,
    trigger: 'scheduled',
    status: 'success',
    reason: 'cycle_advanced',
    dispatch_kind: 'http',
    http_status: 200,
    cycle_key: 1_718_380_800,
    expected_cycle_key: 1_718_380_800,
    idle_secs_since_prev_window: 300,
    replica_id: 'replica-1',
    lease_holder: 'replica-1',
    upstream_spec_revision: 1,
    dialect_plugin_snapshot: null,
    error_detail: null,
    ...overrides,
  };
}

const DEFAULT_TARGET_WINDOWS: Record<WarmupLane, string[]> = {
  primary: ['5h', '7d'],
  third_party: ['third_party_5h', 'third_party_7d'],
};

/**
 * One lane. `last_attempt` defaults to the newest `recent_attempts` entry so
 * a lane built from attempts stays internally consistent.
 */
export function makeWarmupLane(
  overrides: Partial<WarmupLaneSummary> = {},
): WarmupLaneSummary {
  const lane = overrides.lane ?? 'primary';
  const recent = overrides.recent_attempts ?? [];
  return {
    lane,
    eligible: true,
    ineligible_reason: null,
    target_windows: DEFAULT_TARGET_WINDOWS[lane],
    next_scheduled_at_unix_secs: 1_718_398_800,
    last_attempt: recent[0] ?? null,
    recent_attempts: recent,
    ...overrides,
  };
}

export function makeWarmupSummary(
  lanes: WarmupLaneSummary[],
  {
    upstreamId = 'oauth-1',
    dialectPlugin = null,
  }: {
    upstreamId?: string;
    dialectPlugin?: WarmupDialectPluginSnapshot | null;
  } = {},
): WarmupSummary {
  return { upstream_id: upstreamId, dialect_plugin: dialectPlugin, lanes };
}

/** Gemini OAuth summary: Gemini models plus Claude and GPT-OSS models. */
export function makeGeminiWarmupSummary({
  primary = {},
  thirdParty = {},
  upstreamId = 'oauth-1',
}: {
  primary?: Partial<WarmupLaneSummary>;
  thirdParty?: Partial<WarmupLaneSummary>;
  upstreamId?: string;
} = {}): WarmupSummary {
  return makeWarmupSummary(
    [
      makeWarmupLane({
        lane: 'primary',
        target_windows: ['gemini_5h', 'gemini_7d'],
        ...primary,
      }),
      makeWarmupLane({ lane: 'third_party', ...thirdParty }),
    ],
    { upstreamId },
  );
}
