import type {
  Upstream,
  WarmupAttempt,
  WarmupOutcome,
  WarmupReason,
  WarmupRecentSummary,
  WarmupSummary,
  WarmupTrigger,
} from '../../lib/queries';
import type { WarmupMockFixture, WarmupScenarioId } from './types';

const NOW_SECS = Math.floor(Date.now() / 1000);
const UPSTREAM_ID = 'up_mock_warmup';

const empty7d: WarmupRecentSummary = {
  success_fresh: 0,
  success_redundant: 0,
  transient_failure: 0,
  permanent_failure: 0,
  skipped: 0,
};

const baseUpstream: Upstream = {
  id: UPSTREAM_ID,
  name: 'bear-max',
  kind: 'anthropic_oauth',
  enabled: true,
  spec_revision: 32,
  base_url: null,
  api_key_env: null,
  warmup_enabled: true,
  warmup_dialect_plugin: {
    wasm_registry_id: 'subscription-launderer',
    wire_version: 2,
    config: { model: 'claude-haiku-20240307', max_tokens: 16 },
  },
  status: {
    last_apply_error: null,
    last_apply_at_unix_secs: NOW_SECS - 7_200,
    last_warmup_at_unix_secs: NOW_SECS - 1_800,
  },
};

interface AttemptInput {
  readonly id: string;
  readonly offsetSecs: number;
  readonly outcome: WarmupOutcome;
  readonly reason: WarmupReason | null;
  readonly httpStatus: number | null;
  readonly errorDetail: string | null;
  readonly idleSecs: number | null;
  readonly trigger?: WarmupTrigger;
  readonly cycleKey?: number | null;
}

function makeAttempt(input: AttemptInput): WarmupAttempt {
  const attemptedAt = NOW_SECS - input.offsetSecs;
  return {
    id: input.id,
    upstream_id: UPSTREAM_ID,
    attempted_at_unix_secs: attemptedAt,
    completed_at_unix_secs: attemptedAt + 1,
    scheduled_for_unix_secs: attemptedAt,
    trigger: input.trigger ?? 'scheduled',
    outcome: input.outcome,
    reason: input.reason,
    http_status: input.httpStatus,
    cycle_key:
      input.cycleKey !== undefined
        ? input.cycleKey
        : input.outcome === 'success_fresh'
          ? attemptedAt
          : null,
    expected_cycle_key: attemptedAt,
    idle_secs_since_prev_window: input.idleSecs,
    replica_id: 'replica-a',
    lease_holder: 'replica-a',
    upstream_spec_revision: baseUpstream.spec_revision,
    dialect_plugin_snapshot: baseUpstream.warmup_dialect_plugin,
    error_detail: input.errorDetail,
  };
}

function makeSummary(
  lastAttempt: WarmupAttempt | null,
  nextOffsetSecs: number | null,
  recentAttempts: WarmupAttempt[],
  recent7d: WarmupRecentSummary,
): WarmupSummary {
  return {
    upstream_id: UPSTREAM_ID,
    last_attempt: lastAttempt,
    recent_attempts: recentAttempts,
    next_scheduled_at_unix_secs:
      nextOffsetSecs == null ? null : NOW_SECS + nextOffsetSecs,
    recent_summary_7d: recent7d,
    dialect_plugin: baseUpstream.warmup_dialect_plugin,
  };
}

const healthyAttempts = [
  makeAttempt({
    id: 'healthy-1',
    offsetSecs: 900,
    outcome: 'success_fresh',
    reason: null,
    httpStatus: 200,
    errorDetail: null,
    idleSecs: 7_200,
  }),
  makeAttempt({
    id: 'healthy-2',
    offsetSecs: 18_000,
    outcome: 'success_redundant',
    reason: 'window_already_active',
    httpStatus: 200,
    errorDetail: null,
    idleSecs: null,
  }),
];

const degradedAttempts = [
  makeAttempt({
    id: 'degraded-1',
    offsetSecs: 600,
    outcome: 'transient_failure',
    reason: 'upstream_5xx',
    httpStatus: 529,
    errorDetail: 'overloaded_error: API temporarily overloaded',
    idleSecs: null,
    cycleKey: NOW_SECS - 600,
  }),
  makeAttempt({
    id: 'degraded-2',
    offsetSecs: 660,
    outcome: 'transient_failure',
    reason: 'upstream_5xx',
    httpStatus: 529,
    errorDetail: 'overloaded_error: API temporarily overloaded',
    idleSecs: null,
    cycleKey: NOW_SECS - 600,
  }),
  makeAttempt({
    id: 'degraded-3',
    offsetSecs: 720,
    outcome: 'success_fresh',
    reason: null,
    httpStatus: 200,
    errorDetail: null,
    idleSecs: 7_200,
    cycleKey: NOW_SECS - 600,
  }),
  ...healthyAttempts,
];

const downAttempts = [0, 1, 2, 3, 4].map((index) =>
  makeAttempt({
    id: `down-${index + 1}`,
    offsetSecs: 300 + index * 600,
    outcome: 'permanent_failure',
    reason: 'auth_failed',
    httpStatus: 401,
    errorDetail: 'authentication_error: invalid OAuth credentials',
    idleSecs: null,
  }),
);

export const warmupFixtures = {
  healthy: {
    id: 'healthy',
    label: 'Healthy',
    upstream: baseUpstream,
    summary: makeSummary(healthyAttempts[0] ?? null, 1_800, healthyAttempts, {
      ...empty7d,
      success_fresh: 42,
      success_redundant: 8,
      skipped: 2,
    }),
    pluginName: 'subscription-launderer',
  },
  degraded: {
    id: 'degraded',
    label: 'Degraded',
    upstream: { ...baseUpstream, warmup_dialect_plugin: null },
    summary: makeSummary(degradedAttempts[0] ?? null, 300, degradedAttempts, {
      ...empty7d,
      success_fresh: 38,
      success_redundant: 5,
      transient_failure: 4,
      skipped: 3,
    }),
    pluginName: null,
  },
  down: {
    id: 'down',
    label: 'Down',
    upstream: { ...baseUpstream, warmup_dialect_plugin: null },
    summary: makeSummary(downAttempts[0] ?? null, 600, downAttempts, {
      ...empty7d,
      success_fresh: 30,
      success_redundant: 4,
      transient_failure: 2,
      permanent_failure: 8,
      skipped: 1,
    }),
    pluginName: null,
  },
  warmupPaused: {
    id: 'warmupPaused',
    label: 'Warmup Paused',
    upstream: { ...baseUpstream, warmup_enabled: false },
    summary: makeSummary(healthyAttempts[0] ?? null, null, healthyAttempts, {
      ...empty7d,
      success_fresh: 20,
      success_redundant: 3,
    }),
    pluginName: 'subscription-launderer',
  },
  upstreamDisabled: {
    id: 'upstreamDisabled',
    label: 'Upstream Disabled',
    upstream: { ...baseUpstream, enabled: false },
    summary: makeSummary(healthyAttempts[0] ?? null, null, healthyAttempts, {
      ...empty7d,
      success_fresh: 15,
      success_redundant: 2,
      transient_failure: 1,
      skipped: 1,
    }),
    pluginName: 'subscription-launderer',
  },
} satisfies Record<WarmupScenarioId, WarmupMockFixture>;
