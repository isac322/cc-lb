import type { Upstream, WarmupSummary } from '../../lib/queries';

export const WARMUP_SCENARIO_IDS = [
  'healthy',
  'degraded',
  'down',
  'warmupPaused',
  'upstreamDisabled',
] as const;

export type WarmupScenarioId = (typeof WARMUP_SCENARIO_IDS)[number];

export interface WarmupMockFixture {
  readonly id: WarmupScenarioId;
  readonly label: string;
  readonly upstream: Upstream;
  readonly summary: WarmupSummary;
  readonly pluginName: string | null;
}

export function isWarmupScenarioId(value: string): value is WarmupScenarioId {
  return WARMUP_SCENARIO_IDS.some((id) => id === value);
}

export const OAUTH_SCENARIO_IDS = [
  'connected',
  'expiringSoon',
  'expired',
  'noRefresh',
  'notConnected',
  'loading',
] as const;

export type OAuthScenarioId = (typeof OAUTH_SCENARIO_IDS)[number];

export interface OAuthMockFixture {
  readonly id: OAuthScenarioId;
  readonly label: string;
  readonly has_credentials: boolean;
  readonly expires_at_unix_secs: number | null;
  readonly refresh_token_present: boolean;
  readonly scopes: readonly string[];
  readonly runtimeStatus: string;
  readonly isLoading: boolean;
}
