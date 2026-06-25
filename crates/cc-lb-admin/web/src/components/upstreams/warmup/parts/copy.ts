import type { WarmupOutcome, WarmupReason } from '../../../../lib/queries';

export const OUTCOME_LABEL: Record<WarmupOutcome, string> = {
  success_fresh: 'Fresh window',
  success_redundant: 'Already active',
  transient_failure: 'Transient',
  permanent_failure: 'Failed',
  skipped: 'Skipped',
};

export const OUTCOME_TONE: Record<
  WarmupOutcome,
  'ok' | 'warn' | 'danger' | 'neutral'
> = {
  success_fresh: 'ok',
  success_redundant: 'warn',
  transient_failure: 'warn',
  permanent_failure: 'danger',
  skipped: 'neutral',
};

// Short narrative used in card body — "Last attempt {RelativeTime}: {desc}"
export const OUTCOME_DESCRIPTION: Record<WarmupOutcome, string> = {
  success_fresh: 'started a fresh 5h window',
  success_redundant: 'window was already active — no new cycle started',
  transient_failure: 'failed transiently — background loop will retry',
  permanent_failure: 'failed — operator action required',
  skipped: 'skipped',
};

export const REASON_LABEL: Record<WarmupReason, string> = {
  window_already_active: '5h window already active',
  http_429_missing_cycle_key: '429 without anthropic-ratelimit-* headers',
  upstream_5xx: 'Upstream 5xx',
  network_error: 'Network error',
  request_timeout: 'Request timeout',
  request_build_failed: 'Could not build request',
  oauth_refresh_failed: 'OAuth refresh failed',
  credential_decrypt_failed: 'Credential decrypt failed',
  auth_failed: 'Auth rejected (401)',
  forbidden: 'Forbidden (403)',
  bad_request: 'Bad request (400)',
  not_found: 'Not found (404)',
  dialect_plugin_failed: 'Shape plugin error',
  dialect_plugin_transient: 'Shape plugin transient error',
  oauth_credentials_missing: 'No OAuth credentials',
  lease_held: 'Lease held by another replica',
  upstream_disabled: 'Upstream is disabled',
  upstream_deleted: 'Upstream was deleted',
};
