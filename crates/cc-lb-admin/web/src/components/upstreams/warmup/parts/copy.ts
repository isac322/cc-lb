import type {
  WarmupAttemptStatus,
  WarmupPermanentFailureReason,
  WarmupSkipReason,
  WarmupSuccessReason,
  WarmupTransientFailureReason,
} from '../../../../lib/queries';

export const OUTCOME_LABEL: Record<WarmupAttemptStatus, string> = {
  success: 'Success',
  skipped: 'Skipped',
  transient_failure: 'Retrying',
  permanent_failure: 'Failed',
};

export const REASON_LABEL: Record<
  | WarmupSuccessReason
  | WarmupSkipReason
  | WarmupTransientFailureReason
  | WarmupPermanentFailureReason,
  string
> = {
  cycle_advanced: 'Cycle key advanced',
  window_already_active: '5h window already active',
  seven_day_quota_exhausted: '7-day quota exhausted',
  upstream_disabled: 'Upstream is disabled',
  upstream_deleted: 'Upstream was deleted',
  rate_limited_cycle_key_missing: '429 without anthropic-ratelimit-* headers',
  upstream_5xx: 'Upstream 5xx',
  network_error: 'Network error',
  request_timeout: 'Request timeout',
  dialect_plugin_transient: 'Shape plugin transient error',
  oauth_credentials_missing: 'No OAuth credentials',
  request_build_failed: 'Could not build request',
  oauth_refresh_failed: 'OAuth refresh failed',
  credential_decrypt_failed: 'Credential decrypt failed',
  auth_failed: 'Auth rejected (401)',
  forbidden: 'Forbidden (403)',
  bad_request: 'Bad request (400)',
  not_found: 'Not found (404)',
  dialect_plugin_failed: 'Shape plugin error',
};
