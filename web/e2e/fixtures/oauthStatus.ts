import type { UpstreamOAuthStatusResponse } from '../../src/lib/api';

/** Active refreshing OAuth status payload matching `UpstreamOAuthStatusResponse`. */
export function activeAnthropicOAuthStatus(
  upstreamId: string,
  nowSecs = Math.floor(Date.now() / 1000),
): UpstreamOAuthStatusResponse {
  return {
    upstream_id: upstreamId,
    kind: 'anthropic_oauth',
    has_credentials: true,
    status: 'active',
    expires_at_unix_secs: nowSecs + 3600,
    refresh_token_present: true,
    refresh_token_expires_at_unix_secs: nowSecs + 30 * 24 * 60 * 60,
    mode: 'refreshing',
    can_refresh: true,
    scopes: ['user:inference'],
    account_email: 'operator@example.com',
    account_name: 'E2E Operator',
    account_id: 'acct_e2e_alpha',
    plan: 'max',
    plan_name: 'Claude Max',
    project_id: null,
  };
}
