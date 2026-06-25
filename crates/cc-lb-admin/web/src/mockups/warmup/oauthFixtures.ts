import type { OAuthMockFixture, OAuthScenarioId } from './types';

const NOW_SECS = Math.floor(Date.now() / 1000);
const BASE_SCOPES = ['read', 'write'] as const;

export const oauthFixtures = {
  connected: {
    id: 'connected',
    label: 'Connected',
    has_credentials: true,
    expires_at_unix_secs: NOW_SECS + 14_400,
    refresh_token_present: true,
    scopes: BASE_SCOPES,
    runtimeStatus: 'ok',
    isLoading: false,
  },
  expiringSoon: {
    id: 'expiringSoon',
    label: 'Expiring Soon',
    has_credentials: true,
    expires_at_unix_secs: NOW_SECS + 300,
    refresh_token_present: true,
    scopes: BASE_SCOPES,
    runtimeStatus: 'ok',
    isLoading: false,
  },
  expired: {
    id: 'expired',
    label: 'Expired',
    has_credentials: true,
    expires_at_unix_secs: NOW_SECS - 3_600,
    refresh_token_present: true,
    scopes: BASE_SCOPES,
    runtimeStatus: 'error',
    isLoading: false,
  },
  noRefresh: {
    id: 'noRefresh',
    label: 'No Refresh Token',
    has_credentials: true,
    expires_at_unix_secs: NOW_SECS + 3_600,
    refresh_token_present: false,
    scopes: BASE_SCOPES,
    runtimeStatus: 'ok',
    isLoading: false,
  },
  notConnected: {
    id: 'notConnected',
    label: 'Not Connected',
    has_credentials: false,
    expires_at_unix_secs: null,
    refresh_token_present: false,
    scopes: [],
    runtimeStatus: 'unknown',
    isLoading: false,
  },
  loading: {
    id: 'loading',
    label: 'Loading',
    has_credentials: false,
    expires_at_unix_secs: null,
    refresh_token_present: false,
    scopes: [],
    runtimeStatus: 'unknown',
    isLoading: true,
  },
} satisfies Record<OAuthScenarioId, OAuthMockFixture>;
