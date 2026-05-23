import { useState, useEffect, useCallback } from 'react';
import { getJson, CredentialsResponse, OAuthStatusResponse, OAuthCredentialStatus } from '../api';

export interface MergedCredentialRow {
  principal_id: string;
  provider: string;
  kind: string;
  identity: string;
  associated_principals: string[];
  has_credentials: boolean;
  expires_at_unix_secs: number | null;
  status: string;
  refresh_token_present?: boolean;
  last_updated_unix_secs?: number | null;
}

export function useCredentialStatus() {
  const [rows, setRows] = useState<MergedCredentialRow[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);
  const [observed, setObserved] = useState(false);

  const refresh = useCallback(async () => {
    setIsLoading(true);
    setError(null);
    try {
      const isMock = new URLSearchParams(window.location.search).get('mock') === '1';
      if (isMock) {
        const now = Math.floor(Date.now() / 1000);
        setRows([
          {
            principal_id: 'user1',
            provider: 'anthropic_oauth',
            kind: 'oauth',
            identity: 'user1/anthropic_oauth',
            associated_principals: ['user1'],
            has_credentials: true,
            expires_at_unix_secs: now + 3600,
            status: 'valid',
            refresh_token_present: true,
            last_updated_unix_secs: now - 86400,
          },
          {
            principal_id: 'user2',
            provider: 'anthropic_oauth',
            kind: 'oauth',
            identity: 'user2/anthropic_oauth',
            associated_principals: ['user2'],
            has_credentials: true,
            expires_at_unix_secs: now + 120, // expiring soon
            status: 'expiring_soon',
            refresh_token_present: false,
            last_updated_unix_secs: now - 86400,
          },
          {
            principal_id: 'user3',
            provider: 'api_key',
            kind: 'api_key',
            identity: 'user3/api_key',
            associated_principals: ['user3'],
            has_credentials: false,
            expires_at_unix_secs: null,
            status: 'revoked',
          },
          {
            principal_id: 'user4',
            provider: 'api_key',
            kind: 'api_key',
            identity: 'user4/api_key',
            associated_principals: ['user4'],
            has_credentials: true,
            expires_at_unix_secs: null,
            status: 'valid',
            // Fake plaintext field to test redaction if it were rendered (though we don't render arbitrary payload here, just to be safe)
            // @ts-expect-error test redaction
            plaintext: "mock-plaintext-redacted"
          }
        ]);
        setObserved(true);
        setIsLoading(false);
        return;
      }

      const [credsRes, oauthRes] = await Promise.all([
        getJson<CredentialsResponse>('/admin/credentials'),
        getJson<OAuthStatusResponse>('/admin/oauth/status').catch(() => ({ credentials: [], observed: false }))
      ]);

      const merged: MergedCredentialRow[] = [];
      const oauthMap = new Map<string, OAuthCredentialStatus>();
      
      oauthRes.credentials.forEach(c => {
        oauthMap.set(`${c.principal_id}/${c.provider}`, c);
      });

      credsRes.credentials.forEach(c => {
        const oauth = oauthMap.get(`${c.principal_id}/${c.provider}`);
        merged.push({
          ...c,
          refresh_token_present: oauth?.refresh_token_present,
          last_updated_unix_secs: oauth?.last_updated_unix_secs,
          // Prefer OAuth status if available as it might have more detail
          status: oauth ? oauth.status : c.status,
          expires_at_unix_secs: oauth ? oauth.expires_at_unix_secs : c.expires_at_unix_secs,
        });
      });

      setRows(merged);
      setObserved(credsRes.observed || oauthRes.observed);
    } catch (err) {
      setError(err instanceof Error ? err : new Error(String(err)));
    } finally {
      setIsLoading(false);
    }
  }, []);

  useEffect(() => {
    refresh();
    const interval = setInterval(refresh, 30000);
    return () => clearInterval(interval);
  }, [refresh]);

  return { rows, observed, isLoading, error, refresh };
}
