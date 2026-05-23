import { useCallback, useEffect, useState } from 'react';
import {
  type CredentialEntry,
  type CredentialsResponse,
  getJson,
  postJson,
  type RevokeCredentialResponse,
  type RotateCredentialResponse,
} from '../api';

export function useCredentials(mock?: boolean) {
  const [credentials, setCredentials] = useState<CredentialEntry[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  const fetchCredentials = useCallback(async () => {
    if (mock) {
      setCredentials([
        {
          principal_id: 'alice',
          provider: 'anthropic_oauth',
          kind: 'oauth',
          identity: 'alice/anthropic_oauth',
          associated_principals: ['alice'],
          has_credentials: true,
          expires_at_unix_secs: Math.floor(Date.now() / 1000) + 3600,
          status: 'valid',
        },
        {
          principal_id: 'bob',
          provider: 'api_key',
          kind: 'api_key',
          identity: 'bob/api_key',
          associated_principals: ['bob'],
          has_credentials: true,
          expires_at_unix_secs: null,
          status: 'valid',
        },
        {
          principal_id: 'charlie',
          provider: 'api_key',
          kind: 'api_key',
          identity: 'charlie/api_key',
          associated_principals: ['charlie'],
          has_credentials: false,
          expires_at_unix_secs: null,
          status: 'revoked',
        },
      ]);
      setIsLoading(false);
      setError(null);
      return;
    }

    try {
      setIsLoading(true);
      setError(null);
      const res = await getJson<CredentialsResponse>('/admin/credentials');
      setCredentials(res.credentials);
    } catch (err) {
      setError(err instanceof Error ? err : new Error(String(err)));
    } finally {
      setIsLoading(false);
    }
  }, [mock]);

  useEffect(() => {
    fetchCredentials();
  }, [fetchCredentials]);

  const rotateCredential = async (
    principalId: string,
    provider: string,
  ): Promise<RotateCredentialResponse> => {
    if (mock) {
      const cred = credentials.find(
        (c) => c.principal_id === principalId && c.provider === provider,
      );
      if (cred?.kind === 'oauth') {
        throw new Error('rotate_unsupported');
      }
      const res: RotateCredentialResponse = {
        principal_id: principalId,
        provider,
        kind: 'api_key',
        new_key_id: `key_mock_${Date.now()}`,
        revoked_key_id: `key_old_${Date.now()}`,
        plaintext_key: `mock-issued-key-${Math.random().toString(36).substring(2, 15)}`,
        issued_at_unix_secs: Math.floor(Date.now() / 1000),
      };
      await fetchCredentials();
      return res;
    }
    const res = await postJson<RotateCredentialResponse, Record<string, never>>(
      `/admin/credentials/${principalId}/${provider}/rotate`,
      {},
    );
    await fetchCredentials();
    return res;
  };

  const revokeCredential = async (
    principalId: string,
    provider: string,
  ): Promise<RevokeCredentialResponse> => {
    if (mock) {
      const res: RevokeCredentialResponse = {
        principal_id: principalId,
        provider,
        kind:
          credentials.find(
            (c) => c.principal_id === principalId && c.provider === provider,
          )?.kind || 'api_key',
        revoked_keys: [`key_mock_${Date.now()}`],
      };
      setCredentials((prev) =>
        prev.map((c) => {
          if (c.principal_id === principalId && c.provider === provider) {
            return { ...c, has_credentials: false, status: 'revoked' };
          }
          return c;
        }),
      );
      return res;
    }
    const res = await postJson<RevokeCredentialResponse, Record<string, never>>(
      `/admin/credentials/${principalId}/${provider}/revoke`,
      {},
    );
    await fetchCredentials();
    return res;
  };

  return {
    credentials,
    refresh: fetchCredentials,
    isLoading,
    error,
    rotateCredential,
    revokeCredential,
  };
}
