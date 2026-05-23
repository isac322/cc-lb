import { useState, useEffect, useCallback } from 'react';
import { getJson, postJson, ApiKeyRecord, IssueKeyResponse, RevokeKeyResponse } from '../api';

export function usePrincipalKeys(principalId: string, mock?: boolean) {
  const [keys, setKeys] = useState<ApiKeyRecord[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  const fetchKeys = useCallback(async () => {
    if (mock) {
      if (principalId === 'alice') {
        setKeys([
          {
            key_id: 'key_1',
            label: 'Production Key',
            issued_at_unix_secs: Math.floor(Date.now() / 1000) - 86400 * 30,
            revoked_at_unix_secs: null,
          },
          {
            key_id: 'key_2',
            label: 'Development Key',
            issued_at_unix_secs: Math.floor(Date.now() / 1000) - 86400 * 7,
            revoked_at_unix_secs: null,
          },
          {
            key_id: 'key_3',
            label: 'Old Key',
            issued_at_unix_secs: Math.floor(Date.now() / 1000) - 86400 * 60,
            revoked_at_unix_secs: Math.floor(Date.now() / 1000) - 86400 * 30,
          },
        ]);
      } else {
        setKeys([]);
      }
      setIsLoading(false);
      setError(null);
      return;
    }

    try {
      setIsLoading(true);
      setError(null);
      const res = await getJson<{ keys: ApiKeyRecord[] }>(`/admin/principals/${principalId}/keys`);
      setKeys(res.keys);
    } catch (err) {
      setError(err instanceof Error ? err : new Error(String(err)));
    } finally {
      setIsLoading(false);
    }
  }, [principalId, mock]);

  useEffect(() => {
    fetchKeys();
  }, [fetchKeys]);

  const issueKey = async (label?: string): Promise<IssueKeyResponse> => {
    if (mock) {
      const newKey: IssueKeyResponse = {
        principal_id: principalId,
        key_id: `key_mock_${Date.now()}`,
        plaintext_key: `mock-issued-key-${Math.random().toString(36).substring(2, 15)}`,
        issued_at_unix_secs: Math.floor(Date.now() / 1000),
      };
      setKeys((prev) => [
        ...prev,
        {
          key_id: newKey.key_id,
          label: label || null,
          issued_at_unix_secs: newKey.issued_at_unix_secs,
          revoked_at_unix_secs: null,
        },
      ]);
      return newKey;
    }
    const res = await postJson<IssueKeyResponse, { label?: string }>(`/admin/principals/${principalId}/keys`, { label });
    await fetchKeys();
    return res;
  };

  const revokeKey = async (keyId: string): Promise<RevokeKeyResponse> => {
    if (mock) {
      const res: RevokeKeyResponse = {
        key_id: keyId,
        revoked_at_unix_secs: Math.floor(Date.now() / 1000),
      };
      setKeys((prev) =>
        prev.map((k) => (k.key_id === keyId ? { ...k, revoked_at_unix_secs: res.revoked_at_unix_secs } : k))
      );
      return res;
    }
    const res = await postJson<RevokeKeyResponse, Record<string, never>>(`/admin/principals/${principalId}/keys/${keyId}/revoke`, {});
    await fetchKeys();
    return res;
  };

  return { keys, refresh: fetchKeys, isLoading, error, issueKey, revokeKey };
}
