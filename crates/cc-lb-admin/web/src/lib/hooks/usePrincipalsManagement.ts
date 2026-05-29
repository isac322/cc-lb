import { useCallback, useEffect, useState } from 'react';
import { getJson, type PrincipalRuntimeSpec } from '../api';

export interface PrincipalWithId extends PrincipalRuntimeSpec {
  id: string;
}

export function usePrincipalsManagement(mock?: boolean) {
  const [principalList, setPrincipalList] = useState<PrincipalWithId[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  const fetchPrincipals = useCallback(async () => {
    if (mock) {
      setPrincipalList([
        {
          id: 'alice',
          disabled: false,
          allowed_models: ['claude-3-5-sonnet'],
          quotas: {
            default_window_secs: 60,
            default_requests_per_window: 100,
            default_input_tokens: 10000,
            default_output_tokens: 2000,
          },
        },
        {
          id: 'bob',
          disabled: false,
          allowed_models: ['claude-3-haiku'],
        },
        {
          id: 'charlie',
          disabled: true,
          allowed_models: ['*'],
        },
      ]);
      setIsLoading(false);
      setError(null);
      return;
    }

    try {
      setIsLoading(true);
      setError(null);

      const response = await getJson<{ principals: Array<PrincipalRuntimeSpec & { id: string; enabled?: boolean }> }>(
        '/admin/v1/principals',
      );

      const list = response.principals.map((principal) => ({
        ...principal,
        disabled: principal.enabled === false,
      }));

      setPrincipalList(list);
    } catch (err) {
      setError(err instanceof Error ? err : new Error(String(err)));
    } finally {
      setIsLoading(false);
    }
  }, [mock]);

  useEffect(() => {
    fetchPrincipals();
  }, [fetchPrincipals]);

  return {
    principals: principalList,
    refresh: fetchPrincipals,
    isLoading,
    error,
  };
}
