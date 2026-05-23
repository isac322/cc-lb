import { useCallback, useEffect, useState } from 'react';
import { getJson, type PrincipalSpec } from '../api';

export interface PrincipalWithId extends PrincipalSpec {
  id: string;
}

export function usePrincipalsManagement(mock?: boolean) {
  const [principals, setPrincipals] = useState<PrincipalWithId[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  const fetchPrincipals = useCallback(async () => {
    if (mock) {
      setPrincipals([
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

      const [current, draftRes] = await Promise.all([
        getJson<Record<string, unknown>>('/admin/config/current'),
        getJson<Record<string, unknown>>('/admin/config/draft'),
      ]);

      const config = (draftRes.draft as Record<string, unknown>) || current;
      const principalsMap =
        (config.principals as Record<string, unknown>) || {};

      const list = Object.entries(principalsMap).map(
        ([id, spec]: [string, unknown]) => ({
          id,
          ...(spec as PrincipalSpec),
        }),
      );

      setPrincipals(list);
    } catch (err) {
      setError(err instanceof Error ? err : new Error(String(err)));
    } finally {
      setIsLoading(false);
    }
  }, [mock]);

  useEffect(() => {
    fetchPrincipals();
  }, [fetchPrincipals]);

  return { principals, refresh: fetchPrincipals, isLoading, error };
}
