import {
  type UseQueryOptions,
  type UseQueryResult,
  useQuery,
} from '@tanstack/react-query';
import { useEffect, useRef } from 'react';
import { useVisibility } from './visibilityManager';

export type PolledDataResult<TData, TError> = UseQueryResult<TData, TError>;

export function usePolledData<
  TQueryFnData = unknown,
  TError = Error,
  TData = TQueryFnData,
  TQueryKey extends readonly unknown[] = readonly unknown[],
>(
  options: UseQueryOptions<TQueryFnData, TError, TData, TQueryKey>,
  refetchInterval: number | false,
): PolledDataResult<TData, TError> {
  const visibility = useVisibility();
  const isHidden = visibility.gracePeriodElapsed || !visibility.visible;
  const wasHiddenRef = useRef(isHidden);

  const query = useQuery({
    ...options,
    refetchInterval: isHidden ? false : refetchInterval,
  });

  useEffect(() => {
    if (wasHiddenRef.current && !isHidden && query.isEnabled) {
      query.refetch();
    }
    wasHiddenRef.current = isHidden;
  }, [isHidden, query.isEnabled, query.refetch]);

  return query;
}
