// Complete, typed TanStack Query hook results for tests that stub a hook.
// Each builder returns one member of the result union, so a change to the
// hook's data, variables or result contract fails the typecheck instead of
// slipping past a cast.
import {
  type MutationFunctionContext,
  QueryClient,
  type UseMutationResult,
  type UseQueryResult,
} from '@tanstack/react-query';
import { vi } from 'vitest';

/** The context a real mutation passes to per-call callbacks. */
export const mutationContext: MutationFunctionContext = {
  client: new QueryClient(),
  meta: undefined,
};

type MutationFns<TData, TVariables> = Partial<
  Pick<
    UseMutationResult<TData, Error, TVariables>,
    'mutate' | 'mutateAsync' | 'reset'
  >
>;

function mutationBase<TData, TVariables>(fns: MutationFns<TData, TVariables>) {
  return {
    mutate: fns.mutate ?? (() => {}),
    mutateAsync: fns.mutateAsync ?? (() => new Promise<TData>(() => {})),
    reset: fns.reset ?? (() => {}),
    context: undefined,
    failureCount: 0,
    failureReason: null,
    isPaused: false,
    submittedAt: 0,
  };
}

/** A mutation that has not run. */
export function idleMutation<TData, TVariables>(
  fns: MutationFns<TData, TVariables> = {},
): UseMutationResult<TData, Error, TVariables> {
  return {
    ...mutationBase(fns),
    data: undefined,
    variables: undefined,
    error: null,
    isError: false,
    isIdle: true,
    isPending: false,
    isSuccess: false,
    status: 'idle',
  };
}

/** A mutation in flight with `variables`. */
export function pendingMutation<TData, TVariables>(
  variables: NoInfer<TVariables>,
  fns: MutationFns<TData, TVariables> = {},
): UseMutationResult<TData, Error, TVariables> {
  return {
    ...mutationBase(fns),
    submittedAt: 1,
    data: undefined,
    variables,
    error: null,
    isError: false,
    isIdle: false,
    isPending: true,
    isSuccess: false,
    status: 'pending',
  };
}

/** A mutation whose last run with `variables` failed with `error`. */
export function failedMutation<TData, TVariables>(
  error: Error,
  variables: NoInfer<TVariables>,
  fns: MutationFns<TData, TVariables> = {},
): UseMutationResult<TData, Error, TVariables> {
  return {
    ...mutationBase(fns),
    submittedAt: 1,
    failureCount: 1,
    failureReason: error,
    data: undefined,
    variables,
    error,
    isError: true,
    isIdle: false,
    isPending: false,
    isSuccess: false,
    status: 'error',
  };
}

type QueryFns<TData> = Partial<
  Pick<UseQueryResult<TData, Error>, 'refetch' | 'isFetching'>
>;

function queryBase<TData>(fns: QueryFns<TData>) {
  return {
    refetch: fns.refetch ?? vi.fn(),
    dataUpdatedAt: 0,
    errorUpdatedAt: 0,
    failureCount: 0,
    failureReason: null,
    errorUpdateCount: 0,
    isFetched: true,
    isFetchedAfterMount: true,
    isInitialLoading: false,
    isPaused: false,
    isRefetching: false,
    isStale: false,
    isEnabled: true,
  };
}

/** A query whose first fetch is still running. */
export function pendingQuery<TData>(): UseQueryResult<TData, Error> {
  return {
    ...queryBase<TData>({}),
    isFetched: false,
    isFetchedAfterMount: false,
    isInitialLoading: true,
    data: undefined,
    error: null,
    isError: false,
    isPending: true,
    isLoading: true,
    isLoadingError: false,
    isRefetchError: false,
    isSuccess: false,
    isPlaceholderData: false,
    isFetching: true,
    fetchStatus: 'fetching',
    status: 'pending',
  };
}

/** A query holding `data`. */
export function successQuery<TData>(
  data: TData,
  fns: QueryFns<TData> = {},
): UseQueryResult<TData, Error> {
  const isFetching = fns.isFetching ?? false;
  return {
    ...queryBase(fns),
    isRefetching: isFetching,
    data,
    error: null,
    isError: false,
    isPending: false,
    isLoading: false,
    isLoadingError: false,
    isRefetchError: false,
    isSuccess: true,
    isPlaceholderData: false,
    isFetching,
    fetchStatus: isFetching ? 'fetching' : 'idle',
    status: 'success',
  };
}

/** A query whose first fetch failed with `error`. */
export function failedQuery<TData>(
  error: Error,
  fns: QueryFns<TData> = {},
): UseQueryResult<TData, Error> {
  const isFetching = fns.isFetching ?? false;
  return {
    ...queryBase(fns),
    errorUpdatedAt: 1,
    errorUpdateCount: 1,
    failureCount: 1,
    failureReason: error,
    isRefetching: isFetching,
    data: undefined,
    error,
    isError: true,
    isPending: false,
    isLoading: false,
    isLoadingError: true,
    isRefetchError: false,
    isSuccess: false,
    isPlaceholderData: false,
    isFetching,
    fetchStatus: isFetching ? 'fetching' : 'idle',
    status: 'error',
  };
}
