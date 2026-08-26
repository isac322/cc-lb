import {
  keepPreviousData,
  MutationCache,
  QueryCache,
  QueryClient,
} from '@tanstack/react-query';
import { toast } from 'sonner';
import { ApiError } from './api';

function isSilencedError(error: unknown): boolean {
  // 401 is handled by AuthRequiredGate via the cclb:auth-required event;
  // surfacing a toast for it would be redundant.
  return error instanceof ApiError && error.status === 401;
}

function messageOf(error: unknown): string {
  if (error instanceof ApiError) {
    if (
      error.status === 428 ||
      (error.status === 409 && error.code === 'stale_revision')
    ) {
      return 'Concurrent update detected — please retry';
    }
    return error.message || `Request failed (${error.status})`;
  }
  if (error instanceof Error) {
    return error.message;
  }
  return String(error);
}

export const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 1,
      refetchOnWindowFocus: false,
      staleTime: 10_000,
      gcTime: 5 * 60_000,
      // Anti-flicker default: queryKey changes keep the previous data on
      // screen instead of flipping `isLoading` to true. Removing this
      // reintroduces dashboard-wide flicker on range/upstream toggles.
      placeholderData: keepPreviousData,
    },
    mutations: { retry: 0 },
  },
  queryCache: new QueryCache({
    onError: (error) => {
      if (isSilencedError(error)) return;
      toast.error(messageOf(error));
    },
  }),
  mutationCache: new MutationCache({
    onError: (error, _vars, _ctx, mutation) => {
      if (isSilencedError(error)) return;
      // Mutations that render their own error next to the offending input opt
      // out here. Per-call mutate(..., { onError }) callbacks are stored on
      // MutationObserver's private options and are invisible to this handler.
      if (mutation.options.meta?.inlineError === true) return;
      // If the mutation defines its own onError, assume it already handles UX.
      if (mutation.options.onError) return;
      toast.error(messageOf(error));
    },
  }),
});
