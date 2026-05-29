import { useCallback } from 'react';
import { ApiError, deleteJson, getJson, postJson, putJson } from '../api';
import {
  ConflictError,
  type OAuthCompleteRequest,
  type OAuthCompleteResponse,
  type OAuthStartResponse,
  type UpstreamCreateBody,
  type UpstreamListResponse,
  type UpstreamResponse,
  type UpstreamUpdateBody,
} from '../types/v1';

function etag(revision: number): string {
  return `W/"${revision}"`;
}

async function handleConflict<T>(
  err: unknown,
  fetchLatest: () => Promise<T>,
): Promise<never> {
  if (err instanceof ApiError && err.status === 409) {
    const latest = await fetchLatest().catch(() => {
      throw err;
    });
    throw new ConflictError('Conflict detected', latest);
  }
  throw err;
}

export function useList() {
  return useCallback(async () => {
    return getJson<UpstreamListResponse>('/admin/v1/upstreams');
  }, []);
}

export function useGet() {
  return useCallback(async (id: string) => {
    return getJson<UpstreamResponse>(`/admin/v1/upstreams/${id}`);
  }, []);
}

export function useCreate() {
  return useCallback(async (body: UpstreamCreateBody) => {
    return postJson<UpstreamResponse, UpstreamCreateBody>(
      '/admin/v1/upstreams',
      body,
    );
  }, []);
}

export function useUpdate() {
  return useCallback(
    async (id: string, ifMatch: number, body: UpstreamUpdateBody) => {
      if (ifMatch === undefined) throw new Error('ifMatch is required');
      try {
        return await putJson<UpstreamResponse, UpstreamUpdateBody>(
          `/admin/v1/upstreams/${id}`,
          body,
          { headers: { 'If-Match': etag(ifMatch) } },
        );
      } catch (err) {
        return handleConflict(err, () =>
          getJson<UpstreamResponse>(`/admin/v1/upstreams/${id}`),
        );
      }
    },
    [],
  );
}

export function useEnable() {
  return useCallback(async (id: string, ifMatch: number) => {
    if (ifMatch === undefined) throw new Error('ifMatch is required');
    try {
      return await postJson<UpstreamResponse, Record<string, never>>(
        `/admin/v1/upstreams/${id}/enable`,
        {},
        { headers: { 'If-Match': etag(ifMatch) } },
      );
    } catch (err) {
      return handleConflict(err, () =>
        getJson<UpstreamResponse>(`/admin/v1/upstreams/${id}`),
      );
    }
  }, []);
}

export function useDisable() {
  return useCallback(async (id: string, ifMatch: number) => {
    if (ifMatch === undefined) throw new Error('ifMatch is required');
    try {
      return await postJson<UpstreamResponse, Record<string, never>>(
        `/admin/v1/upstreams/${id}/disable`,
        {},
        { headers: { 'If-Match': etag(ifMatch) } },
      );
    } catch (err) {
      return handleConflict(err, () =>
        getJson<UpstreamResponse>(`/admin/v1/upstreams/${id}`),
      );
    }
  }, []);
}

export function useDelete() {
  return useCallback(async (id: string, ifMatch: number) => {
    if (ifMatch === undefined) throw new Error('ifMatch is required');
    try {
      return await deleteJson<void>(`/admin/v1/upstreams/${id}`, {
        headers: { 'If-Match': etag(ifMatch) },
      });
    } catch (err) {
      return handleConflict(err, () =>
        getJson<UpstreamResponse>(`/admin/v1/upstreams/${id}`),
      );
    }
  }, []);
}

export function useOAuthStart() {
  return useCallback(async (id: string) => {
    return postJson<OAuthStartResponse, Record<string, never>>(
      `/admin/v1/upstreams/${id}/oauth/start`,
      {},
    );
  }, []);
}

export function useOAuthComplete() {
  return useCallback(async (id: string, body: OAuthCompleteRequest) => {
    return postJson<OAuthCompleteResponse, OAuthCompleteRequest>(
      `/admin/v1/upstreams/${id}/oauth/complete`,
      body,
    );
  }, []);
}
