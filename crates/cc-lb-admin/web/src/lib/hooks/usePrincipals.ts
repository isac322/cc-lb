import { useCallback } from 'react';
import { ApiError, deleteJson, getJson, postJson, putJson } from '../api';
import {
  type AllowedModelsBody,
  ConflictError,
  type CreatePrincipalBody,
  type PrincipalListResponse,
  type PrincipalResponse,
  type UpdatePrincipalBody,
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
    return getJson<PrincipalListResponse>('/admin/v1/principals');
  }, []);
}

export function useGet() {
  return useCallback(async (id: string) => {
    return getJson<PrincipalResponse>(`/admin/v1/principals/${id}`);
  }, []);
}

export function useCreate() {
  return useCallback(async (body: CreatePrincipalBody) => {
    return postJson<PrincipalResponse, CreatePrincipalBody>(
      '/admin/v1/principals',
      body,
    );
  }, []);
}

export function useUpdate() {
  return useCallback(
    async (id: string, ifMatch: number, body: UpdatePrincipalBody) => {
      if (ifMatch === undefined) throw new Error('ifMatch is required');
      try {
        return await putJson<PrincipalResponse, UpdatePrincipalBody>(
          `/admin/v1/principals/${id}`,
          body,
          { headers: { 'If-Match': etag(ifMatch) } },
        );
      } catch (err) {
        return handleConflict(err, () =>
          getJson<PrincipalResponse>(`/admin/v1/principals/${id}`),
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
      return await postJson<PrincipalResponse, Record<string, never>>(
        `/admin/v1/principals/${id}/enable`,
        {},
        { headers: { 'If-Match': etag(ifMatch) } },
      );
    } catch (err) {
      return handleConflict(err, () =>
        getJson<PrincipalResponse>(`/admin/v1/principals/${id}`),
      );
    }
  }, []);
}

export function useDisable() {
  return useCallback(async (id: string, ifMatch: number) => {
    if (ifMatch === undefined) throw new Error('ifMatch is required');
    try {
      return await postJson<PrincipalResponse, Record<string, never>>(
        `/admin/v1/principals/${id}/disable`,
        {},
        { headers: { 'If-Match': etag(ifMatch) } },
      );
    } catch (err) {
      return handleConflict(err, () =>
        getJson<PrincipalResponse>(`/admin/v1/principals/${id}`),
      );
    }
  }, []);
}

export function useDelete() {
  return useCallback(async (id: string, ifMatch: number) => {
    if (ifMatch === undefined) throw new Error('ifMatch is required');
    try {
      return await deleteJson<void>(`/admin/v1/principals/${id}`, {
        headers: { 'If-Match': etag(ifMatch) },
      });
    } catch (err) {
      return handleConflict(err, () =>
        getJson<PrincipalResponse>(`/admin/v1/principals/${id}`),
      );
    }
  }, []);
}

export function useAllowedModels() {
  return useCallback(async (id: string, ifMatch: number, models: string[]) => {
    if (ifMatch === undefined) throw new Error('ifMatch is required');
    try {
      return await putJson<PrincipalResponse, AllowedModelsBody>(
        `/admin/v1/principals/${id}/allowed_models`,
        { models, expected_revision: ifMatch },
      );
    } catch (err) {
      return handleConflict(err, () =>
        getJson<PrincipalResponse>(`/admin/v1/principals/${id}`),
      );
    }
  }, []);
}
