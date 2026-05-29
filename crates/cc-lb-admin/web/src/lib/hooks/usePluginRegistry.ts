import { useCallback } from 'react';
import { ApiError, deleteJson, getJson, postJson } from '../api';
import { getAdminToken } from '../auth';
import type {
  ChainListResponse,
  InsertChainBody,
  PluginChainEntry,
  RegistryListResponse,
  ReorderBody,
  UploadResponse,
} from '../types/v1';

function etag(revision: number): string {
  return `W/"${revision}"`;
}

export function useRegistryList() {
  return useCallback(async () => {
    return getJson<RegistryListResponse>('/admin/v1/plugins/registry');
  }, []);
}

export function useUploadWasm() {
  return useCallback(async (formData: FormData) => {
    const token = getAdminToken();
    const headers = new Headers();
    if (token) {
      headers.set('Authorization', `Bearer ${token}`);
    }
    const res = await fetch('/admin/v1/plugins/wasm', {
      method: 'POST',
      headers,
      body: formData,
    });
    if (!res.ok) {
      let body: unknown = null;
      let code: string | null = null;
      let message = res.statusText;
      try {
        body = await res.json();
        if (body && typeof body === 'object' && 'code' in body) {
          code = String((body as Record<string, unknown>).code);
        }
        if (body && typeof body === 'object' && 'message' in body) {
          message = String((body as Record<string, unknown>).message);
        }
      } catch (err) {
        console.warn(
          'plugin registry upload error response parse failed:',
          err,
        );
      }
      if (res.status === 401) {
        code = 'unauthorized';
      }
      throw new ApiError(res.status, code, body, message);
    }
    return res.json() as Promise<UploadResponse>;
  }, []);
}

export function useDeleteRegistryEntry() {
  return useCallback(async (id: string, ifMatch: number) => {
    if (ifMatch === undefined) throw new Error('ifMatch is required');
    return await deleteJson<void>(`/admin/v1/plugins/registry/${id}`, {
      headers: { 'If-Match': etag(ifMatch) },
    });
  }, []);
}

export function useChainList() {
  return useCallback(async (principalId: string, slot: string) => {
    return getJson<ChainListResponse>(
      `/admin/v1/principals/${principalId}/plugin-chain?slot=${slot}`,
    );
  }, []);
}

export function useChainInsert() {
  return useCallback(async (principalId: string, body: InsertChainBody) => {
    return postJson<PluginChainEntry, InsertChainBody>(
      `/admin/v1/principals/${principalId}/plugin-chain`,
      body,
    );
  }, []);
}

export function useChainReorder() {
  return useCallback(async (principalId: string, body: ReorderBody) => {
    return postJson<ChainListResponse, ReorderBody>(
      `/admin/v1/principals/${principalId}/plugin-chain/reorder`,
      body,
    );
  }, []);
}

export function useChainRebalance() {
  return useCallback(async (principalId: string, slot: string) => {
    return postJson<ChainListResponse, Record<string, never>>(
      `/admin/v1/principals/${principalId}/plugin-chain/rebalance?slot=${slot}`,
      {},
    );
  }, []);
}

export function useChainDelete() {
  return useCallback(async (id: string, ifMatch: number) => {
    if (ifMatch === undefined) throw new Error('ifMatch is required');
    return await deleteJson<void>(`/admin/v1/plugin-chain-entries/${id}`, {
      headers: { 'If-Match': etag(ifMatch) },
    });
  }, []);
}

export function useGcOrphaned() {
  return useCallback(async () => {
    return postJson<
      { removed: string[]; count: number },
      Record<string, never>
    >('/admin/v1/plugins/wasm/gc', {});
  }, []);
}
