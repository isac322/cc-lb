import { useCallback, useState } from 'react';
import {
  type AllowedModelsRequest,
  type CreatePrincipalRequest,
  type PrincipalAllowedModelsResponse,
  type PrincipalMutationResponse,
  type PrincipalSpec,
  postJson,
  putJson,
  type QuotaOverrideRequest,
  type UpdatePrincipalRequest,
} from '../api';

export function useDraftPrincipals(mock?: boolean) {
  const [lastDraftRevision, setLastDraftRevision] = useState<number | null>(
    null,
  );

  const createPrincipal = useCallback(
    async (
      id: string,
      spec: PrincipalSpec,
    ): Promise<PrincipalMutationResponse> => {
      if (mock) {
        setLastDraftRevision(Date.now());
        return { revision: Date.now(), principal_id: id };
      }
      const res = await postJson<
        PrincipalMutationResponse,
        CreatePrincipalRequest
      >('/admin/principals', { id, spec });
      setLastDraftRevision(res.revision);
      return res;
    },
    [mock],
  );

  const updatePrincipal = useCallback(
    async (
      id: string,
      spec: PrincipalSpec,
    ): Promise<PrincipalMutationResponse> => {
      if (mock) {
        setLastDraftRevision(Date.now());
        return { revision: Date.now(), principal_id: id };
      }
      const res = await putJson<
        PrincipalMutationResponse,
        UpdatePrincipalRequest
      >(`/admin/principals/${id}`, { spec });
      setLastDraftRevision(res.revision);
      return res;
    },
    [mock],
  );

  const disablePrincipal = useCallback(
    async (id: string): Promise<PrincipalMutationResponse> => {
      if (mock) {
        setLastDraftRevision(Date.now());
        return { revision: Date.now(), principal_id: id };
      }
      const res = await postJson<
        PrincipalMutationResponse,
        Record<string, never>
      >(`/admin/principals/${id}/disable`, {});
      setLastDraftRevision(res.revision);
      return res;
    },
    [mock],
  );

  const enablePrincipal = useCallback(
    async (id: string): Promise<PrincipalMutationResponse> => {
      if (mock) {
        setLastDraftRevision(Date.now());
        return { revision: Date.now(), principal_id: id };
      }
      const res = await postJson<
        PrincipalMutationResponse,
        Record<string, never>
      >(`/admin/principals/${id}/enable`, {});
      setLastDraftRevision(res.revision);
      return res;
    },
    [mock],
  );

  const updateAllowedModels = useCallback(
    async (
      id: string,
      allowed_models: string[],
    ): Promise<PrincipalAllowedModelsResponse> => {
      if (mock) {
        setLastDraftRevision(Date.now());
        return { revision: Date.now(), principal_id: id, allowed_models };
      }
      const res = await putJson<
        PrincipalAllowedModelsResponse,
        AllowedModelsRequest
      >(`/admin/principals/${id}/allowed_models`, { allowed_models });
      setLastDraftRevision(res.revision);
      return res;
    },
    [mock],
  );

  const overrideQuota = useCallback(
    async (id: string, override: QuotaOverrideRequest): Promise<void> => {
      if (mock) {
        return;
      }
      await postJson<unknown, QuotaOverrideRequest>(
        `/admin/principals/${id}/quota/override`,
        override,
      );
    },
    [mock],
  );

  return {
    lastDraftRevision,
    createPrincipal,
    updatePrincipal,
    disablePrincipal,
    enablePrincipal,
    updateAllowedModels,
    overrideQuota,
  };
}
