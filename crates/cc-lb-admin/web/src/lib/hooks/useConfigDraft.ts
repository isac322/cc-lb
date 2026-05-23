import { useCallback, useEffect, useState } from 'react';
import {
  ApiError,
  type ConfigDraftResponse,
  getJson,
  type PutConfigDraftRequest,
  type PutConfigDraftResponse,
  putJson,
} from '../api';
import { MOCK_DRAFT } from './mockData';

export function useConfigDraft() {
  const [draftData, setDraftData] = useState<ConfigDraftResponse | null>(null);
  const [error, setError] = useState<Error | null>(null);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [saveError, setSaveError] = useState<Error | null>(null);
  const [conflict, setConflict] = useState(false);

  const fetchDraft = useCallback(async () => {
    const isMock =
      new URLSearchParams(window.location.search).get('mock') === '1';
    if (isMock) {
      const isError =
        new URLSearchParams(window.location.search).get('dialog') ===
        'validate-error';
      setDraftData({
        ...MOCK_DRAFT,
        last_validation_error: isError
          ? 'Validation failed at listener.port: must be >= 1'
          : null,
      });
      setLoading(false);
      return;
    }

    try {
      const data = await getJson<ConfigDraftResponse>('/admin/config/draft');
      setDraftData(data);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err : new Error(String(err)));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchDraft();
  }, [fetchDraft]);

  const saveDraft = useCallback(
    async (newDraft: Record<string, unknown>, expectedRevision: number) => {
      const isMock =
        new URLSearchParams(window.location.search).get('mock') === '1';
      if (isMock) {
        setDraftData((prev) =>
          prev
            ? {
                ...prev,
                draft: newDraft,
                revision: expectedRevision + 1,
                saved_at_unix_secs: Math.floor(Date.now() / 1000),
              }
            : null,
        );
        return;
      }

      setSaving(true);
      setSaveError(null);
      setConflict(false);

      try {
        const req: PutConfigDraftRequest = {
          draft: newDraft,
          expected_revision: expectedRevision,
        };
        const res = await putJson<
          PutConfigDraftResponse,
          PutConfigDraftRequest
        >('/admin/config/draft', req);
        setDraftData((prev) =>
          prev
            ? {
                ...prev,
                draft: newDraft,
                revision: res.revision,
                saved_at_unix_secs: res.saved_at_unix_secs,
                last_validated_revision: null, // Server resets this
              }
            : null,
        );
      } catch (err) {
        if (
          err instanceof ApiError &&
          err.status === 409 &&
          err.code === 'stale_draft_revision'
        ) {
          // Handle conflict
          try {
            const latestData = await getJson<ConfigDraftResponse>(
              '/admin/config/draft',
            );
            // Simple merge: user's newDraft overrides latestData.draft at top level
            const mergedDraft = { ...latestData.draft, ...newDraft };
            const retryReq: PutConfigDraftRequest = {
              draft: mergedDraft,
              expected_revision: latestData.revision,
            };
            const retryRes = await putJson<
              PutConfigDraftResponse,
              PutConfigDraftRequest
            >('/admin/config/draft', retryReq);
            setDraftData({
              ...latestData,
              draft: mergedDraft,
              revision: retryRes.revision,
              saved_at_unix_secs: retryRes.saved_at_unix_secs,
              last_validated_revision: null,
            });
          } catch (retryErr) {
            if (retryErr instanceof ApiError && retryErr.status === 409) {
              setConflict(true);
            } else {
              setSaveError(
                retryErr instanceof Error
                  ? retryErr
                  : new Error(String(retryErr)),
              );
            }
          }
        } else {
          setSaveError(err instanceof Error ? err : new Error(String(err)));
        }
      } finally {
        setSaving(false);
      }
    },
    [],
  );

  return {
    draftData,
    error,
    loading,
    saving,
    saveError,
    conflict,
    saveDraft,
    fetchDraft,
  };
}
