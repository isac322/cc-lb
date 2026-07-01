import { createFileRoute } from '@tanstack/react-router';
import {
  AlertTriangle,
  CheckCircle2,
  PlayCircle,
  Save,
  XCircle,
} from 'lucide-react';
import { useEffect, useMemo, useState } from 'react';
import { toast } from 'sonner';
import { BootEnvReadonly } from '../components/BootEnvReadonly';
import { SchemaForm } from '../components/SchemaForm';
import {
  Button,
  Card,
  CardBody,
  cx,
  Hint,
  INPUT_CLASS,
  PageContainer,
  Section,
  Skeleton,
  StatusBadge,
} from '../components/ui/primitives';
import { RelativeTime } from '../components/ui/RelativeTime';
import {
  useApplyConfig,
  useConfigCurrent,
  useConfigDraft,
  useConfigHistory,
  useConfigSchema,
  useSaveDraft,
  useValidateConfig,
} from '../lib/queries';

export const Route = createFileRoute('/config')({
  component: ConfigPage,
});

// Backend `summarize_restart_required` (crates/cc-lb-server/src/reload.rs)
// classifies every runtime field as restart-required because the proxy
// snapshots them into request-handling components at boot. Keep this list
// in lock-step with that source of truth. Hot-reloadable knobs are an
// explicit denylist below.
const RR_FIELDS = [
  'body',
  'timeouts',
  'downstream_auth',
  'api_keys',
  'scheduler',
  'observability',
  'oauth',
  'subscription_quota',
  'runtime.startup_handshake',
  'circuit_breaker',
  'bulkhead',
  'prompt_cache_shadow',
  'event_bus',
  'cluster',
  'limit_reservation_ttl',
];

function isRestartRequired(path: string) {
  return RR_FIELDS.some((rr) => path === rr || path.startsWith(`${rr}.`));
}

// Boot-only top-level keys + metadata field stripped before seeding a draft so
// the runtime overlay submitted to /admin/config/draft never carries values
// the server will silently override or reject.
const BOOT_ONLY_TOP_LEVEL = ['listener', 'tls', 'storage', 'aead', 'admin'];
const NON_RUNTIME_METADATA = ['effective_revision_unix_secs'];

function sanitizeForDraft(
  value: Record<string, unknown> | null | undefined,
): Record<string, unknown> {
  if (!value || typeof value !== 'object') {
    return {};
  }
  const cleaned: Record<string, unknown> = { ...value };
  for (const key of BOOT_ONLY_TOP_LEVEL) {
    delete cleaned[key];
  }
  for (const key of NON_RUNTIME_METADATA) {
    delete cleaned[key];
  }
  const runtime = cleaned.runtime;
  if (runtime && typeof runtime === 'object') {
    const runtimeCopy = { ...(runtime as Record<string, unknown>) };
    delete runtimeCopy.data_dir;
    cleaned.runtime = runtimeCopy;
  }
  return cleaned;
}

function ConfigPage() {
  const schemaQuery = useConfigSchema();
  const draftQuery = useConfigDraft();
  const currentQuery = useConfigCurrent();
  const historyQuery = useConfigHistory();

  const save = useSaveDraft();
  const validate = useValidateConfig();
  const apply = useApplyConfig();

  const [draftState, setDraftState] = useState<Record<string, any> | null>(
    null,
  );
  const [search, setSearch] = useState('');
  const [activeTab, setActiveTab] = useState<'editor' | 'history'>('editor');

  useEffect(() => {
    if (draftQuery.data && !draftState) {
      const seed = draftQuery.data.draft ?? currentQuery.data ?? {};
      setDraftState(
        sanitizeForDraft(seed as Record<string, unknown>) as Record<
          string,
          any
        >,
      );
    }
  }, [draftQuery.data, currentQuery.data, draftState]);

  const isDirty = useMemo(() => {
    if (!draftQuery.data || !draftState) return false;
    const base = sanitizeForDraft(
      (draftQuery.data.draft ?? currentQuery.data ?? {}) as Record<
        string,
        unknown
      >,
    );
    return JSON.stringify(base) !== JSON.stringify(draftState);
  }, [draftQuery.data, currentQuery.data, draftState]);

  const schema = schemaQuery.data?.schema as any;
  const topLevelProps = schema?.properties || {};

  const filteredKeys = Object.keys(topLevelProps).filter((k) => {
    if (['listener', 'tls', 'storage', 'aead', 'admin'].includes(k))
      return false;
    if (!search) return true;
    return k.toLowerCase().includes(search.toLowerCase());
  });

  const draftRevision = draftQuery.data?.revision ?? null;
  const lastValidatedRevision =
    draftQuery.data?.last_validated_revision ?? null;
  const lastValidationError = draftQuery.data?.last_validation_error ?? null;
  const canApply =
    lastValidatedRevision != null &&
    lastValidationError == null &&
    draftRevision != null &&
    lastValidatedRevision === draftRevision &&
    !isDirty;

  const handleSave = () => {
    if (!draftState) return;
    save.mutate(
      { draft: draftState, expected_revision: draftRevision ?? 0 },
      { onSuccess: () => toast.success('Draft saved') },
    );
  };

  const handleValidate = () => {
    validate.mutate(draftRevision ?? 0, {
      onSuccess: (r) =>
        toast.success(r.valid ? 'Draft valid' : `Invalid: ${r.error}`),
    });
  };

  const handleApply = () => {
    apply.mutate(lastValidatedRevision ?? 0, {
      onSuccess: (r) => toast.success(`Applied revision ${r.applied_revision}`),
    });
  };

  const handleDiscard = () => {
    setDraftState(
      sanitizeForDraft(
        (draftQuery.data?.draft ?? currentQuery.data ?? {}) as Record<
          string,
          unknown
        >,
      ) as Record<string, any>,
    );
    toast.success('Changes discarded');
  };

  // Check if any RR fields are modified
  const hasRRChanges = useMemo(() => {
    if (!draftState || !currentQuery.data) return false;
    // A simple check: just see if any top-level RR field differs.
    // For a deep check, we'd need a diffing function.
    // For now, we'll just check if the stringified RR sections differ.
    return RR_FIELDS.some((rr) => {
      const parts = rr.split('.');
      let curr: any = currentQuery.data;
      let dft: any = draftState;
      for (const p of parts) {
        curr = curr?.[p];
        dft = dft?.[p];
      }
      return JSON.stringify(curr) !== JSON.stringify(dft);
    });
  }, [draftState, currentQuery.data]);

  if (schemaQuery.isLoading || draftQuery.isLoading || currentQuery.isLoading) {
    return (
      <PageContainer>
        <Skeleton className="h-32" />
      </PageContainer>
    );
  }

  return (
    <PageContainer className="pb-32">
      <Section title="Configuration" subtitle="Manage runtime configuration">
        <BootEnvReadonly />

        <div className="flex gap-4 border-b border-subtle mb-4">
          <button
            className={cx(
              'pb-2 text-sm font-medium',
              activeTab === 'editor'
                ? 'text-accent border-b-2 border-accent'
                : 'text-text-faint',
            )}
            onClick={() => setActiveTab('editor')}
          >
            Editor
          </button>
          <button
            className={cx(
              'pb-2 text-sm font-medium',
              activeTab === 'history'
                ? 'text-accent border-b-2 border-accent'
                : 'text-text-faint',
            )}
            onClick={() => setActiveTab('history')}
          >
            History
          </button>
        </div>

        {activeTab === 'editor' && (
          <>
            {hasRRChanges && (
              <div className="bg-warn/10 border border-warn/20 text-warn p-3 rounded-sm flex items-center gap-2 text-sm mb-4">
                <AlertTriangle className="w-4 h-4" />
                <span>
                  Warning: You have modified fields that require a process
                  restart to take effect.
                </span>
              </div>
            )}

            <div className="mb-4">
              <input
                type="text"
                className={INPUT_CLASS}
                placeholder="Search sections..."
                value={search}
                onChange={(e) => setSearch(e.target.value)}
              />
            </div>

            <div className="flex flex-col gap-4">
              {filteredKeys.map((k) => (
                <details
                  key={k}
                  open
                  className="group glass rounded-sm border border-subtle"
                >
                  <summary className="cursor-pointer px-4 py-3 font-medium text-sm select-none flex items-center justify-between bg-overlay-1 group-open:border-b group-open:border-subtle">
                    <div className="flex items-center gap-2">
                      {k}
                      {isRestartRequired(k) && (
                        <Hint label="Requires restart">
                          <AlertTriangle className="w-3 h-3 text-warn" />
                        </Hint>
                      )}
                    </div>
                  </summary>
                  <div className="p-4">
                    <SchemaForm
                      schema={topLevelProps[k]}
                      value={draftState?.[k]}
                      onChange={(val) =>
                        setDraftState((prev) => ({ ...prev, [k]: val }))
                      }
                      path={k}
                      rootSchema={schema}
                    />
                  </div>
                </details>
              ))}
            </div>
          </>
        )}

        {activeTab === 'history' && (
          <Card>
            <div className="overflow-x-auto">
              {historyQuery.isLoading ? (
                <CardBody>
                  <Skeleton className="h-12" />
                </CardBody>
              ) : historyQuery.data?.history.length ? (
                <table className="min-w-[640px] w-full font-mono text-xs">
                  <thead className="table-header sticky top-0 z-10">
                    <tr className="text-[10px] uppercase tracking-wider">
                      <th className="text-right px-4 py-2">Rev</th>
                      <th className="text-left px-4 py-2">Applied</th>
                      <th className="text-center px-4 py-2">TLS</th>
                    </tr>
                  </thead>
                  <tbody>
                    {historyQuery.data.history.map((h) => (
                      <tr key={h.revision} className="border-b border-row">
                        <td className="px-4 py-2 text-right">{h.revision}</td>
                        <td className="px-4 py-2">
                          <RelativeTime
                            ts={new Date(h.applied_at_unix_secs * 1000)}
                          />
                        </td>
                        <td className="px-4 py-2 text-center">
                          <StatusBadge
                            tone={
                              h.config_summary.tls_enabled ? 'ok' : 'neutral'
                            }
                            label={h.config_summary.tls_enabled ? 'on' : 'off'}
                          />
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              ) : (
                <CardBody>
                  <p className="text-xs text-text-faint">
                    No history available.
                  </p>
                </CardBody>
              )}
            </div>
          </Card>
        )}
      </Section>

      {/* Sticky Action Bar */}
      <div className="fixed bottom-0 left-0 right-0 p-4 bg-bg-sub border-t border-subtle shadow-lg z-40 flex items-center justify-between">
        <div className="flex items-center gap-4 text-xs text-text-faint">
          <span>
            Draft Rev:{' '}
            <span className="font-mono text-text">{draftRevision ?? '—'}</span>
          </span>
          <span>
            Last Validated:{' '}
            <span
              className={cx(
                'font-mono',
                lastValidationError ? 'text-red-400' : 'text-text',
              )}
            >
              {lastValidationError
                ? `error @ rev ${lastValidatedRevision ?? '—'}`
                : lastValidatedRevision != null
                  ? `rev ${lastValidatedRevision}`
                  : '—'}
            </span>
          </span>
          {isDirty && (
            <span className="text-accent font-medium">Unsaved changes</span>
          )}
        </div>
        <div className="flex items-center gap-2">
          {lastValidationError && (
            <div
              className="text-xs text-red-400 max-w-md truncate mr-4"
              title={lastValidationError}
            >
              {lastValidationError}
            </div>
          )}
          <Button
            size="sm"
            variant="ghost"
            iconLeft={<XCircle className="w-3 h-3" />}
            onClick={handleDiscard}
            disabled={!isDirty}
          >
            Discard changes
          </Button>
          <Button
            size="sm"
            iconLeft={<Save className="w-3 h-3" />}
            onClick={handleSave}
            disabled={!isDirty}
          >
            Save Draft
          </Button>
          <Button
            size="sm"
            iconLeft={<CheckCircle2 className="w-3 h-3" />}
            onClick={handleValidate}
            disabled={isDirty}
          >
            Validate
          </Button>
          <Button
            size="sm"
            variant="primary"
            iconLeft={<PlayCircle className="w-3 h-3" />}
            disabled={!canApply}
            onClick={handleApply}
          >
            Apply
          </Button>
        </div>
      </div>
    </PageContainer>
  );
}
