import {
  ChevronDown,
  ChevronUp,
  CircleHelp,
  Copy,
  Download,
  FileCheck2,
  FileWarning,
  Plus,
  RotateCcw,
  Save,
  Trash2,
  X,
} from 'lucide-react';
import {
  createContext,
  type ReactNode,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
} from 'react';
import { toast } from 'sonner';
import type {
  ConfigEditorResponse,
  ConfigHistoryResponse,
  ConfigOverrideInfo,
  ConfigValidationIssue,
  ConfigValidationReport,
} from '../../lib/api';
import { ApiError, downloadConfigDraft } from '../../lib/api';
import {
  buildConfigEditorModel,
  CONFIG_EDITOR_CATEGORIES,
  type ConfigSchema,
  getConfigSchemaVariants,
  getConfigValue,
  isNullableConfigSchema,
  normalizeConfigDraft,
  resolveConfigSchema,
  setConfigValue,
  unsetConfigValue,
} from '../../lib/configEditorModel';
import {
  useConfigDraft,
  useConfigEditor,
  useSaveConfigFile,
  useSaveDraft,
  useStatus,
  useValidateConfig,
} from '../../lib/queries';
import {
  Badge,
  Button,
  Card,
  CardBody,
  CardHeader,
  ConfirmDialog,
  cx,
  INPUT_CLASS,
  Notice,
  Section,
  Skeleton,
  ToggleSwitch,
} from '../ui/primitives';
import { RelativeTime } from '../ui/RelativeTime';
import {
  cloneJson,
  isJsonObject,
  isSameJson,
  type JsonObject,
  objectProperties,
  titleForKey,
} from './schema';

interface EditorState {
  value: JsonObject;
  saved: JsonObject;
  revision: number;
  savedAtUnixSecs: number | null;
  hasSavedDraft: boolean;
}

interface ConfigEditorSectionProps {
  history: {
    data?: ConfigHistoryResponse;
    isError: boolean;
  };
}

const INPUT_WITH_ERROR_CLASS = `${INPUT_CLASS} aria-[invalid=true]:border-red-400`;
const OPAQUE_STORAGE_URL_PATH = 'storage.url';
const ADMIN_PROVIDERS_PATH = 'admin.auth.providers';
const RECURRING_JOBS_PATH = 'scheduler.recurring_jobs';

const StorageUrlReplacementContext = createContext<{
  value: string | null;
  onChange: (value: string | null) => void;
}>({
  value: null,
  onChange: () => undefined,
});

const ConfigSourcesContext = createContext<{
  fileConfig: JsonObject;
}>({
  fileConfig: {},
});
function responseError(error: unknown, fallback: string): string {
  if (error instanceof ApiError) return error.message || fallback;
  if (error instanceof Error) return error.message;
  return fallback;
}

function validationReportFromError(
  error: unknown,
): ConfigValidationReport | null {
  if (!(error instanceof ApiError) || !isJsonObject(error.body)) return null;
  for (const key of ['validation', 'report']) {
    const candidate = error.body[key];
    if (
      isJsonObject(candidate) &&
      typeof candidate.revision === 'number' &&
      isJsonObject(candidate.file) &&
      isJsonObject(candidate.effective) &&
      Array.isArray(candidate.filesystem) &&
      Array.isArray(candidate.overrides)
    ) {
      return candidate as unknown as ConfigValidationReport;
    }
  }
  return null;
}

function reportIssues(report: ConfigValidationReport | null | undefined) {
  if (!report) return [];
  const unique = new Map<string, ConfigValidationIssue>();
  for (const issue of [
    ...report.file.issues,
    ...report.effective.issues,
    ...report.filesystem,
    ...report.overrides,
  ]) {
    const key = [issue.severity, issue.path, issue.code, issue.message].join(
      '\u0000',
    );
    if (!unique.has(key)) unique.set(key, issue);
  }
  return [...unique.values()];
}

function reportIsValid(report: ConfigValidationReport | null | undefined) {
  return Boolean(
    report?.file.valid &&
      report.effective.valid &&
      !report.filesystem.some((issue) => issue.severity === 'error'),
  );
}

function categoryForPath(path: string): string {
  return (
    CONFIG_EDITOR_CATEGORIES.find((category) =>
      category.roots.some(
        (root) => path === root || path.startsWith(`${root}.`),
      ),
    )?.id ??
    CONFIG_EDITOR_CATEGORIES.at(-1)?.id ??
    'runtime'
  );
}

function taggedVariants(root: ConfigSchema, schema: ConfigSchema) {
  return getConfigSchemaVariants(root, schema).flatMap((variant) =>
    variant.tag
      ? [
          {
            kind: variant.tag.value,
            property: variant.tag.property,
            label: variant.label,
            schema: variant.schema,
          },
        ]
      : [],
  );
}

function displayValue(value: unknown, sensitive = false): string {
  if (sensitive && value !== undefined) return 'Hidden';
  if (value === undefined) return 'Not set';
  if (value === null) return 'Unset';
  if (typeof value === 'string') return value || 'Empty string';
  if (typeof value === 'boolean' || typeof value === 'number') {
    return String(value);
  }
  if (Array.isArray(value))
    return `${value.length} item${value.length === 1 ? '' : 's'}`;
  return 'Configured object';
}

function overrideForPath(
  overrides: ConfigOverrideInfo[],
  path: string,
): ConfigOverrideInfo | undefined {
  return overrides.find(
    (override) =>
      override.path === path ||
      path.startsWith(`${override.path}.`) ||
      path.startsWith(`${override.path}[`) ||
      override.path.startsWith(`${path}.`) ||
      override.path.startsWith(`${path}[`),
  );
}

function sourceLabel(override: ConfigOverrideInfo | undefined): string {
  if (!override) return 'File / default';
  return override.source === 'cli' ? 'CLI' : 'Environment';
}

function validationTone(issue: ConfigValidationIssue): 'danger' | 'warn' {
  return issue.severity === 'error' ? 'danger' : 'warn';
}

export function ConfigEditorSection({ history }: ConfigEditorSectionProps) {
  const editorQuery = useConfigEditor();
  const draftQuery = useConfigDraft();
  const saveDraft = useSaveDraft();
  const validate = useValidateConfig();
  const saveFile = useSaveConfigFile();
  const [state, setState] = useState<EditorState | null>(null);
  const status = useStatus();
  const [localValidation, setLocalValidation] = useState<{
    revision: number;
    report: ConfigValidationReport;
    storageUrlReplacement: string | null;
  } | null>(null);
  const [openCategories, setOpenCategories] = useState<Record<string, boolean>>(
    {
      network: true,
    },
  );
  const [confirmationOpen, setConfirmationOpen] = useState(false);
  const [selfLockoutRequired, setSelfLockoutRequired] = useState(false);
  const [downloadPending, setDownloadPending] = useState(false);
  const [downloadError, setDownloadError] = useState<string | null>(null);
  const [actionError, setActionError] = useState<string | null>(null);
  const [storageUrlReplacement, setStorageUrlReplacement] = useState<
    string | null
  >(null);
  const downloadLock = useRef(false);

  const editorData = editorQuery.data;
  const draftData = draftQuery.data ?? editorData;

  useEffect(() => {
    if (!editorData || !draftData) return;
    const source = draftData.draft ?? editorData.file_config ?? {};
    if (!isJsonObject(source)) return;
    const revision = draftData.revision;
    setState((current) => {
      if (current && !isSameJson(current.value, current.saved)) return current;
      if (current && current.revision > revision) return current;
      const nextSaved = cloneJson(source);
      if (
        current &&
        current.revision === revision &&
        current.hasSavedDraft === (draftData.draft !== null) &&
        current.savedAtUnixSecs === draftData.saved_at_unix_secs &&
        isSameJson(current.value, nextSaved) &&
        isSameJson(current.saved, nextSaved)
      ) {
        return current;
      }
      return {
        value: nextSaved,
        saved: cloneJson(nextSaved),
        revision,
        savedAtUnixSecs: draftData.saved_at_unix_secs,
        hasSavedDraft: draftData.draft !== null,
      };
    });
  }, [draftData, editorData]);

  const dirty = state ? !isSameJson(state.value, state.saved) : false;
  const serverRevision = draftData?.revision ?? null;
  const stale =
    state !== null &&
    serverRevision !== null &&
    state.revision !== serverRevision;
  const localValidationMatches =
    localValidation !== null &&
    state !== null &&
    localValidation.revision === state.revision &&
    localValidation.storageUrlReplacement === storageUrlReplacement;
  const storedValidation = draftData?.last_validation ?? null;
  const storedValidatedRevision = draftData?.last_validated_revision ?? null;
  const serverReportMatches =
    storageUrlReplacement === null &&
    state !== null &&
    storedValidation?.revision === state.revision;
  const validation = localValidationMatches
    ? localValidation.report
    : serverReportMatches
      ? storedValidation
      : null;
  const validatedRevision = localValidationMatches
    ? localValidation.revision
    : storedValidatedRevision === state?.revision
      ? storedValidatedRevision
      : null;
  const validationReportCurrent = localValidationMatches || serverReportMatches;
  const issues = useMemo(() => reportIssues(validation), [validation]);
  const validationCurrent =
    state !== null && !dirty && !stale && validatedRevision === state.revision;
  const fileValid = Boolean(validation?.file.valid);
  const effectiveValid = Boolean(validation?.effective.valid);
  const filesystemValid = !(validation?.filesystem ?? []).some(
    (issue) => issue.severity === 'error',
  );
  const fileWritable = editorData?.file.mode === 'writable';
  const canSaveDraft = Boolean(
    state && (dirty || !state.hasSavedDraft) && !stale,
  );
  const canValidate = Boolean(state?.hasSavedDraft && !dirty && !stale);
  const canSaveFile = Boolean(
    validationCurrent &&
      fileValid &&
      effectiveValid &&
      filesystemValid &&
      fileWritable,
  );
  const canDownload = Boolean(validationCurrent && fileValid);
  const adminProvidersChanged = Boolean(
    state &&
      editorData &&
      !isSameJson(
        getConfigValue(state.value, ADMIN_PROVIDERS_PATH),
        getConfigValue(editorData.file_config, ADMIN_PROVIDERS_PATH),
      ),
  );
  const latestHistorySavedAtUnixSecs = useMemo(
    () =>
      history.data?.entries.reduce(
        (latest, entry) => Math.max(latest, entry.saved_at_unix_secs),
        0,
      ) || null,
    [history.data],
  );
  const latestSavedAtUnixSecs = Math.max(
    state?.savedAtUnixSecs ?? 0,
    latestHistorySavedAtUnixSecs ?? 0,
  );
  const savedAfterStart = Boolean(
    latestSavedAtUnixSecs &&
      status.data &&
      latestSavedAtUnixSecs >
        Math.floor(Date.now() / 1000) - status.data.uptime_secs,
  );
  const requiresSelfLockoutConfirmation =
    adminProvidersChanged || selfLockoutRequired;

  const updateValue = (next: unknown) => {
    if (!isJsonObject(next)) return;
    setState((current) => (current ? { ...current, value: next } : current));
    setLocalValidation(null);
    setActionError(null);
    setDownloadError(null);
  };

  const handleSaveDraft = () => {
    if (!state || !editorData || !canSaveDraft) return;
    const editorSnapshot = cloneJson(state.value);
    const submitted = normalizeConfigDraft(editorData.schema, state.value);
    const submittedRevision = state.revision;
    setActionError(null);
    saveDraft.mutate(
      { draft: submitted, expected_revision: submittedRevision },
      {
        onSuccess: (response) => {
          setState((current) => {
            if (!current || current.revision !== submittedRevision)
              return current;
            const valueUnchanged = isSameJson(current.value, editorSnapshot);
            return {
              ...current,
              value: valueUnchanged ? cloneJson(submitted) : current.value,
              saved: cloneJson(submitted),
              revision: response.revision,
              savedAtUnixSecs: response.saved_at_unix_secs,
              hasSavedDraft: true,
            };
          });
          setLocalValidation(null);
          toast.success('Configuration draft saved');
        },
        onError: (error) => {
          setActionError(
            responseError(error, 'Failed to save the configuration draft.'),
          );
        },
      },
    );
  };

  const handleValidate = () => {
    if (!state || !canValidate) return;
    setActionError(null);
    validate.mutate(
      {
        expected_revision: state.revision,
        storage_url_replacement: storageUrlReplacement ?? undefined,
      },
      {
        onSuccess: (report) => {
          setLocalValidation({
            revision: report.revision,
            report,
            storageUrlReplacement,
          });
          if (reportIsValid(report)) {
            toast.success('Configuration is valid');
          } else {
            toast.error('Configuration needs attention');
            const firstError = reportIssues(report).find(
              (issue) => issue.severity === 'error',
            );
            if (firstError?.path) {
              focusConfigPath(firstError.path, setOpenCategories);
            }
          }
        },
        onError: (error) => {
          setActionError(
            responseError(error, 'Configuration validation failed.'),
          );
        },
      },
    );
  };

  const persistConfigFile = () => {
    if (!canSaveFile || !editorData || !state) return;
    setActionError(null);
    saveFile.mutate(
      {
        expected_revision: state.revision,
        expected_fingerprint: editorData.file.fingerprint,
        storage_url_replacement: storageUrlReplacement ?? undefined,
        confirm_self_lockout: requiresSelfLockoutConfirmation,
      },
      {
        onSuccess: (response) => {
          setConfirmationOpen(false);
          setSelfLockoutRequired(false);
          setStorageUrlReplacement(null);
          toast.success('Configuration file saved');
          setState((current) =>
            current
              ? {
                  ...current,
                  savedAtUnixSecs:
                    response.saved_at_unix_secs ?? current.savedAtUnixSecs,
                }
              : current,
          );
        },
        onError: (error) => {
          const report = validationReportFromError(error);
          if (report) {
            setLocalValidation({
              revision: report.revision,
              report,
              storageUrlReplacement,
            });
          }
          if (
            error instanceof ApiError &&
            error.code === 'self_lockout_confirmation_required'
          ) {
            setSelfLockoutRequired(true);
            setConfirmationOpen(true);
            return;
          }
          setConfirmationOpen(false);
          setActionError(
            responseError(error, 'Failed to save the configuration file.'),
          );
        },
      },
    );
  };

  const handleSaveFileRequest = () => {
    if (!canSaveFile || !editorData || !state) return;
    if (editorData.file.exists || requiresSelfLockoutConfirmation) {
      setConfirmationOpen(true);
      return;
    }
    persistConfigFile();
  };

  const handleDownload = async () => {
    if (!state || !canDownload || downloadLock.current) return;
    downloadLock.current = true;
    setDownloadPending(true);
    setDownloadError(null);
    try {
      await downloadConfigDraft(
        state.revision,
        storageUrlReplacement ?? undefined,
      );
      toast.success('Validated TOML downloaded');
    } catch (error) {
      setDownloadError(responseError(error, 'Validated TOML download failed.'));
    } finally {
      downloadLock.current = false;
      setDownloadPending(false);
    }
  };

  const loading = !editorData || !draftData || !state;
  const loadError =
    (!editorData && editorQuery.isError) || (!draftData && draftQuery.isError);

  return (
    <Section
      title="Configuration"
      subtitle="Edit the startup configuration, validate it, then save or download TOML."
    >
      <Card data-testid="config-editor-card">
        <CardHeader
          title="Structured config editor"
          subtitle="Every change takes effect after cc-lb restarts. Environment and CLI overrides remain effective after file edits."
          action={
            <div className="flex flex-wrap items-center justify-end gap-2">
              <Button
                size="sm"
                iconLeft={<Save className="h-3.5 w-3.5" />}
                loading={saveDraft.isPending}
                disabled={!canSaveDraft}
                onClick={handleSaveDraft}
              >
                Save draft
              </Button>
              <Button
                size="sm"
                iconLeft={<FileCheck2 className="h-3.5 w-3.5" />}
                loading={validate.isPending}
                disabled={!canValidate}
                onClick={handleValidate}
              >
                Validate
              </Button>
              <Button
                size="sm"
                variant="primary"
                iconLeft={<Save className="h-3.5 w-3.5" />}
                loading={saveFile.isPending}
                disabled={!canSaveFile}
                onClick={handleSaveFileRequest}
              >
                Save to config file
              </Button>
              <Button
                size="sm"
                iconLeft={<Download className="h-3.5 w-3.5" />}
                loading={downloadPending}
                disabled={!canDownload}
                onClick={() => void handleDownload()}
              >
                Download TOML
              </Button>
            </div>
          }
        />
        <CardBody className="space-y-4">
          <EditorMetadata
            loading={loading}
            revision={state?.revision ?? null}
            savedAtUnixSecs={state?.savedAtUnixSecs ?? null}
            validatedRevision={validatedRevision}
            filePath={editorData?.file.path}
          />

          {loadError ? (
            <Notice
              tone="danger"
              title="Configuration editor unavailable"
              action={
                <Button
                  size="sm"
                  loading={editorQuery.isFetching || draftQuery.isFetching}
                  onClick={() => {
                    if (!editorData) void editorQuery.refetch();
                    if (!draftData) void draftQuery.refetch();
                  }}
                >
                  Retry
                </Button>
              }
            >
              The editor or saved draft could not be loaded. Existing runtime
              configuration is unchanged.
            </Notice>
          ) : null}

          {editorData ? <FileCapabilityNotices data={editorData} /> : null}

          {savedAfterStart ? (
            <Notice
              tone="warning"
              title={
                <span data-testid="restart-drift-banner">
                  Saved at{' '}
                  <RelativeTime ts={new Date(latestSavedAtUnixSecs * 1000)} />,
                  not applied yet — restart cc-lb
                </span>
              }
            >
              cc-lb is still using its startup configuration. If this is only a
              draft, validate and save it to the config file before restarting.
            </Notice>
          ) : null}
          {history.isError ? (
            <Notice tone="warning" title="Restart status may be incomplete">
              Saved config history could not be loaded, so the editor cannot
              confirm whether a saved revision is still waiting for a restart.
              Retry the history request below.
            </Notice>
          ) : null}
          {stale ? (
            <Notice tone="danger" title="Draft revision changed">
              This editor started at revision {state?.revision}, but the server
              is now at revision {serverRevision}. Copy any unsaved values, then
              reload before saving.
            </Notice>
          ) : null}

          {dirty ? (
            <Notice tone="warning" title="Draft has unsaved changes">
              Save the draft before validation. The config file and download
              actions remain locked until this exact revision passes validation.
            </Notice>
          ) : !state?.hasSavedDraft && state ? (
            <Notice tone="info" title="Start with a saved draft">
              Save this file configuration as a draft before validating it.
            </Notice>
          ) : null}

          {actionError ? (
            <Notice tone="danger" title="Configuration action failed">
              {actionError}
            </Notice>
          ) : null}

          {downloadError ? (
            <Notice
              tone="danger"
              title="Download failed"
              action={
                <div className="flex gap-2">
                  <Button
                    size="sm"
                    iconLeft={<Copy className="h-3.5 w-3.5" />}
                    onClick={() => {
                      if (!state) return;
                      void navigator.clipboard
                        ?.writeText(JSON.stringify(state.value, null, 2))
                        .then(() => toast.success('Draft JSON copied'))
                        .catch(() => toast.error('Could not copy draft JSON'));
                    }}
                  >
                    Copy draft JSON
                  </Button>
                  <Button size="sm" onClick={() => void handleDownload()}>
                    Retry
                  </Button>
                </div>
              }
            >
              {downloadError} The validated draft remains stored on the server;
              retry the TOML download or copy the draft JSON as a recovery
              fallback.
            </Notice>
          ) : null}

          <ValidationSummary
            report={validation}
            current={validationReportCurrent}
            issues={issues}
            onIssueClick={(path) => focusConfigPath(path, setOpenCategories)}
          />

          {loading ? (
            <ConfigEditorSkeleton />
          ) : editorData && state ? (
            <StorageUrlReplacementContext.Provider
              value={{
                value: storageUrlReplacement,
                onChange: (next) => {
                  setStorageUrlReplacement(next);
                  setLocalValidation(null);
                  setActionError(null);
                  setDownloadError(null);
                },
              }}
            >
              <ConfigForm
                data={{
                  ...editorData,
                  last_validation: validation ?? null,
                  last_validated_revision: validatedRevision,
                }}
                value={state.value}
                issues={issues}
                openCategories={openCategories}
                onOpenCategoriesChange={setOpenCategories}
                onChange={updateValue}
              />
            </StorageUrlReplacementContext.Provider>
          ) : (
            <div
              className="min-h-[420px]"
              data-testid="config-editor-reserved"
            />
          )}
        </CardBody>
      </Card>

      <ConfirmDialog
        open={confirmationOpen}
        onOpenChange={setConfirmationOpen}
        title={
          requiresSelfLockoutConfirmation
            ? editorData?.file.exists
              ? 'Confirm config overwrite and admin access changes'
              : 'Confirm admin access changes'
            : 'Overwrite the current config file?'
        }
        description={
          <span className="space-y-2">
            {editorData?.file.exists ? (
              <span className="block">
                This atomically replaces {editorData.file.path}. Comments and
                formatting in the current TOML file are not preserved.
              </span>
            ) : null}
            {requiresSelfLockoutConfirmation ? (
              <span className="block font-medium text-amber-200">
                Admin authentication providers changed. A wrong provider kind,
                ID, token environment variable, domain, or audience can lock you
                out after restart. Confirm another valid admin access path
                before continuing.
              </span>
            ) : null}
          </span>
        }
        confirmLabel={
          requiresSelfLockoutConfirmation
            ? 'Save and accept lockout risk'
            : 'Overwrite file'
        }
        destructive={requiresSelfLockoutConfirmation}
        pending={saveFile.isPending}
        closeOnConfirm={false}
        onConfirm={persistConfigFile}
      />
    </Section>
  );
}
function EditorMetadata({
  loading,
  revision,
  validatedRevision,
  savedAtUnixSecs,
  filePath,
}: {
  loading: boolean;
  revision: number | null;
  validatedRevision: number | null;
  savedAtUnixSecs: number | null;
  filePath?: string;
}) {
  return (
    <div
      data-testid="config-editor-metadata"
      className="grid min-h-12 grid-cols-2 gap-x-4 gap-y-2 text-xs text-text-faint sm:grid-cols-4"
    >
      <MetadataValue
        label="Draft revision"
        loading={loading}
        value={revision ?? '—'}
      />
      <MetadataValue
        label="Validated revision"
        loading={loading}
        value={validatedRevision ?? '—'}
      />
      <MetadataValue
        label="Draft saved"
        loading={loading}
        value={
          savedAtUnixSecs ? (
            <RelativeTime ts={new Date(savedAtUnixSecs * 1000)} />
          ) : (
            '—'
          )
        }
      />
      <MetadataValue
        label="Config file"
        loading={loading}
        value={<span className="break-all font-mono">{filePath ?? '—'}</span>}
      />
    </div>
  );
}

function MetadataValue({
  label,
  loading,
  value,
}: {
  label: string;
  loading: boolean;
  value: ReactNode;
}) {
  return (
    <div className="min-w-0">
      <div className="mb-0.5 text-[10px] uppercase tracking-wider">{label}</div>
      <div className="min-h-4 text-text-muted">
        {loading ? <Skeleton className="h-4 w-20 max-w-full" /> : value}
      </div>
    </div>
  );
}

function FileCapabilityNotices({ data }: { data: ConfigEditorResponse }) {
  return (
    <div className="space-y-2">
      {data.restart_required ? (
        <Notice tone="warning" title="Restart required after saving">
          cc-lb keeps its startup configuration fixed. After saving this draft
          to the config file, restart the process to run the new values.
        </Notice>
      ) : (
        <Notice tone="info" title="Startup-fixed configuration">
          File edits do not change the running process. Restart cc-lb after
          saving to apply the new configuration.
        </Notice>
      )}
      {data.file.mode === 'read_only' ? (
        <Notice
          tone="warning"
          title={
            data.file.exists
              ? 'Config file is read-only'
              : 'Config file is missing and cannot be created'
          }
        >
          {data.file.reason ??
            'This process cannot atomically replace the config file. You can still save and validate a draft, then download TOML for manual deployment.'}
        </Notice>
      ) : !data.file.exists ? (
        <Notice tone="info" title="Config file is missing">
          Saving will create {data.file.path} using an atomic replace.
        </Notice>
      ) : null}
    </div>
  );
}

function ValidationSummary({
  report,
  current,
  issues,
  onIssueClick,
}: {
  report: ConfigValidationReport | null | undefined;
  current: boolean;
  issues: ConfigValidationIssue[];
  onIssueClick: (path: string) => void;
}) {
  const validationSucceeded =
    current && reportIsValid(report) && issues.length === 0;
  if (validationSucceeded) {
    return (
      <Notice tone="success" title="Configuration validated">
        This draft passed file and effective validation with no issues.
      </Notice>
    );
  }
  if (!report) return null;
  const errors = issues.filter((issue) => issue.severity === 'error');
  const warnings = issues.filter((issue) => issue.severity !== 'error');
  return (
    <details
      open={errors.length > 0}
      className={cx(
        'rounded-sm border',
        errors.length
          ? 'border-red-500/35 bg-red-500/5'
          : 'border-subtle bg-panel/30',
      )}
    >
      <summary className="flex cursor-pointer list-none items-center justify-between gap-3 px-3 py-2.5 text-sm focus-visible:outline focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-2">
        <span className="inline-flex items-center gap-2">
          {errors.length ? (
            <FileWarning className="h-4 w-4 text-red-300" />
          ) : (
            <FileCheck2 className="h-4 w-4 text-emerald-300" />
          )}
          Validation summary
        </span>
        <span className="flex flex-wrap justify-end gap-1.5">
          {!current ? <Badge tone="warn">Stale</Badge> : null}
          <Badge tone={report.file.valid ? 'ok' : 'danger'}>
            File {report.file.valid ? 'valid' : 'invalid'}
          </Badge>
          <Badge tone={report.effective.valid ? 'ok' : 'danger'}>
            Effective {report.effective.valid ? 'valid' : 'invalid'}
          </Badge>
          {errors.length ? (
            <Badge tone="danger">{errors.length} errors</Badge>
          ) : null}
          {warnings.length ? (
            <Badge tone="warn">{warnings.length} warnings</Badge>
          ) : null}
        </span>
      </summary>
      {issues.length ? (
        <div className="space-y-1 border-t border-subtle px-3 py-2">
          {issues.map((issue, index) => (
            <button
              key={`${issue.path}-${issue.code}-${index}`}
              type="button"
              className="flex w-full min-w-0 items-start gap-2 rounded-sm px-2 py-1.5 text-left text-xs hover:bg-overlay-5 focus-visible:outline focus-visible:outline-2 focus-visible:outline-accent"
              onClick={() => issue.path && onIssueClick(issue.path)}
            >
              <Badge tone={validationTone(issue)}>{issue.severity}</Badge>
              <span className="min-w-0">
                <span className="block break-all font-mono text-text">
                  {issue.path || 'configuration'} · {issue.code}
                </span>
                <span className="block text-text-muted">{issue.message}</span>
              </span>
            </button>
          ))}
        </div>
      ) : (
        <div className="border-t border-subtle px-3 py-2 text-xs text-text-muted">
          No validation issues.
        </div>
      )}
    </details>
  );
}

function ConfigEditorSkeleton() {
  return (
    <div
      data-testid="config-editor-skeleton"
      className="min-h-[560px] space-y-2"
      aria-hidden="true"
    >
      {CONFIG_EDITOR_CATEGORIES.map((category, index) => (
        <div
          key={category.id}
          className="rounded-sm border border-subtle bg-panel/30 px-3 py-3"
        >
          <div className="flex items-center justify-between">
            <Skeleton className={cx('h-4', index % 2 ? 'w-36' : 'w-44')} />
            <Skeleton className="h-5 w-16" />
          </div>
          {index === 0 ? (
            <div className="mt-4 grid grid-cols-1 gap-3 sm:grid-cols-2">
              <Skeleton className="h-16 w-full" />
              <Skeleton className="h-16 w-full" />
              <Skeleton className="h-16 w-full" />
              <Skeleton className="h-16 w-full" />
            </div>
          ) : null}
        </div>
      ))}
    </div>
  );
}

function ConfigForm({
  data,
  value,
  issues,
  openCategories,
  onOpenCategoriesChange,
  onChange,
}: {
  data: ConfigEditorResponse;
  value: JsonObject;
  issues: ConfigValidationIssue[];
  openCategories: Record<string, boolean>;
  onOpenCategoriesChange: (value: Record<string, boolean>) => void;
  onChange: (value: unknown) => void;
}) {
  const schema = data.schema as ConfigSchema;
  const topLevel = objectProperties(schema, schema);
  const model = buildConfigEditorModel(data, value);
  const categories = model.categories;
  const overrideList = data.overrides ?? [];
  const unknownLeaves = model.leaves.filter((leaf) => leaf.unknown);
  const fallbackCategoryId = categories.at(-1)?.id;

  return (
    <ConfigSourcesContext.Provider value={{ fileConfig: data.file_config }}>
      <div className="space-y-2" data-testid="structured-config-editor">
        {categories.map((category) => {
          const modifiedCount = category.counts.modified;
          const overrideCount = category.counts.overrides;
          const invalidCount = category.counts.errors;
          const open = Boolean(openCategories[category.id]);
          return (
            <section
              key={category.id}
              data-config-category={category.id}
              className="rounded-sm border border-subtle bg-panel/20"
            >
              <button
                type="button"
                className="flex w-full min-w-0 items-start justify-between gap-3 px-3 py-3 text-left focus-visible:outline focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-2"
                aria-expanded={open}
                onClick={() =>
                  onOpenCategoriesChange({
                    ...openCategories,
                    [category.id]: !open,
                  })
                }
              >
                <span className="min-w-0">
                  <span className="block text-sm font-medium text-text">
                    {category.label}
                  </span>
                  <span className="mt-0.5 block text-xs text-text-faint">
                    {category.description}
                  </span>
                </span>
                <span className="flex shrink-0 flex-wrap items-center justify-end gap-1.5">
                  {modifiedCount ? (
                    <Badge tone="accent">{modifiedCount} modified</Badge>
                  ) : null}
                  {overrideCount ? (
                    <Badge tone="warn">{overrideCount} overridden</Badge>
                  ) : null}
                  {invalidCount ? (
                    <Badge tone="danger">{invalidCount} invalid</Badge>
                  ) : null}
                  {open ? (
                    <ChevronUp className="h-4 w-4 text-text-faint" />
                  ) : (
                    <ChevronDown className="h-4 w-4 text-text-faint" />
                  )}
                </span>
              </button>
              {open ? (
                <div className="space-y-4 border-t border-subtle px-3 py-4 sm:px-4">
                  {category.roots.map((root) =>
                    topLevel[root] ? (
                      <ConfigNode
                        key={root}
                        rootSchema={schema}
                        schema={topLevel[root]}
                        path={root}
                        value={value}
                        defaultConfig={data.default_config}
                        effectiveConfig={data.effective_config}
                        overrides={overrideList}
                        issues={issues}
                        onChange={onChange}
                        depth={0}
                      />
                    ) : null,
                  )}
                  {category.id === fallbackCategoryId &&
                  unknownLeaves.length > 0 ? (
                    <div className="space-y-3 rounded-sm border border-amber-500/35 bg-amber-500/5 p-3">
                      <div>
                        <h3 className="text-xs font-medium text-amber-100">
                          Unrecognized file keys
                        </h3>
                        <p className="mt-0.5 text-xs text-text-faint">
                          These values are preserved for review. Remove or
                          correct them before validation.
                        </p>
                      </div>
                      <div className="grid grid-cols-1 gap-3 lg:grid-cols-2">
                        {unknownLeaves.map((leaf) => (
                          <ScalarField
                            key={leaf.pathString}
                            rootSchema={schema}
                            schema={leaf.schema}
                            nullable={leaf.nullable}
                            path={leaf.pathString}
                            value={value}
                            defaultValue={leaf.defaultValue}
                            effectiveValue={leaf.effectiveValue}
                            override={leaf.override ?? undefined}
                            issues={leaf.issues}
                            onChange={onChange}
                          />
                        ))}
                      </div>
                    </div>
                  ) : null}
                </div>
              ) : null}
            </section>
          );
        })}
      </div>
    </ConfigSourcesContext.Provider>
  );
}

function ConfigNode({
  rootSchema,
  schema: inputSchema,
  path,
  value,
  defaultConfig,
  effectiveConfig,
  overrides,
  issues,
  onChange,
  depth,
}: {
  rootSchema: ConfigSchema;
  schema: ConfigSchema;
  path: string;
  value: JsonObject;
  defaultConfig: JsonObject;
  effectiveConfig: JsonObject;
  overrides: ConfigOverrideInfo[];
  issues: ConfigValidationIssue[];
  onChange: (value: unknown) => void;
  depth: number;
}) {
  const resolvedSchema = resolveConfigSchema(rootSchema, inputSchema);
  const nullable = isNullableConfigSchema(rootSchema, inputSchema);
  const variants = taggedVariants(rootSchema, resolvedSchema);
  const schema =
    variants.length > 0
      ? resolvedSchema
      : (getConfigSchemaVariants(rootSchema, resolvedSchema)[0]?.schema ??
        resolvedSchema);
  const properties = objectProperties(rootSchema, schema);
  const currentValue = getConfigValue(value, path);
  const defaultValue = getConfigValue(defaultConfig, path);
  const effectiveValue = getConfigValue(effectiveConfig, path);

  if (path === ADMIN_PROVIDERS_PATH) {
    return (
      <AdminProvidersEditor
        rootSchema={rootSchema}
        schema={schema}
        path={path}
        value={value}
        defaultConfig={defaultConfig}
        effectiveConfig={effectiveConfig}
        overrides={overrides}
        issues={issues}
        onChange={onChange}
      />
    );
  }
  if (path === RECURRING_JOBS_PATH) {
    return (
      <RecurringJobsEditor
        rootSchema={rootSchema}
        schema={schema}
        path={path}
        value={value}
        defaultConfig={defaultConfig}
        effectiveConfig={effectiveConfig}
        overrides={overrides}
        issues={issues}
        onChange={onChange}
      />
    );
  }
  if (variants.length) {
    return (
      <TaggedUnionEditor
        rootSchema={rootSchema}
        schema={schema}
        path={path}
        value={value}
        defaultConfig={defaultConfig}
        effectiveConfig={effectiveConfig}
        overrides={overrides}
        issues={issues}
        onChange={onChange}
        depth={depth}
      />
    );
  }
  if (Object.keys(properties).length) {
    const configured = isJsonObject(currentValue);
    if (nullable && !configured) {
      return (
        <OptionalObjectToggle
          path={path}
          description={
            typeof schema.description === 'string'
              ? schema.description
              : undefined
          }
          enabled={false}
          onEnable={() => {
            onChange(setConfigValue(value, path, {}) as JsonObject);
          }}
        />
      );
    }
    return (
      <div
        className={cx(
          'space-y-3',
          depth === 0 ? '' : 'rounded-sm border border-subtle/70 bg-bg/25 p-3',
        )}
      >
        <div className="flex min-w-0 items-start justify-between gap-3">
          <div className="min-w-0">
            <h3
              className={cx(
                'font-medium text-text',
                depth === 0 ? 'text-sm' : 'text-xs',
              )}
            >
              {titleForKey(path.split('.').at(-1) ?? path)}
            </h3>
            {typeof schema.description === 'string' ? (
              <p className="mt-0.5 text-xs leading-relaxed text-text-faint">
                {schema.description}
              </p>
            ) : null}
          </div>
          {nullable ? (
            <Button
              size="sm"
              variant="ghost"
              iconLeft={<X className="h-3 w-3" />}
              onClick={() =>
                onChange(unsetConfigValue(value, path) as JsonObject)
              }
            >
              Disable
            </Button>
          ) : null}
        </div>
        <div className="grid grid-cols-1 gap-3 lg:grid-cols-2">
          {Object.entries(properties).map(([key, childSchema]) => (
            <ConfigNode
              key={key}
              rootSchema={rootSchema}
              schema={childSchema}
              path={`${path}.${key}`}
              value={value}
              defaultConfig={defaultConfig}
              effectiveConfig={effectiveConfig}
              overrides={overrides}
              issues={issues}
              onChange={onChange}
              depth={depth + 1}
            />
          ))}
        </div>
      </div>
    );
  }

  return (
    <ScalarField
      rootSchema={rootSchema}
      schema={schema}
      nullable={nullable}
      path={path}
      value={value}
      defaultValue={defaultValue}
      effectiveValue={effectiveValue}
      override={overrideForPath(overrides, path)}
      issues={issues.filter(
        (issue) => issue.path === path || issue.path.startsWith(`${path}.`),
      )}
      onChange={onChange}
    />
  );
}

function OptionalObjectToggle({
  path,
  description,
  enabled,
  onEnable,
}: {
  path: string;
  description?: string;
  enabled: boolean;
  onEnable: () => void;
}) {
  return (
    <div data-config-path={path} tabIndex={-1} className="rounded-sm">
      <ToggleSwitch
        checked={enabled}
        label={titleForKey(path.split('.').at(-1) ?? path)}
        description={
          description ??
          'Optional configuration group. Enable it to write its fields to the file.'
        }
        onChange={(event) => {
          if (event.target.checked) onEnable();
        }}
      />
    </div>
  );
}

function ScalarField({
  rootSchema,
  schema,
  nullable,
  path,
  value,
  defaultValue,
  effectiveValue,
  override,
  issues,
  onChange,
}: {
  rootSchema: ConfigSchema;
  schema: ConfigSchema;
  nullable: boolean;
  path: string;
  value: JsonObject;
  defaultValue: unknown;
  effectiveValue: unknown;
  override?: ConfigOverrideInfo;
  issues: ConfigValidationIssue[];
  onChange: (value: unknown) => void;
}) {
  const { fileConfig } = useContext(ConfigSourcesContext);
  const storageUrlReplacement = useContext(StorageUrlReplacementContext);
  const [replacingOpaque, setReplacingOpaque] = useState(false);
  const [clearedWhileEditing, setClearedWhileEditing] = useState(false);
  const current = getConfigValue(value, path);
  const fileValue = getConfigValue(fileConfig, path);
  const configured = current !== undefined && current !== null;
  const error = issues.find((issue) => issue.severity === 'error');
  const isSensitive =
    Boolean(override?.sensitive) || path === OPAQUE_STORAGE_URL_PATH;
  const label = titleForKey(path.split('.').at(-1) ?? path);
  const description =
    typeof schema.description === 'string' ? schema.description : undefined;
  const schemaTypes = (
    Array.isArray(schema.type) ? schema.type : [schema.type]
  ).filter(
    (type): type is string => typeof type === 'string' && type !== 'null',
  );
  const numeric =
    schemaTypes.includes('integer') || schemaTypes.includes('number');
  const boolean = schemaTypes.includes('boolean');
  const enumValues = Array.isArray(schema.enum)
    ? schema.enum.filter((item): item is string | number =>
        ['string', 'number'].includes(typeof item),
      )
    : [];
  const arrayItems = isJsonObject(schema.items)
    ? resolveConfigSchema(rootSchema, schema.items)
    : {};
  const isStringArray =
    schemaTypes.includes('array') && arrayItems.type === 'string';
  const inputId = `config-${path.replaceAll('.', '-')}`;

  useEffect(() => {
    if (storageUrlReplacement.value === null) {
      setReplacingOpaque(false);
    }
  }, [storageUrlReplacement.value]);

  const reset = () => {
    if (defaultValue !== undefined) {
      onChange(setConfigValue(value, path, cloneJson(defaultValue)));
    } else {
      onChange(unsetConfigValue(value, path));
    }
  };

  return (
    <div
      data-config-path={path}
      tabIndex={-1}
      className={cx(
        'min-w-0 rounded-sm border bg-panel-strong/25 p-3 outline-none focus:ring-2 focus:ring-accent/60',
        error ? 'border-red-500/45' : 'border-subtle/70',
      )}
    >
      <div className="mb-2 flex min-w-0 items-start justify-between gap-2">
        <label
          htmlFor={inputId}
          className="min-w-0 text-xs font-medium text-text"
        >
          {label}
          {description ? (
            <span
              className="ml-1 inline-flex align-middle text-text-faint"
              title={description}
            >
              <CircleHelp className="h-3.5 w-3.5" />
            </span>
          ) : null}
        </label>
        <div className="flex shrink-0 gap-1">
          {path === OPAQUE_STORAGE_URL_PATH ? (
            <>
              {replacingOpaque || storageUrlReplacement.value !== null ? (
                <Button
                  size="sm"
                  variant="ghost"
                  iconLeft={<X className="h-3 w-3" />}
                  onClick={() => {
                    storageUrlReplacement.onChange(null);
                    setReplacingOpaque(false);
                  }}
                >
                  Cancel replacement
                </Button>
              ) : null}
              {configured ? (
                <Button
                  size="sm"
                  variant="ghost"
                  iconLeft={<X className="h-3 w-3" />}
                  onClick={() => {
                    storageUrlReplacement.onChange(null);
                    setReplacingOpaque(false);
                    onChange(unsetConfigValue(value, path));
                  }}
                >
                  Unset
                </Button>
              ) : null}
            </>
          ) : (
            <>
              <Button
                size="sm"
                variant="ghost"
                iconLeft={<RotateCcw className="h-3 w-3" />}
                onClick={reset}
              >
                Reset
              </Button>
              <Button
                size="sm"
                variant="ghost"
                iconLeft={<X className="h-3 w-3" />}
                onClick={() => onChange(unsetConfigValue(value, path))}
              >
                Unset
              </Button>
            </>
          )}
        </div>
      </div>

      {nullable && !configured && !(numeric && clearedWhileEditing) ? (
        <Button
          size="sm"
          iconLeft={<Plus className="h-3 w-3" />}
          onClick={() => {
            const initial =
              defaultValue !== undefined && defaultValue !== null
                ? cloneJson(defaultValue)
                : effectiveValue !== undefined && effectiveValue !== null
                  ? cloneJson(effectiveValue)
                  : boolean
                    ? false
                    : numeric
                      ? 0
                      : schemaTypes.includes('array')
                        ? []
                        : '';
            onChange(setConfigValue(value, path, initial));
          }}
        >
          Set value
        </Button>
      ) : path === OPAQUE_STORAGE_URL_PATH &&
        storageUrlReplacement.value === null &&
        !replacingOpaque ? (
        <div className="flex flex-wrap items-center gap-2">
          <Badge tone="mono">
            {configured
              ? 'Stored value hidden'
              : override
                ? `Supplied by ${override.name}; not stored in file`
                : 'No stored URL'}
          </Badge>
          <Button size="sm" onClick={() => setReplacingOpaque(true)}>
            Replace URL
          </Button>
        </div>
      ) : isStringArray ? (
        <StringArrayControl
          id={inputId}
          values={
            Array.isArray(current)
              ? current.filter(
                  (item): item is string => typeof item === 'string',
                )
              : []
          }
          error={error?.message}
          onChange={(next) => onChange(setConfigValue(value, path, next))}
        />
      ) : boolean ? (
        <ToggleSwitch
          id={inputId}
          checked={configured ? current === true : effectiveValue === true}
          label={
            (configured ? current : effectiveValue) === true
              ? 'Enabled'
              : 'Disabled'
          }
          onChange={(event) =>
            onChange(setConfigValue(value, path, event.target.checked))
          }
        />
      ) : enumValues.length ? (
        <select
          id={inputId}
          className={INPUT_WITH_ERROR_CLASS}
          aria-invalid={Boolean(error)}
          value={
            typeof current === 'string' || typeof current === 'number'
              ? String(current)
              : ''
          }
          onChange={(event) => {
            const raw = event.target.value;
            onChange(setConfigValue(value, path, numeric ? Number(raw) : raw));
          }}
        >
          {!configured ? <option value="">Inherited / not set</option> : null}
          {enumValues.map((option) => (
            <option key={String(option)} value={String(option)}>
              {titleForKey(String(option))}
            </option>
          ))}
        </select>
      ) : (
        <input
          id={inputId}
          className={INPUT_WITH_ERROR_CLASS}
          type={
            path === OPAQUE_STORAGE_URL_PATH
              ? 'password'
              : numeric
                ? 'number'
                : 'text'
          }
          step={schemaTypes.includes('integer') ? 1 : undefined}
          min={typeof schema.minimum === 'number' ? schema.minimum : undefined}
          max={typeof schema.maximum === 'number' ? schema.maximum : undefined}
          aria-invalid={Boolean(error)}
          value={
            path === OPAQUE_STORAGE_URL_PATH
              ? (storageUrlReplacement.value ?? '')
              : typeof current === 'string' || typeof current === 'number'
                ? String(current)
                : ''
          }
          placeholder={
            path === OPAQUE_STORAGE_URL_PATH
              ? 'Enter a replacement URL'
              : `Inherited: ${displayValue(effectiveValue, isSensitive)}`
          }
          onChange={(event) => {
            if (path === OPAQUE_STORAGE_URL_PATH) {
              const replacement = event.target.value;
              storageUrlReplacement.onChange(replacement || null);
              if (!replacement) setReplacingOpaque(false);
              return;
            }
            if (numeric && event.target.value === '') {
              setClearedWhileEditing(true);
              onChange(unsetConfigValue(value, path));
              return;
            }
            if (numeric) setClearedWhileEditing(false);
            const next = numeric
              ? Number(event.target.value)
              : event.target.value;
            onChange(setConfigValue(value, path, next));
          }}
          onBlur={() => {
            if (numeric) setClearedWhileEditing(false);
          }}
        />
      )}

      <div className="mt-2 flex min-w-0 flex-wrap gap-x-3 gap-y-1 text-[10px] text-text-faint">
        <span>Draft: {displayValue(current, isSensitive)}</span>
        <span>File: {displayValue(fileValue, isSensitive)}</span>
        <span>Default: {displayValue(defaultValue, isSensitive)}</span>
        <span>Effective: {displayValue(effectiveValue, isSensitive)}</span>
        {override ? (
          <Badge tone="warn">
            {sourceLabel(override)} · {override.name}
          </Badge>
        ) : null}
        {!configured ? <Badge tone="neutral">Inherited</Badge> : null}
      </div>
      {issues.map((issue, index) => (
        <div
          key={`${issue.code}-${index}`}
          className={cx(
            'mt-1.5 text-[11px]',
            issue.severity === 'error' ? 'text-red-300' : 'text-amber-200',
          )}
        >
          {issue.message}
        </div>
      ))}
    </div>
  );
}
function StringArrayControl({
  id,
  values,
  error,
  onChange,
}: {
  id: string;
  values: string[];
  error?: string;
  onChange: (values: string[]) => void;
}) {
  return (
    <div className="space-y-2">
      {values.map((item, index) => (
        <div key={`${id}-${index}`} className="flex min-w-0 gap-1.5">
          <input
            id={index === 0 ? id : undefined}
            className={INPUT_WITH_ERROR_CLASS}
            aria-invalid={Boolean(error)}
            value={item}
            onChange={(event) => {
              const next = [...values];
              next[index] = event.target.value;
              onChange(next);
            }}
          />
          <Button
            size="sm"
            variant="ghost"
            aria-label={`Move item ${index + 1} up`}
            disabled={index === 0}
            onClick={() => {
              const next = [...values];
              [next[index - 1], next[index]] = [next[index], next[index - 1]];
              onChange(next);
            }}
          >
            <ChevronUp className="h-3.5 w-3.5" />
          </Button>
          <Button
            size="sm"
            variant="ghost"
            aria-label={`Move item ${index + 1} down`}
            disabled={index === values.length - 1}
            onClick={() => {
              const next = [...values];
              [next[index], next[index + 1]] = [next[index + 1], next[index]];
              onChange(next);
            }}
          >
            <ChevronDown className="h-3.5 w-3.5" />
          </Button>
          <Button
            size="sm"
            variant="ghost"
            aria-label={`Remove item ${index + 1}`}
            onClick={() =>
              onChange(values.filter((_, itemIndex) => itemIndex !== index))
            }
          >
            <Trash2 className="h-3.5 w-3.5" />
          </Button>
        </div>
      ))}
      <Button
        size="sm"
        iconLeft={<Plus className="h-3 w-3" />}
        onClick={() => onChange([...values, ''])}
      >
        Add item
      </Button>
    </div>
  );
}

function TaggedUnionEditor({
  rootSchema,
  schema,
  path,
  value,
  defaultConfig,
  effectiveConfig,
  overrides,
  issues,
  onChange,
  depth,
}: {
  rootSchema: ConfigSchema;
  schema: ConfigSchema;
  path: string;
  value: JsonObject;
  defaultConfig: JsonObject;
  effectiveConfig: JsonObject;
  overrides: ConfigOverrideInfo[];
  issues: ConfigValidationIssue[];
  onChange: (value: unknown) => void;
  depth: number;
}) {
  const storageUrlReplacement = useContext(StorageUrlReplacementContext);
  const variants = taggedVariants(rootSchema, schema);
  const current = getConfigValue(value, path);
  const effective = getConfigValue(effectiveConfig, path);
  const currentKind =
    isJsonObject(current) && typeof current.kind === 'string'
      ? current.kind
      : isJsonObject(effective) && typeof effective.kind === 'string'
        ? effective.kind
        : variants[0]?.kind;
  const selected =
    variants.find((variant) => variant.kind === currentKind) ?? variants[0];
  if (!selected) {
    return (
      <ScalarField
        rootSchema={rootSchema}
        schema={{ type: 'string' }}
        nullable={false}
        path={path}
        value={value}
        defaultValue={getConfigValue(defaultConfig, path)}
        effectiveValue={effective}
        override={overrideForPath(overrides, path)}
        issues={issues.filter((issue) => issue.path === path)}
        onChange={onChange}
      />
    );
  }
  const properties = objectProperties(rootSchema, selected.schema);
  return (
    <div
      className="space-y-3 rounded-sm border border-subtle/70 bg-bg/25 p-3"
      data-config-path={path}
      tabIndex={-1}
    >
      <div className="flex flex-col gap-2 sm:flex-row sm:items-end sm:justify-between">
        <div className="min-w-0">
          <h3
            className={cx(
              'font-medium text-text',
              depth === 0 ? 'text-sm' : 'text-xs',
            )}
          >
            {titleForKey(path.split('.').at(-1) ?? path)}
          </h3>
          {typeof schema.description === 'string' ? (
            <p className="mt-0.5 text-xs text-text-faint">
              {schema.description}
            </p>
          ) : null}
        </div>
        <label className="min-w-40 text-[10px] uppercase tracking-wider text-text-faint">
          Kind
          <select
            data-config-path={`${path}.${selected.property}`}
            className={cx(INPUT_CLASS, 'mt-1')}
            value={selected.kind}
            onChange={(event) => {
              const nextVariant = variants.find(
                (variant) => variant.kind === event.target.value,
              );
              if (!nextVariant) return;
              const nextValue = {
                [nextVariant.property]: nextVariant.kind,
              };
              if (path === 'storage') {
                storageUrlReplacement.onChange(null);
              }
              onChange(setConfigValue(value, path, nextValue));
            }}
          >
            {variants.map((variant) => (
              <option key={variant.kind} value={variant.kind}>
                {titleForKey(variant.kind)}
              </option>
            ))}
          </select>
        </label>
      </div>
      <div className="grid grid-cols-1 gap-3 lg:grid-cols-2">
        {Object.entries(properties).map(([key, child]) =>
          key === selected.property ? null : (
            <ConfigNode
              key={key}
              rootSchema={rootSchema}
              schema={child}
              path={`${path}.${key}`}
              value={value}
              defaultConfig={defaultConfig}
              effectiveConfig={effectiveConfig}
              overrides={overrides}
              issues={issues}
              onChange={onChange}
              depth={depth + 1}
            />
          ),
        )}
      </div>
    </div>
  );
}

function AdminProvidersEditor({
  rootSchema,
  schema,
  path,
  value,
  defaultConfig,
  effectiveConfig,
  overrides,
  issues,
  onChange,
}: {
  rootSchema: ConfigSchema;
  schema: ConfigSchema;
  path: string;
  value: JsonObject;
  defaultConfig: JsonObject;
  effectiveConfig: JsonObject;
  overrides: ConfigOverrideInfo[];
  issues: ConfigValidationIssue[];
  onChange: (value: unknown) => void;
}) {
  const itemSchema = isJsonObject(schema.items)
    ? resolveConfigSchema(rootSchema, schema.items)
    : {};
  const variants = taggedVariants(rootSchema, itemSchema);
  const providers = Array.isArray(getConfigValue(value, path))
    ? (getConfigValue(value, path) as unknown[]).filter(isJsonObject)
    : [];
  const providerKeyCounter = useRef(0);
  const providerKeys = useRef<string[]>([]);
  while (providerKeys.current.length < providers.length) {
    providerKeys.current.push(`provider-${providerKeyCounter.current}`);
    providerKeyCounter.current += 1;
  }
  if (providerKeys.current.length > providers.length) {
    providerKeys.current.length = providers.length;
  }

  return (
    <div
      className="space-y-3 rounded-sm border border-subtle/70 bg-bg/25 p-3"
      data-config-path={path}
      tabIndex={-1}
    >
      <div className="flex items-start justify-between gap-3">
        <div>
          <h3 className="text-xs font-medium text-text">Admin Providers</h3>
          <p className="mt-0.5 text-xs text-text-faint">
            Provider order is stable. Environment-backed tokens are referenced
            by name and never displayed.
          </p>
        </div>
        <Button
          size="sm"
          iconLeft={<Plus className="h-3 w-3" />}
          onClick={() => {
            const variant = variants[0];
            const provider = {
              [variant?.property ?? 'kind']: variant?.kind ?? 'static_token',
            };
            providerKeys.current.push(`provider-${providerKeyCounter.current}`);
            providerKeyCounter.current += 1;
            onChange(setConfigValue(value, path, [...providers, provider]));
          }}
        >
          Add provider
        </Button>
      </div>
      {providers.length ? (
        <div className="space-y-3">
          {providers.map((provider, index) => {
            const kind =
              typeof provider.kind === 'string'
                ? provider.kind
                : variants[0]?.kind;
            const variant =
              variants.find((candidate) => candidate.kind === kind) ??
              variants[0];
            const properties = variant
              ? objectProperties(rootSchema, variant.schema)
              : {};
            const providerRoot = `${path}[${index}]`;
            return (
              <div
                key={providerKeys.current[index]}
                data-config-path={providerRoot}
                tabIndex={-1}
                className="rounded-sm border border-subtle bg-panel-strong/30 p-3"
              >
                <div className="mb-3 flex flex-wrap items-end justify-between gap-2">
                  <label className="min-w-44 text-[10px] uppercase tracking-wider text-text-faint">
                    Provider kind
                    <select
                      data-config-path={`${providerRoot}.${variant?.property ?? 'kind'}`}
                      className={cx(INPUT_CLASS, 'mt-1')}
                      value={kind}
                      onChange={(event) => {
                        const nextVariant = variants.find(
                          (candidate) => candidate.kind === event.target.value,
                        );
                        if (!nextVariant) return;
                        const nextProvider = {
                          [nextVariant.property]: nextVariant.kind,
                        };
                        const next = [...providers];
                        next[index] = nextProvider;
                        onChange(setConfigValue(value, path, next));
                        requestAnimationFrame(() =>
                          focusConfigPath(`${providerRoot}.id`),
                        );
                      }}
                    >
                      {variants.map((candidate) => (
                        <option key={candidate.kind} value={candidate.kind}>
                          {titleForKey(candidate.kind)}
                        </option>
                      ))}
                    </select>
                  </label>
                  <div className="flex items-center gap-1">
                    <Button
                      size="sm"
                      variant="ghost"
                      aria-label={`Move provider ${index + 1} up`}
                      disabled={index === 0}
                      onClick={() => {
                        const next = [...providers];
                        [next[index - 1], next[index]] = [
                          next[index],
                          next[index - 1],
                        ];
                        [
                          providerKeys.current[index - 1],
                          providerKeys.current[index],
                        ] = [
                          providerKeys.current[index],
                          providerKeys.current[index - 1],
                        ];
                        onChange(setConfigValue(value, path, next));
                      }}
                    >
                      <ChevronUp className="h-3.5 w-3.5" />
                    </Button>
                    <Button
                      size="sm"
                      variant="ghost"
                      aria-label={`Move provider ${index + 1} down`}
                      disabled={index === providers.length - 1}
                      onClick={() => {
                        const next = [...providers];
                        [next[index], next[index + 1]] = [
                          next[index + 1],
                          next[index],
                        ];
                        [
                          providerKeys.current[index],
                          providerKeys.current[index + 1],
                        ] = [
                          providerKeys.current[index + 1],
                          providerKeys.current[index],
                        ];
                        onChange(setConfigValue(value, path, next));
                      }}
                    >
                      <ChevronDown className="h-3.5 w-3.5" />
                    </Button>
                    <Button
                      size="sm"
                      variant="danger"
                      iconLeft={<Trash2 className="h-3 w-3" />}
                      onClick={() => {
                        providerKeys.current.splice(index, 1);
                        onChange(
                          setConfigValue(
                            value,
                            path,
                            providers.filter(
                              (_, providerIndex) => providerIndex !== index,
                            ),
                          ),
                        );
                      }}
                    >
                      Remove
                    </Button>
                  </div>
                </div>
                <div className="grid grid-cols-1 gap-3 lg:grid-cols-2">
                  {Object.entries(properties).map(([key, child]) =>
                    key === (variant?.property ?? 'kind') ? null : (
                      <ConfigNode
                        key={key}
                        rootSchema={rootSchema}
                        schema={child}
                        path={`${providerRoot}.${key}`}
                        value={value}
                        defaultConfig={defaultConfig}
                        effectiveConfig={effectiveConfig}
                        overrides={overrides}
                        issues={issues}
                        onChange={onChange}
                        depth={2}
                      />
                    ),
                  )}
                </div>
              </div>
            );
          })}
        </div>
      ) : (
        <Notice tone="warning" title="No admin providers in the file">
          Ensure an environment or other deployment access path exists before
          saving and restarting.
        </Notice>
      )}
    </div>
  );
}

function RecurringJobsEditor({
  rootSchema,
  schema,
  path,
  value,
  defaultConfig,
  effectiveConfig,
  overrides,
  issues,
  onChange,
}: {
  rootSchema: ConfigSchema;
  schema: ConfigSchema;
  path: string;
  value: JsonObject;
  defaultConfig: JsonObject;
  effectiveConfig: JsonObject;
  overrides: ConfigOverrideInfo[];
  issues: ConfigValidationIssue[];
  onChange: (value: unknown) => void;
}) {
  const configured = isJsonObject(getConfigValue(value, path))
    ? (getConfigValue(value, path) as JsonObject)
    : {};
  const defaults = isJsonObject(getConfigValue(defaultConfig, path))
    ? (getConfigValue(defaultConfig, path) as JsonObject)
    : {};
  const effective = isJsonObject(getConfigValue(effectiveConfig, path))
    ? (getConfigValue(effectiveConfig, path) as JsonObject)
    : {};
  const keys = [
    ...new Set([
      ...Object.keys(defaults),
      ...Object.keys(configured),
      ...Object.keys(effective),
    ]),
  ].sort();
  const jobSchema = isJsonObject(schema.additionalProperties)
    ? resolveConfigSchema(rootSchema, schema.additionalProperties)
    : {};
  return (
    <div
      className="space-y-3 rounded-sm border border-subtle/70 bg-bg/25 p-3"
      data-config-path={path}
      tabIndex={-1}
    >
      <div>
        <h3 className="text-xs font-medium text-text">Recurring Jobs</h3>
        <p className="mt-0.5 text-xs text-text-faint">
          Known scheduler jobs are listed with any unknown file keys preserved
          for validation.
        </p>
      </div>
      <div className="space-y-3">
        {keys.map((key) => (
          <div
            key={key}
            className="rounded-sm border border-subtle bg-panel-strong/30 p-3"
          >
            <div className="mb-2 flex items-center justify-between gap-2">
              <span className="break-all font-mono text-xs text-text">
                {key}
              </span>
              <div className="flex items-center gap-1">
                {!Object.hasOwn(defaults, key) ? (
                  <Badge tone="warn">Unknown key</Badge>
                ) : null}
                <Button
                  size="sm"
                  variant="ghost"
                  iconLeft={<RotateCcw className="h-3 w-3" />}
                  disabled={!Object.hasOwn(defaults, key)}
                  onClick={() =>
                    onChange(
                      setConfigValue(
                        value,
                        `${path}.${key}`,
                        cloneJson(defaults[key]),
                      ),
                    )
                  }
                >
                  Reset
                </Button>
                <Button
                  size="sm"
                  variant="ghost"
                  iconLeft={<X className="h-3 w-3" />}
                  onClick={() =>
                    onChange(unsetConfigValue(value, `${path}.${key}`))
                  }
                >
                  Unset
                </Button>
              </div>
            </div>
            <div className="grid grid-cols-1 gap-3 lg:grid-cols-2">
              {Object.entries(objectProperties(rootSchema, jobSchema)).map(
                ([field, child]) => (
                  <ConfigNode
                    key={field}
                    rootSchema={rootSchema}
                    schema={child}
                    path={`${path}.${key}.${field}`}
                    value={value}
                    defaultConfig={defaultConfig}
                    effectiveConfig={effectiveConfig}
                    overrides={overrides}
                    issues={issues}
                    onChange={onChange}
                    depth={2}
                  />
                ),
              )}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

function focusConfigPath(
  path: string,
  setOpenCategories?: (
    value: (current: Record<string, boolean>) => Record<string, boolean>,
  ) => void,
) {
  const focus = () => {
    const targets =
      document.querySelectorAll<HTMLElement>('[data-config-path]');
    const exact = [...targets].find(
      (target) => target.dataset.configPath === path,
    );
    const parent = [...targets]
      .filter((target) => {
        const candidate = target.dataset.configPath;
        return Boolean(
          candidate &&
            (path.startsWith(`${candidate}.`) ||
              path.startsWith(`${candidate}[`)),
        );
      })
      .sort(
        (left, right) =>
          (right.dataset.configPath?.length ?? 0) -
          (left.dataset.configPath?.length ?? 0),
      )[0];
    const target = exact ?? parent;
    target?.scrollIntoView({ block: 'center', behavior: 'smooth' });
    const fieldControlSelector =
      'input:not([type="hidden"]):not(:disabled), select:not(:disabled), textarea:not(:disabled), [role="switch"]:not([aria-disabled="true"])';
    const fieldControl = target?.matches(fieldControlSelector)
      ? target
      : target?.querySelector<HTMLElement>(fieldControlSelector);
    const actions = target?.matches('button:not(:disabled)')
      ? [target]
      : [
          ...(target?.querySelectorAll<HTMLElement>('button:not(:disabled)') ??
            []),
        ];
    const primaryAction = actions.find(
      (action) =>
        !['Reset', 'Unset'].includes(action.textContent?.trim() ?? ''),
    );
    (fieldControl ?? primaryAction ?? actions[0] ?? target)?.focus();
  };
  if (setOpenCategories) {
    const category = categoryForPath(path);
    setOpenCategories((current) => ({ ...current, [category]: true }));
    requestAnimationFrame(focus);
  } else {
    requestAnimationFrame(focus);
  }
}
