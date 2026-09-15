import { createFileRoute } from '@tanstack/react-router';
import { CheckCircle2, Download, Save } from 'lucide-react';
import { useEffect, useRef, useState } from 'react';
import { toast } from 'sonner';
import {
  Button,
  Card,
  CardBody,
  CardHeader,
  cx,
  Field,
  INPUT_CLASS,
  PageContainer,
  Section,
  Skeleton,
  SkeletonRow,
  Spinner,
} from '../components/ui/primitives';
import {
  RelativeOffsetTime,
  RelativeTime,
} from '../components/ui/RelativeTime';
import { downloadJson } from '../lib/api';
import { formatAbsolute, useLocale, useTimezone } from '../lib/locale';
import {
  useConfigCurrent,
  useConfigDraft,
  useConfigHistory,
  useSaveDraft,
  useStatus,
  useValidateConfig,
} from '../lib/queries';

const ALL_BCP47_LOCALES = [
  'af-ZA',
  'am-ET',
  'ar-AE',
  'ar-BH',
  'ar-DZ',
  'ar-EG',
  'ar-IQ',
  'ar-JO',
  'ar-KW',
  'ar-LB',
  'ar-LY',
  'ar-MA',
  'ar-OM',
  'ar-QA',
  'ar-SA',
  'ar-SY',
  'ar-TN',
  'ar-YE',
  'as-IN',
  'az-Latn-AZ',
  'be-BY',
  'bg-BG',
  'bn-BD',
  'bn-IN',
  'bs-BA',
  'ca-ES',
  'cs-CZ',
  'cy-GB',
  'da-DK',
  'de-AT',
  'de-CH',
  'de-DE',
  'de-LI',
  'de-LU',
  'el-CY',
  'el-GR',
  'en-AU',
  'en-CA',
  'en-GB',
  'en-IE',
  'en-IN',
  'en-NZ',
  'en-PH',
  'en-SG',
  'en-US',
  'en-ZA',
  'es-AR',
  'es-BO',
  'es-CL',
  'es-CO',
  'es-CR',
  'es-DO',
  'es-EC',
  'es-ES',
  'es-GT',
  'es-HN',
  'es-MX',
  'es-NI',
  'es-PA',
  'es-PE',
  'es-PR',
  'es-PY',
  'es-SV',
  'es-US',
  'es-UY',
  'es-VE',
  'et-EE',
  'eu-ES',
  'fa-IR',
  'fi-FI',
  'fil-PH',
  'fr-BE',
  'fr-CA',
  'fr-CH',
  'fr-FR',
  'fr-LU',
  'fr-MC',
  'ga-IE',
  'gl-ES',
  'gu-IN',
  'he-IL',
  'hi-IN',
  'hr-BA',
  'hr-HR',
  'hu-HU',
  'hy-AM',
  'id-ID',
  'is-IS',
  'it-CH',
  'it-IT',
  'ja-JP',
  'ka-GE',
  'kk-KZ',
  'km-KH',
  'kn-IN',
  'ko-KR',
  'lo-LA',
  'lt-LT',
  'lv-LV',
  'mk-MK',
  'ml-IN',
  'mn-MN',
  'mr-IN',
  'ms-BN',
  'ms-MY',
  'mt-MT',
  'nb-NO',
  'ne-NP',
  'nl-BE',
  'nl-NL',
  'nn-NO',
  'or-IN',
  'pa-IN',
  'pl-PL',
  'ps-AF',
  'pt-BR',
  'pt-PT',
  'ro-MD',
  'ro-RO',
  'ru-RU',
  'si-LK',
  'sk-SK',
  'sl-SI',
  'sq-AL',
  'sr-Cyrl-RS',
  'sr-Latn-RS',
  'sv-FI',
  'sv-SE',
  'sw-KE',
  'sw-TZ',
  'ta-IN',
  'ta-LK',
  'te-IN',
  'th-TH',
  'tr-TR',
  'uk-UA',
  'ur-IN',
  'ur-PK',
  'uz-Latn-UZ',
  'vi-VN',
  'zh-CN',
  'zh-HK',
  'zh-MO',
  'zh-SG',
  'zh-TW',
  'zu-ZA',
];

function getAvailableLocales(
  displayLocale: string,
): { tag: string; label: string }[] {
  const supported = Intl.DateTimeFormat.supportedLocalesOf(ALL_BCP47_LOCALES);
  const dn = new Intl.DisplayNames([displayLocale], { type: 'language' });
  const regionDn = new Intl.DisplayNames([displayLocale], { type: 'region' });
  const items = supported.map((tag) => {
    const loc = new Intl.Locale(tag);
    const lang = dn.of(loc.language) ?? loc.language;
    const region = loc.region ? (regionDn.of(loc.region) ?? loc.region) : '';
    const label = region ? `${lang} (${region}) — ${tag}` : `${lang} — ${tag}`;
    return { tag, label };
  });
  items.sort((a, b) => a.label.localeCompare(b.label, displayLocale));
  return items;
}

export const Route = createFileRoute('/settings')({
  component: SettingsPage,
});

function SettingsPage() {
  const status = useStatus();
  const [downloadingDatabaseSnapshot, setDownloadingDatabaseSnapshot] =
    useState(false);
  const downloadingDatabaseSnapshotRef = useRef(false);
  const { locale, effective, setLocale } = useLocale();
  const { timezone, effective: effectiveTz, setTimezone } = useTimezone();
  const [now, setNow] = useState(new Date());

  useEffect(() => {
    const id = setInterval(() => setNow(new Date()), 1000);
    return () => clearInterval(id);
  }, []);

  // The snapshot is a plain download rather than a mutation, so its in-flight
  // state is local. The ref rejects a second click landing in the same tick,
  // before React has re-rendered the Button as disabled.
  const handleDownloadDatabaseSnapshot = async () => {
    if (downloadingDatabaseSnapshotRef.current) return;
    downloadingDatabaseSnapshotRef.current = true;
    setDownloadingDatabaseSnapshot(true);
    try {
      await downloadDatabaseResourcesSnapshot();
    } finally {
      downloadingDatabaseSnapshotRef.current = false;
      setDownloadingDatabaseSnapshot(false);
    }
  };

  return (
    <PageContainer>
      <Section
        title="Settings"
        subtitle="Version, localization, configuration drafts, and data backups."
      >
        {/* Version card */}
        <Card data-testid="version-card">
          <CardHeader
            title="Version"
            subtitle={
              status.isLoading ? (
                <Skeleton
                  as="span"
                  className="block h-8 w-72 max-w-full sm:h-4"
                />
              ) : (
                <span className="block min-h-8 sm:min-h-4">
                  {status.data ? (
                    <>
                      cc-lb {status.data.version} · {status.data.git_sha} ·
                      started{' '}
                      <RelativeOffsetTime
                        offsetSeconds={-status.data.uptime_secs}
                      />
                    </>
                  ) : (
                    '—'
                  )}
                </span>
              )
            }
          />
          <CardBody
            data-testid="version-metadata"
            className="space-y-3 text-xs"
          >
            <div className="grid grid-cols-3 gap-3">
              <Row
                label="Rust"
                value={
                  status.isLoading ? (
                    <Skeleton className="h-4 w-16" />
                  ) : (
                    (status.data?.build.rust_version ?? '—')
                  )
                }
              />
              <Row
                label="Profile"
                value={
                  status.isLoading ? (
                    <Skeleton className="h-4 w-12" />
                  ) : (
                    (status.data?.build.profile ?? '—')
                  )
                }
              />
              <Row
                label="Generation"
                value={
                  status.isLoading ? (
                    <Skeleton className="h-4 w-8" />
                  ) : (
                    (status.data?.generation ?? '—')
                  )
                }
              />
            </div>
            <div className="pt-2 border-t border-subtle/40">
              <div className="text-text-faint text-[10px] uppercase tracking-wider mb-0.5">
                Build target
              </div>
              <div className="min-h-4 font-mono break-all">
                {status.isLoading ? (
                  <Skeleton className="h-4 w-64 max-w-full" />
                ) : (
                  (status.data?.build.target ?? '—')
                )}
              </div>
            </div>
          </CardBody>
        </Card>

        {/* Localization */}
        <Card>
          <CardHeader
            title="Localization"
            subtitle="Locale and timezone for absolute timestamp display (stored locally in your browser)."
          />
          <CardBody>
            <div className="flex flex-col md:flex-row gap-4">
              <div className="flex-1 min-w-0">
                <Field label="Locale">
                  <select
                    className={INPUT_CLASS}
                    value={locale}
                    onChange={(e) => setLocale(e.target.value)}
                  >
                    <option value="auto">Auto (browser)</option>
                    {getAvailableLocales(effective).map((loc) => (
                      <option key={loc.tag} value={loc.tag}>
                        {loc.label}
                      </option>
                    ))}
                  </select>
                </Field>
              </div>
              <div className="flex-1 min-w-0">
                <Field label="Timezone">
                  <select
                    className={INPUT_CLASS}
                    value={timezone}
                    onChange={(e) => setTimezone(e.target.value)}
                  >
                    <option value="auto">Auto (browser)</option>
                    {Intl.supportedValuesOf ? (
                      Intl.supportedValuesOf('timeZone').map((tz) => (
                        <option key={tz} value={tz}>
                          {tz}
                        </option>
                      ))
                    ) : (
                      <>
                        <option value="UTC">UTC</option>
                        <option value="Asia/Seoul">Asia/Seoul</option>
                        <option value="Asia/Tokyo">Asia/Tokyo</option>
                        <option value="Asia/Shanghai">Asia/Shanghai</option>
                        <option value="Europe/London">Europe/London</option>
                        <option value="Europe/Berlin">Europe/Berlin</option>
                        <option value="America/New_York">
                          America/New_York
                        </option>
                        <option value="America/Los_Angeles">
                          America/Los_Angeles
                        </option>
                      </>
                    )}
                  </select>
                </Field>
              </div>
            </div>
            <div className="mt-3 text-xs text-text-muted">
              Current preview: {formatAbsolute(now, effective, effectiveTz)}
            </div>
          </CardBody>
        </Card>

        {/* Config draft */}
        <ConfigDraftSection />

        {/* History */}
        <ConfigHistorySection />

        {/* Database export */}
        <Section
          title="Data & Backups"
          subtitle="Export resources stored in the cc-lb database."
        >
          <Card>
            <CardHeader
              title="Database resources snapshot"
              subtitle="Download a JSON snapshot of upstreams, principals, plugins, and chains stored in the database."
            />
            <CardBody>
              <Button
                iconLeft={<Download className="w-4 h-4" />}
                loading={downloadingDatabaseSnapshot}
                onClick={handleDownloadDatabaseSnapshot}
              >
                {downloadingDatabaseSnapshot
                  ? 'Downloading...'
                  : 'Download database snapshot'}
              </Button>
            </CardBody>
          </Card>
        </Section>
      </Section>
    </PageContainer>
  );
}

function ConfigDraftSection() {
  const draft = useConfigDraft();
  const current = useConfigCurrent();
  const save = useSaveDraft();
  const validate = useValidateConfig();
  const [editor, setEditor] = useState<{
    text: string;
    savedText: string;
    revision: number;
    savedAtUnixSecs: number | null;
    hasSavedDraft: boolean;
  } | null>(null);
  const [localValidation, setLocalValidation] = useState<{
    revision: number;
    error: string | null;
  } | null>(null);

  useEffect(() => {
    const draftData = draft.data;
    if (!draftData) return;

    const source = draftData.draft ?? current.data;
    if (source === undefined) return;

    const sourceText = JSON.stringify(source, null, 2);
    if (sourceText === undefined) return;

    setEditor((existing) => {
      if (existing && existing.text !== existing.savedText) return existing;
      // A successful save can advance the editor before an invalidated draft
      // query finishes. Never replace that confirmed revision with stale data.
      if (existing && existing.revision > draftData.revision) return existing;

      const hasSavedDraft = draftData.draft != null;
      const savedAtUnixSecs = hasSavedDraft
        ? draftData.saved_at_unix_secs
        : null;
      if (
        existing?.text === sourceText &&
        existing.savedText === sourceText &&
        existing.revision === draftData.revision &&
        existing.savedAtUnixSecs === savedAtUnixSecs &&
        existing.hasSavedDraft === hasSavedDraft
      ) {
        return existing;
      }

      return {
        text: sourceText,
        savedText: sourceText,
        revision: draftData.revision,
        savedAtUnixSecs,
        hasSavedDraft,
      };
    });
  }, [current.data, draft.data]);

  const draftUnavailable = draft.data === undefined;
  const currentConfigRequired = draft.data?.draft == null;
  const currentUnavailable =
    currentConfigRequired && current.data === undefined;
  const editorPending =
    editor == null &&
    ((draftUnavailable && draft.isPending) ||
      (currentUnavailable && current.isPending));
  const editorLoadError =
    editor == null &&
    ((draftUnavailable && draft.isError) ||
      (currentUnavailable && current.isError));
  const editorRetrying =
    (draftUnavailable && draft.isFetching) ||
    (currentUnavailable && current.isFetching);

  const draftRevision = draft.data?.revision ?? null;
  const editorRevision = editor?.revision ?? null;
  const editorDirty = editor ? editor.text !== editor.savedText : false;
  const serverRevisionChanged =
    draftRevision != null &&
    editorRevision != null &&
    draftRevision !== editorRevision;
  const canSave =
    editor != null &&
    (editorDirty || (!editor.hasSavedDraft && !serverRevisionChanged));
  const editorValidation =
    localValidation?.revision === editorRevision ? localValidation : null;
  const serverValidationMatchesEditor =
    draftRevision != null && draftRevision === editorRevision;
  const lastValidatedRevision = editorValidation
    ? editorValidation.revision
    : serverValidationMatchesEditor
      ? (draft.data?.last_validated_revision ?? null)
      : null;
  const lastValidationError = editorValidation
    ? editorValidation.error
    : serverValidationMatchesEditor
      ? (draft.data?.last_validation_error ?? null)
      : null;
  const canValidate =
    editor?.hasSavedDraft === true && !editorDirty && !serverRevisionChanged;
  const lastValidatedLabel = lastValidationError
    ? `error @ rev ${lastValidatedRevision ?? '—'}`
    : lastValidatedRevision != null
      ? `rev ${lastValidatedRevision}`
      : '—';

  const savePending = save.isPending;
  const validatePending = validate.isPending;
  const draftMutationLabel = savePending
    ? 'Saving draft...'
    : validatePending
      ? 'Validating draft...'
      : null;
  const editorGuidance = editorDirty
    ? serverRevisionChanged
      ? `The server draft is revision ${draftRevision}, while this editor started from revision ${editorRevision}. Save checks revision ${editorRevision}; Validate stays disabled until these changes are saved.`
      : 'Save your editor changes before validating. Validate uses the saved server draft.'
    : serverRevisionChanged
      ? `Waiting for server draft revision ${draftRevision} to match editor revision ${editorRevision} before validating.`
      : editor && !editor.hasSavedDraft
        ? 'Save this configuration as a server draft before validating.'
        : null;

  const handleRetryEditor = () => {
    if (draftUnavailable) void draft.refetch();
    if (currentUnavailable) void current.refetch();
  };

  const handleSave = () => {
    if (!editor || !canSave) return;

    const savedText = editor.text;
    const startRevision = editor.revision;
    try {
      const parsed = JSON.parse(savedText) as Record<string, unknown>;
      save.mutate(
        { draft: parsed, expected_revision: startRevision },
        {
          onSuccess: (response) => {
            setEditor((active) => {
              if (!active || active.revision !== startRevision) {
                return active;
              }
              return {
                ...active,
                savedText,
                revision: response.revision,
                savedAtUnixSecs: response.saved_at_unix_secs,
                hasSavedDraft: true,
              };
            });
            setLocalValidation(null);
            toast.success('Draft saved');
          },
        },
      );
    } catch {
      toast.error('Draft is not valid JSON');
    }
  };

  const handleValidate = () => {
    if (!editor || !canValidate) return;

    validate.mutate(editor.revision, {
      onSuccess: (result) => {
        const error = result.valid
          ? null
          : (result.error ?? 'Validation failed');
        setLocalValidation({ revision: result.revision, error });
        if (result.valid) {
          toast.success('Draft valid');
        } else {
          toast.error(`Invalid: ${error}`);
        }
      },
    });
  };

  return (
    <Section
      title="Configuration Draft"
      subtitle="Edit and validate the saved configuration draft."
    >
      <Card>
        <CardHeader
          title="Draft"
          action={
            <div className="flex items-center gap-2">
              <Button
                size="sm"
                iconLeft={<Save className="w-3 h-3" />}
                loading={savePending}
                disabled={!canSave || !editor.text}
                onClick={handleSave}
              >
                {savePending ? 'Saving...' : 'Save'}
              </Button>
              <Button
                size="sm"
                iconLeft={<CheckCircle2 className="w-3 h-3" />}
                loading={validatePending}
                disabled={!canValidate}
                aria-describedby="config-pipeline-status"
                onClick={handleValidate}
              >
                {validatePending ? 'Validating...' : 'Validate'}
              </Button>
            </div>
          }
        />
        <CardBody className="space-y-3">
          <div
            data-testid="draft-metadata"
            className="flex min-h-4 flex-wrap gap-4 text-xs text-text-faint"
          >
            <div className="inline-flex items-center gap-1">
              Revision:
              <div
                data-testid="draft-revision-slot"
                className="inline-flex h-4 min-w-8 items-center font-mono"
              >
                {editorPending ? (
                  <Skeleton className="h-3 w-8" />
                ) : (
                  (editor?.revision ?? '—')
                )}
              </div>
            </div>
            <div className="inline-flex items-center gap-1">
              Last validated:
              <div
                data-testid="draft-last-validated-slot"
                className={cx(
                  'inline-flex h-4 min-w-24 items-center font-mono',
                  lastValidationError ? 'text-red-400' : undefined,
                )}
              >
                {editorPending ? (
                  <Skeleton className="h-3 w-20" />
                ) : (
                  lastValidatedLabel
                )}
              </div>
            </div>
            <div className="inline-flex items-center gap-1">
              Saved at:
              <div
                data-testid="draft-saved-at-slot"
                className="inline-flex h-4 min-w-24 items-center font-mono"
              >
                {editorPending ? (
                  <Skeleton className="h-3 w-20" />
                ) : editor?.savedAtUnixSecs ? (
                  <RelativeTime ts={new Date(editor.savedAtUnixSecs * 1000)} />
                ) : (
                  '—'
                )}
              </div>
            </div>
          </div>
          {editorLoadError ? (
            <div
              className="flex items-center justify-between gap-3 rounded-sm border border-red-500/30 bg-red-500/10 p-3 text-xs text-red-200"
              role="alert"
            >
              <span>
                {draftUnavailable && draft.isError
                  ? 'Failed to load the configuration draft.'
                  : 'Failed to load the current configuration.'}
              </span>
              <Button
                size="sm"
                loading={editorRetrying}
                disabled={editorRetrying}
                onClick={handleRetryEditor}
              >
                {editorRetrying ? 'Retrying...' : 'Retry'}
              </Button>
            </div>
          ) : null}
          <textarea
            data-testid="config-draft-editor"
            className="w-full min-h-[260px] p-3 text-xs font-mono bg-panel-strong border border-subtle rounded-sm placeholder:text-text-faint focus:border-accent focus:outline-none disabled:cursor-not-allowed disabled:opacity-50"
            style={{ lineHeight: 1.5 }}
            value={editor?.text ?? ''}
            disabled={!editor}
            onChange={(event) =>
              setEditor((active) =>
                active ? { ...active, text: event.target.value } : active,
              )
            }
            placeholder="JSON config draft…"
          />
          <div
            id="config-pipeline-status"
            data-testid="config-pipeline-status"
            role="status"
            aria-live="polite"
            className="min-h-4 text-xs text-text-muted"
          >
            {draftMutationLabel ? (
              <span className="inline-flex items-center gap-1.5">
                <Spinner className="w-3 h-3 text-accent" />
                {draftMutationLabel}
              </span>
            ) : (
              editorGuidance
            )}
          </div>
        </CardBody>
      </Card>
    </Section>
  );
}

const HISTORY_CELL_CLASS_NAMES = ['px-4', 'px-4'] as const;
const HISTORY_SKELETON_CLASS_NAMES = ['ml-auto w-8', 'w-24'] as const;
const HISTORY_LOADING_ROW_IDS = [0, 1, 2, 3] as const;

function ConfigHistorySection() {
  const history = useConfigHistory();
  return (
    <Section title="Saved Config History" subtitle="Last 20 saved revisions">
      <Card>
        <div
          data-testid="config-history-slot"
          className="min-h-[173px] overflow-x-auto sm:min-h-[163px]"
        >
          <table className="min-w-[400px] w-full font-mono text-xs">
            <thead className="table-header sticky top-0 z-10">
              <tr className="text-[10px] uppercase tracking-wider">
                <th className="text-right px-4 py-2">Rev</th>
                <th className="text-left px-4 py-2">Saved</th>
              </tr>
            </thead>
            <tbody>
              {history.isLoading ? (
                HISTORY_LOADING_ROW_IDS.map((id) => (
                  <SkeletonRow
                    key={id}
                    cols={2}
                    cellClassNames={HISTORY_CELL_CLASS_NAMES}
                    skeletonClassNames={HISTORY_SKELETON_CLASS_NAMES}
                  />
                ))
              ) : history.data?.history.length ? (
                history.data.history.map((h) => (
                  <tr key={h.revision} className="border-b border-row">
                    <td className="px-4 py-2 text-right">{h.revision}</td>
                    <td className="px-4 py-2">
                      <RelativeTime
                        ts={new Date(h.applied_at_unix_secs * 1000)}
                      />
                    </td>
                  </tr>
                ))
              ) : (
                <tr>
                  <td
                    colSpan={2}
                    className="px-4 py-12 text-center text-xs text-text-faint"
                  >
                    No saved config history available.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </Card>
    </Section>
  );
}

function Row({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div className="flex items-center justify-between">
      <span className="text-text-faint">{label}</span>
      <div className="text-right">{value}</div>
    </div>
  );
}

async function downloadDatabaseResourcesSnapshot() {
  try {
    await downloadJson(
      '/admin/v1/export',
      `cc-lb-database-resources-${new Date().toISOString().slice(0, 16)}.json`,
    );
    toast.success('Database snapshot downloaded');
  } catch (e) {
    toast.error(`Database snapshot download failed: ${String(e)}`);
  }
}
