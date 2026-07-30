import { Collapsible as BaseCollapsible } from '@base-ui/react/collapsible';
import { createFileRoute } from '@tanstack/react-router';
import {
  AlertTriangle,
  CheckCircle2,
  Download,
  PlayCircle,
  RefreshCw,
  Save,
} from 'lucide-react';
import { useEffect, useState } from 'react';
import { toast } from 'sonner';
import {
  Button,
  Card,
  CardBody,
  CardHeader,
  cx,
  Field,
  INPUT_CLASS,
  Modal,
  PageContainer,
  Section,
  Skeleton,
  SkeletonRow,
  StatusBadge,
} from '../components/ui/primitives';
import {
  RelativeOffsetTime,
  RelativeTime,
} from '../components/ui/RelativeTime';
import { downloadJson } from '../lib/api';
import { formatAbsolute, useLocale, useTimezone } from '../lib/locale';
import {
  useApplyConfig,
  useConfigCurrent,
  useConfigDraft,
  useConfigHistory,
  useConfigSchema,
  useReloadConfig,
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
  const [rotateOpen, setRotateOpen] = useState(false);
  const { locale, effective, setLocale } = useLocale();
  const { timezone, effective: effectiveTz, setTimezone } = useTimezone();
  const [now, setNow] = useState(new Date());

  useEffect(() => {
    const id = setInterval(() => setNow(new Date()), 1000);
    return () => clearInterval(id);
  }, []);

  return (
    <PageContainer>
      <Section
        title="Settings"
        subtitle="Admin self-service, configuration draft pipeline, and exports."
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

        {/* Token rotation */}
        <Card>
          <CardHeader
            title="Admin Token"
            subtitle="Rotate the primary bearer token. All current admin sessions will be invalidated."
          />
          <CardBody>
            <Button
              id="btn-rotate-token"
              variant="danger"
              size="md"
              iconLeft={<AlertTriangle className="w-4 h-4" />}
              onClick={() => setRotateOpen(true)}
            >
              Rotate token
            </Button>
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

        {/* Config draft pipeline */}
        <ConfigDraftSection />

        {/* History */}
        <ConfigHistorySection />

        {/* Restart-required matrix */}
        <RestartRequiredMatrix />

        {/* Export */}
        <Card>
          <CardHeader
            title="Configuration Export"
            subtitle="Download a JSON snapshot of all upstreams, principals, plugins, and chains."
          />
          <CardBody>
            <Button
              iconLeft={<Download className="w-4 h-4" />}
              onClick={() => downloadExport()}
            >
              Download export.json
            </Button>
          </CardBody>
        </Card>

        <Modal
          open={rotateOpen}
          onOpenChange={setRotateOpen}
          title="Rotate admin token?"
          footer={
            <>
              <Button onClick={() => setRotateOpen(false)}>Cancel</Button>
              <Button
                variant="danger"
                onClick={() => {
                  toast.success('Token rotation queued (mock)');
                  setRotateOpen(false);
                }}
              >
                Confirm rotate
              </Button>
            </>
          }
        >
          <p className="text-sm text-text-muted">
            Rotation invalidates the current admin token. You will need the new
            token to access this dashboard.
          </p>
        </Modal>
      </Section>
    </PageContainer>
  );
}

function ConfigDraftSection() {
  const draft = useConfigDraft();
  const current = useConfigCurrent();
  const schema = useConfigSchema();
  const save = useSaveDraft();
  const validate = useValidateConfig();
  const apply = useApplyConfig();
  const reload = useReloadConfig();
  const [text, setText] = useState('');

  const draftRevision = draft.data?.revision ?? null;
  const lastValidatedRevision = draft.data?.last_validated_revision ?? null;
  const lastValidationError = draft.data?.last_validation_error ?? null;
  const canApply =
    lastValidatedRevision != null &&
    lastValidationError == null &&
    draftRevision != null &&
    lastValidatedRevision === draftRevision;
  const lastValidatedLabel = lastValidationError
    ? `error @ rev ${lastValidatedRevision ?? '—'}`
    : lastValidatedRevision != null
      ? `rev ${lastValidatedRevision}`
      : '—';

  return (
    <Section
      title="Configuration Draft"
      subtitle="Edit → validate → apply pipeline"
    >
      <Card>
        <CardHeader
          title="Draft"
          action={
            <div className="flex items-center gap-2">
              <Button
                size="sm"
                iconLeft={<Save className="w-3 h-3" />}
                disabled={!text}
                onClick={() => {
                  try {
                    const parsed = JSON.parse(text);
                    save.mutate(
                      { draft: parsed, expected_revision: draftRevision ?? 0 },
                      { onSuccess: () => toast.success('Draft saved') },
                    );
                  } catch {
                    toast.error('Draft is not valid JSON');
                  }
                }}
              >
                Save
              </Button>
              <Button
                size="sm"
                iconLeft={<CheckCircle2 className="w-3 h-3" />}
                onClick={() =>
                  validate.mutate(draftRevision ?? 0, {
                    onSuccess: (r) =>
                      toast.success(
                        r.valid ? 'Draft valid' : `Invalid: ${r.error}`,
                      ),
                  })
                }
              >
                Validate
              </Button>
              <Button
                size="sm"
                variant="primary"
                iconLeft={<PlayCircle className="w-3 h-3" />}
                disabled={!canApply}
                onClick={() =>
                  apply.mutate(lastValidatedRevision ?? 0, {
                    onSuccess: (r) =>
                      toast.success(`Applied revision ${r.applied_revision}`),
                  })
                }
              >
                Apply
              </Button>
              <Button
                size="sm"
                iconLeft={<RefreshCw className="w-3 h-3" />}
                onClick={() =>
                  reload.mutate(undefined, {
                    onSuccess: () => toast.success('Reload triggered'),
                  })
                }
              >
                Reload
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
                {draft.isLoading ? (
                  <Skeleton className="h-3 w-8" />
                ) : (
                  (draftRevision ?? '—')
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
                {draft.isLoading ? (
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
                {draft.isLoading ? (
                  <Skeleton className="h-3 w-20" />
                ) : draft.data?.saved_at_unix_secs ? (
                  <RelativeTime
                    ts={new Date(draft.data.saved_at_unix_secs * 1000)}
                  />
                ) : (
                  '—'
                )}
              </div>
            </div>
          </div>
          <textarea
            className="w-full min-h-[260px] p-3 text-xs font-mono bg-panel-strong border border-subtle rounded-sm placeholder:text-text-faint focus:border-accent focus:outline-none"
            style={{ lineHeight: 1.5 }}
            value={text}
            onChange={(e) => setText(e.target.value)}
            placeholder={
              current.data
                ? JSON.stringify(current.data, null, 2)
                : 'JSON config draft…'
            }
          />
          <div data-testid="config-checklist-slot" className="min-h-[20px]">
            {schema.isLoading ? (
              <Skeleton className="h-4 w-48" />
            ) : schema.data ? (
              <BaseCollapsible.Root className="text-xs">
                <BaseCollapsible.Trigger className="cursor-pointer text-text-faint">
                  Coverage checklist ({schema.data.coverage_checklist.length}{' '}
                  fields)
                </BaseCollapsible.Trigger>
                <BaseCollapsible.Panel className="overflow-hidden h-[var(--collapsible-panel-height)] transition-[height] duration-150 ease-out data-[ending-style]:h-0 data-[starting-style]:h-0">
                  <ul className="mt-2 grid grid-cols-1 md:grid-cols-2 gap-1 font-mono">
                    {schema.data.coverage_checklist.map((f) => (
                      <li key={f}>· {f}</li>
                    ))}
                  </ul>
                </BaseCollapsible.Panel>
              </BaseCollapsible.Root>
            ) : null}
          </div>
        </CardBody>
      </Card>
    </Section>
  );
}

const HISTORY_CELL_CLASS_NAMES = [
  'px-4',
  'px-4',
  'px-4',
  'px-4',
  'px-4',
  'px-4',
] as const;
const HISTORY_SKELETON_CLASS_NAMES = [
  'ml-auto w-8',
  'w-24',
  'ml-auto w-8',
  'ml-auto w-8',
  'ml-auto w-8',
  'mx-auto w-12',
] as const;
const HISTORY_LOADING_ROW_IDS = [0, 1, 2, 3] as const;

function ConfigHistorySection() {
  const history = useConfigHistory();
  return (
    <Section title="Configuration History" subtitle="Last 20 applied revisions">
      <Card>
        <div
          data-testid="config-history-slot"
          className="min-h-[173px] overflow-x-auto sm:min-h-[163px]"
        >
          <table className="min-w-[640px] w-full font-mono text-xs">
            <thead className="table-header sticky top-0 z-10">
              <tr className="text-[10px] uppercase tracking-wider">
                <th className="text-right px-4 py-2">Rev</th>
                <th className="text-left px-4 py-2">Applied</th>
                <th className="text-right px-4 py-2">Upstreams</th>
                <th className="text-right px-4 py-2">Principals</th>
                <th className="text-right px-4 py-2">Plugins</th>
                <th className="text-center px-4 py-2">TLS</th>
              </tr>
            </thead>
            <tbody>
              {history.isLoading ? (
                HISTORY_LOADING_ROW_IDS.map((id) => (
                  <SkeletonRow
                    key={id}
                    cols={6}
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
                    <td className="px-4 py-2 text-right tabular-nums">
                      {h.config_summary.upstreams}
                    </td>
                    <td className="px-4 py-2 text-right tabular-nums">
                      {h.config_summary.principals}
                    </td>
                    <td className="px-4 py-2 text-right tabular-nums">
                      {h.config_summary.plugin_count}
                    </td>
                    <td className="px-4 py-2 text-center">
                      <StatusBadge
                        tone={h.config_summary.tls_enabled ? 'ok' : 'neutral'}
                        label={h.config_summary.tls_enabled ? 'on' : 'off'}
                      />
                    </td>
                  </tr>
                ))
              ) : (
                <tr>
                  <td
                    colSpan={6}
                    className="px-4 py-12 text-center text-xs text-text-faint"
                  >
                    No history available.
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

const RESTART_MATRIX: { field: string; reason: string }[] = [
  {
    field: 'listener.proxy_addr',
    reason: 'Socket bindings are fixed at process start',
  },
  {
    field: 'listener.admin_addr',
    reason: 'Socket bindings are fixed at process start',
  },
  {
    field: 'listener.metrics_addr',
    reason: 'Socket bindings are fixed at process start',
  },
  { field: 'listener.tls.cert_path', reason: 'Listener TLS certificate' },
  { field: 'listener.tls.key_path', reason: 'Listener TLS key' },
  { field: 'tls.cert_path', reason: 'TLS certificate' },
  { field: 'tls.key_path', reason: 'TLS key' },
  { field: 'storage.path', reason: 'Storage backend' },
  { field: 'storage.url', reason: 'Storage backend' },
  { field: 'storage.pool', reason: 'Storage pool' },
  { field: 'aead.key_env', reason: 'Storage encryption key env' },
  { field: 'oauth.anthropic.client_id', reason: 'Anthropic OAuth client' },
  { field: 'oauth.anthropic.auth_url', reason: 'Anthropic OAuth endpoint' },
  { field: 'oauth.anthropic.token_url', reason: 'Anthropic OAuth endpoint' },
  { field: 'oauth.anthropic.redirect_uri', reason: 'Anthropic OAuth redirect' },
  { field: 'oauth.anthropic.scopes', reason: 'Anthropic OAuth scopes' },
];

function RestartRequiredMatrix() {
  return (
    <Section
      title="Hot-Reload Behaviour Matrix"
      subtitle="Which cc-lb.toml fields are hot-reloadable, and which require a process restart."
    >
      <Card>
        <div className="overflow-x-auto">
          <table className="min-w-[640px] w-full font-mono text-xs">
            <thead className="table-header sticky top-0 z-10">
              <tr className="text-[10px] uppercase tracking-wider">
                <th className="text-left px-4 py-2">Field</th>
                <th className="text-center px-4 py-2">Hot reload</th>
                <th className="text-left px-4 py-2">Reason</th>
              </tr>
            </thead>
            <tbody>
              <tr className="border-b border-row bg-[color:var(--color-ok)]/[0.06]">
                <td className="px-4 py-2">
                  upstreams · principals · plugin chains
                </td>
                <td className="px-4 py-2 text-center">
                  <StatusBadge tone="ok" label="Yes" />
                </td>
                <td className="px-4 py-2 text-text-muted">
                  Fully dynamic via admin DB; no restart required.
                </td>
              </tr>
              {RESTART_MATRIX.map((r) => (
                <tr key={r.field} className="border-b border-row">
                  <td className="px-4 py-2">{r.field}</td>
                  <td className="px-4 py-2 text-center">
                    <StatusBadge tone="danger" label="No" />
                  </td>
                  <td className="px-4 py-2 text-text-muted">{r.reason}</td>
                </tr>
              ))}
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

async function downloadExport() {
  try {
    await downloadJson(
      '/admin/v1/export',
      `cc-lb-export-${new Date().toISOString().slice(0, 16)}.json`,
    );
    toast.success('Export downloaded');
  } catch (e) {
    toast.error(`Export failed: ${String(e)}`);
  }
}
