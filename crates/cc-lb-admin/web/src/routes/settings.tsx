import { createFileRoute } from '@tanstack/react-router';
import { Download } from 'lucide-react';
import { useEffect, useRef, useState } from 'react';
import { toast } from 'sonner';
import { ConfigEditorSection } from '../components/settings-config';
import {
  Button,
  Card,
  CardBody,
  CardHeader,
  Field,
  INPUT_CLASS,
  PageContainer,
  Section,
  Skeleton,
} from '../components/ui/primitives';
import {
  RelativeOffsetTime,
  RelativeTime,
} from '../components/ui/RelativeTime';
import { type ConfigHistoryResponse, downloadJson } from '../lib/api';
import { formatAbsolute, useLocale, useTimezone } from '../lib/locale';
import { useConfigHistory, useStatus } from '../lib/queries';

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
  const configHistory = useConfigHistory();
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

        <ConfigEditorSection history={configHistory} />

        {/* History */}
        <ConfigHistorySection history={configHistory} />

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

const HISTORY_LOADING_ROW_IDS = [0, 1, 2, 3] as const;

interface ConfigHistoryQueryResult {
  data?: ConfigHistoryResponse;
  isLoading: boolean;
  isError: boolean;
  isFetching: boolean;
  refetch: () => unknown;
}

function ConfigHistorySection({
  history,
}: {
  history: ConfigHistoryQueryResult;
}) {
  return (
    <Section title="Saved Config History" subtitle="Last 20 saved revisions">
      <Card>
        <div
          data-testid="config-history-slot"
          className="min-h-[173px] sm:min-h-[163px]"
        >
          <div
            className="grid grid-cols-[minmax(4rem,auto)_minmax(0,1fr)] border-b border-subtle px-4 py-2 text-[10px] uppercase tracking-wider text-text-faint"
            role="row"
          >
            <span role="columnheader">Rev</span>
            <span role="columnheader">Saved</span>
          </div>
          <div className="font-mono text-xs">
            {history.isError ? (
              <div
                role="alert"
                className="flex min-h-20 flex-col items-center justify-center gap-2 border-b border-row px-4 py-4 text-center"
              >
                <span className="text-text-muted">
                  Saved config history could not be loaded. Restart status may
                  be incomplete until this request succeeds.
                </span>
                <Button
                  size="sm"
                  loading={history.isFetching}
                  onClick={() => void history.refetch()}
                >
                  Retry history
                </Button>
              </div>
            ) : null}
            {history.isLoading ? (
              HISTORY_LOADING_ROW_IDS.map((id) => (
                <div
                  key={id}
                  className="grid grid-cols-[minmax(4rem,auto)_minmax(0,1fr)] items-center gap-3 border-b border-row px-4 py-2"
                >
                  <Skeleton className="h-4 w-8" />
                  <Skeleton className="h-4 w-24 max-w-full" />
                </div>
              ))
            ) : history.data?.entries.length ? (
              history.data.entries.map((entry) => (
                <div
                  key={entry.revision}
                  className="grid grid-cols-[minmax(4rem,auto)_minmax(0,1fr)] items-center gap-3 border-b border-row px-4 py-2"
                  role="row"
                >
                  <span role="cell">{entry.revision}</span>
                  <span className="min-w-0" role="cell">
                    <RelativeTime
                      ts={new Date(entry.saved_at_unix_secs * 1000)}
                    />
                  </span>
                </div>
              ))
            ) : !history.isError ? (
              <div className="px-4 py-12 text-center text-xs text-text-faint">
                No saved config history available.
              </div>
            ) : null}
          </div>
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
