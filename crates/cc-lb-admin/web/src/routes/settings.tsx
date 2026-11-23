import { createFileRoute, useNavigate } from '@tanstack/react-router';
import { Download } from 'lucide-react';
import { memo, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { toast } from 'sonner';
import { ConfigEditorSection } from '../components/settings-config';
import {
  Button,
  Card,
  Field,
  PageContainer,
  PageHeader,
  Section,
  Skeleton,
} from '../components/ui/primitives';
import { RelativeTime } from '../components/ui/RelativeTime';
import { Select } from '../components/ui/Select';
import {
  Table,
  TableCell,
  TableHead,
  TableHeadCell,
  TableRow,
} from '../components/ui/Table';
import { type ConfigHistoryResponse, downloadJson } from '../lib/api';
import { formatAbsolute, useLocale, useTimezone } from '../lib/locale';
import { useConfigHistory } from '../lib/queries';

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
  validateSearch: (search: Record<string, unknown>) => ({
    category: typeof search.category === 'string' ? search.category : undefined,
    field: typeof search.field === 'string' ? search.field : undefined,
    q: typeof search.q === 'string' ? search.q : undefined,
  }),
  component: SettingsPage,
});

function SettingsPage() {
  const configHistory = useConfigHistory();
  const [downloadingDatabaseSnapshot, setDownloadingDatabaseSnapshot] =
    useState(false);
  const downloadingDatabaseSnapshotRef = useRef(false);
  const { locale, effective, setLocale } = useLocale();
  const { timezone, effective: effectiveTz, setTimezone } = useTimezone();
  const search = Route.useSearch();
  const navigate = useNavigate({ from: Route.fullPath });

  // ConfigEditorSection is memoized; hand it props that only change when the
  // data it reads changes. The query result object itself is rebuilt on every
  // render, so pick the two fields the editor uses.
  const historyData = configHistory.data;
  const historyError = configHistory.isError;
  const editorHistory = useMemo(
    () => ({ data: historyData, isError: historyError }),
    [historyData, historyError],
  );
  const handleEditorNavigate = useCallback(
    (next: { category?: string; field?: string; q?: string }) =>
      navigate({
        search: {
          category: next.category,
          field: next.field,
          q: next.q,
        },
        resetScroll: false,
      }),
    [navigate],
  );

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
      {/* The top bar already names the page: the h1 stays for assistive
      tech only, with no visible header block above the first section. */}
      <PageHeader title="Settings" />

      <Section
        title="Localization"
        subtitle="Locale and timezone for absolute timestamp display (stored locally in your browser)."
      >
        <div>
          <div className="grid max-w-3xl gap-4 md:grid-cols-2">
            <Field label="Locale">
              <Select
                className="max-md:h-10"
                value={locale}
                onChange={setLocale}
                options={[
                  { value: 'auto', label: 'Auto (browser)' },
                  ...getAvailableLocales(effective).map((loc) => ({
                    value: loc.tag,
                    label: loc.label,
                  })),
                ]}
              />
            </Field>
            <Field label="Timezone">
              <Select
                className="max-md:h-10"
                value={timezone}
                onChange={setTimezone}
                options={[
                  { value: 'auto', label: 'Auto (browser)' },
                  ...(Intl.supportedValuesOf
                    ? Intl.supportedValuesOf('timeZone')
                    : FALLBACK_TIMEZONES
                  ).map((tz) => ({ value: tz, label: tz })),
                ]}
              />
            </Field>
          </div>
          <LivePreviewClock locale={effective} timezone={effectiveTz} />
        </div>
      </Section>

      <ConfigEditorSection
        history={editorHistory}
        category={search.category}
        field={search.field}
        query={search.q}
        onNavigate={handleEditorNavigate}
      />

      <ConfigHistorySection history={configHistory} />

      <Section
        title="Data & backups"
        subtitle="Export resources stored in the cc-lb database."
      >
        <div>
          <h3 className="text-title-card text-text">
            Database resources snapshot
          </h3>
          <p className="mt-1 max-w-[70ch] text-body text-text-muted">
            Download a JSON snapshot of upstreams, principals, plugins, and
            chains stored in the database.
          </p>
          <Button
            className="mt-4 max-md:h-11 max-md:w-full"
            iconLeft={<Download className="w-4 h-4" />}
            loading={downloadingDatabaseSnapshot}
            onClick={handleDownloadDatabaseSnapshot}
          >
            {downloadingDatabaseSnapshot
              ? 'Downloading...'
              : 'Download database snapshot'}
          </Button>
        </div>
      </Section>
    </PageContainer>
  );
}

/**
 * Owns the 1 Hz tick so only this line re-renders each second, not the page
 * (and with it the configuration editor).
 */
const LivePreviewClock = memo(function LivePreviewClock({
  locale,
  timezone,
}: {
  locale: string;
  timezone: string;
}) {
  const [now, setNow] = useState(() => new Date());
  useEffect(() => {
    const id = setInterval(() => setNow(new Date()), 1000);
    return () => clearInterval(id);
  }, []);
  return (
    <p className="mt-3 text-body-sm text-text-muted">
      Preview:{' '}
      <span className="text-text tabular-nums">
        {formatAbsolute(now, locale, timezone)}
      </span>
    </p>
  );
});

/** Used when the runtime cannot enumerate IANA zones. */
const FALLBACK_TIMEZONES = [
  'UTC',
  'Asia/Seoul',
  'Asia/Tokyo',
  'Asia/Shanghai',
  'Europe/London',
  'Europe/Berlin',
  'America/New_York',
  'America/Los_Angeles',
];

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
  const entries = history.data?.entries ?? [];
  const empty = !history.isError && !history.isLoading && !entries.length;
  return (
    <Section title="Saved config history" subtitle="Last 20 saved revisions">
      {/* An empty history is a sentence, not a bordered box. */}
      {empty ? (
        <p
          data-testid="config-history-slot"
          className="text-body text-text-muted"
        >
          No saved config history available.
        </p>
      ) : (
        <Card data-testid="config-history-slot">
          {history.isError ? (
            <div
              role="alert"
              className="flex flex-col items-center gap-3 px-4 py-6 text-center"
            >
              <p className="max-w-md text-body text-text-muted">
                Saved config history could not be loaded. Restart status may be
                incomplete until this request succeeds.
              </p>
              <Button
                size="sm"
                loading={history.isFetching}
                onClick={() => void history.refetch()}
              >
                Retry history
              </Button>
            </div>
          ) : history.isLoading || entries.length ? (
            <Table>
              <TableHead sticky={false}>
                <tr>
                  <TableHeadCell className="w-32">Revision</TableHeadCell>
                  <TableHeadCell>Saved</TableHeadCell>
                </tr>
              </TableHead>
              <tbody>
                {history.isLoading
                  ? HISTORY_LOADING_ROW_IDS.map((id) => (
                      <TableRow key={id} dense aria-hidden="true">
                        <TableCell>
                          <Skeleton className="h-3 w-8" />
                        </TableCell>
                        <TableCell>
                          <Skeleton className="h-3 w-24 max-w-full" />
                        </TableCell>
                      </TableRow>
                    ))
                  : entries.map((entry) => (
                      <TableRow key={entry.revision} dense>
                        <TableCell className="tabular-nums">
                          {entry.revision}
                        </TableCell>
                        <TableCell className="text-text-muted">
                          <RelativeTime
                            ts={new Date(entry.saved_at_unix_secs * 1000)}
                          />
                        </TableCell>
                      </TableRow>
                    ))}
              </tbody>
            </Table>
          ) : null}
        </Card>
      )}
    </Section>
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
