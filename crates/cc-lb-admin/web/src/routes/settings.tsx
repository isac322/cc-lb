import { createFileRoute } from '@tanstack/react-router';
import { AlertTriangle, Download } from 'lucide-react';
import { useEffect, useState } from 'react';
import { toast } from 'sonner';
import {
  Button,
  Card,
  CardBody,
  CardHeader,
  Field,
  INPUT_CLASS,
  Modal,
  PageContainer,
  Section,
} from '../components/ui/primitives';
import { RelativeOffsetTime } from '../components/ui/RelativeTime';
import { downloadJson } from '../lib/api';
import { formatAbsolute, useLocale, useTimezone } from '../lib/locale';
import { useStatus } from '../lib/queries';

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
        <Card>
          <CardHeader
            title="Version"
            subtitle={
              status.data ? (
                <>
                  cc-lb {status.data.version} · {status.data.git_sha} · started{' '}
                  <RelativeOffsetTime
                    offsetSeconds={-status.data.uptime_secs}
                  />
                </>
              ) : (
                '—'
              )
            }
          />
          <CardBody className="space-y-3 text-xs">
            <div className="grid grid-cols-3 gap-3">
              <Row
                label="Rust"
                value={status.data?.build.rust_version ?? '—'}
              />
              <Row label="Profile" value={status.data?.build.profile ?? '—'} />
              <Row label="Generation" value={status.data?.generation ?? '—'} />
            </div>
            <div className="pt-2 border-t border-subtle/40">
              <div className="text-text-faint text-[10px] uppercase tracking-wider mb-0.5">
                Build target
              </div>
              <div className="font-mono break-all">
                {status.data?.build.target ?? '—'}
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

        <Card>
          <CardHeader
            title="Runtime configuration"
            subtitle="Runtime config has moved to a dedicated page."
          />
          <CardBody>
            <p className="text-sm text-text-muted">
              Open{' '}
              <a
                href="/config"
                className="text-fg underline-offset-2 hover:underline"
              >
                /config
              </a>{' '}
              to edit every runtime field with schema-driven inputs, validate,
              and apply with revision tracking.
            </p>
          </CardBody>
        </Card>

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
            Rotating the admin token will invalidate the current dashboard
            session.
          </p>
        </Modal>
      </Section>
    </PageContainer>
  );
}

function Row({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div className="flex items-center justify-between">
      <span className="text-text-faint">{label}</span>
      <span className="text-right">{value}</span>
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
