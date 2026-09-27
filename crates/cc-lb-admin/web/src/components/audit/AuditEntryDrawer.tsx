import { ChevronRight } from 'lucide-react';
import type { ReactNode } from 'react';
import { eventTime } from '../../lib/api';
import { formatAbsolute, useLocale, useTimezone } from '../../lib/locale';
import { cx, Drawer } from '../ui/primitives';
import { RelativeTime } from '../ui/RelativeTime';
import {
  AUDIT_CATEGORY_LABEL,
  type AuditEntryLike,
  auditActorLabel,
  auditCategory,
  auditTarget,
  cleanAuditPayload,
  humanizeAuditAction,
  type NameMaps,
  parseAuditAction,
  readableValue,
  statusTextClass,
} from './auditEntry';

const SECTION_TITLE_CLASS = 'text-title-card text-text';

// A value that is nothing but an opaque id or hash (no resolved name).
const RAW_ID =
  /^(?:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}|[0-9a-f]{16,})$/i;

/**
 * Recorded values render in sans once they read as names or prose; mono is
 * kept only for what is still a raw id, hash or JSON.
 */
function RecordedValue({ text, raw }: { text: string; raw: boolean }) {
  return raw || RAW_ID.test(text) ? (
    <span className="font-mono text-data break-all">{text}</span>
  ) : (
    <span>{text}</span>
  );
}

function DetailRow({
  label,
  children,
}: {
  label: string;
  children: ReactNode;
}) {
  return (
    <div className="grid grid-cols-[8rem_minmax(0,1fr)] items-baseline gap-3 py-2">
      <dt className="text-label text-text-faint">{label}</dt>
      <dd className="min-w-0 break-words text-text">{children}</dd>
    </div>
  );
}

export function AuditEntryDrawer({
  entry,
  maps,
  onClose,
}: {
  entry: AuditEntryLike | null;
  maps: NameMaps;
  onClose: () => void;
}) {
  const { effective: locale } = useLocale();
  const { effective: timezone } = useTimezone();
  const parsed = entry ? parseAuditAction(entry) : null;
  const target = entry && parsed ? auditTarget(entry, parsed, maps) : null;
  const date = entry ? eventTime(entry) : null;
  const payloadEntries = Object.entries(entry?.payload ?? {});
  const recordsFieldNames = parsed?.params.some(
    ([key]) => key === 'fields' || key === 'slots',
  );

  return (
    <Drawer
      open={entry != null}
      onOpenChange={(open) => {
        if (!open) onClose();
      }}
      width="lg"
      title={parsed?.name ? humanizeAuditAction(parsed.name) : 'Audit entry'}
      description={
        parsed?.name ? (
          <span className="font-mono text-data break-all">{parsed.name}</span>
        ) : undefined
      }
    >
      {entry && parsed ? (
        <div className="flex flex-col gap-6 p-4 text-body">
          <section aria-labelledby="audit-summary-heading">
            <h3 id="audit-summary-heading" className={SECTION_TITLE_CLASS}>
              Summary
            </h3>
            <dl className="mt-2 divide-y divide-row">
              <DetailRow label="When">
                {date ? (
                  <>
                    {formatAbsolute(date, locale, timezone)}{' '}
                    <span className="text-text-faint">
                      (<RelativeTime ts={date} />)
                    </span>
                  </>
                ) : (
                  '—'
                )}
              </DetailRow>
              <DetailRow label="Type">
                {AUDIT_CATEGORY_LABEL[auditCategory(parsed.name)]}
              </DetailRow>
              <DetailRow label="Target">
                {target ? (
                  <>
                    <span className="text-text-faint">{target.kind} </span>
                    {target.name}
                    {target.resolved &&
                    target.id &&
                    target.id !== target.name ? (
                      <span className="block font-mono text-data text-text-faint">
                        {target.id}
                      </span>
                    ) : null}
                  </>
                ) : (
                  '—'
                )}
              </DetailRow>
              <DetailRow label="Actor">
                {auditActorLabel(entry)}
                {entry.actor_kind ? (
                  <span className="text-text-faint">
                    {' · '}
                    {entry.actor_kind.replaceAll('_', ' ')}
                  </span>
                ) : null}
              </DetailRow>
              <DetailRow label="Route">
                <span className="font-mono text-data">
                  {entry.route || '—'}
                </span>
              </DetailRow>
              <DetailRow label="Status">
                <span
                  className={cx('tabular-nums', statusTextClass(entry.status))}
                >
                  {entry.status}
                </span>
              </DetailRow>
              <DetailRow label="Entry ID">
                <span className="font-mono text-data">{entry.request_id}</span>
              </DetailRow>
            </dl>
          </section>

          {parsed.params.length > 0 || payloadEntries.length > 0 ? (
            <section aria-labelledby="audit-change-heading">
              <h3 id="audit-change-heading" className={SECTION_TITLE_CLASS}>
                Recorded details
              </h3>
              <dl className="mt-2 divide-y divide-row">
                {parsed.params.map(([key, value]) => (
                  <DetailRow key={`param-${key}`} label={key}>
                    <RecordedValue
                      raw={false}
                      text={
                        key === 'fields' || key === 'slots'
                          ? value.split(',').join(', ')
                          : readableValue(value, maps)
                      }
                    />
                  </DetailRow>
                ))}
                {payloadEntries.map(([key, value]) => (
                  <DetailRow key={`payload-${key}`} label={key}>
                    <RecordedValue
                      raw={typeof value !== 'string'}
                      text={
                        typeof value === 'string'
                          ? readableValue(value, maps)
                          : JSON.stringify(value)
                      }
                    />
                  </DetailRow>
                ))}
              </dl>
              {recordsFieldNames ? (
                <p className="mt-2 text-caption text-text-faint">
                  This entry records which fields changed, not their previous or
                  new values.
                </p>
              ) : null}
            </section>
          ) : null}

          <details className="group">
            <summary className="inline-flex w-fit list-none items-center gap-1.5 rounded-sm text-body text-text-muted hover:text-text focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-1 [&::-webkit-details-marker]:hidden">
              <ChevronRight
                className="size-3 shrink-0 transition-transform duration-200 group-open:rotate-90 motion-reduce:transition-none"
                strokeWidth={1.75}
                aria-hidden="true"
              />
              Raw entry
              <span className="text-text-faint">
                (sensitive keys are stripped server-side)
              </span>
            </summary>
            <pre className="well mt-2 overflow-x-auto p-3 font-mono text-data leading-relaxed text-text-muted">
              {JSON.stringify(cleanAuditPayload(entry), null, 2)}
            </pre>
          </details>
        </div>
      ) : null}
    </Drawer>
  );
}
