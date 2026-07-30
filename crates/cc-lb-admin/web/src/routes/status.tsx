import { createFileRoute } from '@tanstack/react-router';
import { AlertTriangle, Power, ShieldCheck } from 'lucide-react';
import { useState } from 'react';
import { toast } from 'sonner';
import {
  Badge,
  Button,
  Card,
  CardBody,
  CardHeader,
  ConfirmDialog,
  cx,
  EmptyState,
  PageContainer,
  Section,
  Skeleton,
  SkeletonRow,
  StatusBadge,
} from '../components/ui/primitives';
import { RelativeTime } from '../components/ui/RelativeTime';
import {
  useCredentials,
  useKillswitch,
  useOAuthStatus,
  usePrincipalNameMap,
  useStatus,
} from '../lib/queries';

export const Route = createFileRoute('/status')({
  component: StatusPage,
});

function expiryTone(
  secs?: number | null,
): 'ok' | 'warn' | 'danger' | 'neutral' {
  if (!secs) return 'neutral';
  const now = Math.floor(Date.now() / 1000);
  const delta = secs - now;
  if (delta < 0) return 'danger';
  if (delta < 60 * 10) return 'warn';
  return 'ok';
}

function ExpiryBadge({ secs }: { readonly secs?: number | null }) {
  return (
    <Badge tone={expiryTone(secs)}>
      <RelativeTime compact ts={secs ? secs * 1000 : null} />
    </Badge>
  );
}

const SYSTEM_CARD_CLASS = 'flex min-h-[144px] flex-col';
const LAST_RELOAD_VALUE_SLOT_CLASS = 'min-w-20';
const PLUGIN_COUNT_VALUE_SLOT_CLASS = 'min-w-8';
const KILLSWITCH_STATUS_SLOT_CLASS =
  'inline-flex min-h-[22px] min-w-14 items-center justify-end text-right';
const OAUTH_CARD_GEOMETRY_CLASS = 'min-h-[230px] sm:min-h-[205px]';
const OAUTH_CARD_CLASS = cx('flex flex-col', OAUTH_CARD_GEOMETRY_CLASS);
const OAUTH_GRID_CLASS = cx(
  'grid grid-cols-1 gap-4 md:grid-cols-2',
  OAUTH_CARD_GEOMETRY_CLASS,
);
const CREDENTIAL_CELL_CLASS_NAMES = [
  'px-4',
  'px-4',
  'px-4',
  'px-4',
  'px-4',
] as const;
const CREDENTIAL_SKELETON_CLASS_NAMES = [
  'h-5 w-20',
  'h-5 w-16',
  'h-5 w-32',
  'h-5 w-20',
  'h-5 w-16',
] as const;

function StatusPage() {
  const status = useStatus();
  const creds = useCredentials();
  const oauth = useOAuthStatus();
  const kill = useKillswitch();
  const principalNameMap = usePrincipalNameMap();
  const [confirmEngageOpen, setConfirmEngageOpen] = useState(false);

  return (
    <PageContainer>
      <Section
        title="Restart-Required Changes"
        subtitle="Fields needing process restart"
      >
        <Card data-testid="restart-required-card">
          <CardBody
            data-testid="restart-required-body"
            className="min-h-[72px]"
          >
            {status.isLoading ? (
              <div className="space-y-2">
                <Skeleton className="h-4 w-1/2" />
                <Skeleton className="h-4 w-2/3" />
              </div>
            ) : status.data ? (
              status.data.restart_required_changes.length ? (
                <ul className="space-y-2">
                  {status.data.restart_required_changes.map((c, i) => (
                    <li key={i} className="text-xs flex items-start gap-2">
                      <AlertTriangle className="w-4 h-4 text-amber-400 shrink-0" />
                      <div>
                        <div className="font-mono">{c.field}</div>
                        <div className="text-text-faint">
                          {c.current} → {c.new} · {c.reason}
                        </div>
                      </div>
                    </li>
                  ))}
                </ul>
              ) : (
                <p className="text-xs text-text-faint">
                  All hot-applied. No restart required.
                </p>
              )
            ) : null}
          </CardBody>
        </Card>
      </Section>

      <Section title="System">
        <div
          data-testid="system-grid"
          className="grid grid-cols-1 md:grid-cols-3 gap-4"
        >
          <Card className={SYSTEM_CARD_CLASS} data-testid="system-card">
            <CardHeader title="Storage Backend" />
            <CardBody className="flex-1 space-y-2 text-xs">
              <Row label="Kind" value="postgres" />
              <Row label="Pool" value="10/10 idle" />
              <Row
                label="Last reload"
                valueClassName={LAST_RELOAD_VALUE_SLOT_CLASS}
                valueTestId="system-last-reload-slot"
                value={
                  status.isLoading ? (
                    <Skeleton className="h-4 w-full" />
                  ) : (
                    <RelativeTime
                      ts={
                        status.data?.last_reload_status?.applied_at_unix_secs
                          ? status.data.last_reload_status
                              .applied_at_unix_secs * 1000
                          : null
                      }
                    />
                  )
                }
              />
            </CardBody>
          </Card>
          <Card className={SYSTEM_CARD_CLASS} data-testid="system-card">
            <CardHeader title="Plugin Chain Summary" />
            <CardBody className="flex-1 space-y-2 text-xs">
              <Row
                label="Principals w/ chain"
                valueClassName={PLUGIN_COUNT_VALUE_SLOT_CLASS}
                valueTestId="system-principal-chain-count-slot"
                value={
                  status.isLoading ? (
                    <Skeleton className="h-4 w-full" />
                  ) : (
                    (status.data?.plugin_chain_summary
                      .principal_count_with_chain ?? 0)
                  )
                }
              />
              <Row
                label="Total entries"
                valueClassName={PLUGIN_COUNT_VALUE_SLOT_CLASS}
                valueTestId="system-total-chain-count-slot"
                value={
                  status.isLoading ? (
                    <Skeleton className="h-4 w-full" />
                  ) : (
                    (status.data?.plugin_chain_summary.total_entries ?? 0)
                  )
                }
              />
            </CardBody>
          </Card>
          <Card className={SYSTEM_CARD_CLASS} data-testid="system-card">
            <CardHeader
              title="Killswitch"
              subtitle="Emergency stop for all traffic"
            />
            <CardBody className="flex flex-1 flex-col space-y-3">
              <div className="flex min-h-[22px] items-center justify-between text-xs">
                <span className="text-text-faint">Status</span>
                <div
                  data-testid="system-killswitch-status-slot"
                  className={KILLSWITCH_STATUS_SLOT_CLASS}
                >
                  {status.isLoading ? (
                    <Skeleton className="h-5 w-full" />
                  ) : (
                    <Badge tone={status.data?.killswitch ? 'danger' : 'ok'}>
                      {status.data?.killswitch ? 'ACTIVE' : 'OFF'}
                    </Badge>
                  )}
                </div>
              </div>
              <Button
                fullWidth
                size="sm"
                className="mt-auto"
                data-testid="killswitch-control"
                aria-label={
                  status.isLoading ? 'Killswitch status loading' : undefined
                }
                disabled={status.isLoading}
                variant={
                  status.isLoading
                    ? 'secondary'
                    : status.data?.killswitch
                      ? 'secondary'
                      : 'danger'
                }
                iconLeft={<Power className="w-3 h-3" />}
                onClick={() => {
                  if (status.data?.killswitch) {
                    kill.mutate(false, {
                      onSuccess: () => toast.success('Killswitch disengaged'),
                    });
                  } else {
                    setConfirmEngageOpen(true);
                  }
                }}
              >
                {status.isLoading ? (
                  <Skeleton className="h-3 w-24" />
                ) : status.data?.killswitch ? (
                  'Disengage'
                ) : (
                  'Engage killswitch'
                )}
              </Button>
            </CardBody>
          </Card>
        </div>
      </Section>

      <Section
        title="Credentials"
        subtitle="API keys and OAuth tokens observed for upstreams"
      >
        <Card>
          <div
            data-testid="credentials-table-slot"
            className="min-h-[191px] overflow-x-auto"
          >
            <table className="min-w-[640px] w-full font-mono text-xs">
              <thead className="table-header sticky top-0 z-10">
                <tr className="text-[10px] uppercase tracking-wider">
                  <th className="text-left px-4 py-2">Provider</th>
                  <th className="text-left px-4 py-2">Kind</th>
                  <th className="text-left px-4 py-2">Identity</th>
                  <th className="text-left px-4 py-2">Expires</th>
                  <th className="text-left px-4 py-2">Status</th>
                </tr>
              </thead>
              <tbody>
                {creds.isLoading ? (
                  <>
                    <SkeletonRow
                      cols={5}
                      cellClassNames={CREDENTIAL_CELL_CLASS_NAMES}
                      skeletonClassNames={CREDENTIAL_SKELETON_CLASS_NAMES}
                    />
                    <SkeletonRow
                      cols={5}
                      cellClassNames={CREDENTIAL_CELL_CLASS_NAMES}
                      skeletonClassNames={CREDENTIAL_SKELETON_CLASS_NAMES}
                    />
                    <SkeletonRow
                      cols={5}
                      cellClassNames={CREDENTIAL_CELL_CLASS_NAMES}
                      skeletonClassNames={CREDENTIAL_SKELETON_CLASS_NAMES}
                    />
                  </>
                ) : creds.data?.credentials.length ? (
                  creds.data.credentials.map((c, i) => (
                    <tr key={i} className="border-b border-row">
                      <td className="px-4 py-2">{c.provider}</td>
                      <td className="px-4 py-2">
                        <Badge tone="mono">{c.kind}</Badge>
                      </td>
                      <td className="px-4 py-2">{c.identity ?? '—'}</td>
                      <td className="px-4 py-2">
                        <ExpiryBadge secs={c.expires_at_unix_secs} />
                      </td>
                      <td className="px-4 py-2">
                        <StatusBadge
                          tone={c.status === 'active' ? 'ok' : 'warn'}
                          label={c.status}
                        />
                      </td>
                    </tr>
                  ))
                ) : (
                  <tr>
                    <td colSpan={5} className="px-4 py-4">
                      <EmptyState title="No credentials observed" />
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </Card>
      </Section>

      <Section title="OAuth Tokens" subtitle="Active OAuth flow status">
        <div data-testid="oauth-grid" className={OAUTH_GRID_CLASS}>
          {oauth.isLoading ? (
            <Card
              className={cx(OAUTH_CARD_CLASS, 'md:col-span-2')}
              data-testid="oauth-card"
            >
              <CardHeader
                title={<Skeleton className="h-4 w-32" />}
                subtitle={<Skeleton className="h-4 w-24" />}
              />
              <CardBody className="flex-1 space-y-2 text-xs">
                <Row
                  label="Expires"
                  value={<Skeleton className="h-4 w-20" />}
                />
                <Row
                  label="Refresh token"
                  value={<Skeleton className="h-4 w-16" />}
                />
                <Row
                  label="Last update"
                  value={<Skeleton className="h-4 w-20" />}
                />
                <Row label="Scopes" value={<Skeleton className="h-4 w-28" />} />
              </CardBody>
            </Card>
          ) : oauth.data?.credentials.length ? (
            oauth.data.credentials.map((o, i) => (
              <Card
                key={i}
                className={cx(
                  OAUTH_CARD_CLASS,
                  oauth.data.credentials.length === 1 && 'md:col-span-2',
                )}
                data-testid="oauth-card"
              >
                <CardHeader
                  title={o.provider}
                  subtitle={
                    o.principal_id
                      ? (principalNameMap.get(o.principal_id) ?? o.principal_id)
                      : 'shared'
                  }
                  action={<ShieldCheck className="w-4 h-4 text-green-400" />}
                />
                <CardBody className="flex-1 space-y-2 text-xs">
                  <Row
                    label="Expires"
                    value={<ExpiryBadge secs={o.expires_at_unix_secs} />}
                  />
                  <Row
                    label="Refresh token"
                    value={o.refresh_token_present ? 'present' : 'missing'}
                  />
                  <Row
                    label="Last update"
                    value={
                      <RelativeTime
                        ts={
                          o.last_updated_unix_secs
                            ? new Date(o.last_updated_unix_secs * 1000)
                            : null
                        }
                      />
                    }
                  />
                  <Row
                    label="Scopes"
                    value={
                      <span className="font-mono">{o.scopes.join(', ')}</span>
                    }
                  />
                </CardBody>
              </Card>
            ))
          ) : (
            <Card
              className={cx(OAUTH_CARD_CLASS, 'md:col-span-2')}
              data-testid="oauth-card"
            >
              <CardBody className="flex flex-1 items-center">
                <div className="w-full">
                  <EmptyState title="No OAuth tokens" />
                </div>
              </CardBody>
            </Card>
          )}
        </div>
      </Section>

      <ConfirmDialog
        open={confirmEngageOpen}
        onOpenChange={setConfirmEngageOpen}
        title="Engage killswitch?"
        description="All proxy traffic will stop immediately. Inbound requests will be rejected until the killswitch is disengaged."
        confirmLabel="Engage killswitch"
        destructive
        onConfirm={() =>
          kill.mutate(true, {
            onSuccess: () => toast.success('Killswitch engaged'),
          })
        }
      />
    </PageContainer>
  );
}

function Row({
  label,
  value,
  valueClassName,
  valueTestId,
}: {
  label: string;
  value: React.ReactNode;
  valueClassName?: string;
  valueTestId?: string;
}) {
  return (
    <div className="flex min-h-[22px] items-center justify-between">
      <span className="text-text-faint">{label}</span>
      <div
        className={cx(
          'inline-flex min-h-[22px] items-center justify-end text-right',
          valueClassName,
        )}
        data-testid={valueTestId}
      >
        {value}
      </div>
    </div>
  );
}
