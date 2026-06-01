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
  EmptyState,
  PageContainer,
  Section,
  Skeleton,
  StatusBadge,
} from '../components/ui/primitives';
import {
  useCredentials,
  useKillswitch,
  useOAuthStatus,
  useStatus,
} from '../lib/queries';

export const Route = createFileRoute('/status')({
  component: StatusPage,
});

function StatusPage() {
  const status = useStatus();
  const creds = useCredentials();
  const oauth = useOAuthStatus();
  const kill = useKillswitch();
  const [confirmEngageOpen, setConfirmEngageOpen] = useState(false);

  return (
    <PageContainer>
      <Section
        title="Restart-Required Changes"
        subtitle="Fields needing process restart"
      >
        {status.isLoading ? (
          <Skeleton className="h-24" />
        ) : status.data ? (
          <Card>
            <CardBody>
              {status.data.restart_required_changes.length ? (
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
              )}
            </CardBody>
          </Card>
        ) : null}
      </Section>

      <Section title="System">
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <Card>
            <CardHeader title="Storage Backend" />
            <CardBody className="space-y-2 text-xs">
              <Row label="Kind" value="postgres" />
              <Row label="Pool" value="10/10 idle" />
              <Row label="Reconciler" value="45s ago" />
            </CardBody>
          </Card>
          <Card>
            <CardHeader title="Plugin Chain Summary" />
            <CardBody className="space-y-2 text-xs">
              <Row
                label="Principals w/ chain"
                value={
                  status.data?.plugin_chain_summary
                    .principal_count_with_chain ?? 0
                }
              />
              <Row
                label="Total entries"
                value={status.data?.plugin_chain_summary.total_entries ?? 0}
              />
            </CardBody>
          </Card>
          <Card>
            <CardHeader
              title="Killswitch"
              subtitle="Emergency stop for all traffic"
            />
            <CardBody className="space-y-3">
              <div className="flex items-center justify-between text-xs">
                <span className="text-text-faint">Status</span>
                <Badge tone={status.data?.killswitch ? 'danger' : 'ok'}>
                  {status.data?.killswitch ? 'ACTIVE' : 'OFF'}
                </Badge>
              </div>
              <Button
                fullWidth
                size="sm"
                variant={status.data?.killswitch ? 'secondary' : 'danger'}
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
                {status.data?.killswitch ? 'Disengage' : 'Engage killswitch'}
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
          <div className="overflow-x-auto">
            {creds.isLoading ? (
              <CardBody>
                <Skeleton className="h-12" />
              </CardBody>
            ) : creds.data?.credentials.length ? (
              <table className="w-full font-mono text-xs">
                <thead className="bg-overlay-1 border-b border-subtle">
                  <tr className="text-text-faint text-[10px] uppercase tracking-wider">
                    <th className="text-left px-4 py-2">Provider</th>
                    <th className="text-left px-4 py-2">Kind</th>
                    <th className="text-left px-4 py-2">Identity</th>
                    <th className="text-left px-4 py-2">Expires</th>
                    <th className="text-left px-4 py-2">Status</th>
                  </tr>
                </thead>
                <tbody>
                  {creds.data.credentials.map((c, i) => (
                    <tr key={i} className="border-b border-subtle/40">
                      <td className="px-4 py-2">{c.provider}</td>
                      <td className="px-4 py-2">
                        <Badge tone="mono">{c.kind}</Badge>
                      </td>
                      <td className="px-4 py-2">{c.identity ?? '—'}</td>
                      <td className="px-4 py-2">
                        {c.expires_at_unix_secs
                          ? new Date(c.expires_at_unix_secs * 1000)
                              .toISOString()
                              .replace('T', ' ')
                              .slice(0, 19)
                          : '—'}
                      </td>
                      <td className="px-4 py-2">
                        <StatusBadge
                          tone={c.status === 'active' ? 'ok' : 'warn'}
                          label={c.status}
                        />
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            ) : (
              <CardBody>
                <EmptyState title="No credentials observed" />
              </CardBody>
            )}
          </div>
        </Card>
      </Section>

      <Section title="OAuth Tokens" subtitle="Active OAuth flow status">
        {oauth.isLoading ? (
          <Skeleton className="h-12" />
        ) : oauth.data?.credentials.length ? (
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {oauth.data.credentials.map((o, i) => (
              <Card key={i}>
                <CardHeader
                  title={o.provider}
                  subtitle={o.principal_id ?? 'shared'}
                  action={<ShieldCheck className="w-4 h-4 text-green-400" />}
                />
                <CardBody className="space-y-2 text-xs">
                  <Row
                    label="Expires"
                    value={
                      o.expires_at_unix_secs
                        ? new Date(o.expires_at_unix_secs * 1000)
                            .toISOString()
                            .replace('T', ' ')
                            .slice(0, 19)
                        : '—'
                    }
                  />
                  <Row
                    label="Refresh token"
                    value={o.refresh_token_present ? 'present' : 'missing'}
                  />
                  <Row
                    label="Last update"
                    value={
                      o.last_updated_unix_secs
                        ? new Date(o.last_updated_unix_secs * 1000)
                            .toISOString()
                            .replace('T', ' ')
                            .slice(0, 19)
                        : '—'
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
            ))}
          </div>
        ) : (
          <EmptyState title="No OAuth tokens" />
        )}
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

function Row({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div className="flex items-center justify-between">
      <span className="text-text-faint">{label}</span>
      <span className="text-right">{value}</span>
    </div>
  );
}
