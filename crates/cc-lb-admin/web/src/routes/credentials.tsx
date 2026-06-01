import { useMutation, useQueryClient } from '@tanstack/react-query';
import { createFileRoute } from '@tanstack/react-router';
import { KeyRound, RefreshCw, ShieldOff } from 'lucide-react';
import { useState } from 'react';
import { toast } from 'sonner';
import {
  Badge,
  Button,
  Card,
  CardBody,
  CardHeader,
  cx,
  EmptyState,
  Modal,
  PageContainer,
  Section,
  Skeleton,
  StatusBadge,
} from '../components/ui/primitives';
import { RelativeTime } from '../components/ui/RelativeTime';
import { postJson } from '../lib/api';
import { formatAbsolute, useLocale, useTimezone } from '../lib/locale';
import {
  useCredentials,
  useOAuthStatus,
  usePrincipalNameMap,
} from '../lib/queries';

export const Route = createFileRoute('/credentials')({
  component: CredentialsPage,
});

interface CredentialLike {
  provider: string;
  kind: string;
  identity?: string | null;
  associated?: string | null;
  expires_at_unix_secs?: number | null;
  status: string;
  refresh_token_present?: boolean;
  last_updated_unix_secs?: number | null;
  principal_id?: string | null;
  scopes?: string[];
  cred_id?: string;
}

function fmtTs(
  secs?: number | null,
  locale: string = 'en-US',
  timezone?: string,
) {
  if (!secs) return '—';
  return formatAbsolute(new Date(secs * 1000), locale, timezone);
}

function fmtRelExpiry(secs?: number | null): {
  label: string;
  tone: 'ok' | 'warn' | 'danger' | 'neutral';
} {
  if (!secs) return { label: 'never', tone: 'neutral' };
  const now = Math.floor(Date.now() / 1000);
  const delta = secs - now;
  if (delta < 0)
    return {
      label: `expired ${Math.abs(Math.floor(delta / 60))}m ago`,
      tone: 'danger',
    };
  if (delta < 60 * 10)
    return { label: `in ${Math.floor(delta / 60)}m`, tone: 'warn' };
  if (delta < 60 * 60 * 24)
    return { label: `in ${Math.floor(delta / 3600)}h`, tone: 'ok' };
  return { label: `in ${Math.floor(delta / 86400)}d`, tone: 'ok' };
}

function CredentialsPage() {
  const credsQ = useCredentials();
  const oauthQ = useOAuthStatus();
  const principalNameMap = usePrincipalNameMap();
  const { effective: effectiveLocale } = useLocale();
  const { effective: effectiveTz } = useTimezone();
  const qc = useQueryClient();
  const [confirm, setConfirm] = useState<{
    kind: 'rotate' | 'revoke';
    cred: CredentialLike;
  } | null>(null);

  const mut = useMutation({
    mutationFn: async (args: {
      provider: string;
      cred_id: string;
      action: 'rotate' | 'revoke';
    }) => {
      return postJson(
        `/admin/credentials/${args.provider}/${args.cred_id}/${args.action}`,
        {},
      );
    },
    onSuccess: (_, vars) => {
      toast.success(`Credential ${vars.action}d`);
      qc.invalidateQueries({ queryKey: ['credentials'] });
      qc.invalidateQueries({ queryKey: ['oauth-status'] });
      setConfirm(null);
    },
    onError: (err) => toast.error(`Failed: ${String(err)}`),
  });

  const apiKeyRows = (credsQ.data?.credentials ?? []).filter(
    (c) => (c as unknown as CredentialLike).kind !== 'oauth',
  ) as unknown as CredentialLike[];
  const oauthRows = (oauthQ.data?.credentials ??
    []) as unknown as CredentialLike[];

  return (
    <PageContainer>
      <Section
        title="Credentials"
        subtitle="API keys and OAuth tokens observed for the configured upstreams. Rotate or revoke from here."
      >
        {/* Summary tiles */}
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
          <SummaryTile label="API keys" value={apiKeyRows.length} />
          <SummaryTile label="OAuth tokens" value={oauthRows.length} />
          <SummaryTile
            label="Expiring soon"
            value={
              [...apiKeyRows, ...oauthRows].filter((c) => {
                if (!c.expires_at_unix_secs) return false;
                const delta =
                  c.expires_at_unix_secs - Math.floor(Date.now() / 1000);
                return delta > 0 && delta < 3600;
              }).length
            }
            tone="warn"
          />
          <SummaryTile
            label="Expired"
            value={
              [...apiKeyRows, ...oauthRows].filter(
                (c) =>
                  c.expires_at_unix_secs != null &&
                  c.expires_at_unix_secs < Math.floor(Date.now() / 1000),
              ).length
            }
            tone="danger"
          />
        </div>
      </Section>

      <Section title="API Keys" subtitle="Long-lived credentials">
        <Card>
          <div className="overflow-x-auto">
            {credsQ.isLoading ? (
              <CardBody>
                <Skeleton className="h-16" />
              </CardBody>
            ) : apiKeyRows.length ? (
              <table className="w-full font-mono text-xs">
                <thead className="table-header sticky top-0 z-10">
                  <tr className="text-[10px] uppercase tracking-wider">
                    <th className="text-left px-4 py-2">Provider</th>
                    <th className="text-left px-4 py-2">Identity</th>
                    <th className="text-left px-4 py-2">Status</th>
                    <th className="text-left px-4 py-2">Expires</th>
                    <th className="text-right px-4 py-2">Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {apiKeyRows.map((c, i) => {
                    const exp = fmtRelExpiry(c.expires_at_unix_secs);
                    return (
                      <tr
                        key={`${c.provider}-${c.cred_id ?? i}`}
                        className="border-b border-row hover:bg-overlay-1"
                      >
                        <td className="px-4 py-2">{c.provider}</td>
                        <td
                          className="px-4 py-2 text-text-muted truncate max-w-[260px]"
                          title={c.identity ?? ''}
                        >
                          {c.identity ?? '—'}
                        </td>
                        <td className="px-4 py-2">
                          <StatusBadge
                            tone={
                              c.status === 'active'
                                ? 'ok'
                                : c.status === 'revoked'
                                  ? 'danger'
                                  : 'warn'
                            }
                            label={c.status}
                          />
                        </td>
                        <td className="px-4 py-2 text-text-muted">
                          <span
                            title={fmtTs(
                              c.expires_at_unix_secs,
                              effectiveLocale,
                              effectiveTz,
                            )}
                          >
                            <Badge tone={exp.tone}>{exp.label}</Badge>
                          </span>
                        </td>
                        <td className="px-4 py-2 text-right">
                          <div className="flex items-center justify-end gap-1">
                            <Button
                              size="sm"
                              iconLeft={<RefreshCw className="w-3 h-3" />}
                              onClick={() =>
                                setConfirm({ kind: 'rotate', cred: c })
                              }
                            >
                              Rotate
                            </Button>
                            <Button
                              size="sm"
                              variant="danger"
                              iconLeft={<ShieldOff className="w-3 h-3" />}
                              onClick={() =>
                                setConfirm({ kind: 'revoke', cred: c })
                              }
                            >
                              Revoke
                            </Button>
                          </div>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            ) : (
              <CardBody>
                <EmptyState
                  title="No API keys"
                  description="No API key credentials have been observed for upstreams."
                />
              </CardBody>
            )}
          </div>
        </Card>
      </Section>

      <Section
        title="OAuth Tokens"
        subtitle="Short-lived bearer credentials with refresh"
      >
        {oauthQ.isLoading ? (
          <Skeleton className="h-24" />
        ) : oauthRows.length ? (
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {oauthRows.map((o, i) => {
              const exp = fmtRelExpiry(o.expires_at_unix_secs);
              return (
                <Card key={`${o.provider}-${o.principal_id ?? 'shared'}-${i}`}>
                  <CardHeader
                    title={
                      <span className="flex items-center gap-2">
                        {o.provider}
                        <Badge tone="mono">oauth</Badge>
                      </span>
                    }
                    subtitle={
                      o.principal_id
                        ? `principal: ${principalNameMap.get(o.principal_id) ?? o.principal_id}`
                        : 'shared / global'
                    }
                    action={
                      <StatusBadge
                        tone={
                          exp.tone === 'danger'
                            ? 'danger'
                            : exp.tone === 'warn'
                              ? 'warn'
                              : 'ok'
                        }
                        label={exp.label}
                      />
                    }
                  />
                  <CardBody className="space-y-2 text-xs">
                    <Row
                      label="Expires"
                      value={
                        <RelativeTime
                          ts={
                            o.expires_at_unix_secs
                              ? new Date(o.expires_at_unix_secs * 1000)
                              : null
                          }
                        />
                      }
                    />
                    <Row
                      label="Refresh token"
                      value={
                        o.refresh_token_present ? (
                          'present'
                        ) : (
                          <span className="text-amber-400">missing</span>
                        )
                      }
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
                        <span className="font-mono break-all">
                          {(o.scopes ?? []).join(', ') || '—'}
                        </span>
                      }
                    />
                    <div className="pt-2 border-t border-subtle/40 flex items-center justify-between gap-2">
                      <Button
                        size="sm"
                        iconLeft={<RefreshCw className="w-3 h-3" />}
                        onClick={() => setConfirm({ kind: 'rotate', cred: o })}
                      >
                        Force refresh
                      </Button>
                      <Button
                        size="sm"
                        variant="danger"
                        iconLeft={<ShieldOff className="w-3 h-3" />}
                        onClick={() => setConfirm({ kind: 'revoke', cred: o })}
                      >
                        Revoke
                      </Button>
                    </div>
                  </CardBody>
                </Card>
              );
            })}
          </div>
        ) : (
          <Card>
            <CardBody>
              <EmptyState
                title="No OAuth tokens"
                description="No OAuth upstreams have completed authorization yet."
              />
            </CardBody>
          </Card>
        )}
      </Section>

      <Modal
        open={!!confirm}
        onOpenChange={(o) => {
          if (!o) setConfirm(null);
        }}
        title={
          confirm
            ? `${confirm.kind === 'rotate' ? 'Rotate' : 'Revoke'} credential?`
            : ''
        }
        footer={
          confirm ? (
            <>
              <Button onClick={() => setConfirm(null)}>Cancel</Button>
              <Button
                variant={confirm.kind === 'revoke' ? 'danger' : 'primary'}
                onClick={() =>
                  mut.mutate({
                    provider: confirm.cred.provider,
                    cred_id:
                      confirm.cred.cred_id ??
                      confirm.cred.identity ??
                      confirm.cred.principal_id ??
                      'default',
                    action: confirm.kind,
                  })
                }
              >
                Confirm {confirm.kind}
              </Button>
            </>
          ) : null
        }
      >
        {confirm ? (
          <div className="space-y-2 text-sm">
            <p className="text-text-muted">
              {confirm.kind === 'rotate'
                ? 'A new credential will be requested. Existing in-flight requests using the old credential may fail until they retry.'
                : 'This credential will be permanently revoked. Upstreams relying on it will start returning 401 until you supply a replacement.'}
            </p>
            <div className="text-xs font-mono bg-overlay-3 border border-subtle rounded-sm p-2 space-y-1">
              <div>provider: {confirm.cred.provider}</div>
              <div>identity: {confirm.cred.identity ?? '—'}</div>
              {confirm.cred.principal_id ? (
                <div>
                  principal:{' '}
                  {principalNameMap.get(confirm.cred.principal_id) ??
                    confirm.cred.principal_id}
                </div>
              ) : null}
            </div>
          </div>
        ) : null}
      </Modal>
    </PageContainer>
  );
}

function Row({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div className="flex items-center justify-between gap-3">
      <span className="text-text-faint">{label}</span>
      <span className="text-right">{value}</span>
    </div>
  );
}

function SummaryTile({
  label,
  value,
  tone = 'neutral',
}: {
  label: string;
  value: number;
  tone?: 'neutral' | 'warn' | 'danger';
}) {
  return (
    <div
      className={cx(
        'rounded-sm border border-subtle bg-overlay-1 p-3 flex flex-col gap-1',
        tone === 'warn'
          ? 'border-[color:var(--color-warn)]/30'
          : tone === 'danger'
            ? 'border-[color:var(--color-danger)]/30'
            : '',
      )}
    >
      <div className="text-[10px] uppercase tracking-wider text-text-faint flex items-center gap-1">
        <KeyRound className="w-3 h-3" /> {label}
      </div>
      <div
        className={cx(
          'text-2xl tabular-nums',
          tone === 'warn'
            ? 'text-[color:var(--color-warn)]'
            : tone === 'danger'
              ? 'text-[color:var(--color-danger)]'
              : 'text-text',
        )}
      >
        {value}
      </div>
    </div>
  );
}
