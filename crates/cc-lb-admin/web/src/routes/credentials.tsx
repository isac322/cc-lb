import { useMutation, useQueryClient } from '@tanstack/react-query';
import { createFileRoute } from '@tanstack/react-router';
import { KeyRound, RefreshCw, ShieldOff } from 'lucide-react';
import { useRef, useState } from 'react';
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
  SkeletonRow,
  Spinner,
  StatusBadge,
} from '../components/ui/primitives';
import { RelativeTime } from '../components/ui/RelativeTime';
import { postJson } from '../lib/api';
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

const API_KEY_COLUMN_CLASS_NAMES = [
  'w-28',
  'w-64',
  'w-28',
  'w-36',
  'w-52',
] as const;

const API_KEY_CELL_CLASS_NAMES = [
  'px-4 w-28',
  'px-4 w-64 max-w-64 truncate',
  'px-4 w-28',
  'px-4 w-36',
  'px-4 w-52 text-right',
] as const;

const API_KEY_SKELETON_CLASS_NAMES = [
  'w-20',
  'w-44',
  'w-16',
  'w-24',
  'w-36 ml-auto',
] as const;
const OAUTH_CARD_CLASS = 'min-h-[258px]';

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

function CredentialsPage() {
  const credsQ = useCredentials();
  const oauthQ = useOAuthStatus();
  const principalNameMap = usePrincipalNameMap();
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
      // Only success clears the target, so a failure leaves the credential in
      // context for a retry instead of dropping the destructive intent.
      setConfirm(null);
    },
    // No local onError: the global MutationCache toast is the single error
    // surface, so adding one here would double-report the same failure.
  });

  // Rotate mints a replacement credential and revoke tears one down, so a
  // duplicate request from one synchronous burst of clicks would strand an
  // unreferenced credential or fire a second teardown. React Query publishes
  // `isPending` only on the next render, which lands too late for that burst,
  // so this ref latches the request until it settles.
  const actionInFlight = useRef(false);
  const pending = mut.isPending;
  // Derived from the in-flight variables rather than `confirm` so the progress
  // copy always names the request the server is actually running.
  const inFlightAction = pending ? (mut.variables?.action ?? null) : null;

  const submitConfirm = () => {
    if (!confirm || actionInFlight.current || pending) return;
    actionInFlight.current = true;
    mut.mutate(
      {
        provider: confirm.cred.provider,
        cred_id:
          confirm.cred.cred_id ??
          confirm.cred.identity ??
          confirm.cred.principal_id ??
          'default',
        action: confirm.kind,
      },
      {
        onSettled: () => {
          actionInFlight.current = false;
        },
      },
    );
  };

  const apiKeyRows = (credsQ.data?.credentials ?? []).filter(
    (c) => (c as unknown as CredentialLike).kind !== 'oauth',
  ) as unknown as CredentialLike[];
  const oauthRows = (oauthQ.data?.credentials ??
    []) as unknown as CredentialLike[];
  const allCredentialRows = [...apiKeyRows, ...oauthRows];
  const combinedCountsLoading = credsQ.isLoading || oauthQ.isLoading;
  const expiringSoonCount = allCredentialRows.filter((c) => {
    if (!c.expires_at_unix_secs) return false;
    const delta = c.expires_at_unix_secs - Math.floor(Date.now() / 1000);
    return delta > 0 && delta < 3600;
  }).length;
  const expiredCount = allCredentialRows.filter(
    (c) =>
      c.expires_at_unix_secs != null &&
      c.expires_at_unix_secs < Math.floor(Date.now() / 1000),
  ).length;

  return (
    <PageContainer>
      <Section
        title="Credentials"
        subtitle="API keys and OAuth tokens observed for the configured upstreams. Rotate or revoke from here."
      >
        <div
          className="grid grid-cols-2 md:grid-cols-4 gap-3"
          data-testid="credential-summary-grid"
        >
          <SummaryTile
            label="API keys"
            value={apiKeyRows.length}
            loading={credsQ.isLoading}
          />
          <SummaryTile
            label="OAuth tokens"
            value={oauthRows.length}
            loading={oauthQ.isLoading}
          />
          <SummaryTile
            label="Expiring soon"
            value={expiringSoonCount}
            loading={combinedCountsLoading}
            tone="warn"
          />
          <SummaryTile
            label="Expired"
            value={expiredCount}
            loading={combinedCountsLoading}
            tone="danger"
          />
        </div>
      </Section>

      <Section title="API Keys" subtitle="Long-lived credentials">
        <Card>
          <div
            className="overflow-x-auto min-h-[186px]"
            data-testid="api-keys-slot"
          >
            {credsQ.isLoading || apiKeyRows.length ? (
              <table className="table-fixed min-w-[52rem] w-full font-mono text-xs">
                <colgroup>
                  {API_KEY_COLUMN_CLASS_NAMES.map((className, index) => (
                    <col key={index} className={className} />
                  ))}
                </colgroup>
                <thead className="table-header sticky top-0 z-10">
                  <tr className="text-[10px] uppercase tracking-wider">
                    <th
                      className={cx(
                        'text-left py-2',
                        API_KEY_CELL_CLASS_NAMES[0],
                      )}
                    >
                      Provider
                    </th>
                    <th
                      className={cx(
                        'text-left py-2',
                        API_KEY_CELL_CLASS_NAMES[1],
                      )}
                    >
                      Identity
                    </th>
                    <th
                      className={cx(
                        'text-left py-2',
                        API_KEY_CELL_CLASS_NAMES[2],
                      )}
                    >
                      Status
                    </th>
                    <th
                      className={cx(
                        'text-left py-2',
                        API_KEY_CELL_CLASS_NAMES[3],
                      )}
                    >
                      Expires
                    </th>
                    <th className={cx('py-2', API_KEY_CELL_CLASS_NAMES[4])}>
                      Actions
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {credsQ.isLoading
                    ? Array.from({ length: 3 }).map((_, i) => (
                        <SkeletonRow
                          key={i}
                          cols={API_KEY_CELL_CLASS_NAMES.length}
                          cellClassNames={API_KEY_CELL_CLASS_NAMES}
                          skeletonClassNames={API_KEY_SKELETON_CLASS_NAMES}
                        />
                      ))
                    : apiKeyRows.map((c, i) => (
                        <tr
                          key={`${c.provider}-${c.cred_id ?? i}`}
                          className="border-b border-row hover:bg-overlay-1"
                        >
                          <td
                            className={cx('py-2', API_KEY_CELL_CLASS_NAMES[0])}
                          >
                            {c.provider}
                          </td>
                          <td
                            className={cx(
                              'py-2 text-text-muted',
                              API_KEY_CELL_CLASS_NAMES[1],
                            )}
                            title={c.identity ?? ''}
                          >
                            {c.identity ?? '—'}
                          </td>
                          <td
                            className={cx('py-2', API_KEY_CELL_CLASS_NAMES[2])}
                          >
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
                          <td
                            className={cx(
                              'py-2 text-text-muted',
                              API_KEY_CELL_CLASS_NAMES[3],
                            )}
                          >
                            <ExpiryBadge secs={c.expires_at_unix_secs} />
                          </td>
                          <td
                            className={cx('py-2', API_KEY_CELL_CLASS_NAMES[4])}
                          >
                            <div className="flex items-center justify-end gap-1">
                              <Button
                                size="sm"
                                disabled={pending}
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
                                disabled={pending}
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
                      ))}
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
        <div
          className="grid grid-cols-1 md:grid-cols-2 gap-4"
          data-testid="oauth-grid"
        >
          {oauthQ.isLoading ? (
            <OAuthCardSkeleton />
          ) : oauthRows.length ? (
            oauthRows.map((o, i) => {
              return (
                <Card
                  key={`${o.provider}-${o.principal_id ?? 'shared'}-${i}`}
                  className={cx(
                    OAUTH_CARD_CLASS,
                    oauthRows.length === 1 && 'md:col-span-2',
                  )}
                  data-testid="oauth-card-slot"
                >
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
                    action={<ExpiryBadge secs={o.expires_at_unix_secs} />}
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
                        disabled={pending}
                        iconLeft={<RefreshCw className="w-3 h-3" />}
                        onClick={() => setConfirm({ kind: 'rotate', cred: o })}
                      >
                        Force refresh
                      </Button>
                      <Button
                        size="sm"
                        variant="danger"
                        disabled={pending}
                        iconLeft={<ShieldOff className="w-3 h-3" />}
                        onClick={() => setConfirm({ kind: 'revoke', cred: o })}
                      >
                        Revoke
                      </Button>
                    </div>
                  </CardBody>
                </Card>
              );
            })
          ) : (
            <Card
              className={cx(OAUTH_CARD_CLASS, 'md:col-span-2')}
              data-testid="oauth-card-slot"
            >
              <CardBody>
                <EmptyState
                  title="No OAuth tokens"
                  description="No OAuth upstreams have completed authorization yet."
                />
              </CardBody>
            </Card>
          )}
        </div>
      </Section>

      <Modal
        open={!!confirm}
        preventDismiss={pending}
        onOpenChange={(o) => {
          if (o) return;
          // Belt-and-braces with `preventDismiss`: no close path may drop the
          // target while the write is still in flight.
          if (pending) return;
          setConfirm(null);
        }}
        title={
          confirm
            ? `${confirm.kind === 'rotate' ? 'Rotate' : 'Revoke'} credential?`
            : ''
        }
        footer={
          confirm ? (
            <>
              <Button disabled={pending} onClick={() => setConfirm(null)}>
                Cancel
              </Button>
              <Button
                variant={confirm.kind === 'revoke' ? 'danger' : 'primary'}
                loading={pending}
                onClick={submitConfirm}
              >
                {inFlightAction === 'rotate'
                  ? 'Rotating...'
                  : inFlightAction === 'revoke'
                    ? 'Revoking...'
                    : `Confirm ${confirm.kind}`}
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
            {inFlightAction ? (
              <p
                role="status"
                aria-live="polite"
                className="flex items-center gap-2 text-xs text-text-faint"
                data-testid="credential-action-progress"
              >
                <Spinner className="w-3 h-3" />
                {inFlightAction === 'rotate' ? 'Rotating' : 'Revoking'}{' '}
                {confirm.cred.provider} credential — waiting for the server.
              </p>
            ) : null}
          </div>
        ) : null}
      </Modal>
    </PageContainer>
  );
}

function OAuthCardSkeleton() {
  return (
    <Card
      className={cx(OAUTH_CARD_CLASS, 'md:col-span-2')}
      data-testid="oauth-card-slot"
      aria-hidden="true"
    >
      <CardHeader
        title={<span className="skeleton block h-4 w-24" />}
        subtitle={<span className="skeleton block h-3 w-32" />}
        action={<Skeleton className="h-5 w-16" />}
      />
      <CardBody className="space-y-3 text-xs">
        {Array.from({ length: 4 }).map((_, index) => (
          <div key={index} className="flex items-center justify-between gap-3">
            <Skeleton className="h-3 w-20" />
            <Skeleton className="h-3 w-28" />
          </div>
        ))}
        <div className="pt-2 border-t border-subtle/40 flex items-center justify-between gap-2">
          <Skeleton className="h-7 w-28" />
          <Skeleton className="h-7 w-20" />
        </div>
      </CardBody>
    </Card>
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
  loading = false,
  tone = 'neutral',
}: {
  label: string;
  value: number;
  loading?: boolean;
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
          'h-8 flex items-center text-2xl tabular-nums',
          tone === 'warn'
            ? 'text-[color:var(--color-warn)]'
            : tone === 'danger'
              ? 'text-[color:var(--color-danger)]'
              : 'text-text',
        )}
        data-testid="credential-summary-value"
      >
        {loading ? <Skeleton className="h-6 w-10" /> : value}
      </div>
    </div>
  );
}
