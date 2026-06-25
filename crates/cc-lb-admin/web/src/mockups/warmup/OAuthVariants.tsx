import { KeyRound } from 'lucide-react';
import {
  Badge,
  Button,
  Card,
  CardBody,
  CardHeader,
  StatusBadge,
} from '../../components/ui/primitives';
import { RelativeTime } from '../../components/ui/RelativeTime';
import type { OAuthMockFixture } from './types';

function oauthBadge(fixture: OAuthMockFixture): {
  tone: 'ok' | 'warn' | 'danger' | 'neutral';
  label: string;
} {
  if (!fixture.has_credentials)
    return { tone: 'neutral', label: 'Not connected' };
  if (fixture.runtimeStatus === 'error')
    return { tone: 'danger', label: 'Reconnect needed' };
  if (
    fixture.expires_at_unix_secs &&
    fixture.expires_at_unix_secs < Date.now() / 1000
  )
    return { tone: 'danger', label: 'Reconnect needed' };
  if (
    fixture.expires_at_unix_secs &&
    fixture.expires_at_unix_secs < Date.now() / 1000 + 3600
  )
    return { tone: 'warn', label: 'Expiring soon' };
  return { tone: 'ok', label: 'Connected' };
}

function OAuthHeaderTitle({ fixture }: { readonly fixture: OAuthMockFixture }) {
  const badge = fixture.isLoading
    ? { tone: 'neutral' as const, label: 'Loading' }
    : oauthBadge(fixture);
  return (
    <div className="flex items-center gap-2">
      <StatusBadge tone={badge.tone} label={badge.label} />
      <span>OAuth Status</span>
    </div>
  );
}

function OAuthAction({ fixture }: { readonly fixture: OAuthMockFixture }) {
  return (
    <Button
      size="sm"
      className="self-center"
      iconLeft={<KeyRound className="w-3 h-3" />}
    >
      {fixture.has_credentials ? 'Reconnect' : 'Connect'}
    </Button>
  );
}

export function OAuthOperationalGrid({
  fixture,
}: {
  readonly fixture: OAuthMockFixture;
}) {
  const badge = fixture.isLoading
    ? { tone: 'neutral' as const, label: 'Loading' }
    : oauthBadge(fixture);
  const hasBoundToken = fixture.has_credentials;

  return (
    <Card className="w-full h-full flex flex-col">
      <CardHeader
        title={<OAuthHeaderTitle fixture={fixture} />}
        subtitle={hasBoundToken ? 'Bound on this upstream' : 'Not connected'}
        action={<OAuthAction fixture={fixture} />}
        align="center"
      />
      <CardBody className="text-sm flex-1">
        {fixture.isLoading ? (
          <div className="h-12 bg-overlay-3 animate-pulse rounded-sm" />
        ) : !hasBoundToken ? (
          <div className="flex flex-wrap items-center gap-3">
            <StatusBadge tone={badge.tone} label={badge.label} />
            <p className="text-xs text-text-faint">
              Run "Connect via OAuth" to authorize this upstream.
            </p>
          </div>
        ) : (
          <div className="space-y-3">
            <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
              <div>
                <div className="text-[11px] text-text-faint uppercase tracking-wider">
                  Expires
                </div>
                <div className="font-mono mt-0.5">
                  <Badge tone={badge.tone}>
                    <RelativeTime
                      ts={
                        fixture.expires_at_unix_secs
                          ? fixture.expires_at_unix_secs * 1000
                          : null
                      }
                    />
                  </Badge>
                </div>
              </div>
              <div>
                <div className="text-[11px] text-text-faint uppercase tracking-wider">
                  Refresh token
                </div>
                <div className="mt-0.5">
                  {fixture.refresh_token_present ? (
                    'present'
                  ) : (
                    <span className="text-amber-400">missing</span>
                  )}
                </div>
              </div>
            </div>
            {fixture.scopes.length > 0 && (
              <div>
                <div className="text-[11px] text-text-faint uppercase tracking-wider">
                  Scopes
                </div>
                <div className="font-mono mt-0.5 break-all text-xs">
                  {fixture.scopes.join(', ')}
                </div>
              </div>
            )}
          </div>
        )}
      </CardBody>
    </Card>
  );
}

export function OAuthExecutiveNarrative({
  fixture,
}: {
  readonly fixture: OAuthMockFixture;
}) {
  const hasBoundToken = fixture.has_credentials;

  return (
    <Card className="w-full h-full flex flex-col">
      <CardHeader
        title={<OAuthHeaderTitle fixture={fixture} />}
        action={<OAuthAction fixture={fixture} />}
        align="center"
      />
      <CardBody className="text-sm flex-1 space-y-4">
        {fixture.isLoading ? (
          <div className="h-12 bg-overlay-3 animate-pulse rounded-sm" />
        ) : !hasBoundToken ? (
          <p className="text-sm leading-6 text-text">
            Not connected. Authorize this upstream to enable requests.
          </p>
        ) : (
          <>
            <p className="text-sm leading-6 text-text">
              Connected and operating normally. Token expires{' '}
              <RelativeTime
                compact
                ts={
                  fixture.expires_at_unix_secs
                    ? fixture.expires_at_unix_secs * 1000
                    : null
                }
              />
              .
            </p>
            <div className="flex flex-wrap items-center gap-2 rounded-sm border border-subtle bg-overlay-2 p-2 text-xs text-text-faint">
              <Badge tone="mono">
                Refresh: {fixture.refresh_token_present ? 'present' : 'missing'}
              </Badge>
              {fixture.scopes.length > 0 ? (
                <span className="font-mono">{fixture.scopes.join(', ')}</span>
              ) : (
                <span>No scopes reported</span>
              )}
            </div>
          </>
        )}
      </CardBody>
    </Card>
  );
}

export function OAuthCompactList({
  fixture,
}: {
  readonly fixture: OAuthMockFixture;
}) {
  const hasBoundToken = fixture.has_credentials;

  return (
    <Card className="w-full h-full flex flex-col">
      <CardHeader
        title={<OAuthHeaderTitle fixture={fixture} />}
        action={<OAuthAction fixture={fixture} />}
        align="center"
      />
      <CardBody className="text-sm flex-1 flex flex-col">
        {fixture.isLoading ? (
          <div className="h-12 bg-overlay-3 animate-pulse rounded-sm" />
        ) : !hasBoundToken ? (
          <div className="flex-1 flex flex-col justify-center items-center text-center space-y-3 py-4">
            <p className="text-xs text-text-faint">No credentials bound.</p>
            <Button size="sm" iconLeft={<KeyRound className="w-3 h-3" />}>
              Connect via OAuth
            </Button>
          </div>
        ) : (
          <div className="space-y-3 flex-1 flex flex-col">
            <div className="flex justify-between items-center border-b border-subtle pb-2">
              <span className="text-text-muted">Expires</span>
              <span className="font-mono text-xs">
                <RelativeTime
                  compact
                  ts={
                    fixture.expires_at_unix_secs
                      ? fixture.expires_at_unix_secs * 1000
                      : null
                  }
                />
              </span>
            </div>
            <div className="flex justify-between items-center border-b border-subtle pb-2">
              <span className="text-text-muted">Refresh</span>
              <span className="text-xs">
                {fixture.refresh_token_present ? 'Present' : 'Missing'}
              </span>
            </div>
            <div className="mt-auto pt-2">
              <Button
                size="sm"
                fullWidth
                iconLeft={<KeyRound className="w-3 h-3" />}
              >
                Reconnect
              </Button>
            </div>
          </div>
        )}
      </CardBody>
    </Card>
  );
}
