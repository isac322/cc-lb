import { windowLabel } from '../../lib/api';
import type { ProviderFamily } from '../../lib/providers';
import { providerOf } from '../../lib/providers';
import { useSubscriptionQuotaLatest, useUpstreams } from '../../lib/queries';
import { Card, CardBody, CardHeader, Skeleton } from '../ui/primitives';
import { UPSTREAM_LATEST_WINDOWS_BY_PROVIDER } from './UpstreamUsageTable';

export function ProviderSubscriptionQuota({
  provider,
}: {
  provider: ProviderFamily;
}) {
  const upstreamsQuery = useUpstreams();
  const upstreams = (upstreamsQuery.data?.upstreams ?? []).filter(
    (upstream) => providerOf(upstream.kind) === provider,
  );
  const oauthIds = upstreams
    .filter((upstream) => upstream.kind.endsWith('_oauth'))
    .map((upstream) => upstream.id)
    .join(',');
  const latest = useSubscriptionQuotaLatest({
    upstreamIds: oauthIds,
    windows: UPSTREAM_LATEST_WINDOWS_BY_PROVIDER[provider],
    source: 'merged',
    enabled: oauthIds.length > 0,
  });

  if (upstreamsQuery.isPending || (oauthIds.length > 0 && latest.isPending)) {
    return <Skeleton className="h-16 w-full" />;
  }
  if (oauthIds.length === 0) {
    return (
      <p className="text-body-sm text-text-muted">No subscription quota</p>
    );
  }
  if (latest.isError) {
    return (
      <p className="text-body-sm text-danger-text" role="alert">
        Subscription quota could not be loaded.
      </p>
    );
  }

  const snapshots = (latest.data?.upstreams ?? []).flatMap((row) =>
    row.windows
      .filter(
        (window) =>
          window.state !== 'absent' &&
          (provider === 'anthropic' || window.state !== 'unobserved'),
      )
      .map((window) => ({
        key: `${row.upstream_id}:${window.window}`,
        name: row.upstream_name,
        label: windowLabel(window.window),
        longLabel: windowLabel(window.window, { long: true }),
        utilization: window.utilization,
      })),
  );
  if (snapshots.length === 0) {
    return (
      <p className="text-body-sm text-text-muted">
        No subscription quota reading
      </p>
    );
  }
  return (
    <div className="grid gap-2 sm:grid-cols-2">
      {snapshots.map((snapshot) => (
        <Card key={snapshot.key}>
          <CardHeader
            title={<span title={snapshot.longLabel}>{snapshot.label}</span>}
            subtitle={snapshot.name}
          />
          <CardBody>
            <p className="text-title-card tabular-nums">
              {snapshot.utilization == null
                ? '—'
                : `${Math.round(snapshot.utilization * 100)}% used`}
            </p>
          </CardBody>
        </Card>
      ))}
    </div>
  );
}
