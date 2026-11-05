import { upstreamTypeLabel } from '../../lib/providers';
import {
  type Upstream,
  useUpstreamSubscriptionMetadata,
} from '../../lib/queries';
import { CredentialIcon } from '../ui/ProviderLogo';
import { Skeleton } from '../ui/primitives';
import { subscriptionPlanLabel } from './UpstreamUsageTable';

/**
 * The upstream's plan/kind caption shared by the Upstreams list rows and the
 * command palette: the Claude plan for Anthropic OAuth, or its credential kind.
 */
export function UpstreamPlanCaption({
  upstream,
  showCredentialIcon = true,
}: {
  upstream: Upstream;
  showCredentialIcon?: boolean;
}) {
  const subscription = upstream.kind.endsWith('_oauth');
  const hasPlanMetadata = upstream.kind === 'anthropic_oauth';
  const meta = useUpstreamSubscriptionMetadata(
    hasPlanMetadata ? upstream.id : '',
  );
  if (!subscription) {
    return (
      <span className="inline-flex min-w-0 max-w-full items-center gap-1">
        {showCredentialIcon ? (
          <CredentialIcon
            credential="api_key"
            className="shrink-0 text-text-muted"
          />
        ) : null}
        <span className="truncate">{upstreamTypeLabel(upstream.kind)}</span>
      </span>
    );
  }
  if (!hasPlanMetadata) {
    return (
      <span className="inline-flex min-w-0 max-w-full items-center gap-1">
        {showCredentialIcon ? (
          <CredentialIcon
            credential="subscription"
            className="shrink-0 text-text-muted"
          />
        ) : null}
        <span className="truncate">{upstreamTypeLabel(upstream.kind)}</span>
      </span>
    );
  }
  if (meta.data === undefined && meta.isPending) {
    return <Skeleton as="span" className="inline-block h-3 w-24" />;
  }
  return (
    <span className="inline-flex min-w-0 max-w-full items-center gap-1">
      {showCredentialIcon ? (
        <CredentialIcon
          credential="subscription"
          className="shrink-0 text-text-muted"
        />
      ) : null}
      <span className="truncate">
        {subscriptionPlanLabel(meta.data?.organization_metadata) ??
          'Subscription'}
      </span>
    </span>
  );
}
