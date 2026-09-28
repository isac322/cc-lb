import {
  type Upstream,
  useUpstreamSubscriptionMetadata,
} from '../../lib/queries';
import { Skeleton } from '../ui/primitives';
import { subscriptionPlanLabel } from './UpstreamUsageTable';

/**
 * The upstream's plan/kind caption shared by the Upstreams list rows and the
 * command palette: the Claude plan for OAuth upstreams, "API key" otherwise.
 */
export function UpstreamPlanCaption({ upstream }: { upstream: Upstream }) {
  const oauth = upstream.kind === 'anthropic_oauth';
  const meta = useUpstreamSubscriptionMetadata(oauth ? upstream.id : '');
  if (!oauth) return <span className="truncate">API key</span>;
  if (meta.data === undefined && meta.isPending)
    return <Skeleton as="span" className="inline-block h-3 w-24" />;
  return (
    <span className="truncate">
      {subscriptionPlanLabel(meta.data?.organization_metadata) ??
        'Subscription'}
    </span>
  );
}
