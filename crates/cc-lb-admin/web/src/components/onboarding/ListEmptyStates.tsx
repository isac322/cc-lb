import { Plus } from 'lucide-react';
import { useUpstreams } from '../../lib/queries';
import { Button, EmptyState } from '../ui/primitives';
import { OnboardingLink } from './OnboardingLink';

/** Upstreams list with none configured: what an upstream is and how to add one. */
export function UpstreamsListEmpty({
  onCreate,
}: {
  readonly onCreate: () => void;
}) {
  return (
    <EmptyState
      headingLevel={2}
      title="No upstreams yet"
      description="An upstream is a Claude subscription or API key the pool routes requests through. Nothing can be proxied until one exists."
      action={
        <Button
          variant="primary"
          iconLeft={<Plus className="w-3 h-3" />}
          onClick={onCreate}
        >
          New upstream
        </Button>
      }
    />
  );
}

/**
 * Principals list with none created. When the pool also has no upstream, the
 * operator is pointed there first, since a principal's requests have nowhere
 * to go without one.
 */
export function PrincipalsListEmpty({
  onCreate,
}: {
  readonly onCreate: () => void;
}) {
  const upstreams = useUpstreams();
  const needsUpstream = upstreams.data?.upstreams.length === 0;
  return (
    <EmptyState
      headingLevel={2}
      title="No principals yet"
      description={
        needsUpstream
          ? 'A principal is a client identity with its own proxy keys and limits. Add an upstream first so its requests have somewhere to go.'
          : 'A principal is a client identity with its own proxy keys and limits. Create one, then issue it a key.'
      }
      action={
        needsUpstream ? (
          <div className="flex flex-col items-center gap-3">
            <OnboardingLink target="new-upstream">
              Add an upstream first
            </OnboardingLink>
            <Button iconLeft={<Plus className="w-3 h-3" />} onClick={onCreate}>
              New principal
            </Button>
          </div>
        ) : (
          <Button
            variant="primary"
            iconLeft={<Plus className="w-3 h-3" />}
            onClick={onCreate}
          >
            New principal
          </Button>
        )
      }
    />
  );
}
