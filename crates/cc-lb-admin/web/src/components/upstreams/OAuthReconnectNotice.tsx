// Reusable OAuth reconnect surfaces: a per-upstream notice for the detail
// view, and a global summary that links into /upstreams?action=reconnect.
// Classification lives in lib/oauthReconnect; these components only render it.

import { Link } from '@tanstack/react-router';
import { AlertTriangle } from 'lucide-react';
import { useMemo } from 'react';
import {
  type OAuthReconnectNudge,
  useOAuthReconnectNudges,
} from '../../lib/oauthReconnect';
import { useUpstreams } from '../../lib/queries';
import { Button, cx, Notice } from '../ui/primitives';
import { RelativeTime } from '../ui/RelativeTime';

const MAX_VISIBLE_ENTRIES = 5;

const LINK_CLASS = cx(
  'font-medium text-text underline decoration-text-faint underline-offset-2 hover:decoration-text',
  'focus-visible:outline focus-visible:outline-2 focus-visible:outline-[color:var(--color-accent)] focus-visible:outline-offset-2 rounded-sm',
);

export function OAuthReconnectNotice({
  nudge,
  onReconnect,
  pending,
}: {
  nudge: OAuthReconnectNudge;
  onReconnect: () => void;
  pending?: boolean;
}) {
  return (
    <Notice
      tone={nudge.tone === 'danger' ? 'danger' : 'warning'}
      title={nudge.label}
      action={
        <Button
          size="sm"
          variant={nudge.tone === 'danger' ? 'danger' : 'secondary'}
          loading={pending}
          onClick={onReconnect}
        >
          {nudge.actionLabel}
        </Button>
      }
    >
      <div className="flex items-start gap-2">
        <AlertTriangle className="w-3.5 h-3.5 shrink-0 mt-0.5" />
        <div className="min-w-0">
          <p>{nudge.description}</p>
          {nudge.expiresAt != null && (
            <p className="mt-1">
              Deadline: <RelativeTime ts={nudge.expiresAt * 1000} />
            </p>
          )}
        </div>
      </div>
    </Notice>
  );
}

/**
 * Global summary of OAuth upstreams that need attention. Self-contained: runs
 * its own queries, excludes disabled upstreams from global attention, and
 * renders nothing when every OAuth connection is healthy or still loading.
 */
export function OAuthReconnectSummary() {
  const upstreams = useUpstreams();
  const { nudges, isError } = useOAuthReconnectNudges(
    upstreams.data?.upstreams,
  );

  const entries = useMemo(() => {
    const list: { id: string; name: string; nudge: OAuthReconnectNudge }[] = [];
    for (const u of upstreams.data?.upstreams ?? []) {
      // Disabled accounts keep their detail nudge but stay out of global
      // attention — they are not serving traffic anyway.
      if (!u.enabled) continue;
      const nudge = nudges.get(u.id);
      if (nudge) list.push({ id: u.id, name: u.name, nudge });
    }
    // Danger first, then soonest deadline.
    list.sort((a, b) => {
      if (a.nudge.tone !== b.nudge.tone)
        return a.nudge.tone === 'danger' ? -1 : 1;
      return (a.nudge.expiresAt ?? Infinity) - (b.nudge.expiresAt ?? Infinity);
    });
    return list;
  }, [upstreams.data, nudges]);

  if (entries.length === 0) {
    // A failed status check must not read as "all healthy".
    if (isError) {
      return (
        <Notice tone="warning" title="OAuth status check failed">
          Connection status could not be loaded; retrying automatically.
        </Notice>
      );
    }
    return null;
  }

  const tone = entries.some((e) => e.nudge.tone === 'danger')
    ? 'danger'
    : 'warning';
  const visible = entries.slice(0, MAX_VISIBLE_ENTRIES);
  const hiddenCount = entries.length - visible.length;

  return (
    <Notice
      tone={tone}
      title={
        entries.length === 1
          ? '1 OAuth connection needs attention'
          : `${entries.length} OAuth connections need attention`
      }
    >
      <ul className="flex flex-col gap-1">
        {visible.map((e) => (
          <li key={e.id} className="flex items-center gap-1.5 flex-wrap">
            <Link
              to="/upstreams"
              search={{ selectedId: e.id }}
              className={LINK_CLASS}
            >
              {e.name}
            </Link>
            <span className="text-text-faint">— {e.nudge.label}</span>
            <Link
              to="/upstreams"
              search={{ selectedId: e.id, action: 'reconnect' }}
              className={LINK_CLASS}
            >
              {e.nudge.actionLabel}
            </Link>
          </li>
        ))}
      </ul>
      {hiddenCount > 0 && (
        <Link
          to="/upstreams"
          search={{}}
          className={cx(LINK_CLASS, 'mt-1 inline-block')}
        >
          +{hiddenCount} more on Upstreams
        </Link>
      )}
      {isError && (
        <p className="mt-1 text-text-faint">
          Some status checks failed; retrying automatically.
        </p>
      )}
    </Notice>
  );
}
