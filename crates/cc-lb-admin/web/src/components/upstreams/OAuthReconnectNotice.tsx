// Reusable OAuth reconnect surfaces: a per-upstream notice for the detail
// view, and a one-line global banner that links into /upstreams.
// Classification lives in lib/oauthReconnect; these components only render it.

import { useNavigate } from '@tanstack/react-router';
import { useMemo } from 'react';
import {
  isTerminalOAuthReconnectReason,
  type OAuthReconnectNudge,
  useOAuthReconnectNudges,
} from '../../lib/oauthReconnect';
import { useUpstreams } from '../../lib/queries';
import { Button, Notice } from '../ui/primitives';
import { RelativeTime } from '../ui/RelativeTime';

/** Names listed inline in the banner before collapsing into "+N more". */
const MAX_NAMED_ENTRIES = 3;

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
        <Button size="sm" loading={pending} onClick={onReconnect}>
          {nudge.actionLabel}
        </Button>
      }
    >
      <p>{nudge.description}</p>
      {nudge.expiresAt != null && (
        <p className="mt-1">
          Deadline: <RelativeTime ts={nudge.expiresAt * 1000} />
        </p>
      )}
    </Notice>
  );
}

/**
 * Global banner for OAuth upstreams that need attention, including disabled
 * accounts. Self-contained: runs its own queries and renders nothing when
 * every OAuth connection is healthy or still loading.
 * One line: a count title, the affected names as a sentence, one action.
 */
export function OAuthReconnectSummary() {
  const navigate = useNavigate();
  const upstreams = useUpstreams();
  const { nudges, isError: nudgeError } = useOAuthReconnectNudges(
    upstreams.data?.upstreams,
  );
  const isError = upstreams.isError || nudgeError;

  const entries = useMemo(() => {
    const list: { id: string; name: string; nudge: OAuthReconnectNudge }[] = [];
    for (const u of upstreams.data?.upstreams ?? []) {
      const nudge = nudges.get(u.id);
      if (nudge) list.push({ id: u.id, name: u.name, nudge });
    }
    // Terminal credential failures first, then danger and the soonest deadline.
    list.sort((a, b) => {
      const terminalA = isTerminalOAuthReconnectReason(a.nudge.reason);
      const terminalB = isTerminalOAuthReconnectReason(b.nudge.reason);
      if (terminalA !== terminalB) return terminalA ? -1 : 1;
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
        <Notice
          variant="banner"
          tone="warning"
          title="OAuth status check failed"
        >
          Connection status could not be loaded; retrying automatically.
        </Notice>
      );
    }
    return null;
  }

  const tone = entries.some((e) => e.nudge.tone === 'danger')
    ? 'danger'
    : 'warning';
  const named = entries.slice(0, MAX_NAMED_ENTRIES).map((e) => e.name);
  const hiddenCount = entries.length - named.length;
  // One shared reason reads as the sentence; mixed reasons stay on /upstreams.
  const reasons = new Set(entries.map((e) => e.nudge.reason));
  const summary = [
    reasons.size === 1 ? `${entries[0]?.nudge.label}: ` : '',
    named.join(', '),
    hiddenCount > 0 ? ` +${hiddenCount} more` : '',
    isError ? '. Some status checks failed; retrying.' : '',
  ].join('');
  // A single affected upstream opens straight into its reconnect flow.
  const only = entries.length === 1 ? entries[0] : undefined;

  return (
    <Notice
      variant="banner"
      tone={tone}
      title={
        entries.length === 1
          ? '1 OAuth connection needs attention'
          : `${entries.length} OAuth connections need attention`
      }
      action={
        <Button
          size="sm"
          variant={tone === 'danger' ? 'danger' : 'secondary'}
          onClick={() =>
            navigate(
              only
                ? {
                    to: '/upstreams',
                    search: { selectedId: only.id, action: 'reconnect' },
                  }
                : { to: '/upstreams', search: {} },
            )
          }
        >
          {only ? only.nudge.actionLabel : 'Review upstreams'}
        </Button>
      }
    >
      {summary}
    </Notice>
  );
}
