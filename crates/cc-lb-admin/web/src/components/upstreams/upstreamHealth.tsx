import type { QuotaSnapshot } from '../../lib/api';
import type { OAuthReconnectNudge } from '../../lib/oauthReconnect';
import { QuotaObservedAt } from './QuotaObservedAt';

export interface UpstreamHealth {
  tone: 'ok' | 'danger' | 'warn' | 'neutral';
  label: string;
}

/**
 * One status for the list dot and the detail header. A danger reconnect
 * nudge (unreadable, rejected or expired credentials) outranks the runtime
 * status: the pool cannot route to the upstream even while the reconciler
 * still reports it active.
 */
export function upstreamHealth(
  enabled: boolean,
  runtimeStatus: string | undefined,
  nudge: OAuthReconnectNudge | null | undefined,
): UpstreamHealth {
  if (!enabled) return { tone: 'neutral', label: 'Disabled' };
  if (nudge?.tone === 'danger') return { tone: 'danger', label: nudge.label };
  if (runtimeStatus === 'error') return { tone: 'danger', label: 'Error' };
  if (runtimeStatus === 'active') return { tone: 'ok', label: 'Active' };
  return { tone: 'neutral', label: 'Status unknown' };
}

type FreshnessSnapshot = Pick<
  QuotaSnapshot,
  'state' | 'observed_at_unix_millis'
>;

function oldest(snapshots: readonly FreshnessSnapshot[]) {
  return snapshots.reduce<FreshnessSnapshot | null>(
    (acc, s) =>
      acc == null ||
      (s.observed_at_unix_millis ?? Infinity) <
        (acc.observed_at_unix_millis ?? Infinity)
        ? s
        : acc,
    null,
  );
}

/**
 * Per-card quota freshness, always visible (no hover): the oldest stale
 * reading wins, otherwise the oldest fresh one. A warn dot marks a stale
 * reading; nothing renders when no window has been observed.
 */
export function QuotaFreshnessCaption({
  snapshots,
}: {
  readonly snapshots: readonly FreshnessSnapshot[];
}) {
  const stale = snapshots.filter(
    (s) => s.state === 'stale' && s.observed_at_unix_millis != null,
  );
  const fresh = snapshots.filter(
    (s) => s.state === 'fresh' && s.observed_at_unix_millis != null,
  );
  const shown = oldest(stale.length ? stale : fresh);
  if (!shown) return null;
  const isStale = stale.length > 0;
  return (
    <div
      data-testid="quota-freshness"
      className="flex items-center gap-1.5 text-caption text-text-faint"
    >
      {isStale ? (
        <>
          <span aria-hidden="true" className="status-dot warn" />
          <span className="sr-only">Stale:</span>
        </>
      ) : null}
      <span>
        Updated <QuotaObservedAt snapshot={shown} />
      </span>
    </div>
  );
}
