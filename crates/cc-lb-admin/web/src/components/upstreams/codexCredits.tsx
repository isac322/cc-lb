import type { QuotaSnapshot } from '../../lib/api';
import { StatusBadge } from '../ui/primitives';

export interface CodexCreditFacts {
  status:
    | 'Blocked'
    | 'Unavailable'
    | 'Stale'
    | 'Unlimited'
    | 'Available'
    | 'Unknown';
  tone: 'danger' | 'ok' | 'neutral';
  statusSource: string;
  balance: string | null;
  balanceSource: string | null;
}

function sourceLabel(snapshot: QuotaSnapshot): string {
  const source =
    snapshot.source === 'header'
      ? 'Response headers'
      : snapshot.source === 'api'
        ? 'Usage API'
        : 'Provider';
  return `${source}${snapshot.state === 'stale' ? ' · stale' : ''}`;
}

function newest(
  snapshots: readonly QuotaSnapshot[],
): QuotaSnapshot | undefined {
  return snapshots.reduce<QuotaSnapshot | undefined>(
    (latest, snapshot) =>
      latest === undefined ||
      (snapshot.observed_at_unix_millis ?? -1) >
        (latest.observed_at_unix_millis ?? -1)
        ? snapshot
        : latest,
    undefined,
  );
}

/** Keep each fact attached to its own observation; a fresh balance is not a fresh availability flag. */
export function selectCodexCredits(
  windows: readonly QuotaSnapshot[],
): CodexCreditFacts | null {
  const observations = windows.filter(
    (snapshot) =>
      (snapshot.window === 'overage' || snapshot.window === 'unified') &&
      snapshot.state !== 'absent' &&
      snapshot.state !== 'unobserved',
  );
  const creditObservations = observations.filter(
    (snapshot) => snapshot.credits != null,
  );
  if (creditObservations.length === 0) return null;

  // Routing retains explicit restrictions from either window, even when stale.
  // A later clear from the other source cannot cancel that restriction.
  const limit = newest(
    observations.filter((snapshot) => snapshot.credits?.limit_reached === true),
  );
  const unavailable = newest(
    observations.filter((snapshot) => snapshot.fallback_available === false),
  );
  const availability = newest(
    creditObservations.filter(
      (snapshot) =>
        snapshot.credits?.unlimited === true ||
        snapshot.credits?.has_credits != null,
    ),
  );
  const statusObservation =
    limit ?? unavailable ?? availability ?? newest(creditObservations);
  const credits = statusObservation?.credits;
  const status = limit
    ? 'Blocked'
    : unavailable
      ? 'Unavailable'
      : statusObservation?.state === 'stale'
        ? 'Stale'
        : credits?.unlimited === true
          ? 'Unlimited'
          : credits?.has_credits === true
            ? 'Available'
            : credits?.has_credits === false
              ? 'Unavailable'
              : 'Unknown';
  const balanceObservation = newest(
    creditObservations.filter((snapshot) => snapshot.credits?.balance != null),
  );
  return {
    status,
    tone:
      status === 'Blocked'
        ? 'danger'
        : status === 'Available' || status === 'Unlimited'
          ? 'ok'
          : 'neutral',
    statusSource: statusObservation
      ? sourceLabel(statusObservation)
      : 'Provider',
    balance: balanceObservation?.credits?.balance ?? null,
    balanceSource: balanceObservation ? sourceLabel(balanceObservation) : null,
  };
}

/** Shared facts for Overview and upstream detail: no meter, conversion or inferred eligibility. */
export function CodexCreditSummary({ facts }: { facts: CodexCreditFacts }) {
  return (
    <div
      className="flex min-w-0 flex-col gap-1.5"
      data-testid="codex-credit-facts"
    >
      <div className="flex flex-wrap items-center gap-x-2 gap-y-1">
        <StatusBadge tone={facts.tone} label={facts.status} />
        <span className="break-all text-body tabular-nums text-text-muted">
          {facts.balance == null
            ? 'No balance reported'
            : `${facts.balance} credits`}
        </span>
      </div>
      <span className="text-caption text-text-muted">
        Status: {facts.statusSource}
      </span>
      {facts.balanceSource != null ? (
        <span className="text-caption text-text-muted">
          Balance: {facts.balanceSource}
        </span>
      ) : null}
    </div>
  );
}
