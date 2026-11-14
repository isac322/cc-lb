import { memo, useMemo } from 'react';
import {
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import type { CreditPoolHistory, QuotaSnapshot } from '../../lib/api';
import {
  type CreditMiniData,
  creditChartCoordinate,
  formatCreditInteger,
  hasCreditHistory,
} from '../../lib/creditHistory';
import { useTimezone } from '../../lib/locale';
import { formatInTimezone } from '../../lib/timezone';
import { CHART_CURSOR } from '../ui/charts';
import { Hint, Skeleton, StatusBadge } from '../ui/primitives';

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

function fieldSourceLabel(
  snapshot: QuotaSnapshot,
  field: 'balance' | 'has_credits' | 'unlimited' | 'limit_reached',
  nowUnixMillis: number,
  maxStalenessSecs: number,
): string {
  const observation = snapshot.credits?.observations?.[field];
  const source =
    observation?.source === 'header'
      ? 'Response headers'
      : observation?.source === 'api'
        ? 'Usage API'
        : snapshot.source === 'header'
          ? 'Response headers'
          : snapshot.source === 'api'
            ? 'Usage API'
            : 'Provider';
  const stale =
    observation != null
      ? Math.max(0, nowUnixMillis - observation.observed_at_unix_millis) >
        maxStalenessSecs * 1000
      : snapshot.state === 'stale';
  return `${source}${stale ? ' · stale' : ''}`;
}

type CreditField = 'balance' | 'has_credits' | 'unlimited' | 'limit_reached';

interface CreditFieldEvidence {
  snapshot: QuotaSnapshot;
  observedAt: number;
  requestOrder?: { publisher: string; sequence: number };
}

function compareCreditEvidence(
  left: CreditFieldEvidence,
  right: CreditFieldEvidence,
): number {
  const leftOrder = left.requestOrder;
  const rightOrder = right.requestOrder;
  if (
    leftOrder != null &&
    rightOrder != null &&
    leftOrder.publisher === rightOrder.publisher &&
    leftOrder.sequence !== rightOrder.sequence
  ) {
    return leftOrder.sequence - rightOrder.sequence;
  }
  return left.observedAt - right.observedAt;
}

function newestCreditField(
  snapshots: readonly QuotaSnapshot[],
  field: CreditField,
): CreditFieldEvidence | undefined {
  return snapshots.reduce<CreditFieldEvidence | undefined>(
    (latest, snapshot) => {
      if (snapshot.credits == null || snapshot.credits[field] == null)
        return latest;
      const observation = snapshot.credits.observations?.[field];
      const candidate = {
        snapshot,
        observedAt:
          observation?.observed_at_unix_millis ??
          snapshot.observed_at_unix_millis ??
          -1,
        requestOrder: observation?.request_order,
      };
      const ordering =
        latest == null ? 1 : compareCreditEvidence(candidate, latest);
      if (latest == null || ordering > 0) return candidate;
      if (ordering < 0) return latest;
      const candidateValue = snapshot.credits[field];
      const latestValue = latest.snapshot.credits?.[field];
      if (
        (field === 'has_credits' &&
          candidateValue === false &&
          latestValue === true) ||
        (field === 'limit_reached' &&
          candidateValue === true &&
          latestValue === false) ||
        (field === 'unlimited' &&
          candidateValue === true &&
          latestValue === false)
      ) {
        return candidate;
      }
      return latest;
    },
    undefined,
  );
}

function creditFieldIsFresh(
  evidence: CreditFieldEvidence | undefined,
  field: Exclude<CreditField, 'balance'>,
  nowUnixMillis: number,
  maxStalenessSecs: number,
): boolean {
  if (evidence == null) return false;
  const observation = evidence.snapshot.credits?.observations?.[field];
  return observation != null
    ? Math.max(0, nowUnixMillis - observation.observed_at_unix_millis) <=
        maxStalenessSecs * 1000
    : evidence.snapshot.state === 'fresh';
}

/** Keep each fact attached to its own observation; a fresh balance is not a fresh availability flag. */
export function selectCodexCredits(
  windows: readonly QuotaSnapshot[],
  maxStalenessSecs = 300,
  nowUnixMillis = Date.now(),
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

  const limitEvidence = newestCreditField(creditObservations, 'limit_reached');
  const limit =
    limitEvidence?.snapshot.credits?.limit_reached === true
      ? limitEvidence
      : undefined;
  const unavailable = newest(
    observations.filter((snapshot) => snapshot.fallback_available === false),
  );
  const hasCredits = newestCreditField(creditObservations, 'has_credits');
  const unlimited = newestCreditField(creditObservations, 'unlimited');
  const latestAvailability = [hasCredits, unlimited]
    .filter((evidence): evidence is CreditFieldEvidence => evidence != null)
    .sort((left, right) => compareCreditEvidence(right, left))[0];
  const hasCreditsFresh = creditFieldIsFresh(
    hasCredits,
    'has_credits',
    nowUnixMillis,
    maxStalenessSecs,
  );
  const unlimitedFresh = creditFieldIsFresh(
    unlimited,
    'unlimited',
    nowUnixMillis,
    maxStalenessSecs,
  );
  const availabilityStatus =
    unlimitedFresh && unlimited?.snapshot.credits?.unlimited === true
      ? 'Unlimited'
      : hasCreditsFresh && hasCredits?.snapshot.credits?.has_credits === true
        ? 'Available'
        : hasCreditsFresh && hasCredits?.snapshot.credits?.has_credits === false
          ? 'Unavailable'
          : latestAvailability != null
            ? 'Stale'
            : 'Unknown';
  const fallbackUnavailable =
    unavailable != null &&
    (unavailable.credits == null ||
      latestAvailability == null ||
      (unavailable.observed_at_unix_millis ?? -1) >=
        latestAvailability.observedAt);
  const statusEvidence =
    limit ??
    (fallbackUnavailable ? undefined : latestAvailability) ??
    (availabilityStatus === 'Stale' ? latestAvailability : undefined);
  const statusObservation =
    limit?.snapshot ??
    (fallbackUnavailable ? unavailable : undefined) ??
    statusEvidence?.snapshot ??
    newest(creditObservations);
  const status = limit
    ? 'Blocked'
    : fallbackUnavailable
      ? 'Unavailable'
      : availabilityStatus;
  const statusSource =
    fallbackUnavailable && unavailable != null
      ? sourceLabel(unavailable)
      : limit != null
        ? fieldSourceLabel(
            limit.snapshot,
            'limit_reached',
            nowUnixMillis,
            maxStalenessSecs,
          )
        : statusEvidence != null
          ? fieldSourceLabel(
              statusEvidence.snapshot,
              statusEvidence === unlimited
                ? 'unlimited'
                : statusEvidence === hasCredits
                  ? 'has_credits'
                  : 'has_credits',
              nowUnixMillis,
              maxStalenessSecs,
            )
          : statusObservation
            ? sourceLabel(statusObservation)
            : 'Provider';
  const balanceObservation = newestCreditField(creditObservations, 'balance');
  return {
    status,
    tone:
      status === 'Blocked'
        ? 'danger'
        : status === 'Available' || status === 'Unlimited'
          ? 'ok'
          : 'neutral',
    statusSource,
    balance: balanceObservation?.snapshot.credits?.balance ?? null,
    balanceSource: balanceObservation
      ? fieldSourceLabel(
          balanceObservation.snapshot,
          'balance',
          nowUnixMillis,
          maxStalenessSecs,
        )
      : null,
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

/** List-only presentation. The existing detailed credit summary stays intact. */
export const CodexCreditMini = memo(function CodexCreditMini({
  data,
  pending = false,
  compact = false,
}: {
  data?: CreditMiniData;
  pending?: boolean;
  compact?: boolean;
}) {
  const { effective: timeZone } = useTimezone();
  const points = useMemo(
    () =>
      data?.history?.series?.map((point) => ({
        unix: point.at_unix_millis / 1000,
        value: creditChartCoordinate(point.balance),
        raw: point.balance,
        observed: point.observed_at_unix_millis,
      })) ?? [],
    [data],
  );
  const latest = data?.history?.latest;
  const current = latest?.state === 'fresh' ? latest.balance : null;
  const value = formatCreditInteger(current);
  const observed = latest?.observed_at_unix_millis;
  const title =
    latest?.balance == null
      ? 'No credit balance reading'
      : `Recorded balance: ${latest.balance} credits${observed == null ? '' : ` · ${formatInTimezone(observed, timeZone)}`}`;
  const numericPoints = points.filter((point) => point.value != null).length;
  return (
    <span
      className="inline-flex max-w-full min-w-0 items-center justify-end gap-2 text-caption tabular-nums text-text-muted"
      data-testid="codex-credit-mini"
      title={title}
    >
      <span
        className={
          compact
            ? 'inline-block h-6 w-12 shrink-0'
            : 'inline-block h-7 w-20 shrink-0'
        }
      >
        {numericPoints > 0 ? (
          <ResponsiveContainer
            width="100%"
            height="100%"
            minWidth={48}
            minHeight={24}
          >
            <LineChart
              data={points}
              margin={{ top: 3, right: 2, bottom: 3, left: 2 }}
              accessibilityLayer={false}
            >
              <XAxis
                hide
                dataKey="unix"
                type="number"
                domain={[data?.start ?? 0, data?.end ?? 0]}
                allowDataOverflow
              />
              <YAxis hide domain={['auto', 'auto']} />
              <Tooltip
                cursor={CHART_CURSOR}
                content={({ active, payload, label }) =>
                  active && payload?.length ? (
                    <span className="glass-strong inline-block whitespace-nowrap rounded-md px-3 py-2 text-caption text-text">
                      <span className="block text-text-muted">
                        {formatInTimezone(Number(label) * 1000, timeZone)}
                      </span>
                      {formatCreditInteger(payload[0].payload.raw)} credits
                    </span>
                  ) : null
                }
              />
              <Line
                type="stepAfter"
                dataKey="value"
                stroke="var(--color-text-muted)"
                strokeWidth={1.25}
                dot={numericPoints === 1 ? { r: 1.5 } : false}
                activeDot={false}
                connectNulls={false}
                isAnimationActive={false}
              />
            </LineChart>
          </ResponsiveContainer>
        ) : null}
      </span>
      {pending && data == null ? (
        <Skeleton as="span" className="inline-block h-3 w-8" />
      ) : (
        <span
          className="max-w-[10ch] truncate text-right"
          aria-label={`${value} credits`}
        >
          {value}
        </span>
      )}
    </span>
  );
});

/** One exact pool figure, with qualifications confined to the existing hint. */
export function CodexCreditTotal({ history }: { history?: CreditPoolHistory }) {
  if (!hasCreditHistory(history) || history == null) return null;
  const point = history.latest;
  const partial = point.contributing_upstreams < point.eligible_upstreams;
  const hint = `${partial ? `Partial balance: ${point.contributing_upstreams} of ${point.eligible_upstreams} accounts. ` : ''}Balances belong to separate accounts and are not transferable.`;
  return (
    <Hint label={hint}>
      <button
        type="button"
        className="inline-flex max-w-full items-baseline gap-1 text-body tabular-nums text-text focus-visible:outline-2 focus-visible:outline-accent"
        data-testid="pool-credit-total"
        title={point.balance ?? undefined}
      >
        <span className="min-w-0 break-all font-medium">
          {formatCreditInteger(point.balance)}
        </span>
        <span className="shrink-0 text-caption text-text-muted">credits</span>
      </button>
    </Hint>
  );
}
