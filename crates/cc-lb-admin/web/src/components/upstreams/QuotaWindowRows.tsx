// The detail pane's Quota list: one compact row per window the API reports.
// Rows share one grid (CSS subgrid), so the name, meter, `N% used` and facts
// columns line up across windows:
//   [swatch + name] [meter, flexible] [N% used] [reset · ETA · burn]
// From a 40rem wide section every window is one line; narrower, the facts
// drop to a second line under the meter.
import type { ReactNode } from 'react';
import type { AnalysisWindowResponse, QuotaSnapshot } from '../../lib/api';
import { WINDOW_LABELS } from '../../lib/api';
import { getWindowColor } from '../../lib/colors';
import {
  formatQuotaPercent,
  QUOTA_SEVERITY_TEXT_CLASS,
  quotaSeverity,
  quotaStatusLabel,
  quotaStatusTone,
} from '../../lib/quotaSeverity';
import { cx, Hint, Skeleton } from '../ui/primitives';
import { RelativeOffsetTime, ResetCountdown } from '../ui/RelativeTime';
import { UsageMeter } from '../ui/UsageMeter';

const ROWS_CLASS =
  'grid grid-cols-[auto_minmax(0,1fr)_auto] gap-x-4 divide-y divide-border-row @[40rem]:grid-cols-[auto_minmax(0,1fr)_auto_auto] @[40rem]:gap-x-6';
const ROW_CLASS =
  'col-span-full grid grid-cols-subgrid items-center gap-y-1 py-2 first:pt-0 last:pb-0';
const NAME_CELL_CLASS = 'flex min-w-0 items-center gap-2';
const USED_CELL_CLASS = 'text-right text-body tabular-nums';
// Narrow: the facts sit on the second line under the meter and `N% used`.
const FACTS_CELL_CLASS =
  'col-start-2 col-span-2 flex min-w-0 flex-wrap items-center gap-x-2 text-body-sm tabular-nums text-text-muted @[40rem]:col-span-1 @[40rem]:col-start-4';
const HINT_TRIGGER_CLASS =
  'cursor-help border-b border-dashed border-text-faint/60';

/** Display name of a quota window (`7d (Fable)`, `Extra usage`, …). */
export function windowLabel(window: string): string {
  return window in WINDOW_LABELS
    ? WINDOW_LABELS[window as keyof typeof WINDOW_LABELS]
    : window;
}

function WindowName({ window }: { window: string }) {
  return (
    <span className={NAME_CELL_CLASS}>
      <span
        aria-hidden="true"
        data-slot="window-swatch"
        className="size-2.5 shrink-0 rounded-xs"
        style={{ backgroundColor: getWindowColor(window).stroke }}
      />
      <span className="truncate text-title-card text-text">
        {windowLabel(window)}
      </span>
    </span>
  );
}

function usd(minorUnits: number): string {
  return `$${(minorUnits / 100).toLocaleString('en-US', {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  })}`;
}

/** A limit status other than plain `allowed`, in sentence case. */
function StatusText({ status }: { status: string }) {
  const tone = quotaStatusTone(status);
  return (
    <span
      className={
        tone === 'danger'
          ? 'text-danger-text'
          : tone === 'warn'
            ? 'text-warn-text'
            : undefined
      }
    >
      {quotaStatusLabel(status)}
    </span>
  );
}

function perMinute(utilizationPerMinute: number): string {
  return `${(utilizationPerMinute * 100).toFixed(2)}%/min`;
}

export function QuotaWindowRow({
  snap,
  analysis,
  analysisPending,
  nowUnixSecs,
}: {
  snap: QuotaSnapshot;
  analysis: AnalysisWindowResponse | undefined;
  analysisPending: boolean;
  nowUnixSecs: number;
}) {
  const isOverage = snap.window === 'overage';
  const isUnified = snap.window === 'unified';
  const timed = !isOverage && !isUnified;
  const unobserved = snap.state === 'unobserved';
  const status = snap.status && snap.status !== 'allowed' ? snap.status : null;

  // Extra usage: the meter is the share of the monthly budget spent.
  const overageOn =
    isOverage &&
    (snap.extra_usage_enabled === true ||
      snap.extra_usage_monthly_limit != null);
  const overageLimit = snap.extra_usage_monthly_limit;
  const overageUsed = snap.extra_usage_used_credits;
  const usedPct = isOverage
    ? overageOn &&
      overageLimit != null &&
      overageUsed != null &&
      overageLimit > 0
      ? (overageUsed / overageLimit) * 100
      : null
    : snap.utilization == null
      ? null
      : snap.utilization * 100;
  const severity = quotaSeverity(usedPct);
  const severityClass =
    severity === 'warn' || severity === 'danger'
      ? QUOTA_SEVERITY_TEXT_CLASS[severity]
      : 'text-text';

  const notStarted =
    timed &&
    !unobserved &&
    (snap.utilization == null ||
      snap.resets_at_unix_secs == null ||
      snap.resets_at_unix_secs <= nowUnixSecs);
  const started = timed && !unobserved && !notStarted;
  const eta = analysis?.actual_account_burn.eta_to_limit_secs;
  const actualBurn = analysis?.actual_account_burn.utilization_per_second;
  const projectedBurn = analysis?.proxy_projected_burn.utilization_per_hour;
  const waitingForGrowth =
    started &&
    (!analysis ||
      analysis.actual_account_burn.reason === 'insufficient_growth_intervals');

  let used: ReactNode;
  if (isOverage && !overageOn) {
    used = <span className="text-text-muted">Off</span>;
  } else if (isOverage && overageUsed != null) {
    used = (
      <span className={severityClass}>
        {usd(overageUsed)}
        {overageLimit != null ? ` of ${usd(overageLimit)}` : ' used'}
      </span>
    );
  } else if (usedPct != null) {
    used = (
      <span className={severityClass}>{formatQuotaPercent(usedPct)} used</span>
    );
  } else {
    used = (
      <span className="text-text-faint">
        <span aria-hidden="true">—</span>
        <span className="sr-only">No reading</span>
      </span>
    );
  }

  const facts: ReactNode[] = [];
  if (status) facts.push(<StatusText key="status" status={status} />);
  if (unobserved) facts.push(<span key="none">No reading</span>);
  if (notStarted)
    facts.push(
      <Hint
        key="not-started"
        label="The window opens on the first request or warm-up; until then there is no reset time."
      >
        <span tabIndex={0} className={HINT_TRIGGER_CLASS}>
          Not started
        </span>
      </Hint>,
    );
  if (
    (started || isOverage) &&
    snap.resets_at_unix_secs != null &&
    snap.resets_at_unix_secs > nowUnixSecs
  )
    facts.push(
      <ResetCountdown
        key="reset"
        compact
        ts={snap.resets_at_unix_secs * 1000}
      />,
    );
  if (isOverage && overageOn && overageLimit == null)
    facts.push(<span key="no-limit">No limit set</span>);
  if (started) {
    if (analysisPending) {
      facts.push(
        <Skeleton key="analysis" as="span" className="inline-block h-3 w-28" />,
      );
    } else {
      if (eta != null)
        facts.push(
          eta <= 0 ? (
            <span key="eta" className="text-danger-text">
              At limit
            </span>
          ) : (
            <span key="eta">
              Limit <RelativeOffsetTime compact offsetSeconds={eta} />
            </span>
          ),
        );
      if (waitingForGrowth) {
        facts.push(
          <Hint
            key="burn"
            label="Burn appears once utilization is seen rising."
          >
            <span tabIndex={0} className={HINT_TRIGGER_CLASS}>
              Burn pending
            </span>
          </Hint>,
        );
      } else if (actualBurn != null || projectedBurn != null) {
        facts.push(
          <Hint
            key="burn"
            label={`Measured burn; projected from proxy traffic: ${
              projectedBurn == null ? '—' : perMinute(projectedBurn / 60)
            }`}
          >
            <span tabIndex={0} className={HINT_TRIGGER_CLASS}>
              {actualBurn == null ? '—' : perMinute(actualBurn * 60)}
            </span>
          </Hint>,
        );
      }
    }
  }

  return (
    <div role="listitem" data-window={snap.window} className={ROW_CLASS}>
      <WindowName window={snap.window} />
      <UsageMeter label={windowLabel(snap.window)} usedPct={usedPct} />
      <span className={USED_CELL_CLASS}>{used}</span>
      {facts.length ? (
        <span data-slot="window-facts" className={FACTS_CELL_CLASS}>
          {facts.map((fact, index) => (
            <span key={index} className="inline-flex items-center gap-x-2">
              {index > 0 ? (
                <span aria-hidden="true" className="text-text-faint">
                  ·
                </span>
              ) : null}
              {fact}
            </span>
          ))}
        </span>
      ) : null}
    </div>
  );
}

/** The rows' shared grid; children are `QuotaWindowRow`s or skeleton rows. */
export function QuotaWindowRows({ children }: { children: ReactNode }) {
  return (
    <div data-testid="quota-snapshot-grid" role="list" className={ROWS_CLASS}>
      {children}
    </div>
  );
}

/** A loading row with the loaded row's cells and line boxes. */
export function QuotaWindowRowSkeleton() {
  return (
    <div
      data-testid="quota-snapshot-skeleton-card"
      aria-hidden="true"
      className={ROW_CLASS}
    >
      <span className={NAME_CELL_CLASS}>
        <Skeleton as="span" className="size-2.5 shrink-0 rounded-xs" />
        <span className="block text-title-card">
          <Skeleton
            as="span"
            className="inline-block h-[0.75em] w-14 align-middle"
          />
        </span>
      </span>
      <Skeleton as="span" className="block h-1 w-full" />
      <span className={cx(USED_CELL_CLASS, 'block')}>
        <Skeleton
          as="span"
          className="inline-block h-[0.75em] w-16 align-middle"
        />
      </span>
      <span className={cx(FACTS_CELL_CLASS, 'block')}>
        <Skeleton
          as="span"
          className="inline-block h-[0.75em] w-40 align-middle"
        />
      </span>
    </div>
  );
}
