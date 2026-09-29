// The detail pane's Quota list: one compact row per window the API reports.
// Rows share one grid (CSS subgrid), so the columns line up across windows.
// From a 40rem wide section every window is one line:
//   [swatch + name] [meter, flexible] [N% used] [reset]
// Narrower (phones), each row reads in two lines:
//   [swatch + name]              [N% used]
//   [meter, flexible]            [reset]
import type { ReactNode } from 'react';
import type { QuotaSnapshot } from '../../lib/api';
import { WINDOW_LABELS } from '../../lib/api';
import { getWindowColor } from '../../lib/colors';
import {
  formatQuotaPercent,
  QUOTA_SEVERITY_TEXT_CLASS,
  quotaPacePct,
  quotaSeverity,
} from '../../lib/quotaSeverity';
import { cx, Hint, Skeleton } from '../ui/primitives';
import { ResetCountdown } from '../ui/RelativeTime';
import { UsageMeter } from '../ui/UsageMeter';

const ROWS_CLASS =
  'grid grid-cols-[minmax(0,1fr)_auto] gap-x-4 divide-y divide-border-row @[40rem]:grid-cols-[auto_minmax(0,1fr)_auto_auto] @[40rem]:gap-x-6';
const ROW_CLASS =
  'col-span-full grid grid-cols-subgrid items-center gap-y-1.5 py-2.5 first:pt-0 last:pb-0 @[40rem]:gap-y-1 @[40rem]:py-2';
// Narrow placement is explicit (name/used on line 1, meter/facts on line 2);
// from 40rem every cell takes its own column on one line.
const NAME_CELL_CLASS =
  'col-start-1 row-start-1 flex min-w-0 items-center gap-2';
const METER_CELL_CLASS =
  'col-start-1 row-start-2 @[40rem]:col-start-2 @[40rem]:row-start-1';
const USED_CELL_CLASS =
  'col-start-2 row-start-1 text-right text-body tabular-nums @[40rem]:col-start-3';
const FACTS_CELL_CLASS =
  'col-start-2 row-start-2 flex min-w-0 flex-wrap items-center justify-end gap-x-2 text-right text-body-sm tabular-nums text-text-muted @[40rem]:col-start-4 @[40rem]:row-start-1 @[40rem]:justify-start @[40rem]:text-left';
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

export function QuotaWindowRow({
  snap,
  nowUnixSecs,
}: {
  snap: QuotaSnapshot;
  nowUnixSecs: number;
}) {
  const isOverage = snap.window === 'overage';
  const isUnified = snap.window === 'unified';
  const timed = !isOverage && !isUnified;
  const unobserved = snap.state === 'unobserved';

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
  // Even pace only for a running 5h / 7d window (never Extra usage/Unified).
  const pacePct = started
    ? quotaPacePct(snap.window, snap.resets_at_unix_secs, nowUnixSecs)
    : null;

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
  // Unified carries a reset (anthropic-ratelimit-unified-reset) but is never
  // `timed`: once observed it counts down, without pace or a "Not started".
  if (
    (started || isOverage || (isUnified && !unobserved)) &&
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

  return (
    <div role="listitem" data-window={snap.window} className={ROW_CLASS}>
      <WindowName window={snap.window} />
      <div className={METER_CELL_CLASS}>
        <UsageMeter
          label={windowLabel(snap.window)}
          pacePct={pacePct}
          usedPct={usedPct}
        />
      </div>
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
      <Skeleton
        as="span"
        className={cx(METER_CELL_CLASS, 'block h-1 w-full')}
      />
      <span className={cx(USED_CELL_CLASS, 'block')}>
        <Skeleton
          as="span"
          className="inline-block h-[0.75em] w-16 align-middle"
        />
      </span>
      <span className={cx(FACTS_CELL_CLASS, 'block')}>
        <Skeleton
          as="span"
          className="inline-block h-[0.75em] w-20 align-middle"
        />
      </span>
    </div>
  );
}
