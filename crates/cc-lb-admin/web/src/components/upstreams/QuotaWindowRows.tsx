// The detail pane's Quota list: one compact row per window the API reports.
// Every row participates in one shared block grid:
//   [label] [meter, flexible] [reset prefix] [reset value] [reset suffix]
// Reset parts are content-sized so the meter absorbs the spare width.
// The label can truncate on narrow viewports; reset values stay on one line.
import type { ReactNode } from 'react';
import {
  type PeriodUnknownQuotaMeter,
  type QuotaSnapshot,
  windowLabel,
} from '../../lib/api';
import { getWindowColor } from '../../lib/colors';
import { useLocale } from '../../lib/locale';
import {
  formatQuotaPercent,
  QUOTA_SEVERITY_TEXT_CLASS,
  quotaSeverity,
} from '../../lib/quotaSeverity';
import { cx, Hint, Skeleton } from '../ui/primitives';
import { ResetCountdownParts } from '../ui/RelativeTime';
import { UsageMeter } from '../ui/UsageMeter';
import { CodexCreditSummary, selectCodexCredits } from './codexCredits';
import { formatPeriodUnknownUsedPct } from './periodUnknownMeters';
import { isOverageActive, quotaCardPacePct } from './quotaWindowVisibility';

const ROWS_CLASS =
  'grid grid-cols-[minmax(0,max-content)_minmax(0,1fr)_max-content_max-content_max-content] divide-y divide-border-row';
const ROW_CLASS =
  'col-span-full grid grid-cols-subgrid items-center gap-y-1.5 py-2.5 first:pt-0 last:pb-0 @[40rem]:gap-y-1 @[40rem]:py-2';
const NAME_CELL_CLASS = 'col-start-1 min-w-0 flex items-center gap-2';
const METER_CELL_CLASS =
  'col-start-2 ml-3 min-w-0 flex items-center gap-3 @[40rem]:ml-6';
const USED_CELL_CLASS = 'shrink-0 text-right text-body tabular-nums';
const RESET_PREFIX_CELL_CLASS =
  'col-start-3 min-w-0 whitespace-nowrap text-left text-body-sm tabular-nums text-text-muted';
const RESET_VALUE_CELL_CLASS =
  'col-start-4 min-w-0 whitespace-nowrap text-left text-body-sm tabular-nums text-text-muted';
const RESET_VALUE_KO_CELL_CLASS =
  'col-start-4 min-w-0 whitespace-nowrap text-right text-body-sm tabular-nums text-text-muted';
const RESET_SUFFIX_CELL_CLASS =
  'col-start-5 min-w-0 whitespace-nowrap text-left text-body-sm tabular-nums text-text-muted';
const RESET_STATUS_CELL_CLASS =
  'col-start-3 col-span-3 min-w-0 whitespace-nowrap text-left text-body-sm tabular-nums text-text-muted';
/** Separates the reset phrase from the usage figure. */
const RESET_GAP_CLASS = 'ml-3 @[40rem]:ml-6';
/**
 * Joins the parts of one reset phrase ("Resets in 2h", "2시간 후 초기화"):
 * a word space, not a column gap, so the phrase still reads as one unit.
 */
const RESET_WORD_GAP_CLASS = 'ml-1';
const HINT_TRIGGER_CLASS =
  'cursor-help border-b border-dashed border-text-faint/60';

/** Compact quota label; callers use the same helper for all surfaces. */
export { windowLabel };

function WindowName({
  window,
  label,
  longLabel,
}: {
  window: string;
  label: string;
  longLabel: string;
}) {
  return (
    <span className={NAME_CELL_CLASS} title={longLabel}>
      <span
        aria-hidden="true"
        data-slot="window-swatch"
        className="size-2.5 shrink-0 rounded-xs"
        style={{ backgroundColor: getWindowColor(window).stroke }}
      />
      <span className="truncate text-title-card text-text">{label}</span>
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
  label,
  creditObservations,
}: {
  snap: QuotaSnapshot;
  nowUnixSecs: number;
  label?: string;
  creditObservations?: readonly QuotaSnapshot[];
}) {
  // The unified window is visible only when it carries Codex credit state.
  const windowName: string = snap.window;
  if (
    (windowName === 'unified' && snap.credits == null) ||
    snap.state === 'absent'
  ) {
    return null;
  }
  if (snap.credits != null) {
    return (
      <CodexCreditsRow
        observations={creditObservations ?? [snap]}
        label={label}
      />
    );
  }
  const displayLabel = label ?? windowLabel(windowName);
  const longLabel = windowLabel(windowName, { long: true });
  const isOverage = snap.window === 'overage';
  const unobserved = snap.state === 'unobserved';
  if (isOverage && !isOverageActive(snap)) return null;

  // Extra usage: the meter is the share of the monthly budget spent.
  const overageLimit = snap.extra_usage_monthly_limit;
  const overageUsed = snap.extra_usage_used_credits;
  const usedPct = isOverage
    ? overageLimit != null && overageUsed != null && overageLimit > 0
      ? (overageUsed / overageLimit) * 100
      : null
    : snap.utilization == null
      ? null
      : snap.utilization * 100;

  const notStarted =
    !isOverage &&
    !unobserved &&
    (snap.utilization == null ||
      snap.resets_at_unix_secs == null ||
      snap.resets_at_unix_secs <= nowUnixSecs);
  const started = !isOverage && !unobserved && !notStarted;
  const pacePct = quotaCardPacePct(snap, nowUnixSecs);
  const severityClass =
    QUOTA_SEVERITY_TEXT_CLASS[quotaSeverity(usedPct, pacePct)];

  let used: ReactNode;
  if (isOverage && overageUsed != null) {
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
  const resetTime =
    (started || isOverage) &&
    snap.resets_at_unix_secs != null &&
    snap.resets_at_unix_secs > nowUnixSecs
      ? snap.resets_at_unix_secs * 1000
      : null;

  const resetParts = <QuotaResetCells resetTime={resetTime} />;

  const resetStatus =
    resetTime != null ? (
      resetParts
    ) : facts.length > 0 ? (
      <ResetStatusCells facts={facts} />
    ) : (
      <EmptyResetCells />
    );

  return (
    <div role="listitem" data-window={snap.window} className={ROW_CLASS}>
      <WindowName
        window={snap.window}
        label={displayLabel}
        longLabel={longLabel}
      />
      <div className={METER_CELL_CLASS}>
        <UsageMeter
          className="min-w-0 flex-1"
          label={longLabel}
          pacePct={pacePct}
          usedPct={usedPct}
        />
        <span className={USED_CELL_CLASS}>{used}</span>
      </div>
      {resetStatus}
    </div>
  );
}

export function QuotaPeriodUnknownRow({
  meter,
  nowUnixSecs,
}: {
  meter: PeriodUnknownQuotaMeter;
  nowUnixSecs: number;
}) {
  const identifier = meter.model_id ?? meter.bucket_id ?? meter.group;
  const knownPeriod = meter.known_period?.trim();
  const usedPct = formatPeriodUnknownUsedPct(meter.remaining_fraction);
  const resetTime =
    meter.reset_time_unix_secs != null &&
    meter.reset_time_unix_secs > nowUnixSecs
      ? meter.reset_time_unix_secs * 1000
      : null;
  return (
    <div
      role="listitem"
      data-period-unknown={meter.group}
      className={ROW_CLASS}
    >
      <span className={NAME_CELL_CLASS}>
        <span
          aria-hidden="true"
          data-slot="window-swatch"
          className="size-2.5 shrink-0 rounded-xs"
          style={{ backgroundColor: 'var(--color-neutral)' }}
        />
        <span className="min-w-0">
          <span
            className="block font-mono text-data text-text truncate max-w-[22ch]"
            title={identifier}
          >
            {identifier}
          </span>
          {knownPeriod ? (
            <Hint label="The provider reported this window period, but its reset time was missing or invalid.">
              <span
                tabIndex={0}
                className="text-caption text-text-muted"
                title="The provider reported this window period, but its reset time was missing or invalid."
              >
                {`${knownPeriod} · Reset unavailable`}
              </span>
            </Hint>
          ) : (
            <span
              className="text-caption text-text-muted"
              title="The provider reported remaining quota without a window duration."
            >
              Period unknown
            </span>
          )}
        </span>
      </span>
      <div className={METER_CELL_CLASS}>
        <UsageMeter
          className="min-w-0 flex-1"
          label={`${identifier} · period unknown`}
          pacePct={null}
          usedPct={usedPct}
        />
        <span className={USED_CELL_CLASS}>
          {usedPct != null ? (
            <span className="text-text">
              {formatQuotaPercent(usedPct)} used
            </span>
          ) : (
            <span className="text-text-faint">
              <span aria-hidden="true">—</span>
              <span className="sr-only">No reading</span>
            </span>
          )}
        </span>
      </div>
      {resetTime != null ? (
        <QuotaResetCells resetTime={resetTime} />
      ) : (
        <EmptyResetCells />
      )}
    </div>
  );
}

/** Canonical and period-unknown rows share reset wording and column alignment. */
function QuotaResetCells({ resetTime }: { resetTime: number | null }) {
  const { effective: locale } = useLocale();
  const korean = locale.toLowerCase().startsWith('ko');
  return (
    <ResetCountdownParts
      ts={resetTime}
      compact
      renderParts={(parts) => (
        <>
          <span
            data-slot="window-reset-prefix"
            className={cx(
              RESET_PREFIX_CELL_CLASS,
              parts.prefix ? RESET_GAP_CLASS : null,
            )}
          >
            {parts.prefix}
            {parts.prefix ? ' ' : null}
          </span>
          <span
            data-slot="window-reset-value"
            className={cx(
              korean ? RESET_VALUE_KO_CELL_CLASS : RESET_VALUE_CELL_CLASS,
              parts.prefix ? RESET_WORD_GAP_CLASS : RESET_GAP_CLASS,
            )}
          >
            {parts.value}
            {parts.suffix ? ' ' : null}
          </span>
          <span
            data-slot="window-reset-suffix"
            className={cx(
              RESET_SUFFIX_CELL_CLASS,
              parts.suffix ? RESET_WORD_GAP_CLASS : null,
            )}
          >
            {parts.suffix}
          </span>
        </>
      )}
    />
  );
}

function CodexCreditsRow({
  observations,
  label,
}: {
  observations: readonly QuotaSnapshot[];
  label?: string;
}) {
  const facts = selectCodexCredits(observations);
  if (facts == null) return null;
  return (
    <div role="listitem" data-window="unified" className={ROW_CLASS}>
      <WindowName
        window="unified"
        label={label ?? 'Codex credits'}
        longLabel="Codex credits"
      />
      <div className="col-start-2 col-span-4 ml-3 min-w-0 @[40rem]:ml-6">
        <CodexCreditSummary facts={facts} />
      </div>
    </div>
  );
}

function EmptyResetCells() {
  return (
    <>
      <span
        data-slot="window-reset-prefix"
        className={RESET_PREFIX_CELL_CLASS}
      />
      <span data-slot="window-reset-value" className={RESET_VALUE_CELL_CLASS} />
      <span
        data-slot="window-reset-suffix"
        className={RESET_SUFFIX_CELL_CLASS}
      />
    </>
  );
}

function ResetStatusCells({ facts }: { facts: ReactNode[] }) {
  return (
    <>
      <span
        className={cx(RESET_STATUS_CELL_CLASS, RESET_GAP_CLASS)}
        data-slot="window-facts"
      >
        {facts}
      </span>
      <span data-slot="window-reset-value" className={RESET_VALUE_CELL_CLASS} />
      <span
        data-slot="window-reset-suffix"
        className={RESET_SUFFIX_CELL_CLASS}
      />
    </>
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
      <div className={METER_CELL_CLASS}>
        <Skeleton as="span" className="block h-1 min-w-0 flex-1" />
        <span className={USED_CELL_CLASS}>
          <Skeleton
            as="span"
            className="inline-block h-[0.75em] w-16 align-middle"
          />
        </span>
      </div>
      <span className={cx(RESET_PREFIX_CELL_CLASS, RESET_GAP_CLASS, 'block')}>
        <Skeleton
          as="span"
          className="inline-block h-[0.75em] w-14 align-middle"
        />
      </span>
      <span className={cx(RESET_VALUE_CELL_CLASS, RESET_GAP_CLASS, 'block')}>
        <Skeleton
          as="span"
          className="inline-block h-[0.75em] w-10 align-middle"
        />
      </span>
      <span className={cx(RESET_SUFFIX_CELL_CLASS, RESET_GAP_CLASS, 'block')}>
        <Skeleton
          as="span"
          className="inline-block h-[0.75em] w-14 align-middle"
        />
      </span>
    </div>
  );
}
