import {
  Cog,
  Laptop,
  LoaderCircle,
  type LucideIcon,
  Network,
  Server,
  TriangleAlert,
} from 'lucide-react';
import type React from 'react';
import {
  fmtBytes,
  fmtIoMs,
  fmtMs,
  fmtMsCompact,
  fmtSetupMs,
  formatCount,
} from '../../../lib/format';
import type { RequestEventWithPhase } from '../../../lib/RequestEventTypes';
import { cx, Hint } from '../primitives';
import { Sparkline } from '../Sparkline';
import {
  CACHE_SETUP_TIMING_STAGES,
  computeLatencyAttribution,
  deriveOtherSetup,
  deriveSetupOverhead,
  hasSetupTimingBreakdown,
  LATENCY_CATEGORY_META,
  type LatencyCategory,
  responseBodyDuration,
} from './computeStageGroups';

const CATEGORY_ICONS: Record<LatencyCategory, LucideIcon> = {
  downstream_network: Laptop,
  cc_lb: Cog,
  upstream_network: Network,
  upstream_processing: Server,
};

const CATEGORY_SHORT_LABELS: Record<LatencyCategory, string> = {
  downstream_network: 'DN',
  cc_lb: 'LB',
  upstream_network: 'UN',
  upstream_processing: 'UP',
};

const MIXED_UPSTREAM_PATTERN =
  'bg-[repeating-linear-gradient(135deg,_#f59e0b_0_4px,_#10b981_4px_8px)]';
const MIXED_RETRY_PATTERN =
  'bg-[repeating-linear-gradient(135deg,_#6366f1_0_4px,_#f59e0b_4px_8px)]';
const UNATTRIBUTED_PATTERN =
  'bg-slate-500/35 bg-[repeating-linear-gradient(45deg,_transparent_0_4px,_rgba(255,255,255,0.12)_4px_8px)]';

function pctOf(value: number | null, denom: number): string {
  if (value == null || denom <= 0) return '—';
  return `${Math.round((Math.max(0, value) / denom) * 100)}%`;
}

function categoryValueLabel(
  key: LatencyCategory,
  value: number | null,
  isPartial: boolean,
): string {
  if (value != null) return fmtMs(value);
  if (key === 'upstream_processing') return 'Not independently measured';
  return isPartial ? 'In progress' : 'Not measured';
}

function DetailRow({
  label,
  value,
  note,
}: {
  label: string;
  value: React.ReactNode;
  note?: string;
}) {
  return (
    <div className="grid min-w-0 grid-cols-[minmax(0,1fr)_auto] gap-x-3 gap-y-0.5 py-0.5 text-[10px]">
      <span className="min-w-0 text-text-faint">{label}</span>
      <span className="tabular-nums text-right text-text-muted">{value}</span>
      {note ? (
        <span className="col-span-2 text-[9px] leading-3 text-text-faint">
          {note}
        </span>
      ) : null}
    </div>
  );
}

function ObservationGroup({
  title,
  children,
}: {
  title: string;
  children: React.ReactNode;
}) {
  return (
    <section className="min-w-0 rounded-sm border border-subtle bg-overlay-1 px-2 py-1.5">
      <h4 className="mb-1 text-[9px] font-semibold uppercase tracking-wider text-text-faint">
        {title}
      </h4>
      {children}
    </section>
  );
}

export function LatencyCell({
  event: e,
  isPartial,
}: {
  event: RequestEventWithPhase;
  isPartial?: boolean;
}) {
  const partial = isPartial === true || e._phase === 'partial';
  const isRenewal = e.source_kind === 'renewal';
  const attribution = computeLatencyAttribution(e);
  const duration = attribution.timelineTotalMs;
  const { value, unit } = fmtMsCompact(duration);
  const bodyDuration = responseBodyDuration(e);
  const limitReconcileMs =
    typeof e.limit_reconcile_ms === 'number' &&
    Number.isFinite(e.limit_reconcile_ms)
      ? e.limit_reconcile_ms
      : null;
  const isCancelledResponse = e._phase === 'final' && e.status === 499;
  const hasSetupBreakdown = hasSetupTimingBreakdown(e);

  const setupItems: {
    label: string;
    value: number | null | undefined;
    setupTiming?: boolean;
  }[] = hasSetupBreakdown
    ? [
        ...CACHE_SETUP_TIMING_STAGES.map(([field, label]) => ({
          label,
          value: e[field],
          setupTiming: true,
        })),
        { label: 'Auth', value: e.auth_ms },
        { label: 'Route', value: e.route_ms },
        { label: 'Limit reserve', value: e.limit_reserve_ms },
        {
          label: 'Prepare signer',
          value: e.prepare_signer_ms,
          setupTiming: true,
        },
        ...(e._phase === 'final' && e.proxy_setup_ms != null
          ? [
              {
                label: 'Other setup',
                value: deriveOtherSetup(e),
                setupTiming: true,
              },
            ]
          : []),
        { label: 'Shape', value: e.shape_ms },
        { label: 'Sign', value: e.sign_ms },
      ]
    : [
        { label: 'Auth', value: e.auth_ms },
        { label: 'Route', value: e.route_ms },
        { label: 'Limit reserve', value: e.limit_reserve_ms },
        {
          label: 'Setup overhead',
          value:
            e._phase === 'final' && e.proxy_setup_ms != null
              ? deriveSetupOverhead(e)
              : undefined,
        },
        { label: 'Shape', value: e.shape_ms },
        { label: 'Sign', value: e.sign_ms },
      ];
  const visibleSetupItems = setupItems.filter((item) => item.value != null);
  const hasCcLbObservations =
    visibleSetupItems.length > 0 ||
    e.bulkhead_wait_ms != null ||
    e.finalize_ms != null ||
    limitReconcileMs != null;
  const hasIngressObservations =
    e.request_body_read_ms != null ||
    e.request_body_first_chunk_ms != null ||
    e.request_body_receive_ms != null ||
    e.request_body_wait_ms != null ||
    e.request_body_process_ms != null ||
    e.request_body_chunk_count != null ||
    e.request_body_bytes != null;
  const hasResponseParentField =
    e.upstream_body_ms != null || e.stream_total_ms != null;
  const hasMeasuredResponseParent =
    e._phase === 'final' &&
    (e.status === 499 ? e.upstream_body_ms != null : hasResponseParentField);
  const hasLegacyMixedResponse =
    attribution.categories
      .find(({ key }) => key === 'upstream_processing')
      ?.note.includes('Legacy response-body time') === true;
  const hasResponseObservations =
    e.response_body_wait_ms != null ||
    e.response_body_process_ms != null ||
    e.response_body_downstream_poll_gap_ms != null ||
    e.upstream_body_ms != null ||
    e.stream_total_ms != null ||
    e.stream_first_content_delta_ms != null ||
    e.stream_last_content_delta_ms != null ||
    e.inter_token_avg_ms != null;

  const categorySummary = attribution.categories.map(({ key, ms }) => {
    const meta = LATENCY_CATEGORY_META[key];
    return `${meta.label} ${categoryValueLabel(key, ms, partial)}`;
  });
  const extraSummary = [
    `Combined upstream wait ${
      attribution.mixedUpstreamMs == null
        ? partial
          ? 'in progress'
          : 'not measured'
        : fmtMs(attribution.mixedUpstreamMs)
    }`,
    ...(attribution.mixedRetryMs != null
      ? [`Retry overhead ${fmtMs(attribution.mixedRetryMs)}`]
      : []),
    ...(attribution.unattributedMs > 0
      ? [`Unattributed ${fmtMs(attribution.unattributedMs)}`]
      : []),
  ];
  const triggerLabel = isRenewal
    ? `Latency ${fmtMs(duration)}. Renewal cycle ${fmtMs(duration)}. Show breakdown`
    : `Latency ${fmtMs(duration)}. ${[...categorySummary, ...extraSummary].join(
        '; ',
      )}. Show breakdown`;
  const sparklineLabel = isRenewal
    ? `Latency distribution: Renewal cycle ${fmtMs(duration)}`
    : `Latency distribution: ${[...categorySummary, ...extraSummary].join(
        '; ',
      )}`;

  const categoryCards = (
    <div
      role="list"
      className="grid grid-cols-2 gap-1.5"
      aria-label="Latency categories"
    >
      {attribution.categories.map(({ key, ms, note }) => {
        const meta = LATENCY_CATEGORY_META[key];
        const Icon = CATEGORY_ICONS[key];
        const valueLabel = categoryValueLabel(key, ms, partial);
        return (
          <section
            role="listitem"
            key={key}
            data-latency-category={key}
            aria-label={`${meta.label}: ${valueLabel}`}
            className={cx(
              'min-w-0 rounded-sm border px-2 py-1.5',
              meta.surfaceClass,
            )}
          >
            <div
              className={cx('flex min-w-0 items-start gap-1.5', meta.textClass)}
            >
              <Icon className="mt-0.5 size-3 shrink-0" aria-hidden="true" />
              <h3 className="min-w-0 text-[10px] font-semibold leading-3">
                {meta.label}
              </h3>
            </div>
            <div className="mt-1 flex items-baseline justify-between gap-2">
              <span className="text-[11px] font-semibold tabular-nums text-text">
                {valueLabel}
              </span>
              <span className="shrink-0 text-[9px] tabular-nums text-text-faint">
                {pctOf(ms, attribution.timelineTotalMs)}
              </span>
            </div>
            <p className="mt-1 text-[9px] leading-3 text-text-faint">{note}</p>
          </section>
        );
      })}
    </div>
  );

  const proxyPopover = (
    <>
      {categoryCards}

      <section
        data-latency-mixed="upstream-wait"
        className="mt-1.5 overflow-hidden rounded-sm border border-amber-500/35"
      >
        <div className="flex min-w-0 items-center gap-2 px-2 py-1.5">
          <span
            className={cx(
              'h-7 w-1.5 shrink-0 rounded-full',
              MIXED_UPSTREAM_PATTERN,
            )}
            aria-hidden="true"
          />
          <div className="min-w-0 flex-1">
            <div className="flex flex-wrap items-baseline justify-between gap-x-3">
              <h3 className="text-[10px] font-semibold text-text">
                Combined upstream wait
              </h3>
              <span className="text-[11px] font-semibold tabular-nums text-text">
                {attribution.mixedUpstreamMs == null
                  ? partial
                    ? 'In progress'
                    : 'Not measured'
                  : fmtMs(attribution.mixedUpstreamMs)}
              </span>
            </div>
            <p className="text-[9px] leading-3 text-text-faint">
              {hasLegacyMixedResponse
                ? 'Legacy response-body time combines upstream wait, local relay work, and downstream consumption. It is counted once and not assigned to either upstream card.'
                : 'Header and response-frame wait can include upstream transit, provider work, and runtime scheduling. It is counted once and not assigned to either upstream card.'}
            </p>
          </div>
        </div>
      </section>

      {attribution.mixedRetryMs != null ? (
        <section
          data-latency-mixed="retry"
          className="mt-1.5 flex min-w-0 items-center gap-2 rounded-sm border border-subtle px-2 py-1.5"
        >
          <span
            className={cx(
              'h-6 w-1.5 shrink-0 rounded-full',
              MIXED_RETRY_PATTERN,
            )}
            aria-hidden="true"
          />
          <div className="min-w-0 flex-1">
            <div className="flex items-baseline justify-between gap-3">
              <span className="text-[10px] font-semibold text-text">
                Retry overhead
              </span>
              <span className="text-[11px] tabular-nums text-text">
                {fmtMs(attribution.mixedRetryMs)}
              </span>
            </div>
            <p className="text-[9px] leading-3 text-text-faint">
              Shared cc-lb and upstream time before the final attempt.
            </p>
          </div>
        </section>
      ) : null}

      <div className="mt-1.5 grid gap-1.5">
        {hasIngressObservations ? (
          <ObservationGroup title="Ingress observations">
            {e.request_body_first_chunk_ms != null ? (
              <DetailRow
                label="Body start → first DATA"
                value={fmtIoMs(e.request_body_first_chunk_ms)}
              />
            ) : null}
            {e.request_body_receive_ms != null ? (
              <DetailRow
                label="First DATA → body complete"
                value={fmtIoMs(e.request_body_receive_ms)}
              />
            ) : null}
            {e.request_body_wait_ms != null ? (
              <DetailRow
                label="Waiting for body frames"
                value={fmtIoMs(e.request_body_wait_ms)}
              />
            ) : null}
            {e.request_body_process_ms != null ? (
              <DetailRow
                label="Local body handling"
                value={fmtIoMs(e.request_body_process_ms)}
              />
            ) : null}
            {e.request_body_chunk_count != null ? (
              <DetailRow
                label="Non-empty DATA frames"
                value={formatCount(e.request_body_chunk_count)}
              />
            ) : null}
            {e.request_body_bytes != null ? (
              <DetailRow
                label="Ingress body"
                value={fmtBytes(e.request_body_bytes)}
              />
            ) : null}
            {e.request_body_read_ms != null ? (
              <DetailRow
                label="Request body parent interval"
                value={fmtMs(e.request_body_read_ms)}
              />
            ) : null}
            {(e.request_body_first_chunk_ms != null ||
              e.request_body_receive_ms != null) && (
              <p className="mt-1 text-[9px] leading-3 text-text-faint">
                First-DATA and receive intervals are overlapping diagnostic
                markers, not additional category totals.
              </p>
            )}
          </ObservationGroup>
        ) : null}

        {hasResponseObservations ? (
          <ObservationGroup title="Response observations">
            {e.response_body_wait_ms != null ? (
              <DetailRow
                label="Waiting for response frames"
                value={fmtIoMs(e.response_body_wait_ms)}
              />
            ) : null}
            {e.response_body_process_ms != null ? (
              <DetailRow
                label="Local response relay"
                value={fmtIoMs(e.response_body_process_ms)}
              />
            ) : null}
            {e.response_body_downstream_poll_gap_ms != null ? (
              <DetailRow
                label="Next downstream consumer poll"
                value={fmtIoMs(e.response_body_downstream_poll_gap_ms)}
                note="Observed consumer/backpressure and scheduling gap, not a wire ACK or RTT."
              />
            ) : null}
            {hasResponseParentField ? (
              <DetailRow
                label={
                  isCancelledResponse
                    ? 'Partial stream (client cancelled)'
                    : e.stream_total_ms != null
                      ? 'Stream relay parent interval'
                      : 'Body collect parent interval'
                }
                value={
                  hasMeasuredResponseParent
                    ? fmtMs(bodyDuration)
                    : partial
                      ? 'In progress'
                      : 'Not measured'
                }
              />
            ) : null}
            {e.stream_first_content_delta_ms != null ? (
              <DetailRow
                label="First content delta"
                value={fmtMs(e.stream_first_content_delta_ms)}
              />
            ) : null}
            {e.stream_last_content_delta_ms != null ? (
              <DetailRow
                label="Last content delta"
                value={fmtMs(e.stream_last_content_delta_ms)}
              />
            ) : null}
            {e.inter_token_avg_ms != null ? (
              <DetailRow
                label="Inter-token average"
                value={fmtMs(e.inter_token_avg_ms)}
              />
            ) : null}
            <p className="mt-1 text-[9px] leading-3 text-text-faint">
              Parent response intervals overlap the observations above and are
              not added again.
            </p>
          </ObservationGroup>
        ) : null}

        {hasCcLbObservations ? (
          <ObservationGroup title="cc-lb observations">
            {visibleSetupItems.map((item) => (
              <DetailRow
                key={item.label}
                label={item.label}
                value={
                  item.setupTiming ? fmtSetupMs(item.value) : fmtMs(item.value)
                }
              />
            ))}
            {e.bulkhead_wait_ms != null ? (
              <DetailRow
                label="Bulkhead wait"
                value={fmtMs(e.bulkhead_wait_ms)}
              />
            ) : null}
            {e.finalize_ms != null ? (
              <DetailRow label="Finalize" value={fmtMs(e.finalize_ms)} />
            ) : limitReconcileMs != null ? (
              <DetailRow
                label="Limit reconcile (internal post)"
                value={fmtMs(limitReconcileMs)}
              />
            ) : null}
            {e.finalize_ms != null && limitReconcileMs != null ? (
              <DetailRow
                label="Limit reconcile (within finalize)"
                value={fmtMs(limitReconcileMs)}
              />
            ) : null}
          </ObservationGroup>
        ) : null}
      </div>

      <div className="mt-1.5 border-t border-subtle pt-1.5">
        <div
          className="mb-1.5 flex flex-wrap gap-x-3 gap-y-1"
          role="list"
          aria-label="Latency category legend"
        >
          {attribution.categories.map(({ key }) => {
            const meta = LATENCY_CATEGORY_META[key];
            const Icon = CATEGORY_ICONS[key];
            return (
              <span
                role="listitem"
                key={key}
                className="inline-flex items-center gap-1 text-[9px] text-text-faint"
              >
                <Icon
                  className={cx('size-2.5', meta.textClass)}
                  aria-hidden="true"
                />
                {meta.label}
              </span>
            );
          })}
        </div>
        <div className="flex items-center gap-2 text-[10px]">
          <span
            className={cx(
              'h-2 w-2 shrink-0 rounded-full',
              UNATTRIBUTED_PATTERN,
            )}
            aria-hidden="true"
          />
          <span className="min-w-0 flex-1 text-text-faint">Unattributed</span>
          <span className="tabular-nums text-text-muted">
            {fmtMs(attribution.unattributedMs)}
          </span>
        </div>
        {attribution.accountingWarning ? (
          <div
            role="status"
            className="mt-1 flex items-start gap-1 text-[9px] leading-3 text-amber-500"
          >
            <TriangleAlert
              className="mt-px size-2.5 shrink-0"
              aria-hidden="true"
            />
            Measured child intervals exceed a parent interval; raw measurements
            are preserved.
          </div>
        ) : null}
      </div>
    </>
  );

  const popover = (
    <div className="max-h-[min(34rem,calc(100vh-1rem))] w-[min(22rem,calc(100vw-1rem))] max-w-[calc(100vw-1rem)] overflow-y-auto font-mono">
      <div className="mb-2 flex items-center justify-between gap-3">
        <div>
          <div className="text-[10px] font-semibold uppercase tracking-wider text-text">
            Latency attribution
          </div>
          <div className="text-[9px] leading-3 text-text-faint">
            Measured portions only
          </div>
        </div>
        <div className="flex items-center gap-1.5 text-right">
          {partial ? (
            <LoaderCircle
              aria-label="Latency measurement in progress"
              className="size-3 animate-spin text-text-faint"
            />
          ) : null}
          <span className="text-xs font-semibold tabular-nums text-text">
            {fmtMs(duration)}
          </span>
        </div>
      </div>

      {isRenewal ? (
        <section className="rounded-sm border border-blue-500/30 bg-blue-500/10 px-2 py-2">
          <div className="flex items-center justify-between gap-3">
            <span className="text-[10px] font-semibold text-text">
              Renewal cycle
            </span>
            <span className="text-[11px] tabular-nums text-text">
              {fmtMs(duration)}
            </span>
          </div>
          <p className="mt-1 text-[9px] leading-3 text-text-faint">
            Credential renewal is not a proxy request and is not split into
            network or processing categories.
          </p>
        </section>
      ) : (
        proxyPopover
      )}
    </div>
  );

  return (
    <td
      className="p-0 text-right whitespace-nowrap"
      onClick={(event) => event.stopPropagation()}
    >
      <Hint label={popover}>
        <button
          type="button"
          aria-label={triggerLabel}
          className="block w-full cursor-help border-0 bg-transparent px-3 py-2 text-inherit focus-visible:outline focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-[-2px]"
        >
          <div className="flex items-center justify-end gap-1 tabular-nums leading-tight">
            {partial ? (
              <LoaderCircle
                aria-hidden="true"
                className="size-2.5 animate-spin text-text-faint"
              />
            ) : null}
            <span className="w-[5ch] shrink-0 text-right text-text">
              {value}
            </span>
            <span className="w-[2ch] shrink-0 text-left text-text-faint">
              {unit}
            </span>
          </div>
          <div role="img" aria-label={sparklineLabel}>
            <Sparkline
              segments={
                isRenewal
                  ? [{ value: duration, color: 'bg-blue-400' }]
                  : [
                      ...attribution.categories.map(({ key, ms }) => ({
                        value: ms ?? 0,
                        color: LATENCY_CATEGORY_META[key].fill,
                      })),
                      {
                        value: attribution.mixedUpstreamMs ?? 0,
                        color: MIXED_UPSTREAM_PATTERN,
                      },
                      {
                        value: attribution.mixedRetryMs ?? 0,
                        color: MIXED_RETRY_PATTERN,
                      },
                      {
                        value: attribution.unattributedMs,
                        color: UNATTRIBUTED_PATTERN,
                      },
                    ]
              }
            />
          </div>
          {!isRenewal ? (
            <div
              role="list"
              aria-label="Compact latency category legend"
              className="mt-1 grid grid-cols-4 gap-0.5"
            >
              {attribution.categories.map(({ key }) => {
                const meta = LATENCY_CATEGORY_META[key];
                return (
                  <span
                    role="listitem"
                    key={key}
                    aria-label={meta.label}
                    title={meta.label}
                    className={cx(
                      'rounded-sm px-0.5 text-center text-[8px] font-semibold leading-3',
                      meta.textClass,
                      meta.surfaceClass,
                    )}
                  >
                    <span aria-hidden="true">{CATEGORY_SHORT_LABELS[key]}</span>
                  </span>
                );
              })}
            </div>
          ) : null}
        </button>
      </Hint>
    </td>
  );
}
