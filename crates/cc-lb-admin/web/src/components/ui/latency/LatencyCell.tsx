import { type ReactNode, useId } from 'react';
import { fmtBytes, fmtMs, fmtMsCompact, fmtSetupMs } from '../../../lib/format';
import type { RequestEventWithPhase } from '../../../lib/RequestEventTypes';
import { cx, Hint } from '../primitives';
import { Sparkline } from '../Sparkline';
import {
  CACHE_SETUP_TIMING_STAGES,
  computeLatencyAttribution,
  deriveOtherSetup,
  deriveSetupOverhead,
  hasSetupTimingBreakdown,
  LATENCY_RESPONSIBILITY_META,
  latencyResponsibilityDescription,
} from './computeStageGroups';

function pctOf(value: number | null | undefined, denom: number): number {
  if (denom <= 0 || value == null || value <= 0) return 0;
  return Math.round((value / denom) * 100);
}

interface SectionItem {
  label: string;
  value: number | null | undefined;
  showZero?: boolean;
  setupTiming?: boolean;
  description?: string;
}

function Section({
  title,
  color,
  total,
  duration,
  items,
  pill,
  showZero,
  description,
}: {
  title: string;
  color: string;
  total: number;
  duration: number;
  items: SectionItem[];
  pill?: ReactNode;
  showZero?: boolean;
  description?: string;
}) {
  const descriptionId = useId();
  const visibleItems = items.filter(
    (item) => item.value != null && (item.value > 0 || item.showZero === true),
  );
  if (total <= 0 && visibleItems.length === 0 && showZero !== true) return null;

  return (
    <div
      aria-describedby={description ? descriptionId : undefined}
      aria-label={`${title} latency`}
      className="mb-2 last:mb-0"
      role="group"
    >
      {description ? (
        <span className="sr-only" id={descriptionId}>
          {description}
        </span>
      ) : null}
      <div className="flex items-center gap-2 text-[11px] mb-1">
        <span className={cx('h-2 w-2 rounded-full shrink-0', color)} />
        <span className="text-text-muted flex-1 font-medium flex items-center gap-2">
          <span
            className={cx(
              description
                ? 'cursor-help border-b border-dashed border-text-faint/60'
                : '',
            )}
            title={description}
          >
            {title}
          </span>
          {pill}
        </span>
        <span className="tabular-nums text-text w-16 text-right">
          {fmtMs(total)}
        </span>
        <span className="tabular-nums text-text-faint w-9 text-right">
          {pctOf(total, duration)}%
        </span>
      </div>
      {visibleItems.length > 0 && (
        <div className="flex flex-col gap-0.5 pl-4">
          {visibleItems.map((item) => (
            <div
              key={item.label}
              className="flex items-center gap-2 text-[10px]"
            >
              <span
                className={cx(
                  'text-text-faint flex-1 truncate',
                  item.description
                    ? 'cursor-help border-b border-dashed border-text-faint/40'
                    : '',
                )}
                title={item.description}
              >
                {item.label}
              </span>
              <span className="tabular-nums text-text-muted w-16 text-right">
                {item.setupTiming ? fmtSetupMs(item.value) : fmtMs(item.value)}
              </span>
              <span className="tabular-nums text-text-faint w-9 text-right">
                {pctOf(item.value, duration)}%
              </span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

export function LatencyCell({ event: e }: { event: RequestEventWithPhase }) {
  const attribution = computeLatencyAttribution(e);
  const isRenewal = attribution.isFinalRenewal;
  const hasFinalize = e.finalize_ms != null;
  const {
    totalMs: duration,
    requestBodyOtherMs: requestBodyOther,
    responseBodyOtherMs: responseBodyOther,
    upstreamHeaderWaitMs: upstream_wait_ms,
    topLevelResidualMs: topLevelResidual,
    downstreamMs: downstreamTotal,
    ccLbMs: ccLbTotal,
    upstreamNetMs: upstreamNetTotal,
    upstreamWaitMs: upstreamWaitTotal,
    unattributedMs: unattributedTotal,
    isCancelledPartial,
    hasRequestBodyBreakdown,
    hasResponseBodyBreakdown,
  } = attribution;
  const limitReconcileMs =
    typeof e.limit_reconcile_ms === 'number' ? e.limit_reconcile_ms : 0;
  const otherFinalize = Math.max(0, (e.finalize_ms ?? 0) - limitReconcileMs);
  const { value, unit } = fmtMsCompact(e._phase === 'final' ? duration : 0);
  const setup_overhead_ms = deriveSetupOverhead(e);
  const hasSetupBreakdown = hasSetupTimingBreakdown(e);
  const internalPreItems: SectionItem[] = hasSetupBreakdown
    ? [
        ...CACHE_SETUP_TIMING_STAGES.map(([field, label]) => ({
          label,
          value: e[field],
          showZero: true,
          setupTiming: true,
        })),
        { label: 'Auth', value: e.auth_ms },
        { label: 'Route', value: e.route_ms },
        { label: 'Limit reserve', value: e.limit_reserve_ms },
        {
          label: 'Prepare signer',
          value: e.prepare_signer_ms,
          showZero: true,
          setupTiming: true,
        },
        ...(e._phase === 'final' && e.proxy_setup_ms != null
          ? [
              {
                label: 'Other setup',
                value: deriveOtherSetup(e),
                showZero: true,
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
        { label: 'Setup overhead', value: setup_overhead_ms },
        { label: 'Shape', value: e.shape_ms },
        { label: 'Sign', value: e.sign_ms },
      ];

  const responseBodyRemainderLabel = isCancelledPartial
    ? hasResponseBodyBreakdown
      ? 'Other response body (client cancelled)'
      : 'Partial stream (client cancelled)'
    : hasResponseBodyBreakdown
      ? 'Other response body'
      : e.stream_total_ms != null
        ? 'Stream relay'
        : 'Body collect';
  const unattributedDescription = latencyResponsibilityDescription(
    'unattributed',
    attribution,
  );
  const responsibilityStages = attribution.responsibilities
    .filter((group) => group.observed && group.valueMs > 0)
    .map((group) => `${group.label} ${fmtMs(group.valueMs)}`);
  const sparklineLabel = `Latency by responsibility: ${responsibilityStages.join(', ')}`;
  const triggerStages = [
    ...responsibilityStages,
    ...(e.request_body_bytes != null && !isRenewal
      ? [`Ingress body ${fmtBytes(e.request_body_bytes)}`]
      : []),
  ];
  const triggerLabel = `Latency ${value} ${unit}${
    triggerStages.length > 0 ? `, ${triggerStages.join(', ')}` : ''
  }, show breakdown`;

  const proxySections = (
    <div className="flex flex-col">
      <Section
        title={LATENCY_RESPONSIBILITY_META.downstream.label}
        color={LATENCY_RESPONSIBILITY_META.downstream.color}
        description={LATENCY_RESPONSIBILITY_META.downstream.description}
        total={downstreamTotal}
        duration={duration}
        items={[
          {
            label: 'Request body wait',
            value: e.request_body_wait_ms,
            showZero: e.request_body_wait_ms != null,
            setupTiming: true,
            description:
              'Client pacing, downstream transit, and runtime scheduling while receiving the request body.',
          },
          {
            label: 'Response poll gap',
            value: e.response_body_downstream_poll_gap_ms,
            showZero: e.response_body_downstream_poll_gap_ms != null,
            setupTiming: true,
            description:
              'Downstream consumer, backpressure, and scheduler gap between response polls.',
          },
        ]}
        pill={null}
        showZero={
          e.request_body_wait_ms != null ||
          e.response_body_downstream_poll_gap_ms != null
        }
      />

      <Section
        title={LATENCY_RESPONSIBILITY_META['cc-lb'].label}
        color={LATENCY_RESPONSIBILITY_META['cc-lb'].color}
        description={LATENCY_RESPONSIBILITY_META['cc-lb'].description}
        total={ccLbTotal}
        duration={duration}
        items={[
          {
            label: 'Request body processing',
            value: e.request_body_process_ms,
            showZero: e.request_body_process_ms != null,
            setupTiming: true,
          },
          ...internalPreItems,
          {
            label: 'Bulkhead wait',
            value:
              typeof e.bulkhead_wait_ms === 'number'
                ? e.bulkhead_wait_ms
                : undefined,
            showZero: typeof e.bulkhead_wait_ms === 'number',
          },
          {
            label: 'Response body processing',
            value:
              typeof e.response_body_process_ms === 'number'
                ? e.response_body_process_ms
                : undefined,
            showZero: typeof e.response_body_process_ms === 'number',
            setupTiming: true,
          },
          ...(hasFinalize
            ? [
                {
                  label: 'Limit reconcile',
                  value:
                    e._phase === 'final' &&
                    e.finalize_ms != null &&
                    typeof e.limit_reconcile_ms === 'number'
                      ? e.limit_reconcile_ms
                      : undefined,
                },
                {
                  label: 'Other finalize',
                  value:
                    e._phase === 'final' && e.finalize_ms != null
                      ? otherFinalize
                      : undefined,
                  showZero: true,
                },
              ]
            : [
                {
                  label: 'Limit reconcile',
                  value:
                    typeof e.limit_reconcile_ms === 'number'
                      ? e.limit_reconcile_ms
                      : undefined,
                  showZero: typeof e.limit_reconcile_ms === 'number',
                },
              ]),
        ]}
      />

      <Section
        title={LATENCY_RESPONSIBILITY_META['upstream-net'].label}
        color={LATENCY_RESPONSIBILITY_META['upstream-net'].color}
        description={LATENCY_RESPONSIBILITY_META['upstream-net'].description}
        total={upstreamNetTotal}
        duration={duration}
        items={[
          {
            label: 'DNS',
            value: e.dns_ms,
            showZero: e.dns_ms != null,
          },
          {
            label: 'Connect (TCP+TLS)',
            value: e.connect_ms,
            showZero: e.connect_ms != null,
          },
        ]}
        pill={
          e.connection_reused ? (
            <span className="px-1 py-0.5 rounded bg-emerald-500/20 text-emerald-400 text-[9px] leading-none">
              Warm pool
            </span>
          ) : null
        }
        showZero={
          e.dns_ms != null ||
          e.connect_ms != null ||
          e.connection_reused === true
        }
      />

      <Section
        title={LATENCY_RESPONSIBILITY_META['upstream-wait'].label}
        color={LATENCY_RESPONSIBILITY_META['upstream-wait'].color}
        description={LATENCY_RESPONSIBILITY_META['upstream-wait'].description}
        total={upstreamWaitTotal}
        duration={duration}
        items={[
          {
            label: 'Header wait',
            value: e.upstream_ttfb_ms != null ? upstream_wait_ms : undefined,
            showZero: e.upstream_ttfb_ms != null,
            description:
              'Combined provider generation, upstream transit, and runtime scheduling until response headers arrive.',
          },
          {
            label: 'Response body wait',
            value: e.response_body_wait_ms,
            showZero: e.response_body_wait_ms != null,
            setupTiming: true,
            description:
              'Combined provider generation, upstream transit, and runtime scheduling between response-body frames.',
          },
        ]}
        showZero={e.upstream_ttfb_ms != null || e.response_body_wait_ms != null}
      />

      <Section
        title={LATENCY_RESPONSIBILITY_META.unattributed.label}
        color={LATENCY_RESPONSIBILITY_META.unattributed.color}
        description={unattributedDescription}
        duration={duration}
        total={unattributedTotal}
        items={[
          {
            label: 'Retry overhead',
            value: e.retry_overhead_ms,
            setupTiming: true,
            description:
              'One aggregate across prior attempts; its cc-lb, network, and upstream portions cannot be separated.',
          },
          {
            label: hasRequestBodyBreakdown
              ? 'Other request body'
              : 'Request body read',
            value: requestBodyOther,
            showZero:
              e.request_body_read_ms != null && !hasRequestBodyBreakdown,
          },
          {
            label: responseBodyRemainderLabel,
            value: responseBodyOther,
            showZero:
              e._phase === 'final' &&
              !hasResponseBodyBreakdown &&
              (e.stream_total_ms != null || e.upstream_body_ms != null),
          },
          {
            label: 'Other lifecycle time',
            value: topLevelResidual,
            description:
              'Residual time with no finer timing witness available for responsibility attribution.',
          },
        ]}
      />
    </div>
  );

  const popover = (
    <div className="min-w-[260px] max-w-[320px] font-mono">
      <div className="text-[10px] uppercase tracking-wider text-text-faint mb-2">
        Latency by responsibility
      </div>
      {e.request_body_bytes != null && !isRenewal ? (
        <div className="text-[9px] leading-3 text-text-faint mb-2">
          Ingress body: {fmtBytes(e.request_body_bytes)}
        </div>
      ) : null}

      {isRenewal ? (
        <Section
          title={LATENCY_RESPONSIBILITY_META.renewal.label}
          color={LATENCY_RESPONSIBILITY_META.renewal.color}
          description={LATENCY_RESPONSIBILITY_META.renewal.description}
          total={duration}
          duration={duration}
          items={[]}
          showZero
        />
      ) : (
        proxySections
      )}

      <div className="border-t border-subtle mt-2 pt-1.5 flex items-center gap-2 text-[11px]">
        <span className="h-2 w-2 shrink-0" />
        <span className="text-text-faint flex-1">Total</span>
        <span className="tabular-nums text-text w-16 text-right">
          {fmtMs(duration)}
        </span>
        <span className="tabular-nums text-text-faint w-9 text-right">
          {duration > 0 ? '100%' : '—'}
        </span>
      </div>
    </div>
  );

  return (
    <td
      className="p-0 text-right whitespace-nowrap"
      onClick={(ev) => ev.stopPropagation()}
    >
      <Hint label={popover}>
        <button
          type="button"
          aria-label={triggerLabel}
          className="w-full px-3 py-2 cursor-help block bg-transparent border-0 text-inherit"
        >
          <div className="flex items-baseline justify-end tabular-nums leading-tight">
            <span className="shrink-0 w-[5ch] text-right text-text">
              {value}
            </span>
            <span className="shrink-0 w-[2ch] text-left text-text-faint">
              {unit}
            </span>
          </div>
          <div role="img" aria-label={sparklineLabel}>
            <Sparkline
              total={duration}
              segments={
                isRenewal
                  ? [
                      {
                        value: duration,
                        color: LATENCY_RESPONSIBILITY_META.renewal.color,
                      },
                    ]
                  : attribution.responsibilities
                      .filter(
                        (group) => group.key !== 'renewal' && group.valueMs > 0,
                      )
                      .map((group) => ({
                        value: group.valueMs,
                        color: group.color,
                      }))
              }
            />
          </div>
        </button>
      </Hint>
    </td>
  );
}
