import type React from 'react';
import { fmtBytes, fmtMs, fmtMsCompact, fmtSetupMs } from '../../../lib/format';
import type { RequestEventWithPhase } from '../../../lib/RequestEventTypes';
import { cx, Hint } from '../primitives';
import { Sparkline } from '../Sparkline';
import {
  CACHE_SETUP_TIMING_STAGES,
  computeStageGroups,
  deriveOtherSetup,
  deriveProxyTimelineDuration,
  deriveSetupOverhead,
  hasSetupTimingBreakdown,
  responseBodyDuration,
} from './computeStageGroups';

function pctOf(value: number | null | undefined, denom: number): number {
  if (denom <= 0 || value == null || value <= 0) return 0;
  return Math.round((value / denom) * 100);
}

function Section({
  title,
  color,
  total,
  duration,
  items,
  pill,
  showZero,
}: {
  title: string;
  color: string;
  total: number;
  duration: number;
  items: {
    label: string;
    value: number | null | undefined;
    showZero?: boolean;
    setupTiming?: boolean;
  }[];
  pill?: React.ReactNode;
  showZero?: boolean;
}) {
  const visibleItems = items.filter(
    (item) => item.value != null && (item.value > 0 || item.showZero === true),
  );
  if (total <= 0 && visibleItems.length === 0 && showZero !== true) return null;

  return (
    <div className="mb-2 last:mb-0">
      <div className="flex items-center gap-2 text-[11px] mb-1">
        <span className={cx('h-2 w-2 rounded-full shrink-0', color)} />
        <span className="text-text-muted flex-1 font-medium flex items-center gap-2">
          {title}
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
              <span className="text-text-faint flex-1 truncate">
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

export function LatencyCell({
  event: e,
  isPartial,
}: {
  event: RequestEventWithPhase;
  isPartial?: boolean;
}) {
  const groups = computeStageGroups(e);
  const isRenewal = e.source_kind === 'renewal';
  const hasFinalize = e.finalize_ms != null;
  const requestBodyRead = e.request_body_read_ms ?? 0;
  const proxyInternalPre = Math.max(0, groups.internalPre - requestBodyRead);
  const bodyDuration = responseBodyDuration(e);
  const isCancelledPartial = e.status === 499 && e.upstream_body_ms != null;
  const limitReconcileMs =
    typeof e.limit_reconcile_ms === 'number' ? e.limit_reconcile_ms : 0;
  const otherFinalize = Math.max(0, (e.finalize_ms ?? 0) - limitReconcileMs);
  const duration = isPartial
    ? deriveProxyTimelineDuration(e)
    : e._phase === 'final'
      ? deriveProxyTimelineDuration(e)
      : 0;
  const { value, unit } = fmtMsCompact(e._phase === 'final' ? duration : 0);

  const setup_overhead_ms = deriveSetupOverhead(e);
  const internalPreItems = hasSetupTimingBreakdown(e)
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

  const upstream_wait_ms = Math.max(
    0,
    (e.upstream_ttfb_ms ?? 0) -
      (e.bulkhead_wait_ms ?? 0) -
      (e.dns_ms ?? 0) -
      (e.connect_ms ?? 0),
  );
  const sparklineStages = isRenewal
    ? [`Renewal cycle ${fmtMs(duration)}`]
    : [
        ...(e.request_body_read_ms != null
          ? [`Request body read ${fmtMs(requestBodyRead)}`]
          : []),
        ...(e.request_body_bytes != null
          ? [`Ingress body ${fmtBytes(e.request_body_bytes)}`]
          : []),
        ...(proxyInternalPre > 0
          ? [`Internal pre ${fmtMs(proxyInternalPre)}`]
          : []),
        ...(groups.wait > 0 ? [`Wait ${fmtMs(groups.wait)}`] : []),
        ...(groups.upstream > 0 ? [`Upstream ${fmtMs(groups.upstream)}`] : []),
        ...(groups.body > 0 ||
        isCancelledPartial ||
        e.stream_total_ms != null ||
        e.upstream_body_ms != null
          ? [
              `${
                isCancelledPartial
                  ? 'Partial stream (client cancelled)'
                  : e.stream_total_ms != null
                    ? 'Stream relay'
                    : 'Body collect'
              } ${fmtMs(bodyDuration)}`,
            ]
          : []),
        ...(hasFinalize
          ? [`Finalize ${fmtMs(groups.internalPost)}`]
          : groups.internalPost > 0 || e.limit_reconcile_ms != null
            ? [`Internal post ${fmtMs(groups.internalPost)}`]
            : []),
        ...(groups.unaccounted > 10
          ? [`Unaccounted ${fmtMs(groups.unaccounted)}`]
          : []),
      ];
  const sparklineLabel = `Latency stages: ${sparklineStages.join(', ')}`;
  const triggerStages = isRenewal
    ? [`Renewal cycle ${fmtMs(duration)}`]
    : [
        ...(e.request_body_read_ms != null
          ? [`Proxy request body read ${fmtMs(requestBodyRead)}`]
          : []),
        ...(e.request_body_bytes != null
          ? [`Ingress body ${fmtBytes(e.request_body_bytes)}`]
          : []),
        ...(isCancelledPartial
          ? [`Partial stream (client cancelled) ${fmtMs(bodyDuration)}`]
          : []),
        ...(hasFinalize
          ? [`Finalize ${fmtMs(groups.internalPost)}`]
          : e.limit_reconcile_ms != null
            ? [`Internal post ${fmtMs(groups.internalPost)}`]
            : []),
      ];
  const triggerLabel = `Latency ${value} ${unit}${
    triggerStages.length > 0 ? `, ${triggerStages.join(', ')}` : ''
  }, show breakdown`;

  const proxySections = (
    <div className="flex flex-col">
      <Section
        title="Request body read"
        color="bg-cyan-400"
        total={requestBodyRead}
        duration={duration}
        items={[]}
        pill={
          e.request_body_bytes != null ? (
            <span className="text-[9px] leading-none text-text-faint">
              Ingress body: {fmtBytes(e.request_body_bytes)}
            </span>
          ) : null
        }
        showZero={e.request_body_read_ms != null}
      />

      <Section
        title="Internal pre"
        color="bg-sky-400"
        total={proxyInternalPre}
        duration={duration}
        items={internalPreItems}
      />

      <Section
        title="Wait"
        color="bg-amber-400"
        total={groups.wait}
        duration={duration}
        items={[
          { label: 'Bulkhead', value: e.bulkhead_wait_ms },
          { label: 'DNS', value: e.dns_ms },
        ]}
      />

      <Section
        title="Upstream"
        color="bg-violet-400"
        total={groups.upstream}
        duration={duration}
        pill={
          e.connection_reused ? (
            <span className="px-1 py-0.5 rounded bg-emerald-500/20 text-emerald-400 text-[9px] leading-none">
              Warm pool
            </span>
          ) : null
        }
        items={[
          { label: 'Connect', value: e.connect_ms },
          { label: 'Wait (TTFB)', value: upstream_wait_ms },
        ]}
      />

      <Section
        title="Body"
        color="bg-emerald-400"
        total={groups.body}
        duration={duration}
        items={[
          {
            label: isCancelledPartial
              ? 'Partial stream (client cancelled)'
              : e.stream_total_ms != null
                ? 'Stream relay'
                : 'Body collect',
            value: bodyDuration,
            showZero: isCancelledPartial,
          },
          {
            label: 'First content delta',
            value:
              e._phase === 'final'
                ? e.stream_first_content_delta_ms
                : undefined,
          },
          {
            label: 'Last content delta',
            value:
              e._phase === 'final' ? e.stream_last_content_delta_ms : undefined,
          },
          {
            label: 'Inter-token avg',
            value: e._phase === 'final' ? e.inter_token_avg_ms : undefined,
          },
        ]}
      />

      {hasFinalize ? (
        <Section
          title="Finalize"
          color="bg-slate-400"
          total={groups.internalPost}
          duration={duration}
          items={[
            {
              label: 'Limit reconcile',
              value:
                e._phase === 'final' && e.finalize_ms != null
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
          ]}
          showZero
        />
      ) : (
        <Section
          title="Internal post"
          color="bg-slate-400"
          total={groups.internalPost}
          duration={duration}
          items={[
            {
              label: 'Limit reconcile',
              value: e._phase === 'final' ? e.limit_reconcile_ms : undefined,
              showZero: e.limit_reconcile_ms != null,
            },
          ]}
          showZero={e.limit_reconcile_ms != null}
        />
      )}
    </div>
  );

  const popover = (
    <div className="min-w-[240px] font-mono">
      <div className="text-[10px] uppercase tracking-wider text-text-faint mb-2">
        Latency
      </div>

      {isRenewal ? (
        <Section
          title="Renewal cycle"
          color="bg-blue-400"
          total={duration}
          duration={duration}
          items={[]}
          showZero
        />
      ) : (
        proxySections
      )}

      <div className="border-t border-subtle mt-2 pt-1.5 flex flex-col gap-1">
        <div className="flex items-center gap-2 text-[11px]">
          <span className="h-2 w-2 shrink-0" />
          <span className="text-text-faint flex-1">Total</span>
          <span className="tabular-nums text-text w-14 text-right">
            {fmtMs(duration)}
          </span>
          <span className="tabular-nums text-text-faint w-9 text-right">
            {duration > 0 ? '100%' : '—'}
          </span>
        </div>
        {!isRenewal && groups.unaccounted > 10 && (
          <div className="flex items-center gap-2 text-[11px]">
            <span className="h-2 w-2 shrink-0 bg-slate-700/30 bg-[repeating-linear-gradient(45deg,_transparent_0_4px,_rgba(255,255,255,0.05)_4px_8px)] rounded-full" />
            <span className="text-text-faint flex-1">Unaccounted</span>
            <span className="tabular-nums text-text-muted w-14 text-right">
              {fmtMs(groups.unaccounted)}
            </span>
            <span className="tabular-nums text-text-faint w-9 text-right">
              {pctOf(groups.unaccounted, duration)}%
            </span>
          </div>
        )}
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
              segments={
                isRenewal
                  ? [{ value: duration, color: 'bg-blue-400' }]
                  : [
                      { value: requestBodyRead, color: 'bg-cyan-400' },
                      { value: proxyInternalPre, color: 'bg-sky-400' },
                      { value: groups.wait, color: 'bg-amber-400' },
                      { value: groups.upstream, color: 'bg-violet-400' },
                      { value: groups.body, color: 'bg-emerald-400' },
                      { value: groups.internalPost, color: 'bg-slate-400' },
                      ...(groups.unaccounted > 10
                        ? [
                            {
                              value: groups.unaccounted,
                              color:
                                'bg-slate-700/30 bg-[repeating-linear-gradient(45deg,_transparent_0_4px,_rgba(255,255,255,0.05)_4px_8px)]',
                            },
                          ]
                        : []),
                    ]
              }
            />
          </div>
        </button>
      </Hint>
    </td>
  );
}
