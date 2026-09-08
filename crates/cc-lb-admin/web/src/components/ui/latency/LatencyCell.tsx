import type React from 'react';
import { fmtMs, fmtMsCompact, fmtSetupMs } from '../../../lib/format';
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
}) {
  const visibleItems = items.filter(
    (item) => item.value != null && (item.value > 0 || item.showZero === true),
  );
  if (total <= 0 && visibleItems.length === 0) return null;

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

  const popover = (
    <div className="min-w-[240px] font-mono">
      <div className="text-[10px] uppercase tracking-wider text-text-faint mb-2">
        Latency
      </div>

      <div className="flex flex-col">
        <Section
          title="Internal pre"
          color="bg-sky-400"
          total={groups.internalPre}
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
            { label: 'Body', value: e.upstream_body_ms },
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
                e._phase === 'final'
                  ? e.stream_last_content_delta_ms
                  : undefined,
            },
            {
              label: 'Inter-token avg',
              value: e._phase === 'final' ? e.inter_token_avg_ms : undefined,
            },
          ]}
        />

        <Section
          title="Internal post"
          color="bg-slate-400"
          total={groups.internalPost}
          duration={duration}
          items={[
            {
              label: 'Limit reconcile',
              value: e._phase === 'final' ? e.limit_reconcile_ms : undefined,
            },
          ]}
        />
      </div>

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
        {groups.unaccounted > 10 && (
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
          aria-label={`Latency ${value} ${unit}, show breakdown`}
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
          <Sparkline
            segments={[
              { value: groups.internalPre, color: 'bg-sky-400' },
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
            ]}
          />
        </button>
      </Hint>
    </td>
  );
}
