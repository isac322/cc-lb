import { Popover as BasePopover } from '@base-ui/react/popover';
import {
  ArrowDownToLine,
  Cpu,
  Network,
  RefreshCw,
  Server,
  Split,
  TriangleAlert,
} from 'lucide-react';
import {
  type ReactElement,
  type ReactNode,
  useCallback,
  useMemo,
  useState,
} from 'react';
import {
  fmtBytes,
  fmtIoMs,
  fmtMs,
  fmtN,
  fmtSetupMs,
} from '../../../lib/format';
import type { RequestEventWithPhase } from '../../../lib/RequestEventTypes';
import { cx, Skeleton } from '../primitives';
import {
  CACHE_SETUP_TIMING_STAGES,
  computeLatencyAttribution,
  computeStageGroups,
  deriveOtherSetup,
  deriveProxyTimelineDuration,
  deriveSetupOverhead,
  hasSetupTimingBreakdown,
  LATENCY_CATEGORY_META,
  type LatencyAttribution,
  type LatencyCategory,
  type LatencyCategoryValue,
  responseBodyDuration,
} from './computeStageGroups';

// -----------------------------------------------------------------------------
// Group / stage taxonomy
// -----------------------------------------------------------------------------

type StageGroup =
  | 'renewal'
  | 'internal_pre'
  | 'retry'
  | 'wait'
  | 'upstream'
  | 'body'
  | 'internal_post';

const GROUP_META: Record<StageGroup, { label: string; text: string }> = {
  renewal: {
    label: 'Scheduler renewal',
    text: 'text-[color:var(--latency-downstream-text)]',
  },
  internal_pre: {
    label: 'Ingress and cc-lb setup',
    text: 'text-[color:var(--latency-proxy-text)]',
  },
  retry: {
    label: 'Mixed retry path',
    text: 'text-[color:var(--latency-retry-text)]',
  },
  wait: {
    label: 'Bulkhead queue and DNS',
    text: 'text-[color:var(--latency-upstream-text)]',
  },
  upstream: {
    label: 'Upstream attempt',
    text: 'text-[color:var(--latency-upstream-text)]',
  },
  body: {
    label: 'Response body parent',
    text: 'text-text-muted',
  },
  internal_post: {
    label: 'cc-lb finalize',
    text: 'text-[color:var(--latency-proxy-text)]',
  },
};

function getGroupLabel(group: StageGroup, hasFinalizeTiming: boolean): string {
  return group === 'internal_post' && hasFinalizeTiming
    ? 'Finalize'
    : GROUP_META[group].label;
}

const PROXY_GROUP_ORDER: StageGroup[] = [
  'internal_pre',
  'retry',
  'wait',
  'upstream',
  'body',
  'internal_post',
];
const RENEWAL_GROUP_ORDER: StageGroup[] = ['renewal'];

const UNACCOUNTED_BG =
  'bg-slate-700/30 bg-[repeating-linear-gradient(45deg,_transparent_0_4px,_rgba(255,255,255,0.06)_4px_8px)]';

const STAGE_DESCRIPTIONS: Record<string, string> = {
  request_body_read:
    'Parent interval for collecting the client request body. New rows split observed frame wait from local handling below; legacy rows remain unattributed rather than being called network time.',
  auth: 'Bearer-token validation, principal lookup, and request-context assembly before routing.',
  route:
    'Match the request path against configured upstream routes and pick a candidate list.',
  limit_reserve:
    'Reserve tokens / requests against the principal budget before dispatching upstream.',
  setup_overhead:
    'Legacy fallback: proxy_setup_ms minus auth + route + limit reserve.',
  json_parse_ms: 'Parse the request body with sonic_rs::from_slice.',
  cache_tokenizer_queue_ms:
    'Wait for a prompt-cache analysis executor semaphore permit.',
  cache_structure_ms:
    'Flatten blocks, resolve breakpoints, and compute structural prefix hashes.',
  cache_serialize_ms:
    'Materialize exact serialized prefix bytes used for token counting.',
  cache_token_key_ms:
    'Hash the credential-scoped exact token-prefix cache key with BLAKE3.',
  cache_count_lookup_ms:
    'Distributed cache intervals: claim, hit/miss bookkeeping, leader completion, count retrieval, and coalesced follower wait. Tokenization is excluded.',
  cache_tokenize_ms:
    'Run exact BPE only when serialized prefix bytes fall between the hybrid threshold fast paths. Byte fast paths and worker fallbacks record zero.',
  prepare_signer_ms:
    'Build upstream credentials, including lookup, decrypt, and lazy OAuth refresh.',
  other_setup:
    'Remaining proxy setup time after auth, route, limit reserve, and measured setup stages.',
  retry_overhead:
    'Wall-clock time from the earlier attempt start until the final retry starts. It mixes cc-lb and upstream work, is counted once, and does not overlap the final-attempt stages.',
  shape:
    'Run the shape plugin: dialect adaptation + Anthropic-format shaping of the outbound body.',
  sign: 'Sign the outbound request (OAuth refresh if the credential needs one).',
  bulkhead_wait:
    'Time queued inside cc-lb for an upstream concurrency slot. This is local waiting, not network time.',
  dns: 'Observed DNS resolution for the upstream host. It is absent when not measured or skipped on connection reuse.',
  connect:
    'Observed TCP connect + TLS handshake. It is absent when a warm pooled connection is reused.',
  upstream_wait:
    'Header wait remaining after bulkhead, DNS, and connect. It combines upstream network, provider work, and runtime scheduling; it is not pure provider processing or RTT.',
  stream_relay:
    'Parent response-body interval. New rows split upstream frame wait, local relay work, and downstream consumer poll gaps below; any remainder stays unattributed.',
  partial_stream:
    'Partial parent response-body interval retained through cancellation. Split child measurements remain visible without implying completion.',
  body_collect:
    'Parent non-stream response-body collection interval. Legacy rows mix upstream wait and local collection work.',
  finalize:
    'Mandatory cc-lb accounting and finalization after the response body completed or cancellation was observed, ending immediately before the terminal event is published.',
  limit_reconcile:
    'Legacy timing for reconciling actual token usage against the reserved budget.',
  renewal_cycle:
    'The complete cache-keepalive renewal cycle performed by the scheduler. This is not a proxy request breakdown.',
};

const MARKER_DESCRIPTIONS: Record<string, string> = {
  message_start:
    'First `event: message_start` frame — Anthropic has begun the assistant turn (usage/model metadata available).',
  content_block_start:
    'First `event: content_block_start` frame — a text or tool_use content block just started.',
  first_delta:
    'Time-to-first-token (TTFT). First `event: content_block_delta` — the first actual token/character delivered.',
  last_delta: 'Last `event: content_block_delta` — the final streamed token.',
  message_stop:
    '`event: message_stop` — the assistant turn is finalized. Final usage + stop reason are available.',
  last_chunk:
    'Last byte received from Anthropic on this stream. End of upstream response.',
};

// -----------------------------------------------------------------------------
// Data model
// -----------------------------------------------------------------------------

interface Stage {
  key: string;
  label: string;
  group: StageGroup;
  ms: number;
  setupTiming?: boolean;
  detail?: string;
}

type StageCategory =
  | LatencyCategory
  | 'mixed_upstream'
  | 'mixed_retry'
  | 'parent_interval'
  | 'renewal';

interface StageDetail extends Stage {
  index: number;
  groupCount: number;
  fill: string;
  category: StageCategory;
}

const STAGE_CATEGORY_META: Record<
  StageCategory,
  { label: string; textClass: string; hue: number; sat: number }
> = {
  downstream_network: {
    label: LATENCY_CATEGORY_META.downstream_network.label,
    textClass: LATENCY_CATEGORY_META.downstream_network.textClass,
    hue: 188,
    sat: 82,
  },
  cc_lb: {
    label: LATENCY_CATEGORY_META.cc_lb.label,
    textClass: LATENCY_CATEGORY_META.cc_lb.textClass,
    hue: 238,
    sat: 82,
  },
  upstream_network: {
    label: LATENCY_CATEGORY_META.upstream_network.label,
    textClass: LATENCY_CATEGORY_META.upstream_network.textClass,
    hue: 42,
    sat: 88,
  },
  upstream_processing: {
    label: LATENCY_CATEGORY_META.upstream_processing.label,
    textClass: LATENCY_CATEGORY_META.upstream_processing.textClass,
    hue: 158,
    sat: 64,
  },
  mixed_upstream: {
    label: 'Combined upstream wait',
    textClass: 'text-[color:var(--latency-upstream-text)]',
    hue: 86,
    sat: 60,
  },
  mixed_retry: {
    label: 'Mixed retry path',
    textClass: 'text-[color:var(--latency-retry-text)]',
    hue: 24,
    sat: 88,
  },
  parent_interval: {
    label: 'Parent interval · mixed / unattributed',
    textClass: 'text-text-muted',
    hue: 215,
    sat: 18,
  },
  renewal: {
    label: 'Scheduler renewal · not proxy-attributed',
    textClass: 'text-[color:var(--latency-downstream-text)]',
    hue: 185,
    sat: 70,
  },
};

function classifyStage(
  stage: Stage,
  event: RequestEventWithPhase,
): StageCategory {
  if (event.source_kind === 'renewal') return 'renewal';
  if (stage.key === 'request_body_read') return 'parent_interval';
  if (stage.key === 'retry_overhead') return 'mixed_retry';
  if (stage.key === 'dns' || stage.key === 'connect') return 'upstream_network';
  if (stage.key === 'upstream_wait') return 'mixed_upstream';
  if (
    stage.key === 'stream_relay' ||
    stage.key === 'partial_stream' ||
    stage.key === 'body_collect'
  )
    return 'parent_interval';
  return 'cc_lb';
}

function stageShadeHsl(
  category: StageCategory,
  index: number,
  count: number,
): string {
  const { hue, sat } = STAGE_CATEGORY_META[category];
  const lightness =
    count <= 1 ? 55 : Math.round(38 + (index / (count - 1)) * 38);
  return `hsl(${hue}deg ${sat}% ${lightness}%)`;
}

function formatStageMs(stage: Stage): string {
  return stage.setupTiming ? fmtSetupMs(stage.ms) : fmtMs(stage.ms);
}

export function buildStageDetails(e: RequestEventWithPhase): StageDetail[] {
  const raw: Stage[] = [];
  const push = (
    key: string,
    label: string,
    group: StageGroup,
    ms: number | null | undefined,
    detail?: string,
  ) => {
    if (ms && ms > 0) raw.push({ key, label, group, ms, detail });
  };
  const pushRecorded = (
    key: string,
    label: string,
    group: StageGroup,
    ms: number | null | undefined,
    detail?: string,
  ) => {
    if (ms != null && ms >= 0) raw.push({ key, label, group, ms, detail });
  };
  const pushMeasured = (
    key: string,
    label: string,
    group: StageGroup,
    ms: number | null | undefined,
  ) => {
    if (ms != null && ms >= 0)
      raw.push({ key, label, group, ms, setupTiming: true });
  };

  if (e._phase === 'final' && e.source_kind === 'renewal') {
    pushRecorded('renewal_cycle', 'Renewal cycle', 'renewal', e.duration_ms);
  } else {
    const requestBodyDetail =
      e.request_body_bytes != null
        ? `Ingress body: ${fmtBytes(e.request_body_bytes)}`
        : undefined;
    pushRecorded(
      'request_body_read',
      'Request body read',
      'internal_pre',
      e.request_body_read_ms,
      requestBodyDetail,
    );

    if (hasSetupTimingBreakdown(e)) {
      for (const [field, label] of CACHE_SETUP_TIMING_STAGES) {
        pushMeasured(field, label, 'internal_pre', e[field]);
      }
      push('auth', 'Auth', 'internal_pre', e.auth_ms);
      push('route', 'Route', 'internal_pre', e.route_ms);
      push(
        'limit_reserve',
        'Limit reserve',
        'internal_pre',
        e.limit_reserve_ms,
      );
      pushMeasured(
        'prepare_signer_ms',
        'Prepare signer',
        'internal_pre',
        e.prepare_signer_ms,
      );
      if (e._phase === 'final' && e.proxy_setup_ms != null) {
        pushMeasured(
          'other_setup',
          'Other setup',
          'internal_pre',
          deriveOtherSetup(e),
        );
      }
    } else {
      push('auth', 'Auth', 'internal_pre', e.auth_ms);
      push('route', 'Route', 'internal_pre', e.route_ms);
      push(
        'limit_reserve',
        'Limit reserve',
        'internal_pre',
        e.limit_reserve_ms,
      );
      push(
        'setup_overhead',
        'Setup overhead',
        'internal_pre',
        deriveSetupOverhead(e),
      );
    }
    pushRecorded(
      'retry_overhead',
      'Earlier attempt before retry',
      'retry',
      e.retry_overhead_ms,
    );
    push('shape', 'Shape', 'internal_pre', e.shape_ms);
    push('sign', 'Sign', 'internal_pre', e.sign_ms);
    push('bulkhead_wait', 'Bulkhead wait', 'wait', e.bulkhead_wait_ms);
    push('dns', 'DNS', 'wait', e.dns_ms);
    push('connect', 'Connect (TCP+TLS)', 'upstream', e.connect_ms);
    if (e.upstream_ttfb_ms != null) {
      const upstreamWait = Math.max(
        0,
        e.upstream_ttfb_ms -
          (e.bulkhead_wait_ms ?? 0) -
          (e.dns_ms ?? 0) -
          (e.connect_ms ?? 0),
      );
      push('upstream_wait', 'Upstream wait', 'upstream', upstreamWait);
    }

    if (e._phase === 'final') {
      const bodyMs = responseBodyDuration(e);
      const hasIncompleteStream =
        e.status === 499 ||
        (e.stream_total_ms == null &&
          ((e.sse_event_count ?? 0) > 0 ||
            e.stream_message_start_ms != null ||
            e.stream_last_chunk_ms != null));
      if (hasIncompleteStream) {
        pushRecorded(
          'partial_stream',
          e.status === 499
            ? 'Partial stream (client cancelled)'
            : 'Partial stream',
          'body',
          e.upstream_body_ms,
        );
      } else if (e.stream_total_ms != null) {
        pushRecorded('stream_relay', 'Stream relay', 'body', bodyMs);
      } else {
        pushRecorded(
          'body_collect',
          'Body collect',
          'body',
          e.upstream_body_ms,
        );
      }

      if (e.finalize_ms != null) {
        const reconcileMs = e.limit_reconcile_ms ?? 0;
        const finalizeDetail =
          e.limit_reconcile_ms != null
            ? `Limit reconcile: ${fmtMs(reconcileMs)} · Other finalize: ${fmtMs(
                Math.max(0, (e.finalize_ms ?? 0) - reconcileMs),
              )}`
            : undefined;
        pushRecorded(
          'finalize',
          'Finalize',
          'internal_post',
          e.finalize_ms,
          finalizeDetail,
        );
      } else {
        push(
          'limit_reconcile',
          'Limit reconcile',
          'internal_post',
          e.limit_reconcile_ms,
        );
      }
    }
  }

  const categorized = raw.map((stage) => ({
    ...stage,
    category: classifyStage(stage, e),
  }));
  const perGroupCount: Partial<Record<StageGroup, number>> = {};
  for (const stage of categorized) {
    perGroupCount[stage.group] = (perGroupCount[stage.group] ?? 0) + 1;
  }
  const seen: Partial<Record<StageGroup, number>> = {};
  return categorized.map((stage) => {
    const groupCount = perGroupCount[stage.group] ?? 1;
    const index = seen[stage.group] ?? 0;
    seen[stage.group] = index + 1;
    return {
      ...stage,
      index,
      groupCount,
      fill: stageShadeHsl(stage.category, index, groupCount),
    };
  });
}

interface SseMarker {
  key: string;
  label: string;
  relayMs: number;
  absMs: number;
  color: string;
  starred?: boolean;
}

export function buildSseMarkers(e: RequestEventWithPhase): SseMarker[] {
  if (e.source_kind === 'renewal') return [];

  const relayStart =
    (e.request_body_read_ms ?? 0) +
    (e.auth_ms ?? 0) +
    (e.route_ms ?? 0) +
    (e.limit_reserve_ms ?? 0) +
    deriveSetupOverhead(e) +
    (e.retry_overhead_ms ?? 0) +
    (e.shape_ms ?? 0) +
    (e.sign_ms ?? 0) +
    (e.upstream_ttfb_ms ?? 0);

  const defs: Array<{
    key: string;
    label: string;
    ms: number | undefined;
    color: string;
    starred?: boolean;
  }> = [
    {
      key: 'message_start',
      label: 'Message start',
      ms: e._phase === 'final' ? e.stream_message_start_ms : undefined,
      color: 'text-[color:var(--latency-downstream-text)]',
    },
    {
      key: 'content_block_start',
      label: 'Content block start',
      ms: e._phase === 'final' ? e.stream_content_block_start_ms : undefined,
      color: 'text-[color:var(--latency-downstream-text)]',
    },
    {
      key: 'first_delta',
      label: 'First content delta (TTFT)',
      ms: e._phase === 'final' ? e.stream_first_content_delta_ms : undefined,
      color: 'text-[color:var(--latency-upstream-text)]',
      starred: true,
    },
    {
      key: 'last_delta',
      label: 'Last content delta',
      ms: e._phase === 'final' ? e.stream_last_content_delta_ms : undefined,
      color: 'text-[color:var(--latency-provider-text)]',
    },
    {
      key: 'message_stop',
      label: 'Message stop',
      ms: e._phase === 'final' ? e.stream_message_stop_ms : undefined,
      color: 'text-[color:var(--latency-marker-stop-text)]',
    },
    {
      key: 'last_chunk',
      label: 'Last chunk',
      ms: e._phase === 'final' ? e.stream_last_chunk_ms : undefined,
      color: 'text-text-muted',
    },
  ];

  return defs
    .filter((d) => d.ms != null && d.ms >= 0)
    .map((d) => ({
      key: d.key,
      label: d.label,
      relayMs: d.ms as number,
      absMs: relayStart + (d.ms as number),
      color: d.color,
      starred: d.starred,
    }));
}

interface StaggeredMarker extends SseMarker {
  track: number;
}

export function computeMarkerTracks(
  markers: SseMarker[],
  total: number,
  minSpacePct = 6,
): StaggeredMarker[] {
  const sorted = [...markers].sort((a, b) => a.absMs - b.absMs);
  const trackEnds: number[] = [];
  const result: StaggeredMarker[] = [];
  for (const m of sorted) {
    const leftPct = total > 0 ? (m.absMs / total) * 100 : 0;
    let track = 0;
    while (
      track < trackEnds.length &&
      leftPct - trackEnds[track] < minSpacePct
    ) {
      track++;
    }
    trackEnds[track] = leftPct;
    result.push({ ...m, track });
  }
  return result;
}

// -----------------------------------------------------------------------------
// Bidirectional highlight state
// -----------------------------------------------------------------------------

interface ActiveKeyApi {
  activeKey: string | null;
  stickyKey: string | null;
  isActive: (key: string) => boolean;
  isSticky: (key: string) => boolean;
  bind: (key: string) => {
    onMouseEnter: () => void;
    onMouseLeave: () => void;
    onFocus: () => void;
    onBlur: () => void;
    onClick: () => void;
  };
}

function useActiveKey(): ActiveKeyApi {
  const [hoverKey, setHoverKey] = useState<string | null>(null);
  const [stickyKey, setStickyKey] = useState<string | null>(null);
  const activeKey = hoverKey ?? stickyKey;

  const bind = useCallback(
    (key: string) => ({
      onMouseEnter: () => setHoverKey(key),
      onMouseLeave: () => setHoverKey(null),
      onFocus: () => setHoverKey(key),
      onBlur: () => setHoverKey(null),
      onClick: () =>
        setStickyKey((prev: string | null) => (prev === key ? null : key)),
    }),
    [],
  );

  return {
    activeKey,
    stickyKey,
    isActive: (key) => activeKey === key,
    isSticky: (key) => stickyKey === key,
    bind,
  };
}

// -----------------------------------------------------------------------------
// Popover helper
// -----------------------------------------------------------------------------

function InfoPopover({
  content,
  side = 'top',
  children,
}: {
  content: ReactNode;
  side?: 'top' | 'right' | 'bottom' | 'left';
  children: ReactElement;
}) {
  return (
    <BasePopover.Root>
      <BasePopover.Trigger
        delay={60}
        closeDelay={80}
        openOnHover
        render={children}
      />
      <BasePopover.Portal>
        <BasePopover.Positioner
          side={side}
          sideOffset={6}
          align="center"
          className="z-[60]"
        >
          <BasePopover.Popup className="z-[60] max-w-[min(280px,calc(100vw-24px))] break-words rounded-md border border-subtle-strong bg-bg-sub px-2.5 py-1.5 text-[11px] text-text shadow-lg">
            {content}
          </BasePopover.Popup>
        </BasePopover.Positioner>
      </BasePopover.Portal>
    </BasePopover.Root>
  );
}

// -----------------------------------------------------------------------------
// Popover content
// -----------------------------------------------------------------------------

function StageInfo({
  stage,
  overview,
  total,
  groupLabel,
}: {
  stage: StageDetail;
  overview: StageDetail[];
  total: number;
  groupLabel: string;
}) {
  const overviewTotal = overview.reduce((a, s) => a + s.ms, 0);
  const description = STAGE_DESCRIPTIONS[stage.key];
  const multi = overview.length > 1 && overviewTotal > 0;

  return (
    <div className="flex flex-col gap-1.5 min-w-[240px]">
      <div
        className={cx(
          'text-[10px] uppercase tracking-wider',
          STAGE_CATEGORY_META[stage.category].textClass,
        )}
      >
        {STAGE_CATEGORY_META[stage.category].label}
      </div>
      {overview.length > 1 ? (
        <div className="text-[9px] text-text-faint">
          Chronological lane: {groupLabel} · {overview.length} parent stages
        </div>
      ) : null}
      {multi ? (
        <>
          <div className="flex h-3 rounded-sm overflow-hidden bg-overlay-5 border border-subtle/60">
            {overview.map((s) => {
              const isActive = s.key === stage.key;
              return (
                <div
                  key={s.key}
                  className={cx(
                    'h-full transition-all',
                    isActive
                      ? 'ring-2 ring-inset ring-white z-10'
                      : 'opacity-60',
                  )}
                  style={{
                    width: `${(s.ms / overviewTotal) * 100}%`,
                    backgroundColor: s.fill,
                    minWidth: '3px',
                  }}
                  title={`${s.label} · ${formatStageMs(s)}`}
                />
              );
            })}
          </div>
          <div className="flex flex-col gap-0.5">
            {overview.map((s) => {
              const isActive = s.key === stage.key;
              return (
                <div
                  key={s.key}
                  className={cx(
                    'flex items-center gap-1.5 text-[10px] px-1 py-0.5 rounded transition',
                    isActive
                      ? 'bg-overlay-10 text-text font-medium'
                      : 'text-text-muted',
                  )}
                >
                  <span
                    aria-hidden
                    className="inline-block h-2 w-2 rounded-sm shrink-0"
                    style={{ backgroundColor: s.fill }}
                  />
                  <span className="flex-1 truncate">{s.label}</span>
                  <span className="tabular-nums shrink-0 w-12 text-right">
                    {formatStageMs(s)}
                  </span>
                  <span
                    className={cx(
                      'tabular-nums shrink-0 w-8 text-right',
                      isActive ? 'text-text-muted' : 'text-text-faint',
                    )}
                  >
                    {pct(s.ms, total)}%
                  </span>
                </div>
              );
            })}
          </div>
        </>
      ) : (
        <div className="flex flex-col gap-0.5">
          <div className="flex items-baseline gap-1.5">
            <span
              aria-hidden
              className="inline-block h-2 w-2 rounded-sm shrink-0"
              style={{ backgroundColor: stage.fill }}
            />
            <span className="text-text font-medium">{stage.label}</span>
          </div>
          <div className="flex items-center gap-2 tabular-nums text-text-muted">
            <span>{formatStageMs(stage)}</span>
            <span className="text-text-faint">·</span>
            <span>{pct(stage.ms, total)}% of timeline</span>
          </div>
        </div>
      )}
      {stage.detail ? (
        <div className="text-text-muted leading-snug">{stage.detail}</div>
      ) : null}
      {description ? (
        <div className="text-text-faint leading-snug border-t border-subtle/40 pt-1.5">
          {description}
        </div>
      ) : null}
    </div>
  );
}

function UnaccountedInfo({ ms, total }: { ms: number; total: number }) {
  return (
    <div className="flex flex-col gap-0.5 min-w-[220px]">
      <div className="flex items-center gap-1.5">
        <span
          aria-hidden
          className={cx(UNACCOUNTED_BG, 'inline-block h-2 w-2 rounded-sm')}
        />
        <span className="text-text">Unaccounted</span>
      </div>
      <div className="flex items-center gap-2 tabular-nums text-text-muted">
        <span>{fmtMs(ms)}</span>
        <span className="text-text-faint">·</span>
        <span>{pct(ms, total)}% of request</span>
      </div>
      <div className="text-text-faint">
        Residual = duration_ms − Σ(known stages). Elevated values point at
        un-instrumented gaps in the proxy pipeline.
      </div>
    </div>
  );
}

function MarkerInfo({ marker }: { marker: SseMarker }) {
  const description = MARKER_DESCRIPTIONS[marker.key];
  return (
    <div className="flex flex-col gap-1 min-w-[220px]">
      <div className="flex items-center gap-1.5">
        <span className={cx(marker.color, 'w-3 text-center leading-none')}>
          {marker.starred ? '★' : '▲'}
        </span>
        <span className="text-text font-medium">{marker.label}</span>
      </div>
      <div className="flex items-center gap-2 tabular-nums text-text-muted">
        <span>{fmtMs(marker.absMs)}</span>
        <span className="text-text-faint">from request start</span>
      </div>
      <div className="flex items-center gap-2 tabular-nums text-text-faint">
        <span>+{fmtMs(marker.relayMs)}</span>
        <span>from relay start</span>
      </div>
      {description ? (
        <div className="text-text-faint leading-snug border-t border-subtle/40 pt-1.5">
          {description}
        </div>
      ) : null}
    </div>
  );
}

// -----------------------------------------------------------------------------
// Building blocks
// -----------------------------------------------------------------------------

function SegmentButton({
  stage,
  startMs,
  total,
  active,
  overview,
  groupLabel,
}: {
  stage: StageDetail;
  startMs: number;
  total: number;
  active: ActiveKeyApi;
  overview: StageDetail[];
  groupLabel: string;
}) {
  const isActive = active.isActive(stage.key);
  const isSticky = active.isSticky(stage.key);

  const leftPct = clampPct((startMs / total) * 100);
  const endPct = clampPct(((startMs + stage.ms) / total) * 100);
  const widthPct = Math.max(0, endPct - leftPct);
  const style: React.CSSProperties = {
    backgroundColor: stage.fill,
    width: `${widthPct}%`,
    left: `${leftPct}%`,
    minWidth: widthPct > 0 ? '2px' : undefined,
  };

  return (
    <InfoPopover
      content={
        <StageInfo
          stage={stage}
          overview={overview}
          total={total}
          groupLabel={groupLabel}
        />
      }
    >
      <button
        type="button"
        {...active.bind(stage.key)}
        data-testid={`latency-segment-${stage.key}`}
        aria-label={`${stage.label} ${formatStageMs(stage)}${stage.detail ? `, ${stage.detail}` : ''}`}
        aria-pressed={isSticky}
        className={cx(
          'absolute top-0 h-full rounded-sm outline-none transition-all cursor-pointer',
          isActive
            ? 'ring-2 ring-inset ring-white/80 z-10 brightness-110'
            : 'hover:brightness-110',
          isSticky ? 'shadow-[0_0_0_1px_rgba(255,255,255,0.35)]' : '',
        )}
        style={style}
      />
    </InfoPopover>
  );
}

function UnaccountedRow({
  ms,
  startMs,
  total,
  active,
}: {
  ms: number;
  startMs: number;
  total: number;
  active: ActiveKeyApi;
}) {
  const isActive = active.isActive('unaccounted');
  return (
    <div>
      <div className="flex items-center justify-between text-[10px] mb-0.5">
        <span
          className={cx('text-text-muted', isActive && 'text-text font-medium')}
        >
          Unaccounted
        </span>
        <span className="text-text-muted tabular-nums">
          {fmtMs(ms)} · {pct(ms, total)}%
        </span>
      </div>
      <div className="relative h-4 w-full rounded-sm bg-overlay-5">
        <InfoPopover content={<UnaccountedInfo ms={ms} total={total} />}>
          <button
            type="button"
            {...active.bind('unaccounted')}
            className={cx(
              UNACCOUNTED_BG,
              'absolute top-0 h-full rounded-sm outline-none transition cursor-pointer',
              isActive
                ? 'ring-2 ring-inset ring-white/70 z-10'
                : 'hover:brightness-110',
            )}
            style={{
              left: `${(startMs / total) * 100}%`,
              width: `${(ms / total) * 100}%`,
            }}
            aria-label={`Unaccounted ${fmtMs(ms)}`}
          />
        </InfoPopover>
      </div>
    </div>
  );
}

function MarkerDot({
  marker,
  total,
  active,
  axisTop,
  dotTop,
}: {
  marker: StaggeredMarker;
  total: number;
  active: ActiveKeyApi;
  axisTop: number;
  dotTop: number;
}) {
  const leftPct = clampPct((marker.absMs / total) * 100);
  const isActive = active.isActive(marker.key);
  const isSticky = active.isSticky(marker.key);
  const stemHeight = Math.max(0, dotTop - axisTop);
  return (
    <>
      {stemHeight > 0 && (
        <div
          aria-hidden
          className="absolute w-0.5 bg-[color:var(--color-text-faint)]/60 pointer-events-none -translate-x-1/2"
          style={{
            left: `${leftPct}%`,
            top: `${axisTop}px`,
            height: `${stemHeight}px`,
          }}
        />
      )}
      <InfoPopover content={<MarkerInfo marker={marker} />}>
        <button
          type="button"
          {...active.bind(marker.key)}
          aria-label={`${marker.label} ${fmtMs(marker.absMs)}`}
          aria-pressed={isSticky}
          className={cx(
            'absolute -translate-x-1/2 -translate-y-1/2 h-5 w-5 flex items-center justify-center outline-none cursor-pointer transition-transform',
            isActive ? 'scale-[1.4] z-10' : 'hover:scale-125',
            isSticky && !isActive ? 'scale-125' : '',
          )}
          style={{ left: `${leftPct}%`, top: `${dotTop}px` }}
        >
          <span
            aria-hidden
            className={cx(
              marker.color,
              'text-[13px] leading-none drop-shadow-[0_1px_1px_rgba(0,0,0,0.7)]',
            )}
          >
            {marker.starred ? '★' : '▲'}
          </span>
        </button>
      </InfoPopover>
    </>
  );
}

const NICE_TICK_INTERVALS_MS = [
  500, 1000, 2000, 5000, 10000, 15000, 30000, 60000, 120000, 300000, 600000,
  900000, 1800000, 3600000, 7200000, 21600000,
];

interface TimeAxisTick {
  leftPct: number;
  label: string;
}

export function pickTimeAxisTicks(
  totalMs: number,
  tsMs: number | null | undefined,
): { ticks: TimeAxisTick[] } {
  if (!totalMs || totalMs <= 0) return { ticks: [] };
  const targetTicks = 5;
  const rawInterval = totalMs / (targetTicks - 1);
  const interval =
    NICE_TICK_INTERVALS_MS.find((n) => n >= rawInterval) ??
    NICE_TICK_INTERVALS_MS[NICE_TICK_INTERVALS_MS.length - 1];
  const wallClock = tsMs != null;
  const format: 'HMS' | 'HM' = interval < 60000 ? 'HMS' : 'HM';
  const push = (ms: number): TimeAxisTick => ({
    leftPct: (ms / totalMs) * 100,
    label: wallClock
      ? formatWallClock((tsMs as number) + ms, format)
      : formatRelative(ms, format),
  });
  const minGapToEndpointFraction = 0.8;
  const interiorCutoffMs = totalMs - interval * minGapToEndpointFraction;
  const ticks: TimeAxisTick[] = [];
  for (let ms = 0; ms <= interiorCutoffMs; ms += interval) {
    ticks.push(push(ms));
  }
  ticks.push(push(totalMs));
  return { ticks };
}

function formatWallClock(ms: number, format: 'HMS' | 'HM'): string {
  const d = new Date(ms);
  const hh = String(d.getHours()).padStart(2, '0');
  const mm = String(d.getMinutes()).padStart(2, '0');
  const ss = String(d.getSeconds()).padStart(2, '0');
  return format === 'HMS' ? `${hh}:${mm}:${ss}` : `${hh}:${mm}`;
}

function formatRelative(ms: number, format: 'HMS' | 'HM'): string {
  const totalSec = Math.round(ms / 1000);
  const h = Math.floor(totalSec / 3600);
  const m = Math.floor((totalSec % 3600) / 60);
  const s = totalSec % 60;
  if (format === 'HMS') {
    if (h)
      return `${h}:${String(m).padStart(2, '0')}:${String(s).padStart(2, '0')}`;
    return `${m}:${String(s).padStart(2, '0')}`;
  }
  if (h) return `${h}:${String(m).padStart(2, '0')}`;
  return `${m}m`;
}

function TimeAxisTicks({
  totalMs,
  tsMs,
}: {
  totalMs: number;
  tsMs: number | null | undefined;
}) {
  const { ticks } = useMemo(
    () => pickTimeAxisTicks(totalMs, tsMs),
    [totalMs, tsMs],
  );
  if (ticks.length === 0) return null;
  const last = ticks.length - 1;
  return (
    <div className="relative h-3 mt-1" aria-hidden>
      {ticks.map((t, i) => {
        const isFirst = i === 0;
        const isLast = i === last;
        const anchor = isFirst
          ? 'left-0'
          : isLast
            ? 'right-0'
            : '-translate-x-1/2';
        const style = isFirst || isLast ? undefined : { left: `${t.leftPct}%` };
        return (
          <span
            key={`${t.leftPct}-${i}`}
            className={cx(
              'absolute top-0 text-[9px] text-text-faint tabular-nums whitespace-nowrap',
              anchor,
            )}
            style={style}
          >
            {t.label}
          </span>
        );
      })}
    </div>
  );
}

function SseLane({
  markers,
  total,
  active,
  event,
}: {
  markers: SseMarker[];
  total: number;
  active: ActiveKeyApi;
  event: RequestEventWithPhase;
}) {
  const staggered = useMemo(
    () => computeMarkerTracks(markers, total, 6),
    [markers, total],
  );
  const maxTrack = staggered.reduce((mx, m) => Math.max(mx, m.track), 0);
  const axisTop = 6;
  const trackStep = 10;
  const dotY = (track: number) => axisTop + 8 + track * trackStep;
  const laneHeight = dotY(maxTrack) + 8;

  return (
    <div>
      <div className="flex items-center justify-between text-[10px] mb-1">
        <span className="uppercase tracking-wider text-text-faint">
          SSE markers
        </span>
        <span className="text-text-faint">request axis</span>
      </div>
      <div className="relative w-full" style={{ height: `${laneHeight}px` }}>
        <div
          className="absolute inset-x-0 h-px bg-[color:var(--color-text-faint)]/40"
          style={{ top: `${axisTop}px` }}
        />
        {staggered.map((m) => (
          <MarkerDot
            key={m.key}
            marker={m}
            total={total}
            active={active}
            axisTop={axisTop}
            dotTop={dotY(m.track)}
          />
        ))}
      </div>
      <TimeAxisTicks totalMs={total} tsMs={event.ts_ms} />
      <StreamCounters event={event} />
    </div>
  );
}

function StreamCounters({ event }: { event: RequestEventWithPhase }) {
  return (
    <div className="mt-2 flex flex-wrap gap-x-3 gap-y-0.5 text-[10px] text-text-muted">
      <CounterPill
        label="SSE"
        value={event._phase === 'final' ? event.sse_event_count : undefined}
        description="Total parsed SSE frames — every `event:` line the proxy relayed (message_start, content_block_start, content_block_delta, ping, message_stop, etc. combined)."
      />
      <CounterPill
        label="deltas"
        value={event._phase === 'final' ? event.content_delta_count : undefined}
        description="Number of `event: content_block_delta` frames — approximates the streamed token chunk count."
      />
      <CounterPill
        label="pings"
        value={event._phase === 'final' ? event.ping_count : undefined}
        description="Number of `event: ping` keepalive frames Anthropic sent to keep the SSE connection alive during long turns."
      />
      <CounterPill
        label="chunks"
        value={event._phase === 'final' ? event.body_chunk_count : undefined}
        description="Count of raw HTTP body chunks relayed downstream. Non-stream: response body chunk count. Stream: low-level stream frame count from upstream."
      />
      <CounterPill
        label="avg gap"
        value={
          event._phase === 'final' && event.inter_token_avg_ms != null
            ? fmtMs(event.inter_token_avg_ms)
            : undefined
        }
        description="Average time between consecutive content_block_delta events: (last_content_delta − first_content_delta) / (delta_count − 1). Approximates per-token pacing."
      />
    </div>
  );
}

function CounterPill({
  label,
  value,
  description,
}: {
  label: string;
  value: number | string | null | undefined;
  description: string;
}) {
  if (value == null || value === '') return null;
  const rendered = typeof value === 'number' ? fmtN(value) : value;
  return (
    <InfoPopover
      content={
        <div className="flex flex-col gap-1">
          <div className="text-text font-medium">{label}</div>
          <div className="text-text-muted leading-snug">{description}</div>
        </div>
      }
    >
      <span className="cursor-help border-b border-dashed border-text-faint/60">
        <span className="text-text-faint">{label}: </span>
        <span className="text-text tabular-nums">{rendered}</span>
      </span>
    </InfoPopover>
  );
}

function CollapsibleSummary({
  suggested,
  children,
}: {
  suggested: boolean;
  children: ReactNode;
}) {
  return (
    <summary
      className={cx(
        'text-[10px] uppercase tracking-wider cursor-pointer select-none flex items-center gap-2 px-1.5 py-1 rounded transition',
        suggested
          ? 'bg-[color:var(--color-warn)]/10 ring-1 ring-[color:var(--color-warn)]/40 text-[color:var(--color-warn)]'
          : 'text-text-faint',
      )}
    >
      {children}
      {suggested ? (
        <span className="ml-auto normal-case tracking-normal text-[10px] text-[color:var(--color-warn)]/90">
          ← click to expand
        </span>
      ) : null}
    </summary>
  );
}

function StageDetailsList({
  stages,
  total,
  unaccounted,
  active,
}: {
  stages: StageDetail[];
  total: number;
  unaccounted: number;
  active: ActiveKeyApi;
}) {
  const [open, setOpen] = useState(false);
  const count = stages.length + (unaccounted > 0 ? 1 : 0);
  const activeKey = active.activeKey;
  const isStageActive =
    activeKey != null &&
    (stages.some((s) => s.key === activeKey) || activeKey === 'unaccounted');
  const suggested = isStageActive && !open;

  return (
    <details
      open={open}
      onToggle={(e) => setOpen((e.currentTarget as HTMLDetailsElement).open)}
      className={cx(
        'border-t border-subtle pt-1.5 transition',
        suggested ? 'rounded' : '',
      )}
    >
      <CollapsibleSummary suggested={suggested}>
        Stage details ({count})
      </CollapsibleSummary>
      <div className="mt-2 space-y-0.5">
        {stages.map((s) => (
          <DetailRow
            key={s.key}
            stateKey={s.key}
            active={active}
            leading={
              <span
                aria-hidden
                className="inline-block h-2 w-2 rounded-sm shrink-0"
                style={{ backgroundColor: s.fill }}
              />
            }
            label={s.label}
            ms={s.ms}
            setupTiming={s.setupTiming}
            total={total}
            hint={STAGE_CATEGORY_META[s.category].label}
            description={STAGE_DESCRIPTIONS[s.key]}
            detail={s.detail}
          />
        ))}
        {unaccounted > 0 && (
          <DetailRow
            stateKey="unaccounted"
            active={active}
            leading={
              <span
                aria-hidden
                className={cx(
                  UNACCOUNTED_BG,
                  'inline-block h-2 w-2 rounded-sm shrink-0',
                )}
              />
            }
            label="Unaccounted"
            ms={unaccounted}
            total={total}
            description="Residual of duration_ms after subtracting every instrumented stage. Elevated values point at un-instrumented gaps in the proxy pipeline."
          />
        )}
      </div>
    </details>
  );
}

function SseDetailsList({
  markers,
  active,
}: {
  markers: SseMarker[];
  active: ActiveKeyApi;
}) {
  const [open, setOpen] = useState(false);
  const activeKey = active.activeKey;
  const isMarkerActive =
    activeKey != null && markers.some((m) => m.key === activeKey);
  const suggested = isMarkerActive && !open;

  return (
    <details
      open={open}
      onToggle={(e) => setOpen((e.currentTarget as HTMLDetailsElement).open)}
      className={cx(
        'border-t border-subtle pt-1.5 transition',
        suggested ? 'rounded' : '',
      )}
    >
      <CollapsibleSummary suggested={suggested}>
        SSE marker details ({markers.length})
      </CollapsibleSummary>
      <div className="mt-2 space-y-0.5">
        {markers.map((m) => {
          const isActive = active.isActive(m.key);
          const isSticky = active.isSticky(m.key);
          const description = MARKER_DESCRIPTIONS[m.key];
          const button = (
            <button
              type="button"
              {...active.bind(m.key)}
              aria-pressed={isSticky}
              className={cx(
                'flex items-center gap-2 text-[10px] w-full px-1.5 py-1 rounded transition text-left cursor-pointer',
                isActive
                  ? 'bg-overlay-10 text-text'
                  : 'text-text-muted hover:bg-overlay-5',
                isSticky
                  ? 'ring-2 ring-inset ring-[color:var(--color-accent)]/70'
                  : isActive
                    ? 'ring-1 ring-inset ring-[color:var(--color-accent)]/45'
                    : '',
              )}
            >
              <span className={cx(m.color, 'shrink-0 w-3 text-center')}>
                {m.starred ? '★' : '▲'}
              </span>
              <span className="flex-1 truncate">{m.label}</span>
              <span className="tabular-nums shrink-0 w-16 text-right">
                {fmtMs(m.absMs)}
              </span>
              <span className="tabular-nums shrink-0 w-20 text-right text-text-faint">
                +{fmtMs(m.relayMs)}
              </span>
            </button>
          );
          if (!description) return <div key={m.key}>{button}</div>;
          return (
            <InfoPopover
              key={m.key}
              side="left"
              content={<MarkerInfo marker={m} />}
            >
              {button}
            </InfoPopover>
          );
        })}
      </div>
    </details>
  );
}

function DetailRow({
  stateKey,
  active,
  leading,
  label,
  ms,
  total,
  hint,
  detail,
  description,
  setupTiming,
}: {
  stateKey: string;
  active: ActiveKeyApi;
  leading: ReactNode;
  label: string;
  ms: number;
  total: number;
  hint?: string;
  detail?: string;
  description?: string;
  setupTiming?: boolean;
}) {
  const isActive = active.isActive(stateKey);
  const isSticky = active.isSticky(stateKey);
  const button = (
    <button
      type="button"
      {...active.bind(stateKey)}
      aria-pressed={isSticky}
      className={cx(
        'grid w-full min-w-0 grid-cols-[minmax(0,1fr)_auto] gap-x-2 gap-y-0.5 rounded px-1.5 py-1 text-left text-[10px] transition cursor-pointer',
        isActive
          ? 'bg-overlay-10 text-text'
          : 'text-text-muted hover:bg-overlay-5',
        isSticky
          ? 'ring-2 ring-inset ring-[color:var(--color-accent)]/70'
          : isActive
            ? 'ring-1 ring-inset ring-[color:var(--color-accent)]/45'
            : '',
      )}
    >
      <span className="flex min-w-0 flex-wrap items-center gap-x-2 gap-y-0.5">
        {leading}
        <span className="min-w-0 break-words">{label}</span>
        {hint ? (
          <span className="break-words text-text-faint">· {hint}</span>
        ) : null}
      </span>
      <span className="whitespace-nowrap tabular-nums text-right">
        {setupTiming ? fmtSetupMs(ms) : fmtMs(ms)} · {pct(ms, total)}%
      </span>
      {detail ? (
        <span className="col-span-2 min-w-0 break-words pl-4 text-text-faint">
          {detail}
        </span>
      ) : null}
    </button>
  );
  if (!description) return button;
  return (
    <InfoPopover
      side="left"
      content={
        <div className="flex flex-col gap-1 min-w-[220px]">
          <div className="flex items-center gap-1.5">
            <span className="text-text font-medium">{label}</span>
            {hint ? (
              <span className="text-text-faint text-[10px]">· {hint}</span>
            ) : null}
          </div>
          <div className="flex items-center gap-2 tabular-nums text-text-muted">
            <span>{setupTiming ? fmtSetupMs(ms) : fmtMs(ms)}</span>
            <span className="text-text-faint">·</span>
            <span>{pct(ms, total)}% of timeline</span>
          </div>
          {detail ? (
            <div className="text-text-muted leading-snug">{detail}</div>
          ) : null}
          <div className="text-text-faint leading-snug">{description}</div>
        </div>
      }
    >
      {button}
    </InfoPopover>
  );
}

// -----------------------------------------------------------------------------
// Attribution summary and I/O diagnostics
// -----------------------------------------------------------------------------

function CategoryIcon({
  category,
  className,
}: {
  category: LatencyCategory;
  className?: string;
}) {
  const props = { 'aria-hidden': true, className: cx('h-4 w-4', className) };
  switch (category) {
    case 'downstream_network':
      return <ArrowDownToLine {...props} />;
    case 'cc_lb':
      return <Cpu {...props} />;
    case 'upstream_network':
      return <Network {...props} />;
    case 'upstream_processing':
      return <Server {...props} />;
  }
}

function CategoryCard({
  value,
  total,
}: {
  value: LatencyCategoryValue;
  total: number;
}) {
  const meta = LATENCY_CATEGORY_META[value.key];
  const measured = value.ms != null;
  const measuredPct = measured ? pct(value.ms ?? 0, total) : null;
  const status =
    value.key === 'upstream_processing' && !measured
      ? 'Not independently measured'
      : measured
        ? `${measuredPct}% of request`
        : 'Not measured';

  return (
    <article
      role="listitem"
      data-testid={`latency-category-${value.key}`}
      data-category-ms={value.ms ?? 'unmeasured'}
      className={cx(
        'min-w-0 rounded-lg border p-3',
        'flex flex-col gap-2 overflow-hidden',
        meta.surfaceClass,
      )}
      aria-label={`${meta.label}: ${measured ? fmtMs(value.ms ?? 0) : status}`}
    >
      <div className="flex min-w-0 items-start gap-2">
        <span
          className={cx(
            'flex h-7 w-7 shrink-0 items-center justify-center rounded-md border border-current/20 bg-overlay-5',
            meta.textClass,
          )}
        >
          <CategoryIcon category={value.key} />
        </span>
        <div className="min-w-0 flex-1">
          <h3
            className={cx(
              'break-words text-[11px] font-semibold leading-tight',
              meta.textClass,
            )}
          >
            {meta.label}
          </h3>
          <div className="mt-1 text-lg font-semibold tabular-nums text-text">
            {measured ? fmtMs(value.ms ?? 0) : '—'}
          </div>
        </div>
      </div>
      <div
        className="h-1.5 overflow-hidden rounded-full bg-overlay-10"
        aria-hidden
      >
        {measured ? (
          <div
            className="h-full rounded-full"
            style={{
              backgroundColor: meta.fill,
              width: `${clampPct(((value.ms ?? 0) / Math.max(total, 1)) * 100)}%`,
            }}
          />
        ) : null}
      </div>
      <div className={cx('text-[10px] font-medium', meta.textClass)}>
        {status}
      </div>
      <p className="break-words text-[10px] leading-snug text-text-muted">
        {value.note}
      </p>
    </article>
  );
}

function LatencyAttributionSummary({
  attribution,
}: {
  attribution: LatencyAttribution;
}) {
  return (
    <section aria-labelledby="latency-attribution-title" className="space-y-2">
      <div className="flex flex-wrap items-end justify-between gap-1">
        <div>
          <h2
            id="latency-attribution-title"
            className="text-xs font-semibold text-text"
          >
            Measured latency attribution
          </h2>
          <p className="text-[10px] leading-snug text-text-faint">
            Measured portions only. Shared waits and unknown time are excluded
            from the four cards.
          </p>
        </div>
        <span className="text-[10px] tabular-nums text-text-muted">
          request {fmtMs(attribution.timelineTotalMs)}
        </span>
      </div>
      <div
        role="list"
        aria-label="Latency category summary"
        className="grid min-w-0 grid-cols-1 gap-2 min-[420px]:grid-cols-2"
      >
        {attribution.categories.map((value) => (
          <CategoryCard
            key={value.key}
            value={value}
            total={attribution.timelineTotalMs}
          />
        ))}
      </div>
    </section>
  );
}

function MixedAttributionPanel({
  attribution,
}: {
  attribution: LatencyAttribution;
}) {
  const mixed = attribution.mixedUpstreamMs;
  const retry = attribution.mixedRetryMs;
  return (
    <section
      data-testid="latency-mixed-upstream"
      data-mixed-upstream-ms={mixed ?? 'unmeasured'}
      aria-labelledby="latency-mixed-upstream-title"
      className="min-w-0 overflow-hidden rounded-lg border border-amber-500/35 bg-amber-500/5"
    >
      <div className="flex min-w-0 items-start gap-2 p-3">
        <span className="flex h-7 w-7 shrink-0 items-center justify-center rounded-md border border-amber-500/30 bg-amber-500/10 text-[color:var(--latency-upstream-text)]">
          <Split aria-hidden className="h-4 w-4" />
        </span>
        <div className="min-w-0 flex-1">
          <div className="flex flex-wrap items-baseline justify-between gap-x-3 gap-y-1">
            <h2
              id="latency-mixed-upstream-title"
              className="text-xs font-semibold text-text"
            >
              Combined upstream wait
            </h2>
            <span className="text-sm font-semibold tabular-nums text-text">
              {mixed == null ? 'Not measured' : fmtMs(mixed)}
            </span>
          </div>
          <p className="mt-1 break-words text-[10px] leading-snug text-text-muted">
            Header and response-frame waits combine upstream network, provider
            work, and runtime scheduling. They span both upstream concepts,
            belong to neither card alone, and are counted once.
          </p>
        </div>
      </div>
      <div
        className="mx-3 mb-3 h-3 overflow-hidden rounded-sm border border-amber-500/25 bg-overlay-5"
        aria-hidden
      >
        {mixed != null ? (
          <div
            className="h-full min-w-px"
            style={{
              width: `${clampPct(
                (mixed / Math.max(attribution.timelineTotalMs, 1)) * 100,
              )}%`,
              backgroundImage:
                'repeating-linear-gradient(135deg, rgba(245,158,11,.78) 0 7px, rgba(16,185,129,.78) 7px 14px)',
            }}
          />
        ) : null}
      </div>
      {retry != null ? (
        <div
          data-testid="latency-mixed-retry"
          data-mixed-retry-ms={retry}
          className="flex min-w-0 flex-wrap items-center gap-x-2 gap-y-1 border-t border-amber-500/20 px-3 py-2 text-[10px]"
        >
          <RefreshCw
            aria-hidden
            className="h-3.5 w-3.5 shrink-0 text-[color:var(--latency-retry-text)]"
          />
          <span className="font-medium text-text">Earlier retry path</span>
          <span className="tabular-nums text-text">{fmtMs(retry)}</span>
          <span className="min-w-0 text-text-muted">
            mixed cc-lb + upstream before the final attempt; counted once
          </span>
        </div>
      ) : null}
      {attribution.unattributedMs > 0 ? (
        <div className="flex min-w-0 flex-wrap items-center gap-x-2 gap-y-1 border-t border-subtle px-3 py-2 text-[10px]">
          <span
            aria-hidden
            className={cx(UNACCOUNTED_BG, 'h-2.5 w-2.5 shrink-0 rounded-sm')}
          />
          <span className="font-medium text-text">Unattributed remainder</span>
          <span className="tabular-nums text-text">
            {fmtMs(attribution.unattributedMs)}
          </span>
          <span className="min-w-0 text-text-muted">
            includes legacy unsplit parents and other uninstrumented gaps
          </span>
        </div>
      ) : null}
      {attribution.accountingWarning ? (
        <div
          role="alert"
          className="flex items-start gap-2 border-t border-[color:var(--color-warn)]/30 bg-[color:var(--color-warn)]/10 px-3 py-2 text-[10px] text-[color:var(--color-warn)]"
        >
          <TriangleAlert aria-hidden className="mt-0.5 h-3.5 w-3.5 shrink-0" />
          <span>
            Recorded child intervals exceed a parent or the request total.
            Values remain visible; visual widths are clamped.
          </span>
        </div>
      ) : null}
    </section>
  );
}

type DiagnosticFormat = 'ms' | 'count' | 'bytes';

interface DiagnosticItem {
  key: string;
  label: string;
  value: number | null | undefined;
  format: DiagnosticFormat;
  category: StageCategory;
  detail: string;
}

function formatDiagnosticValue(
  value: number,
  format: DiagnosticFormat,
): string {
  if (format === 'count') return fmtN(value);
  if (format === 'bytes') return fmtBytes(value);
  return fmtIoMs(value);
}

function DiagnosticList({
  title,
  items,
  isPartial,
}: {
  title: string;
  items: DiagnosticItem[];
  isPartial: boolean;
}) {
  return (
    <section className="min-w-0 rounded-lg border border-subtle bg-overlay-2 p-2.5">
      <h3 className="mb-1.5 text-[11px] font-semibold text-text">{title}</h3>
      <div role="list" className="space-y-1">
        {items.map((item) => {
          const measured = item.value != null;
          const category = STAGE_CATEGORY_META[item.category];
          return (
            <div
              key={item.key}
              role="listitem"
              data-testid={`latency-diagnostic-${item.key}`}
              data-measured={measured}
              className="grid min-w-0 grid-cols-[minmax(0,1fr)_auto] gap-x-2 rounded-md px-1.5 py-1 hover:bg-overlay-5"
            >
              <div className="min-w-0">
                <div className="flex min-w-0 flex-wrap items-baseline gap-x-1.5">
                  <span className="font-medium text-[10px] text-text">
                    {item.label}
                  </span>
                  <span
                    className={cx('break-words text-[9px]', category.textClass)}
                  >
                    {category.label}
                  </span>
                </div>
                <p className="break-words text-[9px] leading-snug text-text-faint">
                  {item.detail}
                </p>
              </div>
              <span className="self-start whitespace-nowrap text-[10px] tabular-nums text-text-muted">
                {measured
                  ? formatDiagnosticValue(item.value ?? 0, item.format)
                  : isPartial
                    ? 'In progress / not measured'
                    : 'Not measured'}
              </span>
            </div>
          );
        })}
      </div>
    </section>
  );
}

function IoDiagnostics({ event }: { event: RequestEventWithPhase }) {
  if (event.source_kind === 'renewal') return null;
  const responseParent =
    event._phase === 'final'
      ? event.status === 499
        ? event.upstream_body_ms
        : (event.stream_total_ms ?? event.upstream_body_ms)
      : undefined;
  const hasAny =
    event.request_body_read_ms != null ||
    event.request_body_bytes != null ||
    event.request_body_first_chunk_ms != null ||
    event.request_body_receive_ms != null ||
    event.request_body_wait_ms != null ||
    event.request_body_process_ms != null ||
    event.request_body_chunk_count != null ||
    responseParent != null ||
    event.response_body_wait_ms != null ||
    event.response_body_process_ms != null ||
    event.response_body_downstream_poll_gap_ms != null;
  if (!hasAny) return null;

  const ingress: DiagnosticItem[] = [
    {
      key: 'request-parent',
      label: 'Request body parent',
      value: event.request_body_read_ms,
      format: 'ms',
      category: 'parent_interval',
      detail: 'Collection start to completion; children below stay inside it.',
    },
    {
      key: 'request-first-chunk',
      label: 'First non-empty chunk',
      value: event.request_body_first_chunk_ms,
      format: 'ms',
      category: 'parent_interval',
      detail: 'Overlapping marker from collection start; diagnostic only.',
    },
    {
      key: 'request-receive',
      label: 'Receive interval',
      value: event.request_body_receive_ms,
      format: 'ms',
      category: 'parent_interval',
      detail: 'Overlapping first-to-last collection interval; diagnostic only.',
    },
    {
      key: 'request-wait',
      label: 'Observed frame wait',
      value: event.request_body_wait_ms,
      format: 'ms',
      category: 'downstream_network',
      detail: 'Client, transit, and runtime scheduling wait; not RTT.',
    },
    {
      key: 'request-process',
      label: 'Local frame handling',
      value: event.request_body_process_ms,
      format: 'ms',
      category: 'cc_lb',
      detail: 'Elapsed synchronous copy and validation work; not CPU time.',
    },
    {
      key: 'request-chunks',
      label: 'Non-empty DATA frames',
      value: event.request_body_chunk_count,
      format: 'count',
      category: 'parent_interval',
      detail: 'Zero is distinct from a missing measurement.',
    },
    {
      key: 'request-bytes',
      label: 'Request body size',
      value: event.request_body_bytes,
      format: 'bytes',
      category: 'parent_interval',
      detail:
        'Bytes collected by the handler; pre-handler upload is not visible.',
    },
  ];
  const response: DiagnosticItem[] = [
    {
      key: 'response-parent',
      label:
        event.status === 499
          ? 'Partial response parent'
          : 'Response body parent',
      value: responseParent,
      format: 'ms',
      category: 'parent_interval',
      detail:
        'Parent interval; split children and any remainder are not added again.',
    },
    {
      key: 'response-wait',
      label: 'Upstream frame wait',
      value: event.response_body_wait_ms,
      format: 'ms',
      category: 'mixed_upstream',
      detail: 'Provider generation, transit, and runtime scheduling combined.',
    },
    {
      key: 'response-process',
      label: 'Local relay work',
      value: event.response_body_process_ms,
      format: 'ms',
      category: 'cc_lb',
      detail: 'Measured local work already contained in the response parent.',
    },
    {
      key: 'response-poll-gap',
      label: 'Downstream consumer poll gaps',
      value: event.response_body_downstream_poll_gap_ms,
      format: 'ms',
      category: 'downstream_network',
      detail:
        'Consumer, backpressure, and scheduling observation; not ACK or RTT.',
    },
    {
      key: 'response-chunks',
      label: 'Raw HTTP body chunks',
      value: event._phase === 'final' ? event.body_chunk_count : undefined,
      format: 'count',
      category: 'parent_interval',
      detail: 'Low-level response frames relayed or collected.',
    },
  ];

  return (
    <section aria-labelledby="latency-io-title" className="space-y-2">
      <div>
        <h2 id="latency-io-title" className="text-xs font-semibold text-text">
          Request and response I/O diagnostics
        </h2>
        <p className="text-[10px] leading-snug text-text-faint">
          Parent intervals preserve chronology. Overlapping markers and split
          children are shown for diagnosis, not summed beside their parents.
        </p>
      </div>
      <div className="grid min-w-0 grid-cols-1 gap-2 lg:grid-cols-2">
        <DiagnosticList
          title="Request ingress"
          items={ingress}
          isPartial={event._phase === 'partial'}
        />
        <DiagnosticList
          title="Response relay"
          items={response}
          isPartial={event._phase === 'partial'}
        />
      </div>
    </section>
  );
}

function RenewalSummary({ event }: { event: RequestEventWithPhase }) {
  const duration =
    event._phase === 'final' && event.duration_ms != null
      ? event.duration_ms
      : event._phase === 'partial'
        ? event.elapsed_ms
        : null;
  return (
    <section className="rounded-lg border border-cyan-500/25 bg-cyan-500/10 p-3">
      <div className="flex items-start gap-2">
        <span className="flex h-7 w-7 shrink-0 items-center justify-center rounded-md border border-cyan-500/25 text-[color:var(--latency-downstream-text)]">
          <RefreshCw aria-hidden className="h-4 w-4" />
        </span>
        <div className="min-w-0 flex-1">
          <div className="flex flex-wrap items-baseline justify-between gap-2">
            <h2 className="text-xs font-semibold text-text">
              Scheduler renewal cycle
            </h2>
            <span className="text-sm font-semibold tabular-nums text-text">
              {duration == null
                ? event._phase === 'partial'
                  ? 'In progress'
                  : 'Not measured'
                : fmtMs(duration)}
            </span>
          </div>
          <p className="mt-1 text-[10px] leading-snug text-text-muted">
            This lifecycle work is not a proxy request and is not assigned to
            downstream, cc-lb, or upstream latency categories.
          </p>
        </div>
      </div>
    </section>
  );
}

// -----------------------------------------------------------------------------
// Utils
// -----------------------------------------------------------------------------

function pct(v: number, total: number): number {
  if (total <= 0) return 0;
  return Math.round((v / total) * 100);
}

function clampPct(v: number): number {
  if (!Number.isFinite(v)) return 0;
  return Math.min(100, Math.max(0, v));
}

// -----------------------------------------------------------------------------
// Entry component
// -----------------------------------------------------------------------------

const LATENCY_LAYOUT_CLASS = 'space-y-3 min-h-80';

export function LatencyTimeline({
  event,
  isPartial,
  isLoading = false,
}: {
  event: RequestEventWithPhase;
  isPartial?: boolean;
  isLoading?: boolean;
}) {
  const total = deriveProxyTimelineDuration(event);
  const attribution = computeLatencyAttribution(event);
  const stages = useMemo(() => buildStageDetails(event), [event]);
  const groups = computeStageGroups(event);
  const markers = useMemo(() => buildSseMarkers(event), [event]);
  const hasFinalizeTiming = event.finalize_ms != null;
  const isRenewal = event.source_kind === 'renewal';
  const isFinalRenewal = event._phase === 'final' && isRenewal;
  const groupOrder = isFinalRenewal ? RENEWAL_GROUP_ORDER : PROXY_GROUP_ORDER;
  const hasMeasuredRenewalCycle =
    isFinalRenewal && event.duration_ms != null && event.duration_ms >= 0;
  const positioned = useMemo(() => {
    let cursor = 0;
    const out = stages.map((s) => {
      const startMs = cursor;
      cursor += s.ms;
      return { ...s, startMs };
    });
    return { positioned: out, unaccountedStartMs: cursor };
  }, [stages]);
  const active = useActiveKey();
  if (isLoading) {
    return (
      <div
        role="status"
        aria-busy="true"
        aria-label="Loading latency detail"
        className={LATENCY_LAYOUT_CLASS}
        data-testid="latency-timeline-region"
      >
        <div
          className="grid grid-cols-1 gap-2 min-[420px]:grid-cols-2"
          aria-hidden="true"
        >
          {(isRenewal ? [0] : [0, 1, 2, 3]).map((key) => (
            <Skeleton key={key} className="h-32 w-full rounded-lg" />
          ))}
        </div>
        {!isRenewal ? (
          <Skeleton aria-hidden="true" className="h-28 w-full rounded-lg" />
        ) : null}
        <div className="space-y-2" aria-hidden="true">
          {groupOrder.map((group) => (
            <div key={group}>
              <div className="mb-0.5 flex items-center justify-between">
                <Skeleton className="h-2.5 w-24" />
                <Skeleton className="h-2.5 w-20" />
              </div>
              <Skeleton className="h-4 w-full" />
            </div>
          ))}
        </div>
      </div>
    );
  }

  const hasMeasuredAttribution =
    attribution.categories.some((category) => category.ms != null) ||
    attribution.mixedUpstreamMs != null ||
    attribution.mixedRetryMs != null;
  const noRecordedTimeline =
    total <= 0 &&
    !hasMeasuredRenewalCycle &&
    stages.length === 0 &&
    !attribution.hasIoTimings &&
    !hasMeasuredAttribution;
  if (noRecordedTimeline) {
    return (
      <div
        className={LATENCY_LAYOUT_CLASS}
        data-testid="latency-timeline-region"
        data-accounted-ms={attribution.accountedMs}
      >
        {isRenewal ? (
          <RenewalSummary event={event} />
        ) : (
          <>
            <LatencyAttributionSummary attribution={attribution} />
            <MixedAttributionPanel attribution={attribution} />
          </>
        )}
        <div className="rounded-lg border border-dashed border-subtle p-3 text-xs text-text-faint">
          No latency data recorded.
        </div>
      </div>
    );
  }

  const unaccounted = groups.unaccounted;
  const showStreamLane = markers.length > 0 && !isPartial;

  return (
    <div
      className={LATENCY_LAYOUT_CLASS}
      data-testid="latency-timeline-region"
      data-accounted-ms={attribution.accountedMs}
      data-chronology-accounted-ms={groups.accounted}
      data-raw-residual-ms={groups.rawResidual}
      data-accounting-warning={attribution.accountingWarning}
    >
      {isRenewal ? (
        <RenewalSummary event={event} />
      ) : (
        <>
          <LatencyAttributionSummary attribution={attribution} />
          <MixedAttributionPanel attribution={attribution} />
          <IoDiagnostics event={event} />
        </>
      )}
      <section aria-labelledby="latency-chronology-title" className="space-y-2">
        <div>
          <h2
            id="latency-chronology-title"
            className="text-xs font-semibold text-text"
          >
            Chronological request path
          </h2>
          <p className="text-[10px] leading-snug text-text-faint">
            Parent stages stay on the request axis. Diagnostic children above do
            not move bars or SSE markers. The axis residual excludes known
            parents; the attribution remainder can include parents that cannot
            be assigned to a physical category.
          </p>
        </div>
        <div className="space-y-2">
          {groupOrder.map((g) => {
            const items = positioned.positioned.filter((p) => p.group === g);
            if (items.length === 0) return null;
            const groupSum = items.reduce((a, s) => a + s.ms, 0);
            const groupTotal =
              g === 'internal_pre' && event._phase === 'final'
                ? groups.internalPre
                : groupSum;
            const interactiveItems = items.filter(
              (item) =>
                item.ms > 0 && (!item.setupTiming || item.ms / total >= 0.001),
            );
            const anyActive = items.some((it) => active.isActive(it.key));
            const groupItems = stages.filter((s) => s.group === g);
            const groupLabel = isRenewal
              ? 'Scheduler renewal work'
              : getGroupLabel(g, hasFinalizeTiming);
            return (
              <div key={g}>
                <div className="mb-0.5 flex min-w-0 flex-wrap items-center justify-between gap-x-2 gap-y-0.5 text-[10px]">
                  <span
                    className={cx(
                      'min-w-0 break-words',
                      GROUP_META[g].text,
                      anyActive ? 'font-medium' : '',
                    )}
                  >
                    {groupLabel}
                  </span>
                  <span className="shrink-0 whitespace-nowrap tabular-nums text-text-muted">
                    {fmtMs(groupTotal)} · {pct(groupTotal, total)}%
                  </span>
                </div>
                <div className="relative h-4 w-full rounded-sm bg-overlay-5">
                  {interactiveItems.map((it) => (
                    <SegmentButton
                      key={it.key}
                      stage={it}
                      startMs={it.startMs}
                      total={total}
                      active={active}
                      overview={groupItems}
                      groupLabel={groupLabel}
                    />
                  ))}
                </div>
              </div>
            );
          })}
          {unaccounted > 0 && (
            <UnaccountedRow
              ms={unaccounted}
              startMs={positioned.unaccountedStartMs}
              total={total}
              active={active}
            />
          )}
        </div>
      </section>

      {showStreamLane && (
        <SseLane
          markers={markers}
          total={total}
          active={active}
          event={event}
        />
      )}

      <StageDetailsList
        stages={stages}
        total={total}
        unaccounted={unaccounted}
        active={active}
      />
      {showStreamLane && <SseDetailsList markers={markers} active={active} />}
    </div>
  );
}
