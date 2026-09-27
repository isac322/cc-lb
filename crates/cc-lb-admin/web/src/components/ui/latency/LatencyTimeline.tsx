import { Popover as BasePopover } from '@base-ui/react/popover';
import { ChevronRight, Star, Triangle } from 'lucide-react';
import {
  type ReactElement,
  type ReactNode,
  useCallback,
  useMemo,
  useState,
} from 'react';
import { fmtBytes, fmtMs, fmtN, fmtSetupMs } from '../../../lib/format';
import type { RequestEventWithPhase } from '../../../lib/RequestEventTypes';
import { cx, Skeleton } from '../primitives';
import {
  CACHE_SETUP_TIMING_STAGES,
  computeLatencyAttribution,
  computeStageGroups,
  deriveOtherSetup,
  deriveSetupOverhead,
  hasSetupTimingBreakdown,
  type LatencyAttribution,
  latencyResponsibilityDescription,
  responseBodyDuration,
} from './computeStageGroups';

// -----------------------------------------------------------------------------
// Group / stage taxonomy
// -----------------------------------------------------------------------------

type StageGroup =
  | 'renewal'
  | 'internal_pre'
  | 'wait'
  | 'upstream'
  | 'body'
  | 'internal_post';

const GROUP_META: Record<
  StageGroup,
  { label: string; hue: number; chroma: number }
> = {
  // OKLCH hues from the series family (sky, teal, green, magenta, neutral).
  // Across the whole lightness ramp every shade stays more than OKLab ΔE 0.1
  // from the accent, warn and danger tokens: brand and severity keep their
  // meaning inside the timeline.
  renewal: { label: 'Renewal', hue: 235, chroma: 0.1 },
  internal_pre: { label: 'Internal pre', hue: 235, chroma: 0.1 },
  wait: { label: 'Wait', hue: 190, chroma: 0.09 },
  upstream: { label: 'Upstream', hue: 150, chroma: 0.11 },
  body: { label: 'Body', hue: 335, chroma: 0.11 },
  internal_post: { label: 'Internal post', hue: 260, chroma: 0.012 },
};

function getGroupLabel(group: StageGroup, hasFinalizeTiming: boolean): string {
  return group === 'internal_post' && hasFinalizeTiming
    ? 'Finalize'
    : GROUP_META[group].label;
}

const PROXY_GROUP_ORDER: StageGroup[] = [
  'internal_pre',
  'wait',
  'upstream',
  'body',
  'internal_post',
];
const RENEWAL_GROUP_ORDER: StageGroup[] = ['renewal'];

const UNACCOUNTED_BG =
  'bg-overlay-6 bg-[repeating-linear-gradient(45deg,_transparent_0_4px,_var(--color-border-strong)_4px_8px)]';

const STAGE_DESCRIPTIONS: Record<string, string> = {
  request_body_read:
    'Time for the server to receive the complete client request body. JSON parsing starts afterward and is excluded.',
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
  shape:
    'Run the shape plugin: dialect adaptation + Anthropic-format shaping of the outbound body.',
  sign: 'Sign the outbound request (OAuth refresh if the credential needs one).',
  bulkhead_wait:
    'Time spent queued in the bulkhead waiting for an upstream concurrency slot.',
  dns: 'DNS resolution for the upstream host (skipped when the connection is reused).',
  connect:
    'TCP connect + TLS handshake. Skipped entirely when the warm pool returns a live connection.',
  upstream_wait:
    'upstream_ttfb − (bulkhead_wait + dns + connect). Approximates provider-side processing + network RTT.',
  stream_relay:
    'Complete SSE response body relay from response headers through the last downstream chunk.',
  partial_stream:
    'Partial response body relay from response headers until client cancellation was observed.',
  body_collect:
    'Non-stream response body download from response headers until the complete body was collected.',
  finalize:
    'Mandatory accounting and finalization after the response body completed or cancellation was observed, ending immediately before the terminal event is published.',
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

interface StageDetail extends Stage {
  index: number;
  groupCount: number;
  fill: string;
}

/** Night ramps 0.58→0.86 lightness, day 0.40→0.72: each shade holds its contrast on the ground. */
function stageShade(group: StageGroup, index: number, count: number): string {
  const { hue, chroma } = GROUP_META[group];
  const t = count <= 1 ? 0.5 : index / (count - 1);
  const night = (0.58 + t * 0.28).toFixed(3);
  const day = (0.4 + t * 0.32).toFixed(3);
  return `light-dark(oklch(${day} ${chroma} ${hue}), oklch(${night} ${chroma} ${hue}))`;
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

  const perGroupCount = new Map<StageGroup, number>();
  for (const s of raw) {
    perGroupCount.set(s.group, (perGroupCount.get(s.group) ?? 0) + 1);
  }
  const seen = new Map<StageGroup, number>();
  return raw.map((s) => {
    const groupCount = perGroupCount.get(s.group) ?? 1;
    const index = seen.get(s.group) ?? 0;
    seen.set(s.group, index + 1);
    return {
      ...s,
      index,
      groupCount,
      fill: stageShade(s.group, index, groupCount),
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
      color: 'text-text-muted',
    },
    {
      key: 'content_block_start',
      label: 'Content block start',
      ms: e._phase === 'final' ? e.stream_content_block_start_ms : undefined,
      color: 'text-text-muted',
    },
    {
      key: 'first_delta',
      label: 'First content delta (TTFT)',
      ms: e._phase === 'final' ? e.stream_first_content_delta_ms : undefined,
      color: 'text-text',
      starred: true,
    },
    {
      key: 'last_delta',
      label: 'Last content delta',
      ms: e._phase === 'final' ? e.stream_last_content_delta_ms : undefined,
      color: 'text-text-muted',
    },
    {
      key: 'message_stop',
      label: 'Message stop',
      ms: e._phase === 'final' ? e.stream_message_stop_ms : undefined,
      color: 'text-text-muted',
    },
    {
      key: 'last_chunk',
      label: 'Last chunk',
      ms: e._phase === 'final' ? e.stream_last_chunk_ms : undefined,
      color: 'text-text-faint',
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
          <BasePopover.Popup className="z-[60] px-2.5 py-1.5 text-caption rounded-md bg-bg-sub border border-subtle-strong text-text shadow-overlay max-w-[280px]">
            {content}
          </BasePopover.Popup>
        </BasePopover.Positioner>
      </BasePopover.Portal>
    </BasePopover.Root>
  );
}

function ResponsibilityOverview({
  attribution,
  event,
}: {
  attribution: LatencyAttribution;
  event: RequestEventWithPhase;
}) {
  const groups = attribution.isFinalRenewal
    ? attribution.responsibilities
    : attribution.responsibilities.filter(
        (group) =>
          group.key !== 'unattributed' || group.observed || group.valueMs > 0,
      );
  const visibleGroups =
    event._phase === 'partial'
      ? groups.filter((group) => group.observed || group.valueMs > 0)
      : groups;
  const barGroups = visibleGroups.filter((group) => group.valueMs > 0);
  const barSum = barGroups.reduce((total, group) => total + group.valueMs, 0);
  const barTotal = Math.max(barSum, attribution.totalMs);

  return (
    <section
      aria-label="Latency by responsibility"
      className="@container space-y-1.5"
    >
      <div className="flex items-center justify-between">
        <span className="text-label text-text-muted">Responsibility</span>
        <span className="text-caption text-text-faint">Share of total</span>
      </div>
      <div
        aria-label="Responsibility distribution"
        className="flex h-2 w-full gap-px overflow-hidden rounded-xs bg-progress-track"
        role="img"
      >
        {barGroups.map((group) => (
          <span
            aria-hidden
            className={cx('h-full min-w-[2px]', group.color)}
            data-ms={group.valueMs}
            data-responsibility={group.key}
            key={group.key}
            style={{
              width: `${clampPct((group.valueMs / barTotal) * 100)}%`,
            }}
          />
        ))}
      </div>
      <div className="grid grid-cols-1 gap-1 @lg:grid-cols-2">
        {visibleGroups.map((group) => (
          <InfoPopover
            content={
              <div className="flex min-w-[220px] flex-col gap-1">
                <div className="text-text font-medium">{group.label}</div>
                <div className="text-text-muted tabular-nums">
                  {group.observed ? fmtMs(group.valueMs) : 'Not recorded'}
                  {group.observed
                    ? ` · ${pct(group.valueMs, attribution.totalMs)}%`
                    : ''}
                </div>
                <div className="text-text-faint leading-snug">
                  {latencyResponsibilityDescription(group.key, attribution)}
                </div>
              </div>
            }
            key={group.key}
          >
            <button
              aria-label={`${group.label}${
                group.key === 'upstream-net' && event.connection_reused === true
                  ? ', warm pool'
                  : ''
              }, ${group.observed ? fmtMs(group.valueMs) : 'not recorded'}, ${
                group.observed
                  ? `${pct(group.valueMs, attribution.totalMs)}% of total`
                  : 'no timing recorded'
              }`}
              className="flex min-w-0 items-center gap-2 rounded-sm px-1.5 py-1 text-left text-caption transition-colors hover:bg-overlay-5 focus-visible:outline-2 focus-visible:outline-accent"
              data-ms={group.observed ? group.valueMs : undefined}
              data-responsibility={group.key}
              type="button"
            >
              <span
                aria-hidden
                className={cx('h-2 w-2 shrink-0 rounded-xs', group.color)}
              />
              <span className="min-w-0 flex-1 truncate border-b border-dashed border-text-faint/50 text-text">
                {group.label}
                {group.key === 'upstream-net' &&
                event.connection_reused === true ? (
                  <span className="ml-1 rounded-sm bg-ok/12 px-1 py-0.5 text-caption leading-none text-success-text">
                    Warm pool
                  </span>
                ) : null}
              </span>
              <span className="shrink-0 tabular-nums text-text-muted">
                {group.observed ? fmtMs(group.valueMs) : '—'}
              </span>
              <span className="w-8 shrink-0 text-right tabular-nums text-text-faint">
                {group.observed
                  ? `${pct(group.valueMs, attribution.totalMs)}%`
                  : '—'}
              </span>
            </button>
          </InfoPopover>
        ))}
      </div>
    </section>
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
      <div className="text-label text-text-muted">
        {groupLabel}
        {overview.length > 1 ? ` · ${overview.length} stages` : ''}
      </div>
      {multi ? (
        <>
          <div className="flex h-2 gap-px rounded-xs overflow-hidden bg-progress-track">
            {overview.map((s) => {
              const isActive = s.key === stage.key;
              return (
                <div
                  key={s.key}
                  className={cx(
                    'h-full transition-all',
                    isActive
                      ? 'ring-2 ring-inset ring-text/70 z-10'
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
                    'flex items-center gap-1.5 text-caption px-1 py-0.5 rounded-sm transition',
                    isActive
                      ? 'bg-overlay-6 text-text font-medium'
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

/** Starred markers are the headline SSE moments; the rest are plain triangles. */
function MarkerIcon({ starred }: { starred?: boolean }) {
  const Icon = starred ? Star : Triangle;
  return (
    <Icon size={12} strokeWidth={1.75} fill="currentColor" aria-hidden="true" />
  );
}

function MarkerInfo({ marker }: { marker: SseMarker }) {
  const description = MARKER_DESCRIPTIONS[marker.key];
  return (
    <div className="flex flex-col gap-1 min-w-[220px]">
      <div className="flex items-center gap-1.5">
        <span
          className={cx(
            marker.color,
            'inline-flex w-3 shrink-0 items-center justify-center',
          )}
        >
          <MarkerIcon starred={marker.starred} />
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

  const style: React.CSSProperties = {
    backgroundColor: stage.fill,
    width: `${(stage.ms / total) * 100}%`,
    left: `${(startMs / total) * 100}%`,
    minWidth: '2px',
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
        className={cx(
          'absolute top-0 h-full rounded-sm outline-none transition-all cursor-pointer',
          isActive
            ? 'ring-2 ring-inset ring-text/70 z-10 brightness-110'
            : 'hover:brightness-110',
          isSticky ? 'outline outline-1 outline-border-strong' : '',
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
      <div className="flex items-center justify-between text-caption mb-0.5">
        <span
          className={cx('text-text-muted', isActive && 'text-text font-medium')}
        >
          Unaccounted
        </span>
        <span className="text-text-muted tabular-nums">
          {fmtMs(ms)} · {pct(ms, total)}%
        </span>
      </div>
      <div className="relative h-3 w-full overflow-hidden rounded-xs bg-progress-track">
        <InfoPopover content={<UnaccountedInfo ms={ms} total={total} />}>
          <button
            type="button"
            {...active.bind('unaccounted')}
            className={cx(
              UNACCOUNTED_BG,
              'absolute top-0 h-full rounded-sm outline-none transition cursor-pointer',
              isActive
                ? 'ring-2 ring-inset ring-text/70 z-10'
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
          className={cx(
            'absolute -translate-x-1/2 -translate-y-1/2 h-5 w-5 flex items-center justify-center outline-none cursor-pointer transition-transform',
            isActive ? 'scale-[1.4] z-10' : 'hover:scale-125',
            isSticky && !isActive ? 'scale-125' : '',
          )}
          style={{ left: `${leftPct}%`, top: `${dotTop}px` }}
        >
          <span aria-hidden className={cx(marker.color, 'inline-flex')}>
            <MarkerIcon starred={marker.starred} />
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
              'absolute top-0 text-caption text-text-faint tabular-nums whitespace-nowrap',
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
      <div className="flex items-center justify-between mb-1">
        <span className="text-label text-text-muted">SSE markers</span>
        <span className="text-caption text-text-faint">Request axis</span>
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
    <div className="mt-2 flex flex-wrap gap-x-3 gap-y-0.5 text-caption text-text-muted">
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
        'text-label cursor-pointer select-none flex items-center gap-2 px-1.5 py-1 rounded-sm transition [&::-webkit-details-marker]:hidden list-none',
        suggested ? 'bg-warn/8 text-warn-text' : 'text-text-muted',
      )}
    >
      <ChevronRight
        aria-hidden
        className="h-3 w-3 shrink-0 transition-transform group-open/details:rotate-90"
      />
      {children}
      {suggested ? (
        <span className="ml-auto text-caption text-warn-text">
          Click to expand
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
  hasFinalizeTiming,
}: {
  stages: StageDetail[];
  total: number;
  unaccounted: number;
  active: ActiveKeyApi;
  hasFinalizeTiming: boolean;
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
        'group/details border-t border-row pt-1.5 transition',
        suggested ? 'rounded-sm' : '',
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
            hint={getGroupLabel(s.group, hasFinalizeTiming)}
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
        'group/details border-t border-row pt-1.5 transition',
        suggested ? 'rounded-sm' : '',
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
              className={cx(
                'flex items-center gap-2 text-caption w-full px-1.5 py-1 rounded-sm transition text-left cursor-pointer',
                isActive
                  ? 'bg-overlay-6 text-text'
                  : 'text-text-muted hover:bg-overlay-5',
                isSticky
                  ? 'ring-2 ring-inset ring-[color:var(--color-accent)]/70'
                  : isActive
                    ? 'ring-1 ring-inset ring-[color:var(--color-accent)]/45'
                    : '',
              )}
            >
              <span
                className={cx(
                  m.color,
                  'inline-flex w-3 shrink-0 items-center justify-center',
                )}
              >
                <MarkerIcon starred={m.starred} />
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
      className={cx(
        'flex items-center gap-2 text-caption w-full px-1.5 py-1 rounded-sm transition text-left cursor-pointer',
        isActive
          ? 'bg-overlay-6 text-text'
          : 'text-text-muted hover:bg-overlay-5',
        isSticky
          ? 'ring-2 ring-inset ring-[color:var(--color-accent)]/70'
          : isActive
            ? 'ring-1 ring-inset ring-[color:var(--color-accent)]/45'
            : '',
      )}
    >
      {leading}
      <span className="flex-1 truncate">{label}</span>
      {hint ? <span className="text-text-faint shrink-0">· {hint}</span> : null}
      {detail ? (
        <span className="text-text-faint shrink-0">· {detail}</span>
      ) : null}
      <span className="tabular-nums shrink-0 w-16 text-right">
        {setupTiming ? fmtSetupMs(ms) : fmtMs(ms)}
      </span>
      <span className="tabular-nums shrink-0 w-8 text-right text-text-faint">
        {pct(ms, total)}%
      </span>
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
              <span className="text-text-faint text-caption">· {hint}</span>
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

/** Below this total the responsibility and timeline bars are not drawn. */
export const MIN_CHARTABLE_LATENCY_MS = 5;

export function LatencyTimeline({
  event,
  isPartial,
  isLoading = false,
}: {
  event: RequestEventWithPhase;
  isPartial?: boolean;
  isLoading?: boolean;
}) {
  const attribution = useMemo(() => computeLatencyAttribution(event), [event]);
  const total = attribution.totalMs;
  const stages = useMemo(() => buildStageDetails(event), [event]);
  const groups = computeStageGroups(event);
  const markers = useMemo(() => buildSseMarkers(event), [event]);
  const hasFinalizeTiming = event.finalize_ms != null;
  const isFinalRenewal =
    event._phase === 'final' && event.source_kind === 'renewal';
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
        <div className="space-y-2" aria-hidden="true">
          {groupOrder.map((group) => (
            <div key={group}>
              <div className="flex items-center justify-between mb-0.5">
                <Skeleton className="h-2.5 w-16" />
                <Skeleton className="h-2.5 w-20" />
              </div>
              <Skeleton className="h-4 w-full" />
            </div>
          ))}
        </div>
        <div aria-hidden="true">
          <div className="flex items-center justify-between mb-1">
            <Skeleton className="h-2.5 w-20" />
            <Skeleton className="h-2.5 w-16" />
          </div>
          <Skeleton className="h-6 w-full" />
          <Skeleton className="mt-1 h-3 w-full" />
          <div className="mt-2 flex gap-3">
            <Skeleton className="h-2.5 w-12" />
            <Skeleton className="h-2.5 w-14" />
            <Skeleton className="h-2.5 w-12" />
          </div>
        </div>
        <div className="border-t border-subtle pt-1.5" aria-hidden="true">
          <Skeleton className="h-5 w-32" />
        </div>
        <div className="border-t border-subtle pt-1.5" aria-hidden="true">
          <Skeleton className="h-5 w-40" />
        </div>
      </div>
    );
  }

  if (total <= 0 && !hasMeasuredRenewalCycle) {
    return (
      <div
        className="min-h-80 text-body text-text-muted"
        data-testid="latency-timeline-region"
      >
        No latency data recorded.
      </div>
    );
  }

  const unaccounted = groups.unaccounted;
  const showStreamLane = markers.length > 0 && !isPartial;
  // Under a few milliseconds the proportional bars are noise: every stage
  // rounds to a sliver, so only the itemized stage list stays.
  const tooFastToChart =
    !isPartial && total > 0 && total < MIN_CHARTABLE_LATENCY_MS;

  return (
    <div
      className={LATENCY_LAYOUT_CLASS}
      data-testid="latency-timeline-region"
      data-accounted-ms={groups.accounted}
      data-raw-residual-ms={groups.rawResidual}
      data-downstream-ms={attribution.downstreamMs}
      data-cc-lb-ms={attribution.ccLbMs}
      data-upstream-net-ms={attribution.upstreamNetMs}
      data-upstream-wait-ms={attribution.upstreamWaitMs}
      data-unattributed-ms={attribution.unattributedMs}
    >
      {tooFastToChart ? (
        <p className="text-text-faint" data-testid="latency-too-fast">
          Finished in under {MIN_CHARTABLE_LATENCY_MS} ms — too fast for a
          meaningful breakdown.
        </p>
      ) : (
        <>
          <ResponsibilityOverview attribution={attribution} event={event} />
          <section
            aria-label="Chronological request stages"
            className="space-y-2"
          >
            <div className="flex items-center justify-between">
              <span className="text-label text-text-muted">
                Request timeline
              </span>
              <span className="text-caption text-text-faint">
                Chronological stages
              </span>
            </div>
            <div className="space-y-2" data-testid="latency-stage-groups">
              {groupOrder.map((g) => {
                const items = positioned.positioned.filter(
                  (p) => p.group === g,
                );
                if (items.length === 0) return null;
                const groupSum = items.reduce((a, s) => a + s.ms, 0);
                const groupTotal =
                  g === 'internal_pre' ? groups.internalPre : groupSum;
                const interactiveItems = items.filter(
                  (item) =>
                    item.ms > 0 &&
                    (!item.setupTiming || item.ms / total >= 0.001),
                );
                const anyActive = items.some((it) => active.isActive(it.key));
                const groupItems = stages.filter((s) => s.group === g);
                const groupLabel = getGroupLabel(g, hasFinalizeTiming);
                return (
                  <div key={g}>
                    <div className="flex items-center justify-between text-caption mb-0.5">
                      <span
                        className={cx(
                          'flex items-center gap-1.5',
                          anyActive
                            ? 'font-medium text-text'
                            : 'text-text-muted',
                        )}
                      >
                        <span
                          aria-hidden
                          className="h-2 w-2 shrink-0 rounded-sm"
                          style={{
                            backgroundColor: stageShade(g, 0, 1),
                          }}
                        />
                        {groupLabel}
                      </span>
                      <span className="text-text-muted tabular-nums">
                        {fmtMs(groupTotal)} · {pct(groupTotal, total)}%
                      </span>
                    </div>
                    <div className="relative h-3 w-full overflow-hidden rounded-xs bg-progress-track">
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
        </>
      )}

      <StageDetailsList
        stages={stages}
        total={total}
        unaccounted={unaccounted}
        active={active}
        hasFinalizeTiming={hasFinalizeTiming}
      />
      {showStreamLane && <SseDetailsList markers={markers} active={active} />}
    </div>
  );
}
