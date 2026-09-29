import type { RequestEventWithPhase } from '../../../lib/RequestEventTypes';

export interface StageGroups {
  internalPre: number;
  setupOverhead: number;
  wait: number;
  upstream: number;
  body: number;
  internalPost: number;
  accounted: number;
  rawResidual: number;
  unaccounted: number;
}

export const CACHE_SETUP_TIMING_STAGES = [
  ['json_parse_ms', 'JSON parse'],
  ['cache_tokenizer_queue_ms', 'Tokenizer queue'],
  ['cache_structure_ms', 'Cache structure'],
  ['cache_serialize_ms', 'Cache serialize'],
  ['cache_token_key_ms', 'Cache token key'],
  ['cache_count_lookup_ms', 'Cache count lookup'],
  ['cache_tokenize_ms', 'Cache tokenize'],
] as const;

export const SETUP_TIMING_STAGES = [
  ...CACHE_SETUP_TIMING_STAGES,
  ['prepare_signer_ms', 'Prepare signer'],
] as const;

export function hasSetupTimingBreakdown(e: RequestEventWithPhase): boolean {
  return SETUP_TIMING_STAGES.some(([field]) => e[field] != null);
}

export function setupTimingTotal(e: RequestEventWithPhase): number {
  return SETUP_TIMING_STAGES.reduce(
    (total, [field]) => total + (e[field] ?? 0),
    0,
  );
}

export function deriveOtherSetup(e: RequestEventWithPhase): number {
  if (e._phase === 'partial' || e.proxy_setup_ms == null) return 0;
  return Math.max(
    0,
    e.proxy_setup_ms -
      (e.auth_ms ?? 0) -
      (e.route_ms ?? 0) -
      (e.limit_reserve_ms ?? 0) -
      setupTimingTotal(e),
  );
}

// `proxy_setup_ms` wraps handle-entry → attempt-entry and includes auth,
// route, limit reservation, the measured setup stages, and an unmeasured
// residual. This helper returns the whole residual so group totals and SSE
// marker offsets retain their existing meaning.
export function deriveSetupOverhead(e: RequestEventWithPhase): number {
  if (e._phase === 'partial' || e.proxy_setup_ms == null) return 0;
  return Math.max(
    0,
    e.proxy_setup_ms -
      (e.auth_ms ?? 0) -
      (e.route_ms ?? 0) -
      (e.limit_reserve_ms ?? 0),
  );
}
export function deriveProxyTimelineDuration(e: RequestEventWithPhase): number {
  if (e._phase === 'partial') return Math.max(0, e.elapsed_ms ?? 0);
  if (e._phase !== 'final') return 0;
  return Math.max(0, e.duration_ms ?? 0);
}

export function responseBodyDuration(e: RequestEventWithPhase): number {
  if (e._phase !== 'final') return 0;
  if (e.status === 499) return e.upstream_body_ms ?? 0;
  return e.stream_total_ms ?? e.upstream_body_ms ?? 0;
}

export type LatencyResponsibilityKey =
  | 'downstream'
  | 'cc-lb'
  | 'upstream-net'
  | 'upstream-wait'
  | 'unattributed'
  | 'renewal';

export interface LatencyResponsibilityMeta {
  label: string;
  color: string;
  description: string;
}

export const LATENCY_RESPONSIBILITY_META: Record<
  LatencyResponsibilityKey,
  LatencyResponsibilityMeta
> = {
  downstream: {
    label: 'Downstream',
    color: 'bg-series-latency-downstream',
    description:
      'Combines client pacing and downstream transit with runtime scheduling after handler entry. It is not a network RTT measurement.',
  },
  'cc-lb': {
    label: 'cc-lb',
    color: 'bg-series-latency-cclb',
    description:
      'Local request handling, routing, cache analysis, queueing, relay processing, and finalization.',
  },
  'upstream-net': {
    label: 'Upstream net',
    color: 'bg-series-latency-net',
    description:
      'DNS resolution and TCP/TLS connection establishment. Reused connections report no connector timing.',
  },
  'upstream-wait': {
    label: 'Upstream wait',
    color: 'bg-series-latency-wait',
    description:
      'Combines provider generation, upstream transit, and runtime scheduling. Those parts cannot be separated.',
  },
  unattributed: {
    label: 'Unattributed',
    color:
      'bg-overlay-6 bg-[repeating-linear-gradient(45deg,_transparent_0_4px,_var(--color-border-strong)_4px_8px)]',
    description:
      'Time that cannot be assigned to one responsibility with the available timing witnesses.',
  },
  renewal: {
    label: 'Renewal cycle',
    color: 'bg-series-latency-cclb',
    description:
      'The complete cache-keepalive renewal cycle performed by the scheduler.',
  },
};

export interface LatencyResponsibilityGroup extends LatencyResponsibilityMeta {
  key: LatencyResponsibilityKey;
  valueMs: number;
  observed: boolean;
}

export interface LatencyAttribution {
  totalMs: number;
  requestBodyReadMs: number;
  requestBodyWaitMs: number;
  requestBodyProcessMs: number;
  requestBodyOtherMs: number;
  proxySetupMs: number;
  responseBodyMs: number;
  responseBodyWaitMs: number;
  responseBodyProcessMs: number;
  downstreamPollGapMs: number;
  responseBodyOtherMs: number;
  retryOverheadMs: number;
  upstreamHeaderWaitMs: number;
  finalizationMs: number;
  topLevelResidualMs: number;
  downstreamMs: number;
  ccLbMs: number;
  upstreamNetMs: number;
  upstreamWaitMs: number;
  unattributedMs: number;
  isFinalRenewal: boolean;
  isCancelledPartial: boolean;
  hasRequestBodyBreakdown: boolean;
  hasResponseBodyBreakdown: boolean;
  responsibilities: LatencyResponsibilityGroup[];
}

function timingField(
  event: RequestEventWithPhase,
  field: string,
): number | null {
  const value: unknown = Reflect.get(event, field);
  return typeof value === 'number' && Number.isFinite(value) && value >= 0
    ? value
    : null;
}

export function computeLatencyAttribution(
  event: RequestEventWithPhase,
): LatencyAttribution {
  const totalMs = deriveProxyTimelineDuration(event);
  const isFinalRenewal =
    event._phase === 'final' && event.source_kind === 'renewal';
  const isCancelledPartial =
    event.status === 499 && event.upstream_body_ms != null;

  if (isFinalRenewal) {
    return {
      totalMs,
      requestBodyReadMs: 0,
      requestBodyWaitMs: 0,
      requestBodyProcessMs: 0,
      requestBodyOtherMs: 0,
      proxySetupMs: 0,
      responseBodyMs: 0,
      responseBodyWaitMs: 0,
      responseBodyProcessMs: 0,
      downstreamPollGapMs: 0,
      responseBodyOtherMs: 0,
      retryOverheadMs: 0,
      upstreamHeaderWaitMs: 0,
      finalizationMs: 0,
      topLevelResidualMs: 0,
      downstreamMs: 0,
      ccLbMs: 0,
      upstreamNetMs: 0,
      upstreamWaitMs: 0,
      unattributedMs: 0,
      isFinalRenewal,
      isCancelledPartial: false,
      hasRequestBodyBreakdown: false,
      hasResponseBodyBreakdown: false,
      responsibilities: [
        {
          key: 'renewal',
          ...LATENCY_RESPONSIBILITY_META.renewal,
          valueMs: totalMs,
          observed: event.duration_ms != null,
        },
      ],
    };
  }

  const requestBodyReadMs = timingField(event, 'request_body_read_ms') ?? 0;
  const requestBodyWaitValue = timingField(event, 'request_body_wait_ms');
  const requestBodyProcessValue = timingField(event, 'request_body_process_ms');
  const requestBodyWaitMs = requestBodyWaitValue ?? 0;
  const requestBodyProcessMs = requestBodyProcessValue ?? 0;
  const responseBodyMs = responseBodyDuration(event);
  const responseBodyWaitValue = timingField(event, 'response_body_wait_ms');
  const responseBodyProcessValue = timingField(
    event,
    'response_body_process_ms',
  );
  const downstreamPollGapValue = timingField(
    event,
    'response_body_downstream_poll_gap_ms',
  );
  const responseBodyWaitMs = responseBodyWaitValue ?? 0;
  const responseBodyProcessMs = responseBodyProcessValue ?? 0;
  const downstreamPollGapMs = downstreamPollGapValue ?? 0;
  const retryOverheadMs = timingField(event, 'retry_overhead_ms') ?? 0;
  const bulkheadWaitMs = timingField(event, 'bulkhead_wait_ms') ?? 0;
  const dnsMs = timingField(event, 'dns_ms') ?? 0;
  const connectMs = timingField(event, 'connect_ms') ?? 0;
  const proxySetupValue = timingField(event, 'proxy_setup_ms');
  const hasSetupBreakdown = hasSetupTimingBreakdown(event);
  const proxySetupMs =
    proxySetupValue ??
    (event.auth_ms ?? 0) +
      (event.route_ms ?? 0) +
      (event.limit_reserve_ms ?? 0) +
      (hasSetupBreakdown
        ? setupTimingTotal(event)
        : deriveSetupOverhead(event));
  const finalizationMs = timingField(event, 'finalize_ms') ?? 0;
  const upstreamHeaderWaitMs = Math.max(
    0,
    (timingField(event, 'upstream_ttfb_ms') ?? 0) -
      bulkheadWaitMs -
      dnsMs -
      connectMs,
  );
  const hasRequestBodyBreakdown =
    requestBodyWaitValue != null || requestBodyProcessValue != null;
  const hasResponseBodyBreakdown =
    responseBodyWaitValue != null ||
    responseBodyProcessValue != null ||
    downstreamPollGapValue != null;
  const requestBodyOtherMs =
    timingField(event, 'request_body_read_ms') == null
      ? 0
      : Math.max(
          0,
          requestBodyReadMs - requestBodyWaitMs - requestBodyProcessMs,
        );
  const hasResponseBodyParent =
    event._phase === 'final' &&
    (event.stream_total_ms != null || event.upstream_body_ms != null);
  const responseBodyOtherMs = hasResponseBodyParent
    ? Math.max(
        0,
        responseBodyMs -
          responseBodyWaitMs -
          responseBodyProcessMs -
          downstreamPollGapMs,
      )
    : 0;
  const downstreamMs = requestBodyWaitMs + downstreamPollGapMs;
  const ccLbMs =
    requestBodyProcessMs +
    proxySetupMs +
    (event.shape_ms ?? 0) +
    (event.sign_ms ?? 0) +
    bulkheadWaitMs +
    responseBodyProcessMs +
    finalizationMs;
  const upstreamNetMs = dnsMs + connectMs;
  const upstreamWaitMs = upstreamHeaderWaitMs + responseBodyWaitMs;
  const stageGroups = computeStageGroups(event);
  const setupTimingOutsideParent =
    proxySetupValue != null
      ? 0
      : Math.max(
          0,
          proxySetupMs -
            (event.auth_ms ?? 0) -
            (event.route_ms ?? 0) -
            (event.limit_reserve_ms ?? 0),
        );
  const requestBodyTimingOutsideParent =
    timingField(event, 'request_body_read_ms') == null
      ? requestBodyWaitMs + requestBodyProcessMs
      : 0;
  const responseBodyTimingOutsideParent = hasResponseBodyParent
    ? 0
    : responseBodyWaitMs + responseBodyProcessMs + downstreamPollGapMs;
  const topLevelResidualMs =
    event._phase === 'final'
      ? Math.max(
          0,
          totalMs -
            stageGroups.accounted -
            retryOverheadMs -
            setupTimingOutsideParent -
            requestBodyTimingOutsideParent -
            responseBodyTimingOutsideParent,
        )
      : 0;
  const unattributedMs =
    requestBodyOtherMs +
    responseBodyOtherMs +
    retryOverheadMs +
    topLevelResidualMs;
  const responsibilityValues: Array<
    [LatencyResponsibilityKey, number, boolean]
  > = [
    [
      'downstream',
      downstreamMs,
      requestBodyWaitValue != null || downstreamPollGapValue != null,
    ],
    [
      'cc-lb',
      ccLbMs,
      [
        requestBodyProcessValue,
        proxySetupValue,
        event.auth_ms,
        event.route_ms,
        event.limit_reserve_ms,
        event.shape_ms,
        event.sign_ms,
        timingField(event, 'bulkhead_wait_ms'),
        responseBodyProcessValue,
        timingField(event, 'finalize_ms'),
      ].some((value) => value != null) || hasSetupBreakdown,
    ],
    [
      'upstream-net',
      upstreamNetMs,
      timingField(event, 'dns_ms') != null ||
        timingField(event, 'connect_ms') != null ||
        event.connection_reused === true,
    ],
    [
      'upstream-wait',
      upstreamWaitMs,
      timingField(event, 'upstream_ttfb_ms') != null ||
        responseBodyWaitValue != null,
    ],
    [
      'unattributed',
      unattributedMs,
      unattributedMs > 0 ||
        (!hasRequestBodyBreakdown &&
          timingField(event, 'request_body_read_ms') != null) ||
        (!hasResponseBodyBreakdown && hasResponseBodyParent),
    ],
  ];

  return {
    totalMs,
    requestBodyReadMs,
    requestBodyWaitMs,
    requestBodyProcessMs,
    requestBodyOtherMs,
    proxySetupMs,
    responseBodyMs,
    responseBodyWaitMs,
    responseBodyProcessMs,
    downstreamPollGapMs,
    responseBodyOtherMs,
    retryOverheadMs,
    upstreamHeaderWaitMs,
    finalizationMs,
    topLevelResidualMs,
    downstreamMs,
    ccLbMs,
    upstreamNetMs,
    upstreamWaitMs,
    unattributedMs,
    isFinalRenewal,
    isCancelledPartial,
    hasRequestBodyBreakdown,
    hasResponseBodyBreakdown,
    responsibilities: responsibilityValues.map(([key, valueMs, observed]) => ({
      key,
      ...LATENCY_RESPONSIBILITY_META[key],
      valueMs,
      observed,
    })),
  };
}

export function latencyResponsibilityDescription(
  key: LatencyResponsibilityKey,
  attribution: LatencyAttribution,
): string {
  const details = [LATENCY_RESPONSIBILITY_META[key].description];
  if (key !== 'unattributed') return details[0];
  if (attribution.retryOverheadMs > 0) {
    details.push(
      'Retry overhead is one aggregate across prior attempts; its cc-lb, network, and upstream portions cannot be separated.',
    );
  }
  if (
    attribution.requestBodyOtherMs > 0 ||
    attribution.responseBodyOtherMs > 0 ||
    attribution.topLevelResidualMs > 0
  ) {
    details.push(
      'Residual time has no finer timing witness available for responsibility attribution.',
    );
  }
  return details.join(' ');
}
export function computeStageGroups(e: RequestEventWithPhase): StageGroups {
  if (e._phase === 'partial') {
    return {
      internalPre: 0,
      setupOverhead: 0,
      wait: 0,
      upstream: 0,
      body: 0,
      internalPost: 0,
      accounted: 0,
      rawResidual: 0,
      unaccounted: 0,
    };
  }

  const total = deriveProxyTimelineDuration(e);
  if (e.source_kind === 'renewal') {
    return {
      internalPre: 0,
      setupOverhead: 0,
      wait: 0,
      upstream: 0,
      body: 0,
      internalPost: 0,
      accounted: total,
      rawResidual: 0,
      unaccounted: 0,
    };
  }

  const setupOverhead = deriveSetupOverhead(e);
  const internalPre =
    (e.request_body_read_ms ?? 0) +
    (e.auth_ms ?? 0) +
    (e.route_ms ?? 0) +
    (e.limit_reserve_ms ?? 0) +
    setupOverhead +
    (e.shape_ms ?? 0) +
    (e.sign_ms ?? 0);
  const wait = (e.bulkhead_wait_ms ?? 0) + (e.dns_ms ?? 0);
  const upstreamPostHandshake = Math.max(
    0,
    (e.upstream_ttfb_ms ?? 0) -
      (e.bulkhead_wait_ms ?? 0) -
      (e.dns_ms ?? 0) -
      (e.connect_ms ?? 0),
  );
  const upstream = (e.connect_ms ?? 0) + upstreamPostHandshake;
  const body = responseBodyDuration(e);
  const internalPost = e.finalize_ms ?? 0;
  const accounted = internalPre + wait + upstream + body + internalPost;
  const rawResidual = total - accounted;
  const unaccounted = Math.max(0, rawResidual);
  return {
    internalPre,
    setupOverhead,
    wait,
    upstream,
    body,
    internalPost,
    accounted,
    rawResidual,
    unaccounted,
  };
}
