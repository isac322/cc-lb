import type { RequestEventWithPhase } from '../../../lib/RequestEventTypes';

export type LatencyCategory =
  | 'downstream_network'
  | 'cc_lb'
  | 'upstream_network'
  | 'upstream_processing';

export const LATENCY_CATEGORY_META: Record<
  LatencyCategory,
  {
    label: string;
    fill: string;
    textClass: string;
    surfaceClass: string;
  }
> = {
  downstream_network: {
    label: 'Downstream network',
    fill: '#06b6d4',
    textClass: 'text-[color:var(--latency-downstream-text)]',
    surfaceClass: 'border-cyan-500/25 bg-cyan-500/10',
  },
  cc_lb: {
    label: 'cc-lb processing',
    fill: '#6366f1',
    textClass: 'text-[color:var(--latency-proxy-text)]',
    surfaceClass: 'border-indigo-500/25 bg-indigo-500/10',
  },
  upstream_network: {
    label: 'Upstream network',
    fill: '#f59e0b',
    textClass: 'text-[color:var(--latency-upstream-text)]',
    surfaceClass: 'border-amber-500/25 bg-amber-500/10',
  },
  upstream_processing: {
    label: 'Upstream processing',
    fill: '#10b981',
    textClass: 'text-[color:var(--latency-provider-text)]',
    surfaceClass: 'border-emerald-500/25 bg-emerald-500/10',
  },
};

export interface LatencyCategoryValue {
  key: LatencyCategory;
  ms: number | null;
  note: string;
}

export interface LatencyAttribution {
  categories: LatencyCategoryValue[];
  mixedUpstreamMs: number | null;
  mixedRetryMs: number | null;
  unattributedMs: number;
  timelineTotalMs: number;
  accountedMs: number;
  hasIoTimings: boolean;
  accountingWarning: boolean;
}

export interface StageGroups {
  internalPre: number;
  setupOverhead: number;
  wait: number;
  upstream: number;
  body: number;
  internalPost: number;
  retryOverhead: number;
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

function measured(value: unknown): number | null {
  if (typeof value !== 'number') return null;
  return Number.isFinite(value) && value >= 0 ? value : null;
}

function sumMeasured(values: readonly unknown[]): number | null {
  let total = 0;
  let found = false;
  for (const value of values) {
    const valid = measured(value);
    if (valid == null) continue;
    total += valid;
    found = true;
  }
  return found ? total : null;
}

function valueOrZero(value: unknown): number {
  return measured(value) ?? 0;
}

export function hasSetupTimingBreakdown(e: RequestEventWithPhase): boolean {
  return SETUP_TIMING_STAGES.some(([field]) => measured(e[field]) != null);
}

export function setupTimingTotal(e: RequestEventWithPhase): number {
  return SETUP_TIMING_STAGES.reduce(
    (total, [field]) => total + valueOrZero(e[field]),
    0,
  );
}

export function deriveOtherSetup(e: RequestEventWithPhase): number {
  const proxySetup = measured(e.proxy_setup_ms);
  if (e._phase === 'partial' || proxySetup == null) return 0;
  return Math.max(
    0,
    proxySetup -
      valueOrZero(e.auth_ms) -
      valueOrZero(e.route_ms) -
      valueOrZero(e.limit_reserve_ms) -
      setupTimingTotal(e),
  );
}

// `proxy_setup_ms` wraps handle-entry → attempt-entry and includes auth,
// route, limit reservation, the measured setup stages, and an unmeasured
// residual. This helper returns the whole residual so group totals and SSE
// marker offsets retain their existing meaning.
export function deriveSetupOverhead(e: RequestEventWithPhase): number {
  const proxySetup = measured(e.proxy_setup_ms);
  if (e._phase === 'partial' || proxySetup == null) return 0;
  return Math.max(
    0,
    proxySetup -
      valueOrZero(e.auth_ms) -
      valueOrZero(e.route_ms) -
      valueOrZero(e.limit_reserve_ms),
  );
}

export function deriveProxyTimelineDuration(e: RequestEventWithPhase): number {
  if (e._phase === 'partial') return valueOrZero(e.elapsed_ms);
  if (e._phase !== 'final') return 0;
  return valueOrZero(e.duration_ms);
}

export function responseBodyDuration(e: RequestEventWithPhase): number {
  if (e._phase !== 'final') return 0;
  if (e.status === 499) return valueOrZero(e.upstream_body_ms);
  return measured(e.stream_total_ms) ?? valueOrZero(e.upstream_body_ms);
}

const IO_DURATION_FIELDS = [
  'request_body_first_chunk_ms',
  'request_body_receive_ms',
  'request_body_wait_ms',
  'request_body_process_ms',
  'response_body_wait_ms',
  'response_body_process_ms',
  'response_body_downstream_poll_gap_ms',
  'retry_overhead_ms',
] as const;

function hasIoTiming(e: RequestEventWithPhase): boolean {
  const chunkCount = e.request_body_chunk_count;
  return (
    IO_DURATION_FIELDS.some((field) => measured(e[field]) != null) ||
    (chunkCount != null &&
      Number.isFinite(chunkCount) &&
      Number.isInteger(chunkCount) &&
      chunkCount >= 0)
  );
}

// These parent intervals and the global timeline total are emitted as
// `Duration::as_millis()` u64 values, while their I/O/setup descendants retain
// f64 milliseconds. A reported parent P therefore represents [P, P + 1), so a
// descendant total below P + 1 is expected precision loss, not corruption.
// Keep this bound specific to known truncated-millisecond parent comparisons;
// it is not a general-purpose accounting epsilon.
function overrunsTruncatedMillisecondParent(
  parent: number | null,
  descendants: number | null,
): boolean {
  return parent != null && descendants != null && descendants >= parent + 1;
}

function setupAttribution(e: RequestEventWithPhase): {
  ms: number | null;
  warning: boolean;
} {
  const parent = measured(e.proxy_setup_ms);
  const children = sumMeasured([
    e.auth_ms,
    e.route_ms,
    e.limit_reserve_ms,
    ...SETUP_TIMING_STAGES.map(([field]) => e[field]),
  ]);
  if (parent != null) {
    return {
      ms: parent,
      warning: overrunsTruncatedMillisecondParent(parent, children),
    };
  }
  return { ms: children, warning: false };
}

function finalizeAttribution(e: RequestEventWithPhase): {
  ms: number | null;
  warning: boolean;
} {
  const parent = measured(e.finalize_ms);
  const child = measured(e.limit_reconcile_ms);
  if (parent != null) {
    return {
      ms: parent,
      warning: overrunsTruncatedMillisecondParent(parent, child),
    };
  }
  return { ms: child, warning: false };
}

export function computeLatencyAttribution(
  e: RequestEventWithPhase,
): LatencyAttribution {
  const timelineTotalMs = deriveProxyTimelineDuration(e);
  const hasIoTimings = hasIoTiming(e);

  if (e.source_kind === 'renewal') {
    return {
      categories: [
        {
          key: 'downstream_network',
          ms: null,
          note: 'Not measured for renewal lifecycle events.',
        },
        {
          key: 'cc_lb',
          ms: null,
          note: 'Renewal is a lifecycle cycle, not a proxy request.',
        },
        {
          key: 'upstream_network',
          ms: null,
          note: 'Not measured for renewal lifecycle events.',
        },
        {
          key: 'upstream_processing',
          ms: null,
          note: 'Not independently measured.',
        },
      ],
      mixedUpstreamMs: null,
      mixedRetryMs: null,
      unattributedMs: timelineTotalMs,
      timelineTotalMs,
      accountedMs: 0,
      hasIoTimings,
      accountingWarning: false,
    };
  }

  const requestParent = measured(e.request_body_read_ms);
  const requestWait = measured(e.request_body_wait_ms);
  const requestProcess = measured(e.request_body_process_ms);
  const requestChildren = (requestWait ?? 0) + (requestProcess ?? 0);
  const requestWarning = overrunsTruncatedMillisecondParent(
    requestParent,
    requestChildren,
  );

  const setup = setupAttribution(e);
  const finalize = finalizeAttribution(e);
  const bulkhead = measured(e.bulkhead_wait_ms);
  const dns = measured(e.dns_ms);
  const connect = measured(e.connect_ms);
  const ttfbParent = measured(e.upstream_ttfb_ms);
  const ttfbChildren = (bulkhead ?? 0) + (dns ?? 0) + (connect ?? 0);
  const headerWait =
    ttfbParent == null ? null : Math.max(0, ttfbParent - ttfbChildren);
  const ttfbWarning = overrunsTruncatedMillisecondParent(
    ttfbParent,
    ttfbChildren,
  );

  const responseParent =
    e._phase !== 'final'
      ? null
      : e.status === 499
        ? measured(e.upstream_body_ms)
        : (measured(e.stream_total_ms) ?? measured(e.upstream_body_ms));
  const responseWait = measured(e.response_body_wait_ms);
  const responseProcess = measured(e.response_body_process_ms);
  const downstreamPollGap = measured(e.response_body_downstream_poll_gap_ms);
  const responseChildren =
    (responseWait ?? 0) + (responseProcess ?? 0) + (downstreamPollGap ?? 0);
  const responseWarning = overrunsTruncatedMillisecondParent(
    responseParent,
    responseChildren,
  );
  const responseHasSplit =
    responseWait != null ||
    responseProcess != null ||
    downstreamPollGap != null;
  const legacyMixedResponse =
    responseParent != null && !responseHasSplit ? responseParent : null;

  const downstreamMs = sumMeasured([requestWait, downstreamPollGap]);
  const setupMs = setup.ms;
  const ccLbMs = sumMeasured([
    setupMs,
    e.shape_ms,
    e.sign_ms,
    bulkhead,
    requestProcess,
    responseProcess,
    finalize.ms,
  ]);
  const upstreamNetworkMs = sumMeasured([dns, connect]);
  const mixedUpstreamMs = sumMeasured([
    headerWait,
    responseWait,
    legacyMixedResponse,
  ]);
  const mixedRetryMs = measured(e.retry_overhead_ms);

  const categories: LatencyCategoryValue[] = [
    {
      key: 'downstream_network',
      ms: downstreamMs,
      note:
        downstreamMs == null
          ? 'Not independently measured. Downstream TLS and headers before the handler, plus buffered delivery after finalization, are outside this timeline.'
          : 'Observed request-body wait and downstream consumer gaps; includes client, transit, backpressure, and runtime scheduling, not wire RTT.',
    },
    {
      key: 'cc_lb',
      ms: ccLbMs,
      note:
        ccLbMs == null
          ? 'No independent cc-lb stage timing is available.'
          : 'Measured proxy setup, routing, shaping, signing, bulkhead wait, finalization, and local body work; elapsed local work, not CPU time.',
    },
    {
      key: 'upstream_network',
      ms: upstreamNetworkMs,
      note:
        upstreamNetworkMs == null
          ? 'Not independently measured. Only recorded DNS and TCP/TLS connect time is attributed here.'
          : 'Observed DNS and TCP/TLS connect time only; it excludes upstream header and response-body waits.',
    },
    {
      key: 'upstream_processing',
      ms: null,
      note:
        legacyMixedResponse != null
          ? 'Not independently measured. Legacy response-body time combines upstream wait, local relay work, and downstream consumption.'
          : 'Not independently measured. Upstream header and response-body waits are shown as combined upstream wait.',
    },
  ];

  const accountedMs =
    categories.reduce((total, category) => total + (category.ms ?? 0), 0) +
    (mixedUpstreamMs ?? 0) +
    (mixedRetryMs ?? 0);
  const unattributedMs = Math.max(0, timelineTotalMs - accountedMs);
  const timelineParent =
    e._phase === 'partial'
      ? measured(e.elapsed_ms)
      : e._phase === 'final'
        ? measured(e.duration_ms)
        : null;
  const timelineWarning =
    timelineParent == null
      ? accountedMs > 0
      : overrunsTruncatedMillisecondParent(timelineParent, accountedMs);
  const accountingWarning =
    requestWarning ||
    setup.warning ||
    ttfbWarning ||
    responseWarning ||
    finalize.warning ||
    timelineWarning;

  return {
    categories,
    mixedUpstreamMs,
    mixedRetryMs,
    unattributedMs,
    timelineTotalMs,
    accountedMs,
    hasIoTimings,
    accountingWarning,
  };
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
      retryOverhead: 0,
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
      retryOverhead: 0,
      accounted: total,
      rawResidual: 0,
      unaccounted: 0,
    };
  }

  const setupOverhead = deriveSetupOverhead(e);
  const internalPre =
    valueOrZero(e.request_body_read_ms) +
    valueOrZero(e.auth_ms) +
    valueOrZero(e.route_ms) +
    valueOrZero(e.limit_reserve_ms) +
    setupOverhead +
    valueOrZero(e.shape_ms) +
    valueOrZero(e.sign_ms);
  const wait = valueOrZero(e.bulkhead_wait_ms) + valueOrZero(e.dns_ms);
  const upstreamPostHandshake = Math.max(
    0,
    valueOrZero(e.upstream_ttfb_ms) -
      valueOrZero(e.bulkhead_wait_ms) -
      valueOrZero(e.dns_ms) -
      valueOrZero(e.connect_ms),
  );
  const upstream = valueOrZero(e.connect_ms) + upstreamPostHandshake;
  const body = responseBodyDuration(e);
  const internalPost =
    measured(e.finalize_ms) ?? valueOrZero(e.limit_reconcile_ms);
  const retryOverhead = valueOrZero(e.retry_overhead_ms);
  const accounted =
    internalPre + wait + upstream + body + internalPost + retryOverhead;
  const rawResidual = total - accounted;
  const unaccounted = Math.max(0, rawResidual);
  return {
    internalPre,
    setupOverhead,
    wait,
    upstream,
    body,
    internalPost,
    retryOverhead,
    accounted,
    rawResidual,
    unaccounted,
  };
}
