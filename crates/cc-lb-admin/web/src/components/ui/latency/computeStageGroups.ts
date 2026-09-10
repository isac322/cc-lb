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
  const internalPost = e.finalize_ms ?? e.limit_reconcile_ms ?? 0;
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
