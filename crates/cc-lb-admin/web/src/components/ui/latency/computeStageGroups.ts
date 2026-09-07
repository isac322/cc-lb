import type { RequestEventWithPhase } from '../../../lib/RequestEventTypes';

export interface StageGroups {
  internalPre: number;
  setupOverhead: number;
  wait: number;
  upstream: number;
  body: number;
  internalPost: number;
  unaccounted: number;
}

// `proxy_setup_ms` wraps handle-entry → attempt-entry and includes
// auth + route + limit_reserve + ctx build. `setup_overhead` is the
// residual inside that wrapper not attributed to a named sub-stage.
export function deriveSetupOverhead(e: RequestEventWithPhase): number {
  if (e._phase === 'partial') return 0;
  if (e.proxy_setup_ms == null) return 0;
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

export function computeStageGroups(e: RequestEventWithPhase): StageGroups {
  if (e._phase === 'partial') {
    return {
      internalPre: 0,
      setupOverhead: 0,
      wait: 0,
      upstream: 0,
      body: 0,
      internalPost: 0,
      unaccounted: 0,
    };
  }
  const setupOverhead = deriveSetupOverhead(e);
  const internalPre =
    (e.auth_ms ?? 0) +
    (e.route_ms ?? 0) +
    (e.limit_reserve_ms ?? 0) +
    setupOverhead +
    (e.shape_ms ?? 0) +
    (e.sign_ms ?? 0);
  const wait = (e.bulkhead_wait_ms ?? 0) + (e.dns_ms ?? 0);
  const upstream_post_handshake = Math.max(
    0,
    (e.upstream_ttfb_ms ?? 0) -
      (e.bulkhead_wait_ms ?? 0) -
      (e.dns_ms ?? 0) -
      (e.connect_ms ?? 0),
  );
  const upstream = (e.connect_ms ?? 0) + upstream_post_handshake;
  const body = e.upstream_body_ms ?? 0;
  const internalPost = e.limit_reconcile_ms ?? 0;
  const sum = internalPre + wait + upstream + body + internalPost;
  const unaccounted = Math.max(0, deriveProxyTimelineDuration(e) - sum);
  return {
    internalPre,
    setupOverhead,
    wait,
    upstream,
    body,
    internalPost,
    unaccounted,
  };
}
