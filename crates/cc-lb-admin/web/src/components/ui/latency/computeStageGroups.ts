import type { RequestEvent } from '../../../lib/api';

export interface StageGroups {
  internalPre: number;
  wait: number;
  upstream: number;
  body: number;
  internalPost: number;
  unaccounted: number;
}

export function computeStageGroups(e: Partial<RequestEvent>): StageGroups {
  const internalPre =
    (e.auth_ms ?? 0) +
    (e.route_ms ?? 0) +
    (e.limit_reserve_ms ?? 0) +
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
  const internalPost =
    (e.observability_post_ms ?? 0) + (e.limit_reconcile_ms ?? 0);
  const sum = internalPre + wait + upstream + body + internalPost;
  const unaccounted = Math.max(0, (e.duration_ms ?? 0) - sum);
  return { internalPre, wait, upstream, body, internalPost, unaccounted };
}
