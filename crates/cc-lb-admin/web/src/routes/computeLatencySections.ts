import type { RequestEvent } from '../lib/api';

export interface LatencyRow {
  label: string;
  value: number;
}
export interface LatencySection {
  title: string;
  rows: LatencyRow[];
  warmPool?: boolean;
}

export function computeLatencySections(e: RequestEvent): {
  sections: LatencySection[];
  total: number;
  unaccounted: number;
} {
  const sections: LatencySection[] = [];
  const push = (
    title: string,
    candidates: Array<[string, number | undefined]>,
    warmPool?: boolean,
  ) => {
    const rows = candidates
      .filter(([_, v]) => v !== undefined && v !== null)
      .map(([label, v]) => ({ label, value: v as number }));
    if (rows.length > 0) sections.push({ title, rows, warmPool });
  };
  push('Internal pre', [
    ['Auth', e.auth_ms],
    ['Route', e.route_ms],
    ['Limit reserve', e.limit_reserve_ms],
    ['Shape', e.shape_ms],
    ['Sign', e.sign_ms],
  ]);
  push('Wait', [
    ['Bulkhead wait', e.bulkhead_wait_ms],
    ['DNS', e.dns_ms],
  ]);
  const derivedUpstreamWait =
    e.upstream_ttfb_ms !== undefined && e.upstream_ttfb_ms !== null
      ? Math.max(
          0,
          e.upstream_ttfb_ms -
            (e.bulkhead_wait_ms ?? 0) -
            (e.dns_ms ?? 0) -
            (e.connect_ms ?? 0),
        )
      : undefined;
  push(
    'Upstream',
    [
      ['Connect (TCP+TLS)', e.connect_ms],
      ['Upstream wait', derivedUpstreamWait],
    ],
    e.connection_reused === true,
  );
  push('Body', [
    ['Body collect', e.upstream_body_ms],
    ['First body chunk', e.first_body_chunk_ms],
    ['First content delta', e.stream_first_content_delta_ms],
    ['Last content delta', e.stream_last_content_delta_ms],
    ['Inter-token avg', e.inter_token_avg_ms],
  ]);
  push('Internal post', [
    ['Observability hook', e.observability_post_ms],
    ['Limit reconcile', e.limit_reconcile_ms],
  ]);
  const total = e.duration_ms ?? 0;
  const known = sections
    .flatMap((s) => s.rows.map((r) => r.value))
    .reduce((a, b) => a + b, 0);
  const unaccounted = Math.max(0, total - known);
  return { sections, total, unaccounted };
}
