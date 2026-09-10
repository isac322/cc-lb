import { describe, expect, it } from 'vitest';
import type { RequestEvent } from '../../../lib/api';
import type { RequestEventWithPhase } from '../../../lib/RequestEventTypes';
import {
  computeStageGroups,
  deriveOtherSetup,
  deriveProxyTimelineDuration,
  deriveSetupOverhead,
  hasSetupTimingBreakdown,
  setupTimingTotal,
} from './computeStageGroups';

function ev(overrides: Partial<RequestEvent>): RequestEventWithPhase {
  return {
    ts: 1234567890,
    request_id: 'req_test',
    upstream: 'anthropic',
    status: 200,
    duration_ms: 0,
    _phase: 'final',
    ...overrides,
  } satisfies RequestEventWithPhase;
}

describe('computeStageGroups', () => {
  it('returns all zero groups for empty event', () => {
    expect(computeStageGroups(ev({}))).toEqual({
      internalPre: 0,
      setupOverhead: 0,
      wait: 0,
      upstream: 0,
      body: 0,
      internalPost: 0,
      unaccounted: 0,
      accounted: 0,
      rawResidual: 0,
    });
  });

  it('sums internal pre stages', () => {
    const r = computeStageGroups(
      ev({
        auth_ms: 50,
        route_ms: 30,
        limit_reserve_ms: 10,
        shape_ms: 5,
        sign_ms: 2,
        duration_ms: 1000,
      }),
    );
    expect(r.internalPre).toBe(97);
  });

  it('sums wait stages', () => {
    const r = computeStageGroups(
      ev({
        bulkhead_wait_ms: 3,
        dns_ms: 100,
        duration_ms: 1000,
      }),
    );
    expect(r.wait).toBe(103);
  });

  it('derives upstream as connect + remaining ttfb', () => {
    const r = computeStageGroups(
      ev({
        bulkhead_wait_ms: 5,
        dns_ms: 20,
        connect_ms: 75,
        upstream_ttfb_ms: 500,
        duration_ms: 1000,
      }),
    );
    // connect_ms (75) + (ttfb 500 - bulkhead 5 - dns 20 - connect 75) = 75 + 400 = 475
    expect(r.upstream).toBe(475);
  });

  it('clamps upstream to connect only when ttfb less than sum of pre-stages', () => {
    const r = computeStageGroups(
      ev({
        bulkhead_wait_ms: 50,
        dns_ms: 200,
        connect_ms: 300,
        upstream_ttfb_ms: 100,
        duration_ms: 1000,
      }),
    );
    // upstream_post_handshake = max(0, 100 - 50 - 200 - 300) = 0; upstream = connect_ms (300)
    expect(r.upstream).toBe(300);
  });

  it('excludes post-response observability from timeline stages', () => {
    const event = ev({
      upstream_body_ms: 200,
      observability_post_ms: 15,
      limit_reconcile_ms: 10,
      duration_ms: 1000,
    });
    const r = computeStageGroups(event);
    expect(deriveProxyTimelineDuration(event)).toBe(1000);
    expect(r.body).toBe(200);
    expect(r.internalPost).toBe(10);
    expect(r.unaccounted).toBe(790);
  });

  it('computes unaccounted as duration minus sum', () => {
    const r = computeStageGroups(ev({ auth_ms: 100, duration_ms: 500 }));
    expect(r.unaccounted).toBe(400);
  });

  it('clamps unaccounted to zero when sum exceeds duration', () => {
    const r = computeStageGroups(ev({ auth_ms: 1000, duration_ms: 100 }));
    expect(r.unaccounted).toBe(0);
  });

  it('handles warm pool (dns and connect both undefined)', () => {
    const r = computeStageGroups(
      ev({
        bulkhead_wait_ms: 2,
        upstream_ttfb_ms: 800,
        upstream_body_ms: 200,
        duration_ms: 1010,
      }),
    );
    expect(r.wait).toBe(2);
    // upstream = 0 (connect_ms undefined) + (800 - 2 - 0 - 0) = 798
    expect(r.upstream).toBe(798);
    expect(r.unaccounted).toBe(10);
  });

  it('derives setup_overhead as proxy_setup_ms minus auth+route+limit_reserve', () => {
    expect(
      deriveSetupOverhead(
        ev({
          proxy_setup_ms: 235,
          auth_ms: 0,
          route_ms: 0,
          limit_reserve_ms: 2,
        }),
      ),
    ).toBe(233);
  });

  it('clamps setup_overhead to zero when named sub-stages exceed proxy_setup_ms', () => {
    expect(
      deriveSetupOverhead(
        ev({
          proxy_setup_ms: 10,
          auth_ms: 40,
          route_ms: 0,
          limit_reserve_ms: 0,
        }),
      ),
    ).toBe(0);
  });

  it('returns zero setup_overhead when proxy_setup_ms is undefined', () => {
    expect(deriveSetupOverhead(ev({ auth_ms: 100 }))).toBe(0);
  });

  it('folds setup_overhead into internalPre and reduces unaccounted', () => {
    const r = computeStageGroups(
      ev({
        auth_ms: 0,
        route_ms: 0,
        limit_reserve_ms: 2,
        shape_ms: 0,
        sign_ms: 0,
        proxy_setup_ms: 235,
        upstream_ttfb_ms: 1000,
        upstream_body_ms: 500,
        duration_ms: 1780,
      }),
    );
    expect(r.setupOverhead).toBe(235 - 2);
    expect(r.internalPre).toBe(235);
    expect(r.unaccounted).toBe(1780 - 235 - 1000 - 500);
  });

  it('preserves the proxy setup total while exposing measured setup stages', () => {
    const event = ev({
      proxy_setup_ms: 20,
      auth_ms: 2,
      route_ms: 1,
      limit_reserve_ms: 1,
      json_parse_ms: 0.125,
      cache_structure_ms: 1,
      cache_token_key_ms: 0.25,
      cache_count_lookup_ms: 2,
      cache_tokenizer_queue_ms: 0.5,
      cache_serialize_ms: 3,
      cache_tokenize_ms: 4,
      prepare_signer_ms: 1.125,
      duration_ms: 100,
    });

    expect(hasSetupTimingBreakdown(event)).toBe(true);
    expect(setupTimingTotal(event)).toBe(12);
    expect(deriveOtherSetup(event)).toBe(4);
    expect(deriveSetupOverhead(event)).toBe(16);
    expect(computeStageGroups(event).internalPre).toBe(20);
  });

  it('distinguishes a measured zero from a missing setup timing', () => {
    const measuredZero = ev({
      proxy_setup_ms: 5,
      cache_tokenize_ms: 0,
    });
    const legacy = ev({ proxy_setup_ms: 5 });

    expect(hasSetupTimingBreakdown(measuredZero)).toBe(true);
    expect(hasSetupTimingBreakdown(legacy)).toBe(false);
    expect(setupTimingTotal(measuredZero)).toBe(0);
    expect(deriveOtherSetup(measuredZero)).toBe(5);
    expect(deriveSetupOverhead(legacy)).toBe(5);
  });

  it('clamps Other setup when measured stages exceed proxy setup', () => {
    const event = ev({
      proxy_setup_ms: 2,
      auth_ms: 1,
      json_parse_ms: 4,
    });
    expect(deriveOtherSetup(event)).toBe(0);
    expect(deriveSetupOverhead(event)).toBe(1);
  });

  it('does not derive Other setup for an in-flight partial row', () => {
    const partial = {
      event_id: 'evt-partial',
      request_id: 'req-partial',
      ts: 1,
      ts_ms: 1000,
      last_update_ms: 1001,
      elapsed_ms: 1,
      stream: false,
      cache_structure_ms: 0.25,
      _phase: 'partial',
    } satisfies RequestEventWithPhase;

    expect(hasSetupTimingBreakdown(partial)).toBe(true);
    expect(setupTimingTotal(partial)).toBe(0.25);
    expect(deriveOtherSetup(partial)).toBe(0);
    expect(computeStageGroups(partial).internalPre).toBe(0);
  });

  it('accounts for a complete proxy request with a sub-10ms residual', () => {
    const event = ev({
      source_kind: 'proxy',
      duration_ms: 1500,
      request_body_read_ms: 100,
      request_body_bytes: 854336,
      proxy_setup_ms: 200,
      shape_ms: 10,
      sign_ms: 5,
      upstream_ttfb_ms: 300,
      upstream_body_ms: 875,
      finalize_ms: 5,
    });

    const groups = computeStageGroups(event);
    expect(groups.internalPre).toBe(315);
    expect(groups.upstream).toBe(300);
    expect(groups.body).toBe(875);
    expect(groups.internalPost).toBe(5);
    expect(groups.accounted).toBe(1495);
    expect(groups.rawResidual).toBe(5);
    expect(groups.unaccounted).toBe(5);
    expect(event.request_body_bytes).toBe(854336);
  });

  it('counts a completed stream body once when both body timings are present', () => {
    const groups = computeStageGroups(
      ev({
        source_kind: 'proxy',
        duration_ms: 1000,
        request_body_read_ms: 100,
        proxy_setup_ms: 100,
        upstream_ttfb_ms: 100,
        stream_total_ms: 600,
        upstream_body_ms: 600,
        finalize_ms: 100,
      }),
    );

    expect(groups.body).toBe(600);
    expect(groups.accounted).toBe(1000);
    expect(groups.rawResidual).toBe(0);
  });

  it('uses the partial stream elapsed time once for a cancelled 499', () => {
    const groups = computeStageGroups(
      ev({
        source_kind: 'proxy',
        status: 499,
        duration_ms: 1000,
        request_body_read_ms: 50,
        proxy_setup_ms: 100,
        upstream_ttfb_ms: 200,
        upstream_body_ms: 600,
        stream_total_ms: 900,
        finalize_ms: 40,
      }),
    );

    expect(groups.body).toBe(600);
    expect(groups.accounted).toBe(990);
    expect(groups.rawResidual).toBe(10);
    expect(groups.unaccounted).toBe(10);
  });

  it('uses one source-specific cycle for renewal events', () => {
    const groups = computeStageGroups(
      ev({
        source_kind: 'renewal',
        duration_ms: 500,
      }),
    );

    expect(groups).toMatchObject({
      internalPre: 0,
      wait: 0,
      upstream: 0,
      body: 0,
      internalPost: 0,
      accounted: 500,
      rawResidual: 0,
      unaccounted: 0,
    });
  });

  it('counts Finalize as the parent without adding Limit reconcile again', () => {
    const groups = computeStageGroups(
      ev({
        source_kind: 'proxy',
        duration_ms: 100,
        finalize_ms: 20,
        limit_reconcile_ms: 5,
      }),
    );

    expect(groups.internalPost).toBe(20);
    expect(groups.accounted).toBe(20);
    expect(groups.rawResidual).toBe(80);
  });

  it('exposes signed over-accounting instead of hiding it behind the clamp', () => {
    const groups = computeStageGroups(
      ev({
        source_kind: 'proxy',
        duration_ms: 100,
        request_body_read_ms: 40,
        proxy_setup_ms: 40,
        upstream_ttfb_ms: 40,
        upstream_body_ms: 40,
        finalize_ms: 40,
      }),
    );

    expect(groups.accounted).toBe(200);
    expect(groups.rawResidual).toBe(-100);
    expect(groups.unaccounted).toBe(0);
  });

  it('keeps legacy, mixed, null, and measured-zero rows distinct', () => {
    const legacy = computeStageGroups(
      ev({ duration_ms: 100, limit_reconcile_ms: 10 }),
    );
    const mixed = computeStageGroups(
      ev({
        duration_ms: 100,
        request_body_read_ms: 20,
        finalize_ms: undefined,
        limit_reconcile_ms: 10,
      }),
    );
    const explicitNull = computeStageGroups(
      ev({
        duration_ms: 100,
        request_body_read_ms: null,
        finalize_ms: null,
        limit_reconcile_ms: 10,
      }),
    );
    const measuredZero = computeStageGroups(
      ev({
        duration_ms: 100,
        request_body_read_ms: 0,
        finalize_ms: 0,
        limit_reconcile_ms: 10,
      }),
    );

    expect(legacy.internalPost).toBe(10);
    expect(mixed.internalPre).toBe(20);
    expect(mixed.internalPost).toBe(10);
    expect(mixed.accounted).toBe(30);
    expect(mixed.rawResidual).toBe(70);
    expect(explicitNull.internalPost).toBe(10);
    expect(measuredZero.internalPost).toBe(0);
    expect(measuredZero.accounted).toBe(0);
  });
});
