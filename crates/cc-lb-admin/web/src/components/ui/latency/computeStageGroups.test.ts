import { describe, expect, it } from 'vitest';
import type { RequestEvent } from '../../../lib/api';
import type { RequestEventWithPhase } from '../../../lib/RequestEventTypes';
import { computeStageGroups, deriveSetupOverhead } from './computeStageGroups';

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

  it('reports body and internalPost', () => {
    const r = computeStageGroups(
      ev({
        upstream_body_ms: 200,
        observability_post_ms: 15,
        limit_reconcile_ms: 10,
        duration_ms: 1000,
      }),
    );
    expect(r.body).toBe(200);
    expect(r.internalPost).toBe(25);
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
});
