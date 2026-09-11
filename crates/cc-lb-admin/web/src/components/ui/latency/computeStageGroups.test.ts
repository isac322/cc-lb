import { describe, expect, it } from 'vitest';
import type { RequestEvent } from '../../../lib/api';
import type { RequestEventWithPhase } from '../../../lib/RequestEventTypes';
import {
  computeLatencyAttribution,
  computeStageGroups,
  deriveOtherSetup,
  deriveProxyTimelineDuration,
  deriveSetupOverhead,
  hasSetupTimingBreakdown,
  LATENCY_CATEGORY_META,
  responseBodyDuration,
  setupTimingTotal,
} from './computeStageGroups';

function ev(
  overrides: Partial<RequestEvent>,
): RequestEvent & { _phase: 'final' } {
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
      retryOverhead: 0,
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

  it('suppresses partial residuals so elapsed time is not mislabeled as unaccounted', () => {
    const partial = {
      event_id: 'evt-partial-residual',
      request_id: 'req-partial-residual',
      ts: 1,
      ts_ms: 1000,
      last_update_ms: 1500,
      elapsed_ms: 500,
      stream: false,
      auth_ms: 25,
      _phase: 'partial',
    } satisfies RequestEventWithPhase;

    expect(deriveProxyTimelineDuration(partial)).toBe(500);
    expect(computeStageGroups(partial)).toMatchObject({
      accounted: 0,
      rawResidual: 0,
      unaccounted: 0,
    });
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

  it('uses one source-specific cycle for final renewal events', () => {
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

describe('computeLatencyAttribution', () => {
  it('publishes four fixed category anchors in display order', () => {
    expect(Object.keys(LATENCY_CATEGORY_META)).toEqual([
      'downstream_network',
      'cc_lb',
      'upstream_network',
      'upstream_processing',
    ]);
    expect(
      Object.values(LATENCY_CATEGORY_META).map(({ label, fill }) => ({
        label,
        fill,
      })),
    ).toEqual([
      { label: 'Downstream network', fill: '#06b6d4' },
      { label: 'cc-lb processing', fill: '#6366f1' },
      { label: 'Upstream network', fill: '#f59e0b' },
      { label: 'Upstream processing', fill: '#10b981' },
    ]);
  });

  it('separates measured categories, combined waits, retry, and unknown time', () => {
    const attribution = computeLatencyAttribution(
      ev({
        duration_ms: 1000,
        request_body_read_ms: 100,
        request_body_wait_ms: 60,
        request_body_process_ms: 20,
        proxy_setup_ms: 100,
        auth_ms: 30,
        route_ms: 20,
        limit_reserve_ms: 10,
        shape_ms: 10,
        sign_ms: 5,
        bulkhead_wait_ms: 20,
        dns_ms: 30,
        connect_ms: 50,
        upstream_ttfb_ms: 300,
        upstream_body_ms: 400,
        stream_total_ms: 400,
        response_body_wait_ms: 250,
        response_body_process_ms: 40,
        response_body_downstream_poll_gap_ms: 30,
        finalize_ms: 20,
        limit_reconcile_ms: 5,
        retry_overhead_ms: 50,
      }),
    );

    expect(attribution.categories.map(({ key, ms }) => ({ key, ms }))).toEqual([
      { key: 'downstream_network', ms: 90 },
      { key: 'cc_lb', ms: 215 },
      { key: 'upstream_network', ms: 80 },
      { key: 'upstream_processing', ms: null },
    ]);
    expect(attribution.mixedUpstreamMs).toBe(450);
    expect(attribution.mixedRetryMs).toBe(50);
    expect(attribution.accountedMs).toBe(885);
    expect(attribution.unattributedMs).toBe(115);
    expect(attribution.hasIoTimings).toBe(true);
    expect(attribution.accountingWarning).toBe(false);
  });

  it('keeps legacy ingress unattributed and legacy response explicitly mixed', () => {
    const attribution = computeLatencyAttribution(
      ev({
        duration_ms: 500,
        request_body_read_ms: 100,
        proxy_setup_ms: 50,
        upstream_ttfb_ms: 100,
        upstream_body_ms: 200,
        finalize_ms: 25,
      }),
    );

    expect(attribution.categories.map(({ ms }) => ms)).toEqual([
      null,
      75,
      null,
      null,
    ]);
    expect(attribution.mixedUpstreamMs).toBe(300);
    expect(attribution.accountedMs).toBe(375);
    expect(attribution.unattributedMs).toBe(125);
    expect(attribution.hasIoTimings).toBe(false);
    expect(attribution.categories[3]?.note).toContain(
      'Legacy response-body time combines',
    );
  });

  it('distinguishes a measured zero from missing category data', () => {
    const attribution = computeLatencyAttribution(
      ev({
        duration_ms: 0,
        request_body_wait_ms: 0,
      }),
    );

    expect(attribution.categories.map(({ ms }) => ms)).toEqual([
      0,
      null,
      null,
      null,
    ]);
    expect(attribution.mixedUpstreamMs).toBeNull();
    expect(attribution.mixedRetryMs).toBeNull();
    expect(attribution.hasIoTimings).toBe(true);
    expect(attribution.accountingWarning).toBe(false);
  });

  it('ignores negative and non-finite measurements instead of manufacturing zeroes', () => {
    const attribution = computeLatencyAttribution(
      ev({
        duration_ms: Number.NaN,
        request_body_wait_ms: -1,
        request_body_process_ms: Number.POSITIVE_INFINITY,
        response_body_wait_ms: Number.NEGATIVE_INFINITY,
        retry_overhead_ms: Number.NaN,
        proxy_setup_ms: Number.POSITIVE_INFINITY,
        dns_ms: -2,
        request_body_chunk_count: 1.5,
      }),
    );

    expect(attribution.timelineTotalMs).toBe(0);
    expect(attribution.categories.map(({ ms }) => ms)).toEqual([
      null,
      null,
      null,
      null,
    ]);
    expect(attribution.mixedUpstreamMs).toBeNull();
    expect(attribution.mixedRetryMs).toBeNull();
    expect(attribution.hasIoTimings).toBe(false);
    expect(attribution.accountedMs).toBe(0);
    expect(attribution.unattributedMs).toBe(0);
    expect(attribution.accountingWarning).toBe(false);
  });

  it('keeps invalid loose partial timing fields unmeasured', () => {
    const partial = {
      event_id: 'evt-partial-invalid-timing',
      request_id: 'req-partial-invalid-timing',
      ts: 1,
      ts_ms: 1000,
      elapsed_ms: 40,
      stream: true,
      proxy_setup_ms: '12',
      limit_reconcile_ms: { milliseconds: 3 },
      _phase: 'partial',
    } satisfies RequestEventWithPhase;

    const attribution = computeLatencyAttribution(partial);

    expect(attribution.categories.map(({ ms }) => ms)).toEqual([
      null,
      null,
      null,
      null,
    ]);
    expect(attribution.accountedMs).toBe(0);
    expect(attribution.unattributedMs).toBe(40);
    expect(attribution.accountingWarning).toBe(false);
  });

  it('renders known partial measurements without treating missing fields as zero', () => {
    const partial = {
      event_id: 'evt-partial-attribution',
      request_id: 'req-partial-attribution',
      ts: 1,
      ts_ms: 1000,
      last_update_ms: 1100,
      elapsed_ms: 100,
      stream: true,
      request_body_wait_ms: 20,
      request_body_process_ms: 5,
      response_body_wait_ms: 10,
      cache_structure_ms: 0,
      _phase: 'partial',
    } satisfies RequestEventWithPhase;

    const attribution = computeLatencyAttribution(partial);
    expect(attribution.categories.map(({ ms }) => ms)).toEqual([
      20,
      5,
      null,
      null,
    ]);
    expect(attribution.mixedUpstreamMs).toBe(10);
    expect(attribution.accountedMs).toBe(35);
    expect(attribution.unattributedMs).toBe(65);
    expect(attribution.timelineTotalMs).toBe(100);
    expect(attribution.hasIoTimings).toBe(true);
  });

  it('treats real sub-millisecond child overruns from u64 parents as rounding, not corruption', () => {
    const attribution = computeLatencyAttribution(
      ev({
        source_kind: 'proxy',
        duration_ms: 7,
        request_body_read_ms: 0,
        request_body_first_chunk_ms: 0.005875,
        request_body_receive_ms: 0.193167,
        request_body_wait_ms: 0.120708,
        request_body_process_ms: 0.078334,
        request_body_chunk_count: 5,
        proxy_setup_ms: 5,
        auth_ms: 0,
        route_ms: 0,
        limit_reserve_ms: 0,
        json_parse_ms: 0.629958,
        cache_structure_ms: 3.634917,
        cache_token_key_ms: 0.001958,
        cache_count_lookup_ms: 0,
        cache_tokenizer_queue_ms: 0.001458,
        cache_serialize_ms: 0.0005,
        cache_tokenize_ms: 0,
        prepare_signer_ms: 0.011208,
        bulkhead_wait_ms: 0,
        dns_ms: 0,
        connect_ms: 0,
        upstream_ttfb_ms: 1,
        upstream_body_ms: 0,
        response_body_wait_ms: 0.003541,
        response_body_process_ms: 0.002042,
        response_body_downstream_poll_gap_ms: null,
        finalize_ms: 0,
      }),
    );

    expect(attribution.categories[0]?.ms).toBe(0.120708);
    expect(attribution.categories[1]?.ms).toBeCloseTo(5.080376);
    expect(attribution.categories[2]?.ms).toBe(0);
    expect(attribution.categories[3]?.ms).toBeNull();
    expect(attribution.mixedUpstreamMs).toBeCloseTo(1.003541);
    expect(attribution.accountedMs).toBeCloseTo(6.204625);
    expect(attribution.unattributedMs).toBeCloseTo(0.795375);
    expect(attribution.accountingWarning).toBe(false);
  });

  it('warns at the exact one-millisecond limit that truncation cannot explain', () => {
    const roundingOnly = computeLatencyAttribution(
      ev({
        duration_ms: 0,
        request_body_read_ms: 0,
        request_body_wait_ms: 0.02,
        request_body_process_ms: 0.014041,
      }),
    );
    expect(roundingOnly.accountedMs).toBeCloseTo(0.034041);
    expect(roundingOnly.accountingWarning).toBe(false);

    const localParentOverrun = computeLatencyAttribution(
      ev({
        duration_ms: 10,
        request_body_read_ms: 0,
        request_body_wait_ms: 0.6,
        request_body_process_ms: 0.4,
      }),
    );
    const globalTotalOverrun = computeLatencyAttribution(
      ev({
        duration_ms: 0,
        request_body_wait_ms: 1,
      }),
    );

    expect(localParentOverrun.accountedMs).toBe(1);
    expect(localParentOverrun.accountingWarning).toBe(true);
    expect(globalTotalOverrun.accountedMs).toBe(1);
    expect(globalTotalOverrun.accountingWarning).toBe(true);
  });

  it('preserves raw child values and warns when they overrun measured parents', () => {
    const attribution = computeLatencyAttribution(
      ev({
        duration_ms: 100,
        request_body_read_ms: 10,
        request_body_wait_ms: 8,
        request_body_process_ms: 7,
        proxy_setup_ms: 5,
        auth_ms: 10,
        bulkhead_wait_ms: 8,
        dns_ms: 5,
        connect_ms: 5,
        upstream_ttfb_ms: 10,
        upstream_body_ms: 20,
        response_body_wait_ms: 15,
        response_body_process_ms: 10,
        response_body_downstream_poll_gap_ms: 5,
        finalize_ms: 5,
        limit_reconcile_ms: 10,
      }),
    );

    expect(attribution.categories.map(({ ms }) => ms)).toEqual([
      13,
      35,
      10,
      null,
    ]);
    expect(attribution.mixedUpstreamMs).toBe(15);
    expect(attribution.accountedMs).toBe(73);
    expect(attribution.unattributedMs).toBe(27);
    expect(attribution.accountingWarning).toBe(true);
  });

  it('keeps response children diagnostic and counts retry overhead once in parent chronology', () => {
    const parentOnly = ev({
      source_kind: 'proxy',
      duration_ms: 900,
      request_body_read_ms: 100,
      proxy_setup_ms: 100,
      upstream_ttfb_ms: 200,
      upstream_body_ms: 400,
      finalize_ms: 50,
      retry_overhead_ms: 50,
    });
    const withChildren = ev({
      ...parentOnly,
      request_body_first_chunk_ms: 80,
      request_body_receive_ms: 90,
      request_body_wait_ms: 80,
      request_body_process_ms: 20,
      request_body_chunk_count: 4,
      response_body_wait_ms: 300,
      response_body_process_ms: 50,
      response_body_downstream_poll_gap_ms: 50,
    });

    expect(computeStageGroups(withChildren)).toEqual(
      computeStageGroups(parentOnly),
    );
    expect(computeStageGroups(withChildren)).toMatchObject({
      internalPre: 200,
      upstream: 200,
      body: 400,
      internalPost: 50,
      retryOverhead: 50,
      accounted: 900,
      rawResidual: 0,
    });
  });

  it('keeps overlapping ingress markers out of category accounting', () => {
    const attribution = computeLatencyAttribution(
      ev({
        duration_ms: 100,
        request_body_read_ms: 50,
        request_body_first_chunk_ms: 30,
        request_body_receive_ms: 45,
        request_body_chunk_count: 2,
      }),
    );

    expect(attribution.categories.map(({ ms }) => ms)).toEqual([
      null,
      null,
      null,
      null,
    ]);
    expect(attribution.accountedMs).toBe(0);
    expect(attribution.unattributedMs).toBe(100);
    expect(attribution.hasIoTimings).toBe(true);
    expect(attribution.accountingWarning).toBe(false);
  });

  it('falls back to a valid response parent when stream total is invalid', () => {
    const event = ev({
      duration_ms: 50,
      stream_total_ms: -1,
      upstream_body_ms: 20,
    });

    expect(responseBodyDuration(event)).toBe(20);
    expect(computeLatencyAttribution(event).mixedUpstreamMs).toBe(20);
  });

  it('uses the cancelled response parent and keeps provider processing unknown', () => {
    const attribution = computeLatencyAttribution(
      ev({
        status: 499,
        duration_ms: 1000,
        request_body_read_ms: 50,
        proxy_setup_ms: 100,
        upstream_ttfb_ms: 200,
        upstream_body_ms: 600,
        stream_total_ms: 900,
        finalize_ms: 40,
        retry_overhead_ms: 10,
      }),
    );

    expect(attribution.mixedUpstreamMs).toBe(800);
    expect(attribution.categories[3]?.ms).toBeNull();
    expect(attribution.mixedRetryMs).toBe(10);
    expect(attribution.accountedMs).toBe(950);
    expect(attribution.unattributedMs).toBe(50);
  });

  it('does not classify renewal duration as proxy latency', () => {
    const attribution = computeLatencyAttribution(
      ev({
        source_kind: 'renewal',
        duration_ms: 500,
      }),
    );

    expect(attribution.categories.every(({ ms }) => ms == null)).toBe(true);
    expect(attribution.mixedUpstreamMs).toBeNull();
    expect(attribution.mixedRetryMs).toBeNull();
    expect(attribution.accountedMs).toBe(0);
    expect(attribution.unattributedMs).toBe(500);
    expect(attribution.timelineTotalMs).toBe(500);
  });

  it('keeps accounting equal to category and mixed contributions', () => {
    const events = [
      ev({ duration_ms: 100 }),
      ev({
        duration_ms: 100,
        request_body_wait_ms: 0,
        response_body_process_ms: 5,
      }),
      ev({
        duration_ms: 100,
        dns_ms: 10,
        connect_ms: 20,
        upstream_ttfb_ms: 50,
        retry_overhead_ms: 7,
      }),
    ];

    for (const event of events) {
      const attribution = computeLatencyAttribution(event);
      const contributionTotal =
        attribution.categories.reduce(
          (total, category) => total + (category.ms ?? 0),
          0,
        ) +
        (attribution.mixedUpstreamMs ?? 0) +
        (attribution.mixedRetryMs ?? 0);
      expect(attribution.accountedMs).toBe(contributionTotal);
      expect(attribution.unattributedMs).toBe(
        Math.max(0, attribution.timelineTotalMs - contributionTotal),
      );
    }
  });
});
