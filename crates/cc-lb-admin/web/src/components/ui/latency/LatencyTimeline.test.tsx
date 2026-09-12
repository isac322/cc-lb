import {
  cleanup,
  fireEvent,
  render,
  screen,
  within,
} from '@testing-library/react';
import { afterEach, describe, expect, it } from 'vitest';
import type { RequestEvent } from '../../../lib/api';
import type { RequestEventWithPhase } from '../../../lib/RequestEventTypes';
import {
  buildSseMarkers,
  buildStageDetails,
  computeMarkerTracks,
  LatencyTimeline,
} from './LatencyTimeline';

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

describe('buildStageDetails', () => {
  it('assigns index and groupCount so stages within a group can be shaded distinctly', () => {
    const stages = buildStageDetails(
      ev({
        duration_ms: 100,
        auth_ms: 2,
        route_ms: 3,
        limit_reserve_ms: 4,
        shape_ms: 5,
        sign_ms: 6,
      }),
    );
    const internalPre = stages.filter((s) => s.group === 'internal_pre');
    expect(internalPre.map((s) => s.key)).toEqual([
      'auth',
      'route',
      'limit_reserve',
      'shape',
      'sign',
    ]);
    expect(internalPre.map((s) => s.index)).toEqual([0, 1, 2, 3, 4]);
    expect(internalPre.map((s) => s.groupCount)).toEqual([5, 5, 5, 5, 5]);
    const uniqueFills = new Set(internalPre.map((s) => s.fill));
    expect(uniqueFills.size).toBe(5);
  });

  it('drops fields that are zero or undefined', () => {
    const stages = buildStageDetails(
      ev({
        duration_ms: 100,
        auth_ms: 4,
        route_ms: 0,
        shape_ms: 10,
      }),
    );
    expect(stages.map((s) => s.key)).toEqual(['auth', 'shape']);
  });

  it('derives upstream_wait from upstream_ttfb minus queue/dns/connect', () => {
    const stages = buildStageDetails(
      ev({
        duration_ms: 5000,
        bulkhead_wait_ms: 20,
        dns_ms: 30,
        connect_ms: 100,
        upstream_ttfb_ms: 500,
      }),
    );
    const uw = stages.find((s) => s.key === 'upstream_wait');
    expect(uw?.ms).toBe(500 - 20 - 30 - 100);
  });

  it('picks stream_relay for streaming events and body_collect for non-stream', () => {
    const streaming = buildStageDetails(
      ev({
        duration_ms: 1000,
        sse_event_count: 5,
        stream_total_ms: 900,
        upstream_body_ms: 400,
      }),
    );
    expect(streaming.map((s) => s.key)).toContain('stream_relay');
    expect(streaming.map((s) => s.key)).not.toContain('body_collect');

    const nonStream = buildStageDetails(
      ev({
        duration_ms: 500,
        upstream_body_ms: 350,
      }),
    );
    expect(nonStream.map((s) => s.key)).toContain('body_collect');
    expect(nonStream.map((s) => s.key)).not.toContain('stream_relay');
  });

  it('inserts setup_overhead between limit_reserve and shape when proxy_setup_ms is set', () => {
    const stages = buildStageDetails(
      ev({
        duration_ms: 1000,
        auth_ms: 0,
        route_ms: 0,
        limit_reserve_ms: 2,
        proxy_setup_ms: 235,
        shape_ms: 4,
        sign_ms: 3,
      }),
    );
    const internalPre = stages.filter((s) => s.group === 'internal_pre');
    expect(internalPre.map((s) => s.key)).toEqual([
      'limit_reserve',
      'setup_overhead',
      'shape',
      'sign',
    ]);
    const overhead = internalPre.find((s) => s.key === 'setup_overhead')!;
    expect(overhead.ms).toBe(235 - 2);
  });

  it('omits setup_overhead when proxy_setup_ms is undefined', () => {
    const stages = buildStageDetails(ev({ duration_ms: 500, auth_ms: 10 }));
    expect(stages.map((s) => s.key)).not.toContain('setup_overhead');
  });

  it('replaces legacy setup overhead with eight measured stages and Other setup', () => {
    const stages = buildStageDetails(
      ev({
        proxy_setup_ms: 20,
        auth_ms: 2,
        route_ms: 1,
        limit_reserve_ms: 1,
        json_parse_ms: 0.125,
        cache_tokenizer_queue_ms: 0.25,
        cache_structure_ms: 0.5,
        cache_serialize_ms: 1,
        cache_token_key_ms: 1.5,
        cache_count_lookup_ms: 2,
        cache_tokenize_ms: 0,
        prepare_signer_ms: 3,
      }),
    );
    const internalPre = stages.filter(
      (stage) => stage.group === 'internal_pre',
    );

    expect(internalPre.map((stage) => stage.key)).toEqual([
      'json_parse_ms',
      'cache_tokenizer_queue_ms',
      'cache_structure_ms',
      'cache_serialize_ms',
      'cache_token_key_ms',
      'cache_count_lookup_ms',
      'cache_tokenize_ms',
      'auth',
      'route',
      'limit_reserve',
      'prepare_signer_ms',
      'other_setup',
    ]);
    expect(stages.find((stage) => stage.key === 'cache_tokenize_ms')?.ms).toBe(
      0,
    );
    expect(stages.find((stage) => stage.key === 'other_setup')?.ms).toBe(7.625);
    expect(stages.map((stage) => stage.key)).not.toContain('setup_overhead');
  });

  it('keeps completed setup timings on partial rows without inventing missing stages', () => {
    const stages = buildStageDetails({
      event_id: 'evt-partial',
      request_id: 'req-partial',
      ts: 1,
      ts_ms: 1000,
      last_update_ms: 1001,
      elapsed_ms: 1,
      stream: false,
      json_parse_ms: 0,
      cache_structure_ms: 0.25,
      _phase: 'partial',
    });

    expect(stages.map((stage) => stage.key)).toEqual([
      'json_parse_ms',
      'cache_structure_ms',
    ]);
    expect(stages[0]?.ms).toBe(0);
  });

  it('orders request ingress before proxy work and Finalize after the response body', () => {
    const stages = buildStageDetails(
      ev({
        source_kind: 'proxy',
        duration_ms: 100,
        request_body_read_ms: 5,
        request_body_bytes: 854336,
        proxy_setup_ms: 10,
        shape_ms: 2,
        sign_ms: 1,
        upstream_ttfb_ms: 20,
        upstream_body_ms: 50,
        finalize_ms: 10,
        limit_reconcile_ms: 3,
      }),
    );

    expect(stages.map((stage) => stage.key)).toEqual([
      'request_body_read',
      'setup_overhead',
      'shape',
      'sign',
      'upstream_wait',
      'body_collect',
      'finalize',
    ]);
    expect(stages.find((stage) => stage.key === 'request_body_read')?.ms).toBe(
      5,
    );
    expect(stages.find((stage) => stage.key === 'finalize')?.ms).toBe(10);
    expect(stages.map((stage) => stage.key)).not.toContain('limit_reconcile');
  });

  it('selects one complete stream body stage when both body fields match', () => {
    const stages = buildStageDetails(
      ev({
        source_kind: 'proxy',
        duration_ms: 1000,
        stream_total_ms: 600,
        upstream_body_ms: 600,
      }),
    );

    expect(
      stages.filter((stage) =>
        ['stream_relay', 'body_collect', 'partial_stream'].includes(stage.key),
      ),
    ).toMatchObject([{ key: 'stream_relay', ms: 600 }]);
  });

  it('labels a cancelled 499 body as a partial stream', () => {
    const stages = buildStageDetails(
      ev({
        source_kind: 'proxy',
        status: 499,
        duration_ms: 700,
        upstream_body_ms: 640,
        stream_total_ms: 690,
      }),
    );

    expect(
      stages.filter((stage) =>
        ['stream_relay', 'body_collect', 'partial_stream'].includes(stage.key),
      ),
    ).toMatchObject([
      {
        key: 'partial_stream',
        label: 'Partial stream (client cancelled)',
        ms: 640,
      },
    ]);
  });

  it('builds only one Renewal cycle for final renewal events', () => {
    const stages = buildStageDetails(
      ev({
        source_kind: 'renewal',
        duration_ms: 500,
      }),
    );

    expect(stages).toMatchObject([
      { key: 'renewal_cycle', label: 'Renewal cycle', ms: 500 },
    ]);
  });

  it('keeps measured-zero renewal distinct from a missing duration', () => {
    const measuredZero = buildStageDetails(
      ev({
        source_kind: 'renewal',
        duration_ms: 0,
      }),
    );
    const missing = buildStageDetails(
      ev({
        source_kind: 'renewal',
        duration_ms: undefined,
      }),
    );

    expect(measuredZero).toMatchObject([
      { key: 'renewal_cycle', label: 'Renewal cycle', ms: 0 },
    ]);
    expect(missing).toEqual([]);
  });

  it('shows measured zero for new parent stages but omits missing and null', () => {
    const measuredZero = buildStageDetails(
      ev({
        source_kind: 'proxy',
        duration_ms: 100,
        request_body_read_ms: 0,
        finalize_ms: 0,
      }),
    );
    const missing = buildStageDetails(
      ev({
        source_kind: 'proxy',
        duration_ms: 100,
      }),
    );
    const explicitNull = buildStageDetails(
      ev({
        source_kind: 'proxy',
        duration_ms: 100,
        request_body_read_ms: null,
        finalize_ms: null,
      }),
    );
    const mixed = buildStageDetails(
      ev({
        source_kind: 'proxy',
        duration_ms: 100,
        request_body_read_ms: 20,
        finalize_ms: undefined,
        limit_reconcile_ms: 10,
      }),
    );

    expect(measuredZero.map((stage) => stage.key)).toEqual([
      'request_body_read',
      'finalize',
    ]);
    expect(missing).toEqual([]);
    expect(explicitNull).toEqual([]);
    expect(mixed.map((stage) => stage.key)).toEqual([
      'request_body_read',
      'limit_reconcile',
    ]);
    expect(mixed.find((stage) => stage.key === 'limit_reconcile')?.ms).toBe(10);
  });
});

describe('buildSseMarkers', () => {
  it('shifts SSE offsets by internalPre + upstream_ttfb to place markers on the request axis', () => {
    const markers = buildSseMarkers(
      ev({
        duration_ms: 2000,
        auth_ms: 10,
        route_ms: 5,
        upstream_ttfb_ms: 500,
        stream_first_content_delta_ms: 20,
        stream_last_chunk_ms: 1000,
      }),
    );
    const first = markers.find((m) => m.key === 'first_delta')!;
    expect(first.relayMs).toBe(20);
    expect(first.absMs).toBe(515 + 20);
    const last = markers.find((m) => m.key === 'last_chunk')!;
    expect(last.absMs).toBe(515 + 1000);
  });

  it('folds setup_overhead into relayStart so SSE markers stay aligned with the timeline', () => {
    const markers = buildSseMarkers(
      ev({
        duration_ms: 2000,
        auth_ms: 0,
        route_ms: 0,
        limit_reserve_ms: 2,
        proxy_setup_ms: 235,
        shape_ms: 0,
        sign_ms: 0,
        upstream_ttfb_ms: 500,
        stream_first_content_delta_ms: 20,
      }),
    );
    const first = markers.find((m) => m.key === 'first_delta')!;
    expect(first.absMs).toBe(2 + (235 - 2) + 500 + 20);
  });

  it('keeps SSE marker offsets anchored to proxy_setup_ms for new rows', () => {
    const markers = buildSseMarkers(
      ev({
        proxy_setup_ms: 20,
        auth_ms: 2,
        route_ms: 1,
        limit_reserve_ms: 1,
        json_parse_ms: 4,
        cache_structure_ms: 20,
        upstream_ttfb_ms: 500,
        stream_first_content_delta_ms: 20,
      }),
    );
    const first = markers.find((marker) => marker.key === 'first_delta');
    expect(first?.absMs).toBe(20 + 500 + 20);
  });
});

describe('computeMarkerTracks', () => {
  it('assigns markers within minSpacePct to different tracks', () => {
    const total = 4520;
    const markers = buildSseMarkers(
      ev({
        duration_ms: total,
        auth_ms: 4,
        route_ms: 2,
        limit_reserve_ms: 3,
        shape_ms: 10,
        sign_ms: 3,
        upstream_ttfb_ms: 780,
        stream_message_start_ms: 5,
        stream_content_block_start_ms: 40,
        stream_first_content_delta_ms: 55,
        stream_last_content_delta_ms: 3600,
        stream_message_stop_ms: 3650,
        stream_last_chunk_ms: 3700,
      }),
    );
    const staggered = computeMarkerTracks(markers, total, 6);
    const byKey = Object.fromEntries(staggered.map((m) => [m.key, m.track]));
    expect(byKey.message_start).toBe(0);
    expect(byKey.content_block_start).toBe(1);
    expect(byKey.first_delta).toBe(2);
    expect(byKey.last_delta).toBe(0);
    expect(byKey.message_stop).toBe(1);
    expect(byKey.last_chunk).toBe(2);
  });

  it('reuses lower tracks once markers are far enough apart', () => {
    const total = 1000;
    const markers: Parameters<typeof computeMarkerTracks>[0] = [
      {
        key: 'a',
        label: 'A',
        relayMs: 0,
        absMs: 0,
        color: 'text-white',
      },
      {
        key: 'b',
        label: 'B',
        relayMs: 0,
        absMs: 100,
        color: 'text-white',
      },
    ];
    const staggered = computeMarkerTracks(markers, total, 6);
    expect(staggered.map((m) => m.track)).toEqual([0, 0]);
  });
});

describe('LatencyTimeline', () => {
  afterEach(() => {
    cleanup();
  });

  it('renders group rows for a streaming event', () => {
    const event = ev({
      duration_ms: 4520,
      auth_ms: 4,
      route_ms: 2,
      limit_reserve_ms: 3,
      shape_ms: 10,
      sign_ms: 3,
      bulkhead_wait_ms: 2,
      upstream_ttfb_ms: 780,
      stream_message_start_ms: 5,
      stream_content_block_start_ms: 40,
      stream_first_content_delta_ms: 55,
      stream_last_content_delta_ms: 3600,
      stream_message_stop_ms: 3650,
      stream_last_chunk_ms: 3700,
      stream_total_ms: 3720,
      sse_event_count: 138,
      content_delta_count: 128,
      ping_count: 6,
      inter_token_avg_ms: 28,
      observability_post_ms: 6,
      limit_reconcile_ms: 4,
    });
    render(<LatencyTimeline event={event} />);
    expect(screen.getByText('Internal pre')).toBeTruthy();
    expect(screen.getByText('Wait')).toBeTruthy();
    expect(screen.getByText('Upstream')).toBeTruthy();
    expect(screen.getByText('Body')).toBeTruthy();
    expect(screen.getByText('Internal post')).toBeTruthy();
    expect(screen.getByText('SSE markers')).toBeTruthy();
    expect(screen.queryByText('Observability post')).toBeNull();
    const stageDetails = screen
      .getByText(/^Stage details \(\d+\)$/)
      .closest('details');
    expect(stageDetails).not.toBeNull();
    expect(
      within(stageDetails!).getByRole('button', {
        name: /Limit reconcile.*Internal post.*4 ms/,
      }),
    ).toBeTruthy();
  });

  it('shows the same responsibility totals as the compact popover', async () => {
    render(
      <LatencyTimeline
        event={ev({
          source_kind: 'proxy',
          duration_ms: 1000,
          request_body_read_ms: 100,
          request_body_wait_ms: 30,
          request_body_process_ms: 20,
          proxy_setup_ms: 200,
          auth_ms: 50,
          route_ms: 20,
          limit_reserve_ms: 10,
          json_parse_ms: 5,
          cache_tokenizer_queue_ms: 5,
          cache_structure_ms: 5,
          cache_serialize_ms: 5,
          retry_overhead_ms: 75,
          shape_ms: 25,
          sign_ms: 10,
          upstream_ttfb_ms: 300,
          bulkhead_wait_ms: 10,
          dns_ms: 20,
          connect_ms: 30,
          stream_total_ms: 240,
          response_body_wait_ms: 100,
          response_body_process_ms: 20,
          response_body_downstream_poll_gap_ms: 10,
          finalize_ms: 50,
          limit_reconcile_ms: 20,
        })}
      />,
    );

    const timeline = screen.getByTestId('latency-timeline-region');
    expect(timeline.getAttribute('data-downstream-ms')).toBe('40');
    expect(timeline.getAttribute('data-cc-lb-ms')).toBe('335');
    expect(timeline.getAttribute('data-upstream-net-ms')).toBe('50');
    expect(timeline.getAttribute('data-upstream-wait-ms')).toBe('340');
    expect(timeline.getAttribute('data-unattributed-ms')).toBe('235');
    expect(
      screen.getByRole('region', { name: 'Latency by responsibility' }),
    ).toBeTruthy();
    expect(
      screen.getByRole('button', {
        name: 'Downstream, 40 ms, 4% of total',
      }),
    ).toBeTruthy();
    expect(
      screen.getByRole('button', {
        name: 'cc-lb, 335 ms, 34% of total',
      }),
    ).toBeTruthy();
    expect(
      screen.getByRole('button', {
        name: 'Upstream net, 50 ms, 5% of total',
      }),
    ).toBeTruthy();
    expect(
      screen.getByRole('button', {
        name: 'Upstream wait, 340 ms, 34% of total',
      }),
    ).toBeTruthy();
    expect(
      screen.getByRole('button', {
        name: 'Unattributed, 235 ms, 24% of total',
      }),
    ).toBeTruthy();

    fireEvent.click(
      screen.getByRole('button', {
        name: 'Upstream wait, 340 ms, 34% of total',
      }),
    );
    expect(
      await screen.findByText(
        /provider generation, upstream transit, and runtime scheduling/i,
      ),
    ).toBeTruthy();
  });

  it('keeps every responsibility visible when recorded timings exceed total', () => {
    render(
      <LatencyTimeline
        event={ev({
          source_kind: 'proxy',
          duration_ms: 100,
          request_body_read_ms: 40,
          proxy_setup_ms: 40,
          upstream_ttfb_ms: 40,
          upstream_body_ms: 40,
          finalize_ms: 40,
        })}
      />,
    );

    const distribution = screen.getByRole('img', {
      name: 'Responsibility distribution',
    });
    const segments = Array.from(
      distribution.querySelectorAll<HTMLElement>('[data-responsibility]'),
    );
    expect(
      segments.map((segment) => [
        segment.getAttribute('data-responsibility'),
        segment.style.width,
      ]),
    ).toEqual([
      ['cc-lb', '40%'],
      ['upstream-wait', '20%'],
      ['unattributed', '40%'],
    ]);
    expect(
      segments.reduce(
        (total, segment) => total + Number.parseFloat(segment.style.width),
        0,
      ),
    ).toBe(100);
  });

  it('leaves unobserved elapsed time empty on live partial rows', () => {
    const partial = {
      event_id: 'evt-partial-responsibility',
      request_id: 'req-partial-responsibility',
      ts: 1,
      ts_ms: 1000,
      last_update_ms: 6000,
      elapsed_ms: 5000,
      stream: true,
      request_body_read_ms: 6,
      request_body_wait_ms: 4,
      request_body_process_ms: 2,
      auth_ms: 2,
      route_ms: 1,
      upstream_ttfb_ms: 500,
      _phase: 'partial',
    } satisfies RequestEventWithPhase;
    render(<LatencyTimeline event={partial} isPartial />);

    const distribution = screen.getByRole('img', {
      name: 'Responsibility distribution',
    });
    const segments = Array.from(
      distribution.querySelectorAll<HTMLElement>('[data-responsibility]'),
    );
    expect(
      Object.fromEntries(
        segments.map((segment) => [
          segment.getAttribute('data-responsibility'),
          segment.style.width,
        ]),
      ),
    ).toEqual({
      downstream: '0.08%',
      'cc-lb': '0.1%',
      'upstream-wait': '10%',
    });
    expect(
      segments.reduce(
        (total, segment) => total + Number.parseFloat(segment.style.width),
        0,
      ),
    ).toBeLessThan(11);
    expect(
      screen.getByRole('button', {
        name: 'Upstream wait, 500 ms, 10% of total',
      }),
    ).toBeTruthy();
  });

  it('announces Warm pool when connection timing is absent', () => {
    render(
      <LatencyTimeline
        event={ev({
          source_kind: 'proxy',
          duration_ms: 100,
          connection_reused: true,
          upstream_ttfb_ms: 20,
        })}
      />,
    );

    expect(
      screen.getByRole('button', {
        name: 'Upstream net, warm pool, 0 ms, 0% of total',
      }),
    ).toBeTruthy();
  });

  it('keeps zero and fractional setup details without overlapping tiny segments', () => {
    const event = ev({
      duration_ms: 1000,
      proxy_setup_ms: 20,
      auth_ms: 2,
      route_ms: 1,
      limit_reserve_ms: 1,
      json_parse_ms: 0,
      cache_structure_ms: 0.125,
      prepare_signer_ms: 3,
    });
    render(<LatencyTimeline event={event} />);

    expect(screen.getByText('0.125 ms')).toBeTruthy();
    expect(screen.getByText('0 ms')).toBeTruthy();
    expect(screen.queryByTestId('latency-segment-json_parse_ms')).toBeNull();
    expect(
      screen.queryByTestId('latency-segment-cache_structure_ms'),
    ).toBeNull();
    expect(
      screen.getByTestId('latency-segment-prepare_signer_ms'),
    ).toBeTruthy();
    expect(screen.getByText('20 ms · 2%')).toBeTruthy();
  });

  it('keeps the latency region height reserved while detail hydrates', () => {
    const event = ev({
      duration_ms: 500,
      auth_ms: 4,
      upstream_ttfb_ms: 200,
      upstream_body_ms: 250,
    });
    const { rerender } = render(
      <LatencyTimeline event={event} isLoading={true} />,
    );

    const pendingRegion = screen.getByTestId('latency-timeline-region');
    expect(pendingRegion.className).toContain('min-h-80');
    expect(pendingRegion.getAttribute('aria-busy')).toBe('true');
    expect(pendingRegion.querySelectorAll('.skeleton').length).toBeGreaterThan(
      0,
    );

    rerender(<LatencyTimeline event={event} />);

    const hydratedRegion = screen.getByTestId('latency-timeline-region');
    expect(hydratedRegion).toBe(pendingRegion);
    expect(hydratedRegion.className).toContain('min-h-80');
    expect(hydratedRegion.getAttribute('aria-busy')).toBeNull();
    expect(screen.getByText('Internal pre')).toBeTruthy();
  });

  it('omits the SSE lane for non-stream events', () => {
    const event = ev({
      duration_ms: 500,
      auth_ms: 4,
      upstream_ttfb_ms: 200,
      upstream_body_ms: 250,
    });
    render(<LatencyTimeline event={event} />);
    expect(screen.queryByText('SSE markers')).toBeNull();
  });

  it('renders request ingress metadata before proxy stages and Finalize after Body', () => {
    render(
      <LatencyTimeline
        event={ev({
          source_kind: 'proxy',
          duration_ms: 100,
          request_body_read_ms: 5,
          request_body_bytes: 854336,
          proxy_setup_ms: 10,
          upstream_ttfb_ms: 20,
          upstream_body_ms: 55,
          finalize_ms: 10,
          limit_reconcile_ms: 3,
        })}
      />,
    );

    expect(
      screen
        .getByTestId('latency-segment-request_body_read')
        .getAttribute('aria-label'),
    ).toBe('Request body read 5 ms, Ingress body: 834.3 KB');
    const stageDetails = screen
      .getByText(/^Stage details \(\d+\)$/)
      .closest('details');
    expect(stageDetails).not.toBeNull();
    expect(
      within(stageDetails!).getByRole('button', {
        name: /Request body read.*Ingress body: 834\.3 KB.*5 ms/,
      }),
    ).toBeTruthy();
    const groupRows = screen.getByTestId('latency-stage-groups');
    expect(groupRows).not.toBeNull();
    expect(
      Array.from(
        groupRows!.children,
        (row) => row.firstElementChild?.firstElementChild?.textContent,
      ),
    ).toEqual(['Internal pre', 'Upstream', 'Body', 'Finalize']);
    expect(
      within(stageDetails!).getByRole('button', {
        name: /Finalize.*Finalize.*10 ms/,
      }),
    ).toBeTruthy();

    const internalPreRow = groupRows!.children.item(0) as HTMLElement;
    const requestBodySegment = within(internalPreRow).getByTestId(
      'latency-segment-request_body_read',
    );
    const setupSegment = within(internalPreRow).getByTestId(
      'latency-segment-setup_overhead',
    );
    expect(
      requestBodySegment.compareDocumentPosition(setupSegment) &
        Node.DOCUMENT_POSITION_FOLLOWING,
    ).not.toBe(0);

    const requestBodyDetail = within(stageDetails!).getByRole('button', {
      name: /Request body read.*Ingress body: 834\.3 KB.*5 ms/,
    });
    const setupDetail = within(stageDetails!).getByRole('button', {
      name: /Setup overhead.*Internal pre.*10 ms/,
    });
    expect(
      requestBodyDetail.compareDocumentPosition(setupDetail) &
        Node.DOCUMENT_POSITION_FOLLOWING,
    ).not.toBe(0);
    expect(screen.queryByText('Internal post')).toBeNull();
    expect(
      within(stageDetails!).getByRole('button', {
        name: /Finalize[\s\S]*Limit reconcile: 3 ms[\s\S]*Other finalize: 7 ms[\s\S]*10 ms/,
      }),
    ).toBeTruthy();
  });
  it('keeps mixed ingress rows on the legacy Internal post path', () => {
    render(
      <LatencyTimeline
        event={ev({
          source_kind: 'proxy',
          duration_ms: 100,
          request_body_read_ms: 20,
          limit_reconcile_ms: 10,
        })}
      />,
    );

    expect(screen.getByText('Internal post')).toBeTruthy();
    expect(screen.queryByText('Finalize')).toBeNull();
    const stageDetails = screen
      .getByText(/^Stage details \(\d+\)$/)
      .closest('details');
    expect(stageDetails).not.toBeNull();
    expect(
      within(stageDetails!).getByRole('button', {
        name: /Limit reconcile.*Internal post.*10 ms/,
      }),
    ).toBeTruthy();
  });

  it('renders a cancelled 499 as Partial stream instead of a completed body', () => {
    render(
      <LatencyTimeline
        event={ev({
          source_kind: 'proxy',
          status: 499,
          duration_ms: 700,
          upstream_body_ms: 640,
          stream_total_ms: 690,
          finalize_ms: 60,
        })}
      />,
    );

    expect(screen.getByText('Partial stream (client cancelled)')).toBeTruthy();
    expect(screen.queryByText('Stream relay')).toBeNull();
    expect(screen.queryByText('Body collect')).toBeNull();
  });

  it('keeps a positive residual above the budget visible', () => {
    render(
      <LatencyTimeline
        event={ev({
          source_kind: 'proxy',
          duration_ms: 100,
          request_body_read_ms: 10,
          proxy_setup_ms: 10,
          upstream_ttfb_ms: 20,
          upstream_body_ms: 30,
          finalize_ms: 10,
        })}
      />,
    );

    const timeline = screen.getByTestId('latency-timeline-region');
    expect(timeline.getAttribute('data-raw-residual-ms')).toBe('20');
    expect(
      within(timeline).getByRole('button', { name: 'Unaccounted 20 ms' }),
    ).toBeTruthy();
    expect(
      within(timeline)
        .getAllByText('Unaccounted')
        .some((label) => label.closest('details') === null),
    ).toBe(true);
  });

  it('renders only Renewal cycle for final renewal source rows', () => {
    render(
      <LatencyTimeline
        event={ev({
          source_kind: 'renewal',
          duration_ms: 500,
        })}
      />,
    );

    expect(screen.getAllByText('Renewal cycle').length).toBeGreaterThan(0);
    const renewalResponsibility = screen.getByRole('button', {
      name: 'Renewal cycle, 500 ms, 100% of total',
    });
    expect(
      renewalResponsibility.querySelector('.text-blue-300'),
    ).not.toBeNull();
    expect(screen.queryByText('Internal pre')).toBeNull();
    expect(screen.queryByText('Body')).toBeNull();
    expect(screen.queryByText('Finalize')).toBeNull();
    expect(screen.queryByText('Unaccounted')).toBeNull();
  });

  it('renders partial renewal rows with proxy groups instead of an empty Renewal group', () => {
    const partialRenewal = {
      event_id: 'evt-partial-renewal',
      request_id: 'req-partial-renewal',
      ts: 1,
      ts_ms: 1000,
      last_update_ms: 1100,
      elapsed_ms: 100,
      stream: false,
      source_kind: 'renewal',
      cache_structure_ms: 25,
      _phase: 'partial',
    } satisfies RequestEventWithPhase;

    render(<LatencyTimeline event={partialRenewal} isPartial />);

    expect(screen.getByText('Internal pre')).toBeTruthy();
    expect(screen.getByText('Cache structure')).toBeTruthy();
    expect(
      screen.getByTestId('latency-segment-cache_structure_ms'),
    ).toBeTruthy();
    expect(screen.queryByText('Renewal')).toBeNull();
    expect(screen.queryByText('Renewal cycle')).toBeNull();
    expect(screen.queryByText('No latency data recorded.')).toBeNull();
    expect(screen.queryByText('Unaccounted')).toBeNull();
  });

  it('renders a measured zero Renewal cycle instead of the missing-data hint', () => {
    render(
      <LatencyTimeline
        event={ev({
          source_kind: 'renewal',
          duration_ms: 0,
        })}
      />,
    );

    expect(screen.queryByText('No latency data recorded.')).toBeNull();
    expect(screen.getAllByText('Renewal cycle').length).toBeGreaterThan(0);
    const stageDetails = screen
      .getByText(/^Stage details \(\d+\)$/)
      .closest('details');
    expect(stageDetails).not.toBeNull();
    expect(
      within(stageDetails!).getByRole('button', {
        name: /Renewal cycle.*Renewal.*0 ms/,
      }),
    ).toBeTruthy();
  });

  it('keeps a missing Renewal duration on the no-data path', () => {
    render(
      <LatencyTimeline
        event={ev({
          source_kind: 'renewal',
          duration_ms: undefined,
        })}
      />,
    );

    expect(screen.getByText('No latency data recorded.')).toBeTruthy();
    expect(screen.queryByText('Renewal cycle')).toBeNull();
  });

  it('shows a hint when there is no latency data', () => {
    const event = ev({ duration_ms: 0 });
    render(<LatencyTimeline event={event} />);
    expect(screen.getByText('No latency data recorded.')).toBeTruthy();
  });
});
