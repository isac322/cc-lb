import { cleanup, render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
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

  it('renders four measured-category cards and accounts mixed time once', () => {
    render(
      <LatencyTimeline
        event={ev({
          source_kind: 'proxy',
          duration_ms: 300,
          request_body_read_ms: 30,
          request_body_first_chunk_ms: 15,
          request_body_receive_ms: 20,
          request_body_wait_ms: 20,
          request_body_process_ms: 5,
          request_body_chunk_count: 3,
          request_body_bytes: 100,
          proxy_setup_ms: 10,
          shape_ms: 2,
          sign_ms: 1,
          bulkhead_wait_ms: 7,
          dns_ms: 3,
          connect_ms: 4,
          upstream_ttfb_ms: 50,
          upstream_body_ms: 100,
          response_body_wait_ms: 60,
          response_body_process_ms: 10,
          response_body_downstream_poll_gap_ms: 15,
          retry_overhead_ms: 40,
          finalize_ms: 8,
        })}
      />,
    );

    const summary = screen.getByRole('list', {
      name: 'Latency category summary',
    });
    const cards = within(summary).getAllByRole('listitem');
    expect(cards).toHaveLength(4);
    expect(cards.map((card) => card.getAttribute('data-testid'))).toEqual([
      'latency-category-downstream_network',
      'latency-category-cc_lb',
      'latency-category-upstream_network',
      'latency-category-upstream_processing',
    ]);
    expect(
      screen
        .getByTestId('latency-category-downstream_network')
        .getAttribute('data-category-ms'),
    ).toBe('35');
    expect(
      screen
        .getByTestId('latency-category-cc_lb')
        .getAttribute('data-category-ms'),
    ).toBe('43');
    expect(
      screen
        .getByTestId('latency-category-upstream_network')
        .getAttribute('data-category-ms'),
    ).toBe('7');
    expect(
      screen
        .getByTestId('latency-category-upstream_processing')
        .getAttribute('data-category-ms'),
    ).toBe('unmeasured');

    const mixed = screen.getByTestId('latency-mixed-upstream');
    expect(mixed.getAttribute('data-mixed-upstream-ms')).toBe('96');
    expect(
      screen
        .getByTestId('latency-mixed-retry')
        .getAttribute('data-mixed-retry-ms'),
    ).toBe('40');
    expect(
      screen
        .getByTestId('latency-timeline-region')
        .getAttribute('data-accounted-ms'),
    ).toBe('221');
  });

  it('keeps every ingress and response split visible without adding diagnostics to chronology', () => {
    render(
      <LatencyTimeline
        event={ev({
          source_kind: 'proxy',
          duration_ms: 200,
          request_body_read_ms: 40,
          request_body_first_chunk_ms: 12.5,
          request_body_receive_ms: 27.5,
          request_body_wait_ms: 30,
          request_body_process_ms: 0.0004,
          request_body_chunk_count: 4,
          request_body_bytes: 2048,
          upstream_ttfb_ms: 20,
          upstream_body_ms: 100,
          response_body_wait_ms: 70,
          response_body_process_ms: 20,
          response_body_downstream_poll_gap_ms: 10,
          body_chunk_count: 8,
        })}
      />,
    );

    const expectedMeasured = [
      'request-parent',
      'request-first-chunk',
      'request-receive',
      'request-wait',
      'request-process',
      'request-chunks',
      'request-bytes',
      'response-parent',
      'response-wait',
      'response-process',
      'response-poll-gap',
      'response-chunks',
    ];
    for (const key of expectedMeasured) {
      expect(
        screen
          .getByTestId(`latency-diagnostic-${key}`)
          .getAttribute('data-measured'),
      ).toBe('true');
    }
    expect(
      screen.getByTestId('latency-diagnostic-request-first-chunk').textContent,
    ).toContain('12.5 ms');
    expect(
      screen.getByTestId('latency-diagnostic-request-receive').textContent,
    ).toContain('27.5 ms');
    expect(
      screen.getByTestId('latency-diagnostic-request-process').textContent,
    ).toContain('<0.001 ms');
    expect(
      screen.getByTestId('latency-diagnostic-request-chunks').textContent,
    ).toContain('4');
    expect(
      screen.getByTestId('latency-diagnostic-request-bytes').textContent,
    ).toContain('2.0 KB');
    expect(
      screen.getByTestId('latency-diagnostic-response-poll-gap').textContent,
    ).toContain('10 ms');

    const stages = buildStageDetails(
      ev({
        duration_ms: 200,
        request_body_read_ms: 40,
        request_body_first_chunk_ms: 12.5,
        request_body_receive_ms: 27.5,
        request_body_wait_ms: 30,
        request_body_process_ms: 10,
        upstream_body_ms: 100,
        response_body_wait_ms: 70,
        response_body_process_ms: 20,
        response_body_downstream_poll_gap_ms: 10,
      }),
    );
    expect(stages.map((stage) => stage.key)).toEqual([
      'request_body_read',
      'body_collect',
    ]);
  });

  it('keeps legacy response parents mixed and provider processing unmeasured', () => {
    render(
      <LatencyTimeline
        event={ev({
          source_kind: 'proxy',
          duration_ms: 100,
          upstream_body_ms: 40,
        })}
      />,
    );

    expect(
      screen
        .getByTestId('latency-mixed-upstream')
        .getAttribute('data-mixed-upstream-ms'),
    ).toBe('40');
    expect(
      screen
        .getByTestId('latency-category-upstream_processing')
        .getAttribute('data-category-ms'),
    ).toBe('unmeasured');
    expect(
      screen
        .getByTestId('latency-diagnostic-response-wait')
        .getAttribute('data-measured'),
    ).toBe('false');
    expect(
      screen.getByTestId('latency-diagnostic-response-parent').textContent,
    ).toContain('40 ms');
  });

  it('transitions partial known values to final splits without treating missing fields as zero', () => {
    const partial = {
      event_id: 'evt-partial',
      request_id: 'req-partial',
      ts: 1,
      ts_ms: 1000,
      last_update_ms: 1100,
      elapsed_ms: 100,
      stream: false,
      source_kind: 'proxy',
      request_body_first_chunk_ms: 10,
      request_body_wait_ms: 25,
      request_body_process_ms: 0,
      request_body_chunk_count: 2,
      _phase: 'partial',
    } satisfies RequestEventWithPhase;
    const { rerender } = render(<LatencyTimeline event={partial} isPartial />);

    expect(
      screen
        .getByTestId('latency-category-downstream_network')
        .getAttribute('data-category-ms'),
    ).toBe('25');
    expect(
      screen
        .getByTestId('latency-category-cc_lb')
        .getAttribute('data-category-ms'),
    ).toBe('0');
    expect(
      screen.getByTestId('latency-diagnostic-response-wait').textContent,
    ).toContain('In progress / not measured');

    rerender(
      <LatencyTimeline
        event={ev({
          duration_ms: 160,
          request_body_read_ms: 35,
          request_body_first_chunk_ms: 10,
          request_body_receive_ms: 25,
          request_body_wait_ms: 25,
          request_body_process_ms: 10,
          request_body_chunk_count: 2,
          upstream_body_ms: 100,
          response_body_wait_ms: 80,
          response_body_process_ms: 10,
          response_body_downstream_poll_gap_ms: 10,
        })}
      />,
    );

    expect(
      screen
        .getByTestId('latency-diagnostic-response-wait')
        .getAttribute('data-measured'),
    ).toBe('true');
    expect(
      screen.getByTestId('latency-diagnostic-response-wait').textContent,
    ).toContain('80 ms');
    expect(
      screen
        .getByTestId('latency-category-downstream_network')
        .getAttribute('data-category-ms'),
    ).toBe('35');
  });

  it('surfaces inconsistent parent/child accounting without hiding raw values', () => {
    render(
      <LatencyTimeline
        event={ev({
          duration_ms: 20,
          request_body_read_ms: 5,
          request_body_wait_ms: 10,
          request_body_process_ms: 5,
        })}
      />,
    );

    const timeline = screen.getByTestId('latency-timeline-region');
    expect(timeline.getAttribute('data-accounting-warning')).toBe('true');
    expect(screen.getByRole('alert')).toBeTruthy();
    expect(
      screen.getByTestId('latency-diagnostic-request-wait').textContent,
    ).toContain('10 ms');
    expect(
      screen.getByTestId('latency-diagnostic-request-process').textContent,
    ).toContain('5 ms');
  });

  it('supports keyboard selection and exposes the same actual category in stage details', async () => {
    const user = userEvent.setup();
    render(
      <LatencyTimeline
        event={ev({
          duration_ms: 100,
          auth_ms: 10,
        })}
      />,
    );

    const segment = screen.getByTestId('latency-segment-auth');
    segment.focus();
    await user.keyboard('{Enter}');
    expect(segment.getAttribute('aria-pressed')).toBe('true');

    const summary = screen.getByText(/^Stage details \(\d+\)$/);
    expect(summary.tagName).toBe('SUMMARY');
    summary.focus();
    expect(document.activeElement).toBe(summary);
    const details = summary.closest('details') as HTMLDetailsElement;
    await user.click(summary);
    expect(details.open).toBe(true);
    const authDetail = within(details).getByRole('button', {
      name: /Auth.*cc-lb processing.*10 ms/,
    });
    authDetail.focus();
    await user.keyboard(' ');
    expect(authDetail.getAttribute('aria-pressed')).toBe('false');
  });

  it('labels chronological stages with their actual attribution category', async () => {
    const user = userEvent.setup();
    render(
      <LatencyTimeline
        event={ev({
          duration_ms: 100,
          bulkhead_wait_ms: 5,
          dns_ms: 3,
          connect_ms: 7,
          upstream_ttfb_ms: 30,
        })}
      />,
    );
    await user.click(screen.getByText(/^Stage details \(\d+\)$/));
    const details = screen
      .getByText(/^Stage details \(\d+\)$/)
      .closest('details');
    expect(details).not.toBeNull();
    expect(
      within(details!).getByRole('button', {
        name: /Bulkhead wait.*cc-lb processing.*5 ms/,
      }),
    ).toBeTruthy();
    expect(
      within(details!).getByRole('button', {
        name: /DNS.*Upstream network.*3 ms/,
      }),
    ).toBeTruthy();
    expect(
      within(details!).getByRole('button', {
        name: /Upstream wait.*Combined upstream wait.*15 ms/,
      }),
    ).toBeTruthy();
  });

  it('keeps retry before the final attempt and shifts SSE origin exactly once', () => {
    const event = ev({
      duration_ms: 200,
      proxy_setup_ms: 10,
      retry_overhead_ms: 25,
      shape_ms: 2,
      sign_ms: 1,
      upstream_ttfb_ms: 50,
      stream_first_content_delta_ms: 5,
    });
    const stages = buildStageDetails(event);
    expect(stages.map((stage) => stage.key)).toEqual([
      'setup_overhead',
      'retry_overhead',
      'shape',
      'sign',
      'upstream_wait',
    ]);
    const first = buildSseMarkers(event).find(
      (marker) => marker.key === 'first_delta',
    );
    expect(first?.absMs).toBe(10 + 25 + 2 + 1 + 50 + 5);
  });

  it('does not shift SSE markers when overlapping I/O diagnostics change', () => {
    const parent = {
      duration_ms: 200,
      request_body_read_ms: 20,
      proxy_setup_ms: 10,
      upstream_ttfb_ms: 50,
      stream_first_content_delta_ms: 5,
    };
    const withoutChildren = buildSseMarkers(ev(parent));
    const withChildren = buildSseMarkers(
      ev({
        ...parent,
        request_body_first_chunk_ms: 19,
        request_body_receive_ms: 1,
        request_body_wait_ms: 18,
        request_body_process_ms: 2,
        response_body_wait_ms: 100,
        response_body_process_ms: 10,
        response_body_downstream_poll_gap_ms: 30,
      }),
    );
    expect(withChildren).toEqual(withoutChildren);
  });

  it('retains cancellation measurements and keeps renewal outside proxy categories', () => {
    const { rerender } = render(
      <LatencyTimeline
        event={ev({
          source_kind: 'proxy',
          status: 499,
          duration_ms: 100,
          upstream_body_ms: 60,
          response_body_wait_ms: 40,
          response_body_process_ms: 10,
        })}
      />,
    );

    expect(
      screen.getByTestId('latency-diagnostic-response-parent').textContent,
    ).toContain('Partial response parent');
    expect(screen.getByText('Partial stream (client cancelled)')).toBeTruthy();

    rerender(
      <LatencyTimeline
        event={ev({
          source_kind: 'renewal',
          duration_ms: 50,
        })}
      />,
    );
    expect(screen.getByText('Scheduler renewal cycle')).toBeTruthy();
    expect(
      screen.queryByRole('list', { name: 'Latency category summary' }),
    ).toBeNull();
    expect(screen.queryByTestId('latency-mixed-upstream')).toBeNull();
  });

  it('preserves terminal error measurements instead of inventing completed response time', () => {
    render(
      <LatencyTimeline
        event={ev({
          source_kind: 'proxy',
          status: 502,
          duration_ms: 30,
          request_body_read_ms: 5,
          request_body_wait_ms: 4,
          request_body_process_ms: 1,
          retry_overhead_ms: 3,
        })}
      />,
    );

    expect(
      screen
        .getByTestId('latency-category-downstream_network')
        .getAttribute('data-category-ms'),
    ).toBe('4');
    expect(
      screen
        .getByTestId('latency-category-cc_lb')
        .getAttribute('data-category-ms'),
    ).toBe('1');
    expect(
      screen
        .getByTestId('latency-mixed-retry')
        .getAttribute('data-mixed-retry-ms'),
    ).toBe('3');
    expect(
      screen
        .getByTestId('latency-diagnostic-response-parent')
        .getAttribute('data-measured'),
    ).toBe('false');
  });

  it('keeps four unmeasured cards visible on the no-data path', () => {
    render(<LatencyTimeline event={ev({ duration_ms: 0 })} />);
    expect(
      within(
        screen.getByRole('list', { name: 'Latency category summary' }),
      ).getAllByRole('listitem'),
    ).toHaveLength(4);
    expect(screen.getByText('No latency data recorded.')).toBeTruthy();
  });

  it('reserves the redesigned region while detail hydrates', () => {
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
    expect(
      pendingRegion.querySelectorAll('.skeleton').length,
    ).toBeGreaterThanOrEqual(5);

    rerender(<LatencyTimeline event={event} />);
    expect(screen.getByTestId('latency-timeline-region')).toBe(pendingRegion);
    expect(pendingRegion.getAttribute('aria-busy')).toBeNull();
    expect(
      screen.getByRole('heading', { name: 'Chronological request path' }),
    ).toBeTruthy();
  });
});
