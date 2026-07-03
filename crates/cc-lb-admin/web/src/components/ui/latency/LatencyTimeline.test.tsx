import { cleanup, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it } from 'vitest';
import type { RequestEvent } from '../../../lib/api';
import {
  buildSseMarkers,
  buildStageDetails,
  computeMarkerTracks,
  LatencyTimeline,
} from './LatencyTimeline';

function ev(overrides: Partial<RequestEvent>): RequestEvent {
  return {
    request_id: 'req_test',
    upstream: 'anthropic',
    status: 200,
    duration_ms: 0,
    ...overrides,
  } as RequestEvent;
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

  it('shows a hint when there is no latency data', () => {
    const event = ev({ duration_ms: 0 });
    render(<LatencyTimeline event={event} />);
    expect(screen.getByText('No latency data recorded.')).toBeTruthy();
  });
});
