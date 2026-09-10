import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
} from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { RequestEventWithPhase } from '../../../lib/RequestEventTypes';
import { LatencyCell } from './LatencyCell';

function renderCell(
  event: RequestEventWithPhase,
  openBy: 'hover' | 'focus' = 'hover',
) {
  render(
    <table>
      <tbody>
        <tr>
          <LatencyCell event={event} />
        </tr>
      </tbody>
    </table>,
  );
  const trigger = screen.getByRole('button', {
    name: /Latency .* show breakdown/,
  });
  if (openBy === 'focus') {
    fireEvent.focus(trigger);
  } else {
    fireEvent.pointerEnter(trigger);
    act(() => vi.advanceTimersByTime(200));
  }
  return trigger;
}

const baseEvent = {
  ts: 1,
  request_id: 'req-latency',
  status: 200,
  duration_ms: 100,
  _phase: 'final',
} satisfies RequestEventWithPhase;

describe('LatencyCell timing breakdown', () => {
  beforeEach(() => vi.useFakeTimers());

  afterEach(() => {
    cleanup();
    vi.useRealTimers();
  });

  it('shows all measured setup labels, measured zero, and Other setup', () => {
    renderCell({
      ...baseEvent,
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
      shape_ms: 1,
      sign_ms: 1,
    });

    const chronology = [
      'JSON parse',
      'Tokenizer queue',
      'Cache structure',
      'Cache serialize',
      'Cache token key',
      'Cache count lookup',
      'Cache tokenize',
      'Auth',
      'Route',
      'Limit reserve',
      'Prepare signer',
      'Other setup',
      'Shape',
      'Sign',
    ];
    for (const label of chronology) {
      expect(screen.getByText(label)).toBeDefined();
    }
    const popoverText =
      screen.getByText('Latency').parentElement?.textContent ?? '';
    const positions = chronology.map((label) => popoverText.indexOf(label));
    expect(positions.every((position) => position >= 0)).toBe(true);
    expect(positions).toEqual([...positions].sort((a, b) => a - b));
    expect(screen.queryByText('Setup overhead')).toBeNull();
    expect(screen.getByText('0.125 ms')).toBeDefined();
    expect(screen.getAllByText('0 ms').length).toBeGreaterThan(0);
  });

  it('uses a native button and opens the popover on keyboard focus', () => {
    const trigger = renderCell(
      {
        ...baseEvent,
        proxy_setup_ms: 20,
        json_parse_ms: 0.125,
      },
      'focus',
    );

    expect(trigger.getAttribute('aria-label')).toMatch(
      /^Latency 100 ms, show breakdown$/,
    );
    expect(screen.getByText('Latency')).toBeDefined();
    expect(screen.getByText('JSON parse')).toBeDefined();
  });

  it('keeps the single Setup overhead fallback for legacy rows', () => {
    renderCell({
      ...baseEvent,
      proxy_setup_ms: 20,
      auth_ms: 2,
      route_ms: 1,
      limit_reserve_ms: 1,
    });

    expect(screen.getByText('Setup overhead')).toBeDefined();
    expect(screen.queryByText('JSON parse')).toBeNull();
    expect(screen.queryByText('Other setup')).toBeNull();
  });

  it('shows completed setup timing values on partial rows', () => {
    renderCell({
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

    expect(screen.getByText('JSON parse')).toBeDefined();
    expect(screen.getByText('Cache structure')).toBeDefined();
    expect(screen.queryByText('Other setup')).toBeNull();
  });

  it('shows request ingress first, exact body bytes, and Finalize for a new proxy row', () => {
    const trigger = renderCell(
      {
        ...baseEvent,
        source_kind: 'proxy',
        request_body_read_ms: 10,
        request_body_bytes: 854336,
        proxy_setup_ms: 20,
        upstream_ttfb_ms: 20,
        upstream_body_ms: 40,
        finalize_ms: 10,
      },
      'focus',
    );

    expect(trigger.getAttribute('aria-label')).toBe(
      'Latency 100 ms, Proxy request body read 10 ms, Ingress body 834.3 KB, Finalize 10 ms, show breakdown',
    );

    expect(screen.getByText('Ingress body: 834.3 KB')).toBeDefined();
    const chronology = [
      'Request body read',
      'Internal pre',
      'Upstream',
      'Body',
      'Finalize',
    ];
    const popoverText =
      screen.getByText('Latency').parentElement?.textContent ?? '';
    const positions = chronology.map((label) => popoverText.indexOf(label));
    expect(positions.every((position) => position >= 0)).toBe(true);
    expect(positions).toEqual([...positions].sort((a, b) => a - b));
    expect(screen.queryByText('Internal post')).toBeNull();
    expect(
      screen.getByRole('img', {
        name: /Request body read 10 ms.*Ingress body 834\.3 KB.*Finalize 10 ms/,
      }),
    ).toBeDefined();
    expect(screen.queryByText('Unaccounted')).toBeNull();
  });

  it('shows one stream relay label for a completed stream', () => {
    renderCell({
      ...baseEvent,
      source_kind: 'proxy',
      duration_ms: 1000,
      request_body_read_ms: 100,
      proxy_setup_ms: 100,
      upstream_ttfb_ms: 100,
      stream_total_ms: 600,
      upstream_body_ms: 600,
      finalize_ms: 100,
    });

    expect(screen.getByText('Stream relay')).toBeDefined();
    expect(screen.queryByText('Body collect')).toBeNull();
    expect(
      screen.getByRole('img', { name: /Stream relay 600 ms/ }),
    ).toBeDefined();
    expect(screen.queryByText('Partial stream (client cancelled)')).toBeNull();
  });

  it('identifies the preserved response body time on a cancelled 499', () => {
    const trigger = renderCell(
      {
        ...baseEvent,
        source_kind: 'proxy',
        status: 499,
        duration_ms: 700,
        upstream_body_ms: 640,
        stream_total_ms: 690,
        finalize_ms: 60,
      },
      'focus',
    );

    expect(trigger.getAttribute('aria-label')).toBe(
      'Latency 700 ms, Partial stream (client cancelled) 640 ms, Finalize 60 ms, show breakdown',
    );

    expect(screen.getByText('Partial stream (client cancelled)')).toBeDefined();
    expect(screen.getAllByText('640 ms').length).toBeGreaterThan(0);
    expect(screen.queryByText('Stream relay')).toBeNull();
    expect(
      screen.getByRole('img', {
        name: /Partial stream \(client cancelled\) 640 ms/,
      }),
    ).toBeDefined();
  });

  it('renders renewal as one source-specific cycle without proxy residuals', () => {
    const trigger = renderCell(
      {
        ...baseEvent,
        source_kind: 'renewal',
        duration_ms: 500,
      },
      'focus',
    );

    expect(trigger.getAttribute('aria-label')).toBe(
      'Latency 500 ms, Renewal cycle 500 ms, show breakdown',
    );

    expect(screen.getByText('Renewal cycle')).toBeDefined();
    expect(screen.queryByText('Request body read')).toBeNull();
    expect(screen.queryByText('Internal pre')).toBeNull();
    expect(screen.queryByText('Body')).toBeNull();
    expect(screen.queryByText('Finalize')).toBeNull();
    expect(screen.queryByText('Unaccounted')).toBeNull();
    expect(
      screen.getByRole('img', {
        name: /^Latency stages: Renewal cycle 500 ms$/,
      }),
    ).toBeDefined();
  });

  it('keeps an over-budget positive residual visible in the popover and sparkline', () => {
    renderCell({
      ...baseEvent,
      source_kind: 'proxy',
      request_body_read_ms: 10,
      proxy_setup_ms: 10,
      upstream_ttfb_ms: 20,
      upstream_body_ms: 30,
      finalize_ms: 10,
    });

    expect(screen.getByText('Unaccounted')).toBeDefined();
    expect(
      screen.getByRole('img', { name: /Unaccounted 20 ms/ }),
    ).toBeDefined();
  });
  it('shows Limit reconcile as a Finalize detail without adding it again', () => {
    renderCell({
      ...baseEvent,
      source_kind: 'proxy',
      finalize_ms: 20,
      limit_reconcile_ms: 5,
    });

    expect(screen.getByText('Finalize')).toBeDefined();
    expect(screen.getByText('Limit reconcile')).toBeDefined();
    expect(screen.getAllByText('20 ms')).toHaveLength(1);
    expect(screen.getByText('Other finalize')).toBeDefined();
    expect(screen.getAllByText('15 ms')).toHaveLength(1);
    expect(screen.getAllByText('5 ms')).toHaveLength(1);
  });

  it('shows measured zero for new stages', () => {
    renderCell({
      ...baseEvent,
      source_kind: 'proxy',
      request_body_read_ms: 0,
      finalize_ms: 0,
    });

    expect(screen.getByText('Request body read')).toBeDefined();
    expect(screen.getByText('Finalize')).toBeDefined();
    expect(screen.getAllByText('0 ms').length).toBeGreaterThanOrEqual(2);
  });

  it('renders a mixed row without promoting a Finalize child to a parent', () => {
    const trigger = renderCell(
      {
        ...baseEvent,
        source_kind: 'proxy',
        request_body_read_ms: 20,
        finalize_ms: undefined,
        limit_reconcile_ms: 10,
      },
      'focus',
    );

    expect(trigger.getAttribute('aria-label')).toBe(
      'Latency 100 ms, Proxy request body read 20 ms, Internal post 10 ms, show breakdown',
    );
    expect(screen.getByText('Request body read')).toBeDefined();
    expect(screen.queryByText('Finalize')).toBeNull();
    expect(screen.getByText('Internal post')).toBeDefined();
    expect(screen.getByText('Limit reconcile')).toBeDefined();
    expect(
      screen.getByRole('img', {
        name: /Internal post 10 ms.*Unaccounted 70 ms/,
      }),
    ).toBeDefined();
  });

  it('keeps a measured-zero Limit reconcile audible as Internal post', () => {
    const trigger = renderCell(
      {
        ...baseEvent,
        source_kind: 'proxy',
        request_body_read_ms: 20,
        finalize_ms: undefined,
        limit_reconcile_ms: 0,
      },
      'focus',
    );

    expect(trigger.getAttribute('aria-label')).toBe(
      'Latency 100 ms, Proxy request body read 20 ms, Internal post 0 ms, show breakdown',
    );
    expect(screen.getByText('Internal post')).toBeDefined();
    expect(screen.getByText('Limit reconcile')).toBeDefined();
    expect(screen.getAllByText('0 ms').length).toBeGreaterThan(0);
  });

  it('does not invent new stages for missing or null fields', () => {
    renderCell({
      ...baseEvent,
      source_kind: 'proxy',
      request_body_read_ms: null,
      finalize_ms: null,
    });

    expect(screen.queryByText('Request body read')).toBeNull();
    expect(screen.queryByText('Finalize')).toBeNull();
  });
});
