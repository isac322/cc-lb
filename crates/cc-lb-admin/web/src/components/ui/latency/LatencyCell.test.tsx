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
  openBy: 'hover' | 'click' = 'hover',
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
  if (openBy === 'click') {
    fireEvent.click(trigger);
  } else {
    fireEvent.pointerEnter(trigger);
    act(() => vi.advanceTimersByTime(200));
  }
  return trigger;
}

function describedText(element: Element): string {
  return (element.getAttribute('aria-describedby') ?? '')
    .split(/\s+/)
    .filter(Boolean)
    .map((id) => document.getElementById(id)?.textContent ?? '')
    .join(' ');
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
      screen.getByText('Latency by responsibility').parentElement
        ?.textContent ?? '';
    const positions = chronology.map((label) => popoverText.indexOf(label));
    expect(positions.every((position) => position >= 0)).toBe(true);
    expect(positions).toEqual([...positions].sort((a, b) => a - b));
    expect(screen.queryByText('Setup overhead')).toBeNull();
    expect(screen.getByText('0.125 ms')).toBeDefined();
    expect(screen.getAllByText('0 ms').length).toBeGreaterThan(0);
  });

  it('separates responsibility domains and keeps measurement limits contextual', () => {
    renderCell({
      ...baseEvent,
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
    });

    const expected = [
      ['Downstream latency', /40(?:\.0)? ms.*4%/],
      ['cc-lb latency', /335(?:\.0)? ms.*34%/],
      ['Upstream net latency', /50(?:\.0)? ms.*5%/],
      ['Upstream wait latency', /340(?:\.0)? ms.*34%/],
      ['Unattributed latency', /235(?:\.0)? ms.*24%/],
    ] as const;
    for (const [name, value] of expected) {
      expect(screen.getByRole('group', { name }).textContent).toMatch(value);
    }

    expect(
      describedText(screen.getByRole('group', { name: 'Downstream latency' })),
    ).toMatch(/not a network RTT measurement/i);
    expect(
      describedText(
        screen.getByRole('group', { name: 'Upstream wait latency' }),
      ),
    ).toMatch(/provider generation, upstream transit, and runtime scheduling/i);
    expect(screen.getByText('Retry overhead').getAttribute('title')).toMatch(
      /one aggregate across prior attempts/i,
    );
    expect(
      describedText(
        screen.getByRole('group', { name: 'Unattributed latency' }),
      ),
    ).toMatch(
      /cannot be assigned to one responsibility.*one aggregate across prior attempts.*no finer timing witness/i,
    );
    expect(screen.queryByText(/mixed/i)).toBeNull();

    const responsibilitySparkline = screen.getByRole('img', {
      name: /Downstream 40 ms.*cc-lb 335 ms.*Upstream net 50 ms.*Upstream wait 340 ms.*Unattributed 235 ms/,
    });
    expect(
      Array.from(
        responsibilitySparkline.querySelectorAll<HTMLElement>('span'),
        (segment) => segment.style.width,
      ),
    ).toEqual(['4%', '33.5%', '5%', '34%', '23.5%']);
  });

  it('does not double-count setup timings recorded before proxy_setup_ms', () => {
    renderCell({
      ...baseEvent,
      source_kind: 'proxy',
      duration_ms: 120,
      proxy_setup_ms: undefined,
      auth_ms: 5,
      route_ms: 2,
      limit_reserve_ms: 1,
      json_parse_ms: 48,
    });

    expect(
      screen.getByRole('group', { name: 'cc-lb latency' }).textContent,
    ).toMatch(/56(?:\.0)? ms.*47%/);
    expect(
      screen.getByRole('group', { name: 'Unattributed latency' }).textContent,
    ).toMatch(/64(?:\.0)? ms.*53%/);
  });

  it('does not double-count request-body timings when the parent is absent', () => {
    renderCell({
      ...baseEvent,
      source_kind: 'proxy',
      duration_ms: 100,
      request_body_read_ms: null,
      request_body_wait_ms: 30,
      request_body_process_ms: 20,
    });

    expect(
      screen.getByRole('group', { name: 'Downstream latency' }).textContent,
    ).toMatch(/30(?:\.0)? ms.*30%/);
    expect(
      screen.getByRole('group', { name: 'cc-lb latency' }).textContent,
    ).toMatch(/20(?:\.0)? ms.*20%/);
    expect(
      screen.getByRole('group', { name: 'Unattributed latency' }).textContent,
    ).toMatch(/50(?:\.0)? ms.*50%/);
  });

  it('does not open on keyboard focus, only on explicit activation', () => {
    render(
      <table>
        <tbody>
          <tr>
            <LatencyCell event={baseEvent} />
          </tr>
        </tbody>
      </table>,
    );
    fireEvent.focus(
      screen.getByRole('button', { name: /Latency .* show breakdown/ }),
    );
    expect(screen.queryByText('Latency by responsibility')).toBeNull();
    cleanup();

    const trigger = renderCell(
      {
        ...baseEvent,
        proxy_setup_ms: 20,
        json_parse_ms: 0.125,
      },
      'click',
    );

    expect(trigger.getAttribute('aria-label')).toBe(
      'Latency 100 ms, show breakdown',
    );
    expect(describedText(trigger)).toBe('cc-lb 20 ms, Unattributed 80 ms');
    expect(screen.getByText('Latency by responsibility')).toBeDefined();
    expect(screen.getByText('JSON parse')).toBeDefined();
  });

  it('keeps the Warm pool indicator visible when DNS and connect are absent', () => {
    renderCell({
      ...baseEvent,
      source_kind: 'proxy',
      connection_reused: true,
      upstream_ttfb_ms: 20,
    });

    const upstreamNet = screen.getByRole('group', {
      name: 'Upstream net latency',
    });
    expect(upstreamNet.textContent).toMatch(/Upstream net.*Warm pool.*0 ms/);
    expect(screen.queryByText('DNS')).toBeNull();
    expect(screen.queryByText('Connect (TCP+TLS)')).toBeNull();
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

  it('shows ingress metadata and responsibility groups for a proxy row', () => {
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
      'click',
    );

    expect(trigger.getAttribute('aria-label')).toBe(
      'Latency 100 ms, show breakdown',
    );
    expect(describedText(trigger)).toBe(
      'cc-lb 30 ms, Upstream wait 20 ms, Unattributed 50 ms, Ingress body 834.3 KB',
    );

    expect(screen.getByText('Ingress body: 834.3 KB')).toBeDefined();
    const responsibilityOrder = ['cc-lb', 'Upstream wait', 'Unattributed'];
    const popoverText =
      screen.getByText('Latency by responsibility').parentElement
        ?.textContent ?? '';
    const positions = responsibilityOrder.map((label) =>
      popoverText.indexOf(label),
    );
    expect(positions.every((position) => position >= 0)).toBe(true);
    expect(positions).toEqual([...positions].sort((a, b) => a - b));
    expect(screen.queryByText('Internal post')).toBeNull();
    expect(
      screen.getByRole('img', {
        name: /cc-lb 30 ms.*Upstream wait 20 ms.*Unattributed 50 ms/,
      }),
    ).toBeDefined();
    expect(screen.getByText('Unattributed')).toBeDefined();
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
      screen.getByRole('img', {
        name: /cc-lb 200 ms.*Upstream wait 100 ms.*Unattributed 700 ms/,
      }),
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
        response_body_wait_ms: 500,
        response_body_process_ms: 20,
        response_body_downstream_poll_gap_ms: 10,
      },
      'click',
    );

    expect(trigger.getAttribute('aria-label')).toBe(
      'Latency 700 ms, show breakdown',
    );
    expect(describedText(trigger)).toBe(
      'Downstream 10 ms, cc-lb 80 ms, Upstream wait 500 ms, Unattributed 110 ms',
    );

    expect(
      screen.getByText('Other response body (client cancelled)'),
    ).toBeDefined();
    expect(screen.getAllByText('110 ms').length).toBeGreaterThan(0);
    expect(screen.queryByText('Stream relay')).toBeNull();
    expect(
      screen.getByRole('img', {
        name: /Downstream 10 ms.*cc-lb 80 ms.*Upstream wait 500 ms.*Unattributed 110 ms/,
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
      'click',
    );

    expect(trigger.getAttribute('aria-label')).toBe(
      'Latency 500 ms, show breakdown',
    );
    expect(describedText(trigger)).toBe('Renewal cycle 500 ms');

    expect(screen.getByText('Renewal cycle')).toBeDefined();
    expect(screen.queryByText('Request body read')).toBeNull();
    expect(screen.queryByText('Internal pre')).toBeNull();
    expect(screen.queryByText('Body')).toBeNull();
    expect(screen.queryByText('Finalize')).toBeNull();
    expect(screen.queryByText('Unaccounted')).toBeNull();
    expect(
      screen.getByRole('img', {
        name: /^Latency by responsibility: Renewal cycle 500 ms$/,
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

    expect(screen.getByText('Unattributed')).toBeDefined();
    expect(
      screen.getByText('Other lifecycle time').getAttribute('title'),
    ).toMatch(/no finer timing witness/i);
    expect(
      screen.getByRole('img', {
        name: /cc-lb 20 ms.*Upstream wait 20 ms.*Unattributed 60 ms/,
      }),
    ).toBeDefined();
  });
  it('shows Limit reconcile within cc-lb without adding it again', () => {
    renderCell({
      ...baseEvent,
      source_kind: 'proxy',
      finalize_ms: 20,
      limit_reconcile_ms: 5,
    });

    expect(screen.getByRole('group', { name: 'cc-lb latency' })).toBeDefined();
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
    expect(screen.getByRole('group', { name: 'cc-lb latency' })).toBeDefined();
    expect(
      screen.getByRole('group', { name: 'Unattributed latency' }),
    ).toBeDefined();
    expect(screen.getAllByText('0 ms').length).toBeGreaterThanOrEqual(2);
  });

  it('keeps legacy request-body and post timing in honest responsibility groups', () => {
    const trigger = renderCell(
      {
        ...baseEvent,
        source_kind: 'proxy',
        request_body_read_ms: 20,
        finalize_ms: undefined,
        limit_reconcile_ms: 10,
      },
      'click',
    );

    expect(trigger.getAttribute('aria-label')).toBe(
      'Latency 100 ms, show breakdown',
    );
    expect(describedText(trigger)).toBe('cc-lb 10 ms, Unattributed 90 ms');
    expect(screen.getByText('Request body read')).toBeDefined();
    expect(screen.queryByText('Finalize')).toBeNull();
    expect(screen.getByRole('group', { name: 'cc-lb latency' })).toBeDefined();
    expect(
      screen.getByRole('group', { name: 'Unattributed latency' }),
    ).toBeDefined();
    expect(screen.getByText('Limit reconcile')).toBeDefined();
    expect(
      screen.getByRole('img', {
        name: /cc-lb 10 ms.*Unattributed 90 ms/,
      }),
    ).toBeDefined();
  });

  it('keeps a measured-zero Limit reconcile audible within cc-lb', () => {
    const trigger = renderCell(
      {
        ...baseEvent,
        source_kind: 'proxy',
        request_body_read_ms: 20,
        finalize_ms: undefined,
        limit_reconcile_ms: 0,
      },
      'click',
    );

    expect(trigger.getAttribute('aria-label')).toBe(
      'Latency 100 ms, show breakdown',
    );
    expect(describedText(trigger)).toBe('Unattributed 100 ms');
    expect(screen.getByRole('group', { name: 'cc-lb latency' })).toBeDefined();
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
