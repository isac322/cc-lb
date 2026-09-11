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
  openBy: 'hover' | 'focus' = 'focus',
) {
  const rendered = render(
    <table>
      <tbody>
        <tr>
          <LatencyCell event={event} isPartial={event._phase === 'partial'} />
        </tr>
      </tbody>
    </table>,
  );
  const trigger = screen.getByRole('button', { name: /^Latency / });
  if (openBy === 'hover') {
    fireEvent.pointerEnter(trigger);
    act(() => vi.advanceTimersByTime(200));
  } else {
    fireEvent.focus(trigger);
  }
  return { ...rendered, trigger };
}

const baseEvent = {
  ts: 1,
  request_id: 'req-latency',
  status: 200,
  duration_ms: 120,
  _phase: 'final',
  source_kind: 'proxy',
} satisfies RequestEventWithPhase;

describe('LatencyCell latency attribution', () => {
  beforeEach(() => vi.useFakeTimers());

  afterEach(() => {
    cleanup();
    vi.useRealTimers();
  });

  it('makes the four shared categories visible in the compact cell and popover', () => {
    const { trigger } = renderCell({
      ...baseEvent,
      proxy_setup_ms: 10,
      shape_ms: 2,
      sign_ms: 1,
      bulkhead_wait_ms: 3,
      dns_ms: 2,
      connect_ms: 3,
      upstream_ttfb_ms: 50,
      request_body_wait_ms: 12,
      request_body_process_ms: 1,
      response_body_wait_ms: 20,
      response_body_process_ms: 2,
      response_body_downstream_poll_gap_ms: 5,
      finalize_ms: 4,
    });

    expect(
      screen.getByRole('list', {
        name: 'Compact latency category legend',
      }),
    ).toBeDefined();
    for (const category of [
      'downstream_network',
      'cc_lb',
      'upstream_network',
      'upstream_processing',
    ]) {
      expect(
        document.querySelector(`[data-latency-category="${category}"]`),
      ).not.toBeNull();
    }

    const accessibleName = trigger.getAttribute('aria-label') ?? '';
    expect(accessibleName).toContain('Downstream network');
    expect(accessibleName).toContain('cc-lb processing');
    expect(accessibleName).toContain('Upstream network');
    expect(accessibleName).toContain(
      'Upstream processing Not independently measured',
    );
    expect(accessibleName).toContain('Combined upstream wait');
    expect(
      screen.getByRole('img', { name: /^Latency distribution:/ }),
    ).toBeDefined();
    expect(
      screen.getByRole('list', { name: 'Latency category legend' }),
    ).toBeDefined();
  });

  it('keeps shared upstream wait hatched and provider time unknown', () => {
    renderCell({
      ...baseEvent,
      duration_ms: 100,
      bulkhead_wait_ms: 5,
      dns_ms: 5,
      connect_ms: 10,
      upstream_ttfb_ms: 60,
      response_body_wait_ms: 20,
    });

    const mixed = document.querySelector(
      '[data-latency-mixed="upstream-wait"]',
    );
    expect(mixed?.textContent).toContain('Combined upstream wait');
    expect(mixed?.textContent).toContain('60 ms');
    expect(
      mixed?.querySelector('[class*="repeating-linear-gradient"]'),
    ).not.toBeNull();
    expect(mixed?.textContent).toContain(
      'not assigned to either upstream card',
    );

    const provider = document.querySelector(
      '[data-latency-category="upstream_processing"]',
    );
    expect(provider?.textContent).toContain('Not independently measured');
    expect(provider?.textContent).not.toContain('60 ms');
  });

  it('keeps legacy response-body time combined instead of calling it provider time', () => {
    renderCell({
      ...baseEvent,
      duration_ms: 100,
      upstream_body_ms: 60,
    });

    const mixed = document.querySelector(
      '[data-latency-mixed="upstream-wait"]',
    );
    expect(mixed?.textContent).toContain(
      'Legacy response-body time combines upstream wait, local relay work, and downstream consumption',
    );
    expect(mixed?.textContent).toContain('60 ms');
    expect(
      document.querySelector('[data-latency-category="upstream_processing"]')
        ?.textContent,
    ).toContain('Not independently measured');
  });

  it('separates first DATA delay, receive remainder, wait, local work, frames, and bytes', () => {
    renderCell({
      ...baseEvent,
      request_body_read_ms: 30,
      request_body_first_chunk_ms: 0,
      request_body_receive_ms: 12.5,
      request_body_wait_ms: 8,
      request_body_process_ms: 0.0004,
      request_body_chunk_count: 0,
      request_body_bytes: 0,
    });

    expect(screen.getByText('Ingress observations')).toBeDefined();
    expect(screen.getByText('Body start → first DATA')).toBeDefined();
    expect(screen.getByText('First DATA → body complete')).toBeDefined();
    expect(screen.getByText('Waiting for body frames')).toBeDefined();
    expect(screen.getByText('Local body handling')).toBeDefined();
    expect(screen.getByText('Non-empty DATA frames')).toBeDefined();
    expect(screen.getByText('Ingress body')).toBeDefined();
    expect(screen.getByText('Request body parent interval')).toBeDefined();
    expect(screen.getAllByText('0 ms').length).toBeGreaterThan(0);
    expect(screen.getByText('12.5 ms')).toBeDefined();
    expect(screen.getByText('<0.001 ms')).toBeDefined();
    expect(screen.getByText('0 B')).toBeDefined();
    expect(screen.getByText(/overlapping diagnostic markers/)).toBeDefined();
  });

  it('qualifies downstream poll gaps without presenting them as RTT', () => {
    renderCell({
      ...baseEvent,
      duration_ms: 90,
      upstream_body_ms: 70,
      response_body_wait_ms: 30,
      response_body_process_ms: 4,
      response_body_downstream_poll_gap_ms: 20,
    });

    expect(screen.getByText('Response observations')).toBeDefined();
    expect(screen.getByText('Waiting for response frames')).toBeDefined();
    expect(screen.getByText('Local response relay')).toBeDefined();
    expect(screen.getByText('Next downstream consumer poll')).toBeDefined();
    expect(screen.getByText(/not a wire ACK or RTT/)).toBeDefined();
    expect(screen.getByText(/Parent response intervals overlap/)).toBeDefined();
  });

  it('preserves measured zero and does not turn missing categories into zero', () => {
    renderCell({
      ...baseEvent,
      duration_ms: 0,
      request_body_wait_ms: 0,
      request_body_process_ms: 0,
      dns_ms: 0,
      connect_ms: 0,
      response_body_wait_ms: 0,
      response_body_process_ms: 0,
      response_body_downstream_poll_gap_ms: 0,
      finalize_ms: 0,
    });

    expect(
      document.querySelector('[data-latency-category="downstream_network"]')
        ?.textContent,
    ).toContain('0 ms');
    expect(
      document.querySelector('[data-latency-category="cc_lb"]')?.textContent,
    ).toContain('0 ms');
    expect(
      document.querySelector('[data-latency-category="upstream_network"]')
        ?.textContent,
    ).toContain('0 ms');
    expect(
      document.querySelector('[data-latency-category="upstream_processing"]')
        ?.textContent,
    ).toContain('Not independently measured');

    cleanup();
    renderCell(baseEvent);
    expect(
      document.querySelector('[data-latency-category="downstream_network"]')
        ?.textContent,
    ).toContain('Not measured');
    expect(
      document.querySelector('[data-latency-category="upstream_network"]')
        ?.textContent,
    ).toContain('Not measured');
    expect(screen.queryByText('Ingress observations')).toBeNull();
  });

  it('shows completed measurements and an in-progress indicator on partial rows', () => {
    const { trigger } = renderCell({
      event_id: 'evt-partial',
      request_id: 'req-partial',
      ts: 1,
      ts_ms: 1000,
      last_update_ms: 1012,
      elapsed_ms: 12,
      stream: false,
      request_body_wait_ms: 0,
      request_body_process_ms: 0.25,
      json_parse_ms: 0,
      stream_total_ms: 5,
      _phase: 'partial',
    });

    expect(trigger.getAttribute('aria-label')).toContain('Latency 12 ms');
    expect(
      screen.getByLabelText('Latency measurement in progress'),
    ).toBeDefined();
    expect(screen.getByText('Waiting for body frames')).toBeDefined();
    expect(screen.getByText('JSON parse')).toBeDefined();
    expect(screen.getAllByText('0 ms').length).toBeGreaterThan(0);
    expect(screen.getByText('0.25 ms')).toBeDefined();
    expect(
      screen.getByText('Stream relay parent interval').parentElement
        ?.textContent,
    ).toContain('In progress');
    expect(screen.queryByText('Other setup')).toBeNull();
  });

  it('retains setup detail, including sub-millisecond zero values and legacy fallback', () => {
    renderCell({
      ...baseEvent,
      proxy_setup_ms: 20,
      auth_ms: 2,
      route_ms: 1,
      limit_reserve_ms: 1,
      json_parse_ms: 0.125,
      cache_tokenize_ms: 0,
      prepare_signer_ms: 3,
      shape_ms: 1,
      sign_ms: 1,
    });

    expect(screen.getByText('cc-lb observations')).toBeDefined();
    expect(screen.getByText('JSON parse')).toBeDefined();
    expect(screen.getByText('Other setup')).toBeDefined();
    expect(screen.getByText('0.125 ms')).toBeDefined();
    expect(screen.getAllByText('0 ms').length).toBeGreaterThan(0);

    cleanup();
    renderCell({
      ...baseEvent,
      proxy_setup_ms: 20,
      auth_ms: 2,
      route_ms: 1,
      limit_reserve_ms: 1,
    });
    expect(screen.getByText('Setup overhead')).toBeDefined();
    expect(screen.queryByText('Other setup')).toBeNull();
  });

  it('formats only finite numeric limit reconcile values and preserves zero', () => {
    renderCell({
      ...baseEvent,
      finalize_ms: undefined,
      limit_reconcile_ms: 0,
    });

    expect(
      screen.getByText('Limit reconcile (internal post)').parentElement
        ?.textContent,
    ).toContain('0 ms');
    expect(screen.queryByText('Finalize')).toBeNull();

    const partialEvent = {
      event_id: 'evt-partial-reconcile',
      request_id: 'req-partial-reconcile',
      ts: 1,
      ts_ms: 1000,
      elapsed_ms: 12,
      _phase: 'partial',
    } satisfies RequestEventWithPhase;

    cleanup();
    renderCell({
      ...partialEvent,
      limit_reconcile_ms: '0',
    });
    expect(screen.queryByText('Limit reconcile (internal post)')).toBeNull();

    cleanup();
    renderCell({
      ...partialEvent,
      limit_reconcile_ms: Number.POSITIVE_INFINITY,
    });
    expect(screen.queryByText('Limit reconcile (internal post)')).toBeNull();

    cleanup();
    renderCell({
      ...partialEvent,
      limit_reconcile_ms: null,
    });
    expect(screen.queryByText('Limit reconcile (internal post)')).toBeNull();

    cleanup();
    renderCell(partialEvent);
    expect(screen.queryByText('Limit reconcile (internal post)')).toBeNull();
  });

  it('keeps cancelled response measurements and retry overhead explicit', () => {
    renderCell({
      ...baseEvent,
      status: 499,
      duration_ms: 700,
      upstream_body_ms: 640,
      stream_total_ms: 690,
      response_body_wait_ms: 500,
      response_body_process_ms: 20,
      response_body_downstream_poll_gap_ms: 120,
      retry_overhead_ms: 15,
      finalize_ms: 60,
    });

    expect(screen.getByText('Partial stream (client cancelled)')).toBeDefined();
    expect(screen.getAllByText('640 ms').length).toBeGreaterThan(0);
    expect(
      document.querySelector('[data-latency-mixed="retry"]')?.textContent,
    ).toContain('15 ms');
    expect(screen.getByText('Finalize')).toBeDefined();
    expect(
      screen.getByText(/Measured child intervals exceed a parent interval/),
    ).toBeDefined();

    cleanup();
    renderCell({
      ...baseEvent,
      status: 499,
      duration_ms: 70,
      stream_total_ms: 60,
    });
    expect(
      screen.getByText('Partial stream (client cancelled)').parentElement
        ?.textContent,
    ).toContain('Not measured');
    expect(
      document.querySelector('[data-latency-mixed="upstream-wait"]')
        ?.textContent,
    ).not.toContain('Legacy response-body time');
  });

  it('opens from hover and keyboard focus through the native button', () => {
    const { trigger } = renderCell(baseEvent, 'hover');
    expect(trigger.tagName).toBe('BUTTON');
    expect(screen.getByText('Latency attribution')).toBeDefined();

    cleanup();
    const focused = renderCell(baseEvent, 'focus').trigger;
    expect(focused.tagName).toBe('BUTTON');
    expect(screen.getByText('Latency attribution')).toBeDefined();
  });

  it('renders renewal as one cycle without proxy attribution', () => {
    const { trigger } = renderCell({
      ...baseEvent,
      source_kind: 'renewal',
      duration_ms: 500,
    });

    expect(trigger.getAttribute('aria-label')).toBe(
      'Latency 500 ms. Renewal cycle 500 ms. Show breakdown',
    );
    expect(screen.getByText('Renewal cycle')).toBeDefined();
    expect(
      screen.getByText(/not a proxy request and is not split/),
    ).toBeDefined();
    expect(document.querySelector('[data-latency-category]')).toBeNull();
    expect(
      screen.getByRole('img', {
        name: 'Latency distribution: Renewal cycle 500 ms',
      }),
    ).toBeDefined();
  });
});
