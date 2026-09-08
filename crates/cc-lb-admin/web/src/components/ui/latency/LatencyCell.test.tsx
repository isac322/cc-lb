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

describe('LatencyCell setup timing breakdown', () => {
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
});
