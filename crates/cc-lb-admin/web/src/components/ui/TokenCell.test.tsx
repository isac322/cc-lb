import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it } from 'vitest';
import type { RequestEventWithPhase } from '../../lib/RequestEventTypes';
import { TokenCell } from './TokenCell';

afterEach(cleanup);

const baseEvent = {
  ts: 1,
  request_id: 'req-token',
  status: 200,
  duration_ms: 100,
  _phase: 'final',
} satisfies RequestEventWithPhase;

function renderTokenCell(event: RequestEventWithPhase) {
  render(
    <table>
      <tbody>
        <tr>
          <TokenCell event={event} />
        </tr>
      </tbody>
    </table>,
  );

  return screen.getByRole('button', { name: /Tokens .*show breakdown/ });
}

function breakdownRow(label: string) {
  const row = screen.getByText(label).parentElement;
  if (!row) throw new Error(`Expected breakdown row for ${label}`);
  return row;
}

describe('TokenCell totals and cache hit', () => {
  it('counts native cache writes once even when retention buckets are also present', () => {
    const trigger = renderTokenCell({
      ...baseEvent,
      input_tokens: 900,
      output_tokens: 50,
      cache_read_input_tokens: 200,
      cache_write_input_tokens: 300,
      cache_write_input_tokens_30m: 200,
      cache_write_input_tokens_12h: 100,
    });

    expect(trigger.getAttribute('aria-label')).toMatch(/1\.4k in/);
    expect(trigger.getAttribute('aria-label')).toMatch(/50 out/);
    expect(trigger.getAttribute('aria-label')).toMatch(/cache hit 14%/);

    fireEvent.click(trigger);

    expect(breakdownRow('Cache read').textContent).toContain('200');
    expect(breakdownRow('Cache write 30m').textContent).toContain('200');
    expect(breakdownRow('Cache write 12h').textContent).toContain('100');
    expect(breakdownRow('Input').textContent).toContain('900');
  });

  it('shows a zero cache hit for a write-only prompt', () => {
    const trigger = renderTokenCell({
      ...baseEvent,
      cache_write_input_tokens: 300,
    });

    expect(trigger.getAttribute('aria-label')).toMatch(/300 in/);
    expect(trigger.getAttribute('aria-label')).toMatch(/cache hit 0%/);
  });

  it('does not add retention buckets to already normalized input tokens', () => {
    const trigger = renderTokenCell({
      ...baseEvent,
      input_tokens: 1_000,
      cache_read_input_tokens: 200,
      cache_write_input_tokens_30m: 200,
      cache_write_input_tokens_12h: 100,
    });

    expect(trigger.getAttribute('aria-label')).toMatch(/1.2k in/);
    expect(trigger.getAttribute('aria-label')).toMatch(/cache hit 16%/);
  });

  it('preserves Anthropic five-minute and one-hour cache creation counts', () => {
    const trigger = renderTokenCell({
      ...baseEvent,
      input_tokens: 900,
      output_tokens: 50,
      cache_read_input_tokens: 200,
      cache_creation_input_tokens_5m: 200,
      cache_creation_input_tokens_1h: 100,
    });

    expect(trigger.getAttribute('aria-label')).toMatch(/1.4k in/);
    expect(trigger.getAttribute('aria-label')).toMatch(/cache hit 14%/);

    fireEvent.click(trigger);

    expect(breakdownRow('Cache create 5m').textContent).toContain('200');
    expect(breakdownRow('Cache create 1h').textContent).toContain('100');
  });
});

describe('TokenCell empty and no-cache states', () => {
  it('shows no hit when the request has no token observations', () => {
    const trigger = renderTokenCell(baseEvent);
    const label = trigger.getAttribute('aria-label') ?? '';

    expect(label).toMatch(/0 in/);
    expect(label).toMatch(/0 out/);
    expect(label).not.toContain('cache hit');
    expect(screen.getByText('—')).toBeDefined();
  });

  it('shows zero percent when prompt input is present without a cache read', () => {
    const trigger = renderTokenCell({
      ...baseEvent,
      input_tokens: 900,
      output_tokens: 0,
    });

    expect(trigger.getAttribute('aria-label')).toMatch(/900 in/);
    expect(trigger.getAttribute('aria-label')).toMatch(/cache hit 0%/);
  });
});
