import { cleanup, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it } from 'vitest';
import type { RequestEventWithPhase } from '../../lib/RequestEventTypes';
import { RequestEventDrawer } from './RequestEventDrawer';

describe('RequestEventDrawer', () => {
  afterEach(() => {
    cleanup();
  });

  it('updates terminal-only sections when transitioning from partial to final', () => {
    const partialEvent = {
      event_id: 'evt_1',
      request_id: 'req_1',
      ts: 1718553120,
      ts_ms: 1718553120000,
      status: 200,
      duration_ms: 0,
      _phase: 'partial',
    } satisfies RequestEventWithPhase;

    const { rerender } = render(
      <RequestEventDrawer
        event={partialEvent}
        principalName={null}
        onClose={() => {}}
      />,
    );

    // While partial, it should show "Live" badge and "In progress" status
    expect(screen.getByText('Live')).toBeDefined();
    expect(screen.getByText('In progress')).toBeDefined();

    // Update to final event
    const finalEvent = {
      ...partialEvent,
      _phase: 'final',
      duration_ms: 150,
      error_code: 'upstream_timeout',
    } satisfies RequestEventWithPhase;

    rerender(
      <RequestEventDrawer
        event={finalEvent}
        principalName={null}
        onClose={() => {}}
      />,
    );

    // "Live" badge and "In progress" should be gone
    expect(screen.queryByText('Live')).toBeNull();
    expect(screen.queryByText('In progress')).toBeNull();

    // Terminal sections should appear
    expect(screen.getByText('upstream_timeout')).toBeDefined();
    expect(screen.getAllByText('150 ms').length).toBeGreaterThan(0);
  });

  it('renders structured upstream failure details when present', () => {
    const event = {
      event_id: 'evt_2',
      request_id: 'req_2',
      ts: 1718553120,
      ts_ms: 1718553120000,
      status: 429,
      duration_ms: 150,
      error_code: 'upstream_4xx',
      upstream_error_type: 'rate_limit_error',
      upstream_error_message: 'forced fake rate limit response <markup>',
      _phase: 'final',
    } satisfies RequestEventWithPhase;

    render(
      <RequestEventDrawer
        event={event}
        principalName={null}
        onClose={() => {}}
      />,
    );

    expect(screen.getByText('Upstream Failure')).toBeDefined();
    expect(screen.getByText('rate_limit_error')).toBeDefined();
    expect(
      screen.getByText('forced fake rate limit response <markup>'),
    ).toBeDefined();
    expect(screen.getByText('upstream_4xx')).toBeDefined();
  });

  it('omits structured upstream failure details when absent', () => {
    const event = {
      event_id: 'evt_3',
      request_id: 'req_3',
      ts: 1718553120,
      ts_ms: 1718553120000,
      status: 200,
      duration_ms: 150,
      _phase: 'final',
    } satisfies RequestEventWithPhase;

    render(
      <RequestEventDrawer
        event={event}
        principalName={null}
        onClose={() => {}}
      />,
    );

    expect(screen.queryByText('Upstream Failure')).toBeNull();
  });

  it('renders structured upstream failure details when only type is present', () => {
    const event = {
      event_id: 'evt_4',
      request_id: 'req_4',
      ts: 1718553120,
      ts_ms: 1718553120000,
      status: 429,
      duration_ms: 150,
      error_code: 'upstream_4xx',
      upstream_error_type: 'rate_limit_error',
      _phase: 'final',
    } satisfies RequestEventWithPhase;

    render(
      <RequestEventDrawer
        event={event}
        principalName={null}
        onClose={() => {}}
      />,
    );

    expect(screen.getByText('Upstream Failure')).toBeDefined();
    expect(screen.getByText('rate_limit_error')).toBeDefined();
    expect(screen.getByText('upstream_4xx')).toBeDefined();
  });

  it('renders structured upstream failure details when only message is present', () => {
    const event = {
      event_id: 'evt_5',
      request_id: 'req_5',
      ts: 1718553120,
      ts_ms: 1718553120000,
      status: 429,
      duration_ms: 150,
      error_code: 'upstream_4xx',
      upstream_error_message: 'forced fake rate limit response',
      _phase: 'final',
    } satisfies RequestEventWithPhase;

    render(
      <RequestEventDrawer
        event={event}
        principalName={null}
        onClose={() => {}}
      />,
    );

    expect(screen.getByText('Upstream Failure')).toBeDefined();
    expect(screen.getByText('forced fake rate limit response')).toBeDefined();
    expect(screen.getByText('upstream_4xx')).toBeDefined();
  });

  it('renders Client disconnected and preserves numeric status for 499 + client_closed_request', () => {
    const event = {
      event_id: 'evt_499',
      request_id: 'req_499',
      ts: 1718553120,
      ts_ms: 1718553120000,
      status: 499,
      error_code: 'client_closed_request',
      duration_ms: 150,
      _phase: 'final',
    } satisfies RequestEventWithPhase;

    render(
      <RequestEventDrawer
        event={event}
        principalName={null}
        onClose={() => {}}
      />,
    );

    expect(screen.getByText('Client disconnected')).toBeDefined();
    expect(screen.getByText('499')).toBeDefined();
    expect(screen.getByText('client_closed_request')).toBeDefined();
  });

  it('renders numeric status for 499 with other error code', () => {
    const event = {
      event_id: 'evt_499_other',
      request_id: 'req_499_other',
      ts: 1718553120,
      ts_ms: 1718553120000,
      status: 499,
      error_code: 'other_error',
      duration_ms: 150,
      _phase: 'final',
    } satisfies RequestEventWithPhase;

    render(
      <RequestEventDrawer
        event={event}
        principalName={null}
        onClose={() => {}}
      />,
    );

    expect(screen.queryByText('Client disconnected')).toBeNull();
    expect(screen.getByText('499')).toBeDefined();
    expect(screen.getByText('other_error')).toBeDefined();
  });

  it('renders numeric status for 0/terminal_dropped', () => {
    const event = {
      event_id: 'evt_0',
      request_id: 'req_0',
      ts: 1718553120,
      ts_ms: 1718553120000,
      status: 0,
      error_code: 'terminal_dropped',
      duration_ms: 150,
      _phase: 'final',
    } satisfies RequestEventWithPhase;

    render(
      <RequestEventDrawer
        event={event}
        principalName={null}
        onClose={() => {}}
      />,
    );

    expect(screen.queryByText('Client disconnected')).toBeNull();
    expect(screen.getByText('0')).toBeDefined();
    expect(screen.getByText('terminal_dropped')).toBeDefined();
  });

  it('renders numeric status for 504/tower_timeout', () => {
    const event = {
      event_id: 'evt_504',
      request_id: 'req_504',
      ts: 1718553120,
      ts_ms: 1718553120000,
      status: 504,
      error_code: 'tower_timeout',
      duration_ms: 150,
      _phase: 'final',
    } satisfies RequestEventWithPhase;

    render(
      <RequestEventDrawer
        event={event}
        principalName={null}
        onClose={() => {}}
      />,
    );

    expect(screen.queryByText('Client disconnected')).toBeNull();
    expect(screen.getByText('504')).toBeDefined();
    expect(screen.getByText('tower_timeout')).toBeDefined();
  });

  it('applies wrapping classes to long values in KvRow', () => {
    const event = {
      event_id: 'evt_long',
      request_id: 'req_long_id_that_should_wrap_properly_in_narrow_viewports',
      ts: 1718553120,
      ts_ms: 1718553120000,
      status: 200,
      duration_ms: 150,
      model: 'claude-3-5-sonnet-20240620-very-long-model-name-that-should-wrap',
      upstream_name: 'anthropic-very-long-upstream-name-that-should-wrap',
      error_code: 'very_long_error_code_that_should_wrap_properly',
      _phase: 'final',
    } satisfies RequestEventWithPhase;

    render(
      <RequestEventDrawer
        event={event}
        principalName={null}
        onClose={() => {}}
      />,
    );

    const modelSpan = screen.getByText(
      'claude-3-5-sonnet-20240620-very-long-model-name-that-should-wrap',
    );
    expect(modelSpan.className).toContain('break-all');

    const upstreamSpan = screen.getByText(
      'anthropic-very-long-upstream-name-that-should-wrap',
    );
    expect(upstreamSpan.className).toContain('break-all');

    const errorSpan = screen.getByText(
      'very_long_error_code_that_should_wrap_properly',
    );
    expect(errorSpan.className).toContain('break-all');

    const requestIdSpan = screen.getByText(
      'req_long_id_that_should_wrap_properly_in_narrow_viewports',
    );
    expect(requestIdSpan.className).toContain('break-all');
  });

  it('hides Tokens and Cost when the request used no tokens (zero, not just null)', () => {
    const event = {
      event_id: 'evt_no_tokens',
      request_id: 'req_no_tokens',
      ts: 1718553120,
      ts_ms: 1718553120000,
      status: 400,
      duration_ms: 5,
      error_code: 'upstream_4xx',
      upstream_error_message: 'temperature: range: 0..1',
      input_tokens: 0,
      output_tokens: 0,
      cache_creation_input_tokens: 0,
      cache_read_input_tokens: 0,
      _phase: 'final',
    } satisfies RequestEventWithPhase;

    render(
      <RequestEventDrawer
        event={event}
        principalName={null}
        onClose={() => {}}
      />,
    );

    expect(screen.queryByRole('heading', { name: 'Tokens' })).toBeNull();
    expect(screen.queryByRole('heading', { name: 'Cost' })).toBeNull();
  });

  it('shows Tokens and Cost when the request used tokens', () => {
    const event = {
      event_id: 'evt_tokens',
      request_id: 'req_tokens',
      ts: 1718553120,
      ts_ms: 1718553120000,
      status: 200,
      duration_ms: 150,
      input_tokens: 12,
      output_tokens: 3,
      _phase: 'final',
    } satisfies RequestEventWithPhase;

    render(
      <RequestEventDrawer
        event={event}
        principalName={null}
        onClose={() => {}}
      />,
    );

    expect(screen.getByRole('heading', { name: 'Tokens' })).toBeDefined();
    expect(screen.getByRole('heading', { name: 'Cost' })).toBeDefined();
  });

  describe('Badges', () => {
    it('renders reasoning badge when thinking_budget_tokens is set', () => {
      const event = {
        event_id: 'evt_reasoning',
        request_id: 'req_reasoning',
        ts: 1718553120,
        ts_ms: 1718553120000,
        status: 200,
        duration_ms: 100,
        model: 'claude-3-5-sonnet',
        thinking_budget_tokens: 18000,
        _phase: 'final',
      } satisfies RequestEventWithPhase;

      render(
        <RequestEventDrawer
          event={event}
          principalName={null}
          onClose={() => {}}
        />,
      );

      expect(screen.getByText('high · 18000')).toBeDefined();
    });

    it('renders no reasoning badge when thinking_budget_tokens is omitted', () => {
      const event = {
        event_id: 'evt_no_reasoning',
        request_id: 'req_no_reasoning',
        ts: 1718553120,
        ts_ms: 1718553120000,
        status: 200,
        duration_ms: 100,
        model: 'claude-3-5-sonnet',
        _phase: 'final',
      } satisfies RequestEventWithPhase;

      render(
        <RequestEventDrawer
          event={event}
          principalName={null}
          onClose={() => {}}
        />,
      );

      expect(
        screen.queryByText(/low ·|medium ·|high ·|xhigh ·|max ·/),
      ).toBeNull();
    });

    it('renders fast badge when service_tier is priority', () => {
      const event = {
        event_id: 'evt_fast',
        request_id: 'req_fast',
        ts: 1718553120,
        ts_ms: 1718553120000,
        status: 200,
        duration_ms: 100,
        model: 'claude-3-5-sonnet',
        service_tier: 'priority',
        _phase: 'final',
      } satisfies RequestEventWithPhase;

      render(
        <RequestEventDrawer
          event={event}
          principalName={null}
          onClose={() => {}}
        />,
      );

      expect(screen.getByText('fast')).toBeDefined();
    });

    it('renders standard badge when service_tier is standard', () => {
      const event = {
        event_id: 'evt_standard',
        request_id: 'req_standard',
        ts: 1718553120,
        ts_ms: 1718553120000,
        status: 200,
        duration_ms: 100,
        model: 'claude-3-5-sonnet',
        service_tier: 'standard',
        _phase: 'final',
      } satisfies RequestEventWithPhase;

      render(
        <RequestEventDrawer
          event={event}
          principalName={null}
          onClose={() => {}}
        />,
      );

      expect(screen.getByText('standard')).toBeDefined();
    });

    it('renders batch badge when service_tier is batch', () => {
      const event = {
        event_id: 'evt_batch',
        request_id: 'req_batch',
        ts: 1718553120,
        ts_ms: 1718553120000,
        status: 200,
        duration_ms: 100,
        model: 'claude-3-5-sonnet',
        service_tier: 'batch',
        _phase: 'final',
      } satisfies RequestEventWithPhase;

      render(
        <RequestEventDrawer
          event={event}
          principalName={null}
          onClose={() => {}}
        />,
      );

      expect(screen.getByText('batch')).toBeDefined();
    });

    it('renders no fast badge when service_tier is omitted', () => {
      const event = {
        event_id: 'evt_no_tier',
        request_id: 'req_no_tier',
        ts: 1718553120,
        ts_ms: 1718553120000,
        status: 200,
        duration_ms: 100,
        model: 'claude-3-5-sonnet',
        _phase: 'final',
      } satisfies RequestEventWithPhase;

      render(
        <RequestEventDrawer
          event={event}
          principalName={null}
          onClose={() => {}}
        />,
      );

      expect(screen.queryByText('fast')).toBeNull();
      expect(screen.queryByText('standard')).toBeNull();
      expect(screen.queryByText('batch')).toBeNull();
    });
  });
});
