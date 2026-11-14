import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import {
  act,
  cleanup,
  fireEvent,
  type RenderOptions,
  render as rtlRender,
  screen,
  waitFor,
} from '@testing-library/react';
import type { ReactElement } from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { RequestEventWithPhase } from '../../lib/RequestEventTypes';
import { RequestEventDrawer } from './RequestEventDrawer';

function render(ui: ReactElement, options?: RenderOptions) {
  const queryClient = new QueryClient();
  return rtlRender(ui, {
    wrapper: ({ children }) => (
      <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    ),
    ...options,
  });
}

function jsonResponse(body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status: 200,
    headers: { 'Content-Type': 'application/json' },
  });
}

describe('RequestEventDrawer', () => {
  beforeEach(() => {
    vi.stubGlobal(
      'fetch',
      vi.fn(() => Promise.reject(new Error('network disabled in test'))),
    );
  });

  afterEach(() => {
    cleanup();
    vi.unstubAllGlobals();
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

    // Terminal sections should appear. A 2xx carrying an error_code is an
    // abnormal outcome, so the code shows twice: once in the outcome badge and
    // once as the KvRow value. Pin the count so losing either one fails here.
    expect(screen.getAllByText('upstream_timeout')).toHaveLength(2);
    expect(screen.getAllByText('150 ms').length).toBeGreaterThan(0);
  });

  it('keeps same-request timeline and thinking level visible until final detail arrives', async () => {
    let resolveDetail: ((response: Response) => void) | undefined;
    vi.stubGlobal(
      'fetch',
      vi.fn(
        () =>
          new Promise<Response>((resolve) => {
            resolveDetail = resolve;
          }),
      ),
    );
    const partialEvent = {
      event_id: 'evt_progressive_detail',
      request_id: 'req_progressive_detail',
      ts: 1718553120,
      ts_ms: 1718553120000,
      elapsed_ms: 120,
      auth_ms: 10,
      route_ms: 5,
      upstream_ttfb_ms: 80,
      thinking_level: 'low',
      thinking_budget_tokens: 18000,
      _phase: 'partial',
    } satisfies RequestEventWithPhase;
    const { rerender } = render(
      <RequestEventDrawer
        event={partialEvent}
        principalName={null}
        onClose={() => {}}
      />,
    );

    const partialTimeline = screen.getByTestId('latency-timeline-region');
    expect(screen.getByText('Internal pre')).toBeDefined();
    expect(partialTimeline.textContent).toContain('Upstream');

    const finalSlimEvent = {
      event_id: 'evt_progressive_detail',
      request_id: 'req_progressive_detail',
      ts: 1718553120,
      ts_ms: 1718553120000,
      status: 200,
      duration_ms: 150,
      _phase: 'final',
    } satisfies RequestEventWithPhase;
    rerender(
      <RequestEventDrawer
        event={finalSlimEvent}
        principalName={null}
        onClose={() => {}}
      />,
    );

    const pendingTimeline = screen.getByTestId('latency-timeline-region');
    expect(pendingTimeline).toBe(partialTimeline);
    expect(pendingTimeline.querySelectorAll('.skeleton')).toHaveLength(0);
    expect(screen.getByText('Internal pre')).toBeDefined();
    expect(pendingTimeline.textContent).toContain('Upstream');
    expect(
      screen.getByText('Thinking level').parentElement?.textContent,
    ).toContain('low');

    await waitFor(() => expect(resolveDetail).toBeDefined());
    const resolvePendingDetail = resolveDetail;
    if (!resolvePendingDetail) throw new Error('Expected detail request');
    await act(async () => {
      resolvePendingDetail(
        jsonResponse({
          ...finalSlimEvent,
          duration_ms: 200,
          auth_ms: 20,
          route_ms: 5,
          upstream_ttfb_ms: 90,
          upstream_body_ms: 40,
          body_bytes: 2048,
          thinking_level: 'high',
          thinking_tokens: 8200,
          service_tier: 'priority',
        }),
      );
      await Promise.resolve();
    });

    await waitFor(() => {
      const finalTimeline = screen.getByTestId('latency-timeline-region');
      expect(finalTimeline).toBe(partialTimeline);
      expect(finalTimeline.textContent).toContain('90 ms');
      expect(finalTimeline.textContent).toContain('40 ms');
    });
    expect(screen.getAllByText('200 ms').length).toBeGreaterThan(0);
    const bodyRow = screen.getByText('Body bytes').parentElement;
    expect(bodyRow?.querySelectorAll('.skeleton')).toHaveLength(0);
    expect(bodyRow?.textContent).toContain('2.0 KB');
    expect(
      screen.getByText('Thinking level').parentElement?.textContent,
    ).toContain('high');
    expect(screen.getByText('Reasoning').parentElement?.textContent).toContain(
      'high · 8.2k',
    );
    expect(
      screen.getByText('Service tier').parentElement?.textContent,
    ).toContain('priority');
  });

  it('does not speculate upstream failure while detail-only fields are pending', () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(() => new Promise<Response>(() => {})),
    );
    const event = {
      event_id: 'evt_pending_detail',
      request_id: 'req_pending_detail',
      ts: 1718553120,
      ts_ms: 1718553120000,
      principal_id: 'principal-1',
      upstream_name: 'anthropic',
      model: 'claude-sonnet',
      status: 502,
      duration_ms: 150,
      error_code: 'upstream_5xx',
      _phase: 'final',
    } satisfies RequestEventWithPhase;

    render(
      <RequestEventDrawer
        event={event}
        principalName="Principal One"
        onClose={() => {}}
      />,
    );

    expect(screen.getByText('req_pending_detail')).toBeDefined();
    expect(screen.getAllByText('Principal One').length).toBeGreaterThan(0);
    expect(screen.getByText('anthropic')).toBeDefined();
    expect(screen.getByText('claude-sonnet')).toBeDefined();
    expect(screen.getAllByText('150 ms').length).toBeGreaterThan(0);

    const principalRow = screen.getByText('Principal').parentElement;
    expect(principalRow?.querySelectorAll('.skeleton')).toHaveLength(1);

    const keyRow = screen.getByText('Key ID').parentElement;
    expect(keyRow?.querySelectorAll('.skeleton')).toHaveLength(1);
    expect(keyRow?.textContent).not.toContain('—');

    expect(
      screen.queryByRole('heading', { name: 'Upstream failure' }),
    ).toBeNull();

    const bodyRow = screen.getByText('Body bytes').parentElement;
    expect(bodyRow?.querySelectorAll('.skeleton')).toHaveLength(1);

    const latency = screen.getByTestId('latency-timeline-region');
    expect(latency.getAttribute('aria-busy')).toBeNull();
    expect(latency.querySelectorAll('.skeleton')).toHaveLength(0);
    expect(screen.queryByText('No latency data recorded.')).toBeNull();

    const dialog = screen.getByRole('dialog', { name: 'Request detail' });
    expect(dialog.className).toContain('w-full');
    expect(dialog.className).toContain('max-w-lg');
  });

  it('does not reuse a previous request timeline for a new request', () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(() => new Promise<Response>(() => {})),
    );
    const firstEvent = {
      event_id: 'evt_first',
      request_id: 'req_first',
      ts: 1718553120,
      ts_ms: 1718553120000,
      status: 200,
      duration_ms: 400,
      auth_ms: 20,
      upstream_ttfb_ms: 200,
      _phase: 'final',
    } satisfies RequestEventWithPhase;
    const { rerender } = render(
      <RequestEventDrawer
        event={firstEvent}
        principalName={null}
        onClose={() => {}}
      />,
    );
    expect(screen.getByText('Internal pre')).toBeDefined();

    const newEvent = {
      event_id: 'evt_new',
      request_id: 'req_new',
      ts: 1718553130,
      ts_ms: 1718553130000,
      status: 200,
      duration_ms: 0,
      _phase: 'final',
    } satisfies RequestEventWithPhase;
    rerender(
      <RequestEventDrawer
        event={newEvent}
        principalName={null}
        onClose={() => {}}
      />,
    );

    const latency = screen.getByTestId('latency-timeline-region');
    expect(latency.getAttribute('aria-busy')).toBe('true');
    expect(latency.querySelectorAll('.skeleton').length).toBeGreaterThan(0);
    expect(screen.queryByText('Internal pre')).toBeNull();
  });

  it('renders known upstream failure details immediately while detail is pending', () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(() => new Promise<Response>(() => {})),
    );

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

    expect(screen.getByText('Upstream failure')).toBeDefined();
    expect(screen.getByText('rate_limit_error')).toBeDefined();
    expect(
      screen.getByText('forced fake rate limit response <markup>'),
    ).toBeDefined();
    expect(screen.getByText('upstream_4xx')).toBeDefined();
    expect(screen.getByText('429')).toBeDefined();
  });

  it('explains a known error code with a next step linking to the principal router', () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(() => new Promise<Response>(() => {})),
    );
    const event = {
      event_id: 'evt_route',
      request_id: 'req_route',
      principal_id: 'principal-7',
      ts: 1718553120,
      ts_ms: 1718553120000,
      status: 503,
      duration_ms: 3,
      error_code: 'route_no_upstream_after_filter',
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
      screen.getByText(
        'The router’s filters removed every upstream, so none could serve the request.',
      ),
    ).toBeDefined();
    const link = screen.getByRole('link', { name: 'Open principal router' });
    expect(link.getAttribute('href')).toBe(
      '/principals?selectedId=principal-7#principal-router',
    );
    expect(screen.getByTestId('request-error-code').textContent).toBe(
      'route_no_upstream_after_filter',
    );
  });

  it('keeps an unknown error code visible behind a generic sentence', () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(() => new Promise<Response>(() => {})),
    );
    const event = {
      event_id: 'evt_unknown',
      request_id: 'req_unknown',
      ts: 1718553120,
      ts_ms: 1718553120000,
      status: 500,
      duration_ms: 40,
      error_code: 'brand_new_failure',
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
      screen.getByText(
        'The request ended with an error the dashboard has no description for.',
      ),
    ).toBeDefined();
    expect(screen.getByTestId('request-error-code').textContent).toBe(
      'brand_new_failure',
    );
    expect(screen.queryByRole('link')).toBeNull();
  });

  it('shows a final SSE error as the outcome while preserving HTTP 200', () => {
    const event = {
      event_id: 'evt_stream_error',
      request_id: 'req_stream_error',
      ts: 1718553120,
      ts_ms: 1718553120000,
      status: 200,
      duration_ms: 12,
      error_code: 'upstream_stream_error',
      upstream_error_type: 'overloaded_error',
      upstream_error_message: 'Overloaded',
      _phase: 'final',
    } satisfies RequestEventWithPhase;

    render(
      <RequestEventDrawer
        event={event}
        principalName={null}
        onClose={() => {}}
      />,
    );

    expect(screen.getAllByText('overloaded_error').length).toBeGreaterThan(0);
    expect(screen.getByText('HTTP status')).toBeDefined();
    expect(screen.getByText('200')).toBeDefined();
    expect(screen.getByText('upstream_stream_error')).toBeDefined();
    expect(screen.getByText('Upstream failure')).toBeDefined();
    expect(screen.getByText('Overloaded')).toBeDefined();
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

    expect(screen.queryByText('Upstream failure')).toBeNull();
  });

  it('renders Claude session identity and explicit request kind', () => {
    const event = {
      event_id: 'evt_identity',
      request_id: 'req_identity',
      ts: 1718553120,
      status: 200,
      duration_ms: 150,
      thread_id: 'session-thread',
      observed_session_id: 'session-observed',
      request_kind: 'subagent',
      claude_agent_id: 'agent-a',
      claude_parent_agent_id: 'agent-parent',
      parent_session_id: 'session-parent',
      client_app: 'cli-bg',
      session_id_source: 'x-claude-code-session-id',
      _phase: 'final',
    } satisfies RequestEventWithPhase;

    render(
      <RequestEventDrawer
        event={event}
        principalName={null}
        onClose={() => {}}
      />,
    );

    expect(screen.getByText('Observed session')).toBeDefined();
    expect(screen.getByTitle('session-observed')).toBeDefined();
    expect(screen.getByLabelText('Copy observed session id')).toBeDefined();
    const sessionRow = screen.getByText('Session').parentElement;
    expect(sessionRow?.textContent).not.toContain('sub');
    const requestKindRow = screen.getByText('Request kind').parentElement;
    expect(requestKindRow?.textContent).toContain('subagent');
    expect(screen.getByText('Session source')).toBeDefined();
    expect(screen.getByText('x-claude-code-session-id')).toBeDefined();
    expect(screen.getByText('Parent session')).toBeDefined();
    expect(screen.getByTitle('session-parent')).toBeDefined();
    expect(screen.getByLabelText('Copy parent session id')).toBeDefined();
    expect(screen.getByText('Agent')).toBeDefined();
    expect(screen.getByText('agent-a')).toBeDefined();
    expect(screen.getByLabelText('Copy agent id')).toBeDefined();
    expect(screen.getByText('Parent agent')).toBeDefined();
    expect(screen.getByText('agent-parent')).toBeDefined();
    expect(screen.getByLabelText('Copy parent agent id')).toBeDefined();
    expect(screen.getByText('Client app')).toBeDefined();
    expect(screen.getByText('cli-bg')).toBeDefined();
  });

  it('passes OpenAI provider context to latency explanations', async () => {
    const event = {
      event_id: 'evt_openai_latency',
      request_id: 'req_openai_latency',
      ts: 1718553120,
      ts_ms: 1718553120000,
      status: 200,
      duration_ms: 1000,
      source_kind: 'proxy',
      event_kind: 'openai_responses',
      shape_ms: 25,
      upstream_ttfb_ms: 100,
      stream_message_start_ms: 5,
      stream_content_block_start_ms: 15,
      stream_first_content_delta_ms: 20,
      stream_last_content_delta_ms: 600,
      stream_message_stop_ms: 650,
      stream_last_chunk_ms: 700,
      stream_total_ms: 750,
      sse_event_count: 40,
      content_delta_count: 30,
      ping_count: 2,
      inter_token_avg_ms: 20,
      _phase: 'final',
    } satisfies RequestEventWithPhase;

    vi.stubGlobal(
      'fetch',
      vi.fn(() => Promise.resolve(jsonResponse(event))),
    );

    render(
      <RequestEventDrawer
        event={event}
        principalName={null}
        onClose={() => {}}
      />,
    );

    const shapeButton = screen.getByRole('button', { name: /^Shape 25 ms$/ });
    fireEvent.click(shapeButton);
    const shapeDescription = await screen.findByText(
      /shape the outbound body for OpenAI/i,
    );
    expect(shapeDescription.textContent).not.toMatch(/Anthropic/i);
    fireEvent.click(shapeButton);
    await waitFor(() =>
      expect(
        screen.queryByText(/shape the outbound body for OpenAI/i),
      ).toBeNull(),
    );

    const markerButton = screen.getByRole('button', {
      name: /^First content delta \(TTFT\) \d+ ms$/,
    });
    fireEvent.click(markerButton);
    const markerDescription = await screen.findByText(
      /First streamed content delta/i,
    );
    expect(markerDescription.textContent).not.toMatch(/Anthropic|event:/i);
  });

  it('uses upstream provider context for shared latency event kinds', async () => {
    const event = {
      event_id: 'evt_shared_latency',
      request_id: 'req_shared_latency',
      ts: 1718553120,
      ts_ms: 1718553120000,
      upstream_id: 'upstream-shared',
      status: 200,
      duration_ms: 1000,
      source_kind: 'proxy',
      event_kind: 'chat_completions',
      shape_ms: 25,
      _phase: 'final',
    } satisfies RequestEventWithPhase;

    vi.stubGlobal(
      'fetch',
      vi.fn(() => Promise.resolve(jsonResponse(event))),
    );

    render(
      <RequestEventDrawer
        event={event}
        principalName={null}
        onClose={() => {}}
        providerByUpstream={new Map([['upstream-shared', 'openai']])}
      />,
    );

    fireEvent.click(screen.getByRole('button', { name: /^Shape 25 ms$/ }));
    const shapeDescription = await screen.findByText(
      /shape the outbound body for OpenAI/i,
    );
    expect(shapeDescription.textContent).not.toMatch(/selected upstream/i);
  });

  it('omits Claude session identity rows when absent', () => {
    const event = {
      event_id: 'evt_identity_absent',
      request_id: 'req_identity_absent',
      ts: 1718553120,
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

    expect(screen.queryByText('Session source')).toBeNull();
    expect(screen.getByText('Request kind').parentElement?.textContent).toBe(
      'Request kind—',
    );
    expect(screen.queryByText('Parent session')).toBeNull();
    expect(screen.queryByText('Agent')).toBeNull();
    expect(screen.queryByText('Parent agent')).toBeNull();
    expect(screen.queryByText('Client app')).toBeNull();
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

    expect(screen.getByText('Upstream failure')).toBeDefined();
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

    expect(screen.getByText('Upstream failure')).toBeDefined();
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

    // A 2xx row carrying an error_code now also renders the abnormal-outcome
    // badge, so scope to the monospaced KvRow value this test is about.
    const errorSpan = screen
      .getAllByText('very_long_error_code_that_should_wrap_properly')
      .find((el) => el.className.includes('font-mono'));
    expect(errorSpan?.className).toContain('break-all');

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
    expect(screen.getByText('List price unavailable')).toBeDefined();
    expect(screen.queryByText('No cost')).toBeNull();
  });

  it.each([
    [null, 'List price unavailable'],
    [0, 'No cost'],
    [2_500_000, 'Per-category cost not recorded'],
  ] as const)(
    'preserves cost truth after fetching full request detail: %s',
    async (cost_usd_micros, caption) => {
      const event = {
        event_id: 'pricing-detail',
        request_id: 'pricing-detail',
        ts: 1718553120,
        status: 200,
        duration_ms: 10,
        input_tokens: 100,
        output_tokens: 10,
        cost_usd_micros,
        _phase: 'final',
      } satisfies RequestEventWithPhase;
      vi.stubGlobal(
        'fetch',
        vi.fn(() => Promise.resolve(jsonResponse(event))),
      );
      render(
        <RequestEventDrawer
          event={event}
          principalName={null}
          onClose={() => {}}
        />,
      );
      await waitFor(() => expect(screen.getByText(caption)).toBeDefined());
      expect(screen.getByRole('heading', { name: /^Cost$/ })).toBeDefined();
      if (cost_usd_micros == null) {
        expect(screen.queryByText('$0.0000')).toBeNull();
        expect(screen.queryByText('No cost')).toBeNull();
      } else if (cost_usd_micros > 0) {
        expect(screen.getByText('$2.5000')).toBeDefined();
        expect(screen.queryByText('No cost')).toBeNull();
      }
    },
  );

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

      expect(screen.getByText('high')).toBeDefined();
    });

    it('renders reasoning badge when reasoning_effort and thinking_tokens are set', () => {
      const event = {
        event_id: 'evt_reasoning_effort',
        request_id: 'req_reasoning_effort',
        ts: 1718553120,
        ts_ms: 1718553120000,
        status: 200,
        duration_ms: 100,
        model: 'claude-3-5-sonnet',
        reasoning_effort: 'max',
        thinking_tokens: 8200,
        _phase: 'final',
      } satisfies RequestEventWithPhase;

      render(
        <RequestEventDrawer
          event={event}
          principalName={null}
          onClose={() => {}}
        />,
      );

      expect(screen.getByText('max · 8.2k')).toBeDefined();
    });

    it('renders reasoning badge when thinking_budget_tokens and thinking_tokens are set', () => {
      const event = {
        event_id: 'evt_reasoning_budget_tokens',
        request_id: 'req_reasoning_budget_tokens',
        ts: 1718553120,
        ts_ms: 1718553120000,
        status: 200,
        duration_ms: 100,
        model: 'claude-3-5-sonnet',
        thinking_budget_tokens: 18000,
        thinking_tokens: 8200,
        _phase: 'final',
      } satisfies RequestEventWithPhase;

      render(
        <RequestEventDrawer
          event={event}
          principalName={null}
          onClose={() => {}}
        />,
      );

      expect(screen.getByText('high · 8.2k')).toBeDefined();
    });
    it('renders Gemini thinking level separately from budget and observed service tier', () => {
      const event = {
        event_id: 'evt_gemini_metadata',
        request_id: 'req_gemini_metadata',
        ts: 1718553120,
        ts_ms: 1718553120000,
        status: 200,
        duration_ms: 100,
        model: 'gemini-2.5-pro',
        thinking_level: 'high',
        thinking_budget_tokens: 18000,
        thinking_tokens: 8200,
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

      expect(
        screen.getByText('Thinking level').parentElement?.textContent,
      ).toContain('high');
      expect(
        screen.getByText('Reasoning').parentElement?.textContent,
      ).toContain('high · 8.2k');
      expect(
        screen.getByText('Service tier').parentElement?.textContent,
      ).toContain('priority');
    });
    it.each([
      ['claude-sonnet-4-5', 'max', undefined],
      ['gpt-5', 'high', undefined],
      ['gemini-2.5-pro', undefined, null],
    ] as const)(
      'keeps existing reasoning for %s without inventing a thinking level',
      (model, reasoningEffort, thinkingLevel) => {
        const event = {
          event_id: 'evt_no_level',
          request_id: 'req_no_level',
          ts: 1718553120,
          status: 200,
          duration_ms: 100,
          model,
          thinking_level: thinkingLevel,
          reasoning_effort: reasoningEffort,
          thinking_budget_tokens: 18000,
          thinking_tokens: 8200,
          _phase: 'final',
        } satisfies RequestEventWithPhase;

        render(
          <RequestEventDrawer
            event={event}
            principalName={null}
            onClose={() => {}}
          />,
        );

        expect(screen.queryByText('Thinking level')).toBeNull();
        expect(
          screen.getByText('Reasoning').parentElement?.textContent,
        ).toContain(`${reasoningEffort ?? 'high'} · 8.2k`);
      },
    );

    it('renders no reasoning badge when thinking_budget_tokens, reasoning_effort, and thinking_tokens are omitted', () => {
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

    it('renders priority badge when service_tier is priority', () => {
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

      expect(screen.getByText('priority')).toBeDefined();
    });

    it('does not render a badge when service_tier is standard', () => {
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

      expect(screen.queryByText('standard')).toBeNull();
      expect(screen.queryByText('priority')).toBeNull();
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
      expect(screen.queryByText('priority')).toBeNull();
    });

    it('renders flex badge when service_tier is flex', () => {
      const event = {
        event_id: 'evt_flex',
        request_id: 'req_flex',
        ts: 1718553120,
        ts_ms: 1718553120000,
        status: 200,
        duration_ms: 100,
        model: 'claude-3-5-sonnet',
        service_tier: 'flex',
        _phase: 'final',
      } satisfies RequestEventWithPhase;

      render(
        <RequestEventDrawer
          event={event}
          principalName={null}
          onClose={() => {}}
        />,
      );

      expect(screen.getByText('flex')).toBeDefined();
      expect(screen.queryByText('priority')).toBeNull();
    });

    it('renders no priority badge when service_tier is omitted', () => {
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

      expect(screen.queryByText('priority')).toBeNull();
      expect(screen.queryByText('standard')).toBeNull();
      expect(screen.queryByText('batch')).toBeNull();
    });
  });
});
