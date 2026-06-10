import { cleanup, render, screen } from '@testing-library/react';
import { afterEach, expect, test, vi } from 'vitest';
import { RequestEventsTable } from '../components/ui/RequestEventsTable';
import type { RequestEvent } from '../lib/api';

afterEach(() => {
  cleanup();
});

vi.mock('../components/ui/primitives', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../components/ui/primitives')>();
  return {
    ...actual,
    Hint: ({ label, children }: any) => (
      <div>
        {children}
        <div data-testid="hint-label">{label}</div>
      </div>
    ),
  };
});

const mockEvent: RequestEvent = {
  ts: 1718000000,
  request_id: 'req-123',
  status: 200,
  duration_ms: 100,
  upstream: 'up-1',
  upstream_name: 'Upstream 1',
  model: 'claude-3-sonnet',
};

test('renders upstream without routing trace', () => {
  render(
    <RequestEventsTable
      events={[mockEvent]}
      principalNameMap={new Map()}
      upstreamNameMap={new Map()}
    />
  );
  expect(screen.getAllByText('Upstream 1').length).toBeGreaterThan(0);
});

test('renders upstream with routing trace hover panel', async () => {
  const eventWithTrace: RequestEvent = {
    ...mockEvent,
    routing_trace: {
      stages: [
        { stage_name: 'router', upstream_id: 'up-1', reason: 'matched rule' },
      ],
      terminal: { upstream_id: 'up-1', strategy: 'first-pick' },
    },
  };

  render(
    <RequestEventsTable
      events={[eventWithTrace]}
      principalNameMap={new Map()}
      upstreamNameMap={new Map()}
    />
  );

  const upstreamCell = screen.getAllByText('Upstream 1')[0];
  expect(upstreamCell).toBeTruthy();

  expect(screen.getByText('Routing Trace')).toBeTruthy();
  expect(screen.getByText('router')).toBeTruthy();
  expect(screen.getByText('matched rule')).toBeTruthy();
  expect(screen.getByText('Terminal')).toBeTruthy();
  expect(screen.getByText('Strategy: first-pick')).toBeTruthy();
});

test('renders status without internal errors', () => {
  render(
    <RequestEventsTable
      events={[mockEvent]}
      principalNameMap={new Map()}
      upstreamNameMap={new Map()}
    />
  );
  expect(screen.getAllByText('200').length).toBeGreaterThan(0);
});

test('renders status with internal errors hover panel', async () => {
  const eventWithErrors: RequestEvent = {
    ...mockEvent,
    status: 500,
    internal_errors: [
      { stage: 'router', kind: 'plugin_error', message: 'failed to route' },
    ],
  };

  render(
    <RequestEventsTable
      events={[eventWithErrors]}
      principalNameMap={new Map()}
      upstreamNameMap={new Map()}
    />
  );

  const statusCell = screen.getAllByText('500')[0];
  expect(statusCell).toBeTruthy();

  expect(screen.getByText('Internal Errors')).toBeTruthy();
  expect(screen.getByText('router')).toBeTruthy();
  expect(screen.getByText('plugin_error')).toBeTruthy();
  expect(screen.getByText('failed to route')).toBeTruthy();
});
