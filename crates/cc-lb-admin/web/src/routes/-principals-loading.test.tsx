import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { cleanup, render, screen } from '@testing-library/react';
import type React from 'react';
import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest';
import type { Principal } from '../lib/queries';
import * as queries from '../lib/queries';
import * as liveEvents from '../lib/useLiveEventStream';
import { RecentRequestsCard } from './principals';

vi.mock('../lib/queries', async () => {
  const actual = await vi.importActual<typeof queries>('../lib/queries');
  return {
    ...actual,
    usePrincipalNameMap: vi.fn(),
    useRecentEventsPage: vi.fn(),
  };
});

vi.mock('../lib/useLiveEventStream', async () => {
  const actual = await vi.importActual<typeof liveEvents>(
    '../lib/useLiveEventStream',
  );
  return {
    ...actual,
    useLiveEventStream: vi.fn(),
  };
});

const principal: Principal = {
  id: 'principal-a',
  name: 'Ada',
  kind: 'human',
  enabled: true,
  revision: 1,
  allowed_models: [],
  allowed_upstreams: [],
  default_limits: [],
  cache_keepalive: null,
};

const otherPrincipal: Principal = {
  ...principal,
  id: 'principal-b',
  name: 'Grace',
};

function recentEvent(requestId: string, principalId: string) {
  return {
    ts: 1_700_000_000,
    request_id: requestId,
    principal_id: principalId,
    event_kind: 'messages',
    status: 200,
    duration_ms: 25,
  };
}

const queryClient = new QueryClient({
  defaultOptions: { queries: { retry: false } },
});

function renderWithProviders(ui: React.ReactElement) {
  return render(
    <QueryClientProvider client={queryClient}>{ui}</QueryClientProvider>,
  );
}

function liveState() {
  return {
    eventsMap: new Map(),
    version: 0,
    status: 'live',
    lastActivityAt: null,
    lastCursor: null,
    error: null,
    malformedFrameCount: 0,
    permanentFailure: false,
    permanentFailureSince: null,
    reconnectAttempts: 0,
    forceReconnect: vi.fn(),
  } as never;
}

function pageState(
  events: readonly unknown[],
  overrides: Record<string, unknown> = {},
) {
  return {
    data: {
      events,
      observed: true,
      count: events.length,
      limit: 500,
    },
    isFetching: false,
    isPending: false,
    isPlaceholderData: false,
    error: null,
    ...overrides,
  } as never;
}

describe('principal recent-request feed', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    queryClient.clear();
    vi.mocked(queries.usePrincipalNameMap).mockReturnValue(
      new Map([
        [principal.id, principal.name],
        [otherPrincipal.id, otherPrincipal.name],
      ]),
    );
    vi.mocked(queries.useRecentEventsPage).mockReturnValue({
      data: undefined,
      isFetching: true,
      isPending: true,
      isPlaceholderData: false,
      error: null,
    } as never);
    vi.mocked(liveEvents.useLiveEventStream).mockReturnValue(liveState());
  });

  afterEach(() => cleanup());

  test('keeps the last successful rows visible while the same principal refreshes', async () => {
    let recent = pageState([recentEvent('request-old', principal.id)]);
    vi.mocked(queries.useRecentEventsPage).mockImplementation(
      () => recent as never,
    );

    const view = renderWithProviders(
      <RecentRequestsCard principal={principal} />,
    );

    expect(
      await screen.findByLabelText('View request request-old'),
    ).toBeDefined();

    recent = pageState([recentEvent('request-old', principal.id)], {
      isFetching: true,
      isPlaceholderData: true,
    });
    view.rerender(
      <QueryClientProvider client={queryClient}>
        <RecentRequestsCard principal={principal} />
      </QueryClientProvider>,
    );

    expect(screen.getByLabelText('View request request-old')).toBeDefined();

    recent = pageState([
      recentEvent('request-old', principal.id),
      recentEvent('request-new', principal.id),
    ]);
    view.rerender(
      <QueryClientProvider client={queryClient}>
        <RecentRequestsCard principal={principal} />
      </QueryClientProvider>,
    );

    expect(screen.getByLabelText('View request request-old')).toBeDefined();
    expect(screen.getByLabelText('View request request-new')).toBeDefined();
  });

  test('changes feed scope with the principal and never retains another principal locally', async () => {
    vi.mocked(queries.useRecentEventsPage).mockImplementation((filters) =>
      filters.principal_id === principal.id
        ? pageState([recentEvent('ada-request', principal.id)])
        : ({
            data: undefined,
            isFetching: true,
            isPending: true,
            isPlaceholderData: false,
            error: null,
          } as never),
    );

    const view = renderWithProviders(
      <RecentRequestsCard principal={principal} />,
    );
    expect(
      await screen.findByLabelText('View request ada-request'),
    ).toBeDefined();

    view.rerender(
      <QueryClientProvider client={queryClient}>
        <RecentRequestsCard principal={otherPrincipal} />
      </QueryClientProvider>,
    );

    expect(screen.queryByLabelText('View request ada-request')).toBeNull();
    const requestTable = screen.getByText('Timestamp').closest('table');
    expect(requestTable?.querySelectorAll('tbody tr')).toHaveLength(5);
  });
});
