import { cleanup, render, screen } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest';
import type { Principal } from '../lib/queries';
import * as queries from '../lib/queries';
import { RecentRequestsCard } from './principals';

vi.mock('../lib/queries', async () => {
  const actual = await vi.importActual<typeof queries>('../lib/queries');
  return {
    ...actual,
    usePrincipalNameMap: vi.fn(),
    useRecentEvents: vi.fn(),
    useUpstreamNameMap: vi.fn(),
  };
});

vi.mock('../components/ui/RequestEventsTable', () => ({
  RequestEventsTable: ({
    events,
    loading,
  }: {
    events: ReadonlyArray<{ request_id: string }>;
    loading?: boolean;
  }) => (
    <div
      data-loading={loading ? 'true' : 'false'}
      data-testid="recent-requests-table"
    >
      {events.map((event) => (
        <span key={event.request_id}>{event.request_id}</span>
      ))}
    </div>
  ),
}));

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
    status: 200,
    duration_ms: 25,
  };
}

describe('principal recent-request polling', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(queries.usePrincipalNameMap).mockReturnValue(
      new Map([
        [principal.id, principal.name],
        [otherPrincipal.id, otherPrincipal.name],
      ]),
    );
    vi.mocked(queries.useUpstreamNameMap).mockReturnValue(new Map());
  });

  afterEach(() => cleanup());

  test('keeps the last successful rows visible while the same principal refreshes', () => {
    const initial = {
      events: [recentEvent('request-old', principal.id)],
      observed: true,
      count: 1,
      limit: 5,
    };
    let recent = {
      data: initial,
      isFetching: false,
      isPending: false,
      isPlaceholderData: false,
    };
    vi.mocked(queries.useRecentEvents).mockImplementation(
      () => recent as never,
    );

    const view = render(<RecentRequestsCard principal={principal} />);

    expect(screen.getByText('request-old')).toBeDefined();
    expect(screen.getByTestId('recent-requests-table').dataset.loading).toBe(
      'false',
    );
    expect(screen.getByText('Last 1 from Ada')).toBeDefined();

    recent = {
      data: initial,
      isFetching: true,
      isPending: false,
      isPlaceholderData: true,
    };
    view.rerender(<RecentRequestsCard principal={principal} />);

    expect(screen.getByText('request-old')).toBeDefined();
    expect(screen.getByTestId('recent-requests-table').dataset.loading).toBe(
      'false',
    );
    expect(
      screen.queryByTestId('recent-requests-subtitle-skeleton'),
    ).toBeNull();

    recent = {
      data: {
        events: [recentEvent('request-new', principal.id)],
        observed: true,
        count: 1,
        limit: 5,
      },
      isFetching: false,
      isPending: false,
      isPlaceholderData: false,
    };
    view.rerender(<RecentRequestsCard principal={principal} />);

    expect(screen.getByText('request-new')).toBeDefined();
    expect(screen.queryByText('request-old')).toBeNull();
  });

  test('changes query identity with the principal and never retains another principal locally', () => {
    const states: Record<string, unknown> = {
      [principal.id]: {
        data: {
          events: [recentEvent('ada-request', principal.id)],
          observed: true,
          count: 1,
          limit: 5,
        },
        isFetching: false,
        isPending: false,
        isPlaceholderData: false,
      },
      [otherPrincipal.id]: {
        data: undefined,
        isFetching: true,
        isPending: true,
        isPlaceholderData: false,
      },
    };
    vi.mocked(queries.useRecentEvents).mockImplementation(
      (filters) => states[filters.principal_id ?? ''] as never,
    );

    const view = render(<RecentRequestsCard principal={principal} />);
    expect(screen.getByText('ada-request')).toBeDefined();

    view.rerender(<RecentRequestsCard principal={otherPrincipal} />);

    expect(queries.useRecentEvents).toHaveBeenLastCalledWith({
      principal_id: otherPrincipal.id,
      limit: '5',
    });
    expect(screen.queryByText('ada-request')).toBeNull();
    expect(screen.getByTestId('recent-requests-table').dataset.loading).toBe(
      'true',
    );
    expect(
      screen.getByTestId('recent-requests-subtitle-skeleton'),
    ).toBeDefined();
  });
});
