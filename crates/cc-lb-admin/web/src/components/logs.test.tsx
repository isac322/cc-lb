import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { filterLogRows } from '../lib/logRows';
import type { RequestEventWithPhase } from '../lib/RequestEventTypes';
import { Route } from '../routes/logs';

const mockEvents = Array.from({ length: 120 }, (_, i) => ({
  request_id: `req-${i}`,
  thread_id: i % 2 === 0 ? 'session-a' : 'session-b',
  timestamp: new Date().toISOString(),
  model: 'claude-3',
  status: 200,
  tokens: 100,
  cost: 0.01,
}));

vi.mock('../lib/queries', () => ({
  useUpstreams: () => ({ data: { upstreams: [] } }),
  usePrincipalNameMap: () => new Map(),
  useUpstreamNameMap: () => new Map(),
  useRecentEventsInfinite: () => ({
    data: { pages: [{ events: mockEvents }] },
    hasNextPage: false,
    isFetchingNextPage: false,
    isPlaceholderData: false,
    refetch: vi.fn(),
  }),
}));

vi.mock('../lib/useLiveEventStream', () => ({
  useLiveEventStream: () => ({
    eventsMap: new Map(),
    version: 0,
    status: 'idle',
    permanentFailure: false,
  }),
}));

vi.mock('@tanstack/react-router', async (importOriginal) => {
  const actual =
    await importOriginal<typeof import('@tanstack/react-router')>();
  return {
    ...actual,
    useNavigate: () => vi.fn(),
  };
});

global.URL.createObjectURL = vi.fn(() => 'blob:test');
global.URL.revokeObjectURL = vi.fn();

global.IntersectionObserver = class IntersectionObserver {
  observe() {}
  unobserve() {}
  disconnect() {}
} as unknown as typeof global.IntersectionObserver;

describe('LogsPage', () => {
  afterEach(() => {
    cleanup();
    vi.restoreAllMocks();
  });

  it('applies every row filter individually and in combination', () => {
    const target = {
      ts: null,
      request_id: 'target',
      principal_id: 'principal-target',
      upstream_id: 'upstream-target',
      thread_id: 'thread-target',
      model: 'model-target',
      status: 429,
      duration_ms: 0,
      source_kind: 'renewal',
      _phase: 'final',
    } as RequestEventWithPhase;
    const rows = [
      target,
      {
        ...target,
        request_id: 'wrong-principal',
        principal_id: 'principal-other',
      },
      {
        ...target,
        request_id: 'wrong-upstream',
        upstream_id: 'upstream-other',
      },
      { ...target, request_id: 'wrong-session', thread_id: 'thread-other' },
      { ...target, request_id: 'wrong-model', model: 'model-other' },
      { ...target, request_id: 'wrong-status', status: 200 },
      { ...target, request_id: 'wrong-source', source_kind: 'request' },
    ] as RequestEventWithPhase[];

    for (const [filters, excludedRequestId] of [
      [
        { principal_id: 'principal-target', source_kind: 'all' },
        'wrong-principal',
      ],
      [
        { upstream_id: 'upstream-target', source_kind: 'all' },
        'wrong-upstream',
      ],
      [{ session: 'thread-target', source_kind: 'all' }, 'wrong-session'],
      [{ model: 'model-target', source_kind: 'all' }, 'wrong-model'],
      [{ status: '4xx', source_kind: 'all' }, 'wrong-status'],
      [{ source_kind: 'renewal' }, 'wrong-source'],
    ] as const) {
      const requestIds = filterLogRows(rows, filters).map(
        (row) => row.request_id,
      );
      expect(requestIds).toContain('target');
      expect(requestIds).not.toContain(excludedRequestId);
    }

    expect(
      filterLogRows(rows, {
        principal_id: 'principal-target',
        upstream_id: 'upstream-target',
        session: 'thread-target',
        model: 'model-target',
        status: '4xx',
        source_kind: 'renewal',
      }).map((row) => row.request_id),
    ).toEqual(['target']);
  });

  it('renders Session select with an enforced w-64 and Clear button with h-9', async () => {
    const queryClient = new QueryClient();

    vi.spyOn(Route, 'useSearch').mockReturnValue({ session: '123' });

    const LogsPage = Route.options.component;
    if (LogsPage === undefined) {
      throw new Error('Expected logs route component');
    }

    render(
      <QueryClientProvider client={queryClient}>
        <LogsPage />
      </QueryClientProvider>,
    );

    const clearButton = await screen.findByRole('button', { name: /Clear/i });

    expect(clearButton.className).not.toContain('h-7');
    expect(clearButton.className).toContain('h-9');

    const sessionSelect = screen.getByText('123').closest('button');
    expect(sessionSelect).not.toBeNull();
    expect(sessionSelect?.className).toContain('!w-64');
  });

  // Budgeted, not raced. This body has no `await`, `waitFor` or `findBy`: it is one synchronous
  // block that mounts LogsPage over 120 rows and re-renders it four times (three page clicks plus
  // a filter rerender), so there is no async boundary that can hang and nothing to widen a
  // tolerance around. Its cost is pure render throughput and tracks available CPU:
  //
  //   isolated, node                          529-555ms
  //   full suite, 8 cores                        998ms
  //   full suite, 2 cores pinned                1774ms
  //   full suite, 2 cores + 4 busy loops        7168ms  <- reproduces the CI failure
  //
  // `oracle4-cc-lb` is one self-hosted box shared by clippy, nextest-cov, e2e and docker-build
  // from the same PR, so the last row is the realistic case and the 5s default has no headroom.
  // Capping vitest workers was measured and rejected: `--maxWorkers=2` under the same pin made
  // this test worse (1774ms -> 3427ms) by packing more files onto each worker's event loop.
  //
  // Budgeting this one test does not just move the failure elsewhere. Under the same load the
  // next-slowest test in the whole suite is 1543ms - 3.4x cheaper, and 31% of the stock 5s
  // budget - so this mount is a genuine outlier rather than the first casualty of a tier that
  // needs raising. Re-check that gap before budgeting a second test here.
  it('paginates rows correctly and clamps on filter change', async () => {
    const queryClient = new QueryClient();
    let currentSearch: Record<string, string> = {};
    vi.spyOn(Route, 'useSearch').mockImplementation(() => currentSearch);

    const LogsPage = Route.options.component;
    if (LogsPage === undefined) {
      throw new Error('Expected logs route component');
    }

    const { rerender } = render(
      <QueryClientProvider client={queryClient}>
        <LogsPage />
      </QueryClientProvider>,
    );

    const getRowCount = () => {
      const tbody = document.querySelector('tbody');
      return tbody ? tbody.querySelectorAll('tr').length : 0;
    };

    expect(screen.getByText('Showing 1–50 of 120')).toBeDefined();
    expect(screen.getByText('Page 1 of 3')).toBeDefined();
    expect(getRowCount()).toBe(50);

    const scroller = document.querySelector<HTMLDivElement>(
      'div.flex-1.overflow-auto.min-h-0',
    );
    if (scroller === null) throw new Error('Expected log table scroller');
    scroller.scrollTop = 100;

    const nextBtn = screen.getByRole('button', { name: /Next page/i });
    fireEvent.click(nextBtn);
    expect(screen.getByText('Showing 51–100 of 120')).toBeDefined();
    expect(screen.getByText('Page 2 of 3')).toBeDefined();
    expect(getRowCount()).toBe(50);
    expect(scroller.scrollTop).toBe(0);

    fireEvent.click(nextBtn);
    expect(screen.getByText('Showing 101–120 of 120')).toBeDefined();
    expect(screen.getByText('Page 3 of 3')).toBeDefined();
    expect(getRowCount()).toBe(20);

    const prevBtn = screen.getByRole('button', { name: /Previous page/i });
    fireEvent.click(prevBtn);
    expect(screen.getByText('Showing 51–100 of 120')).toBeDefined();
    expect(screen.getByText('Page 2 of 3')).toBeDefined();
    expect(getRowCount()).toBe(50);

    scroller.scrollTop = 100;
    currentSearch = { session: 'session-a' };
    rerender(
      <QueryClientProvider client={queryClient}>
        <LogsPage />
      </QueryClientProvider>,
    );

    expect(screen.getByText('Showing 1–50 of 60')).toBeDefined();
    expect(screen.getByText('Page 1 of 2')).toBeDefined();
    expect(getRowCount()).toBe(50);
    expect(scroller.scrollTop).toBe(0);
  }, 30_000);

  it('exports all visible rows, not just the current page', async () => {
    const queryClient = new QueryClient();
    vi.spyOn(Route, 'useSearch').mockReturnValue({});

    const LogsPage = Route.options.component;
    if (LogsPage === undefined) {
      throw new Error('Expected logs route component');
    }

    render(
      <QueryClientProvider client={queryClient}>
        <LogsPage />
      </QueryClientProvider>,
    );

    const exportBtn = screen.getByRole('button', { name: /Export/i });

    let capturedBlob: Blob | undefined;
    vi.spyOn(URL, 'createObjectURL').mockImplementation((object) => {
      if (!(object instanceof Blob)) throw new Error('Expected Blob export');
      capturedBlob = object;
      return 'blob:test';
    });

    const clickSpy = vi
      .spyOn(HTMLAnchorElement.prototype, 'click')
      .mockImplementation(() => {});

    fireEvent.click(exportBtn);

    expect(clickSpy).toHaveBeenCalled();
    expect(capturedBlob).toBeDefined();

    if (capturedBlob === undefined) throw new Error('Expected exported Blob');
    const text = await capturedBlob.text();
    const exportedRows: unknown = JSON.parse(text);
    expect(Array.isArray(exportedRows)).toBe(true);
    if (Array.isArray(exportedRows)) expect(exportedRows.length).toBe(120);

    const tbody = document.querySelector('tbody');
    expect(tbody?.querySelectorAll('tr').length).toBe(50);
  });
});
