import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
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
  });

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
