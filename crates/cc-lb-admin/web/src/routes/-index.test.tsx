import { cleanup, render, screen } from '@testing-library/react';
import type { ComponentType } from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import * as queries from '../lib/queries';
import * as liveEvents from '../lib/useLiveEventStream';
import { PoolQuotaLegend, Route, TopPrincipalsCard, ValueTile } from './index';

vi.mock('../lib/queries', async () => {
  const actual = await vi.importActual<typeof queries>('../lib/queries');
  return {
    ...actual,
    usePrincipalNameMap: vi.fn(),
    useRecentEventsInfinite: vi.fn(),
    useSubscriptionQuotaAggregate: vi.fn(),
    useSubscriptionQuotaPoolHistory: vi.fn(),
    useSummary: vi.fn(),
    useUpstreamNameMap: vi.fn(),
    useUsage: vi.fn(),
  };
});

vi.mock('../lib/useLiveEventStream', () => ({
  useLiveEventStream: vi.fn(),
}));

const OverviewPage = Route.options.component as ComponentType;

function mockPendingOverviewQueries() {
  vi.mocked(queries.useSummary).mockReturnValue({
    data: undefined,
    isPending: true,
  } as never);
  vi.mocked(queries.useUsage).mockReturnValue({
    data: undefined,
    isPending: true,
    isPlaceholderData: false,
  } as never);
  vi.mocked(queries.useRecentEventsInfinite).mockReturnValue({
    data: { pages: [{ events: [] }] },
    fetchNextPage: vi.fn(),
    hasNextPage: false,
    isFetchingNextPage: false,
    isLoading: false,
  } as never);
  vi.mocked(queries.usePrincipalNameMap).mockReturnValue(new Map());
  vi.mocked(queries.useUpstreamNameMap).mockReturnValue(new Map());
  vi.mocked(queries.useSubscriptionQuotaAggregate).mockReturnValue({
    data: undefined,
    isPending: true,
    isPlaceholderData: false,
  } as never);
  vi.mocked(queries.useSubscriptionQuotaPoolHistory).mockReturnValue({
    data: undefined,
    isPending: true,
    isPlaceholderData: false,
  } as never);
  vi.mocked(liveEvents.useLiveEventStream).mockReturnValue({
    error: null,
    eventsMap: new Map(),
    forceReconnect: vi.fn(),
    lastActivityAt: null,
    lastCursor: null,
    malformedFrameCount: 0,
    permanentFailure: false,
    permanentFailureSince: null,
    reconnectAttempts: 0,
    status: 'idle',
    version: 0,
  });
}

function mockResolvedEmptyQuotaQueries() {
  vi.mocked(queries.useSubscriptionQuotaAggregate).mockReturnValue({
    data: { upstream_count: 0, windows: [] },
    isPending: false,
    isPlaceholderData: false,
  } as never);
  vi.mocked(queries.useSubscriptionQuotaPoolHistory).mockReturnValue({
    data: { windows: [] },
    isPending: false,
    isPlaceholderData: false,
  } as never);
}

beforeEach(() => {
  vi.clearAllMocks();
});

afterEach(() => {
  cleanup();
});

describe('PoolQuotaLegend', () => {
  it('renders Fable only when the selected range has Fable data', () => {
    const { rerender } = render(
      <PoolQuotaLegend
        latest={{ '5h': 20, '7d': 40, '7d_fable': null }}
        showFable={false}
      />,
    );
    expect(screen.queryByText(/Fable/)).toBeNull();

    rerender(
      <PoolQuotaLegend
        latest={{ '5h': 20, '7d': 40, '7d_fable': 28 }}
        showFable
      />,
    );
    expect(screen.getByText(/Fable/).textContent).toBe('Fable · 28%');
  });
});

describe('Overview loading geometry', () => {
  it('keeps ValueTile value, sub, and sparkline slots fixed', () => {
    const { container, rerender } = render(
      <ValueTile
        icon={<span />}
        label="requests"
        loading
        size="sm"
        spark={[]}
        sub="12 / 24h"
        value="12"
      />,
    );

    const tileClassName = container.firstElementChild?.className;
    const valueClassName = container.querySelector(
      '[data-slot="value"]',
    )?.className;
    const subClassName =
      container.querySelector('[data-slot="sub"]')?.className;
    const sparklineClassName = container.querySelector(
      '[data-slot="sparkline"]',
    )?.className;

    expect(
      container.querySelector('[data-slot="value"] .skeleton'),
    ).not.toBeNull();
    expect(
      container.querySelector('[data-slot="sub"] .skeleton'),
    ).not.toBeNull();
    expect(
      container.querySelector('[data-slot="sparkline"] .skeleton'),
    ).not.toBeNull();
    expect(valueClassName).toContain('h-5');
    expect(subClassName).toContain('h-4');
    expect(sparklineClassName).toContain('h-8');
    expect(screen.queryByText('12')).toBeNull();
    expect(screen.queryByText('12 / 24h')).toBeNull();

    rerender(
      <ValueTile
        icon={<span />}
        label="requests"
        loading={false}
        size="sm"
        spark={[]}
        sub="12 / 24h"
        value="12"
      />,
    );

    expect(container.firstElementChild?.className).toBe(tileClassName);
    expect(container.querySelector('[data-slot="value"]')?.className).toBe(
      valueClassName,
    );
    expect(container.querySelector('[data-slot="sub"]')?.className).toBe(
      subClassName,
    );
    expect(container.querySelector('[data-slot="sparkline"]')?.className).toBe(
      sparklineClassName,
    );
    expect(
      container.querySelector('[data-slot="sparkline"] .skeleton'),
    ).toBeNull();
    expect(screen.getByText('12')).toBeDefined();
    expect(screen.getByText('12 / 24h')).toBeDefined();
  });

  it('keeps the principal card shell stable across loading, empty, and loaded states', () => {
    const { container, rerender } = render(
      <TopPrincipalsCard loading principals={[]} range="24h" />,
    );

    const cardClassName = screen
      .getByText('Top principals')
      .closest('.glass')?.className;
    const listClassName = container.querySelector(
      '[data-slot="principal-list"]',
    )?.className;
    const skeletonRows = screen.getAllByTestId('top-principal-skeleton-row');

    expect(skeletonRows).toHaveLength(5);
    for (const row of skeletonRows) {
      const skeletons = row.querySelectorAll('.skeleton');
      expect(row.className).toContain('min-h-[66px]');
      expect(row.className).toContain('px-3');
      expect(row.className).toContain('py-2');
      expect(skeletons).toHaveLength(5);
      expect(skeletons[0]?.className).toContain('h-5');
      expect(skeletons[1]?.className).toContain('h-3');
      expect(skeletons[2]?.className).toContain('h-1.5');
      expect(skeletons[3]?.className).toContain('h-5');
      expect(skeletons[4]?.className).toContain('h-3');
    }
    expect(listClassName).toContain('min-h-80');
    expect(screen.queryByText('Loading top principals…')).toBeNull();

    rerender(<TopPrincipalsCard loading={false} principals={[]} range="24h" />);

    expect(
      screen.getByText('Top principals').closest('.glass')?.className,
    ).toBe(cardClassName);
    expect(
      container.querySelector('[data-slot="principal-list"]')?.className,
    ).toBe(listClassName);
    expect(screen.getByText('No usage data')).toBeDefined();

    rerender(
      <TopPrincipalsCard
        loading={false}
        principals={[
          {
            cost_usd: 1.25,
            id: 'principal-1',
            max_cost: 1.25,
            name: 'Primary principal',
            primary_model: '—',
            requests: 12,
            share_pct: 100,
            tokens: 345,
          },
        ]}
        range="24h"
      />,
    );

    expect(
      screen.getByText('Top principals').closest('.glass')?.className,
    ).toBe(cardClassName);
    expect(
      container.querySelector('[data-slot="principal-list"]')?.className,
    ).toBe(listClassName);
    expect(screen.getByTestId('top-principal-row').className).toContain(
      'min-h-[66px]',
    );
    expect(screen.getByText('Primary principal')).toBeDefined();
  });

  it('renders pending Overview KPIs and principals without fake values or loading text', () => {
    mockPendingOverviewQueries();

    render(<OverviewPage />);

    for (const label of [
      'avg req/s',
      'tokens',
      'equiv $',
      'Avg latency',
      'err rate',
    ]) {
      const tile = screen.getByText(label).closest('.glass');
      expect(tile?.textContent).toBe(label);
      expect(
        tile?.querySelector('[data-slot="value"] .skeleton'),
      ).not.toBeNull();
      expect(
        tile?.querySelector('[data-slot="sparkline"] .skeleton'),
      ).not.toBeNull();
    }

    expect(screen.queryByText('Loading top principals…')).toBeNull();
    expect(screen.getAllByTestId('top-principal-skeleton-row')).toHaveLength(5);
  });

  it.each([
    ['aggregate', true, false],
    ['history', false, true],
  ])(
    'keeps Pool quota structured while the %s query is pending',
    (_queryName, aggregatePending, historyPending) => {
      mockPendingOverviewQueries();
      vi.mocked(queries.useSubscriptionQuotaAggregate).mockReturnValue({
        data: aggregatePending ? undefined : { upstream_count: 0, windows: [] },
        isPending: aggregatePending,
        isPlaceholderData: false,
      } as never);
      vi.mocked(queries.useSubscriptionQuotaPoolHistory).mockReturnValue({
        data: historyPending ? undefined : { windows: [] },
        isPending: historyPending,
        isPlaceholderData: false,
      } as never);

      render(<OverviewPage />);

      const card = screen.getByTestId('pool-quota-card');
      expect(card.getAttribute('aria-busy')).toBe('true');
      expect(card.textContent).not.toContain('no data');
      expect(card.textContent).not.toContain('No timeline data yet');

      const snapshotSlots = card.querySelectorAll(
        '[data-testid="pool-quota-snapshot-slot"]',
      );
      expect(snapshotSlots).toHaveLength(3);
      expect(
        Array.from(snapshotSlots, (slot) => slot.textContent?.trim()),
      ).toEqual(['5h pool', '7d pool', '7d (Fable) pool']);
      for (const slot of snapshotSlots) {
        expect(slot.className).toContain('min-h-[56px]');
        expect(slot.querySelectorAll('.skeleton')).toHaveLength(2);
      }

      const legendSlots = card.querySelectorAll(
        '[data-testid="pool-quota-legend-slot"]',
      );
      expect(legendSlots).toHaveLength(3);
      expect(
        Array.from(legendSlots, (slot) => slot.textContent?.trim()),
      ).toEqual(['5h', '7d', 'Fable']);
      for (const slot of legendSlots) {
        expect(slot.querySelector('.skeleton')).not.toBeNull();
      }

      const chartSlot = screen.getByTestId('pool-quota-chart-slot');
      expect(chartSlot.className).toContain('min-h-64');
      expect(chartSlot.querySelector('.skeleton')).not.toBeNull();
      const chartHeader = chartSlot.previousElementSibling;
      expect(chartHeader?.className).toContain('flex-col');
      expect(chartHeader?.className).toContain('sm:flex-row');
    },
  );

  it('keeps all three Pool quota slots mounted when pending resolves to legitimate no-data', () => {
    mockPendingOverviewQueries();

    const { rerender } = render(<OverviewPage />);
    const pendingCard = screen.getByTestId('pool-quota-card');
    const pendingSnapshotSlots = Array.from(
      pendingCard.querySelectorAll('[data-testid="pool-quota-snapshot-slot"]'),
    );
    const pendingChartSlot = screen.getByTestId('pool-quota-chart-slot');

    mockResolvedEmptyQuotaQueries();
    rerender(<OverviewPage />);

    const resolvedCard = screen.getByTestId('pool-quota-card');
    const resolvedSnapshotSlots = Array.from(
      resolvedCard.querySelectorAll('[data-testid="pool-quota-snapshot-slot"]'),
    );
    expect(resolvedCard).toBe(pendingCard);
    expect(resolvedCard.getAttribute('aria-busy')).toBe('false');
    for (let index = 0; index < resolvedSnapshotSlots.length; index += 1) {
      expect(resolvedSnapshotSlots[index]).toBe(pendingSnapshotSlots[index]);
    }
    expect(resolvedSnapshotSlots).toHaveLength(3);
    expect(
      resolvedCard.querySelectorAll('[data-testid="pool-quota-legend-slot"]'),
    ).toHaveLength(3);
    expect(screen.getByTestId('pool-quota-chart-slot')).toBe(pendingChartSlot);
    expect(resolvedCard.querySelectorAll('.skeleton')).toHaveLength(0);
    expect(resolvedCard.textContent?.match(/no data/g)).toHaveLength(3);
    expect(resolvedCard.textContent).toContain(
      'No timeline data yet for this range',
    );
  });

  it('keeps the Recent Requests scroll box height fixed while data resolves', () => {
    mockPendingOverviewQueries();
    vi.mocked(queries.useRecentEventsInfinite).mockReturnValue({
      data: undefined,
      fetchNextPage: vi.fn(),
      hasNextPage: false,
      isFetchingNextPage: false,
      isLoading: true,
    } as never);

    const { container, rerender } = render(<OverviewPage />);
    const loadingScrollBox = container.querySelector('.scroll-fade-right');

    expect(loadingScrollBox?.className).toContain('h-[50vh]');
    expect(loadingScrollBox?.className).not.toContain('max-h-[50vh]');

    vi.mocked(queries.useRecentEventsInfinite).mockReturnValue({
      data: {
        pages: [
          {
            events: [
              {
                duration_ms: 10,
                model: 'test-model',
                request_id: 'request-1',
                status: 200,
                ts: 1,
              },
            ],
          },
        ],
      },
      fetchNextPage: vi.fn(),
      hasNextPage: false,
      isFetchingNextPage: false,
      isLoading: false,
    } as never);

    rerender(<OverviewPage />);

    expect(container.querySelector('.scroll-fade-right')?.className).toBe(
      loadingScrollBox?.className,
    );
    expect(screen.getByText('test-model')).toBeDefined();
  });
});
