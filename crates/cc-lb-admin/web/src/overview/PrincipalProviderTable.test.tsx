import {
  cleanup,
  fireEvent,
  render,
  screen,
  within,
} from '@testing-library/react';
import type { ReactNode } from 'react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import type {
  DashboardUsageResponse,
  PricingCompleteness,
  UsageBucket,
} from '../lib/api';
import { PrincipalProviderTable } from './PrincipalProviderTable';
import {
  type MergedPrincipalUsage,
  mergePrincipalProviderUsage,
} from './principalMerge';

vi.mock('@tanstack/react-router', () => ({
  Link: ({ children, to }: { children: ReactNode; to: string }) => (
    <a href={to}>{children}</a>
  ),
  useNavigate: () => vi.fn(),
}));
afterEach(cleanup);

function row(
  name: string,
  state: PricingCompleteness,
  cost = 0,
): MergedPrincipalUsage {
  const pricing = {
    pricing_completeness: state,
    request_count: state === 'partial' ? 2 : 1,
    priced_request_count:
      state === 'priced' || state === 'known_zero' || state === 'partial'
        ? 1
        : 0,
    unpriced_request_count: state === 'unpriced' || state === 'partial' ? 1 : 0,
    unmetered_request_count: state === 'unmetered' ? 1 : 0,
  };
  return {
    ...pricing,
    id: name,
    name,
    totalCostMicros: cost,
    totalRequests: pricing.request_count,
    totalTokens: 100,
    totalCacheHitRatio: null,
    sharePct: state === 'unknown' || state === 'unpriced' ? null : 100,
    providers: {
      anthropic: {
        ...pricing,
        costMicros: cost,
        cacheHitRatio: null,
        requests: pricing.request_count,
        tokens: 100,
        promptTokens: 100,
      },
    },
  };
}
function show(rows: MergedPrincipalUsage[]) {
  return render(
    <PrincipalProviderTable
      rows={rows}
      providers={['anthropic']}
      loading={false}
      error={false}
    />,
  );
}

describe('PrincipalProviderTable pricing truth', () => {
  it('qualifies shares only when the cost denominator is incomplete', () => {
    const view = show([row('Priced', 'priced', 2_500_000)]);
    expect(
      screen.getByRole('columnheader', { name: 'Provider share' }),
    ).toBeDefined();
    view.unmount();
    show([row('Priced', 'priced', 2_500_000), row('Legacy', 'unknown')]);
    expect(
      screen.getByRole('columnheader', { name: 'Known-cost share' }),
    ).toBeDefined();
  });

  it.each(['priced', 'unknown'] as const)(
    'shows an absent provider as a plain dash beside a %s provider',
    (state) => {
      render(
        <PrincipalProviderTable
          rows={[
            row('One provider', state, state === 'priced' ? 2_500_000 : 0),
          ]}
          providers={['anthropic', 'openai']}
          loading={false}
          error={false}
        />,
      );
      const principal = screen.getByTestId('multi-provider-principal-row');
      const cells = within(principal).getAllByRole('cell');
      const absent = cells[cells.length - 1];
      expect(absent.textContent).toBe('—');
      expect(absent.querySelector('[title]')).toBeNull();
      expect(absent.querySelector('[aria-label]')).toBeNull();
      expect(absent.textContent).not.toContain('Price unknown');
      expect(absent.textContent).not.toContain('$0.00');
      if (state === 'unknown') {
        expect(
          principal.querySelectorAll('[title="Price unknown"]'),
        ).toHaveLength(2);
      }
    },
  );

  it('keeps unclassified zero rows and both footer totals unknown', () => {
    const bucket: UsageBucket = {
      bucket_start_unix_secs: 0,
      request_count: 1,
      input_tokens: 0,
      output_tokens: 0,
      cache_creation_input_tokens: 0,
      cache_read_input_tokens: 0,
      error_count: 0,
      virtual_cost_micros: 0,
      latency_ms_sum: 0,
      latency_count: 0,
      priced_request_count: 0,
      unpriced_request_count: 0,
      unmetered_request_count: 0,
    };
    const usage: DashboardUsageResponse = {
      range: '24h',
      step: 'hour',
      group_by: 'principal',
      window_start_unix_secs: 0,
      window_end_unix_secs: 3600,
      observed: true,
      series: [{ key: 'Legacy principal', buckets: [bucket] }],
    };
    const { container } = show(
      mergePrincipalProviderUsage({ anthropic: usage }, new Map()),
    );
    const principal = screen.getByTestId('multi-provider-principal-row');
    const footer = container.querySelector('tfoot');
    expect(principal.querySelectorAll('[title="Price unknown"]')).toHaveLength(
      2,
    );
    expect(footer?.querySelectorAll('[title="Price unknown"]')).toHaveLength(2);
    expect(principal.textContent).not.toContain('$0.00');
    expect(footer?.textContent).not.toContain('$0.00');
    expect(footer?.textContent).not.toContain('100%');
  });

  it('puts indeterminate costs last in either sort direction', () => {
    show([
      row('Unpriced', 'unpriced'),
      row('Priced', 'priced', 2_000_000),
      row('Unknown', 'unknown'),
      row('Zero', 'known_zero'),
    ]);
    const names = () =>
      screen
        .getAllByTestId('multi-provider-principal-row')
        .map((element) => within(element).getByRole('link').textContent);
    expect(names()).toEqual(['Priced', 'Zero', 'Unknown', 'Unpriced']);
    fireEvent.click(screen.getByRole('button', { name: 'Total cost' }));
    expect(names()).toEqual(['Zero', 'Priced', 'Unknown', 'Unpriced']);
    const unknown = screen.getAllByTestId('multi-provider-principal-row')[2];
    expect(unknown.textContent).toContain('Price unknown');
    expect(unknown.textContent).not.toContain('$0.00');
    expect(unknown.querySelectorAll('[title="Price unknown"]').length).toBe(2);
  });
  it('qualifies known subtotals and preserves known zeros', () => {
    show([
      row('Partial', 'partial', 2_500_000),
      row('Zero', 'known_zero'),
      row('No usage', 'unmetered'),
    ]);
    const partial = screen.getAllByTestId('multi-provider-principal-row')[0];
    expect(partial.textContent).toContain('$2.50+');
    expect(partial.textContent).toContain('1 unpriced');
    expect(
      screen.getAllByTestId('multi-provider-principal-row')[1].textContent,
    ).toContain('$0.00');
    expect(screen.queryByText('No list price for this model')).toBeNull();
  });
  it('has no numeric share footer when all costs are unavailable', () => {
    const { container } = show([row('Unpriced', 'unpriced')]);
    expect(container.querySelector('tfoot')?.textContent).toContain(
      'List price unavailable',
    );
    expect(container.querySelector('tfoot')?.textContent).not.toContain('100%');
    expect(container.querySelector('tfoot')?.textContent).not.toContain(
      '$0.00',
    );
  });
});
