import { cleanup, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it } from 'vitest';
import type {
  KeyUsageMonth,
  KeyUsageProvenance,
  PricingCompleteness,
} from '../../../lib/api';
import { derivePricingCompleteness } from '../../../lib/pricingCompleteness';
import { CreditStrip } from '../CreditStrip';

const provenance: KeyUsageProvenance = {
  source: 'estimated',
  stale: false,
  stopped: false,
  last_reported_unix_secs: null,
  pending_first_report: false,
  attribution: 'api_key',
  workspace: null,
  verified: true,
  key_class: 'org_scoped',
  fix: null,
};
const month: KeyUsageMonth = {
  start_unix_secs: 1793491200,
  end_unix_secs: 1796083200,
  tokens: 100,
  cost_usd_micros: 0,
  billing_stop: null,
};
afterEach(cleanup);

describe('CreditStrip pricing availability', () => {
  it.each([
    ['legacy all-zero/native omitted', 0, 0, 0, 0, '—', 'Price unknown'],
    ['legacy plus unpriced', 0, 1, 0, 0, '—', 'Price unknown'],
    ['legacy plus auxiliary', 0, 0, 1, 0, '—', 'Price unknown'],
    ['legacy plus priced zero', 1, 0, 0, 0, '$0.00+', 'Partly priced'],
    ['positive legacy', 0, 0, 0, 2_500_000, '$2.50+', 'Partly priced'],
  ] as const)(
    'keeps %s honest in estimated credits',
    (_name, priced, unpriced, unmetered, cost, figure, caption) => {
      const { container } = render(
        <CreditStrip
          month={{
            ...month,
            tokens: 0,
            cost_usd_micros: cost,
            pricing_completeness: derivePricingCompleteness({
              request_count: 2,
              priced_event_count: priced,
              unpriced_event_count: unpriced,
              unmetered_event_count: unmetered,
              virtual_cost_micros: cost,
            }),
          }}
          provenance={provenance}
          isLoading={false}
        />,
      );
      expect(
        container.querySelector('[data-cell="Credits used"]')?.textContent,
      ).toContain(figure);
      expect(screen.getByText(caption)).toBeDefined();
      expect(screen.queryByText('No billable usage')).toBeNull();
    },
  );

  it.each([
    ['unpriced', 'List price unavailable'],
    ['unknown', 'Price unknown'],
  ] as const)('shows %s without a false zero', (state, caption) => {
    const { container } = render(
      <CreditStrip
        month={{ ...month, pricing_completeness: state }}
        provenance={provenance}
        isLoading={false}
      />,
    );
    const cell = container.querySelector('[data-cell="Credits used"]');
    expect(cell?.textContent).toContain('—');
    expect(cell?.textContent).toContain(caption);
    expect(cell?.textContent).not.toContain('$0.00');
    expect(screen.getByText(caption).className).not.toMatch(/warn|danger/);
  });
  it('qualifies partial month spend', () => {
    const { container } = render(
      <CreditStrip
        month={{
          ...month,
          cost_usd_micros: 2_500_000,
          pricing_completeness: 'partial',
        }}
        provenance={provenance}
        isLoading={false}
      />,
    );
    expect(
      container.querySelector('[data-cell="Credits used"]')?.textContent,
    ).toContain('$2.50+');
    expect(screen.getByText('Partly priced')).toBeDefined();
  });
  it.each(['known_zero', 'unmetered', undefined] as const)(
    'keeps %s at a known zero',
    (pricing_completeness) => {
      const { container } = render(
        <CreditStrip
          month={{ ...month, pricing_completeness }}
          provenance={provenance}
          isLoading={false}
        />,
      );
      expect(
        container.querySelector('[data-cell="Credits used"]')?.textContent,
      ).toContain('$0.00');
    },
  );
  it('leaves provider-reported figures unchanged even if classification is supplied', () => {
    const { container } = render(
      <CreditStrip
        month={{
          ...month,
          pricing_completeness: 'unpriced' as PricingCompleteness,
        }}
        provenance={{ ...provenance, source: 'reported' }}
        isLoading={false}
      />,
    );
    expect(
      container.querySelector('[data-cell="Credits used"]')?.textContent,
    ).toContain('$0.00At list price');
    expect(screen.queryByText('List price unavailable')).toBeNull();
  });
});
