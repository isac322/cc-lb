import { cleanup, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it } from 'vitest';
import type { QuotaSnapshot } from '../../lib/api';
import { QuotaWindowRow, QuotaWindowRows } from './QuotaWindowRows';

afterEach(() => {
  cleanup();
});

const NOW = Math.floor(Date.now() / 1000);

function snap(overrides: Partial<QuotaSnapshot>): QuotaSnapshot {
  return {
    window: '5h',
    state: 'fresh',
    source: 'api',
    utilization: 0.42,
    status: 'allowed',
    resets_at_unix_secs: NOW + 3600,
    surpassed_threshold: null,
    representative_claim: null,
    disabled_reason: null,
    extra_usage_enabled: null,
    extra_usage_monthly_limit: null,
    extra_usage_used_credits: null,
    observed_at_unix_millis: NOW * 1000,
    age_secs: 0,
    ...overrides,
  };
}

function renderRow(s: QuotaSnapshot) {
  render(
    <QuotaWindowRows>
      <QuotaWindowRow snap={s} nowUnixSecs={NOW} />
    </QuotaWindowRows>,
  );
  return screen.getByRole('listitem');
}

describe('QuotaWindowRow', () => {
  it('shows a started window as N% used with its reset', () => {
    const row = renderRow(snap({}));
    expect(row.textContent).toContain('5h');
    expect(row.textContent).toContain('42% used');
    expect(row.textContent).toContain('Resets in');
    // Resets in 1h of a 5h window: 4h elapsed is 80% even pace.
    const meter = screen.getByRole('meter');
    expect(meter.getAttribute('aria-valuetext')).toBe(
      '42% used, even pace 80%',
    );
    const marker = row.querySelector<HTMLElement>('[data-slot="pace-marker"]');
    expect(marker?.style.left).toBe('80%');
  });

  it('paces a 7d window over seven days', () => {
    const row = renderRow(
      snap({ window: '7d_fable', resets_at_unix_secs: NOW + 5 * 86400 }),
    );
    const marker = row.querySelector<HTMLElement>('[data-slot="pace-marker"]');
    expect(Number(marker?.dataset.pacePct)).toBeCloseTo((2 / 7) * 100, 6);
  });

  it('colors a window 30+ points ahead of pace as danger', () => {
    // 55% used of a 5h window resetting in 4h: 1h elapsed is a 20% pace,
    // so usage runs 35 points ahead — past the 30-point danger gap.
    renderRow(snap({ utilization: 0.55, resets_at_unix_secs: NOW + 4 * 3600 }));
    const meter = screen.getByRole('meter');
    expect(meter.getAttribute('aria-valuetext')).toBe(
      '55% used, even pace 20%',
    );
    expect(meter.querySelector('.bg-danger')).not.toBeNull();
  });

  it('draws no pace tick while the reset sits more than one length ahead', () => {
    const row = renderRow(
      snap({ utilization: 0.2, resets_at_unix_secs: NOW + 6 * 3600 }),
    );
    expect(row.querySelector('[data-slot="pace-marker"]')).toBeNull();
    expect(screen.getByRole('meter').getAttribute('aria-valuetext')).toBe(
      '20% used',
    );
  });

  it('shows extra usage as dollars spent of the monthly limit', () => {
    const row = renderRow(
      snap({
        window: 'overage',
        utilization: null,
        extra_usage_enabled: true,
        extra_usage_monthly_limit: 5000,
        extra_usage_used_credits: 1200,
      }),
    );
    expect(row.textContent).toContain('Extra usage');
    expect(row.textContent).toContain('$12.00 of $50.00');
    expect(screen.getByRole('meter').getAttribute('aria-valuenow')).toBe('24');
    // Extra usage is a monthly budget, not a timed window: no pace.
    expect(row.querySelector('[data-slot="pace-marker"]')).toBeNull();
  });

  it.each([
    { enabled: false, budget: 5000 },
    { enabled: true, budget: 0 },
    { enabled: true, budget: null },
  ])(
    'omits extra usage without an active positive budget',
    ({ enabled, budget }) => {
      render(
        <QuotaWindowRows>
          <QuotaWindowRow
            snap={snap({
              window: 'overage',
              extra_usage_enabled: enabled,
              extra_usage_monthly_limit: budget,
            })}
            nowUnixSecs={NOW}
          />
        </QuotaWindowRows>,
      );
      expect(screen.queryByRole('listitem')).toBeNull();
      expect(screen.queryByText('Extra usage')).toBeNull();
    },
  );

  it('says a window without a live reset has not started', () => {
    const row = renderRow(snap({ utilization: 0, resets_at_unix_secs: null }));
    expect(row.textContent).toContain('Not started');
    expect(row.textContent).not.toContain('Resets in');
    expect(row.querySelector('[data-slot="pace-marker"]')).toBeNull();
  });

  it('renders nothing for the backend-only unified window', () => {
    // `unified` is not a SubscriptionQuotaWindow; the backend may still
    // report it on the wire for routing, but the row must not render.
    render(
      <QuotaWindowRows>
        <QuotaWindowRow
          snap={snap({
            window: 'unified',
          } as unknown as Partial<QuotaSnapshot>)}
          nowUnixSecs={NOW}
        />
      </QuotaWindowRows>,
    );
    expect(screen.queryByRole('listitem')).toBeNull();
  });
});
