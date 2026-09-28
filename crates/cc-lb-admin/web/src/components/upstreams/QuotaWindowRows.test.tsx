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

  it('shows disabled extra usage as Off with an empty meter', () => {
    const row = renderRow(
      snap({
        window: 'overage',
        utilization: null,
        extra_usage_enabled: false,
      }),
    );
    expect(row.textContent).toContain('Off');
    expect(screen.getByRole('meter').getAttribute('aria-valuenow')).toBeNull();
  });

  it('says a window without a live reset has not started', () => {
    const row = renderRow(snap({ utilization: 0, resets_at_unix_secs: null }));
    expect(row.textContent).toContain('Not started');
    expect(row.textContent).not.toContain('Resets in');
    expect(row.querySelector('[data-slot="pace-marker"]')).toBeNull();
  });

  it('counts down to reset on an observed unified window, without pace', () => {
    const row = renderRow(
      snap({ window: 'unified', resets_at_unix_secs: NOW + 7200 }),
    );
    expect(row.textContent).toContain('Unified');
    expect(row.textContent).toContain('42% used');
    expect(row.textContent).toContain('Resets in');
    expect(row.textContent).not.toContain('Not started');
    expect(row.querySelector('[data-slot="pace-marker"]')).toBeNull();
  });

  it('shows no reset countdown on an unobserved unified window', () => {
    const row = renderRow(
      snap({
        window: 'unified',
        state: 'unobserved',
        resets_at_unix_secs: NOW + 7200,
      }),
    );
    expect(row.textContent).toContain('No reading');
    expect(row.textContent).not.toContain('Resets in');
  });
});
