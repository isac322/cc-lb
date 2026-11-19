import { cleanup, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it } from 'vitest';
import type { CouponEffectRow } from '../../lib/couponPresentation';
import { CouponEffects } from './CouponEffects';

function row(overrides: Partial<CouponEffectRow> = {}): CouponEffectRow {
  return {
    window: '5h',
    label: '5-hour',
    current: {
      window: '5h',
      label: '5-hour',
      percentUsed: 60,
      state: 'fresh',
      source: 'quota',
    },
    effect: 'reset',
    schedule: 'confirmed_after_use',
    resetsAtUnixSecs: null,
    ...overrides,
  };
}

afterEach(cleanup);

describe('expected quota changes', () => {
  it('shows current usage and an expected reset without presenting the future as observed usage', () => {
    render(
      <CouponEffects
        effects={{ rows: [row()], otherLimitsUnchanged: false }}
      />,
    );
    expect(screen.getByRole('definition').textContent).toContain('60% used');
    expect(screen.getByRole('definition').textContent).toContain(
      'After use: 0% used',
    );
  });

  it('never labels an old stale percentage as current usage', () => {
    render(
      <CouponEffects
        effects={{
          rows: [
            row({
              current: {
                window: '5h',
                label: '5-hour',
                percentUsed: 87,
                state: 'stale',
                source: 'quota',
              },
            }),
          ],
          otherLimitsUnchanged: false,
        }}
      />,
    );
    expect(screen.queryByText(/87%/)).toBeNull();
    expect(screen.getByText('Unknown')).toBeDefined();
    expect(screen.getByText('0% used')).toBeDefined();
  });

  it('renders all visible windows and retains qualitative Fable changes even when its current bar is zero', () => {
    const rows = [
      row(),
      row({
        window: '7d',
        label: 'Weekly',
        effect: 'fable_weekly',
        schedule: 'unchanged',
      }),
      row({
        window: '7d_fable',
        label: 'Weekly (Fable)',
        effect: 'fable_scoped',
        schedule: 'unchanged',
        current: {
          window: '7d_fable',
          label: 'Weekly (Fable)',
          percentUsed: 0,
          state: 'fresh',
          source: 'quota',
        },
      }),
      row({
        window: '7d_sonnet',
        label: 'Weekly (Sonnet)',
        effect: 'unchanged',
        schedule: 'unchanged',
      }),
      row({
        window: '7d_opus',
        label: 'Weekly (Opus)',
        effect: 'unchanged',
        schedule: 'unchanged',
      }),
    ];
    render(<CouponEffects effects={{ rows, otherLimitsUnchanged: false }} />);
    expect(screen.getAllByRole('term')).toHaveLength(5);
    expect(screen.getByText(/Fable portion restored/)).toBeDefined();
    expect(screen.getByText(/Fable usage restored/)).toBeDefined();
    expect(
      screen.getByText(/Other-model weekly usage unchanged/),
    ).toBeDefined();
  });

  it('renders Weekly (Fable) as 0% used for weekly coupons clearing 7d_overage_included instead of Effect not reported', async () => {
    const { presentCouponEffects } = await import(
      '../../lib/couponPresentation'
    );
    const effects = presentCouponEffects(
      {
        id: 'full-reset-2611',
        label: 'Weekly limit reset',
        type: 'weekly',
        gives_back_weekly: false,
        unavailable_reason: null,
        resets_total: 1,
        resets_left: 1,
        starts_at: null,
        ends_at: '2026-12-01T00:00:00Z',
        clears: ['7d', '7d_overage_included'],
        paused: false,
        usable_now: true,
        use_requires_limit: true,
        percent_used: { '7d': 100, '7d_overage_included': 80 },
        blocking: [],
      },
      [
        {
          window: '5h',
          utilization: 0.2,
          state: 'fresh',
          resets_at_unix_secs: null,
        },
        {
          window: '7d',
          utilization: 1,
          state: 'fresh',
          resets_at_unix_secs: null,
        },
        {
          window: '7d_fable',
          utilization: 0.8,
          state: 'fresh',
          resets_at_unix_secs: null,
        },
      ],
      { provider: 'anthropic', providerFresh: true },
    );
    render(<CouponEffects effects={effects} />);
    expect(screen.queryByText(/Effect not reported/)).toBeNull();
    const defs = screen.getAllByRole('definition').map((el) => el.textContent);
    expect(defs).toHaveLength(3);
    expect(defs[0]).toContain('20% used');
    expect(defs[0]).toContain('After use: Unchanged');
    expect(defs[1]).toContain('100% used');
    expect(defs[1]).toContain('After use: 0% used');
    expect(defs[2]).toContain('80% used');
    expect(defs[2]).toContain('After use: 0% used');
  });

  it('does not invent a precise future reset timestamp for a restarted window', () => {
    const future = Date.now() / 1000 + 3600;
    render(
      <CouponEffects
        effects={{
          rows: [
            row({ resetsAtUnixSecs: future, schedule: 'updated_after_use' }),
          ],
          otherLimitsUnchanged: true,
        }}
      />,
    );
    expect(screen.getByText(/Updated after use/)).toBeDefined();
    expect(screen.queryByText(/Resets in/)).toBeNull();
    expect(
      screen.getByText(/Other model and feature limits unchanged/),
    ).toBeDefined();
  });
});
