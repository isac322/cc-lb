import { cleanup, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it } from 'vitest';
import { PoolQuotaLegend } from '../designs/default/OverviewPage';
import type { AggregateResponse, PoolHistoryWindowResponse } from '../lib/api';
import { poolWindowPace, poolWindowResets } from '../routes/-overviewPoolQuota';
import { presentProviderWindows } from './ProviderPoolCard';

const NOW = 1_700_000_000;

afterEach(cleanup);

function aggregateWindow(
  window: string,
  state: string,
  utilizationPercent: number | null,
): AggregateResponse['windows'][number] {
  return {
    window,
    cc_window_start_unix_secs: NOW - 3600,
    cc_window_reset_unix_secs: NOW + 3600,
    pool_pace_fraction: state === 'absent' ? null : 0.5,
    next_reset_unix_secs: state === 'absent' ? null : NOW + 3600,
    used_tokens: 100,
    utilization: utilizationPercent == null ? null : utilizationPercent / 100,
    utilization_percent: utilizationPercent,
    capacity_to_now_tokens_estimate: 500,
    projected_capacity_tokens_estimate: 1000,
    remaining_to_now_tokens_estimate: 400,
    confidence: 'high',
    contributing_upstreams: state === 'absent' ? 0 : 1,
    stale_upstreams: 0,
    missing_capacity_upstreams: state === 'absent' ? 1 : 0,
    provider_lots: [
      {
        upstream_id: 'upstream-1',
        upstream_name: 'Upstream One',
        window,
        source: 'oauth',
        state,
        provider_start_unix_secs: NOW - 3600,
        provider_reset_unix_secs: NOW + 3600,
        observed_at_unix_millis: NOW * 1000,
        utilization:
          utilizationPercent == null ? null : utilizationPercent / 100,
        capacity_estimate_tokens: 1000,
        used_before_cc_window_tokens: 0,
        capacity_to_now_tokens_estimate: 500,
        projected_capacity_tokens_estimate: 1000,
        confidence: 'high',
        capacity_ratio: 1,
      },
    ],
    caveats: [],
  };
}

function aggregate(
  windows: readonly AggregateResponse['windows'][number][],
): AggregateResponse {
  return {
    now_unix_secs: NOW,
    window_anchor_unix_secs: NOW,
    max_staleness_secs: 300,
    upstream_count: 1,
    windows: [...windows],
    caveats: [],
  };
}

function history(windows: readonly string[]): PoolHistoryWindowResponse[] {
  return windows.map((window) => ({
    window,
    latest: {
      snapshot_at_unix_secs: NOW,
      utilization: 0.4,
      utilization_percent: 40,
      contributing_upstreams: 1,
      eligible_upstreams: 1,
      stale_upstreams: 0,
      max_observed_at_unix_millis: NOW * 1000,
    },
    series: [{ snapshot_at_unix_secs: NOW, utilization_percent: 40 }],
  }));
}

describe('presentProviderWindows', () => {
  it('hides an absent 5h pool window even when old history remains', () => {
    const result = presentProviderWindows(
      ['5h', '7d', '7d_fable'],
      aggregate([
        aggregateWindow('5h', 'absent', null),
        aggregateWindow('7d', 'ready', 40),
      ]),
      history(['5h', '7d']),
      false,
    );

    expect(result).toEqual(['7d']);
  });

  it('shows the 5h pool window again when a later sample returns', () => {
    const result = presentProviderWindows(
      ['5h', '7d', '7d_fable'],
      aggregate([
        aggregateWindow('5h', 'ready', 42),
        aggregateWindow('7d', 'ready', 40),
      ]),
      history(['5h', '7d']),
      false,
    );

    expect(result).toEqual(['5h', '7d']);
  });
  it('keeps a fresh first provider reading visible before history has snapshots', () => {
    const windows = history(['gemini_5h', 'third_party_7d']).map((entry) => ({
      ...entry,
      series: [],
    }));
    expect(
      presentProviderWindows(
        ['gemini_5h', 'gemini_7d', 'third_party_5h', 'third_party_7d'],
        undefined,
        windows,
        false,
      ),
    ).toEqual(['gemini_5h', 'third_party_7d']);
  });

  it('keeps an all-stale eligible pool visible with no historical numeric reading', () => {
    const windows = history(['5h']).map((entry) => ({
      ...entry,
      latest: {
        ...entry.latest,
        snapshot_at_unix_secs: NOW,
        utilization: null,
        utilization_percent: null,
        eligible_upstreams: 1,
        contributing_upstreams: 0,
        stale_upstreams: 1,
        max_observed_at_unix_millis: null,
      },
      series: [],
    }));
    expect(
      presentProviderWindows(['5h', '7d'], undefined, windows, false),
    ).toEqual(['5h']);
  });
  it('uses live eligibility instead of superseded aggregate lots for provider windows', () => {
    const entry = history(['5h'])[0];
    if (!entry?.latest) throw new Error('Expected a fixture reading');
    const live = {
      ...entry,
      latest: { ...entry.latest, max_staleness_secs: 300 },
    };
    expect(
      presentProviderWindows(
        ['5h'],
        aggregate([aggregateWindow('5h', 'absent', null)]),
        [live],
        false,
      ),
    ).toEqual(['5h']);
    expect(
      presentProviderWindows(
        ['5h'],
        aggregate([aggregateWindow('5h', 'ready', 40)]),
        [{ ...live, latest: { ...live.latest, eligible_upstreams: 0 } }],
        false,
      ),
    ).toEqual([]);
  });
});

describe('provider pool quota legend', () => {
  it.each(['5h', 'gemini_5h'])(
    'uses weighted account pace and the next account reset for %s',
    (window) => {
      const entry = {
        ...aggregateWindow(window, 'fresh', 87.6),
        pool_pace_fraction: 0.54,
        next_reset_unix_secs: NOW + 2 * 3600,
      };
      const response = aggregate([entry]);
      render(
        <PoolQuotaLegend
          latest={{ [window]: 87.6 }}
          nowUnixSecs={NOW}
          paces={poolWindowPace(response)}
          resets={poolWindowResets(response)}
          windows={[window]}
        />,
      );

      const row = screen.getByTestId('pool-quota-legend-slot');
      const tick = row.querySelector('[data-slot="pace-marker"]');
      expect(tick?.getAttribute('data-pace-pct')).toBe('54');
      expect((tick as HTMLElement).style.left).toBe('54%');
      expect(
        row.querySelector('[role="meter"]')?.getAttribute('aria-valuetext'),
      ).toBe('88% used, even pace 54%');
      expect(
        row.querySelector('[data-slot="pool-quota-legend-used"]')?.className,
      ).toContain('text-danger-text');
      expect(
        row.querySelector('[data-slot="pool-quota-legend-reset"]')?.textContent,
      ).toBe('2h');
      expect(row.getAttribute('aria-label')).toContain('resets in 2h');
    },
  );

  it('draws no pace tick when the server has no pace despite an upcoming reset', () => {
    const response = aggregate([
      {
        ...aggregateWindow('5h', 'fresh', 87.6),
        pool_pace_fraction: null,
      },
    ]);
    render(
      <PoolQuotaLegend
        latest={{ '5h': 87.6 }}
        nowUnixSecs={NOW}
        paces={poolWindowPace(response)}
        resets={poolWindowResets(response)}
        windows={['5h']}
      />,
    );

    const row = screen.getByTestId('pool-quota-legend-slot');
    expect(row.querySelector('[data-slot="pace-marker"]')).toBeNull();
    expect(
      row.querySelector('[data-slot="pool-quota-legend-used"]')?.className,
    ).toContain('text-text');
    expect(
      row.querySelector('[role="meter"]')?.getAttribute('aria-valuetext'),
    ).toBe('88% used');
  });

  it('keeps a zero pace tick without inventing an account reset', () => {
    const response = aggregate([
      {
        ...aggregateWindow('5h', 'fresh', 40),
        pool_pace_fraction: 0,
        next_reset_unix_secs: null,
      },
    ]);
    render(
      <PoolQuotaLegend
        latest={{ '5h': 40 }}
        nowUnixSecs={NOW}
        paces={poolWindowPace(response)}
        resets={poolWindowResets(response)}
        windows={['5h']}
      />,
    );

    const row = screen.getByTestId('pool-quota-legend-slot');
    expect(
      row
        .querySelector('[data-slot="pace-marker"]')
        ?.getAttribute('data-pace-pct'),
    ).toBe('0');
    expect(
      row.querySelector('[data-slot="pool-quota-legend-reset"]')?.textContent,
    ).toBe('—');
  });
});
