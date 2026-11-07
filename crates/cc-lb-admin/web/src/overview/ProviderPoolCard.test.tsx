import { describe, expect, it } from 'vitest';
import type { AggregateResponse, PoolHistoryWindowResponse } from '../lib/api';
import { presentProviderWindows } from './ProviderPoolCard';

const NOW = 1_700_000_000;

function aggregateWindow(
  window: string,
  state: string,
  utilizationPercent: number | null,
): AggregateResponse['windows'][number] {
  return {
    window,
    cc_window_start_unix_secs: NOW - 3600,
    cc_window_reset_unix_secs: NOW + 3600,
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
});
