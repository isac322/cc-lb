import { describe, expect, it } from 'vitest';
import type { PeriodUnknownQuotaMeter } from '../../lib/api';
import {
  formatPeriodUnknownUsedPct,
  selectPeriodUnknownMeters,
} from './periodUnknownMeters';

describe('QA-17: period-unknown quota meters', () => {
  it('tolerates old servers, omitted fields and missing upstream rows', () => {
    expect(selectPeriodUnknownMeters(null)).toEqual([]);
    expect(selectPeriodUnknownMeters(undefined)).toEqual([]);
    expect(
      selectPeriodUnknownMeters({
        upstream_id: '1',
        upstream_name: 'Gemini',
        windows: [],
      }),
    ).toEqual([]);
  });

  it('keeps both groups, stale meters and identifiers without synthesizing canonical windows', () => {
    const meters: PeriodUnknownQuotaMeter[] = ['gemini', 'third_party'].map(
      (group) => ({
        group: group as PeriodUnknownQuotaMeter['group'],
        bucket_id: 'bucket-1',
        remaining_fraction: 0.25,
        utilization: 0.75,
        reset_time_unix_secs: null,
        origin: 'retrieve_user_quota_summary',
        source: 'api',
        state: 'stale',
        observed_at_unix_millis: 1000,
        age_secs: 600,
      }),
    );
    const latest = {
      upstream_id: '1',
      upstream_name: 'Gemini',
      windows: [],
      period_unknown_meters: meters,
    };
    expect(selectPeriodUnknownMeters(latest)).toBe(meters);
    expect(latest.windows).toEqual([]);
  });

  it.each([
    [0, 100],
    [1, 0],
    [0.4, 60],
    [-2, 100],
    [2, 0],
    [null, null],
    [undefined, null],
    [NaN, null],
    [Infinity, null],
    [-Infinity, null],
  ])('formats remaining %s as used %s', (remaining, used) => {
    expect(formatPeriodUnknownUsedPct(remaining)).toBe(used);
  });
});
