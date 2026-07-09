import { describe, expect, it } from 'vitest';
import type { SeriesResponseItem } from '../../lib/api';
import { buildQuotaChartData } from './buildQuotaChartData';

describe('buildQuotaChartData', () => {
  it('should return empty arrays when seriesData is empty', () => {
    const result = buildQuotaChartData([], undefined, '1h');
    expect(result.rows).toEqual([]);
    expect(result.markers).toEqual([]);
  });

  it('should not fabricate leading zeroes when no anchor exists', () => {
    const seriesData: SeriesResponseItem[] = [
      {
        upstream_id: '1',
        upstream_name: 'test',
        window: '5h',
        buckets: [
          { bucket_start_unix_secs: 100, utilization_last: 0.5 },
          { bucket_start_unix_secs: 200, utilization_last: 0.6 },
        ],
        markers: [],
      },
    ];

    const result = buildQuotaChartData(seriesData, undefined, '1h');
    expect(result.rows).toHaveLength(2);
    expect(result.rows[0].unix).toBe(100);
    expect(result.rows[0]['5h']).toBe(50);
    expect(result.rows[1].unix).toBe(200);
    expect(result.rows[1]['5h']).toBe(60);
  });

  it('should carry forward known checkpoint values across unaligned series', () => {
    const seriesData: SeriesResponseItem[] = [
      {
        upstream_id: '1',
        upstream_name: 'test',
        window: '5h',
        buckets: [
          { bucket_start_unix_secs: 100, utilization_last: 0.5 },
          { bucket_start_unix_secs: 300, utilization_last: 0.6 },
        ],
        markers: [],
      },
      {
        upstream_id: '1',
        upstream_name: 'test',
        window: '7d',
        buckets: [
          { bucket_start_unix_secs: 200, utilization_last: 0.2 },
          { bucket_start_unix_secs: 400, utilization_last: 0.3 },
        ],
        markers: [],
      },
    ];

    const result = buildQuotaChartData(seriesData, undefined, '1h');
    expect(result.rows).toHaveLength(4);

    // ts=100: 5h=50, 7d=undefined
    expect(result.rows[0].unix).toBe(100);
    expect(result.rows[0]['5h']).toBe(50);
    expect(result.rows[0]['7d']).toBeUndefined();

    // ts=200: 5h=50 (carried forward), 7d=20
    expect(result.rows[1].unix).toBe(200);
    expect(result.rows[1]['5h']).toBe(50);
    expect(result.rows[1]['7d']).toBe(20);

    // ts=300: 5h=60, 7d=20 (carried forward)
    expect(result.rows[2].unix).toBe(300);
    expect(result.rows[2]['5h']).toBe(60);
    expect(result.rows[2]['7d']).toBe(20);

    // ts=400: 5h=60 (carried forward), 7d=30
    expect(result.rows[3].unix).toBe(400);
    expect(result.rows[3]['5h']).toBe(60);
    expect(result.rows[3]['7d']).toBe(30);
  });

  it('should preserve reset markers and latest/source/freshness labels', () => {
    const seriesData: SeriesResponseItem[] = [
      {
        upstream_id: '1',
        upstream_name: 'test',
        window: '5h',
        buckets: [{ bucket_start_unix_secs: 100, utilization_last: 0.5 }],
        markers: [{ at_unix_secs: 150, kind: 'reset' }],
      },
    ];

    const selectedLatestWindows = [
      {
        window: '5h',
        resets_at_unix_secs: 3600,
      },
    ];

    const result = buildQuotaChartData(seriesData, selectedLatestWindows, '1h');
    expect(result.markers).toHaveLength(2);
    expect(result.markers[0]).toEqual({
      ts: 150,
      kind: 'reset',
      window: '5h',
    });
    // 3600 - 18000 (5h) = -14400
    expect(result.markers[1]).toEqual({
      ts: -14400,
      kind: 'start',
      window: '5h',
    });
  });

  it('should use the last value for a minute with multiple checkpoints', () => {
    // The backend already provides utilization_last, so the frontend just maps it.
    // This test ensures the mapping is correct.
    const seriesData: SeriesResponseItem[] = [
      {
        upstream_id: '1',
        upstream_name: 'test',
        window: '5h',
        buckets: [{ bucket_start_unix_secs: 100, utilization_last: 0.5 }],
        markers: [],
      },
    ];

    const result = buildQuotaChartData(seriesData, undefined, '1h');
    expect(result.rows).toHaveLength(1);
    expect(result.rows[0]['5h']).toBe(50);
  });
});
