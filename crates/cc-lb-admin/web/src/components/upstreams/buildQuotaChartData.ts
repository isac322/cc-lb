import type { SeriesResponseItem } from '../../lib/api';
import { WINDOW_DURATION_SECS } from '../../lib/colors';

export type ChartRow = { ts: string; unix: number } & Record<
  string,
  number | null | string
>;
export type ChartMarker = { ts: number; kind: string; window: string };

export function buildQuotaChartData(
  seriesData: readonly SeriesResponseItem[] | undefined,
  selectedLatestWindows:
    | readonly { window: string; resets_at_unix_secs?: number | null }[]
    | undefined,
  range: '1h' | '6h' | '24h' | '7d',
) {
  if (!seriesData?.length) {
    return { rows: [] as ChartRow[], markers: [] as ChartMarker[] };
  }

  const bucketsByTime = new Map<number, ChartRow>();
  const markers: ChartMarker[] = [];

  for (const series of seriesData) {
    const windowName = series.window;
    for (const bucket of series.buckets) {
      const ts = bucket.bucket_start_unix_secs;
      if (!bucketsByTime.has(ts)) {
        const date = new Date(ts * 1000);
        const label =
          range === '7d' || range === '24h'
            ? `${date.getMonth() + 1}/${date.getDate()} ${date.getHours()}h`
            : date.toTimeString().slice(0, 5);
        bucketsByTime.set(ts, { ts: label, unix: ts });
      }
      const row = bucketsByTime.get(ts);
      if (row) {
        row[windowName] =
          bucket.utilization_last != null
            ? bucket.utilization_last * 100
            : null;
      }
    }

    for (const marker of series.markers) {
      if (marker.at_unix_secs) {
        markers.push({
          ts: marker.at_unix_secs,
          kind: marker.kind,
          window: windowName,
        });
      }
    }
  }

  if (selectedLatestWindows) {
    for (const snap of selectedLatestWindows) {
      const duration = WINDOW_DURATION_SECS[snap.window];
      if (snap.resets_at_unix_secs && duration) {
        markers.push({
          ts: snap.resets_at_unix_secs - duration,
          kind: 'start',
          window: snap.window,
        });
      }
    }
  }

  // To ensure step interpolation works correctly across multiple series with unaligned timestamps,
  // we need to carry forward the last known value for each series explicitly if a bucket is missing.
  // But if the backend provides buckets at exact checkpoint timestamps, they might not align.
  // Let's sort the rows first.
  const rows = Array.from(bucketsByTime.values()).sort(
    (a, b) => a.unix - b.unix,
  );

  // Carry forward values for step chart if a series has a missing bucket but another series has one.
  const lastKnownValues: Record<string, number | null> = {};
  const safeSeriesData: readonly SeriesResponseItem[] = seriesData ?? [];
  for (const row of rows) {
    for (const s of safeSeriesData) {
      const windowName = s.window;
      if (row[windowName] !== undefined && row[windowName] !== null) {
        lastKnownValues[windowName] = row[windowName] as number;
      } else if (lastKnownValues[windowName] !== undefined) {
        row[windowName] = lastKnownValues[windowName];
      }
    }
  }

  return {
    rows,
    markers,
  };
}
