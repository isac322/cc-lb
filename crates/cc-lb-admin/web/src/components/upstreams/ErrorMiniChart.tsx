import { useEffect, useState } from 'react';
import { type DashboardUsageResponse, getJson } from '../../lib/api';
import { Sparkline } from '../overview/Sparkline';

interface ErrorMiniChartProps {
  upstreamName: string;
}

export function ErrorMiniChart({ upstreamName }: ErrorMiniChartProps) {
  const [buckets, setBuckets] = useState<
    { bucket_start_unix_secs: number; error_count: number }[]
  >([]);
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    const isMock =
      new URLSearchParams(window.location.search).get('mock') === '1';
    if (isMock) {
      const now = Math.floor(Date.now() / 1000);
      const mockBuckets = Array.from({ length: 12 }).map((_, i) => ({
        bucket_start_unix_secs: now - (11 - i) * 300,
        error_count: Math.floor(Math.random() * 5),
      }));
      setBuckets(mockBuckets);
      setIsLoading(false);
      return;
    }

    getJson<DashboardUsageResponse>(
      '/admin/usage?range=1h&step=minute&group_by=upstream',
    )
      .then((res) => {
        const series = res.series.find((s) => s.key === upstreamName);
        if (series) {
          // Take last 12 buckets (1 hour with 5-min step, or 12 mins with 1-min step)
          // The API returns 1-min step for 1h range. We'll just take the last 12.
          const last12 = series.buckets.slice(-12).map((b) => ({
            bucket_start_unix_secs: b.bucket_start_unix_secs,
            error_count: b.error_count,
          }));
          setBuckets(last12);
        }
      })
      .catch(() => {
        // Ignore errors, just hide chart
      })
      .finally(() => {
        setIsLoading(false);
      });
  }, [upstreamName]);

  if (isLoading || buckets.length === 0) {
    return <div className="h-8 w-24 bg-gray-50 rounded animate-pulse" />;
  }

  // Map to the format expected by Sparkline component
  const sparklineBuckets = buckets.map((b) => ({
    bucket_start_unix_secs: b.bucket_start_unix_secs,
    request_count: 0,
    input_tokens: 0,
    output_tokens: 0,
    error_count: b.error_count,
    virtual_cost_micros: 0,
    latency_ms_sum: 0,
    latency_count: 0,
  }));

  return (
    <div className="h-8 w-24 relative">
      <Sparkline
        data={sparklineBuckets}
        dataKey="error_count"
        color="#ef4444"
      />
    </div>
  );
}
