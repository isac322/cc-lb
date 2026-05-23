import { useSearchParams } from 'react-router';
import { KpiCardGrid } from '../components/overview/KpiCardGrid';
import { RangeSelector } from '../components/overview/RangeSelector';
import { StackedAreaChart } from '../components/overview/StackedAreaChart';
import { PrincipalUsageStrip } from '../components/overview/PrincipalUsageStrip';
import { EmbeddedLiveLog } from '../components/overview/EmbeddedLiveLog';
import { useDashboardSummary } from '../lib/hooks/useDashboardSummary';
import { useUsageSeries } from '../lib/hooks/useUsageSeries';
import { DashboardSummaryResponse, DashboardUsageResponse } from '../lib/api';

// Mock data for ?mock=1 mode
const MOCK_SUMMARY: DashboardSummaryResponse = {
  range: '1h',
  step: 'minute',
  window_start_unix_secs: Math.floor(Date.now() / 1000) - 3600,
  window_end_unix_secs: Math.floor(Date.now() / 1000),
  totals: {
    request_count: 12403,
    input_tokens: 1200000,
    output_tokens: 880000,
    error_count: 670,
    error_rate: 0.054,
    virtual_cost_micros: 45000000,
    avg_latency_ms: 450,
  },
  sparkline: {
    buckets: Array.from({ length: 60 }).map((_, i) => ({
      bucket_start_unix_secs: Math.floor(Date.now() / 1000) - 3600 + i * 60,
      request_count: 150 + Math.floor(Math.random() * 100),
      input_tokens: 15000 + Math.floor(Math.random() * 10000),
      output_tokens: 10000 + Math.floor(Math.random() * 8000),
      error_count: Math.floor(Math.random() * 10),
      virtual_cost_micros: 500000 + Math.floor(Math.random() * 200000),
      latency_ms_sum: 45000,
      latency_count: 100,
    })),
  },
  observed: true,
};

const MOCK_USAGE_MODEL: DashboardUsageResponse = {
  range: '1h',
  step: 'minute',
  group_by: 'model',
  window_start_unix_secs: Math.floor(Date.now() / 1000) - 3600,
  window_end_unix_secs: Math.floor(Date.now() / 1000),
  series: [
    {
      key: 'claude-3-opus-20240229',
      buckets: MOCK_SUMMARY.sparkline.buckets.map(b => ({
        ...b,
        request_count: Math.floor(b.request_count * 0.3),
      })),
    },
    {
      key: 'claude-3-sonnet-20240229',
      buckets: MOCK_SUMMARY.sparkline.buckets.map(b => ({
        ...b,
        request_count: Math.floor(b.request_count * 0.7),
      })),
    },
  ],
  observed: true,
};

const MOCK_USAGE_PRINCIPAL: DashboardUsageResponse = {
  range: '1h',
  step: 'minute',
  group_by: 'principal',
  window_start_unix_secs: Math.floor(Date.now() / 1000) - 3600,
  window_end_unix_secs: Math.floor(Date.now() / 1000),
  series: [
    {
      key: 'principal-prod-app-1',
      buckets: MOCK_SUMMARY.sparkline.buckets.map(b => ({
        ...b,
        request_count: Math.floor(b.request_count * 0.8),
        input_tokens: Math.floor(b.input_tokens * 0.8),
        output_tokens: Math.floor(b.output_tokens * 0.8),
        error_count: Math.floor(b.error_count * 0.8),
        virtual_cost_micros: Math.floor(b.virtual_cost_micros * 0.8),
      })),
    },
    {
      key: 'principal-dev-test-2',
      buckets: MOCK_SUMMARY.sparkline.buckets.map(b => ({
        ...b,
        request_count: Math.floor(b.request_count * 0.2),
        input_tokens: Math.floor(b.input_tokens * 0.2),
        output_tokens: Math.floor(b.output_tokens * 0.2),
        error_count: Math.floor(b.error_count * 0.2),
        virtual_cost_micros: Math.floor(b.virtual_cost_micros * 0.2),
      })),
    },
  ],
  observed: true,
};

export default function Overview() {
  const [searchParams] = useSearchParams();
  const range = searchParams.get('range') || '1h';
  const isMock = searchParams.get('mock') === '1';

  const summaryHook = useDashboardSummary(range);
  const usageModelHook = useUsageSeries(range, 'model');
  const usagePrincipalHook = useUsageSeries(range, 'principal');

  const summary = isMock ? MOCK_SUMMARY : summaryHook.data;
  const summaryLoading = isMock ? false : summaryHook.isLoading;
  const summaryError = isMock ? null : summaryHook.error;

  const usageModel = isMock ? MOCK_USAGE_MODEL : usageModelHook.data;
  const usageModelLoading = isMock ? false : usageModelHook.isLoading;
  const usageModelError = isMock ? null : usageModelHook.error;

  const usagePrincipal = isMock ? MOCK_USAGE_PRINCIPAL : usagePrincipalHook.data;
  const usagePrincipalLoading = isMock ? false : usagePrincipalHook.isLoading;
  const usagePrincipalError = isMock ? null : usagePrincipalHook.error;

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h2 className="text-lg font-semibold text-graphite-50">Overview</h2>
        <RangeSelector />
      </div>

      {summaryError && (
        <div className="p-4 bg-red-500/10 border border-red-500/20 rounded-md text-red-400 text-sm flex items-center justify-between">
          <span>Failed to load summary: {summaryError.message}</span>
          <button onClick={summaryHook.refresh} className="px-3 py-1 bg-red-500/20 hover:bg-red-500/30 rounded transition-colors">
            Retry
          </button>
        </div>
      )}

      <KpiCardGrid summary={summary} isLoading={summaryLoading} />

      {usageModelError && (
        <div className="p-4 bg-red-500/10 border border-red-500/20 rounded-md text-red-400 text-sm flex items-center justify-between">
          <span>Failed to load chart data: {usageModelError.message}</span>
          <button onClick={usageModelHook.refresh} className="px-3 py-1 bg-red-500/20 hover:bg-red-500/30 rounded transition-colors">
            Retry
          </button>
        </div>
      )}

      <StackedAreaChart usage={usageModel} isLoading={usageModelLoading} />

      {usagePrincipalError && (
        <div className="p-4 bg-red-500/10 border border-red-500/20 rounded-md text-red-400 text-sm flex items-center justify-between">
          <span>Failed to load principal usage: {usagePrincipalError.message}</span>
          <button onClick={usagePrincipalHook.refresh} className="px-3 py-1 bg-red-500/20 hover:bg-red-500/30 rounded transition-colors">
            Retry
          </button>
        </div>
      )}

      <PrincipalUsageStrip usage={usagePrincipal} isLoading={usagePrincipalLoading} />

      <EmbeddedLiveLog />
    </div>
  );
}
