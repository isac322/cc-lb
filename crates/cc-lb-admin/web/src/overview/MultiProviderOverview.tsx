import { Link, useNavigate, useSearch } from '@tanstack/react-router';
import { useMemo } from 'react';
import * as z from 'zod';
import {
  PageContainer,
  Section,
  SegmentedControl,
} from '../components/ui/primitives';
import {
  FeedLiveStatus,
  RequestEventsFeed,
} from '../components/ui/RequestEventsFeed';
import type { DashboardUsageResponse } from '../lib/api';
import { type ProviderFamily, useInstallation } from '../lib/providers';
import { usePrincipalNameMap, useUpstreams, useUsage } from '../lib/queries';
import {
  TIME_PRESET_OPTIONS,
  TIME_PRESET_SECONDS,
  TIME_PRESETS,
  type TimePreset,
  useSharedTimeRange,
} from '../lib/timePresets';
import { useRequestEventsFeed } from '../lib/useRequestEventsFeed';
import { PrincipalProviderTable } from './PrincipalProviderTable';
import { ProviderPoolCard } from './ProviderPoolCard';
import { mergePrincipalProviderUsage } from './principalMerge';

function LiveRequestsSection({
  principalNameMap,
  providers,
}: {
  principalNameMap: Map<string, string>;
  providers: readonly ProviderFamily[];
}) {
  const feed = useRequestEventsFeed({
    filters: {},
    providers,
    mode: 'infinite',
    initialHistoryLimit: 500,
    pageSize: 50,
    maxRetained: 500,
    live: true,
  });

  return (
    <Section
      title="Latest requests (any time)"
      subtitle={
        <span className="flex flex-wrap items-center gap-x-3 gap-y-1">
          <FeedLiveStatus feed={feed} />
          <span>
            Newest first, not limited to the usage range — full view on Logs
            page
          </span>
        </span>
      }
      action={
        <Link
          to="/logs"
          className="relative -my-2 inline-flex min-h-10 items-center px-2 text-body-sm text-accent-text underline after:absolute after:-inset-2 after:content-['']"
        >
          Open logs
        </Link>
      }
    >
      <RequestEventsFeed
        feed={feed}
        principalNameMap={principalNameMap}
        emptyTitle="No recent requests"
        tableContainerClassName="relative min-w-0 max-md:-mx-4 md:glass md:rounded-md min-h-48 h-96 max-h-[60vh] overflow-auto"
      />
    </Section>
  );
}

export const multiProviderOverviewSearchSchema = z.object({
  range: z.enum(TIME_PRESETS).optional().catch(undefined),
});

type UsageResult = {
  data: DashboardUsageResponse | undefined;
  isPending: boolean;
  isError: boolean;
};

function stepFor(range: TimePreset): 'hour' | 'minute' {
  return range === '7d' || range === '24h' ? 'hour' : 'minute';
}
function PrincipalUsageSection({
  range,
  providers,
  principalNames,
}: {
  range: TimePreset;
  providers: readonly ProviderFamily[];
  principalNames: Map<string, string>;
}) {
  const step = stepFor(range);
  const anthropicUsage = useUsage(
    range,
    step,
    'principal',
    undefined,
    'totals',
    { providers: providers.includes('anthropic') ? ['anthropic'] : [] },
  );
  const openaiUsage = useUsage(range, step, 'principal', undefined, 'totals', {
    providers: providers.includes('openai') ? ['openai'] : [],
  });
  const geminiUsage = useUsage(range, step, 'principal', undefined, 'totals', {
    providers: providers.includes('gemini') ? ['gemini'] : [],
  });
  const usageQueries: Record<ProviderFamily, UsageResult> = {
    anthropic: anthropicUsage,
    openai: openaiUsage,
    gemini: geminiUsage,
  };
  const usageByProvider = useMemo(
    () =>
      ({
        anthropic: anthropicUsage.data,
        openai: openaiUsage.data,
        gemini: geminiUsage.data,
      }) as Partial<Record<ProviderFamily, DashboardUsageResponse | undefined>>,
    [anthropicUsage.data, openaiUsage.data, geminiUsage.data],
  );
  const rows = useMemo(
    () => mergePrincipalProviderUsage(usageByProvider, principalNames),
    [principalNames, usageByProvider],
  );
  const anyLoading = providers.some(
    (provider) => usageQueries[provider].isPending,
  );
  const anyError = providers.some((provider) => usageQueries[provider].isError);

  return (
    <Section
      title="Principal usage"
      subtitle={`Estimated list price · last ${range}`}
    >
      <PrincipalProviderTable
        error={anyError}
        loading={anyLoading}
        providers={providers}
        rows={rows}
      />
    </Section>
  );
}

export function MultiProviderOverview() {
  const installation = useInstallation();
  const upstreams = useUpstreams();
  const providers = installation.providers;
  const search = useSearch({ from: '/' }) as {
    range?: TimePreset;
  };
  const shared = useSharedTimeRange();
  const range = search.range ?? shared.range;
  const navigate = useNavigate({ from: '/' });
  const principalNames = usePrincipalNameMap();
  const configuredUpstreams = upstreams.data?.upstreams ?? [];
  const hasSubscription = (provider: ProviderFamily) =>
    configuredUpstreams.some(
      (upstream) =>
        upstream.kind.startsWith(`${provider}_`) &&
        upstream.kind.endsWith('_oauth'),
    );
  const selectRange = (next: TimePreset) => {
    shared.setRange(next);
    void navigate({
      search: (previous) => ({ ...previous, range: next }),
      replace: true,
      resetScroll: false,
    });
  };
  const poolGridClass =
    providers.length === 1
      ? 'grid min-w-0 gap-4'
      : providers.length === 2
        ? 'grid min-w-0 gap-4 md:grid-cols-2'
        : 'grid min-w-0 gap-4 md:grid-cols-2 lg:grid-cols-3';

  return (
    <PageContainer>
      <div
        className="min-w-0 space-y-12 lg:space-y-16"
        data-testid="multi-provider-overview"
      >
        <header className="flex flex-wrap items-center justify-between gap-4">
          <div className="min-w-0">
            <h1 className="text-title-page text-text">Overview</h1>
            <p className="mt-1 text-body-sm text-text-muted">
              Subscription pools and principal usage by provider
            </p>
          </div>
          <SegmentedControl
            ariaLabel="Usage range"
            className="shrink-0 max-md:grid max-md:w-full max-md:grid-cols-4"
            options={TIME_PRESET_OPTIONS}
            value={range}
            onChange={selectRange}
          />
        </header>
        <Section title="Pool quota usage">
          <div className={poolGridClass}>
            {providers.map((provider) => (
              <ProviderPoolCard
                compact
                hasSubscription={hasSubscription(provider)}
                key={provider}
                provider={provider}
                rangeSecs={TIME_PRESET_SECONDS[range]}
              />
            ))}
          </div>
        </Section>
        <PrincipalUsageSection
          principalNames={principalNames}
          providers={providers}
          range={range}
        />
        <LiveRequestsSection
          principalNameMap={principalNames}
          providers={providers}
        />
      </div>
    </PageContainer>
  );
}
