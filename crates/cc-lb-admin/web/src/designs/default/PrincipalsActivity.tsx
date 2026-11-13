import { DetailSection } from '../../components/ui/DetailPane';
import {
  EmptyState,
  SegmentedControl,
  Skeleton,
} from '../../components/ui/primitives';
import type { UsageBucket } from '../../lib/api';
import { formatCostMicros, formatCount } from '../../lib/format';
import {
  usePreviewInstallation,
  useProviderScope,
} from '../../lib/previewControls';
import {
  isIndeterminatePricing,
  pricingQualifier,
  sumBucketPricing,
  sumCompletenessTokens,
} from '../../lib/pricingCompleteness';
import { PROVIDER_LABELS } from '../../lib/providers';
import { useUsage } from '../../lib/queries';
import {
  TIME_PRESET_OPTIONS,
  type TimePreset,
  useSharedTimeRange,
} from '../../lib/timePresets';

const stepFor = (range: TimePreset): 'hour' | 'minute' =>
  range === '7d' || range === '24h' ? 'hour' : 'minute';

function aggregateBuckets(buckets: readonly UsageBucket[]) {
  const totals = buckets.reduce(
    (sum, bucket) => ({
      requests: sum.requests + bucket.request_count,
      errors: sum.errors + bucket.error_count,
      input:
        sum.input +
        bucket.input_tokens +
        bucket.cache_creation_input_tokens +
        bucket.cache_read_input_tokens,
      output: sum.output + bucket.output_tokens,
      cost: sum.cost + bucket.virtual_cost_micros,
      completenessTokens:
        sum.completenessTokens + sumCompletenessTokens(bucket),
    }),
    {
      requests: 0,
      errors: 0,
      input: 0,
      output: 0,
      cost: 0,
      completenessTokens: 0,
    },
  );
  return { ...totals, ...sumBucketPricing(buckets) };
}

function RangeControl({
  range,
  onChange,
}: {
  range: TimePreset;
  onChange: (next: TimePreset) => void;
}) {
  return (
    <SegmentedControl
      ariaLabel="Usage range"
      value={range}
      options={TIME_PRESET_OPTIONS}
      onChange={onChange}
      className="max-md:grid max-md:w-full max-md:grid-cols-4"
    />
  );
}

export function PrincipalsActivity({ principalId }: { principalId: string }) {
  const installation = usePreviewInstallation();
  const { scope } = useProviderScope();
  const { range, setRange } = useSharedTimeRange();
  const providers =
    scope === 'all'
      ? installation.providers
      : installation.providers.includes(scope)
        ? [scope]
        : [];
  const query = useUsage(
    range,
    stepFor(range),
    'provider',
    undefined,
    'totals',
    {
      providers: scope === 'all' ? undefined : [scope],
      principalId,
    },
  );
  const totalsByProvider = new Map(
    (query.data?.series ?? []).map((series) => [
      series.key,
      aggregateBuckets(series.buckets),
    ]),
  );
  return (
    <DetailSection
      span="full"
      title="Provider consumption"
      description={`Activity in the ${scope === 'all' ? 'All providers' : PROVIDER_LABELS[scope]} lens. Identity, keys and limits are shared across providers.`}
      action={<RangeControl range={range} onChange={setRange} />}
    >
      {installation.mode === 'unknown' ? (
        <p role="status" className="text-body-sm text-text-muted">
          {installation.stale
            ? 'Provider inventory unavailable.'
            : 'Loading providers…'}
        </p>
      ) : !providers.length ? (
        <EmptyState title="No providers configured" />
      ) : query.isError ? (
        <p role="alert" className="text-body-sm text-danger-text">
          Provider consumption unavailable.
        </p>
      ) : query.isPending ? (
        <Skeleton className="h-12 w-full" />
      ) : (
        <div className="divide-y divide-row border-y border-row">
          {providers.map((provider) => {
            const totals = totalsByProvider.get(provider);
            return (
              <div key={`${principalId}-${provider}`} className="py-4">
                <h4 className="mb-3 text-title-card text-text">
                  {PROVIDER_LABELS[provider]}
                </h4>
                {totals ? (
                  <dl className="grid grid-cols-2 gap-x-5 gap-y-3 @2xl:grid-cols-4">
                    {[
                      ['Requests', formatCount(totals.requests)],
                      [
                        'Input / output',
                        `${formatCount(totals.input)} / ${formatCount(totals.output)}`,
                      ],
                      [
                        'Errors',
                        `${formatCount(totals.errors)}${totals.requests > 0 ? ` · ${((totals.errors / totals.requests) * 100).toFixed(1)}%` : ''}`,
                      ],
                      [
                        'Est. catalog cost',
                        isIndeterminatePricing(totals.pricing_completeness)
                          ? '—'
                          : `${formatCostMicros(totals.cost)}${totals.pricing_completeness === 'partial' ? '+' : ''}`,
                      ],
                    ].map(([label, value]) => (
                      <div key={label}>
                        <dt className="text-caption text-text-faint">
                          {label}
                        </dt>
                        <dd className="mt-1 text-body tabular-nums text-text">
                          {value}
                        </dd>
                        {label === 'Est. catalog cost' &&
                        (isIndeterminatePricing(totals.pricing_completeness) ||
                          totals.pricing_completeness === 'partial') ? (
                          <p className="text-caption text-text-faint">
                            {pricingQualifier(totals)}
                          </p>
                        ) : null}
                      </div>
                    ))}
                  </dl>
                ) : (
                  <p className="text-body-sm text-text-muted">
                    No recorded activity in this range.
                  </p>
                )}
              </div>
            );
          })}
        </div>
      )}
      <p className="mt-3 text-caption text-text-faint">
        Recorded input includes observed cache-read and cache-creation tokens.
        Cache policies differ by provider; no combined cache-miss rate is
        inferred.
      </p>
    </DetailSection>
  );
}
