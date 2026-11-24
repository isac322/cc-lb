import type {
  PeriodUnknownQuotaMeter,
  UpstreamLatestSnapshots,
} from '../../lib/api';

export function selectPeriodUnknownMeters(
  upstreamLatest: UpstreamLatestSnapshots | null | undefined,
): PeriodUnknownQuotaMeter[] {
  return upstreamLatest?.period_unknown_meters ?? [];
}

/** Convert provider remaining fraction to the product's used-percent convention. */
export function formatPeriodUnknownUsedPct(
  remainingFraction: number | null | undefined,
): number | null {
  if (remainingFraction == null || !Number.isFinite(remainingFraction))
    return null;
  return (1 - Math.min(1, Math.max(0, remainingFraction))) * 100;
}
