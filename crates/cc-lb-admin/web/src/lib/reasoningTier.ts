import { splitNum } from './format';

/**
 * Reasoning tier classification based on thinking budget (in tokens).
 * Single tunable threshold table for budget-to-tier mapping.
 */

export const REASONING_TIER_THRESHOLDS = {
  none: { min: 0, max: 1023 },
  low: { min: 1024, max: 4000 },
  medium: { min: 4001, max: 12000 },
  high: { min: 12001, max: 24000 },
  xhigh: { min: 24001, max: 32000 },
  max: { min: 32001, max: Infinity },
} as const;

export type ReasoningTier = keyof typeof REASONING_TIER_THRESHOLDS;

/**
 * Classify a thinking budget into a reasoning tier.
 * @param budget - The thinking budget in tokens, or null/undefined
 * @returns The reasoning tier: 'none' | 'low' | 'medium' | 'high' | 'xhigh' | 'max'
 */
export function budgetToTier(budget: number | null | undefined): ReasoningTier {
  // Handle null/undefined by checking against the 'none' tier's max threshold
  if (budget === null || budget === undefined) {
    return 'none';
  }

  // Iterate through the threshold table to find the matching tier
  for (const [tier, bounds] of Object.entries(REASONING_TIER_THRESHOLDS)) {
    if (budget >= bounds.min && budget <= bounds.max) {
      return tier as ReasoningTier;
    }
  }

  // Fallback (should never reach here given the table structure)
  return 'max';
}

/** Service-tier badge text: the raw tier verbatim, or null when it should be hidden (standard / absent). */
export function serviceTierBadgeText(
  tier: string | null | undefined,
): string | null {
  if (tier == null || tier === '' || tier === 'standard') return null;
  return tier;
}

/** Badge text combining requested reasoning effort/tier with actual thinking tokens used. */
export function reasoningBadgeText(
  effort: string | null | undefined,
  budget: number | null | undefined,
  thinkingTokens: number | null | undefined,
): string | null {
  const requested =
    effort != null && effort !== ''
      ? effort
      : budgetToTier(budget) !== 'none'
        ? budgetToTier(budget)
        : null;
  let actual: string | null = null;
  if (
    thinkingTokens != null &&
    Number.isFinite(thinkingTokens) &&
    thinkingTokens > 0
  ) {
    const s = splitNum(thinkingTokens);
    actual = `${s.value}${s.unit}`;
  }
  if (requested == null && actual == null) return null;
  if (requested != null && actual != null) return `${requested} · ${actual}`;
  return requested ?? actual;
}
