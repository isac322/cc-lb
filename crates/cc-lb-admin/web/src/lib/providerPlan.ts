import type { OrganizationMetadataInner } from './api';
import type { ProviderFamily } from './providers';

const PLAN_SEPARATORS = /[_-]+/g;
const WORD_START = /\b\w/g;
const CLAUDE_MAX_MULTIPLIER = /max_(\d+)x/i;
const CLAUDE_ULTRA = /ultra|max_20x/i;
const CLAUDE_MAX = /max/i;
const CLAUDE_PRO = /pro/i;
const CLAUDE_TEAM = /team/i;
const CLAUDE_ENTERPRISE = /enterprise/i;

const CHATGPT_PLANS: Readonly<Record<string, string>> = {
  free: 'Free',
  plus: 'Plus',
  pro: 'Pro (More)',
  prolite: 'Pro',
  promax: 'Pro (Max)',
  team: 'Team',
  business: 'Business',
  enterprise: 'Enterprise',
};

export interface ProviderPlanMetadata {
  plan?: string | null;
  plan_name?: string | null;
  organization_metadata?: Pick<
    OrganizationMetadataInner,
    'organization_type' | 'rate_limit_tier'
  > | null;
}

function readablePlan(value: string): string {
  return value
    .replace(PLAN_SEPARATORS, ' ')
    .replace(WORD_START, (letter) => letter.toUpperCase());
}

/** Display only: provider plan names never determine routing or quota weight. */
export function providerPlanLabel(
  provider: ProviderFamily,
  metadata: ProviderPlanMetadata | null | undefined,
): string | null {
  if (provider === 'anthropic') {
    const tier = metadata?.organization_metadata?.rate_limit_tier?.trim() ?? '';
    const type =
      metadata?.organization_metadata?.organization_type?.trim() ?? '';
    if (CLAUDE_ULTRA.test(tier) || CLAUDE_ULTRA.test(type))
      return 'Claude Ultra';
    const multiplier = CLAUDE_MAX_MULTIPLIER.exec(tier)?.[1];
    if (multiplier && multiplier !== '5' && multiplier !== '20')
      return `Claude Max ${multiplier}x`;
    if (CLAUDE_MAX.test(tier) || CLAUDE_MAX.test(type)) return 'Claude Max';
    if (CLAUDE_PRO.test(type)) return 'Claude Pro';
    if (CLAUDE_TEAM.test(type)) return 'Claude Team';
    if (CLAUDE_ENTERPRISE.test(type)) return 'Claude Enterprise';
    return type || tier ? readablePlan(type || tier) : null;
  }

  const plan = metadata?.plan?.trim();
  if (provider === 'gemini') {
    return metadata?.plan_name?.trim() || (plan ? readablePlan(plan) : null);
  }
  if (!plan) return null;
  return `ChatGPT ${CHATGPT_PLANS[plan.toLowerCase()] ?? readablePlan(plan)}`;
}
