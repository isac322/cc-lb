import { cleanup, render, screen } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest';
import type { Upstream } from '../../lib/queries';
import * as queries from '../../lib/queries';
import { UpstreamPlanCaption } from './UpstreamPlanCaption';

vi.mock('../../lib/queries', () => ({
  useUpstreamSubscriptionMetadata: vi.fn(),
}));

const upstream = (kind: Upstream['kind']): Upstream =>
  ({ id: 'example-upstream', kind }) as Upstream;

beforeEach(() => vi.clearAllMocks());
afterEach(cleanup);

describe('provider plan captions', () => {
  test.each([
    [
      'anthropic_oauth',
      {
        organization_metadata: {
          organization_type: 'claude_max',
          rate_limit_tier: 'default_claude_max_20x',
        },
      },
      'Claude Ultra',
    ],
    [
      'anthropic_oauth',
      {
        organization_metadata: {
          organization_type: 'claude_max',
          rate_limit_tier: 'default_claude_max_5x',
        },
      },
      'Claude Max',
    ],
    [
      'anthropic_oauth',
      {
        organization_metadata: {
          organization_type: 'claude_ultra',
          rate_limit_tier: 'default_claude_ultra',
        },
      },
      'Claude Ultra',
    ],
    ['openai_oauth', { plan: 'pro' }, 'ChatGPT Pro (More)'],
    ['openai_oauth', { plan: 'prolite' }, 'ChatGPT Pro'],
    ['openai_oauth', { plan: 'promax' }, 'ChatGPT Pro (Max)'],
    ['openai_oauth', { plan: 'business' }, 'ChatGPT Business'],
    [
      'gemini_oauth',
      { plan: 'g1-ultra-tier', plan_name: 'Google AI Ultra' },
      'Google AI Ultra',
    ],
    [
      'gemini_oauth',
      { plan: 'future-tier', plan_name: 'Future Google Plan' },
      'Future Google Plan',
    ],
    [
      'anthropic_oauth',
      { organization_metadata: { organization_type: 'future_plan' } },
      'Future Plan',
    ],
    ['openai_oauth', { plan: 'future_plan' }, 'ChatGPT Future Plan'],
    ['gemini_oauth', { plan: 'future-tier' }, 'Future Tier'],
    ['openai_oauth', { plan: null }, 'Subscription'],
    ['gemini_oauth', { plan: null }, 'Subscription'],
  ] as const)(
    'renders %s using provider-owned names',
    (kind, metadata, label) => {
      vi.mocked(queries.useUpstreamSubscriptionMetadata).mockReturnValue({
        data: metadata,
        isPending: false,
      } as never);
      render(<UpstreamPlanCaption upstream={upstream(kind)} />);
      expect(screen.getByText(label)).toBeDefined();
      expect(queries.useUpstreamSubscriptionMetadata).toHaveBeenCalledWith(
        'example-upstream',
      );
    },
  );

  test.each(['anthropic_api_key', 'openai_api_key', 'gemini_api_key'] as const)(
    'keeps %s independent from subscription metadata',
    (kind) => {
      vi.mocked(queries.useUpstreamSubscriptionMetadata).mockReturnValue({
        data: undefined,
        isPending: true,
      } as never);
      render(<UpstreamPlanCaption upstream={upstream(kind)} />);
      expect(screen.getByText('API key')).toBeDefined();
      expect(screen.queryByText('Subscription')).toBeNull();
    },
  );

  test.each(['anthropic_oauth', 'openai_oauth', 'gemini_oauth'] as const)(
    'does not invent a plan while %s metadata loads',
    (kind) => {
      vi.mocked(queries.useUpstreamSubscriptionMetadata).mockReturnValue({
        data: undefined,
        isPending: true,
      } as never);
      const { container, rerender } = render(
        <UpstreamPlanCaption upstream={upstream(kind)} />,
      );
      expect(container.querySelector('.skeleton')).not.toBeNull();
      expect(screen.queryByText('Subscription')).toBeNull();
      vi.mocked(queries.useUpstreamSubscriptionMetadata).mockReturnValue({
        data: undefined,
        isPending: false,
        isError: true,
      } as never);
      rerender(<UpstreamPlanCaption upstream={upstream(kind)} />);
      expect(screen.getByText('Subscription')).toBeDefined();
    },
  );

  test.each(['openai_oauth', 'gemini_oauth'] as const)(
    'does not retain the previous account plan when selecting another %s',
    (kind) => {
      vi.mocked(queries.useUpstreamSubscriptionMetadata).mockImplementation(
        (id) =>
          ({
            data:
              id === 'first-account'
                ? {
                    plan: 'team',
                    plan_name:
                      kind === 'gemini_oauth' ? 'First Google Plan' : null,
                  }
                : undefined,
            isPending: id !== 'first-account',
          }) as never,
      );
      const { rerender } = render(
        <UpstreamPlanCaption
          upstream={{ ...upstream(kind), id: 'first-account' }}
        />,
      );
      const label =
        kind === 'openai_oauth' ? 'ChatGPT Team' : 'First Google Plan';
      expect(screen.getByText(label)).toBeDefined();
      rerender(
        <UpstreamPlanCaption
          upstream={{ ...upstream(kind), id: 'second-account' }}
        />,
      );
      expect(screen.queryByText(label)).toBeNull();
      expect(screen.queryByText('Subscription')).toBeNull();
    },
  );
});
