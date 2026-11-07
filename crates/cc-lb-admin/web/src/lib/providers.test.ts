import { describe, expect, test } from 'vitest';
import {
  type RequestEventKind,
  RequestEventKindSchema,
  type Upstream,
  type UpstreamKind,
} from './api';
import { requestModelLabel } from './logRows';
import { intersectProviders } from './previewControls';
import {
  capabilitiesOf,
  defaultEndpointFor,
  installationFromUpstreams,
  isDefaultProviderEndpoint,
  isOAuthUpstreamKind,
  parseProviders,
  providerOf,
  providersForRequestEventKind,
  requestEventKindsForProvider,
} from './providers';

function upstream(
  kind: UpstreamKind,
  enabled = true,
  error: string | null = null,
): Upstream {
  return {
    id: kind,
    name: kind,
    kind,
    enabled,
    base_url: null,
    warmup_enabled: false,
    warmup_dialect_plugin: null,
    spec_revision: 1,
    status: {
      last_apply_error: error,
      last_apply_at_unix_secs: null,
      last_warmup_at_unix_secs: null,
    },
  };
}

describe('virtual provider installation', () => {
  test('maps all six credential kinds to three families', () => {
    for (const family of ['anthropic', 'openai', 'gemini'] as const) {
      expect(providerOf(`${family}_oauth`)).toBe(family);
      expect(providerOf(`${family}_api_key`)).toBe(family);
    }
  });
  test('does not mistake loading or failed inventory for an empty installation', () => {
    expect(installationFromUpstreams(undefined).mode).toBe('unknown');
    expect(installationFromUpstreams(undefined, true).mode).toBe('unknown');
    expect(installationFromUpstreams([]).mode).toBe('empty');
  });
  test('counts disabled providers as configured without counting their attention', () => {
    const installation = installationFromUpstreams(
      [
        upstream('anthropic_oauth'),
        upstream('openai_api_key', false, 'Apply failed'),
      ],
      true,
    );
    expect(installation.mode).toBe('multi');
    expect(installation.providers).toEqual(['anthropic', 'openai']);
    expect(installation.counts.openai).toEqual({
      configured: 1,
      enabled: 0,
      attention: 0,
    });
    expect(installation.stale).toBe(true);
    expect(
      installationFromUpstreams([upstream('gemini_oauth', false)]).mode,
    ).toBe('single');
  });
  test('canonicalizes subsets and intersects preview, global scope and local facets', () => {
    expect(parseProviders(undefined)).toBeUndefined();
    expect(parseProviders('openai,anthropic,openai')).toEqual([
      'anthropic',
      'openai',
    ]);
    expect(
      intersectProviders(undefined, ['openai', 'anthropic'], ['openai']),
    ).toEqual(['openai']);
    expect(intersectProviders(['anthropic'], ['gemini'])).toEqual([]);
    expect(intersectProviders(undefined, undefined)).toBeUndefined();
  });
});

describe('request event provider taxonomy', () => {
  test.each([
    ['messages', ['anthropic']],
    ['count_tokens', ['anthropic', 'openai']],
    ['openai_responses', ['openai']],
    ['gemini_generate_content', ['gemini']],
    ['gemini_count_tokens', ['gemini']],
    ['chat_completions', ['openai', 'gemini']],
    ['completions', ['anthropic', 'openai']],
    ['embeddings', ['openai', 'gemini']],
    ['moderations', ['openai']],
    ['audio', ['openai']],
    ['images', ['openai']],
    ['codex_auxiliary', ['openai']],
    ['gemini_predict', ['gemini']],
    ['models', []],
    ['files', []],
    ['other', []],
    ['renewal', []],
    ['unclassified', []],
  ] as const)('%s maps to %s', (kind, expected) => {
    expect(providersForRequestEventKind(kind)).toEqual(expected);
  });

  test('covers every RequestEventKind exactly once in the table', () => {
    const covered = new Set<RequestEventKind>([
      'messages',
      'count_tokens',
      'openai_responses',
      'gemini_generate_content',
      'gemini_count_tokens',
      'chat_completions',
      'completions',
      'embeddings',
      'moderations',
      'audio',
      'images',
      'codex_auxiliary',
      'gemini_predict',
      'models',
      'files',
      'other',
      'renewal',
      'unclassified',
    ]);
    expect(new Set(RequestEventKindSchema.options)).toEqual(covered);
  });

  test('returns provider-specific endpoint kinds for overview feeds', () => {
    expect(requestEventKindsForProvider('gemini')).toContain('gemini_predict');
    expect(requestEventKindsForProvider('openai')).toEqual(
      expect.arrayContaining([
        'chat_completions',
        'completions',
        'embeddings',
        'images',
        'audio',
        'codex_auxiliary',
      ]),
    );
  });

  test('recognizes every OAuth credential kind', () => {
    expect(
      (
        ['anthropic_oauth', 'openai_oauth', 'gemini_oauth'] as UpstreamKind[]
      ).map(isOAuthUpstreamKind),
    ).toEqual([true, true, true]);
    expect(
      (
        [
          'anthropic_api_key',
          'openai_api_key',
          'gemini_api_key',
        ] as UpstreamKind[]
      ).map(isOAuthUpstreamKind),
    ).toEqual([false, false, false]);
  });
});

describe('provider capability and display truth', () => {
  test('exposes warmup for OAuth upstreams but not API keys', () => {
    for (const family of ['anthropic', 'openai', 'gemini'] as const) {
      expect(capabilitiesOf(`${family}_oauth`).warmup).toBe(true);
      expect(capabilitiesOf(`${family}_api_key`).warmup).toBe(false);
    }
  });
  test('reports quota capabilities for imported provider subscriptions only', () => {
    expect(capabilitiesOf('anthropic_oauth').subscriptionQuota).toBe(true);
    expect(capabilitiesOf('anthropic_api_key').subscriptionQuota).toBe(false);
    expect(capabilitiesOf('anthropic_api_key').apiKeyUsageReport).toBe(true);
    expect(capabilitiesOf('openai_oauth').subscriptionQuota).toBe(true);
    expect(capabilitiesOf('gemini_oauth').subscriptionQuota).toBe(true);
    expect(capabilitiesOf('openai_api_key').subscriptionQuota).toBe(false);
    expect(capabilitiesOf('gemini_api_key').subscriptionQuota).toBe(false);
  });
  test('shows actual provider and credential-specific proxy default endpoints', () => {
    expect(defaultEndpointFor('openai_oauth')).toBe(
      'https://chatgpt.com/backend-api/codex',
    );
    expect(defaultEndpointFor('gemini_oauth')).toBe(
      'https://cloudcode-pa.googleapis.com/v1internal',
    );
    expect(
      isDefaultProviderEndpoint('openai_api_key', 'https://api.openai.com/'),
    ).toBe(true);
    expect(
      isDefaultProviderEndpoint('gemini_api_key', 'https://api.anthropic.com'),
    ).toBe(false);
  });
  test('shortens Claude IDs without inventory, but preserves known compatible-provider aliases', () => {
    expect(requestModelLabel('claude-sonnet-4-5', 'anthropic')).toBe(
      'sonnet-4-5',
    );
    expect(requestModelLabel('gpt-5', 'openai')).toBe('gpt-5');
    expect(requestModelLabel('gemini-2.5-pro', 'gemini')).toBe(
      'gemini-2.5-pro',
    );
    expect(requestModelLabel('claude-custom', 'openai')).toBe('claude-custom');
    expect(requestModelLabel('claude-sonnet-4-5')).toBe('sonnet-4-5');
  });
});
