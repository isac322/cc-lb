import { useMemo } from 'react';
import {
  apiKeyAccessOf,
  type RequestEventKind,
  type Upstream,
  type UpstreamKind,
} from './api';
import { useUpstreams } from './queries';

export const PROVIDER_FAMILIES = ['anthropic', 'openai', 'gemini'] as const;
export type ProviderFamily = (typeof PROVIDER_FAMILIES)[number];
export const PROVIDER_LABELS: Record<ProviderFamily, string> = {
  anthropic: 'Anthropic',
  openai: 'OpenAI',
  gemini: 'Gemini',
};

/**
 * Request endpoint kinds that can be attributed to a provider without an
 * upstream inventory lookup. Shared endpoint families stay in every provider
 * that can expose them; ambiguous or provider-neutral kinds are intentionally
 * absent and fall back to the upstream identity.
 */
export const REQUEST_EVENT_KINDS_BY_PROVIDER: Record<
  ProviderFamily,
  readonly RequestEventKind[]
> = {
  anthropic: ['messages', 'count_tokens', 'completions'],
  openai: [
    'count_tokens',
    'openai_responses',
    'chat_completions',
    'completions',
    'embeddings',
    'moderations',
    'audio',
    'images',
    'codex_auxiliary',
  ],
  gemini: [
    'gemini_generate_content',
    'gemini_count_tokens',
    'chat_completions',
    'embeddings',
    'gemini_predict',
  ],
};

export function requestEventKindsForProvider(
  provider: ProviderFamily,
): readonly RequestEventKind[] {
  return REQUEST_EVENT_KINDS_BY_PROVIDER[provider];
}

export function providersForRequestEventKind(
  kind: RequestEventKind | null | undefined,
): readonly ProviderFamily[] {
  if (kind == null) return [];
  return PROVIDER_FAMILIES.filter((provider) =>
    REQUEST_EVENT_KINDS_BY_PROVIDER[provider].includes(kind),
  );
}

export function providerOf(kind: UpstreamKind): ProviderFamily {
  if (kind === 'openai_oauth' || kind === 'openai_api_key') return 'openai';
  if (kind === 'gemini_oauth' || kind === 'gemini_api_key') return 'gemini';
  return 'anthropic';
}

export function isOAuthUpstreamKind(kind: UpstreamKind): boolean {
  return kind.endsWith('_oauth');
}
export function upstreamTypeLabel(
  kind: UpstreamKind,
): 'Subscription' | 'API key' {
  return kind.endsWith('_oauth') ? 'Subscription' : 'API key';
}

export function upstreamKindLabel(kind: UpstreamKind): string {
  const type = kind.endsWith('_oauth') ? 'subscription' : 'API key';
  return `${PROVIDER_LABELS[providerOf(kind)]} ${type}`;
}

/** Canonical order also makes URL parameters and query-cache identities stable. */
export function parseProviders(
  value: string | undefined,
): ProviderFamily[] | undefined {
  if (value === undefined) return undefined;
  const selected = new Set(value.split(','));
  return PROVIDER_FAMILIES.filter((provider) => selected.has(provider));
}

export interface ProviderCapabilities {
  subscriptionQuota: boolean;
  quotaHistory: boolean;
  limitReset: boolean;
  warmup: boolean;
  cacheKeepalive: boolean;
  apiKeyUsageReport: boolean;
  oauthBrowserConnect: boolean;
  credits: boolean;
}
const NONE: ProviderCapabilities = {
  subscriptionQuota: false,
  quotaHistory: false,
  limitReset: false,
  warmup: false,
  cacheKeepalive: false,
  apiKeyUsageReport: false,
  oauthBrowserConnect: false,
  credits: false,
};
/** UI-exposed backend capabilities. Subscription quota is available for
 * imported OpenAI/Gemini credentials through the shared quota endpoints.
 * API-key reporting remains Anthropic-specific today.
 */
export const PROVIDER_CAPABILITIES: Record<
  UpstreamKind,
  Readonly<ProviderCapabilities>
> = {
  anthropic_oauth: {
    ...NONE,
    subscriptionQuota: true,
    quotaHistory: true,
    limitReset: true,
    warmup: true,
    cacheKeepalive: true,
    oauthBrowserConnect: true,
    credits: true,
  },
  anthropic_api_key: { ...NONE, cacheKeepalive: true, apiKeyUsageReport: true },
  openai_oauth: {
    ...NONE,
    subscriptionQuota: true,
    quotaHistory: true,
    limitReset: true,
    warmup: true,
    cacheKeepalive: true,
  },
  openai_api_key: { ...NONE, cacheKeepalive: true },
  gemini_oauth: {
    ...NONE,
    subscriptionQuota: true,
    quotaHistory: true,
    warmup: true,
  },
  gemini_api_key: NONE,
};
export function capabilitiesOf(
  kind: UpstreamKind,
): Readonly<ProviderCapabilities> {
  return PROVIDER_CAPABILITIES[kind];
}

export const PROVIDER_DEFAULT_ENDPOINTS: Record<ProviderFamily, string> = {
  anthropic: 'https://api.anthropic.com',
  openai: 'https://api.openai.com',
  gemini: 'https://generativelanguage.googleapis.com',
};
export function isDefaultProviderEndpoint(
  kind: UpstreamKind,
  value: string,
): boolean {
  return value.trim().replace(/\/+$/, '') === defaultEndpointFor(kind);
}
/** Proxy defaults differ for imported OAuth credentials (Codex / Code Assist). */
export function defaultEndpointFor(kind: UpstreamKind): string {
  if (kind === 'openai_oauth') return 'https://chatgpt.com/backend-api/codex';
  if (kind === 'gemini_oauth')
    return 'https://cloudcode-pa.googleapis.com/v1internal';
  return PROVIDER_DEFAULT_ENDPOINTS[providerOf(kind)];
}

export type UpstreamAttentionReason =
  | 'apply_error'
  | 'key_unverified'
  | 'credential_check';
/** Shared base attention rule used by installation summaries and upstream rows.
 * Disabled upstreams do not count unless the upstream list has a live nudge,
 * which is unavailable in the inventory-only installation summary.
 */
export function upstreamAttentionReason(
  upstream: Upstream,
): UpstreamAttentionReason | null {
  if (!upstream.enabled) return null;
  if (upstream.status.last_apply_error) return 'apply_error';
  if (apiKeyAccessOf(upstream)?.verified === false) return 'key_unverified';
  if (
    upstream.api_key_access?.inference === 'denied' ||
    upstream.api_key_access?.last_error
  ) {
    return 'credential_check';
  }
  return null;
}
export interface ProviderCounts {
  configured: number;
  enabled: number;
  attention: number;
}
export interface Installation {
  mode: 'unknown' | 'empty' | 'single' | 'multi';
  /** Configured families, including disabled upstreams; never the current lens. */
  providers: ProviderFamily[];
  counts: Record<ProviderFamily, ProviderCounts>;
  /** Cached inventory can remain useful while its last refresh failed. */
  stale: boolean;
}
export function installationFromUpstreams(
  upstreams: readonly Upstream[] | undefined,
  stale = false,
): Installation {
  const counts: Record<ProviderFamily, ProviderCounts> = {
    anthropic: { configured: 0, enabled: 0, attention: 0 },
    openai: { configured: 0, enabled: 0, attention: 0 },
    gemini: { configured: 0, enabled: 0, attention: 0 },
  };
  for (const upstream of upstreams ?? []) {
    const count = counts[providerOf(upstream.kind)];
    count.configured += 1;
    if (upstream.enabled) count.enabled += 1;
    if (upstreamAttentionReason(upstream) !== null) count.attention += 1;
  }
  const providers = PROVIDER_FAMILIES.filter(
    (provider) => counts[provider].configured > 0,
  );
  return {
    mode:
      upstreams === undefined
        ? 'unknown'
        : providers.length === 0
          ? 'empty'
          : providers.length === 1
            ? 'single'
            : 'multi',
    providers,
    counts,
    stale,
  };
}
/** Preview inventory changes installation mode; a design's provider lens does not. */
export function useInstallation(): Installation {
  const inventory = useUpstreams();
  return useMemo(
    () =>
      installationFromUpstreams(inventory.data?.upstreams, inventory.isError),
    [inventory.data, inventory.isError],
  );
}
