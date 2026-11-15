// Built-in entry only from plugin-registry-final.json captured on 2026-11-14.
// The synthetic ID replaces the captured registry ID; no account data is retained.
export const subscriptionPreference = {
  id: 'builtin-subscription-preference-fixture',
  sha256_hex: '737562736372697074696f6e2d707265666572656e6365000000000000000000',
  name: 'subscription-preference',
  original_filename: 'builtin://subscription-preference',
  version: null,
  label: 'Built-in subscription preference filter',
  size_bytes: 0,
  refcount: 2,
  revision: 0,
  uploaded_at_unix_secs: 0,
  kind: 'filter',
  description: 'Prefer subscription/OAuth upstreams while quota appears alive.',
  usage: 'Attach to router chains before API-key fallback filters.',
  hook_metadata: {
    filter: {
      wire_version: 1,
      description: 'Built-in filter hook',
      usage: 'Called by the router filter pipeline.',
      mode: 'active',
    },
  },
  is_builtin: true,
  metadata: {
    purpose: 'Prefer subscription/OAuth upstreams while quota appears alive; use API-key upstreams only when subscription candidates are exhausted.',
    keeps: 'OAuth candidates whose subscription quota is not clearly exhausted, or API-key candidates when every OAuth candidate is exhausted.',
    drops: 'API-key candidates while at least one OAuth candidate appears alive; exhausted OAuth candidates when API-key fallback is available.',
    empty_behavior: 'Never drops everything. If no API-key fallback exists, exhausted OAuth candidates pass through so the upstream/provider returns the authoritative result.',
    examples: [
      'OAuth and API-key candidates, OAuth quota alive → keep OAuth candidates only.',
      'OAuth and API-key candidates, every OAuth quota exhausted → keep API-key candidates only.',
      'Only API-key candidates → keep API-key candidates.',
    ],
  },
  supported_slots: ['router'],
  supported_providers: ['anthropic', 'gemini', 'openai'],
  provider_hooks: {
    anthropic: ['filter'], gemini: ['filter'], openai: ['filter'],
  },
  provider_hook_credential_kinds: {
    anthropic: { filter: ['anthropic_api_key', 'anthropic_oauth'] },
    gemini: { filter: ['gemini_api_key', 'gemini_oauth'] },
    openai: { filter: ['openai_api_key', 'openai_oauth'] },
  },
};

export const upstreamPools = {
  'Anthropic-only': ['anthropic_oauth'],
  'OpenAI-only': ['openai_oauth'],
  'Gemini-only': ['gemini_oauth'],
  mixed: ['anthropic_oauth', 'openai_oauth', 'gemini_oauth', 'openai_api_key'],
  'API-key-only': ['anthropic_api_key', 'openai_api_key', 'gemini_api_key'],
} as const;

export function poolUpstreams(kinds: readonly string[]) {
  return kinds.map((kind, index) => ({
    id: `fixture-upstream-${index}`, name: `Fixture ${kind}`, kind,
    enabled: true, base_url: null, spec_revision: 1,
    warmup_enabled: false, warmup_dialect_plugin: null,
    status: { last_apply_error: null, last_apply_at_unix_secs: null, last_warmup_at_unix_secs: null },
  }));
}
