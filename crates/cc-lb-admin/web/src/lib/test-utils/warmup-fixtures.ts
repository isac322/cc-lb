import type {
  FireNowErrorReason,
  FireNowResponse,
  PluginEntry,
  Upstream,
} from '../queries';

export const FIXED_NOW = new Date('2026-06-14T11:25:00.000Z');

export function makeOauthUpstream(overrides?: Partial<Upstream>): Upstream {
  return {
    id: 'oauth-1',
    name: 'my-prod-oauth',
    kind: 'anthropic_oauth',
    enabled: true,
    warmup_enabled: true,
    warmup_dialect_plugin: null,
    next_warmup_at: '2026-06-14T23:04:12Z',
    last_warmup_cycle_key: 1718380800, // 2024-06-14T17:50:00Z
    revision: 1,
    ...overrides,
  };
}

export function makeApiKeyUpstream(overrides?: Partial<Upstream>): Upstream {
  return {
    id: 'api-key-1',
    name: 'my-api-key',
    kind: 'anthropic_api_key',
    enabled: true,
    warmup_enabled: false,
    warmup_dialect_plugin: null,
    next_warmup_at: null,
    last_warmup_cycle_key: null,
    revision: 1,
    ...overrides,
  };
}

export function makePluginRegistryEntry(
  overrides?: Partial<PluginEntry>,
): PluginEntry {
  return {
    id: 'plugin-1',
    sha256_hex: 'mock-sha256',
    name: 'anthropic-shape-v2',
    original_filename: 'anthropic-shape-v2.wasm',
    label: 'Anthropic Shape V2',
    size_bytes: 1024,
    refcount: 0,
    revision: 1,
    uploaded_at_unix_secs: 1718380800,
    metadata: null,
    supported_slots: ['shape'],
    ...overrides,
  };
}

export function makeShapePlugin(id: string, name: string): PluginEntry {
  return makePluginRegistryEntry({ id, name, supported_slots: ['shape'] });
}

export function makeRouterPlugin(id: string, name: string): PluginEntry {
  return makePluginRegistryEntry({ id, name, supported_slots: ['router'] });
}

export function makeFireNowSuccess(): FireNowResponse {
  return { fired: true, cycle_key: 1718380800 };
}

export function makeFireNowLeaseHeld(heldBy = 'replica-2'): FireNowResponse {
  return { fired: false, reason: 'lease_held', held_by: heldBy };
}

export function makeFireNowError(reason: FireNowErrorReason): FireNowResponse {
  return { fired: false, reason };
}
