import type {
  FireNowErrorReason,
  FireNowResponse,
  PluginEntry,
  Upstream,
} from '../queries';

export const FIXED_NOW = new Date('2026-06-14T11:25:00.000Z');

type UpstreamFixtureOverrides = Partial<Upstream> & {
  revision?: number;
  last_warmup_at_unix_secs?: number | null;
  status?: Partial<Upstream['status']>;
};

function mergeUpstreamOverrides(
  base: Upstream,
  overrides: UpstreamFixtureOverrides = {},
): Upstream {
  const { revision, last_warmup_at_unix_secs, status, ...rest } = overrides;
  const mergedStatus: Upstream['status'] = {
    ...base.status,
    ...status,
  };
  if ('last_warmup_at_unix_secs' in overrides) {
    mergedStatus.last_warmup_at_unix_secs = last_warmup_at_unix_secs ?? null;
  }
  return {
    ...base,
    ...rest,
    spec_revision: rest.spec_revision ?? revision ?? base.spec_revision,
    status: mergedStatus,
  };
}

export function makeOauthUpstream(
  overrides?: UpstreamFixtureOverrides,
): Upstream {
  return mergeUpstreamOverrides(
    {
      id: 'oauth-1',
      name: 'my-prod-oauth',
      kind: 'anthropic_oauth',
      enabled: true,
      warmup_enabled: true,
      warmup_dialect_plugin: null,
      spec_revision: 1,
      status: {
        last_apply_error: null,
        last_apply_at_unix_secs: null,
        last_warmup_at_unix_secs: 1718380800,
      },
    },
    overrides,
  );
}

export function makeApiKeyUpstream(
  overrides?: UpstreamFixtureOverrides,
): Upstream {
  return mergeUpstreamOverrides(
    {
      id: 'api-key-1',
      name: 'my-api-key',
      kind: 'anthropic_api_key',
      enabled: true,
      warmup_enabled: false,
      warmup_dialect_plugin: null,
      spec_revision: 1,
      status: {
        last_apply_error: null,
        last_apply_at_unix_secs: null,
        last_warmup_at_unix_secs: null,
      },
    },
    overrides,
  );
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
    description: 'Mock description',
    usage: 'Mock usage',
    hook_metadata: {},
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
