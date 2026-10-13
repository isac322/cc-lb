import { describe, expect, it } from 'vitest';
import {
  API_KEY_ACCESS_NOT_CHECKED,
  apiKeyAccessOf,
  UpstreamSchema,
} from './api';

// The upstream body the Rust admin API serves (`UpstreamResponse` +
// `api_key_access`, plan §1.5). Only the fields the schema reads.
const base = {
  id: 'upstream-1',
  name: 'production',
  enabled: true,
  base_url: null,
  warmup_enabled: false,
  warmup_dialect_plugin: null,
  spec_revision: 3,
  status: {
    last_apply_error: null,
    last_apply_at_unix_secs: null,
    last_warmup_at_unix_secs: null,
  },
};

const checkedOrgAccess = {
  verified: true,
  checked_at_unix_secs: 1_792_065_600,
  key_class: 'org_scoped',
  inference: 'ok',
  workspace: { mode: 'selected', id: 'wrkspc_P', name: 'Production' },
  workspace_list: 'ok',
  usage_report: 'ok',
  key_type: 'service_account',
  last_error: null,
};

describe('apiKeyAccessOf', () => {
  // ACC-01: present only for API-key upstreams.
  it('returns the stored snapshot for a checked API-key upstream', () => {
    const upstream = UpstreamSchema.parse({
      ...base,
      kind: 'anthropic_api_key',
      key_hint: 'sk-ant-api03-...AAAA',
      api_key_access: checkedOrgAccess,
    });
    expect(apiKeyAccessOf(upstream)).toEqual(checkedOrgAccess);
  });

  it('returns null for OAuth upstreams, with or without the field', () => {
    for (const extra of [{}, { api_key_access: null }]) {
      const upstream = UpstreamSchema.parse({
        ...base,
        kind: 'anthropic_oauth',
        ...extra,
      });
      expect(apiKeyAccessOf(upstream)).toBeNull();
    }
  });

  // ACC-02: a row saved before key checks existed has no snapshot.
  it('reads a legacy API-key upstream as never checked', () => {
    for (const extra of [{}, { api_key_access: null }]) {
      const upstream = UpstreamSchema.parse({
        ...base,
        kind: 'anthropic_api_key',
        ...extra,
      });
      expect(apiKeyAccessOf(upstream)).toEqual(API_KEY_ACCESS_NOT_CHECKED);
    }
    expect(API_KEY_ACCESS_NOT_CHECKED).toMatchObject({
      verified: false,
      key_class: 'unknown',
      inference: 'unverified',
      workspace_list: 'not_applicable',
      usage_report: 'not_applicable',
      key_type: null,
      last_error: null,
    });
  });

  it('rejects a snapshot outside the contract', () => {
    expect(
      UpstreamSchema.safeParse({
        ...base,
        kind: 'anthropic_api_key',
        api_key_access: { ...checkedOrgAccess, usage_report: 'reported' },
      }).success,
    ).toBe(false);
  });
});
