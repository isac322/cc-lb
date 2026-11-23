import { describe, expect, it } from 'vitest';
import {
  API_KEY_ACCESS_NOT_CHECKED,
  ApiKeyAccessSchema,
  apiKeyAccessOf,
  keyUsageProvenance,
  type ProviderReportState,
  type UpstreamKeyUsageResponse,
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

  it('supportsAllApiKeyKinds', () => {
    for (const [kind, provider] of [
      ['anthropic_api_key', 'anthropic'],
      ['openai_api_key', 'openai'],
      ['gemini_api_key', 'gemini'],
    ] as const) {
      const snapshot = {
        ...checkedOrgAccess,
        provider,
        project_id: 'project-production',
        project_source: 'configured',
        provider_report_state:
          provider === 'gemini' ? 'not_applicable' : 'not_configured',
        provider_report_status_metadata:
          '{"reason":"admin_credential_missing"}',
      };
      const upstream = UpstreamSchema.parse({
        ...base,
        kind,
        api_key_access: snapshot,
      });
      expect(apiKeyAccessOf(upstream)).toEqual(snapshot);
      for (const extra of [{}, { api_key_access: null }]) {
        expect(
          apiKeyAccessOf(UpstreamSchema.parse({ ...base, kind, ...extra })),
        ).toEqual(API_KEY_ACCESS_NOT_CHECKED);
      }
    }
  });

  it('accepts nullable additive provider fields on legacy snapshots', () => {
    const snapshot = {
      ...checkedOrgAccess,
      provider: null,
      project_id: null,
      project_source: null,
      provider_report_state: null,
      provider_report_status_metadata: null,
    };
    expect(ApiKeyAccessSchema.parse(snapshot)).toEqual(snapshot);
  });

  it.each([
    'reported',
    'estimated',
    'not_configured',
    'unavailable',
    'denied',
    'not_applicable',
    'stale',
    'failed',
  ] as const)(
    'preserves provider reporting state %s in snapshots and provenance',
    (state: ProviderReportState) => {
      const access = ApiKeyAccessSchema.parse({
        ...checkedOrgAccess,
        provider: 'openai',
        project_id: 'proj-production',
        project_source: 'admin_resolved',
        provider_report_state: state,
      });
      const usage: UpstreamKeyUsageResponse = {
        range: '24h',
        step: '1h',
        group_by: 'model',
        window_start_unix_secs: 1_793_491_200,
        window_end_unix_secs: 1_796_083_200,
        series: [],
        observed: true,
        source: state,
        attribution: 'api_key',
        attribution_workspace_id: null,
        stale: state === 'stale',
        last_reported_unix_secs: null,
        totals: { cost_usd_micros: 0, cost_is_estimate: true },
        fix: null,
        month: {
          start_unix_secs: 1_793_491_200,
          end_unix_secs: 1_796_083_200,
          tokens: 0,
          cost_usd_micros: 0,
          billing_stop: null,
        },
      };
      expect(keyUsageProvenance(usage, access)).toMatchObject({
        source: state,
        provider: 'openai',
        project_id: 'proj-production',
        provider_report_state: state,
        pending_first_report: false,
      });
    },
  );

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
