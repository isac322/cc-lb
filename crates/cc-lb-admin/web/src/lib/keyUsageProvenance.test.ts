import { describe, expect, test } from 'vitest';
import {
  API_KEY_ACCESS_NOT_CHECKED,
  type ApiKeyAccess,
  keyUsageProvenance,
  type UpstreamKeyUsageResponse,
  type UsageBucket,
} from './api';

const ACCESS: ApiKeyAccess = {
  verified: true,
  checked_at_unix_secs: 1_700_000_000,
  key_class: 'org_scoped',
  inference: 'ok',
  workspace: { mode: 'selected', id: 'wrkspc_01Prod', name: 'Production' },
  workspace_list: 'ok',
  usage_report: 'ok',
  key_type: 'service_account',
  last_error: null,
};

const WINDOW_START = 1_760_000_000;
const WINDOW_END = WINDOW_START + 86_400;

function usage(
  overrides: Partial<UpstreamKeyUsageResponse> = {},
): UpstreamKeyUsageResponse {
  return {
    range: '24h',
    step: 'hour',
    group_by: 'model',
    window_start_unix_secs: WINDOW_START,
    window_end_unix_secs: WINDOW_END,
    series: [],
    observed: true,
    source: 'real',
    attribution: 'api_key',
    attribution_workspace_id: null,
    stale: false,
    last_reported_unix_secs: null,
    totals: { cost_usd_micros: 0, cost_is_estimate: true },
    fix: null,
    month: {
      start_unix_secs: WINDOW_START,
      end_unix_secs: WINDOW_END,
      tokens: 0,
      cost_usd_micros: 0,
      billing_stop: null,
    },
    ...overrides,
  };
}
function bucket(start: number): UsageBucket {
  return {
    bucket_start_unix_secs: start,
    request_count: 0,
    input_tokens: 1,
    output_tokens: 1,
    cache_creation_input_tokens: 0,
    cache_read_input_tokens: 0,
    error_count: 0,
    virtual_cost_micros: 0,
    latency_ms_sum: 0,
    latency_count: 0,
  };
}

describe('keyUsageProvenance', () => {
  // UI-U-14 / R2-2: only `usage_poll_failed` stops reports.
  test('a stale snapshot without a fix code is stale, not stopped', () => {
    const p = keyUsageProvenance(usage({ stale: true }), ACCESS);
    expect(p.source).toBe('reported');
    expect(p.stale).toBe(true);
    expect(p.stopped).toBe(false);
    expect(p.fix).toBeNull();
  });

  // UI-U-11: Anthropic's 401/403 is the Stopped state.
  test('usage_poll_failed is stopped', () => {
    const p = keyUsageProvenance(
      usage({ stale: true, fix: { code: 'usage_poll_failed' } }),
      ACCESS,
    );
    expect(p.stale).toBe(true);
    expect(p.stopped).toBe(true);
  });

  test('the last report is the stored poll success, whatever the window', () => {
    const polled = WINDOW_START - 3 * 86_400;
    for (const range of ['1h', '24h', '7d']) {
      const p = keyUsageProvenance(
        usage({
          range,
          stale: true,
          last_reported_unix_secs: polled,
          series: [{ key: 'm', buckets: [bucket(WINDOW_END - 60)] }],
        }),
        ACCESS,
      );
      expect(p.last_reported_unix_secs).toBe(polled);
    }
  });

  test('no stored success: the last report time is unknown, not the window start', () => {
    const p = keyUsageProvenance(usage({ stale: true }), ACCESS);
    expect(p.last_reported_unix_secs).toBeNull();
  });

  test('estimates carry no last-report time', () => {
    const p = keyUsageProvenance(
      usage({ source: 'estimated', fix: { code: 'role_insufficient' } }),
      { ...ACCESS, usage_report: 'denied' },
    );
    expect(p.source).toBe('estimated');
    expect(p.last_reported_unix_secs).toBeNull();
    expect(p.pending_first_report).toBe(false);
  });

  // UI-U-15 / R2-4: Access says Reported, the first poll has not landed.
  test('reported access with an estimated, fix-less answer is pending', () => {
    const p = keyUsageProvenance(usage({ source: 'estimated' }), ACCESS);
    expect(p.pending_first_report).toBe(true);
    // Any fix code means the estimate has a reason other than "not yet".
    expect(
      keyUsageProvenance(
        usage({
          source: 'estimated',
          fix: { code: 'non_anthropic_base_url' },
        }),
        ACCESS,
      ).pending_first_report,
    ).toBe(false);
    expect(
      keyUsageProvenance(
        usage({ source: 'estimated' }),
        API_KEY_ACCESS_NOT_CHECKED,
      ).pending_first_report,
    ).toBe(false);
  });

  test('workspace attribution names the workspace', () => {
    const p = keyUsageProvenance(
      usage({
        attribution: 'workspace',
        attribution_workspace_id: 'wrkspc_01Prod',
      }),
      ACCESS,
    );
    expect(p.attribution).toBe('workspace');
    expect(p.workspace).toBe('Production');
  });

  test("a workspace edit never relabels the old response's figures", () => {
    // Access already names the new workspace; the cached usage is still A's.
    const p = keyUsageProvenance(
      usage({ attribution: 'workspace', attribution_workspace_id: 'wrkspc_A' }),
      {
        ...ACCESS,
        workspace: { mode: 'selected', id: 'wrkspc_B', name: 'Workspace B' },
      },
    );
    expect(p.workspace).toBe('wrkspc_A');
  });
});
