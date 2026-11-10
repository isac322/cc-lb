import { cleanup, render, screen, within } from '@testing-library/react';
import type { ReactNode } from 'react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  getJson,
  type LatestResponse,
  type QuotaCredits,
  type QuotaSnapshot,
} from '../../lib/api';
import type { Upstream } from '../../lib/queries';
import { selectCodexCredits } from './codexCredits';
import { QuotaWindowRow, QuotaWindowRows } from './QuotaWindowRows';
import {
  selectQuotaCardSnapshots,
  selectVisibleGraphWindows,
} from './quotaWindowVisibility';
import {
  buildUpstreamUsageRows,
  UPSTREAM_LATEST_WINDOWS_BY_PROVIDER,
  UpstreamUsageTable,
} from './UpstreamUsageTable';

vi.mock('@tanstack/react-router', () => ({
  Link: ({
    to,
    search: _search,
    children,
    ...rest
  }: {
    to: string;
    search?: unknown;
    children?: ReactNode;
  }) => (
    <a href={to} {...rest}>
      {children}
    </a>
  ),
}));

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

const NOW = 1_794_355_200;
const upstream = {
  id: 'codex',
  name: 'Codex subscription',
  kind: 'openai_oauth',
  enabled: true,
  spec_revision: 1,
  base_url: null,
  warmup_enabled: false,
  warmup_dialect_plugin: null,
  status: {
    last_apply_error: null,
    last_apply_at_unix_secs: null,
    last_warmup_at_unix_secs: null,
  },
} as Upstream;

function snapshot(overrides: Partial<QuotaSnapshot> = {}): QuotaSnapshot {
  return {
    window: 'overage',
    state: 'fresh',
    source: 'api',
    utilization: null,
    status: null,
    resets_at_unix_secs: null,
    surpassed_threshold: null,
    representative_claim: null,
    disabled_reason: null,
    extra_usage_enabled: null,
    extra_usage_monthly_limit: null,
    extra_usage_used_credits: null,
    observed_at_unix_millis: NOW * 1000,
    age_secs: 0,
    ...overrides,
  };
}

function credit(overrides: Partial<QuotaCredits> = {}): QuotaCredits {
  return {
    has_credits: null,
    unlimited: null,
    limit_reached: null,
    balance: null,
    approx_local_messages: null,
    approx_cloud_messages: null,
    ...overrides,
  };
}

function renderSurfaces(windows: QuotaSnapshot[]) {
  const base = [
    snapshot({
      window: '5h',
      utilization: 0.25,
      resets_at_unix_secs: NOW + 3600,
    }),
    snapshot({
      window: '7d',
      utilization: 0.5,
      resets_at_unix_secs: NOW + 86400,
    }),
    ...windows,
  ];
  const latest: LatestResponse = {
    now_unix_secs: NOW,
    max_staleness_secs: 300,
    upstreams: [
      { upstream_id: upstream.id, upstream_name: upstream.name, windows: base },
    ],
  };
  render(
    <>
      <section aria-label="Overview test surface">
        <UpstreamUsageTable
          data={{
            rows: buildUpstreamUsageRows({
              upstreams: [upstream],
              latest,
              status: [],
              nudges: new Map(),
              usageByUpstreamId: new Map(),
            }),
            isLoading: false,
            quotaPending: false,
            statusPending: false,
            usagePending: false,
            quotaError: false,
            reconnectCount: 0,
          }}
        />
      </section>
      <section aria-label="Detail test surface">
        <QuotaWindowRows>
          {selectQuotaCardSnapshots({
            latestWindows: base,
            provider: 'openai',
          }).map((snap) => (
            <QuotaWindowRow
              key={snap.window}
              snap={snap}
              nowUnixSecs={NOW}
              creditObservations={base}
            />
          ))}
        </QuotaWindowRows>
      </section>
    </>,
  );
  return ['Overview test surface', 'Detail test surface'].map((name) =>
    within(screen.getByRole('region', { name })),
  );
}

describe('Codex credit observations', () => {
  it.each([
    {
      name: 'available',
      credits: credit({ has_credits: true, balance: '62500.0000000000' }),
      status: 'Available',
    },
    {
      name: 'unavailable',
      credits: credit({ has_credits: false, balance: '0.0000000000' }),
      status: 'Unavailable',
    },
    {
      name: 'unlimited independent of has_credits',
      credits: credit({ has_credits: false, unlimited: true }),
      status: 'Unlimited',
    },
    {
      name: 'blocked even when unlimited',
      credits: credit({ unlimited: true, limit_reached: true }),
      status: 'Blocked',
    },
    {
      name: 'unknown despite balance',
      credits: credit({ balance: '123.4500000000' }),
      status: 'Unknown',
    },
    {
      name: 'stale availability',
      credits: credit({ has_credits: true, balance: '123.4500000000' }),
      status: 'Stale',
      state: 'stale' as const,
    },
  ])(
    'shows the same $name facts on overview and detail',
    ({ credits, status, state }) => {
      const surfaces = renderSurfaces([
        snapshot({ credits, state: state ?? 'fresh' }),
      ]);
      for (const surface of surfaces) {
        const facts = surface.getByTestId('codex-credit-facts');
        expect(surface.getAllByTestId('codex-credit-facts')).toHaveLength(1);
        expect(within(facts).getByText(status, { exact: true })).not.toBeNull();
        expect(facts.textContent).toContain(
          credits.balance == null
            ? 'No balance reported'
            : `${credits.balance} credits`,
        );
        expect(facts.textContent).not.toContain('$');
        expect(facts.textContent).not.toContain('%');
        expect(within(facts).queryByRole('meter')).toBeNull();
        expect(surface.getAllByRole('meter')).toHaveLength(2);
      }
    },
  );

  it('keeps an API limit block over a newer available header and renders one fact set', () => {
    const surfaces = renderSurfaces([
      snapshot({
        credits: credit({
          limit_reached: true,
          has_credits: true,
          balance: '10.000',
        }),
      }),
      snapshot({
        window: 'unified',
        source: 'header',
        observed_at_unix_millis: NOW * 1000 + 1000,
        credits: credit({ has_credits: true, balance: '11.0000000000' }),
      }),
    ]);
    for (const surface of surfaces) {
      const facts = surface.getByTestId('codex-credit-facts');
      expect(facts.textContent).toContain('Blocked');
      expect(facts.textContent).not.toContain('Available');
      expect(facts.textContent).toContain('Status: Usage API');
      expect(facts.textContent).toContain('11.0000000000 credits');
      expect(facts.textContent).toContain('Balance: Response headers');
    }
  });

  it('retains a stale unavailable header restriction after the API clears', () => {
    const surfaces = renderSurfaces([
      snapshot({
        credits: credit({ has_credits: true, balance: '99' }),
        fallback_available: true,
      }),
      snapshot({
        window: 'unified',
        source: 'header',
        state: 'stale',
        observed_at_unix_millis: (NOW - 600) * 1000,
        fallback_available: false,
      }),
    ]);
    for (const surface of surfaces) {
      const facts = surface.getByTestId('codex-credit-facts');
      expect(facts.textContent).toContain('Unavailable');
      expect(facts.textContent).toContain('Status: Response headers · stale');
      expect(facts.textContent).not.toContain('Available');
    }
  });

  it('does not refresh stale availability with a fresh balance-only observation', () => {
    const facts = selectCodexCredits([
      snapshot({
        state: 'stale',
        observed_at_unix_millis: (NOW - 600) * 1000,
        credits: credit({ has_credits: true, balance: '1.0' }),
      }),
      snapshot({
        window: 'unified',
        source: 'header',
        credits: credit({ balance: '2.000' }),
      }),
    ]);
    expect(facts).toMatchObject({
      status: 'Stale',
      statusSource: 'Usage API · stale',
      balance: '2.000',
      balanceSource: 'Response headers',
    });
  });

  it('labels an older balance honestly when a fresh availability observation has none', () => {
    const surfaces = renderSurfaces([
      snapshot({
        state: 'stale',
        observed_at_unix_millis: (NOW - 600) * 1000,
        credits: credit({ balance: '1.0000000000' }),
      }),
      snapshot({
        window: 'unified',
        source: 'header',
        credits: credit({ has_credits: true }),
      }),
    ]);
    for (const surface of surfaces) {
      const facts = surface.getByTestId('codex-credit-facts');
      expect(facts.textContent).toContain('Available');
      expect(facts.textContent).toContain('Balance: Usage API · stale');
      expect(facts.textContent).toContain('1.0000000000 credits');
    }
  });

  it('never plots either credit observation, even with utilization and a budget', () => {
    const windows = ['overage', 'unified'].map((window) =>
      snapshot({
        window: window as QuotaSnapshot['window'],
        credits: credit({ has_credits: true }),
        utilization: 0.5,
        extra_usage_enabled: true,
        extra_usage_monthly_limit: 100,
      }),
    );
    expect(
      selectVisibleGraphWindows({
        latestWindows: windows,
        sinceUnixSecs: NOW - 60,
        series: windows.map((snap) => ({
          upstream_id: 'codex',
          upstream_name: 'Codex',
          window: snap.window,
          buckets: [{ bucket_start_unix_secs: NOW, utilization_last: 0.5 }],
          markers: [],
        })),
      }),
    ).toEqual([]);
  });

  it('requests both observations and preserves raw decimal balance through the API boundary', async () => {
    const windows = UPSTREAM_LATEST_WINDOWS_BY_PROVIDER.openai.split(',');
    expect(windows).toEqual(['5h', '7d', 'overage', 'unified']);
    const body: LatestResponse = {
      now_unix_secs: NOW,
      max_staleness_secs: 300,
      upstreams: [
        {
          upstream_id: 'codex',
          upstream_name: 'Codex',
          windows: [
            snapshot({
              credits: credit({
                balance: '62500.0000000000',
                limit_reached: true,
              }),
            }),
            snapshot({
              window: 'unified',
              source: 'header',
              fallback_available: false,
            }),
          ],
        },
      ],
    };
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue(
        new Response(JSON.stringify(body), {
          status: 200,
          headers: { 'content-type': 'application/json' },
        }),
      ),
    );
    const response = await getJson<LatestResponse>(
      `/admin/v1/subscription-quotas/latest?windows=${windows.join(',')}`,
    );
    expect(response).toEqual(body);
    expect(selectCodexCredits(response.upstreams[0]!.windows)).toMatchObject({
      status: 'Blocked',
      balance: '62500.0000000000',
      balanceSource: 'Usage API',
    });
  });
});
