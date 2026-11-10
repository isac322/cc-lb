import {
  cleanup,
  fireEvent,
  render,
  screen,
  within,
} from '@testing-library/react';
import type { ReactNode } from 'react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import type {
  LatestResponse,
  QuotaSnapshot,
  SubscriptionQuotaWindow,
} from '../../lib/api';
import type { OAuthReconnectNudge } from '../../lib/oauthReconnect';
import type { Upstream } from '../../lib/queries';
import {
  buildUpstreamUsageRows,
  countReconnectNudges,
  rowMaxUsedPct,
  sortUpstreamRowsByUsage,
  type UpstreamUsageData,
  UpstreamUsageTable,
} from './UpstreamUsageTable';

vi.mock('@tanstack/react-router', () => ({
  Link: ({
    to,
    search,
    children,
    ...rest
  }: {
    to: string;
    search?: Record<string, string>;
    children?: ReactNode;
  }) => (
    <a
      href={search ? `${to}?${new URLSearchParams(search).toString()}` : to}
      {...rest}
    >
      {children}
    </a>
  ),
}));

afterEach(() => {
  cleanup();
});

const NOW = Math.floor(Date.now() / 1000);

function upstream(
  id: string,
  kind: Upstream['kind'],
  enabled = true,
): Upstream {
  return {
    id,
    name: id,
    kind,
    enabled,
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
}

function snap(
  window: SubscriptionQuotaWindow,
  utilization: number | null,
  state: QuotaSnapshot['state'] = 'fresh',
): QuotaSnapshot {
  return {
    window,
    state,
    source: 'api',
    utilization,
    status: null,
    resets_at_unix_secs:
      state === 'absent' || state === 'unobserved'
        ? null
        : NOW + 3 * 3600 + 12 * 60 + 30,
    surpassed_threshold: false,
    representative_claim: null,
    disabled_reason: null,
    extra_usage_enabled: null,
    extra_usage_monthly_limit: null,
    extra_usage_used_credits: null,
    observed_at_unix_millis: NOW * 1000,
    age_secs: 0,
  };
}

/** `[id, kind, enabled, runtime status, 5h, 7d]` */
type Spec = [
  string,
  Upstream['kind'],
  boolean,
  string,
  number | null,
  number | null,
];

function data(
  specs: Spec[],
  nudges: ReadonlyMap<string, OAuthReconnectNudge> = new Map(),
): UpstreamUsageData {
  const upstreams = specs.map(([id, kind, enabled]) =>
    upstream(id, kind, enabled),
  );
  const latest: LatestResponse = {
    now_unix_secs: NOW,
    max_staleness_secs: 300,
    upstreams: specs.map(([id, , , , five, seven]) => ({
      upstream_id: id,
      upstream_name: id,
      windows: [
        ...(five == null ? [] : [snap('5h', five)]),
        ...(seven == null ? [] : [snap('7d', seven)]),
      ],
    })),
  };
  return {
    rows: buildUpstreamUsageRows({
      upstreams,
      latest,
      status: specs.map(([id, , , status]) => ({
        id,
        status,
        last_apply_error: null,
      })),
      nudges,
      usageByUpstreamId: new Map(),
    }),
    isLoading: false,
    quotaPending: false,
    statusPending: false,
    usagePending: false,
    quotaError: false,
    reconnectCount: nudges.size,
  };
}

const rowNames = () =>
  screen
    .getAllByTestId('upstream-usage-row')
    .map((row) => row.querySelector('[title]')?.getAttribute('title'));

describe('UpstreamUsageTable', () => {
  it('counts reconnect nudges only from the provider-scoped rows', () => {
    const usageData = data(
      [
        ['anthropic', 'anthropic_oauth', true, 'active', null, null],
        ['openai', 'openai_oauth', true, 'active', null, null],
      ],
      new Map<string, OAuthReconnectNudge>([
        [
          'openai',
          {
            tone: 'danger',
            reason: 'renewal_rejected',
            label: 'Reconnect required',
            description: 'Reconnect',
            actionLabel: 'Reconnect',
            expiresAt: null,
          },
        ],
      ]),
    );
    expect(
      countReconnectNudges(
        usageData.rows.filter((row) =>
          row.upstream.kind.startsWith('anthropic'),
        ),
      ),
    ).toBe(0);
    expect(
      countReconnectNudges(
        usageData.rows.filter((row) => row.upstream.kind.startsWith('openai')),
      ),
    ).toBe(1);
  });
  it('orders problems first, then quota by most-used window, API keys, disabled last', () => {
    const usageData = data([
      ['key-a', 'anthropic_api_key', true, 'active', null, null],
      ['calm', 'anthropic_oauth', true, 'active', 0.1, 0.2],
      ['off', 'anthropic_oauth', false, 'active', 0.99, 0.99],
      ['busy', 'anthropic_oauth', true, 'active', 0.3, 0.9],
      ['broken', 'anthropic_api_key', true, 'error', null, null],
    ]);
    usageData.rows
      .find((row) => row.upstream.id === 'busy')!
      .windows.push(snap('7d_fable', null, 'unobserved'));
    render(<UpstreamUsageTable data={usageData} />);

    expect(rowNames()).toEqual(['broken', 'busy', 'calm', 'key-a', 'off']);
    const rows = screen.getAllByTestId('upstream-usage-row');
    expect(rows[1]?.querySelector('a')?.getAttribute('href')).toBe(
      '/upstreams?selectedId=busy',
    );
    // Only problems speak: a healthy row has no status line.
    expect(within(rows[0]!).getByTestId('usage-row-status').textContent).toBe(
      'Error',
    );
    expect(within(rows[1]!).queryByTestId('usage-row-status')).toBeNull();

    // API-key upstreams say so instead of drawing empty meters.
    expect(within(rows[3]!).getByTestId('usage-no-quota').textContent).toBe(
      'No subscription quota',
    );
    expect(within(rows[3]!).queryAllByRole('meter')).toHaveLength(0);

    const busy7d = rows[1]!.querySelector('[data-window="7d"]')!;
    expect(busy7d.textContent).toContain('90%used');
    expect(busy7d.textContent).toContain('resets in 3h 12m');
    // Resets in 3h 12m 30s of a 7d window: 98% of it has elapsed.
    expect(
      within(busy7d as HTMLElement)
        .getByRole('meter')
        .getAttribute('aria-valuetext'),
    ).toBe('90% used, even pace 98%');
    expect(
      rows[1]!.querySelector('[data-window="7d_fable"]')?.textContent,
    ).toContain('No reading');
    // A window without a reading draws no pace tick.
    expect(
      rows[1]!.querySelector(
        '[data-window="7d_fable"] [data-slot="pace-marker"]',
      ),
    ).toBeNull();
  });

  it('omits the 5h header and cells for an all-weekly OAuth fleet', () => {
    const usageData = data([
      ['weekly-a', 'anthropic_oauth', true, 'active', null, 0.2],
      ['weekly-b', 'anthropic_oauth', true, 'active', null, 0.6],
      ['key', 'anthropic_api_key', true, 'active', 0.99, null],
    ]);
    for (const row of usageData.rows) {
      if (row.upstream.kind === 'anthropic_oauth') {
        row.windows.push(snap('5h', null, 'absent'));
      }
    }
    const { container } = render(<UpstreamUsageTable data={usageData} />);

    expect(screen.queryByText('5h')).toBeNull();
    expect(container.querySelectorAll('[data-window="5h"]')).toHaveLength(0);
    expect(screen.getAllByRole('meter')).toHaveLength(2);
    expect(screen.queryAllByText('No reading')).toHaveLength(0);
    expect(screen.queryByText('0%')).toBeNull();
    expect(screen.getByTestId('usage-no-quota').textContent).toBe(
      'No subscription quota',
    );
    expect(screen.getByTestId('usage-no-quota').className).toContain(
      '@4xl:col-span-1',
    );
  });

  it('keeps an inert desktop cell for absent 5h in a mixed fleet and hides it when stacked', () => {
    const usageData = data([
      ['weekly', 'anthropic_oauth', true, 'active', null, 0.2],
      ['normal', 'anthropic_oauth', true, 'active', 0.4, 0.3],
      ['key', 'anthropic_api_key', true, 'active', null, null],
    ]);
    usageData.rows[0]!.windows.push(snap('5h', null, 'absent'));
    render(<UpstreamUsageTable data={usageData} />);

    expect(screen.getAllByText('5h')).toHaveLength(2);
    const weekly = screen.getByRole('link', { name: /weekly/ });
    const normal = screen.getByRole('link', { name: /normal/ });
    const absent = within(weekly).getByTestId('usage-absent-cell');
    expect(absent.getAttribute('data-window')).toBe('5h');
    expect(absent.getAttribute('aria-hidden')).toBe('true');
    expect(absent.className).toBe('hidden @4xl:block');
    expect(absent.textContent).toBe('');
    expect(within(absent).queryByRole('meter')).toBeNull();
    expect(within(weekly).queryAllByText('No reading')).toHaveLength(0);
    expect(within(weekly).queryByText('0%')).toBeNull();
    expect(within(weekly).getAllByTestId('usage-cell')).toHaveLength(1);
    expect(within(normal).getAllByTestId('usage-cell')).toHaveLength(2);
    expect(weekly.className).toBe(normal.className);
    expect(weekly.className).toContain(
      '@4xl:grid-cols-[minmax(12rem,1.4fr)_repeat(2,minmax(0,1fr))]',
    );
    expect(screen.getByTestId('usage-no-quota').className).toContain(
      '@4xl:col-span-2',
    );
  });

  it('ignores absent readings when computing max usage and ordering rows', () => {
    const usageData = data([
      ['weekly', 'anthropic_oauth', true, 'active', null, 0.2],
      ['normal', 'anthropic_oauth', true, 'active', 0.4, 0.3],
      ['absent-only', 'anthropic_oauth', true, 'active', null, null],
    ]);
    usageData.rows[0]!.windows.push(snap('5h', 0.99, 'absent'));
    usageData.rows[2]!.windows.push(snap('5h', 1, 'absent'));

    expect(rowMaxUsedPct(usageData.rows[0]!)).toBe(20);
    expect(rowMaxUsedPct(usageData.rows[2]!)).toBeNull();
    expect(
      sortUpstreamRowsByUsage(usageData.rows).map((row) => row.upstream.id),
    ).toEqual(['normal', 'weekly', 'absent-only']);
  });

  it('preserves fresh and unobserved 5h cells for normal subscription plans', () => {
    const usageData = data([
      ['normal', 'anthropic_oauth', true, 'active', 0.4, 0.3],
      ['unknown', 'anthropic_oauth', true, 'active', null, 0.2],
    ]);
    usageData.rows[1]!.windows.push(snap('5h', null, 'unobserved'));
    render(<UpstreamUsageTable data={usageData} />);

    expect(screen.getAllByText('5h')).toHaveLength(3);
    const normal = screen.getByRole('link', { name: /normal/ });
    const unknown = screen.getByRole('link', { name: /unknown/ });
    expect(
      within(normal)
        .getByRole('meter', { name: '5h window used' })
        .getAttribute('aria-valuenow'),
    ).toBe('40');
    expect(
      within(unknown)
        .getByRole('meter', { name: '5h window used' })
        .getAttribute('aria-valuetext'),
    ).toBe('No reading');
    expect(screen.queryByTestId('usage-absent-cell')).toBeNull();
  });

  it('retains all existing columns and skeleton cells while quota is pending', () => {
    const usageData = data([
      ['pending', 'anthropic_oauth', true, 'active', null, null],
    ]);
    const { container } = render(
      <UpstreamUsageTable data={{ ...usageData, quotaPending: true }} />,
    );

    expect(screen.getAllByText('5h')).toHaveLength(2);
    expect(screen.getAllByText('7d')).toHaveLength(2);
    expect(screen.getAllByText('7d (Fable)')).toHaveLength(2);
    expect(screen.getAllByTestId('usage-cell')).toHaveLength(3);
    expect(screen.queryAllByRole('meter')).toHaveLength(0);
    expect(screen.queryAllByText('No reading')).toHaveLength(0);
    expect(container.querySelectorAll('.skeleton')).toHaveLength(9);
  });

  it('removes and restores the 5h column as the reported plan changes', () => {
    const usageData = data([
      ['changing', 'anthropic_oauth', true, 'active', 0.4, 0.3],
    ]);
    const { rerender } = render(<UpstreamUsageTable data={usageData} />);
    expect(screen.getByRole('meter', { name: '5h window used' })).toBeTruthy();

    const weeklyData = {
      ...usageData,
      rows: usageData.rows.map((row) => ({
        ...row,
        windows: [snap('5h', null, 'absent'), snap('7d', 0.3)],
      })),
    };
    rerender(<UpstreamUsageTable data={weeklyData} />);
    expect(screen.queryByText('5h')).toBeNull();
    expect(screen.queryByRole('meter', { name: '5h window used' })).toBeNull();

    rerender(<UpstreamUsageTable data={usageData} />);
    expect(screen.getByRole('meter', { name: '5h window used' })).toBeTruthy();
  });
  it('renders only reported OpenAI subscription windows', () => {
    render(
      <UpstreamUsageTable
        data={data([['pro', 'openai_oauth', true, 'active', null, 0.4]])}
      />,
    );
    const row = screen.getByTestId('upstream-usage-row');
    expect(row.querySelector('[data-window="5h"]')).toBeNull();
    expect(row.querySelector('[data-window="7d"]')?.textContent).toContain(
      '40%used',
    );
    expect(row.textContent).toContain('7d');
    expect(row.textContent).not.toContain('No subscription quota');
  });
  it('omits an OpenAI window with no reported utilization', () => {
    const providerData = data([
      ['pro', 'openai_oauth', true, 'active', 0.0, 0.18],
    ]);
    providerData.rows[0]!.windows[0]!.utilization = null;
    render(<UpstreamUsageTable data={providerData} />);
    const row = screen.getByTestId('upstream-usage-row');
    expect(row.querySelector('[data-window="5h"]')).toBeNull();
    expect(row.querySelector('[data-window="7d"]')?.textContent).toContain(
      '18%used',
    );
  });

  it.each([
    ['openai_oauth', '5h', '7d'],
    ['gemini_oauth', 'gemini_5h', 'gemini_7d'],
  ] as const)(
    'omits absent windows across a weekly-only %s fleet and preserves mixed-row alignment',
    (kind, shortWindow, weeklyWindow) => {
      const weekly = {
        ...data([['weekly', kind, true, 'active', null, null]]).rows[0]!,
        windows: [snap(shortWindow, null, 'absent'), snap(weeklyWindow, 0.2)],
      };
      const usageData = {
        ...data([]),
        rows: [weekly],
      };
      const { rerender } = render(<UpstreamUsageTable data={usageData} />);
      expect(
        screen
          .getByTestId('upstream-usage-row')
          .querySelector(`[data-window="${shortWindow}"]`),
      ).toBeNull();

      rerender(
        <UpstreamUsageTable
          data={{
            ...usageData,
            rows: [
              weekly,
              {
                ...weekly,
                upstream: { ...weekly.upstream, id: 'normal', name: 'normal' },
                windows: [snap(shortWindow, 0.4), snap(weeklyWindow, 0.3)],
              },
            ],
          }}
        />,
      );
      const absent = within(
        screen.getByRole('link', { name: /weekly/ }),
      ).getByTestId('usage-absent-cell');
      expect(absent.getAttribute('data-window')).toBe(shortWindow);
      expect(absent.className).toBe('hidden @4xl:block');
      expect(absent.textContent).toBe('');
    },
  );

  it.each([
    ['stale', 0.42, '42%used'],
    ['unobserved', null, 'No reading'],
  ] as const)(
    'keeps an Anthropic %s 5h slot visible',
    (state, utilization, text) => {
      const providerData = data([
        ['claude', 'anthropic_oauth', true, 'active', 0.42, 0.18],
      ]);
      const row = providerData.rows[0];
      const fiveHour = row?.windows.find((window) => window.window === '5h');
      if (!fiveHour) throw new Error('Missing Anthropic 5h fixture');
      fiveHour.state = state;
      fiveHour.utilization = utilization;

      render(<UpstreamUsageTable data={providerData} />);

      const usageRow = screen.getByTestId('upstream-usage-row');
      const fiveHourCell = usageRow.querySelector('[data-window="5h"]');
      expect(fiveHourCell).not.toBeNull();
      expect(
        fiveHourCell?.querySelector('[data-testid="usage-cell-empty"]'),
      ).toBeNull();
      expect(fiveHourCell?.textContent).toContain(text);
    },
  );

  it('renders Gemini quota windows with their labels', () => {
    const providerData = data([
      ['antigravity', 'gemini_oauth', true, 'active', null, null],
    ]);
    const rowData = providerData.rows.at(0);
    if (!rowData) throw new Error('Missing Gemini upstream fixture');
    rowData.windows = [
      snap('third_party_7d', 0.5),
      snap('gemini_5h', 0.75),
      snap('third_party_5h', 0.5),
      snap('gemini_7d', 0.25),
    ];
    render(<UpstreamUsageTable data={providerData} />);
    const row = screen.getByTestId('upstream-usage-row');
    expect(
      Array.from(row.querySelectorAll('[data-window]'), (cell) =>
        cell.getAttribute('data-window'),
      ),
    ).toEqual(['gemini_5h', 'gemini_7d', 'third_party_5h', 'third_party_7d']);
    expect(within(row).getByText('Gemini 5h')).toBeDefined();
    expect(within(row).getByText('Gemini 7d')).toBeDefined();
    expect(within(row).getByText('Claude·OSS 5h')).toBeDefined();
    expect(within(row).getByText('Claude·OSS 7d')).toBeDefined();
  });

  it('keeps a disabled expired OAuth warning above quota danger and disabled healthy rows', () => {
    const nudge: OAuthReconnectNudge = {
      reason: 'refresh_token_expired',
      tone: 'danger',
      label: 'Refresh token expired',
      description: 'Reconnect the account.',
      actionLabel: 'Reconnect',
      expiresAt: NOW - 1,
    };
    const rows = buildUpstreamUsageRows({
      upstreams: [
        upstream('disabled-expired', 'anthropic_oauth', false),
        upstream('quota-danger', 'anthropic_oauth', true),
        upstream('disabled-healthy', 'anthropic_oauth', false),
      ],
      latest: {
        now_unix_secs: NOW,
        max_staleness_secs: 300,
        upstreams: [
          {
            upstream_id: 'quota-danger',
            upstream_name: 'quota-danger',
            windows: [snap('5h', 0.99)],
          },
          {
            upstream_id: 'disabled-healthy',
            upstream_name: 'disabled-healthy',
            windows: [snap('5h', 0.99)],
          },
        ],
      },
      status: [
        { id: 'disabled-expired', status: 'active', last_apply_error: null },
        { id: 'quota-danger', status: 'active', last_apply_error: null },
        { id: 'disabled-healthy', status: 'active', last_apply_error: null },
      ],
      nudges: new Map([['disabled-expired', nudge]]),
      usageByUpstreamId: new Map(),
    });

    expect(sortUpstreamRowsByUsage(rows).map((row) => row.upstream.id)).toEqual(
      ['disabled-expired', 'quota-danger', 'disabled-healthy'],
    );

    render(<UpstreamUsageTable data={{ ...data([]), rows }} />);
    const firstRow = screen.getAllByTestId('upstream-usage-row')[0]!;
    expect(
      within(firstRow).getByTestId('usage-row-status').textContent,
    ).toContain('Refresh token expired');
    expect(
      within(firstRow).getByTestId('usage-row-status').textContent,
    ).toContain('Disabled');
  });
  it('keeps a disabled rejected credential above a healthy enabled 96% quota row', () => {
    const nudge: OAuthReconnectNudge = {
      reason: 'renewal_rejected',
      tone: 'danger',
      label: 'Token renewal failed',
      description: 'Reconnect the account.',
      actionLabel: 'Reconnect',
      expiresAt: null,
    };
    const rows = data(
      [
        ['disabled-rejected', 'anthropic_oauth', false, 'active', null, null],
        ['enabled-quota', 'anthropic_oauth', true, 'active', 0.96, null],
      ],
      new Map([['disabled-rejected', nudge]]),
    ).rows;

    expect(sortUpstreamRowsByUsage(rows).map((row) => row.upstream.id)).toEqual(
      ['disabled-rejected', 'enabled-quota'],
    );
  });

  it('shows every row up to the limit, then the rest behind Show all N', () => {
    const specs: Spec[] = Array.from({ length: 11 }, (_, index) => [
      `q${String(index).padStart(2, '0')}`,
      'anthropic_oauth',
      true,
      'active',
      index / 100,
      null,
    ]);
    const { rerender } = render(
      <UpstreamUsageTable collapsedRows={8} data={data(specs.slice(0, 8))} />,
    );
    expect(screen.getAllByTestId('upstream-usage-row')).toHaveLength(8);
    expect(screen.queryByTestId('upstream-usage-toggle')).toBeNull();

    rerender(<UpstreamUsageTable collapsedRows={8} data={data(specs)} />);
    expect(screen.getAllByTestId('upstream-usage-row')).toHaveLength(8);
    // Most used first, so the hidden rows are the calmest.
    expect(rowNames()[0]).toBe('q10');
    const toggle = screen.getByTestId('upstream-usage-toggle');
    expect(toggle.textContent).toBe('Show all 11');
    expect(toggle.getAttribute('aria-expanded')).toBe('false');

    fireEvent.click(toggle);
    expect(screen.getAllByTestId('upstream-usage-row')).toHaveLength(11);
    expect(toggle.getAttribute('aria-expanded')).toBe('true');
    expect(toggle.textContent).toBe('Show fewer');
  });
});
