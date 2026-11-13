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
import type { CouponInventory } from '../../lib/couponInventory';
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
  it('adds one compact coupon column to a provider-scoped upstream table', () => {
    const usageData = data([
      ['claude', 'anthropic_oauth', true, 'active', 0.1, 0.2],
    ]);
    const inventory: CouponInventory = {
      total: 4,
      complete: true,
      supported: true,
      groups: [],
      credentialsStored: true,
    };
    render(
      <UpstreamUsageTable
        data={{ ...usageData, coupons: new Map([['claude', inventory]]) }}
      />,
    );
    expect(screen.getAllByText('Coupons')).toHaveLength(1);
    const button = screen.getByRole('button', {
      name: 'Coupons for claude: 4',
    });
    expect(button.textContent).toBe('×4');
    expect(button.closest('a')).toBeNull();
    expect(button.getAttribute('aria-haspopup')).toBe('dialog');
    expect(
      screen.getByRole('link', { name: /claude/ }).getAttribute('href'),
    ).toBe('/upstreams?selectedId=claude');
  });

  it('aligns mixed provider rows to the union of their observed windows', () => {
    const usageData = data([
      ['claude', 'anthropic_oauth', true, 'active', 0.1, 0.2],
      ['gemini', 'gemini_oauth', true, 'active', null, null],
    ]);
    usageData.rows[1]!.windows = [snap('gemini_5h', 0.3)];
    render(<UpstreamUsageTable data={usageData} />);
    const claude = screen.getByRole('link', { name: /claude/ });
    const gemini = screen.getByRole('link', { name: /gemini/ });
    expect(claude.querySelectorAll('[data-window]')).toHaveLength(3);
    expect(gemini.querySelectorAll('[data-window]')).toHaveLength(3);
    expect(
      claude
        .querySelector('[data-window="gemini_5h"]')
        ?.getAttribute('aria-hidden'),
    ).toBe('true');
    expect(
      gemini.querySelector('[data-window="5h"]')?.getAttribute('aria-hidden'),
    ).toBe('true');
    expect(screen.getAllByText('Coupons')).toHaveLength(1);
  });

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
    expect(absent.textContent).toBe('');
    expect(within(absent).queryByRole('meter')).toBeNull();
    expect(within(weekly).queryAllByText('No reading')).toHaveLength(0);
    expect(within(weekly).queryByText('0%')).toBeNull();
    expect(within(weekly).getAllByTestId('usage-cell')).toHaveLength(1);
    expect(within(normal).getAllByTestId('usage-cell')).toHaveLength(2);
    const apiKeyRow = screen.getByTestId('usage-no-quota').closest('li');
    expect(apiKeyRow).not.toBeNull();
    expect(within(apiKeyRow!).queryByRole('meter')).toBeNull();
    expect(screen.getByTestId('usage-no-quota').textContent).toBe(
      'No subscription quota',
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

describe('UpstreamUsageTable shared layout', () => {
  /** Desktop window header labels (aria-hidden row), without name/Coupons. */
  const headerLabels = (container: HTMLElement) =>
    Array.from(
      container.querySelectorAll('[aria-hidden="true"].border-b > span'),
      (span) => span.textContent,
    ).filter((text) => text !== 'Upstream' && text !== 'Coupons');

  function mixedFleet() {
    const usageData = data([
      ['claude', 'anthropic_oauth', true, 'active', 0.1, 0.2],
      ['codex', 'openai_oauth', true, 'active', 0.4, 0.5],
      ['gemini', 'gemini_oauth', true, 'active', null, null],
      ['key', 'anthropic_api_key', true, 'active', null, null],
    ]);
    const claude = usageData.rows[0]!;
    claude.windows = [...claude.windows, snap('7d_fable', 0.6)];
    usageData.rows[2]!.windows = [
      snap('gemini_5h', 0.3),
      snap('gemini_7d', 0.35),
      snap('third_party_5h', 0.9),
      snap('third_party_7d', 0.7),
    ];
    return usageData;
  }

  const slotWindows = (cell: Element | null) =>
    Array.from(cell?.querySelectorAll('[data-window]') ?? [], (slot) =>
      slot.getAttribute('data-window'),
    );

  it('maps every provider into exactly the generic 5h and 7d columns', () => {
    const { container } = render(
      <UpstreamUsageTable data={mixedFleet()} layout="shared" />,
    );
    expect(headerLabels(container)).toEqual(['5h', '7d']);

    const claude = screen.getByRole('link', { name: /claude/ });
    expect(claude.querySelector('[data-window="5h"]')?.textContent).toContain(
      '10%used',
    );
    const claude7d = claude.querySelector('[data-column="7d"]');
    expect(slotWindows(claude7d)).toEqual(['7d', '7d_fable']);
    expect(
      within(claude7d as HTMLElement).getByText('All models'),
    ).toBeTruthy();
    expect(within(claude7d as HTMLElement).getByText('Fable')).toBeTruthy();
    expect(
      within(claude).getByRole('meter', { name: '7d (Fable) window used' }),
    ).toBeTruthy();

    const codex = screen.getByRole('link', { name: /codex/ });
    expect(
      Array.from(codex.querySelectorAll('[data-window]'), (cell) =>
        cell.getAttribute('data-window'),
      ),
    ).toEqual(['5h', '7d']);
    expect(codex.querySelector('[data-column]')).toBeNull();

    const key = screen.getByRole('link', { name: /key/ });
    expect(within(key).getByTestId('usage-no-quota').textContent).toBe(
      'No subscription quota',
    );
  });

  it('shows each Gemini group as its own aligned slot, never combined', () => {
    render(<UpstreamUsageTable data={mixedFleet()} layout="shared" />);
    const gemini = screen.getByRole('link', { name: /gemini/ });
    const fiveHour = gemini.querySelector('[data-column="5h"]');
    const weekly = gemini.querySelector('[data-column="7d"]');
    expect(slotWindows(fiveHour)).toEqual(['gemini_5h', 'third_party_5h']);
    expect(slotWindows(weekly)).toEqual(['gemini_7d', 'third_party_7d']);

    const [geminiSlot, thirdPartySlot] = Array.from(
      fiveHour?.querySelectorAll('[data-window]') ?? [],
    ) as HTMLElement[];
    expect(within(geminiSlot!).getByText('Gemini models')).toBeTruthy();
    expect(geminiSlot!.textContent).toContain('30%used');
    expect(within(thirdPartySlot!).getByText('Claude & GPT-OSS')).toBeTruthy();
    expect(thirdPartySlot!.textContent).toContain('90%used');
    expect(fiveHour?.textContent).not.toContain('60%');
    for (const name of [
      'Gemini models · 5-hour limit window used',
      'Claude and GPT-OSS models · 5-hour limit window used',
      'Gemini models · weekly limit window used',
      'Claude and GPT-OSS models · weekly limit window used',
    ]) {
      expect(within(gemini).getByRole('meter', { name })).toBeTruthy();
    }
    expect(screen.queryByText('Gemini 5h')).toBeNull();
    expect(screen.queryByText('Claude·OSS 7d')).toBeNull();
  });

  it.each([
    ['absent', 'not-reported', 'Not reported'],
    ['unobserved', 'no-reading', 'No reading'],
  ] as const)(
    'keeps a %s Gemini 5h slot as a neutral placeholder beside an exhausted weekly reading (%s)',
    (state, placeholder, text) => {
      const usageData = data([
        ['gemini', 'gemini_oauth', true, 'active', null, null],
      ]);
      usageData.rows[0]!.windows = [
        snap('gemini_5h', null, state),
        snap('gemini_7d', 1),
        snap('third_party_5h', 0.2),
        snap('third_party_7d', 0.4),
      ];
      const { container } = render(
        <UpstreamUsageTable data={usageData} layout="shared" />,
      );
      expect(headerLabels(container)).toEqual(['5h', '7d']);
      const slot = screen.getByTestId('usage-slot-placeholder');
      expect(slot.getAttribute('data-window')).toBe('gemini_5h');
      expect(slot.getAttribute('data-state')).toBe(placeholder);
      expect(slot.textContent).toContain('Gemini models');
      expect(slot.textContent).toContain(text);
      expect(slot.textContent).not.toMatch(/%|used|resets|Reset/);
      expect(within(slot).queryByRole('meter')).toBeNull();
      expect(
        screen.queryByRole('meter', {
          name: 'Gemini models · 5-hour limit window used',
        }),
      ).toBeNull();
      const weekly = container.querySelector(
        '[data-column="7d"] [data-window="gemini_7d"]',
      );
      expect(weekly?.textContent).toContain('100%used');
      // Alignment: the 5h column still lists both groups in order.
      expect(
        slotWindows(container.querySelector('[data-column="5h"]')),
      ).toEqual(['gemini_5h', 'third_party_5h']);
    },
  );

  it('hides a wholly unreported Gemini group from both columns', () => {
    const usageData = data([
      ['gemini', 'gemini_oauth', true, 'active', null, null],
    ]);
    usageData.rows[0]!.windows = [
      snap('gemini_5h', 0.3),
      snap('gemini_7d', 0.4),
      snap('third_party_5h', null, 'absent'),
      snap('third_party_7d', null, 'absent'),
    ];
    const { container } = render(
      <UpstreamUsageTable data={usageData} layout="shared" />,
    );
    expect(screen.queryByText('Claude & GPT-OSS')).toBeNull();
    expect(screen.queryByTestId('usage-slot-placeholder')).toBeNull();
    expect(slotWindows(container.querySelector('[data-column="5h"]'))).toEqual([
      'gemini_5h',
    ]);
    expect(slotWindows(container.querySelector('[data-column="7d"]'))).toEqual([
      'gemini_7d',
    ]);
  });

  it('keeps both columns for a weekly-only fleet and names absent 5h as Not offered', () => {
    const usageData = data([
      ['claude', 'anthropic_oauth', true, 'active', null, null],
      ['codex', 'openai_oauth', true, 'active', null, null],
      ['gemini', 'gemini_oauth', true, 'active', null, null],
    ]);
    usageData.rows[0]!.windows = [snap('5h', null, 'absent'), snap('7d', 0.2)];
    usageData.rows[1]!.windows = [snap('5h', null, 'absent'), snap('7d', 0.3)];
    usageData.rows[2]!.windows = [
      snap('gemini_5h', null, 'absent'),
      snap('gemini_7d', 0.4),
      snap('third_party_5h', null, 'absent'),
      snap('third_party_7d', null, 'absent'),
    ];
    const { container } = render(
      <UpstreamUsageTable data={usageData} layout="shared" />,
    );
    expect(headerLabels(container)).toEqual(['5h', '7d']);
    for (const name of [/^claude/, /^codex/]) {
      const row = screen.getByRole('link', { name });
      const cell = within(row).getByTestId('usage-cell-status');
      expect(cell.getAttribute('data-window')).toBe('5h');
      expect(cell.getAttribute('data-state')).toBe('not-offered');
      expect(cell.getAttribute('aria-hidden')).toBeNull();
      expect(within(cell).getByText('Not offered').className).toBe('sr-only');
      expect(cell.textContent).toContain('—');
      expect(cell.textContent).not.toContain('%');
      expect(
        within(row).queryByRole('meter', { name: '5h window used' }),
      ).toBeNull();
      expect(
        within(row).getByRole('meter', { name: '7d window used' }),
      ).toBeTruthy();
    }
    const gemini = screen.getByRole('link', { name: /^gemini/ });
    const placeholder = within(gemini).getByTestId('usage-slot-placeholder');
    expect(placeholder.getAttribute('data-window')).toBe('gemini_5h');
    expect(placeholder.textContent).toContain('Not reported');
    expect(screen.queryByTestId('usage-absent-cell')).toBeNull();
    expect(screen.queryByTestId('usage-cell-empty')).toBeNull();
  });

  it.each([
    ['anthropic_oauth', '5h'],
    ['openai_oauth', '5h'],
  ] as const)(
    'shows an unobserved %s window as No reading without a meter',
    (kind, window) => {
      const usageData = data([['solo', kind, true, 'active', null, 0.5]]);
      usageData.rows[0]!.windows.push(snap(window, null, 'unobserved'));
      render(<UpstreamUsageTable data={usageData} layout="shared" />);
      const cell = screen.getByTestId('usage-cell-status');
      expect(cell.getAttribute('data-window')).toBe('5h');
      expect(cell.getAttribute('data-state')).toBe('no-reading');
      expect(cell.textContent).toContain('No reading');
      expect(cell.textContent).not.toMatch(/%|—/);
      expect(
        screen.queryByRole('meter', { name: '5h window used' }),
      ).toBeNull();
    },
  );

  it('reads No reading in both columns when no Gemini group has a reading', () => {
    const usageData = data([
      ['gemini', 'gemini_oauth', true, 'active', null, null],
    ]);
    usageData.rows[0]!.windows = [
      snap('gemini_5h', null, 'unobserved'),
      snap('gemini_7d', null, 'unobserved'),
      snap('third_party_5h', null, 'absent'),
      snap('third_party_7d', null, 'absent'),
    ];
    const { container } = render(
      <UpstreamUsageTable data={usageData} layout="shared" />,
    );
    expect(headerLabels(container)).toEqual(['5h', '7d']);
    expect(
      screen
        .getAllByTestId('usage-cell-status')
        .map((cell) => `${cell.dataset.window}:${cell.dataset.state}`),
    ).toEqual(['5h:no-reading', '7d:no-reading']);
    expect(screen.queryByText('Gemini models')).toBeNull();
    expect(screen.queryAllByRole('meter')).toHaveLength(0);
  });

  it('reads Not offered in both columns for an all-absent Gemini upstream', () => {
    const usageData = data([
      ['gemini', 'gemini_oauth', true, 'active', null, null],
    ]);
    usageData.rows[0]!.windows = [
      snap('gemini_5h', null, 'absent'),
      snap('gemini_7d', null, 'absent'),
      snap('third_party_5h', null, 'absent'),
      snap('third_party_7d', null, 'absent'),
    ];
    const { rerender } = render(
      <UpstreamUsageTable data={usageData} layout="shared" />,
    );
    const cells = screen.getAllByTestId('usage-cell-status');
    expect(
      cells.map((cell) => `${cell.dataset.window}:${cell.dataset.state}`),
    ).toEqual(['5h:not-offered', '7d:not-offered']);
    for (const cell of cells) {
      expect(cell.textContent).toContain('—');
      expect(within(cell).getByText('Not offered').className).toBe('sr-only');
      expect(cell.textContent).not.toContain('No reading');
    }

    // A period-unknown raw meter is a reading, so absence is not proven.
    rerender(
      <UpstreamUsageTable
        data={{
          ...usageData,
          rows: [
            {
              ...usageData.rows[0]!,
              periodUnknownMeters: [
                {
                  group: 'gemini',
                  model_id: 'gemini-2.5-pro',
                  known_period: null,
                  remaining_fraction: 0.6,
                  utilization: 0.4,
                  reset_time_unix_secs: null,
                  origin: 'retrieve_user_quota',
                  source: 'api',
                  state: 'fresh',
                  observed_at_unix_millis: NOW * 1000,
                  age_secs: 0,
                },
              ],
            },
          ],
        }}
        layout="shared"
      />,
    );
    expect(
      screen
        .getAllByTestId('usage-cell-status')
        .map((cell) => cell.dataset.state),
    ).toEqual(['no-reading', 'no-reading']);
  });

  it('marks a stale Gemini slot and a stale Anthropic cell, without pace', () => {
    const usageData = data([
      ['claude', 'anthropic_oauth', true, 'active', 0.42, 0.2],
      ['gemini', 'gemini_oauth', true, 'active', null, null],
    ]);
    const claude5h = usageData.rows[0]!.windows[0]!;
    claude5h.state = 'stale';
    claude5h.age_secs = 12 * 60;
    usageData.rows[1]!.windows = [
      { ...snap('gemini_5h', 0.3), state: 'stale', age_secs: 3 * 3600 },
      snap('gemini_7d', 0.35),
    ];
    render(<UpstreamUsageTable data={usageData} layout="shared" />);

    const claude = screen.getByRole('link', { name: /^claude/ });
    const stale5h = claude.querySelector('[data-window="5h"]') as HTMLElement;
    expect(stale5h.dataset.state).toBe('stale');
    expect(stale5h.textContent).toContain('42%used');
    expect(within(stale5h).getByText('Stale · 12m ago')).toBeTruthy();
    expect(stale5h.textContent).not.toContain('resets in');
    const claudeMeter = within(claude).getByRole('meter', {
      name: '5h window used, stale reading',
    });
    expect(claudeMeter.querySelector('[data-slot="pace-marker"]')).toBeNull();
    // The fresh 7d reading in the same row keeps its pace tick.
    expect(
      within(claude)
        .getByRole('meter', { name: '7d window used' })
        .querySelector('[data-slot="pace-marker"]'),
    ).not.toBeNull();
    expect(
      within(claude).getByRole('meter', { name: '7d window used' }),
    ).toBeTruthy();

    const gemini = screen.getByRole('link', { name: /^gemini/ });
    const staleSlot = gemini.querySelector(
      '[data-window="gemini_5h"]',
    ) as HTMLElement;
    expect(staleSlot.dataset.state).toBe('stale');
    expect(within(staleSlot).getByText('Stale · 3h ago')).toBeTruthy();
    expect(
      within(staleSlot).getByRole('meter', {
        name: 'Gemini models · 5-hour limit window used, stale reading',
      }),
    ).toBeTruthy();
    const fresh7d = gemini.querySelector(
      '[data-window="gemini_7d"]',
    ) as HTMLElement;
    expect(fresh7d.dataset.state).toBeUndefined();
    expect(fresh7d.textContent).toContain('resets in');
  });

  it('keeps the shared columns for an API-key-only fleet', () => {
    const { container } = render(
      <UpstreamUsageTable
        data={data([['key', 'anthropic_api_key', true, 'active', null, null]])}
        layout="shared"
      />,
    );
    expect(headerLabels(container)).toEqual(['5h', '7d']);
    expect(screen.getByTestId('usage-no-quota').textContent).toBe(
      'No subscription quota',
    );
  });

  it.each([
    ['anthropic_oauth', '5h'],
    ['openai_oauth', '5h'],
    ['gemini_oauth', 'gemini_5h'],
  ] as const)(
    'keeps generic 5h/7d columns for a %s-only fleet',
    (kind, fiveHour) => {
      const usageData = data([['solo', kind, true, 'active', null, null]]);
      usageData.rows[0]!.windows = [
        snap(fiveHour, 0.25),
        snap(fiveHour === '5h' ? '7d' : 'gemini_7d', 0.5),
      ];
      const { container } = render(
        <UpstreamUsageTable data={usageData} layout="shared" />,
      );
      expect(headerLabels(container)).toEqual(['5h', '7d']);
    },
  );

  it('keeps the provider layout columns when no layout is given', () => {
    const geminiOnly = data([
      ['gemini', 'gemini_oauth', true, 'active', null, null],
    ]);
    geminiOnly.rows[0]!.windows = mixedFleet().rows[2]!.windows;
    const { container, unmount } = render(
      <UpstreamUsageTable data={geminiOnly} />,
    );
    expect(headerLabels(container)).toEqual([
      'Gemini 5h',
      'Gemini 7d',
      'Claude·OSS 5h',
      'Claude·OSS 7d',
    ]);
    expect(container.querySelector('[data-column]')).toBeNull();
    unmount();

    const anthropicOnly = data([
      ['claude', 'anthropic_oauth', true, 'active', 0.1, 0.2],
    ]);
    anthropicOnly.rows[0]!.windows.push(snap('7d_fable', 0.6));
    const provider = render(<UpstreamUsageTable data={anthropicOnly} />);
    expect(headerLabels(provider.container)).toEqual([
      '5h',
      '7d',
      '7d (Fable)',
    ]);
  });

  it('keeps the Codex credits column beside the shared columns', () => {
    const usageData = data([
      ['codex', 'openai_oauth', true, 'active', 0.4, 0.5],
    ]);
    usageData.rows[0]!.windows.push({
      ...snap('unified', null),
      credits: {
        has_credits: true,
        unlimited: false,
        limit_reached: false,
        balance: '12.5',
        approx_local_messages: null,
        approx_cloud_messages: null,
      },
    });
    const { container } = render(
      <UpstreamUsageTable data={usageData} layout="shared" />,
    );
    expect(headerLabels(container)).toEqual(['5h', '7d', 'Codex credits']);
    expect(screen.getByTestId('codex-credit-facts')).toBeTruthy();
  });

  it('pends an all-Gemini fleet with only 5h and 7d skeletons', () => {
    const usageData = data([
      ['gemini-a', 'gemini_oauth', true, 'active', null, null],
      ['gemini-b', 'gemini_oauth', true, 'active', null, null],
    ]);
    const { container } = render(
      <UpstreamUsageTable
        data={{ ...usageData, quotaPending: true }}
        layout="shared"
      />,
    );
    expect(headerLabels(container)).toEqual(['5h', '7d']);
    for (const row of screen.getAllByTestId('upstream-usage-row')) {
      expect(
        Array.from(row.querySelectorAll('[data-window]'), (cell) =>
          cell.getAttribute('data-window'),
        ),
      ).toEqual(['5h', '7d']);
    }
    expect(screen.queryByText('7d (Fable)')).toBeNull();
    expect(screen.queryByText('Codex credits')).toBeNull();
    expect(screen.queryAllByRole('meter')).toHaveLength(0);
    // Three skeletons per cell: figure, caption, meter.
    expect(container.querySelectorAll('.skeleton')).toHaveLength(2 * 2 * 3);
  });

  it('loads the upstream list with two shared skeleton columns', () => {
    const { container } = render(
      <UpstreamUsageTable
        data={{ ...data([]), isLoading: true }}
        layout="shared"
      />,
    );
    const [first] = screen.getAllByTestId('upstream-list-loading-row');
    // Name skeleton plus two columns of figure and meter.
    expect(first!.querySelectorAll('.skeleton')).toHaveLength(1 + 2 * 2);
    expect(container.querySelectorAll('.skeleton')).toHaveLength(3 * 5);
  });
});
