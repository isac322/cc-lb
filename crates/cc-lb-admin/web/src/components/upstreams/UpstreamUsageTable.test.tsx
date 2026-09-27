import {
  cleanup,
  fireEvent,
  render,
  screen,
  within,
} from '@testing-library/react';
import type { ReactNode } from 'react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import type { LatestResponse, QuotaSnapshot } from '../../lib/api';
import type { Upstream } from '../../lib/queries';
import {
  buildUpstreamUsageRows,
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
  kind: 'anthropic_oauth' | 'anthropic_api_key',
  enabled = true,
): Upstream {
  return {
    id,
    name: id,
    kind,
    enabled,
    spec_revision: 1,
    base_url: null,
    api_key_env: null,
    warmup_enabled: false,
    warmup_dialect_plugin: null,
    status: {
      last_apply_error: null,
      last_apply_at_unix_secs: null,
      last_warmup_at_unix_secs: null,
    },
  } as Upstream;
}

function snap(window: '5h' | '7d', utilization: number): QuotaSnapshot {
  return {
    window,
    state: 'fresh',
    source: 'api',
    utilization,
    status: null,
    resets_at_unix_secs: NOW + 3 * 3600 + 12 * 60 + 30,
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
  'anthropic_oauth' | 'anthropic_api_key',
  boolean,
  string,
  number | null,
  number | null,
];

function data(specs: Spec[]): UpstreamUsageData {
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
      nudges: new Map(),
      usageByUpstreamId: new Map(),
    }),
    isLoading: false,
    quotaPending: false,
    statusPending: false,
    usagePending: false,
    quotaError: false,
    reconnectCount: 0,
  };
}

const rowNames = () =>
  screen
    .getAllByTestId('upstream-usage-row')
    .map((row) => row.querySelector('[title]')?.getAttribute('title'));

describe('UpstreamUsageTable', () => {
  it('orders problems first, then quota by most-used window, API keys, disabled last', () => {
    render(
      <UpstreamUsageTable
        data={data([
          ['key-a', 'anthropic_api_key', true, 'active', null, null],
          ['calm', 'anthropic_oauth', true, 'active', 0.1, 0.2],
          ['off', 'anthropic_oauth', false, 'active', 0.99, 0.99],
          ['busy', 'anthropic_oauth', true, 'active', 0.3, 0.9],
          ['broken', 'anthropic_api_key', true, 'error', null, null],
        ])}
      />,
    );

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
    expect(
      within(busy7d as HTMLElement)
        .getByRole('meter')
        .getAttribute('aria-valuetext'),
    ).toBe('90% used');
    expect(
      rows[1]!.querySelector('[data-window="7d_fable"]')?.textContent,
    ).toContain('No reading');
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
