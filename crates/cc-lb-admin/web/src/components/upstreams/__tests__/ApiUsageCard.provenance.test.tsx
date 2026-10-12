// The API usage card's header badge, description and caption per provenance
// state (UI-U-01, 02, 07, 12–15). Key status and fixes live elsewhere.

import { cleanup, render, screen, within } from '@testing-library/react';
import { afterEach, describe, expect, test } from 'vitest';
import type {
  DashboardUsageResponse,
  KeyUsageMonth,
  KeyUsageProvenance,
} from '../../../lib/api';
import { ApiUsageCard } from '../ApiUsageCard';

afterEach(cleanup);

const baseProps = {
  range: '24h' as const,
  onRangeChange: () => {},
  metric: 'tokens' as const,
  onMetricChange: () => {},
};

const NOW = Math.floor(Date.now() / 1000);

function bucket(start: number, tokens: number) {
  return {
    bucket_start_unix_secs: start,
    request_count: 0,
    input_tokens: tokens,
    output_tokens: 0,
    cache_creation_input_tokens: 0,
    cache_read_input_tokens: 0,
    error_count: 0,
    virtual_cost_micros: 0,
    latency_ms_sum: 0,
    latency_count: 0,
  };
}

const EMPTY: DashboardUsageResponse = {
  range: '24h',
  step: 'hour',
  group_by: 'model',
  window_start_unix_secs: NOW - 86_400,
  window_end_unix_secs: NOW,
  series: [],
  observed: true,
};

const MONTH: KeyUsageMonth = {
  start_unix_secs: Date.UTC(2026, 9, 1) / 1000,
  end_unix_secs: Date.UTC(2026, 10, 1) / 1000,
  tokens: 48_200_000,
  cost_usd_micros: 412_300_000,
  billing_stop: null,
};

const REPORTED: KeyUsageProvenance = {
  source: 'reported',
  stale: false,
  stopped: false,
  last_reported_unix_secs: NOW - 600,
  pending_first_report: false,
  attribution: 'api_key',
  workspace: null,
  verified: true,
  key_class: 'org_scoped',
  fix: null,
};
const STOPPED: KeyUsageProvenance = {
  ...REPORTED,
  stale: true,
  stopped: true,
  fix: 'usage_poll_failed',
};
const STALE: KeyUsageProvenance = { ...REPORTED, stale: true };
const ESTIMATED: KeyUsageProvenance = {
  ...REPORTED,
  source: 'estimated',
  last_reported_unix_secs: null,
  fix: 'role_insufficient',
};
const PENDING: KeyUsageProvenance = {
  ...ESTIMATED,
  fix: null,
  pending_first_report: true,
};

function renderCard(
  provenance: KeyUsageProvenance | undefined,
  opts: {
    data?: DashboardUsageResponse;
    metric?: 'tokens' | 'cost';
    usageModeShown?: boolean;
    month?: KeyUsageMonth;
  } = {},
) {
  render(
    <ApiUsageCard
      {...baseProps}
      data={opts.data ?? EMPTY}
      isLoading={false}
      metric={opts.metric ?? 'tokens'}
      provenance={provenance}
      month={opts.month ?? MONTH}
      usageModeShown={opts.usageModeShown ?? true}
    />,
  );
  return screen.getByTestId('api-usage-card');
}

/** The card header: everything before the body's credit strip. */
function header(card: HTMLElement): HTMLElement {
  const headerEl = card.querySelector('header') ?? card.firstElementChild;
  if (!(headerEl instanceof HTMLElement)) throw new Error('no card header');
  return headerEl;
}

describe('ApiUsageCard header (UI-U-01)', () => {
  test.each<[string, KeyUsageProvenance, string | null]>([
    ['reported', REPORTED, null],
    ['estimated', ESTIMATED, 'Estimated'],
    ['stopped', STOPPED, 'Last report'],
    ['stale', STALE, 'Last report'],
  ])('%s: the only header element is the provenance badge', (_l, p, badge) => {
    const card = renderCard(p);
    const head = header(card);
    expect(head.textContent).toContain('API usage');
    if (badge === null) {
      expect(within(head).queryByText('Estimated')).toBeNull();
      expect(within(head).queryByText('Last report')).toBeNull();
    } else {
      expect(within(head).getByText(badge)).toBeDefined();
    }
    // No key-state badge and no Re-check in the card.
    expect(within(card).queryByText('Key accepted')).toBeNull();
    expect(within(card).queryByText('Key not verified')).toBeNull();
    expect(within(card).queryByRole('button', { name: 'Re-check' })).toBeNull();
  });
});

describe('ApiUsageCard description (UI-U-02)', () => {
  test('reported', () => {
    expect(renderCard(REPORTED).textContent).toContain(
      'Tokens reported by Anthropic · updates about every 5 min',
    );
  });

  test('stopped: reports stopped', () => {
    const card = renderCard(STOPPED);
    expect(card.textContent).toContain(
      'Last tokens reported by Anthropic · reports stopped',
    );
    expect(card.textContent).toContain(
      "Showing the last reported figures — all of this key's usage.",
    );
  });

  test('estimated: a button pointing at Access', () => {
    const card = renderCard(ESTIMATED);
    expect(card.textContent).toContain('Estimated from cc-lb traffic');
    expect(
      within(card).getByRole('button', { name: 'Get reported usage' }),
    ).toBeDefined();
  });
});

describe('ApiUsageCard stale-transient (UI-U-14)', () => {
  test('stale without a fix: Anthropic has not answered, no Stopped and no fix', () => {
    const card = renderCard(STALE);
    expect(card.textContent).toContain(
      "Last tokens reported by Anthropic · Anthropic hasn't answered since",
    );
    expect(card.textContent).not.toContain('reports stopped');
    expect(card.textContent).not.toContain('org role');
    expect(card.textContent).not.toContain('All workspaces key');
    expect(within(header(card)).getByText('Last report')).toBeDefined();
  });

  test('stale with no known report time still names the outage', () => {
    const card = renderCard({ ...STALE, last_reported_unix_secs: null });
    expect(card.textContent).toContain(
      "Last tokens reported by Anthropic · Anthropic hasn't answered lately",
    );
  });
});

describe('ApiUsageCard first report pending (UI-U-15)', () => {
  test('Reported in Access, estimated here: the card says reports are coming', () => {
    const card = renderCard(PENDING);
    expect(card.textContent).toContain('Reported figures arrive within 5 min');
    expect(
      within(card).queryByRole('button', { name: 'Get reported usage' }),
    ).toBeNull();
    // No Estimated badge contradicting Access's Reported.
    expect(within(header(card)).queryByText('Estimated')).toBeNull();
    expect(card.textContent).not.toContain('Estimated from cc-lb traffic');
  });
});

describe('ApiUsageCard captions', () => {
  // UI-U-12
  test('custom endpoint: estimates only, list-price note on cost', () => {
    const card = renderCard(
      { ...ESTIMATED, fix: 'non_anthropic_base_url' },
      { metric: 'cost' },
    );
    expect(card.textContent).toContain(
      'Real usage needs api.anthropic.com Admin API access. This upstream points elsewhere → showing estimates only. Cost computed at list price.',
    );
    expect(card.textContent).not.toContain("cc-lb's request log");
  });

  // UI-U-13
  test('attribution: this key vs the whole workspace', () => {
    const key = renderCard(REPORTED);
    expect(key.textContent).toContain(
      "All of this key's usage, including traffic outside cc-lb.",
    );
    cleanup();
    const ws = renderCard({
      ...REPORTED,
      attribution: 'workspace',
      workspace: 'Production',
    });
    expect(ws.textContent).toContain(
      "All usage in Production — Anthropic can't attribute it to this key.",
    );
    expect(
      ws.querySelector('[data-cell="Tokens used"]')?.textContent,
    ).toContain('Whole workspace');
  });
});

describe('ApiUsageCard real zero and models (UI-U-07)', () => {
  test('a fresh upstream with no traffic reads 0 and $0.00, chart empty', () => {
    const card = renderCard(REPORTED, {
      month: { ...MONTH, tokens: 0, cost_usd_micros: 0 },
    });
    const tokens = card.querySelector('[data-cell="Tokens used"]');
    const credits = card.querySelector('[data-cell="Credits used"]');
    expect(tokens?.textContent).toContain('0');
    expect(tokens?.textContent).not.toContain('No reading');
    expect(credits?.textContent).toContain('$0.00');
    expect(within(card).getByText('No usage in selected range')).toBeDefined();
  });

  test('usage Anthropic reports without a model reads "Other"', () => {
    const card = renderCard(REPORTED, {
      data: {
        ...EMPTY,
        series: [
          { key: 'claude-sonnet-4', buckets: [bucket(NOW - 3_600, 10)] },
          { key: '', buckets: [bucket(NOW - 3_600, 5)] },
        ],
      },
    });
    const legend = within(card).getByTestId('api-usage-legend-slot');
    expect(legend.textContent).toContain('claude-sonnet-4');
    expect(legend.textContent).toContain('Other');
  });
});
