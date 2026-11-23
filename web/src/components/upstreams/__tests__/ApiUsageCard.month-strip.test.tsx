import {
  cleanup,
  fireEvent,
  render,
  screen,
  within,
} from '@testing-library/react';
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

const data: DashboardUsageResponse = {
  range: '24h',
  step: 'hour',
  group_by: 'model',
  window_start_unix_secs: Date.UTC(2026, 9, 19, 13) / 1000,
  window_end_unix_secs: Date.UTC(2026, 9, 20, 13) / 1000,
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
  last_reported_unix_secs: null,
  pending_first_report: false,
  attribution: 'api_key',
  workspace: null,
  verified: true,
  key_class: 'org_scoped',
  fix: null,
};

const ESTIMATED: KeyUsageProvenance = {
  ...REPORTED,
  source: 'estimated',
  fix: 'role_insufficient',
};

describe('ApiUsageCard month strip and toolbar', () => {
  test('the strip leads the body; chart controls sit in the toolbar, not the header', () => {
    render(
      <ApiUsageCard
        data={data}
        isLoading={false}
        provenance={REPORTED}
        month={MONTH}
        {...baseProps}
      />,
    );
    const card = screen.getByTestId('api-usage-card');
    const strip = within(card).getByTestId('api-key-credits');
    const range = within(card).getByRole('radiogroup', { name: 'Time range' });
    // Strip comes before the toolbar in reading order.
    expect(
      strip.compareDocumentPosition(range) & Node.DOCUMENT_POSITION_FOLLOWING,
    ).toBeTruthy();
    // The controls share the chart block with the legend, under the strip.
    const legend = within(card).getByTestId('api-usage-legend-slot');
    expect(legend.parentElement?.contains(range)).toBe(true);
    expect(within(card).queryByText('Estimated')).toBeNull();
  });

  test('estimated: header badge and a pointer to Access → Usage data', () => {
    const target = document.createElement('div');
    target.dataset.testid = 'api-key-usage-mode';
    target.tabIndex = -1;
    target.scrollIntoView = () => {};
    document.body.append(target);

    render(
      <ApiUsageCard
        data={data}
        isLoading={false}
        provenance={ESTIMATED}
        month={MONTH}
        usageModeShown
        {...baseProps}
      />,
    );
    const card = screen.getByTestId('api-usage-card');
    expect(within(card).getByText('Estimated')).toBeDefined();
    expect(card.textContent).toContain('Estimated from cc-lb traffic');
    // The fix sentence lives only in Access.
    expect(card.textContent).not.toContain('org role');
    fireEvent.click(
      within(card).getByRole('button', { name: 'Get reported usage' }),
    );
    expect(document.activeElement).toBe(target);
    target.remove();
  });

  test('no pointer when Access has no Usage data row (rejected key)', () => {
    render(
      <ApiUsageCard
        data={data}
        isLoading={false}
        provenance={ESTIMATED}
        month={MONTH}
        usageModeShown={false}
        {...baseProps}
      />,
    );
    const card = screen.getByTestId('api-usage-card');
    expect(card.textContent).toContain('Estimated from cc-lb traffic');
    expect(
      within(card).queryByRole('button', { name: 'Get reported usage' }),
    ).toBeNull();
  });

  test('the caption keeps only the custom-endpoint sentence for estimates', () => {
    render(
      <ApiUsageCard
        data={data}
        isLoading={false}
        provenance={{ ...ESTIMATED, fix: 'non_anthropic_base_url' }}
        month={MONTH}
        {...baseProps}
      />,
    );
    const card = screen.getByTestId('api-usage-card');
    expect(card.textContent).toContain(
      'Real usage needs api.anthropic.com Admin API access.',
    );
    expect(card.textContent).not.toContain("cc-lb's request log");
  });

  test('a usage request failure surfaces in the strip', () => {
    render(
      <ApiUsageCard
        data={undefined}
        isLoading={false}
        provenance={undefined}
        month={undefined}
        monthError="Request failed (500)"
        {...baseProps}
      />,
    );
    // The failed request leaves no provenance; the strip still says why.
    expect(
      within(screen.getByTestId('api-key-credits')).getByRole('alert')
        .textContent,
    ).toBe("Couldn't load this month: Request failed (500)");
  });

  test('a failed refresh keeps the figures, marks them, and alerts once', () => {
    render(
      <ApiUsageCard
        data={data}
        isLoading={false}
        provenance={REPORTED}
        month={MONTH}
        monthError="Request failed (500)"
        {...baseProps}
      />,
    );
    const card = screen.getByTestId('api-usage-card');
    expect(card.textContent).toContain('48.2M');
    expect(card.textContent).toContain('Not refreshed');
    expect(card.textContent).toContain(
      'Tokens reported by Anthropic · not refreshed',
    );
    expect(card.textContent).not.toContain('updates about every 5 min');
    const alerts = within(card).getAllByRole('alert');
    expect(alerts).toHaveLength(1);
    expect(alerts[0]?.textContent).toContain(
      "Couldn't refresh usage: Request failed (500)",
    );
  });
});
