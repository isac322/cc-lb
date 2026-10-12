import { cleanup, render, screen, within } from '@testing-library/react';
import { afterEach, describe, expect, test } from 'vitest';
import type {
  KeyBillingStopKind,
  KeyUsageMonth,
  KeyUsageProvenance,
} from '../../lib/api';
import { CreditStrip } from './CreditStrip';

afterEach(cleanup);

// October 2026, UTC.
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

function renderStrip(
  month: KeyUsageMonth | undefined,
  provenance: KeyUsageProvenance | undefined = REPORTED,
  opts: { isLoading?: boolean; loadError?: string | null } = {},
) {
  render(
    <CreditStrip
      month={month}
      provenance={provenance}
      isLoading={opts.isLoading ?? false}
      loadError={opts.loadError ?? null}
    />,
  );
  return screen.getByTestId('api-key-credits');
}

function cell(strip: HTMLElement, label: string): HTMLElement {
  const el = strip.querySelector<HTMLElement>(`[data-cell="${label}"]`);
  if (!el) throw new Error(`no ${label} cell`);
  return el;
}

describe('CreditStrip', () => {
  test('reported month: three cells, Balance not in the API', () => {
    const strip = renderStrip(MONTH);

    expect(strip.textContent).toContain('This month (UTC) · resets Nov 1');
    const tokens = cell(strip, 'Tokens used');
    expect(tokens.textContent).toContain('48.2');
    expect(tokens.textContent).toContain('M');
    expect(tokens.textContent).toContain('Reported');
    const credits = cell(strip, 'Credits used');
    expect(credits.textContent).toContain('$412.30');
    expect(credits.textContent).toContain('At list price');
    const balance = cell(strip, 'Balance');
    expect(balance.textContent).toContain('No reading');
    expect(balance.textContent).toContain('Not in API');
    expect(
      within(balance)
        .getByRole('link', { name: /Billing/ })
        .getAttribute('href'),
    ).toBe('https://platform.claude.com/settings/billing');
  });

  test('estimated month: cc-lb traffic only, no key status or fix', () => {
    const strip = renderStrip(MONTH, ESTIMATED);

    expect(cell(strip, 'Tokens used').textContent).toContain(
      'cc-lb traffic only',
    );
    expect(within(strip).queryByText('Key not verified')).toBeNull();
    expect(
      within(strip).queryByRole('button', { name: 'Re-check' }),
    ).toBeNull();
    expect(strip.textContent).not.toContain('org role');
  });

  test('workspace attribution reads as the whole workspace', () => {
    const strip = renderStrip(MONTH, { ...REPORTED, attribution: 'workspace' });
    expect(cell(strip, 'Tokens used').textContent).toContain('Whole workspace');
  });

  test('a retention-shortened month says where it starts', () => {
    const strip = renderStrip({
      ...MONTH,
      start_unix_secs: Date.UTC(2026, 9, 5) / 1000,
    });
    expect(strip.textContent).toContain('Since Oct 5 (UTC) · resets Nov 1');
  });

  test.each<[KeyBillingStopKind, string, string, string]>([
    [
      'credits_exhausted',
      'Out of credits',
      'Seen',
      'Buy credits or turn on auto-reload in Console → Billing → requests resume.',
    ],
    [
      'tier_spend_cap',
      'Spend limit reached',
      'Resumes Nov 1',
      'Request a tier increase in Console → resume before Nov 1.',
    ],
    [
      'spend_limit',
      'Spend limit reached',
      'Seen',
      'Raise the limit in Console → Billing → requests resume.',
    ],
    [
      'billing_error',
      'Billing issue',
      'Seen',
      'Fix the payment method in Console → Billing → requests resume.',
    ],
  ])(
    'billing stop %s shows on Balance with a visible fix',
    (kind, label, fact, fix) => {
      const strip = renderStrip({
        ...MONTH,
        billing_stop: {
          kind,
          observed_at_unix_secs: Math.floor(Date.now() / 1000) - 180,
        },
      });
      const balance = cell(strip, 'Balance');
      expect(balance.textContent).toContain(label);
      expect(balance.textContent).toContain(fact);
      expect(balance.textContent).not.toContain('Not in API');
      // The fix is on screen, not in a tooltip.
      expect(strip.textContent).toContain(fix);
    },
  );

  test('a failed usage request shows no reading and the error, never zero', () => {
    const strip = renderStrip(undefined, undefined, {
      loadError: 'Request failed (500)',
    });

    for (const label of ['Tokens used', 'Credits used']) {
      const el = cell(strip, label);
      expect(el.textContent).toContain('No reading');
      expect(el.textContent).not.toMatch(/\d/);
    }
    expect(within(strip).getByRole('alert').textContent).toBe(
      "Couldn't load this month: Request failed (500)",
    );
    expect(strip.textContent).not.toContain('cc-lb traffic only');
  });

  test('a failed refresh keeps the last figures and says they may be out of date', () => {
    const strip = renderStrip(MONTH, REPORTED, {
      loadError: 'Request failed (500)',
    });

    expect(cell(strip, 'Tokens used').textContent).toContain('48.2');
    expect(cell(strip, 'Credits used').textContent).toContain('$412.30');
    const alerts = within(strip).getAllByRole('alert');
    expect(alerts).toHaveLength(1);
    expect(alerts[0]?.textContent).toBe(
      "Couldn't refresh usage: Request failed (500) — showing the last loaded figures, which may be out of date.",
    );
  });

  test('a sub-cent month reads <$0.01; a zero month reads $0.00 and 0', () => {
    const tiny = renderStrip({ ...MONTH, cost_usd_micros: 4_000 });
    expect(cell(tiny, 'Credits used').textContent).toContain('<$0.01');
    cleanup();
    const zero = renderStrip({ ...MONTH, tokens: 0, cost_usd_micros: 0 });
    expect(cell(zero, 'Credits used').textContent).toContain('$0.00');
    expect(cell(zero, 'Tokens used').textContent).toContain('0');
  });

  test('loading shows skeletons', () => {
    const strip = renderStrip(undefined, undefined, { isLoading: true });
    expect(strip.querySelectorAll('.skeleton').length).toBeGreaterThan(0);
    expect(strip.textContent).not.toContain('No reading');
  });
});

describe('UI-U-04 strip period is UTC whatever the browser zone', () => {
  const originalTz = process.env.TZ;
  afterEach(() => {
    process.env.TZ = originalTz;
  });

  test.each(['Asia/Seoul', 'America/Los_Angeles'])('%s', (tz) => {
    process.env.TZ = tz;
    // Oct 31 23:30 UTC is already Nov 1 in Seoul; the month is still October.
    const october = renderStrip(MONTH);
    expect(october.textContent).toContain('This month (UTC) · resets Nov 1');
    cleanup();
    // Nov 1 00:30 UTC is still Oct 31 in Los Angeles; the month is November.
    const november = renderStrip({
      ...MONTH,
      start_unix_secs: Date.UTC(2026, 10, 1) / 1000,
      end_unix_secs: Date.UTC(2026, 11, 1) / 1000,
    });
    expect(november.textContent).toContain('This month (UTC) · resets Dec 1');
    cleanup();
    const estimated = renderStrip(
      { ...MONTH, start_unix_secs: Date.UTC(2026, 9, 5) / 1000 },
      ESTIMATED,
    );
    expect(estimated.textContent).toContain('Since Oct 5 (UTC) · resets Nov 1');
  });
});

describe('UI-U-05 strip cells', () => {
  test('labels, values and subs; Billing opens Console in a new tab', () => {
    const strip = renderStrip(MONTH);
    const labels = [...strip.querySelectorAll('dt')].map((d) => d.textContent);
    expect(labels).toEqual(['Tokens used', 'Credits used', 'Balance']);
    // Values carry no "… used" suffix: the label says it.
    for (const dd of strip.querySelectorAll('dd')) {
      expect(dd.textContent).not.toMatch(/used/);
    }
    expect(cell(strip, 'Tokens used').textContent).toContain('48.2M');
    const link = within(cell(strip, 'Balance')).getByRole('link', {
      name: /Billing/,
    });
    expect(link.getAttribute('target')).toBe('_blank');
    expect(link.getAttribute('rel')).toContain('noopener');
  });
});
