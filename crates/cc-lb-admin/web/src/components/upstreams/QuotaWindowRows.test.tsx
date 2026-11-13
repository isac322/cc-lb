import { cleanup, render, screen } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { PeriodUnknownQuotaMeter, QuotaSnapshot } from '../../lib/api';
import {
  QuotaPeriodUnknownRow,
  QuotaWindowRow,
  QuotaWindowRows,
} from './QuotaWindowRows';

afterEach(() => {
  cleanup();
  vi.useRealTimers();
  window.localStorage.removeItem('cclb.locale');
});

const NOW = Math.floor(Date.now() / 1000);

beforeEach(() => {
  vi.useFakeTimers();
  vi.setSystemTime(NOW * 1000);
});

function snap(overrides: Partial<QuotaSnapshot>): QuotaSnapshot {
  return {
    window: '5h',
    state: 'fresh',
    source: 'api',
    utilization: 0.42,
    status: 'allowed',
    resets_at_unix_secs: NOW + 3600,
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

function renderRow(s: QuotaSnapshot) {
  render(
    <QuotaWindowRows>
      <QuotaWindowRow snap={s} nowUnixSecs={NOW} />
    </QuotaWindowRows>,
  );
  return screen.getByRole('listitem');
}

describe('QuotaWindowRow', () => {
  it('shows a started window as N% used with its reset', () => {
    const row = renderRow(snap({}));
    expect(row.textContent).toContain('5h');
    expect(row.textContent).toContain('42% used');
    expect(row.textContent).toContain('Resets in 1h');
    // Resets in 1h of a 5h window: 4h elapsed is 80% even pace.
    const meter = screen.getByRole('meter');
    expect(meter.getAttribute('aria-valuetext')).toBe(
      '42% used, even pace 80%',
    );
    const marker = row.querySelector<HTMLElement>('[data-slot="pace-marker"]');
    expect(marker?.style.left).toBe('80%');
  });
  it('keeps a stale 5h reading visible', () => {
    const row = renderRow(snap({ window: '5h', state: 'stale' }));
    expect(row.textContent).toContain('42% used');
    expect(screen.getByRole('meter')).not.toBeNull();
  });

  it('aligns reset parts in shared content-sized columns', () => {
    render(
      <QuotaWindowRows>
        <QuotaWindowRow
          snap={snap({ resets_at_unix_secs: NOW + 2 * 3600 + 40 * 60 })}
          nowUnixSecs={NOW}
        />
        <QuotaWindowRow
          snap={snap({ resets_at_unix_secs: NOW + 3600 })}
          nowUnixSecs={NOW}
        />
        <QuotaWindowRow
          snap={snap({ window: '7d', resets_at_unix_secs: null })}
          nowUnixSecs={NOW}
        />
      </QuotaWindowRows>,
    );
    const rows = screen.getAllByRole('listitem');
    const prefixes = rows
      .slice(0, 2)
      .map((row) => row.querySelector('[data-slot="window-reset-prefix"]'));
    expect(
      prefixes.every((cell) => cell?.className === prefixes[0]?.className),
    ).toBe(true);
    expect(screen.getByTestId('quota-snapshot-grid').className).toContain(
      'grid-cols-[minmax(0,max-content)_minmax(0,1fr)_max-content_max-content_max-content]',
    );
    expect(screen.getByTestId('quota-snapshot-grid').className).not.toContain(
      '20ch',
    );
    expect(
      rows[2].querySelector('[data-slot="window-reset-value"]')?.textContent,
    ).toBe('');
    expect(
      rows[2].querySelector('[data-slot="window-reset-suffix"]')?.textContent,
    ).toBe('');
  });

  it('does not reserve a trailing gap for English reset text', () => {
    const row = renderRow(snap({}));
    const grid = screen.getByTestId('quota-snapshot-grid');
    const suffix = row.querySelector('[data-slot="window-reset-suffix"]');
    expect(grid.className).not.toContain('gap-x-');
    expect(suffix?.className).not.toContain('ml-3');
    expect(suffix?.className).not.toContain('@[40rem]:ml-6');
  });

  it('keeps the not-started status separate from empty reset cells', () => {
    const row = renderRow(snap({ utilization: 0, resets_at_unix_secs: null }));
    expect(row.textContent).toContain('Not started');
    expect(row.querySelector('[data-slot="window-facts"]')?.textContent).toBe(
      'Not started',
    );
    expect(
      row.querySelector('[data-slot="window-reset-value"]')?.textContent,
    ).toBe('');
  });

  it('aligns the Korean reset suffix against the shared value edge', () => {
    window.localStorage.setItem('cclb.locale', 'ko-KR');
    const row = renderRow(snap({ resets_at_unix_secs: NOW + 2 * 3600 }));
    expect(row.textContent).toContain('2시간 후 초기화');
    expect(
      row.querySelector('[data-slot="window-reset-value"]')?.className,
    ).toContain('text-right');
    expect(
      row.querySelector('[data-slot="window-reset-suffix"]')?.className,
    ).toContain('text-left');
  });
  it.each([
    ['gemini_5h', 'Gemini 5h', 'Gemini models · 5-hour limit'],
    ['gemini_7d', 'Gemini 7d', 'Gemini models · weekly limit'],
    [
      'third_party_5h',
      'Claude·OSS 5h',
      'Claude and GPT-OSS models · 5-hour limit',
    ],
    [
      'third_party_7d',
      'Claude·OSS 7d',
      'Claude and GPT-OSS models · weekly limit',
    ],
  ] as const)(
    'uses the compact provider label and full accessible name for %s',
    (window, label, longLabel) => {
      const row = renderRow(snap({ window }));
      expect(row.textContent).toContain(label);
      expect(row.querySelector(`[title="${longLabel}"]`)).not.toBeNull();
      expect(screen.getByRole('meter').getAttribute('aria-label')).toBe(
        longLabel,
      );
    },
  );

  it('paces a 7d window over seven days', () => {
    const row = renderRow(
      snap({ window: '7d_fable', resets_at_unix_secs: NOW + 5 * 86400 }),
    );
    const marker = row.querySelector<HTMLElement>('[data-slot="pace-marker"]');
    expect(Number(marker?.dataset.pacePct)).toBeCloseTo((2 / 7) * 100, 6);
  });

  it('colors a window 30+ points ahead of pace as danger', () => {
    // 55% used of a 5h window resetting in 4h: 1h elapsed is a 20% pace,
    // so usage runs 35 points ahead — past the 30-point danger gap.
    renderRow(snap({ utilization: 0.55, resets_at_unix_secs: NOW + 4 * 3600 }));
    const meter = screen.getByRole('meter');
    expect(meter.getAttribute('aria-valuetext')).toBe(
      '55% used, even pace 20%',
    );
    expect(meter.querySelector('.bg-danger')).not.toBeNull();
  });

  it('draws no pace tick while the reset sits more than one length ahead', () => {
    const row = renderRow(
      snap({ utilization: 0.2, resets_at_unix_secs: NOW + 6 * 3600 }),
    );
    expect(row.querySelector('[data-slot="pace-marker"]')).toBeNull();
    expect(screen.getByRole('meter').getAttribute('aria-valuetext')).toBe(
      '20% used',
    );
  });

  it('shows extra usage as dollars spent of the monthly limit', () => {
    const row = renderRow(
      snap({
        window: 'overage',
        utilization: null,
        extra_usage_enabled: true,
        extra_usage_monthly_limit: 5000,
        extra_usage_used_credits: 1200,
      }),
    );
    expect(row.textContent).toContain('Extra usage');
    expect(row.textContent).toContain('$12.00 of $50.00');
    expect(screen.getByRole('meter').getAttribute('aria-valuenow')).toBe('24');
    // Extra usage is a monthly budget, not a timed window: no pace.
    expect(row.querySelector('[data-slot="pace-marker"]')).toBeNull();
  });

  it.each([
    { enabled: false, budget: 5000 },
    { enabled: true, budget: 0 },
    { enabled: true, budget: null },
  ])(
    'omits extra usage without an active positive budget',
    ({ enabled, budget }) => {
      render(
        <QuotaWindowRows>
          <QuotaWindowRow
            snap={snap({
              window: 'overage',
              extra_usage_enabled: enabled,
              extra_usage_monthly_limit: budget,
            })}
            nowUnixSecs={NOW}
          />
        </QuotaWindowRows>,
      );
      expect(screen.queryByRole('listitem')).toBeNull();
      expect(screen.queryByText('Extra usage')).toBeNull();
    },
  );

  it('says a window without a live reset has not started', () => {
    const row = renderRow(snap({ utilization: 0, resets_at_unix_secs: null }));
    expect(row.textContent).toContain('Not started');
    expect(row.textContent).not.toContain('Resets in');
    expect(row.querySelector('[data-slot="pace-marker"]')).toBeNull();
  });
  it('renders nothing for a unified restriction without credit facts', () => {
    // Unified is a transport envelope, not a timed usage window. Only its
    // Codex credit observations are visible.
    render(
      <QuotaWindowRows>
        <QuotaWindowRow
          snap={snap({
            window: 'unified',
          } as unknown as Partial<QuotaSnapshot>)}
          nowUnixSecs={NOW}
        />
      </QuotaWindowRows>,
    );
    expect(screen.queryByRole('listitem')).toBeNull();
  });

  it.each([
    {
      name: 'available',
      credits: {
        has_credits: true,
        unlimited: false,
        limit_reached: false,
        balance: '62500.0000000000',
        approx_local_messages: [10, 20],
        approx_cloud_messages: null,
      },
      status: 'Available',
    },
    {
      name: 'unlimited',
      credits: {
        has_credits: true,
        unlimited: true,
        limit_reached: false,
        balance: null,
        approx_local_messages: null,
        approx_cloud_messages: null,
      },
      status: 'Unlimited',
    },
    {
      name: 'blocked',
      credits: {
        has_credits: true,
        unlimited: true,
        limit_reached: true,
        balance: '62500.0000000000',
        approx_local_messages: null,
        approx_cloud_messages: null,
      },
      status: 'Blocked',
    },
    {
      name: 'unknown',
      credits: {
        has_credits: null,
        unlimited: null,
        limit_reached: null,
        balance: '62500.0000000000',
        approx_local_messages: null,
        approx_cloud_messages: null,
      },
      status: 'Unknown',
    },
  ])(
    'renders Codex credits as a status and raw balance ($name)',
    ({ credits, status }) => {
      const row = renderRow(
        snap({
          window: 'unified',
          utilization: null,
          resets_at_unix_secs: null,
          credits,
        }),
      );
      expect(row.textContent).toContain('Codex credits');
      expect(row.textContent).toContain(status);
      expect(row.textContent).toContain(
        credits.balance != null
          ? `${credits.balance} credits`
          : 'No balance reported',
      );
      expect(screen.queryByRole('meter')).toBeNull();
    },
  );

  it('marks stale Codex credits without treating them as fresh availability', () => {
    const row = renderRow(
      snap({
        window: 'unified',
        state: 'stale',
        utilization: null,
        resets_at_unix_secs: null,
        credits: {
          has_credits: true,
          unlimited: false,
          limit_reached: false,
          balance: '62500.0000000000',
          approx_local_messages: null,
          approx_cloud_messages: null,
        },
      }),
    );
    expect(row.textContent).toContain('Stale');
    expect(row.textContent).not.toContain('Available');
  });
  it('omits an explicitly absent 5h window without a row, meter, or placeholder', () => {
    render(
      <QuotaWindowRows>
        <QuotaWindowRow
          snap={snap({
            window: '5h',
            state: 'absent',
            utilization: null,
            resets_at_unix_secs: null,
          })}
          nowUnixSecs={NOW}
        />
      </QuotaWindowRows>,
    );
    expect(screen.queryByRole('listitem')).toBeNull();
    expect(screen.queryByRole('meter')).toBeNull();
    expect(screen.queryByText('No reading')).toBeNull();
  });

  it('keeps the normal unobserved placeholder for a 5h window', () => {
    const row = renderRow(
      snap({
        window: '5h',
        state: 'unobserved',
        utilization: null,
        resets_at_unix_secs: null,
      }),
    );
    expect(row.textContent).toContain('No reading');
    expect(screen.getByRole('meter').getAttribute('aria-valuetext')).toBe(
      'No reading',
    );
  });

  it.each([
    ['not-reported', 'absent', 'Not reported'],
    ['no-reading', 'unobserved', 'No reading'],
  ] as const)(
    'renders a %s Gemini placeholder with its label and no meter, pace, reset or number',
    (placeholder, state, text) => {
      render(
        <QuotaWindowRows>
          <QuotaWindowRow
            snap={snap({
              window: 'gemini_5h',
              state,
              utilization: null,
              resets_at_unix_secs: null,
            })}
            nowUnixSecs={NOW}
            placeholder={placeholder}
          />
          <QuotaWindowRow
            snap={snap({ window: 'gemini_7d', utilization: 1 })}
            nowUnixSecs={NOW}
          />
        </QuotaWindowRows>,
      );
      const [row, weekly] = screen.getAllByRole('listitem');
      expect(row.getAttribute('data-window')).toBe('gemini_5h');
      expect(row.getAttribute('data-placeholder')).toBe(placeholder);
      expect(row.textContent).toBe(`Gemini 5h${text}`);
      expect(row.textContent).not.toMatch(/%|Resets|Not started|—/);
      expect(row.querySelector('[role="meter"]')).toBeNull();
      expect(row.querySelector('[data-slot="pace-marker"]')).toBeNull();
      expect(
        row.querySelector('[class*="warn"], [class*="danger"]'),
      ).toBeNull();
      expect(weekly.textContent).toContain('100% used');
      expect(screen.getAllByRole('meter')).toHaveLength(1);
    },
  );
});

describe('QA-18: period-unknown meter row', () => {
  const identifier = 'gemini-model-with-a-very-long-machine-identifier';
  function raw(
    overrides: Partial<PeriodUnknownQuotaMeter> = {},
  ): PeriodUnknownQuotaMeter {
    return {
      group: 'gemini',
      model_id: identifier,
      remaining_fraction: 0.4,
      utilization: 0.6,
      reset_time_unix_secs: NOW + 3600,
      source: 'api',
      state: 'fresh',
      origin: 'retrieve_user_quota',
      observed_at_unix_millis: NOW * 1000,
      age_secs: 0,
      ...overrides,
    };
  }
  function renderRaw(overrides: Partial<PeriodUnknownQuotaMeter> = {}) {
    render(
      <QuotaWindowRows>
        <QuotaPeriodUnknownRow meter={raw(overrides)} nowUnixSecs={NOW} />
      </QuotaWindowRows>,
    );
    return screen.getByRole('listitem');
  }

  it.each([0, 0.01, 0.4, 1])(
    'stays neutral without pace at remaining fraction %s',
    (remaining_fraction) => {
      const row = renderRaw({ remaining_fraction });
      expect(row.textContent).toContain('Period unknown');
      expect(row.querySelector('[data-slot="pace-marker"]')).toBeNull();
      expect(
        row.querySelector('[class*="warn"], [class*="danger"]'),
      ).toBeNull();
      expect(
        screen.getByRole('meter').getAttribute('aria-valuetext'),
      ).not.toContain('pace');
      expect(
        row.querySelector<HTMLElement>('[data-slot="window-swatch"]')?.style
          .backgroundColor,
      ).toBe('var(--color-neutral)');
      const name = screen.getByTitle(identifier);
      expect(name.className).toContain(
        'font-mono text-data text-text truncate max-w-[22ch]',
      );
      expect(screen.getByTestId('quota-snapshot-grid').className).not.toMatch(
        /gap-x-|20ch/,
      );
    },
  );

  it('labels a provider-reported period when the reset is unavailable', () => {
    const row = renderRaw({ known_period: '5h', reset_time_unix_secs: null });
    expect(row.textContent).toContain('5h · Reset unavailable');
    expect(row.textContent).not.toContain('Period unknown');
    expect(
      screen.getByRole('meter', {
        name: `${identifier} 5h reset unavailable`,
      }),
    ).not.toBeNull();
    expect(
      screen.getByText('5h · Reset unavailable').getAttribute('title'),
    ).toContain('window period');
  });

  it('keeps the unknown-period cue for an empty known period', () => {
    const row = renderRaw({ known_period: '   ', reset_time_unix_secs: null });
    expect(row.textContent).toContain('Period unknown');
    expect(row.textContent).not.toContain('Reset unavailable');
    expect(
      screen.getByRole('meter', {
        name: `${identifier} period unknown`,
      }),
    ).not.toBeNull();
  });

  it.each([NaN, Infinity, -Infinity])(
    'shows no reading for non-finite remaining %s',
    (remaining_fraction) => {
      const row = renderRaw({ remaining_fraction });
      expect(row.textContent).toContain('—');
      expect(screen.getByText('No reading').className).toBe('sr-only');
      expect(screen.getByRole('meter').getAttribute('aria-valuetext')).toBe(
        'No reading',
      );
    },
  );

  it.each([null, NOW, NOW - 1])(
    'leaves missing or past reset %s quiet',
    (reset_time_unix_secs) => {
      const row = renderRaw({ reset_time_unix_secs });
      expect(row.textContent).not.toMatch(/Not started|Resets in/);
      expect(
        row.querySelector('[data-slot="window-reset-value"]')?.textContent,
      ).toBe('');
    },
  );

  it.each(['en-US', 'ko-KR'])(
    'shares exact canonical reset classes in %s',
    (locale) => {
      window.localStorage.setItem('cclb.locale', locale);
      render(
        <QuotaWindowRows>
          <QuotaWindowRow snap={snap({})} nowUnixSecs={NOW} />
          <QuotaPeriodUnknownRow meter={raw()} nowUnixSecs={NOW} />
        </QuotaWindowRows>,
      );
      const [canonical, unknown] = screen.getAllByRole('listitem');
      expect(unknown.className).toBe(canonical.className);
      for (const part of ['prefix', 'value', 'suffix']) {
        const selector = `[data-slot="window-reset-${part}"]`;
        expect(unknown.querySelector(selector)?.className).toBe(
          canonical.querySelector(selector)?.className,
        );
        expect(unknown.querySelector(selector)?.textContent).toBe(
          canonical.querySelector(selector)?.textContent,
        );
      }
      expect(unknown.textContent).toContain(
        locale === 'ko-KR' ? '1시간 후 초기화' : 'Resets in 1h',
      );
    },
  );

  it.each([
    { model_id: null, bucket_id: 'bucket-7', expected: 'bucket-7' },
    { model_id: null, bucket_id: null, expected: 'gemini' },
  ])(
    'uses the available provider identifier: $expected',
    ({ expected, ...overrides }) => {
      renderRaw(overrides);
      expect(screen.getByTitle(expected).textContent).toBe(expected);
      expect(
        screen.queryByText(/retrieveUserQuota|retrieve_user_quota/),
      ).toBeNull();
    },
  );
});

describe('QA-19: quota row and legend share pace eligibility', () => {
  it.each([
    { name: 'unobserved reading', overrides: { state: 'unobserved' } },
    { name: 'missing utilization', overrides: { utilization: null } },
    { name: 'missing reset', overrides: { resets_at_unix_secs: null } },
    { name: 'expired reset', overrides: { resets_at_unix_secs: NOW } },
    {
      name: 'future unopened window',
      overrides: { resets_at_unix_secs: NOW + 6 * 3600 },
    },
  ] satisfies { name: string; overrides: Partial<QuotaSnapshot> }[])(
    'never draws a pace tick for $name',
    ({ overrides }) => {
      const row = renderRow(snap(overrides));
      expect(row.querySelector('[data-slot="pace-marker"]')).toBeNull();
      expect(
        screen.getByRole('meter').getAttribute('aria-valuetext'),
      ).not.toContain('even pace');
    },
  );
});
