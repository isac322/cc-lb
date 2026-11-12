import { cleanup, render, screen } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest';
import * as queries from '../../../lib/queries';
import {
  makeOauthUpstream,
  makeWarmupAttempt,
  makeWarmupLane,
  makeWarmupSummary,
} from '../../../lib/test-utils/warmup-fixtures';
import { WarmupCardMinimal } from './WarmupCardMinimal';

vi.mock('../../../lib/queries', async () => {
  const actual = await vi.importActual<Record<string, unknown>>(
    '../../../lib/queries',
  );
  return {
    ...actual,
    useClearUpstreamWarmupDialectPlugin: vi.fn(),
    useFireNowUpstreamWarmup: vi.fn(),
    usePluginRegistry: vi.fn(),
    useUpdateUpstreamWarmupSettings: vi.fn(),
    useWarmupSummary: vi.fn(),
  };
});

vi.mock('./WarmupHistoryDrawer', () => ({
  WarmupHistoryDrawer: () => null,
}));

const upstream = makeOauthUpstream();

function makeAttempt(
  overrides: Partial<queries.WarmupAttempt> = {},
): queries.WarmupAttempt {
  return makeWarmupAttempt({ upstream_id: upstream.id, ...overrides });
}

/** Single primary lane; `recent` is newest first. */
function makeSummary(
  lastAttempt: queries.WarmupAttempt | null,
  recent: queries.WarmupAttempt[] = lastAttempt ? [lastAttempt] : [],
): queries.WarmupSummary {
  return makeWarmupSummary(
    [makeWarmupLane({ last_attempt: lastAttempt, recent_attempts: recent })],
    { upstreamId: upstream.id },
  );
}

function setSummaryQuery(
  data: queries.WarmupSummary | undefined,
  isPending: boolean,
) {
  vi.mocked(queries.useWarmupSummary).mockReturnValue({
    data,
    isLoading: isPending,
    isPending,
  } as never);
}

function setRegistryPending(isPending: boolean) {
  vi.mocked(queries.usePluginRegistry).mockReturnValue({
    data: isPending ? undefined : { entries: [] },
    isLoading: isPending,
    isPending,
  } as never);
}

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(queries.useUpdateUpstreamWarmupSettings).mockReturnValue({
    isPending: false,
    mutate: vi.fn(),
  } as never);
  vi.mocked(queries.useClearUpstreamWarmupDialectPlugin).mockReturnValue({
    isPending: false,
    mutate: vi.fn(),
  } as never);
  vi.mocked(queries.useFireNowUpstreamWarmup).mockReturnValue({
    isPending: false,
    mutateAsync: vi.fn(),
  } as never);
  setRegistryPending(false);
  setSummaryQuery(makeSummary(makeAttempt()), false);
});

afterEach(cleanup);

describe('WarmupCardMinimal current state', () => {
  test('does not claim an outcome before the first summary is available', () => {
    setSummaryQuery(undefined, true);
    render(<WarmupCardMinimal upstream={upstream} />);

    expect(screen.getByTestId('warmup-status-value').textContent).toBe('');
    expect(screen.queryByText('Healthy')).toBeNull();
    expect(screen.queryByText('Not scheduled')).toBeNull();
    expect(screen.queryByText('Never')).toBeNull();
  });

  test.each([
    { status: 'success', expected: 'Healthy' },
    { status: 'transient_failure', expected: 'Degraded' },
    { status: 'permanent_failure', expected: 'Down' },
    { status: 'skipped', expected: 'Idle' },
  ] as const)(
    'reports the observed $status outcome as $expected',
    ({ status, expected }) => {
      setSummaryQuery(makeSummary(makeAttempt({ status })), false);
      render(<WarmupCardMinimal upstream={upstream} />);
      expect(screen.getByTestId('warmup-status-value').textContent).toBe(
        expected,
      );
    },
  );

  test.each(['transient_failure', 'permanent_failure'] as const)(
    'does not let a later skip erase an unresolved %s',
    (status) => {
      const failure = makeAttempt({
        status,
        reason: 'auth_failed',
        error_detail: 'token rejected',
      });
      const skipped = makeAttempt({ id: 'skip', status: 'skipped' });
      setSummaryQuery(makeSummary(skipped, [skipped, failure]), false);
      render(<WarmupCardMinimal upstream={upstream} />);

      expect(screen.getByTestId('warmup-status-value').textContent).toBe(
        status === 'transient_failure' ? 'Degraded' : 'Down',
      );
      expect(screen.getByTestId('warmup-failure-slot').textContent).toContain(
        'token rejected',
      );
    },
  );

  test('a successful attempt resolves older failures', () => {
    const failure = makeAttempt({
      status: 'permanent_failure',
      reason: 'auth_failed',
      error_detail: 'token rejected',
    });
    const success = makeAttempt({ id: 'recovered' });
    setSummaryQuery(makeSummary(success, [success, failure]), false);
    render(<WarmupCardMinimal upstream={upstream} />);

    expect(screen.getByTestId('warmup-status-value').textContent).toBe(
      'Healthy',
    );
    expect(screen.getByTestId('warmup-failure-slot').textContent).toBe('');
  });

  test.each([
    { enabled: true, warmup_enabled: false },
    { enabled: false, warmup_enabled: true },
  ])('shows deliberate pause separately from an earlier failure', (flags) => {
    const failure = makeAttempt({
      status: 'permanent_failure',
      reason: 'auth_failed',
      error_detail: 'token rejected',
    });
    setSummaryQuery(makeSummary(failure), false);
    render(<WarmupCardMinimal upstream={{ ...upstream, ...flags }} />);

    expect(screen.getByTestId('warmup-status-value').textContent).toBe(
      'Paused',
    );
    expect(screen.getByTestId('warmup-last').textContent).toContain('Failed');
    expect(screen.getByTestId('warmup-failure-slot').textContent).toContain(
      'token rejected',
    );
  });

  test.each(['status_400', 'status_401', 'refresh_token_expired'])(
    'an authoritative credential failure %s outranks cached warm-up success',
    (last_apply_error) => {
      render(
        <WarmupCardMinimal
          upstream={{
            ...upstream,
            status: { ...upstream.status, last_apply_error },
          }}
        />,
      );

      expect(screen.getByTestId('warmup-status-value').textContent).toBe(
        'Down',
      );
    },
  );

  test('does not paint cached success as current when the summary failed', () => {
    vi.mocked(queries.useWarmupSummary).mockReturnValue({
      data: makeSummary(makeAttempt()),
      isPending: false,
      isError: true,
    } as never);
    render(<WarmupCardMinimal upstream={upstream} />);

    expect(screen.getByTestId('warmup-status-value').textContent).toBe(
      'Unavailable',
    );
  });

  test('a deliberate pause does not hide a status lookup failure', () => {
    vi.mocked(queries.useWarmupSummary).mockReturnValue({
      data: undefined,
      isPending: false,
      isError: true,
    } as never);
    render(
      <WarmupCardMinimal upstream={{ ...upstream, warmup_enabled: false }} />,
    );

    expect(screen.getByTestId('warmup-status-value').textContent).toBe(
      'Paused',
    );
    expect(screen.getByTestId('warmup-failure-slot').textContent).toContain(
      'Warm-up status could not be loaded',
    );
  });

  test('Fire now stays disabled until the warm-up summary has loaded', () => {
    setSummaryQuery(undefined, true);
    const { rerender } = render(<WarmupCardMinimal upstream={upstream} />);

    const fire = () =>
      screen.getByTestId('warmup-fire-now') as HTMLButtonElement;
    expect(fire().disabled).toBe(true);

    setSummaryQuery(makeSummary(makeAttempt()), false);
    rerender(<WarmupCardMinimal upstream={upstream} />);
    expect(fire().disabled).toBe(false);
  });

  test('Fire now stays disabled when the summary failed with no data', () => {
    vi.mocked(queries.useWarmupSummary).mockReturnValue({
      data: undefined,
      isPending: false,
      isError: true,
    } as never);
    render(<WarmupCardMinimal upstream={upstream} />);

    expect(
      (screen.getByTestId('warmup-fire-now') as HTMLButtonElement).disabled,
    ).toBe(true);
  });

  test('no recorded attempts stays pending rather than healthy', () => {
    setSummaryQuery(makeSummary(null), false);
    render(<WarmupCardMinimal upstream={upstream} />);

    expect(screen.getByTestId('warmup-status-value').textContent).toBe(
      'Pending',
    );
  });
});
