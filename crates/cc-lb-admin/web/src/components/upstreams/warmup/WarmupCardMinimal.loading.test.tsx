import { cleanup, render, screen } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest';
import * as queries from '../../../lib/queries';
import { makeOauthUpstream } from '../../../lib/test-utils/warmup-fixtures';
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
  return {
    id: 'attempt-1',
    upstream_id: upstream.id,
    attempted_at_unix_secs: 1_718_380_800,
    completed_at_unix_secs: 1_718_380_801,
    scheduled_for_unix_secs: 1_718_380_800,
    trigger: 'scheduled',
    status: 'success',
    reason: 'cycle_advanced',
    dispatch_kind: 'http',
    http_status: 200,
    cycle_key: 1_718_380_800,
    expected_cycle_key: 1_718_380_800,
    idle_secs_since_prev_window: 300,
    replica_id: 'replica-1',
    lease_holder: 'replica-1',
    upstream_spec_revision: 1,
    dialect_plugin_snapshot: null,
    error_detail: null,
    ...overrides,
  };
}

function makeSummary(
  lastAttempt: queries.WarmupAttempt,
): queries.WarmupSummary {
  return {
    upstream_id: upstream.id,
    last_attempt: lastAttempt,
    recent_attempts: [lastAttempt],
    next_scheduled_at_unix_secs: 1_718_398_800,
    recent_summary_7d: {
      success: lastAttempt.status === 'success' ? 1 : 0,
      skipped: lastAttempt.status === 'skipped' ? 1 : 0,
      transient_failure: lastAttempt.status === 'transient_failure' ? 1 : 0,
      permanent_failure: lastAttempt.status === 'permanent_failure' ? 1 : 0,
    },
    dialect_plugin: null,
  };
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
    mutate: vi.fn(),
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
      setSummaryQuery(
        { ...makeSummary(skipped), recent_attempts: [skipped, failure] },
        false,
      );
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
    setSummaryQuery(
      { ...makeSummary(success), recent_attempts: [success, failure] },
      false,
    );
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

  test('no recorded attempts stays pending rather than healthy', () => {
    setSummaryQuery(
      {
        ...makeSummary(makeAttempt()),
        last_attempt: null,
        recent_attempts: [],
      },
      false,
    );
    render(<WarmupCardMinimal upstream={upstream} />);

    expect(screen.getByTestId('warmup-status-value').textContent).toBe(
      'Pending',
    );
  });
});
