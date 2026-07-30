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

describe('WarmupCardMinimal loading geometry', () => {
  test('skeletonizes every cold-load warmup value inside its loaded line box', () => {
    setSummaryQuery(undefined, true);
    setRegistryPending(true);

    render(<WarmupCardMinimal upstream={upstream} />);

    for (const testId of [
      'warmup-status-value',
      'warmup-next-value',
      'warmup-last',
      'warmup-plugin-row',
      'warmup-failure-slot',
    ]) {
      expect(
        screen.getByTestId(testId).querySelectorAll('.skeleton'),
      ).toHaveLength(1);
    }

    expect(screen.getByTestId('warmup-status-value').className).toContain(
      'min-h-5',
    );
    expect(screen.getByTestId('warmup-status-value').className).toContain(
      'w-24',
    );
    expect(screen.getByTestId('warmup-next-value').className).toContain(
      'min-h-5',
    );
    expect(screen.getByTestId('warmup-last').className).toContain('min-h-5');
    expect(screen.getByTestId('warmup-plugin-row').className).toContain(
      'min-h-7',
    );
    expect(screen.getByTestId('warmup-failure-slot').className).toContain(
      'min-h-12',
    );
    expect(screen.queryByText('Pending')).toBeNull();
    expect(screen.queryByText('Not scheduled')).toBeNull();
    expect(screen.queryByText('Never')).toBeNull();
  });

  test('keeps the failure callout slot identical for success and failure', () => {
    const successSummary = makeSummary(makeAttempt());
    setSummaryQuery(successSummary, false);

    const view = render(<WarmupCardMinimal upstream={upstream} />);

    let slot = screen.getByTestId('warmup-failure-slot');
    const reservedClassName = slot.className;
    expect(reservedClassName).toContain('min-h-12');
    expect(slot.textContent).toBe('');
    expect(slot.querySelectorAll('.skeleton')).toHaveLength(0);

    setSummaryQuery(
      makeSummary(
        makeAttempt({
          status: 'permanent_failure',
          reason: 'auth_failed',
          http_status: 401,
          error_detail: 'token rejected',
        }),
      ),
      false,
    );
    view.rerender(<WarmupCardMinimal upstream={upstream} />);

    slot = screen.getByTestId('warmup-failure-slot');
    expect(slot.className).toBe(reservedClassName);
    expect(slot.textContent).toContain('token rejected');
    expect(slot.firstElementChild?.className).toContain('min-h-12');
  });
});
