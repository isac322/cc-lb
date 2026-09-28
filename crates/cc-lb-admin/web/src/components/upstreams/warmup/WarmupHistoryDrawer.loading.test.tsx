import { cleanup, render, screen } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest';
import * as queries from '../../../lib/queries';
import { makeOauthUpstream } from '../../../lib/test-utils/warmup-fixtures';
import { WarmupHistoryDrawer } from './WarmupHistoryDrawer';

vi.mock('../../../lib/queries', async () => {
  const actual = await vi.importActual<Record<string, unknown>>(
    '../../../lib/queries',
  );
  return {
    ...actual,
    useWarmupAttempts: vi.fn(),
  };
});

const upstream = makeOauthUpstream();

function makeAttempt(
  overrides: Partial<queries.WarmupAttempt> = {},
): queries.WarmupAttempt {
  const attemptedAt = Math.floor(Date.now() / 1000);
  return {
    id: 'attempt-1',
    upstream_id: upstream.id,
    attempted_at_unix_secs: attemptedAt,
    completed_at_unix_secs: attemptedAt + 1,
    scheduled_for_unix_secs: attemptedAt,
    trigger: 'scheduled',
    status: 'success',
    reason: 'cycle_advanced',
    dispatch_kind: 'http',
    http_status: 200,
    cycle_key: attemptedAt,
    expected_cycle_key: attemptedAt,
    idle_secs_since_prev_window: 300,
    replica_id: 'replica-1',
    lease_holder: 'replica-1',
    upstream_spec_revision: 1,
    dialect_plugin_snapshot: null,
    error_detail: null,
    ...overrides,
  };
}

function setAttemptsQuery(
  attempts: queries.WarmupAttempt[] | undefined,
  isPending: boolean,
) {
  vi.mocked(queries.useWarmupAttempts).mockReturnValue({
    data: attempts
      ? {
          pages: [{ attempts, next_cursor: null }],
          pageParams: [undefined],
        }
      : undefined,
    fetchNextPage: vi.fn(),
    hasNextPage: false,
    isFetchingNextPage: false,
    isLoading: isPending,
    isPending,
  } as never);
}

beforeEach(() => {
  vi.clearAllMocks();
  setAttemptsQuery([makeAttempt()], false);
});

afterEach(() => {
  cleanup();
  document.body.innerHTML = '';
});

describe('WarmupHistoryDrawer loading geometry', () => {
  test('keeps the fixed drawer shell and renders five attempt-shaped skeleton rows', () => {
    setAttemptsQuery(undefined, true);

    render(
      <WarmupHistoryDrawer
        open={true}
        onOpenChange={() => {}}
        upstream={upstream}
      />,
    );

    expect(
      screen.getByRole('dialog', { name: 'Warm-up history' }),
    ).toBeDefined();
    const drawer = screen.getByTestId('warmup-history-drawer');
    expect(screen.getByRole('radio', { name: '24h' })).toBeDefined();
    expect(screen.getByRole('radio', { name: 'All' })).toBeDefined();
    expect(screen.getByRole('button', { name: 'All' })).toBeDefined();

    const overviewSlot = screen.getByTestId(
      'warmup-history-dominant-failure-slot',
    );
    expect(overviewSlot.className).toContain('min-h-4');
    expect(overviewSlot.querySelectorAll('.skeleton')).toHaveLength(1);
    expect(screen.queryByText('0 attempts')).toBeNull();

    const list = screen.getByTestId('warmup-attempt-skeleton-list');
    expect(list.className).toContain('flex');
    expect(list.className).toContain('flex-col');
    expect(list.className).toContain('gap-1');

    const rows = screen.getAllByTestId('warmup-attempt-skeleton-row');
    expect(rows).toHaveLength(5);
    for (const row of rows) {
      expect(row.firstElementChild?.className).toContain('w-full');
      expect(row.firstElementChild?.className).toContain('rounded-sm');
      expect(row.firstElementChild?.className).toContain('px-2.5');
      expect(row.firstElementChild?.className).toContain('py-1.5');
      expect(row.querySelectorAll('.skeleton')).toHaveLength(5);
    }
    expect(drawer.querySelector('.animate-spin')).toBeNull();
  });

  test('reserves the dominant-failure line for both success and failure', () => {
    setAttemptsQuery([makeAttempt()], false);

    const view = render(
      <WarmupHistoryDrawer
        open={true}
        onOpenChange={() => {}}
        upstream={upstream}
      />,
    );

    let slot = screen.getByTestId('warmup-history-dominant-failure-slot');
    const reservedClassName = slot.className;
    expect(reservedClassName).toContain('min-h-4');
    expect(slot.textContent).toBe('');

    setAttemptsQuery(
      [
        makeAttempt({
          id: 'attempt-failed',
          status: 'permanent_failure',
          reason: 'auth_failed',
          http_status: 401,
          error_detail: 'token rejected',
        }),
      ],
      false,
    );
    view.rerender(
      <WarmupHistoryDrawer
        open={true}
        onOpenChange={() => {}}
        upstream={upstream}
      />,
    );

    slot = screen.getByTestId('warmup-history-dominant-failure-slot');
    expect(slot.className).toBe(reservedClassName);
    expect(slot.textContent).toContain('Most common failure:');
    expect(slot.textContent).toContain('(1×)');
  });
});
