import {
  cleanup,
  fireEvent,
  render,
  screen,
  within,
} from '@testing-library/react';
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

function setAttemptsQuery(attempts: queries.WarmupAttempt[] | undefined) {
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
    isLoading: false,
    isPending: false,
  } as never);
}

function renderDrawer() {
  return render(
    <WarmupHistoryDrawer
      open={true}
      onOpenChange={() => {}}
      upstream={upstream}
    />,
  );
}

/** The filter chip for an outcome plus its `.status-dot` span. */
function filterChip(name: string) {
  const chip = screen.getByRole('button', { name });
  return { chip, dot: chip.querySelector('.status-dot') };
}

beforeEach(() => {
  vi.clearAllMocks();
  setAttemptsQuery([makeAttempt()]);
});

afterEach(() => {
  cleanup();
  document.body.innerHTML = '';
});

describe('WarmupHistoryDrawer outcome colors', () => {
  test.each([
    {
      status: 'success' as const,
      reason: 'cycle_advanced' as const,
      label: 'Success',
      textClass: 'text-traffic-success-text',
    },
    {
      status: 'transient_failure' as const,
      reason: 'upstream_5xx' as const,
      label: 'Retrying',
      textClass: 'text-warn-text',
    },
    {
      status: 'permanent_failure' as const,
      reason: 'auth_failed' as const,
      label: 'Failed',
      textClass: 'text-danger-text',
    },
    {
      status: 'skipped' as const,
      reason: 'upstream_disabled' as const,
      label: 'Skipped',
      textClass: 'text-text-muted',
    },
  ])(
    '$status rows use the matching semantic tokens',
    ({ status, reason, label, textClass }) => {
      setAttemptsQuery([makeAttempt({ status, reason })]);
      renderDrawer();

      const list = screen.getByRole('list');
      const labelEl = within(list).getByText(label);
      expect(labelEl.classList.contains(textClass)).toBe(true);
    },
  );

  test('row timestamps and metadata stay neutral, not status-colored', () => {
    setAttemptsQuery([
      makeAttempt({ status: 'transient_failure', reason: 'upstream_5xx' }),
    ]);
    renderDrawer();

    const list = screen.getByRole('list');
    const labelEl = within(list).getByText('Retrying');
    const row = labelEl.closest('button');
    for (const meta of row?.querySelectorAll('.text-caption') ?? []) {
      for (const cls of ['text-warn-text', 'text-danger-text', 'traffic']) {
        expect(meta.className).not.toContain(cls);
      }
    }
  });

  test.each([
    {
      status: 'success' as const,
      reason: 'cycle_advanced' as const,
      httpStatus: 200,
      textClass: 'text-traffic-success-text',
    },
    {
      status: 'transient_failure' as const,
      reason: 'rate_limited_cycle_key_missing' as const,
      httpStatus: 429,
      textClass: 'text-warn-text',
    },
    {
      status: 'transient_failure' as const,
      reason: 'upstream_5xx' as const,
      httpStatus: 503,
      textClass: 'text-danger-text',
    },
  ])(
    'HTTP $httpStatus carries its semantic color in list and detail',
    ({ status, reason, httpStatus, textClass }) => {
      setAttemptsQuery([
        makeAttempt({ status, reason, http_status: httpStatus }),
      ]);
      renderDrawer();

      const list = screen.getByRole('list');
      const rowFigure = within(list).getByText(String(httpStatus));
      expect(rowFigure.classList.contains(textClass)).toBe(true);
      const row = rowFigure.closest('button');
      if (!row) throw new Error('expected the attempt row button');

      fireEvent.click(row);
      const term = screen.getByText('HTTP', { selector: 'dt' });
      const detailFigure = term.nextElementSibling;
      expect(detailFigure?.textContent).toBe(String(httpStatus));
      expect(detailFigure?.classList.contains(textClass)).toBe(true);
    },
  );

  test('the All filter chip stays plain — it is not an outcome', () => {
    renderDrawer();

    const { dot } = filterChip('All');
    expect(dot).toBeNull();
  });

  test('attempt detail narrative follows the row severity', () => {
    setAttemptsQuery([
      makeAttempt({ status: 'permanent_failure', reason: 'auth_failed' }),
    ]);
    renderDrawer();

    const list = screen.getByRole('list');
    const row = within(list).getByText('Failed').closest('button');
    if (!row) throw new Error('expected the attempt row button');
    fireEvent.click(row);

    const narrative = screen.getByText(/Auth rejected \(401\)\./);
    expect(narrative.classList.contains('text-danger-text')).toBe(true);
  });

  test('overview counts carry the severity colors', () => {
    setAttemptsQuery([
      makeAttempt({ id: 'ok-1' }),
      makeAttempt({ id: 'ok-2' }),
      makeAttempt({
        id: 'perm-1',
        status: 'permanent_failure',
        reason: 'auth_failed',
      }),
      makeAttempt({
        id: 'skip-1',
        status: 'skipped',
        reason: 'upstream_disabled',
      }),
    ]);
    renderDrawer();

    const countsLine = screen.getByText(/success/);
    const [successCount, failedCount, skippedCount] =
      countsLine.querySelectorAll(':scope > span');
    expect(successCount?.textContent).toBe('2');
    expect(successCount?.classList.contains('text-traffic-success-text')).toBe(
      true,
    );
    expect(failedCount?.textContent).toBe('1');
    expect(failedCount?.classList.contains('text-danger-text')).toBe(true);
    expect(skippedCount?.textContent).toBe('1');
    expect(skippedCount?.classList.contains('text-text-muted')).toBe(true);
  });

  test('transient-only failures color the merged failed count warn, not danger', () => {
    setAttemptsQuery([
      makeAttempt({ id: 'ok-1' }),
      makeAttempt({
        id: 'trans-1',
        status: 'transient_failure',
        reason: 'upstream_5xx',
      }),
      makeAttempt({
        id: 'trans-2',
        status: 'transient_failure',
        reason: 'network_error',
      }),
    ]);
    renderDrawer();

    const countsLine = screen.getByText(/success/);
    const [successCount, failedCount] =
      countsLine.querySelectorAll(':scope > span');
    expect(successCount?.classList.contains('text-traffic-success-text')).toBe(
      true,
    );
    expect(failedCount?.textContent).toBe('2');
    expect(failedCount?.classList.contains('text-warn-text')).toBe(true);
    expect(failedCount?.classList.contains('text-danger-text')).toBe(false);
  });

  test('zero counts stay faint so empty buckets do not alarm', () => {
    setAttemptsQuery([makeAttempt()]);
    renderDrawer();

    const countsLine = screen.getByText(/success/);
    const [successCount, failedCount, skippedCount] =
      countsLine.querySelectorAll(':scope > span');
    expect(successCount?.textContent).toBe('1');
    expect(failedCount?.textContent).toBe('0');
    expect(failedCount?.className).toBe('');
    expect(skippedCount?.textContent).toBe('0');
    expect(skippedCount?.classList.contains('text-text-muted')).toBe(true);
  });

  test('the dominant failure line shares the failure severity', () => {
    setAttemptsQuery([
      makeAttempt({
        id: 'perm-1',
        status: 'permanent_failure',
        reason: 'auth_failed',
      }),
    ]);
    renderDrawer();

    const slot = screen.getByTestId('warmup-history-dominant-failure-slot');
    const reason = within(slot).getByText('Auth rejected (401) (1×)');
    expect(reason.classList.contains('text-danger-text')).toBe(true);
  });

  test('a transient-only dominant failure stays warn', () => {
    setAttemptsQuery([
      makeAttempt({
        id: 'trans-1',
        status: 'transient_failure',
        reason: 'upstream_5xx',
      }),
    ]);
    renderDrawer();

    const slot = screen.getByTestId('warmup-history-dominant-failure-slot');
    const reason = within(slot).getByText('Upstream 5xx (1×)');
    expect(reason.classList.contains('text-warn-text')).toBe(true);
  });
});
