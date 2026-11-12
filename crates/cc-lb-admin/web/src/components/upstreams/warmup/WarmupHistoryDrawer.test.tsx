import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
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
    lane: 'primary',
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
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
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

describe('WarmupHistoryDrawer lanes', () => {
  function renderLaneDrawer(
    laneFilter: 'all' | queries.WarmupLane,
    onLaneFilterChange = vi.fn(),
  ) {
    render(
      <WarmupHistoryDrawer
        open={true}
        onOpenChange={() => {}}
        upstream={upstream}
        lanes={['primary', 'third_party']}
        laneFilter={laneFilter}
        onLaneFilterChange={onLaneFilterChange}
      />,
    );
    return onLaneFilterChange;
  }

  function lastAttemptsFilters() {
    return vi.mocked(queries.useWarmupAttempts).mock.calls.at(-1)?.[1];
  }

  test('a single-lane upstream has no model group filter and sends no lane', () => {
    renderDrawer();

    expect(
      screen.queryByRole('radiogroup', { name: 'Model group' }),
    ).toBeNull();
    expect(lastAttemptsFilters()?.lane ?? null).toBeNull();
    expect(screen.queryByTestId('warmup-attempt-lane')).toBeNull();
  });

  test('All lists every lane and captions each row with its lane', () => {
    setAttemptsQuery([
      makeAttempt({ id: 'p', lane: 'primary' }),
      makeAttempt({ id: 't', lane: 'third_party' }),
    ]);
    renderLaneDrawer('all');

    const group = screen.getByRole('radiogroup', { name: 'Model group' });
    expect(
      within(group)
        .getByRole('radio', { name: 'All' })
        .getAttribute('aria-checked'),
    ).toBe('true');
    expect(lastAttemptsFilters()?.lane ?? null).toBeNull();
    expect(
      screen.getAllByTestId('warmup-attempt-lane').map((el) => el.textContent),
    ).toEqual(['· Gemini', '· Claude·OSS']);
  });

  test('a selected lane is sent as the lane filter and drops the row caption', () => {
    setAttemptsQuery([makeAttempt({ id: 't', lane: 'third_party' })]);
    renderLaneDrawer('third_party');

    expect(lastAttemptsFilters()).toEqual(
      expect.objectContaining({ lane: 'third_party' }),
    );
    expect(screen.queryByTestId('warmup-attempt-lane')).toBeNull();
  });

  test('choosing a model group reports the new lane filter', () => {
    const onChange = renderLaneDrawer('all');

    fireEvent.click(screen.getByRole('radio', { name: 'Claude·OSS' }));

    expect(onChange).toHaveBeenCalledWith('third_party');
  });

  test('attempt detail names the model group', () => {
    setAttemptsQuery([makeAttempt({ id: 't', lane: 'third_party' })]);
    renderLaneDrawer('all');

    fireEvent.click(within(screen.getByRole('list')).getByRole('button'));

    expect(screen.getByTestId('warmup-attempt-detail-lane').textContent).toBe(
      'Claude and GPT-OSS models',
    );
  });

  test('a quota group skip reads as Model group not available', () => {
    setAttemptsQuery([
      makeAttempt({
        id: 's',
        lane: 'third_party',
        status: 'skipped',
        reason: 'quota_group_unavailable',
      }),
    ]);
    renderLaneDrawer('third_party');

    expect(
      within(screen.getByRole('list')).getByText('Model group not available'),
    ).toBeTruthy();
  });
});

describe('WarmupHistoryDrawer ranges', () => {
  test('filters rows and requests by range, with a stable cutoff until reopened', async () => {
    const now = 1_800_000_000;
    const clock = vi.spyOn(Date, 'now').mockReturnValue(now * 1000);
    const attempts = [
      makeAttempt({ id: 'recent', attempted_at_unix_secs: now - 60 }),
      makeAttempt({
        id: 'three-days-old',
        attempted_at_unix_secs: now - 3 * 86400,
        http_status: 201,
      }),
      makeAttempt({
        id: 'nine-days-old',
        attempted_at_unix_secs: now - 9 * 86400,
        http_status: 202,
      }),
    ];
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const url = new URL(
        input instanceof Request ? input.url : String(input),
        'http://localhost',
      );
      const since = url.searchParams.get('since_unix_secs');
      return new Response(
        JSON.stringify({
          attempts: attempts.filter(
            (attempt) =>
              since === null || attempt.attempted_at_unix_secs >= Number(since),
          ),
          next_cursor: null,
        }),
        { headers: { 'Content-Type': 'application/json' } },
      );
    });
    vi.stubGlobal('fetch', fetchMock);
    const actual = await vi.importActual<typeof queries>(
      '../../../lib/queries',
    );
    vi.mocked(queries.useWarmupAttempts).mockImplementation(
      actual.useWarmupAttempts,
    );
    const client = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });
    const drawer = (open: boolean) => (
      <QueryClientProvider client={client}>
        <WarmupHistoryDrawer
          open={open}
          onOpenChange={() => {}}
          upstream={upstream}
        />
      </QueryClientProvider>
    );
    const { rerender } = render(drawer(true));
    const range = screen.getByRole('radiogroup', { name: 'History range' });
    const requestedSince = () => {
      const input = fetchMock.mock.calls.at(-1)?.[0];
      return new URL(String(input), 'http://localhost').searchParams.get(
        'since_unix_secs',
      );
    };
    const rows = () => within(screen.getByRole('list'));

    await waitFor(() => expect(rows().getAllByRole('button')).toHaveLength(1));
    expect(requestedSince()).toBe(String(now - 86400));
    expect(rows().queryByText('201')).toBeNull();
    expect(rows().queryByText('202')).toBeNull();

    // Unrelated rerenders must not create a fresh query every second.
    clock.mockReturnValue((now + 120) * 1000);
    rerender(drawer(true));
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(
      vi.mocked(queries.useWarmupAttempts).mock.calls.at(-1)?.[1]
        ?.sinceUnixSecs,
    ).toBe(now - 86400);

    fireEvent.click(within(range).getByRole('radio', { name: '7d' }));
    await waitFor(() => expect(rows().getAllByRole('button')).toHaveLength(2));
    expect(requestedSince()).toBe(String(now + 120 - 604800));
    expect(rows().getByText('201')).toBeTruthy();
    expect(rows().queryByText('202')).toBeNull();
    expect(screen.getByText('2 attempts')).toBeTruthy();

    fireEvent.click(within(range).getByRole('radio', { name: 'All' }));
    await waitFor(() => expect(rows().getAllByRole('button')).toHaveLength(3));
    expect(requestedSince()).toBeNull();
    expect(rows().getByText('202')).toBeTruthy();

    fireEvent.click(rows().getAllByRole('button')[2]);
    fireEvent.click(within(range).getByRole('radio', { name: '24h' }));
    await waitFor(() => expect(rows().getAllByRole('button')).toHaveLength(1));
    expect(requestedSince()).toBe(String(now + 120 - 86400));
    expect(rows().queryByText('201')).toBeNull();
    expect(rows().queryByText('202')).toBeNull();
    expect(
      screen.queryByRole('button', { name: 'Close attempt detail' }),
    ).toBeNull();

    rerender(drawer(false));
    clock.mockReturnValue((now + 240) * 1000);
    rerender(drawer(true));
    await waitFor(() =>
      expect(requestedSince()).toBe(String(now + 240 - 86400)),
    );
  });

  test('keeps the existing empty state for a range with no attempts', () => {
    setAttemptsQuery([]);
    renderDrawer();

    fireEvent.click(screen.getByRole('radio', { name: '7d' }));

    expect(screen.getByText('No matching attempts')).toBeTruthy();
    expect(screen.getByText('Try a different filter.')).toBeTruthy();
    expect(screen.getByText('0 attempts')).toBeTruthy();
  });
});
