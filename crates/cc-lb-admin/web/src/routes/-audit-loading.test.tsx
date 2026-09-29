import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  within,
} from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import type React from 'react';
import { afterEach, beforeEach, expect, test, vi } from 'vitest';
import * as queries from '../lib/queries';
import { Route as AuditRoute } from './audit';

const routerMocks = vi.hoisted(() => ({
  navigate: vi.fn(),
}));

vi.mock('@tanstack/react-router', async () => {
  const actual = await vi.importActual<Record<string, unknown>>(
    '@tanstack/react-router',
  );
  return {
    ...actual,
    useNavigate: () => routerMocks.navigate,
  };
});

vi.mock('../lib/queries', async () => {
  const actual =
    await vi.importActual<Record<string, unknown>>('../lib/queries');
  return {
    ...actual,
    useAudit: vi.fn(),
    usePrincipalNameMap: vi.fn(),
    usePrincipals: vi.fn(),
    useUpstreamNameMap: vi.fn(),
  };
});

const AuditComponent = AuditRoute.options.component as React.ComponentType;

function renderAudit(
  search: {
    principal_id?: string;
    since?: number;
    until?: number;
    type?: string;
  } = {},
) {
  Object.assign(AuditRoute, { useSearch: () => search });
  return render(<AuditComponent />);
}

beforeEach(() => {
  vi.useFakeTimers({
    toFake: ['Date'],
    now: new Date('2026-06-18T00:00:01.000Z'),
  });
  vi.clearAllMocks();
  vi.mocked(queries.useAudit).mockReturnValue({
    data: undefined,
    isFetching: true,
    isLoading: true,
    isPending: true,
    refetch: vi.fn(),
  } as never);
  vi.mocked(queries.usePrincipals).mockReturnValue({
    data: { principals: [] },
    isFetching: false,
    isLoading: false,
    isPending: false,
  } as never);
  vi.mocked(queries.usePrincipalNameMap).mockReturnValue(new Map());
  vi.mocked(queries.useUpstreamNameMap).mockReturnValue(new Map());
});

afterEach(() => {
  cleanup();
  vi.useRealTimers();
});

function mockEntries(entries: unknown[]) {
  vi.mocked(queries.useAudit).mockReturnValue({
    data: { entries },
    isFetching: false,
    isLoading: false,
    isPending: false,
    refetch: vi.fn(),
  } as never);
}

function tableRows(): HTMLTableRowElement[] {
  const table = screen.getByRole('table') as HTMLTableElement;
  return Array.from(table.tBodies[0]?.rows ?? []).filter(
    (row) => row.cells.length > 1,
  );
}

function applyLastSearch(prev: Record<string, unknown>) {
  const call = routerMocks.navigate.mock.lastCall?.[0] as {
    search: (p: Record<string, unknown>) => Record<string, unknown>;
  };
  return call.search(prev);
}

test('audit loads admin entries with supported filters and no native select', () => {
  renderAudit();

  expect(queries.useAudit).toHaveBeenLastCalledWith({
    limit: '200',
    admin_only: 'true',
    principal_id: undefined,
    since: undefined,
    until: undefined,
  });
  expect(
    screen.getByTestId('audit-summary').querySelectorAll('.skeleton'),
  ).toHaveLength(1);
  expect(screen.getByLabelText('Range start')).toBeDefined();
  expect(screen.getByLabelText('Range end')).toBeDefined();
  expect(screen.getByRole('button', { name: 'Apply' })).toBeDefined();
  expect(document.querySelector('select')).toBeNull();
  expect(
    within(screen.getByRole('radiogroup', { name: 'Action type' }))
      .getByRole('radio', { name: 'Writes' })
      .getAttribute('aria-checked'),
  ).toBe('true');
});

test('audit sends principal and time filters and reset clears them together', () => {
  renderAudit({
    principal_id: 'principal-1',
    since: 1_718_665_200,
    until: 1_718_668_800,
  });

  expect(queries.useAudit).toHaveBeenLastCalledWith({
    limit: '200',
    admin_only: 'true',
    principal_id: 'principal-1',
    since: '1718665200',
    until: '1718668800',
  });
  expect(screen.getByRole('button', { name: 'Filters (2)' })).toBeDefined();

  fireEvent.click(screen.getByRole('button', { name: 'Reset filters' }));
  expect(routerMocks.navigate).toHaveBeenCalledWith({ search: {} });
});

test('audit offers the shared presets in order with All time last, and 6h sets since', () => {
  renderAudit();

  const group = screen.getByRole('radiogroup', { name: 'Time range' });
  expect(
    within(group)
      .getAllByRole('radio')
      .map((radio) => radio.textContent),
  ).toEqual(['1h', '6h', '24h', '7d', 'All time']);

  fireEvent.click(within(group).getByRole('radio', { name: '6h' }));
  const nowSecs = Math.floor(Date.now() / 1000);
  expect(applyLastSearch({})).toEqual({
    since: nowSecs - 6 * 3600,
    until: undefined,
    range: '6h',
  });
  // A preset re-scopes the trail in place: the reader keeps their place.
  expect(routerMocks.navigate).toHaveBeenLastCalledWith(
    expect.objectContaining({ resetScroll: false }),
  );
});

test('audit shows writes by default and one click switches to all actions', () => {
  mockEntries([
    {
      request_id: 'w1',
      ts: 1_750_204_800,
      status: 201,
      principal_id: '',
      route: '/admin/v1/principals',
      upstream: 'admin',
      admin_action: 'principal_key_issue',
    },
    {
      request_id: 'r1',
      ts: 1_750_204_801,
      status: 200,
      principal_id: '',
      route: '/admin/v1/config/draft',
      upstream: 'admin',
      admin_action: 'config_draft_read',
    },
    {
      request_id: 'a1',
      ts: 1_750_204_802,
      status: 401,
      principal_id: '',
      route: '/admin/v1/status',
      upstream: 'admin',
      admin_action: 'auth_rejected',
    },
  ]);

  const { rerender } = renderAudit();
  expect(tableRows().map((row) => row.cells[1]?.textContent)).toEqual([
    'Principal key issuedprincipal_key_issue',
  ]);
  expect(screen.getByTestId('audit-summary').textContent).toBe(
    'Showing 1 write of 3 loaded entries',
  );

  fireEvent.click(screen.getByRole('radio', { name: 'All' }));
  expect(applyLastSearch({ principal_id: 'p' })).toEqual({
    principal_id: 'p',
    type: 'all',
  });

  Object.assign(AuditRoute, { useSearch: () => ({ type: 'all' }) });
  rerender(<AuditComponent />);
  expect(tableRows()).toHaveLength(3);

  fireEvent.click(screen.getByRole('radio', { name: 'Writes' }));
  expect(applyLastSearch({ type: 'all' })).toEqual({ type: undefined });
});

test('audit rows open their entry from the keyboard and name entities', async () => {
  const upstreamId = '0348c6a6-374c-4df9-8cf4-374e8d26f70a';
  vi.mocked(queries.useUpstreamNameMap).mockReturnValue(
    new Map([
      [upstreamId, 'isac-max'],
      ['isac-max', 'isac-max'],
    ]),
  );
  mockEntries([
    {
      request_id: 'u1',
      ts: 1_750_204_800,
      status: 200,
      principal_id: '',
      route: `/admin/v1/upstreams/${upstreamId}`,
      upstream: 'isac-max',
      actor: 'static-token/local',
      admin_action: `upstream_update(id=${upstreamId}, fields=weight,enabled)`,
    },
  ]);
  const user = userEvent.setup();

  renderAudit();
  const [row] = tableRows();
  const time = row?.cells[0]?.querySelector('[title]');
  expect(time?.textContent).toBe('1y ago');
  expect(time?.getAttribute('title')).toContain('2025');
  expect(row?.cells[1]?.textContent).toBe(
    'Upstream updatedupstream_update· fields: weight, enabled',
  );
  expect(row?.cells[2]?.textContent).toBe('upstream isac-max');
  expect(
    within(row?.cells[2] as HTMLElement)
      .getByRole('link', { name: 'isac-max' })
      .getAttribute('href'),
  ).toBe(`/upstreams?selectedId=${upstreamId}`);
  expect(row?.cells[3]?.textContent).toBe('2 fields: weight, enabled');
  expect(row?.cells[5]?.textContent).toBe('200/upstreams/isac-max');

  const open = within(row as HTMLElement).getByRole('button', {
    name: /^Open Upstream updated entry from /,
  });
  open.focus();
  await user.keyboard('{Enter}');

  const dialog = screen.getByRole('dialog', { name: 'Upstream updated' });
  expect(dialog.textContent).toContain('upstream_update');
  expect(dialog.textContent).toContain(
    'This entry records which fields changed, not their previous or new values.',
  );
  expect(dialog.textContent).toContain(`isac-max (${upstreamId.slice(0, 8)})`);
});

test('audit details leave out the target and unbounded query limits', () => {
  const principalId = '7fb023cf-180a-4738-9cc8-79c0d6572182';
  vi.mocked(queries.usePrincipalNameMap).mockReturnValue(
    new Map([[principalId, 'local-traffic']]),
  );
  mockEntries([
    {
      request_id: 'r1',
      ts: 1_750_204_800,
      status: 200,
      principal_id: principalId,
      route: `/admin/v1/principals/${principalId}/keys`,
      upstream: 'admin',
      actor: 'static-token/local',
      admin_action: 'principal_keys_list',
      payload: {
        principal_id: principalId,
        status: 'active',
        since: 0,
        // u64::MAX as the JSON parser delivers it.
        until: 2 ** 64,
        actor_subject: null,
      },
    },
  ]);

  renderAudit({ type: 'all' });
  const [row] = tableRows();
  expect(row?.cells[2]?.textContent).toBe('principal local-traffic');
  expect(row?.cells[3]?.textContent).toBe('status: active · since: 0');
});

test('audit preserves duplicate request-id events across filter result transitions', () => {
  const duplicateRequestIds = [
    'admin-v1-principal-principal-1-1750204800',
    'admin-v1-principal-principal-1-1750204801',
    'admin-v1-principal-principal-1-1750204802',
    'admin-v1-principal-principal-1-1750204803',
  ] as const;
  const principalSpecs = [
    [duplicateRequestIds[0], 'principal.rename'],
    [duplicateRequestIds[0], 'principal.disable'],
    [duplicateRequestIds[1], 'principal.set_scopes'],
    [duplicateRequestIds[1], 'principal.set_scopes'],
    [duplicateRequestIds[2], 'credential.create'],
    [duplicateRequestIds[2], 'credential.delete'],
    [duplicateRequestIds[3], 'principal.set_limit'],
    [duplicateRequestIds[3], 'principal.set_limit'],
    ['admin-v1-principal-principal-1-1750204804', 'principal.create'],
    ['admin-v1-principal-principal-1-1750204805', 'principal.archive'],
    ['admin-v1-principal-principal-1-1750204806', 'principal.restore'],
    ['admin-v1-principal-principal-1-1750204807', 'principal.rotate_key'],
    ['admin-v1-principal-principal-1-1750204808', 'principal.delete'],
  ] as const;
  const principalRows = principalSpecs.map(
    ([request_id, admin_action], index) => ({
      request_id,
      ts_ms: 1_750_204_800_900 - Math.floor(index / 2),
      principal_id: 'principal-1',
      route: '/v1/principals',
      upstream: null,
      status: 200,
      actor: 'admin',
      admin_action,
      payload:
        request_id === duplicateRequestIds[1]
          ? { revision: index - 1 }
          : undefined,
    }),
  );
  const otherRows = Array.from({ length: 4 }, (_, index) => ({
    request_id: `admin-v1-principal-principal-2-${1_750_204_900 + index}`,
    ts_ms: 1_750_204_801_000 - index,
    principal_id: 'principal-2',
    route: '/v1/principals',
    upstream: null,
    status: 200,
    actor: 'admin',
    admin_action: `other.action.${index}`,
  }));
  const unfilteredRows = [...otherRows, ...principalRows];
  const timeRows = principalRows.slice(0, 2);

  vi.mocked(queries.useAudit).mockImplementation(
    ({ principal_id, since, until }) => {
      let entries = unfilteredRows;
      if (principal_id != null) entries = principalRows;
      if (since != null || until != null) entries = timeRows;
      return {
        data: { entries },
        isFetching: false,
        isLoading: false,
        isPending: false,
        refetch: vi.fn(),
      } as never;
    },
  );

  const expectAuditRows = (expectedActions: readonly string[]) => {
    const dataRows = tableRows();
    expect(screen.getByTestId('audit-summary').textContent).toContain(
      `Showing ${expectedActions.length} writes`,
    );
    expect(dataRows).toHaveLength(expectedActions.length);
    expect(
      dataRows.map(
        (row) => row.cells[1]?.querySelector('.font-mono')?.textContent,
      ),
    ).toEqual(expectedActions);
  };
  const openRow = (action: string, index = 0) => {
    const table = screen.getByRole('table');
    const cell = within(table).getAllByText(action)[index];
    fireEvent.click(cell?.closest('tr') as HTMLElement);
    return screen.getByRole('dialog');
  };

  const { rerender } = renderAudit();
  expectAuditRows(unfilteredRows.map((entry) => entry.admin_action));

  Object.assign(AuditRoute, {
    useSearch: () => ({ principal_id: 'principal-1' }),
  });
  rerender(<AuditComponent />);
  expect(queries.useAudit).toHaveBeenLastCalledWith({
    limit: '200',
    admin_only: 'true',
    principal_id: 'principal-1',
    since: undefined,
    until: undefined,
  });
  expectAuditRows(principalRows.map((entry) => entry.admin_action));

  for (const index of [0, 1]) {
    const dialog = openRow('principal.set_scopes', index);
    expect(dialog.textContent).toContain(`"revision": ${index + 1}`);
    fireEvent.click(within(dialog).getByRole('button', { name: 'Close' }));
  }

  Object.assign(AuditRoute, {
    useSearch: () => ({
      principal_id: 'principal-1',
      since: 1_750_204_800,
      until: 1_750_204_801,
    }),
  });
  rerender(<AuditComponent />);
  expect(queries.useAudit).toHaveBeenLastCalledWith({
    limit: '200',
    admin_only: 'true',
    principal_id: 'principal-1',
    since: '1750204800',
    until: '1750204801',
  });
  expectAuditRows(timeRows.map((entry) => entry.admin_action));

  for (const entry of timeRows) {
    const dialog = openRow(entry.admin_action);
    expect(dialog.textContent).toContain(
      `"admin_action": "${entry.admin_action}"`,
    );
    fireEvent.click(within(dialog).getByRole('button', { name: 'Close' }));
  }
});

test('manual audit refresh blocks same-tick duplicates and clears progress after settling', async () => {
  let resolveRefetch!: () => void;
  const refetchPromise = new Promise<void>((resolve) => {
    resolveRefetch = resolve;
  });
  const refetch = vi.fn(() => refetchPromise);
  vi.mocked(queries.useAudit).mockReturnValue({
    data: { entries: [] },
    isFetching: false,
    isLoading: false,
    isPending: false,
    refetch,
  } as never);

  renderAudit();
  const refresh = screen.getByRole('button', { name: 'Refresh' });
  act(() => {
    refresh.click();
    refresh.click();
  });

  expect(refetch).toHaveBeenCalledTimes(1);
  const refreshing = screen.getByRole('button', { name: 'Refreshing...' });
  expect(refreshing).toBe(screen.getByTestId('audit-refresh'));
  expect(refreshing.hasAttribute('disabled')).toBe(true);
  expect(refreshing.getAttribute('aria-busy')).toBe('true');
  expect(refreshing.querySelector('svg.animate-spin')).not.toBeNull();

  await act(async () => {
    resolveRefetch();
    await refetchPromise;
  });

  const settled = screen.getByRole('button', { name: 'Refresh' });
  expect(settled.hasAttribute('disabled')).toBe(false);
  expect(settled.hasAttribute('aria-busy')).toBe(false);
});

test('background audit fetching leaves manual Refresh idle and enabled', () => {
  vi.mocked(queries.useAudit).mockReturnValue({
    data: { entries: [] },
    isFetching: true,
    isLoading: false,
    isPending: false,
    refetch: vi.fn(),
  } as never);

  renderAudit();

  const refresh = screen.getByRole('button', { name: 'Refresh' });
  expect(refresh).toBe(screen.getByTestId('audit-refresh'));
  expect(refresh.hasAttribute('disabled')).toBe(false);
  expect(refresh.hasAttribute('aria-busy')).toBe(false);
  expect(refresh.textContent).toBe('Refresh');
});
