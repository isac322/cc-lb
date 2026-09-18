import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  within,
} from '@testing-library/react';
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
  search: { principal_id?: string; since?: number; until?: number } = {},
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

test('audit loading reserves the resolved count width and keeps the nine-column table', () => {
  const { rerender } = renderAudit();

  const subtitle = screen
    .getByText(/admin actions · unfiltered/)
    .closest('[data-slot="section-subtitle"]');
  expect(subtitle?.querySelectorAll('.skeleton')).toHaveLength(1);
  expect(subtitle?.textContent).not.toContain('0 admin actions');
  expect(screen.getByTestId('audit-count-slot').className).toContain('min-w-5');
  expect(screen.getByLabelText('Principal').className).toContain('w-full');
  expect(screen.getByLabelText('Range start')).toBeDefined();
  expect(screen.getByLabelText('Range end')).toBeDefined();
  expect(screen.getByRole('button', { name: 'Apply' })).toBeDefined();
  expect(screen.queryByLabelText('Upstream')).toBeNull();
  expect(screen.queryByLabelText('Route')).toBeNull();
  expect(screen.queryByLabelText('Status')).toBeNull();
  expect(queries.useAudit).toHaveBeenLastCalledWith({
    limit: '200',
    admin_only: 'true',
    principal_id: undefined,
    since: undefined,
    until: undefined,
  });

  const table = screen.getByRole('table') as HTMLTableElement;
  expect(table.className).toContain('table-fixed');
  expect(within(table).getAllByRole('columnheader')).toHaveLength(9);

  const rows: HTMLTableRowElement[] = Array.from(table.tBodies[0]?.rows ?? []);
  expect(rows).toHaveLength(5);
  for (const row of rows) {
    const cells: HTMLTableCellElement[] = Array.from(row.cells);
    expect(cells).toHaveLength(9);
    expect(cells.every((cell) => cell.colSpan === 1)).toBe(true);
  }
  const auditWidths = [
    'w-36',
    'w-40',
    'w-36',
    'w-40',
    'w-44',
    'w-72',
    'w-40',
    'w-20',
    'w-20',
  ];
  auditWidths.forEach((width, index) => {
    expect(rows[0]?.cells[index]?.className).toContain(width);
  });

  vi.mocked(queries.useAudit).mockReturnValue({
    data: { entries: [] },
    isFetching: false,
    isLoading: false,
    isPending: false,
    refetch: vi.fn(),
  } as never);
  rerender(<AuditComponent />);
  const resolvedCountSlot = screen.getByTestId('audit-count-slot');
  expect(resolvedCountSlot.className).toContain('min-w-5');
  expect(resolvedCountSlot.textContent).toBe('0');
});

test('audit sends only supported principal and time filters and clears them together', () => {
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
  expect(screen.getByText(/3 filters active/)).toBeDefined();
  expect(screen.queryByLabelText('Upstream')).toBeNull();
  expect(screen.queryByLabelText('Route')).toBeNull();
  expect(screen.queryByLabelText('Status')).toBeNull();

  fireEvent.click(screen.getByRole('button', { name: 'Clear' }));
  expect(routerMocks.navigate).toHaveBeenCalledWith({ search: {} });
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
      kind: 'admin',
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
    kind: 'admin',
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
    const table = screen.getByRole('table') as HTMLTableElement;
    const dataRows: HTMLTableRowElement[] = Array.from(
      table.tBodies[0]?.rows ?? [],
    ).filter((row) => row.cells.length === 9);
    expect(screen.getByTestId('audit-count-slot').textContent).toBe(
      String(expectedActions.length),
    );
    expect(dataRows).toHaveLength(expectedActions.length);
    expect(dataRows.map((row) => row.cells[5]?.textContent)).toEqual(
      expectedActions,
    );
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

  const repeatedActionCells = screen.getAllByText('principal.set_scopes');
  expect(repeatedActionCells).toHaveLength(2);
  repeatedActionCells.forEach((cell, index) => {
    fireEvent.click(cell.closest('tr')!);
    const dialog = screen.getByRole('dialog', {
      name: `Audit entry ${duplicateRequestIds[1]}`,
    });
    expect(dialog.textContent).toContain(`"revision": ${index + 1}`);
    fireEvent.click(within(dialog).getByRole('button', { name: 'Close' }));
  });

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
    fireEvent.click(screen.getByText(entry.admin_action).closest('tr')!);
    const dialog = screen.getByRole('dialog', {
      name: `Audit entry ${duplicateRequestIds[0]}`,
    });
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
