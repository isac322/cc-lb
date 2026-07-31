import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { cleanup, render, screen, within } from '@testing-library/react';
import type React from 'react';
import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest';
import * as queries from '../lib/queries';
import { Route as AuditRoute } from './audit';
import { Route as CredentialsRoute } from './credentials';

vi.mock('@tanstack/react-router', async () => {
  const actual = await vi.importActual<Record<string, unknown>>(
    '@tanstack/react-router',
  );
  return {
    ...actual,
    useNavigate: () => vi.fn(),
  };
});

vi.mock('../lib/queries', async () => {
  const actual =
    await vi.importActual<Record<string, unknown>>('../lib/queries');
  return {
    ...actual,
    useAudit: vi.fn(),
    useCredentials: vi.fn(),
    useOAuthStatus: vi.fn(),
    usePrincipalNameMap: vi.fn(),
    usePrincipals: vi.fn(),
    useUpstreamNameMap: vi.fn(),
    useUpstreams: vi.fn(),
  };
});

const AuditComponent = AuditRoute.options.component as React.ComponentType;
const CredentialsComponent = CredentialsRoute.options
  .component as React.ComponentType;

function renderAudit() {
  Object.assign(AuditRoute, { useSearch: () => ({}) });
  return render(<AuditComponent />);
}

function renderCredentials() {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  return render(
    <QueryClientProvider client={queryClient}>
      <CredentialsComponent />
    </QueryClientProvider>,
  );
}

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(queries.useAudit).mockReturnValue({
    data: undefined,
    isLoading: true,
    refetch: vi.fn(),
  } as never);
  vi.mocked(queries.usePrincipals).mockReturnValue({
    data: { principals: [] },
    isLoading: false,
  } as never);
  vi.mocked(queries.useUpstreams).mockReturnValue({
    data: { upstreams: [] },
    isLoading: false,
  } as never);
  vi.mocked(queries.usePrincipalNameMap).mockReturnValue(new Map());
  vi.mocked(queries.useUpstreamNameMap).mockReturnValue(new Map());
});

afterEach(cleanup);

test('audit loading reserves the resolved count width and keeps the nine-column table', () => {
  const { rerender } = renderAudit();

  const subtitle = screen
    .getByText(/admin actions · unfiltered/)
    .closest('[data-slot="section-subtitle"]');
  expect(subtitle?.querySelectorAll('.skeleton')).toHaveLength(1);
  expect(subtitle?.textContent).not.toContain('0 admin actions');
  expect(screen.getByTestId('audit-count-slot').className).toContain('min-w-5');
  const filterGrid = screen.getByLabelText('Principal').closest('div.grid');
  expect(filterGrid?.className).toContain('grid-cols-2');
  expect(screen.getByLabelText('Principal').className).toContain('w-full');
  expect(screen.getByLabelText('Upstream').className).toContain('w-full');
  expect(screen.getByLabelText('Route').className).toContain('w-full');
  expect(screen.getByLabelText('Status').className).toContain('w-full');

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
    isLoading: false,
    refetch: vi.fn(),
  } as never);
  rerender(<AuditComponent />);
  const resolvedCountSlot = screen.getByTestId('audit-count-slot');
  expect(resolvedCountSlot.className).toContain('min-w-5');
  expect(resolvedCountSlot.textContent).toBe('0');
});

describe('credential loading geometry', () => {
  function mockCredentialQueries({
    loading,
    populated = false,
  }: {
    loading: boolean;
    populated?: boolean;
  }) {
    vi.mocked(queries.useCredentials).mockReturnValue({
      data: populated
        ? {
            credentials: [
              {
                provider: 'anthropic',
                kind: 'api_key',
                identity: 'key@example.com',
                status: 'active',
                expires_at_unix_secs: null,
                cred_id: 'key-1',
              },
            ],
            observed: true,
          }
        : loading
          ? undefined
          : { credentials: [], observed: true },
      isLoading: loading,
    } as never);
    vi.mocked(queries.useOAuthStatus).mockReturnValue({
      data: populated
        ? {
            credentials: [
              {
                provider: 'anthropic',
                kind: 'oauth',
                identity: 'oauth@example.com',
                status: 'active',
                expires_at_unix_secs: 4_102_444_800,
                refresh_token_present: true,
                last_updated_unix_secs: 4_102_441_200,
                principal_id: 'principal-1',
                scopes: ['messages:write'],
                cred_id: 'oauth-1',
              },
            ],
            observed: true,
          }
        : loading
          ? undefined
          : { credentials: [], observed: true },
      isLoading: loading,
    } as never);
  }

  test('credential loading reserves summary, table, and one full-width OAuth card', () => {
    mockCredentialQueries({ loading: true });

    renderCredentials();

    const summaryValues = screen.getAllByTestId('credential-summary-value');
    expect(summaryValues).toHaveLength(4);
    for (const value of summaryValues) {
      expect(value.className).toContain('h-8');
      expect(value.querySelectorAll('.skeleton')).toHaveLength(1);
      expect(value.textContent).toBe('');
    }

    const apiKeySlot = screen.getByTestId('api-keys-slot');
    expect(apiKeySlot.className).toContain('min-h-[186px]');
    const apiKeyTable = within(apiKeySlot).getByRole(
      'table',
    ) as HTMLTableElement;
    expect(within(apiKeyTable).getAllByRole('columnheader')).toHaveLength(5);
    const apiKeyRows: HTMLTableRowElement[] = Array.from(
      apiKeyTable.tBodies[0]?.rows ?? [],
    );
    for (const row of apiKeyRows) {
      const cells: HTMLTableCellElement[] = Array.from(row.cells);
      expect(cells).toHaveLength(5);
      expect(cells.every((cell) => cell.colSpan === 1)).toBe(true);
    }
    const apiKeyWidths = ['w-28', 'w-64', 'w-28', 'w-36', 'w-52'];
    apiKeyWidths.forEach((width, index) => {
      expect(apiKeyRows[0]?.cells[index]?.className).toContain(width);
    });

    const oauthGrid = screen.getByTestId('oauth-grid');
    expect(oauthGrid.className).toContain('md:grid-cols-2');
    const oauthCard = screen.getByTestId('oauth-card-slot');
    expect(screen.getAllByTestId('oauth-card-slot')).toHaveLength(1);
    expect(oauthCard.className).toContain('min-h-[258px]');
    expect(oauthCard.className).toContain('md:col-span-2');
    expect(oauthCard.querySelector('.skeleton')).not.toBeNull();
  });

  test('credential empty state retains the table and OAuth geometry slots', () => {
    mockCredentialQueries({ loading: false });
    renderCredentials();

    const summaryValues = screen.getAllByTestId('credential-summary-value');
    expect(summaryValues.map((value) => value.textContent)).toEqual([
      '0',
      '0',
      '0',
      '0',
    ]);
    expect(screen.getByTestId('api-keys-slot').className).toContain(
      'min-h-[186px]',
    );
    expect(screen.getByText('No API keys')).toBeDefined();

    const oauthGrid = screen.getByTestId('oauth-grid');
    expect(oauthGrid.className).toContain('md:grid-cols-2');
    const oauthCard = screen.getByTestId('oauth-card-slot');
    expect(oauthCard.className).toContain('min-h-[258px]');
    expect(oauthCard.className).toContain('md:col-span-2');
    expect(screen.getByText('No OAuth tokens')).toBeDefined();
  });

  test('credential loaded state reuses the reserved table and OAuth card geometry', () => {
    mockCredentialQueries({ loading: false, populated: true });
    renderCredentials();

    const apiKeySlot = screen.getByTestId('api-keys-slot');
    expect(apiKeySlot.className).toContain('min-h-[186px]');
    const apiKeyTable = within(apiKeySlot).getByRole(
      'table',
    ) as HTMLTableElement;
    expect(apiKeyTable.tBodies[0]?.rows).toHaveLength(1);
    expect(apiKeyTable.tBodies[0]?.rows[0]?.cells).toHaveLength(5);

    const oauthGrid = screen.getByTestId('oauth-grid');
    expect(oauthGrid.className).toContain('md:grid-cols-2');
    const oauthCard = screen.getByTestId('oauth-card-slot');
    expect(oauthCard.className).toContain('min-h-[258px]');
    expect(oauthCard.className).toContain('md:col-span-2');
    expect(within(oauthCard).getByText('anthropic')).toBeDefined();
  });
});
