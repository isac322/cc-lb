import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from '@testing-library/react';
import { useState } from 'react';
import { beforeEach, describe, expect, it, type Mock, vi } from 'vitest';

vi.mock('sonner', () => ({
  toast: {
    error: vi.fn(),
    success: vi.fn(),
  },
}));

import { toast } from 'sonner';
import { ApiError } from '../../../../lib/api';
import * as queries from '../../../../lib/queries';
import { CacheKeepaliveSettingsDrawer } from '../CacheKeepaliveSettingsDrawer';

const mockPrincipal: queries.Principal = {
  id: 'p-123',
  name: 'Test Principal',
  kind: 'machine',
  enabled: true,
  revision: 42,
  allowed_models: [],
  allowed_upstreams: [],
  default_limits: [],
  cache_keepalive: {
    enabled: true,
    refresh_lead_time_5m_secs: 45,
    refresh_lead_time_1h_secs: 400,
    max_refreshes_per_session: 10,
    max_total_duration_secs: 7200,
    snapshot_max_bytes: 256000,
    classifier: {
      extra_wait_for_user_tools: ['custom_tool'],
      treat_end_turn_as_ambiguous: true,
    },
  },
};

const mockPrincipalEmpty: queries.Principal = {
  ...mockPrincipal,
  cache_keepalive: null,
};

function renderWithProviders(ui: React.ReactElement) {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  return render(
    <QueryClientProvider client={queryClient}>{ui}</QueryClientProvider>,
  );
}

describe('CacheKeepaliveSettingsDrawer', () => {
  let mutateMock: Mock;

  beforeEach(() => {
    mutateMock = vi.fn();
    vi.mocked(toast.error).mockReset();
    vi.mocked(toast.success).mockReset();
    vi.spyOn(queries, 'useUpdatePrincipalCacheKeepalive').mockReturnValue({
      mutate: mutateMock,
      isPending: false,
    } as unknown as ReturnType<
      typeof queries.useUpdatePrincipalCacheKeepalive
    >);
    vi.spyOn(queries, 'usePrincipalWritePending').mockReturnValue(0);
  });

  it('renders exact field labels, helper text, and buttons', () => {
    renderWithProviders(
      <CacheKeepaliveSettingsDrawer
        open={true}
        onOpenChange={() => {}}
        principal={mockPrincipal}
      />,
    );

    expect(screen.getAllByText('Cache keepalive settings')[0]).toBeDefined();
    expect(screen.getAllByText('Test Principal')[0]).toBeDefined();
    expect(screen.getAllByLabelText('Close settings')[0]).toBeDefined();

    expect(screen.getAllByText('Enabled')[0]).toBeDefined();
    expect(screen.getAllByText('Renewal lead time · 5m TTL')[0]).toBeDefined();
    expect(
      screen.getAllByText('→ renews 30s before the 5m cache expires')[0],
    ).toBeDefined();
    expect(screen.getAllByText('Renewal lead time · 1h TTL')[0]).toBeDefined();
    expect(screen.getAllByText('Max renewals per session')[0]).toBeDefined();
    expect(screen.getAllByText('Max total duration')[0]).toBeDefined();
    expect(screen.getAllByText('= 4h')[0]).toBeDefined();
    expect(screen.getAllByText('Snapshot max bytes')[0]).toBeDefined();
    expect(screen.getAllByText('= 512 KiB')[0]).toBeDefined();
    expect(screen.getAllByText('extra_wait_for_user_tools')[0]).toBeDefined();
    expect(screen.getAllByPlaceholderText('Add tool...')[0]).toBeDefined();
    expect(screen.getAllByText('treat end_turn as ambiguous')[0]).toBeDefined();
    expect(screen.getAllByText('LLM Judge')[0]).toBeDefined();
    expect(
      screen.getAllByText('Reserved for a future release')[0],
    ).toBeDefined();

    expect(screen.getAllByText('Reset')[0]).toBeDefined();
    expect(screen.getAllByText('Save changes')[0]).toBeDefined();

    // Check max-w-md
    const popup = screen.getByTestId('cache-keepalive-settings-drawer');
    expect(popup.className).toContain('max-w-md');
  });

  it('toggles have role switch and aria-checked', () => {
    renderWithProviders(
      <CacheKeepaliveSettingsDrawer
        open={true}
        onOpenChange={() => {}}
        principal={mockPrincipal}
      />,
    );

    const switches = screen.getAllByRole('switch');
    const enabledSwitch = switches[0];
    expect(enabledSwitch.getAttribute('aria-checked')).toBe('true');

    const treatAmbiguousSwitch = switches[1];
    expect(treatAmbiguousSwitch.getAttribute('aria-checked')).toBe('true');
  });

  it('populates fields from principal config', () => {
    renderWithProviders(
      <CacheKeepaliveSettingsDrawer
        open={true}
        onOpenChange={() => {}}
        principal={mockPrincipal}
      />,
    );

    const inputs = screen.getAllByRole('spinbutton') as HTMLInputElement[];
    expect(inputs[0].value).toBe('45'); // lead5m
    expect(inputs[1].value).toBe('400'); // lead1h
    expect(inputs[2].value).toBe('10'); // maxRenewals
    expect(inputs[3].value).toBe('7200'); // maxDuration
    expect(inputs[4].value).toBe('256000'); // snapshotBytes

    expect(screen.getAllByText('custom_tool')[0]).toBeDefined();
  });

  it('populates default values when config is null', () => {
    renderWithProviders(
      <CacheKeepaliveSettingsDrawer
        open={true}
        onOpenChange={() => {}}
        principal={mockPrincipalEmpty}
      />,
    );

    const inputs = screen.getAllByRole('spinbutton') as HTMLInputElement[];
    expect(inputs[0].value).toBe('30');
    expect(inputs[1].value).toBe('300');
    expect(inputs[2].value).toBe('12');
    expect(inputs[3].value).toBe('14400');
    expect(inputs[4].value).toBe('524288');
  });

  it('calls update mutation on save', async () => {
    renderWithProviders(
      <CacheKeepaliveSettingsDrawer
        open={true}
        onOpenChange={() => {}}
        principal={mockPrincipal}
      />,
    );

    const inputs = screen.getAllByRole('spinbutton') as HTMLInputElement[];
    fireEvent.change(inputs[0], { target: { value: '50' } });

    const saveButtons = screen.getAllByText('Save changes');
    fireEvent.click(saveButtons[saveButtons.length - 1]);

    expect(mutateMock).toHaveBeenCalledWith(
      {
        id: 'p-123',
        expected_revision: 42,
        cache_keepalive: {
          enabled: true,
          refresh_lead_time_5m_secs: 50,
          refresh_lead_time_1h_secs: 400,
          max_refreshes_per_session: 10,
          max_total_duration_secs: 7200,
          snapshot_max_bytes: 256000,
          classifier: {
            extra_wait_for_user_tools: ['custom_tool'],
            treat_end_turn_as_ambiguous: true,
          },
        },
      },
      expect.any(Object),
    );
  });

  it('keeps a same-principal draft and its opening revision until the drawer is reopened', () => {
    const latestPrincipal: queries.Principal = {
      ...mockPrincipal,
      revision: 43,
      cache_keepalive: {
        ...mockPrincipal.cache_keepalive!,
        refresh_lead_time_5m_secs: 30,
      },
    };
    const queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });
    const drawer = (open: boolean, principal: queries.Principal) => (
      <QueryClientProvider client={queryClient}>
        <CacheKeepaliveSettingsDrawer
          open={open}
          onOpenChange={() => {}}
          principal={principal}
        />
      </QueryClientProvider>
    );
    const { rerender } = render(drawer(true, mockPrincipal));

    const initialInputs = screen.getAllByRole(
      'spinbutton',
    ) as HTMLInputElement[];
    fireEvent.change(initialInputs[0], { target: { value: '77' } });

    rerender(drawer(true, latestPrincipal));

    const liveInputs = screen.getAllByRole('spinbutton') as HTMLInputElement[];
    expect(liveInputs[0].value).toBe('77');
    fireEvent.click(screen.getByRole('button', { name: 'Save changes' }));
    expect(mutateMock).toHaveBeenLastCalledWith(
      expect.objectContaining({
        id: 'p-123',
        expected_revision: 42,
        cache_keepalive: expect.objectContaining({
          refresh_lead_time_5m_secs: 77,
        }),
      }),
      expect.any(Object),
    );

    mutateMock.mockClear();
    rerender(drawer(false, latestPrincipal));
    rerender(drawer(true, latestPrincipal));

    const reopenedInputs = screen.getAllByRole(
      'spinbutton',
    ) as HTMLInputElement[];
    expect(reopenedInputs[0].value).toBe('30');
    fireEvent.click(screen.getByRole('button', { name: 'Save changes' }));
    expect(mutateMock).toHaveBeenLastCalledWith(
      expect.objectContaining({
        id: 'p-123',
        expected_revision: 43,
        cache_keepalive: expect.objectContaining({
          refresh_lead_time_5m_secs: 30,
        }),
      }),
      expect.any(Object),
    );
  });

  it('starts a fresh draft and revision when the open principal identity changes', () => {
    const otherPrincipal: queries.Principal = {
      ...mockPrincipal,
      id: 'p-456',
      name: 'Other Principal',
      revision: 9,
      cache_keepalive: {
        ...mockPrincipal.cache_keepalive!,
        refresh_lead_time_5m_secs: 88,
      },
    };
    const queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });
    const { rerender } = render(
      <QueryClientProvider client={queryClient}>
        <CacheKeepaliveSettingsDrawer
          open={true}
          onOpenChange={() => {}}
          principal={mockPrincipal}
        />
      </QueryClientProvider>,
    );
    const initialInputs = screen.getAllByRole(
      'spinbutton',
    ) as HTMLInputElement[];
    fireEvent.change(initialInputs[0], { target: { value: '77' } });

    rerender(
      <QueryClientProvider client={queryClient}>
        <CacheKeepaliveSettingsDrawer
          open={true}
          onOpenChange={() => {}}
          principal={otherPrincipal}
        />
      </QueryClientProvider>,
    );

    const otherInputs = screen.getAllByRole('spinbutton') as HTMLInputElement[];
    expect(otherInputs[0].value).toBe('88');
    fireEvent.click(screen.getByRole('button', { name: 'Save changes' }));
    expect(mutateMock).toHaveBeenLastCalledWith(
      expect.objectContaining({
        id: 'p-456',
        expected_revision: 9,
        cache_keepalive: expect.objectContaining({
          refresh_lead_time_5m_secs: 88,
        }),
      }),
      expect.any(Object),
    );
  });

  it('resets fields on reset click', () => {
    renderWithProviders(
      <CacheKeepaliveSettingsDrawer
        open={true}
        onOpenChange={() => {}}
        principal={mockPrincipal}
      />,
    );

    const inputs = screen.getAllByRole('spinbutton') as HTMLInputElement[];
    fireEvent.change(inputs[0], { target: { value: '999' } });
    expect(inputs[0].value).toBe('999');

    const resetButtons = screen.getAllByText('Reset');
    fireEvent.click(resetButtons[resetButtons.length - 1]);
    expect(inputs[0].value).toBe('45');
  });

  it('handles the backend storage conflict without discarding the draft', () => {
    const onOpenChange = vi.fn();
    mutateMock.mockImplementation(
      (_vars: unknown, options: { onError: (err: Error) => void }) => {
        options.onError(
          new ApiError(
            409,
            'storage_conflict',
            {
              error: 'storage_conflict',
              message: 'principal revision changed',
            },
            'principal revision changed',
          ),
        );
      },
    );

    renderWithProviders(
      <CacheKeepaliveSettingsDrawer
        open={true}
        onOpenChange={onOpenChange}
        principal={mockPrincipal}
      />,
    );

    const inputs = screen.getAllByRole('spinbutton') as HTMLInputElement[];
    fireEvent.change(inputs[0], { target: { value: '77' } });
    fireEvent.click(screen.getByRole('button', { name: 'Save changes' }));

    expect(mutateMock).toHaveBeenCalledWith(
      expect.objectContaining({
        expected_revision: 42,
        cache_keepalive: expect.objectContaining({
          refresh_lead_time_5m_secs: 77,
        }),
      }),
      expect.any(Object),
    );
    expect(inputs[0].value).toBe('77');
    expect(onOpenChange).not.toHaveBeenCalled();
    expect(toast.error).toHaveBeenCalledWith(
      'Principal was modified by another user. Please refresh and try again.',
    );
  });

  it.each([
    [412, 'precondition_failed'],
    [409, 'stale_revision'],
  ])('keeps legacy conflict handling for HTTP %i %s', (status, code) => {
    mutateMock.mockImplementation(
      (_vars: unknown, options: { onError: (err: Error) => void }) => {
        options.onError(new ApiError(status, code, { error: code }, code));
      },
    );

    renderWithProviders(
      <CacheKeepaliveSettingsDrawer
        open={true}
        onOpenChange={() => {}}
        principal={mockPrincipal}
      />,
    );

    fireEvent.click(screen.getByRole('button', { name: 'Save changes' }));

    expect(toast.error).toHaveBeenCalledWith(
      'Principal was modified by another user. Please refresh and try again.',
    );
  });

  it('surfaces an unrelated 409 error instead of treating every conflict as stale', () => {
    mutateMock.mockImplementation(
      (_vars: unknown, options: { onError: (err: Error) => void }) => {
        options.onError(
          new ApiError(
            409,
            'principal_in_use',
            { error: 'principal_in_use', message: 'Principal is in use' },
            'Principal is in use',
          ),
        );
      },
    );

    renderWithProviders(
      <CacheKeepaliveSettingsDrawer
        open={true}
        onOpenChange={() => {}}
        principal={mockPrincipal}
      />,
    );

    fireEvent.click(screen.getByRole('button', { name: 'Save changes' }));

    expect(toast.error).toHaveBeenCalledWith('Principal is in use');
  });

  it('validates numeric inputs before saving', async () => {
    renderWithProviders(
      <CacheKeepaliveSettingsDrawer
        open={true}
        onOpenChange={() => {}}
        principal={mockPrincipal}
      />,
    );

    const inputs = screen.getAllByRole('spinbutton') as HTMLInputElement[];
    fireEvent.change(inputs[0], { target: { value: '-5' } });

    const saveButtons = screen.getAllByText('Save changes');
    fireEvent.click(saveButtons[saveButtons.length - 1]);

    expect(mutateMock).not.toHaveBeenCalled();
  });

  it('locks every control and retains the drawer while saving', () => {
    cleanup();
    const onOpenChange = vi.fn();
    vi.mocked(queries.useUpdatePrincipalCacheKeepalive).mockReturnValue({
      mutate: mutateMock,
      isPending: true,
    } as never);

    renderWithProviders(
      <CacheKeepaliveSettingsDrawer
        open={true}
        onOpenChange={onOpenChange}
        principal={mockPrincipal}
      />,
    );

    const drawer = screen.getByTestId('cache-keepalive-settings-drawer');
    const fieldset = screen.getByTestId('cache-keepalive-settings-form');
    expect(fieldset.hasAttribute('disabled')).toBe(true);
    expect(fieldset.getAttribute('aria-busy')).toBe('true');
    for (const control of [
      ...screen.getAllByRole('spinbutton'),
      ...screen.getAllByRole('switch'),
      screen.getByPlaceholderText('Add tool...'),
      screen.getByRole('button', { name: 'Remove custom_tool' }),
    ]) {
      expect(control.matches(':disabled')).toBe(true);
    }

    const close = screen.getByRole('button', { name: 'Close settings' });
    const reset = screen.getByRole('button', { name: 'Reset' });
    const saving = screen.getByRole('button', { name: 'Saving...' });
    expect(close.hasAttribute('disabled')).toBe(true);
    expect(close.getAttribute('aria-disabled')).toBe('true');
    expect(reset.hasAttribute('disabled')).toBe(true);
    expect(saving.hasAttribute('disabled')).toBe(true);
    expect(saving.getAttribute('aria-busy')).toBe('true');
    expect(saving.querySelector('svg.animate-spin')).not.toBeNull();

    fireEvent.click(close);
    fireEvent.keyDown(drawer, { key: 'Escape', code: 'Escape' });

    expect(onOpenChange).not.toHaveBeenCalled();
    expect(screen.getByTestId('cache-keepalive-settings-drawer')).toBeDefined();
  });

  it('honors the same-principal write lock without claiming save progress', () => {
    const onOpenChange = vi.fn();
    cleanup();
    vi.mocked(queries.usePrincipalWritePending).mockReturnValue(1);

    renderWithProviders(
      <CacheKeepaliveSettingsDrawer
        open={true}
        onOpenChange={onOpenChange}
        principal={mockPrincipal}
      />,
    );

    expect(queries.usePrincipalWritePending).toHaveBeenCalledWith('p-123');

    const drawer = screen.getByTestId('cache-keepalive-settings-drawer');
    const fieldset = screen.getByTestId('cache-keepalive-settings-form');
    expect(fieldset.hasAttribute('disabled')).toBe(true);
    expect(fieldset.getAttribute('aria-busy')).toBe('false');
    expect(drawer.contains(fieldset)).toBe(true);

    const save = screen.getByRole('button', { name: 'Save changes' });
    expect(save.hasAttribute('disabled')).toBe(true);
    expect(save.getAttribute('aria-busy')).toBeNull();
    expect(
      screen.getByRole('button', { name: 'Reset' }).hasAttribute('disabled'),
    ).toBe(true);

    const close = screen.getByRole('button', { name: 'Close settings' });
    expect(close.hasAttribute('disabled')).toBe(false);
    fireEvent.click(close);
    expect(onOpenChange).toHaveBeenCalledWith(false);

    fireEvent.click(save);
    expect(mutateMock).not.toHaveBeenCalled();
  });

  it('handles open/close and focus return', async () => {
    const Wrapper = () => {
      const [open, setOpen] = useState(false);
      return (
        <QueryClientProvider
          client={
            new QueryClient({ defaultOptions: { queries: { retry: false } } })
          }
        >
          <button data-testid="opener" onClick={() => setOpen(true)}>
            Open
          </button>
          <CacheKeepaliveSettingsDrawer
            open={open}
            onOpenChange={setOpen}
            principal={mockPrincipal}
          />
        </QueryClientProvider>
      );
    };
    render(<Wrapper />);

    const opener = screen.getByTestId('opener');
    opener.focus();
    fireEvent.click(opener);

    const dialogs = await screen.findAllByTestId(
      'cache-keepalive-settings-drawer',
    );
    expect(dialogs.length).toBeGreaterThan(0);

    const closeBtns = screen.getAllByLabelText('Close settings');
    fireEvent.click(closeBtns[closeBtns.length - 1]);

    await waitFor(() => {
      expect(document.activeElement).toBe(opener);
    });
  });
});
