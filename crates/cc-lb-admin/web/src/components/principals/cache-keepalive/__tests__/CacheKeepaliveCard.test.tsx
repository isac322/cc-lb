import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

vi.mock('sonner', () => ({
  toast: {
    error: vi.fn(),
    success: vi.fn(),
  },
}));

import { toast } from 'sonner';
import { ApiError } from '../../../../lib/api';
import * as queries from '../../../../lib/queries';
import { cacheKeepaliveAnimationContract } from '../__fixtures__/cacheKeepaliveContract';
import { CacheKeepaliveCard } from '../CacheKeepaliveCard';

const mockPrincipal: queries.Principal = {
  id: 'p-123',
  name: 'Test Principal',
  kind: 'machine',
  enabled: true,
  revision: 42,
  allowed_models: [],
  allowed_upstreams: [],
  default_limits: [],
  prompt_cache_mode: 'preserve_client',
  cache_keepalive: {
    enabled: true,
    refresh_lead_time_5m_secs: 45,
    refresh_lead_time_1h_secs: 400,
    max_refreshes_per_session: 10,
    max_total_duration_secs: 7200,
    snapshot_max_bytes: 256000,
    classifier: {
      extra_wait_for_user_tools: [],
      treat_end_turn_as_ambiguous: true,
    },
  },
};

function renderWithProviders(ui: React.ReactElement) {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  return render(
    <QueryClientProvider client={queryClient}>{ui}</QueryClientProvider>,
  );
}

describe('CacheKeepaliveCard', () => {
  afterEach(() => {
    cleanup();
    document.body.innerHTML = '';
    window.PAUSE_ANIMATIONS = false;
  });

  beforeEach(() => {
    vi.spyOn(queries, 'useUpdatePrincipalCacheKeepalive').mockReturnValue({
      mutate: vi.fn(),
      isPending: false,
    } as unknown as ReturnType<
      typeof queries.useUpdatePrincipalCacheKeepalive
    >);
    vi.spyOn(queries, 'useUpdatePrincipalPromptCacheMode').mockReturnValue({
      mutate: vi.fn(),
      isPending: false,
    } as unknown as ReturnType<
      typeof queries.useUpdatePrincipalPromptCacheMode
    >);
    vi.spyOn(queries, 'usePrincipalWritePending').mockReturnValue(0);
    vi.spyOn(queries, 'useCacheKeepaliveSummary').mockReturnValue({
      data: {
        renewing_now: 1,
        sessions_last_5m: 2,
        renewals_fired: 3,
        cost_saved: 4.56,
      },
    } as unknown as ReturnType<typeof queries.useCacheKeepaliveSummary>);
  });

  it('renders four stable metric skeletons while the summary is loading', () => {
    vi.spyOn(queries, 'useCacheKeepaliveSummary').mockReturnValue({
      data: undefined,
      isLoading: true,
    } as never);

    renderWithProviders(<CacheKeepaliveCard principal={mockPrincipal} />);

    const card = screen.getByTestId('cache-keepalive-card');
    const valueLines = card.querySelectorAll(
      '[data-testid="cache-keepalive-metric-value"]',
    );

    expect(valueLines).toHaveLength(4);
    expect(card.querySelectorAll('.skeleton')).toHaveLength(4);
    valueLines.forEach((line) => {
      expect(line.className).toContain('h-9');
      expect(line.querySelectorAll('.skeleton')).toHaveLength(1);
    });
    expect(screen.queryAllByText('0')).toHaveLength(0);
    expect(screen.queryByText('$0.00')).toBeNull();
  });

  it('renders exact card labels, subcaptions, caption, and tooltip text', async () => {
    vi.spyOn(queries, 'useCacheKeepaliveSummary').mockReturnValue({
      data: {
        renewing_now: 1000,
        sessions_last_5m: 2000,
        renewals_fired: 3182,
        cost_saved: 4.56,
      },
    } as unknown as ReturnType<typeof queries.useCacheKeepaliveSummary>);

    renderWithProviders(<CacheKeepaliveCard principal={mockPrincipal} />);

    expect(screen.getByText('Cache keepalive')).toBeDefined();

    expect(
      screen.getByText('Renews the prompt-cache TTL during idle gaps.'),
    ).toBeDefined();

    expect(screen.getByText('Renewing now')).toBeDefined();
    expect(screen.getByText('scheduled or mid-renewal')).toBeDefined();
    expect(screen.getByText('1,000')).toBeDefined();

    expect(screen.getByText('Sessions (last 5m)')).toBeDefined();
    expect(screen.getByText('seen in last 5 min')).toBeDefined();
    expect(screen.getByText('2,000')).toBeDefined();

    expect(screen.getByText('Renewals fired')).toBeDefined();
    expect(screen.getByText('all-time')).toBeDefined();
    expect(screen.getByText('3,182')).toBeDefined();

    expect(screen.getByText('Cost saved')).toBeDefined();
    expect(screen.getByText('net, after renewal spend')).toBeDefined();

    expect(screen.getByText('$4.56')).toBeDefined();

    const help = screen.getByLabelText('Cache keepalive help');
    expect(help).toBeDefined();
    fireEvent.focus(help);

    await waitFor(() => {
      expect(
        screen.getByText(
          'Keeps the Anthropic prompt cache warm by renewing its TTL — fires a tiny synthetic request just before the prompt cache expires so the next real request still hits a warm cache.',
        ),
      ).toBeDefined();
    });
  });

  it('switch has role switch and shows its state as visible text', () => {
    renderWithProviders(<CacheKeepaliveCard principal={mockPrincipal} />);

    const toggle = screen.getByRole('switch', {
      name: 'Toggle cache keepalive',
    }) as HTMLInputElement;
    expect(toggle.checked).toBe(true);
    expect(toggle.closest('label')?.textContent).toBe('Enabled');
  });

  it('replaces the metric row with one line when keepalive is off', () => {
    renderWithProviders(
      <CacheKeepaliveCard
        principal={{
          ...mockPrincipal,
          cache_keepalive: mockPrincipal.cache_keepalive && {
            ...mockPrincipal.cache_keepalive,
            enabled: false,
          },
        }}
      />,
    );

    const toggle = screen.getByRole('switch', {
      name: 'Toggle cache keepalive',
    }) as HTMLInputElement;
    expect(toggle.checked).toBe(false);
    expect(toggle.closest('label')?.textContent).toBe('Disabled');
    expect(
      screen.getByText(
        'Off. Turn on to renew the prompt-cache TTL for idle sessions.',
      ),
    ).toBeDefined();
    expect(screen.queryByText('Renewing now')).toBeNull();
    expect(screen.queryByText('$4.56')).toBeNull();
    expect(screen.getByRole('button', { name: 'Sessions' })).toBeDefined();
    expect(screen.getByRole('button', { name: 'Settings' })).toBeDefined();
  });

  it('locks the toggle during another write to the same principal', () => {
    const mutate = vi.fn();
    vi.mocked(queries.usePrincipalWritePending).mockReturnValue(1);
    vi.mocked(queries.useUpdatePrincipalCacheKeepalive).mockReturnValue({
      mutate,
      isPending: false,
    } as never);

    renderWithProviders(<CacheKeepaliveCard principal={mockPrincipal} />);

    expect(queries.usePrincipalWritePending).toHaveBeenCalledWith('p-123');

    const toggle = screen.getByTestId('cache-keepalive-switch');
    expect(toggle.hasAttribute('disabled')).toBe(true);
    expect(toggle.getAttribute('aria-busy')).toBeNull();
    fireEvent.click(toggle);
    expect(mutate).not.toHaveBeenCalled();
  });

  it('Given one-hour cache mode, When rendered, Then keepalive controls read as dormant', () => {
    renderWithProviders(
      <CacheKeepaliveCard
        principal={{
          ...mockPrincipal,
          prompt_cache_mode: 'one_hour_no_keepalive',
        }}
      />,
    );

    const modeToggle = screen.getByTestId(
      'prompt-cache-mode-switch',
    ) as HTMLInputElement;
    expect(modeToggle.checked).toBe(true);

    const toggle = screen.getByTestId('cache-keepalive-switch');
    expect(toggle.hasAttribute('disabled')).toBe(true);
    expect(
      screen
        .getByRole('switch', { name: 'Toggle cache keepalive' })
        .closest('label')?.textContent,
    ).toBe('Disabled by one-hour cache mode');

    expect(screen.getByTestId('cache-keepalive-dormant')).toBeDefined();
    expect(
      screen.getByText(
        'Keep-alives are disabled by one-hour cache mode. Saved settings return when you switch it off.',
      ),
    ).toBeDefined();
    expect(
      screen.getByRole('button', { name: 'Settings' }).hasAttribute('disabled'),
    ).toBe(true);
    // Historical usage stays viewable.
    expect(
      screen.getByRole('button', { name: 'Sessions' }).hasAttribute('disabled'),
    ).toBe(false);
    expect(screen.queryByText('Renewing now')).toBeNull();
    expect(screen.queryByTestId('cache-keepalive-off')).toBeNull();
  });

  it('Given client policy, When the one-hour switch turns on, Then one atomic mode write carries the current revision', () => {
    const mutate = vi.fn();
    vi.mocked(queries.useUpdatePrincipalPromptCacheMode).mockReturnValue({
      mutate,
      isPending: false,
    } as never);

    renderWithProviders(<CacheKeepaliveCard principal={mockPrincipal} />);

    fireEvent.click(screen.getByTestId('prompt-cache-mode-switch'));

    expect(mutate).toHaveBeenCalledWith(
      {
        id: 'p-123',
        expected_revision: 42,
        prompt_cache_mode: 'one_hour_no_keepalive',
      },
      expect.any(Object),
    );
  });

  it('Given the one-hour label contains a help button, When the visible slider is clicked, Then the mode write fires', () => {
    const mutate = vi.fn();
    vi.mocked(queries.useUpdatePrincipalPromptCacheMode).mockReturnValue({
      mutate,
      isPending: false,
    } as never);
    renderWithProviders(<CacheKeepaliveCard principal={mockPrincipal} />);
    const toggle = screen.getByTestId('prompt-cache-mode-switch');
    const track = toggle.nextElementSibling;
    if (!track) throw new Error('missing visible switch track');

    fireEvent.click(
      screen.getByRole('button', { name: 'One-hour cache help' }),
    );
    expect(mutate).not.toHaveBeenCalled();
    fireEvent.click(track);
    expect(mutate).toHaveBeenCalledWith(
      {
        id: 'p-123',
        expected_revision: 42,
        prompt_cache_mode: 'one_hour_no_keepalive',
      },
      expect.any(Object),
    );
  });

  it('Given one-hour cache mode, When the switch turns off, Then client policy is restored through the same single write', () => {
    const mutate = vi.fn();
    vi.mocked(queries.useUpdatePrincipalPromptCacheMode).mockReturnValue({
      mutate,
      isPending: false,
    } as never);

    renderWithProviders(
      <CacheKeepaliveCard
        principal={{
          ...mockPrincipal,
          prompt_cache_mode: 'one_hour_no_keepalive',
        }}
      />,
    );

    fireEvent.click(screen.getByTestId('prompt-cache-mode-switch'));

    expect(mutate).toHaveBeenCalledWith(
      {
        id: 'p-123',
        expected_revision: 42,
        prompt_cache_mode: 'preserve_client',
      },
      expect.any(Object),
    );
  });

  it('Given a stale revision, When the mode write fails, Then the conflict is surfaced', () => {
    const mutate = vi.fn(
      (_vars: unknown, options: { onError: (error: Error) => void }) => {
        options.onError(
          new ApiError(
            412,
            'precondition_failed',
            { error: 'precondition_failed' },
            'precondition_failed',
          ),
        );
      },
    );
    vi.mocked(queries.useUpdatePrincipalPromptCacheMode).mockReturnValue({
      mutate,
      isPending: false,
    } as never);

    renderWithProviders(<CacheKeepaliveCard principal={mockPrincipal} />);

    fireEvent.click(screen.getByTestId('prompt-cache-mode-switch'));

    expect(toast.error).toHaveBeenCalledWith(
      'Principal was modified by another user. Please refresh and try again.',
    );
  });

  it('Given the undo action, When pressed, Then the previous mode is written back against the returned revision', () => {
    const mutate = vi.fn(
      (_vars: unknown, options: { onSuccess: (updated: unknown) => void }) => {
        options.onSuccess({ ...mockPrincipal, revision: 43 });
      },
    );
    vi.mocked(queries.useUpdatePrincipalPromptCacheMode).mockReturnValue({
      mutate,
      isPending: false,
    } as never);

    renderWithProviders(<CacheKeepaliveCard principal={mockPrincipal} />);

    fireEvent.click(screen.getByTestId('prompt-cache-mode-switch'));
    expect(toast.success).toHaveBeenCalledWith(
      'One-hour cache enabled',
      expect.objectContaining({
        action: expect.objectContaining({ label: 'Undo' }),
      }),
    );

    const toastOptions = vi.mocked(toast.success).mock
      .calls[0][1] as unknown as { action: { onClick: () => void } };
    toastOptions.action.onClick();

    expect(mutate).toHaveBeenLastCalledWith(
      {
        id: 'p-123',
        expected_revision: 43,
        prompt_cache_mode: 'preserve_client',
      },
      expect.any(Object),
    );
  });

  it('Given a pending principal write, When rendered, Then the mode switch is locked', () => {
    vi.mocked(queries.usePrincipalWritePending).mockReturnValue(1);

    renderWithProviders(<CacheKeepaliveCard principal={mockPrincipal} />);

    const modeToggle = screen.getByTestId('prompt-cache-mode-switch');
    expect(modeToggle.hasAttribute('disabled')).toBe(true);
  });

  it('has no Active badge and no live dot text/element', () => {
    const { container } = renderWithProviders(
      <CacheKeepaliveCard principal={mockPrincipal} />,
    );
    expect(screen.queryByText('Active')).toBeNull();
    expect(container.querySelector('.status-dot')).toBeNull();
  });

  it('Sessions and Settings buttons open separate drawers', () => {
    renderWithProviders(<CacheKeepaliveCard principal={mockPrincipal} />);

    const sessionsBtn = screen.getByRole('button', { name: 'Sessions' });
    expect(sessionsBtn.querySelector('.lucide-history')).not.toBeNull();

    const settingsBtn = screen.getByRole('button', { name: 'Settings' });
    expect(
      settingsBtn.querySelector('.lucide-sliders-horizontal'),
    ).not.toBeNull();

    fireEvent.click(sessionsBtn);
    expect(
      screen.getByRole('dialog', { name: 'Cache keepalive sessions' }),
    ).toBeDefined();

    fireEvent.click(settingsBtn);
    expect(
      screen.getByRole('dialog', { name: 'Cache keepalive settings' }),
    ).toBeDefined();
  });

  it('Given metric change, When rendered, Then flashes ONLY when value changes', () => {
    const { rerender } = renderWithProviders(
      <CacheKeepaliveCard principal={mockPrincipal} />,
    );

    let renewingNow = screen.getByText('1');
    expect(renewingNow.className).not.toContain('flash-text-active');

    vi.spyOn(queries, 'useCacheKeepaliveSummary').mockReturnValue({
      data: {
        renewing_now: 99,
        sessions_last_5m: 2,
        renewals_fired: 3,
        cost_saved: 4.56,
      },
    } as unknown as ReturnType<typeof queries.useCacheKeepaliveSummary>);

    rerender(
      <QueryClientProvider
        client={
          new QueryClient({ defaultOptions: { queries: { retry: false } } })
        }
      >
        <CacheKeepaliveCard principal={mockPrincipal} />
      </QueryClientProvider>,
    );

    renewingNow = screen.getByText('99');
    expect(renewingNow.className).toContain('flash-text-active');
    expect(renewingNow.style.animation).toBe(
      `flash-text ${cacheKeepaliveAnimationContract.metricFlash}`,
    );

    const sessions = screen.getByText('2', {
      selector: '[data-testid="cache-keepalive-metric-value"]',
    });
    expect(sessions.className).not.toContain('flash-text-active');
  });

  it('Given PAUSE_ANIMATIONS=true, When metric changes, Then no flash animation is applied', () => {
    window.PAUSE_ANIMATIONS = true;
    const { rerender } = renderWithProviders(
      <CacheKeepaliveCard principal={mockPrincipal} />,
    );

    vi.spyOn(queries, 'useCacheKeepaliveSummary').mockReturnValue({
      data: {
        renewing_now: 99,
        sessions_last_5m: 2,
        renewals_fired: 3,
        cost_saved: 4.56,
      },
    } as unknown as ReturnType<typeof queries.useCacheKeepaliveSummary>);

    rerender(
      <QueryClientProvider
        client={
          new QueryClient({ defaultOptions: { queries: { retry: false } } })
        }
      >
        <CacheKeepaliveCard principal={mockPrincipal} />
      </QueryClientProvider>,
    );

    const renewingNow = screen.getByText('99');
    expect(renewingNow.className).not.toContain('flash-text-active');
    expect(renewingNow.style.animation).toBe('');
  });
});
