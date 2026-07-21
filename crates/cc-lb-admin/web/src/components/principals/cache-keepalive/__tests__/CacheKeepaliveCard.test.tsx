import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
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
    vi.spyOn(queries, 'useCacheKeepaliveSummary').mockReturnValue({
      data: {
        renewing_now: 1,
        sessions_last_5m: 2,
        renewals_fired: 3,
        cost_saved: 4.56,
      },
    } as unknown as ReturnType<typeof queries.useCacheKeepaliveSummary>);
  });

  it('renders exact card labels, subcaptions, caption, and tooltip text', async () => {
    renderWithProviders(<CacheKeepaliveCard principal={mockPrincipal} />);

    expect(screen.getByText('Cache keepalive')).toBeDefined();
    expect(
      screen.getByText('Renews the prompt-cache TTL during idle gaps.'),
    ).toBeDefined();

    expect(screen.getByText('Renewing now')).toBeDefined();
    expect(screen.getByText('scheduled or mid-renewal')).toBeDefined();
    expect(screen.getByText('1')).toBeDefined();

    expect(screen.getByText('Sessions (last 5m)')).toBeDefined();
    expect(screen.getByText('seen in last 5 min')).toBeDefined();
    expect(screen.getByText('2')).toBeDefined();

    expect(screen.getByText('Renewals fired')).toBeDefined();
    expect(screen.getByText('all-time')).toBeDefined();
    expect(screen.getByText('3')).toBeDefined();

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

  it('switch has role switch and aria-checked', () => {
    renderWithProviders(<CacheKeepaliveCard principal={mockPrincipal} />);

    const toggle = screen.getByLabelText('Toggle cache keepalive');
    expect(toggle.getAttribute('role')).toBe('switch');
    expect(toggle.getAttribute('aria-checked')).toBe('true');
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

    const sessionsBtn = screen.getByText('Sessions');
    const settingsBtn = screen.getByText('Settings');

    fireEvent.click(sessionsBtn);
    expect(screen.getByTestId('cache-keepalive-sessions-drawer')).toBeDefined();

    fireEvent.click(settingsBtn);
    expect(screen.getByTestId('cache-keepalive-settings-drawer')).toBeDefined();
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

    const sessions = screen.getByText('2', { selector: '.text-sm' });
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
