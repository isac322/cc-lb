import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import {
  cleanup,
  fireEvent,
  render,
  screen,
  within,
} from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import * as queries from '../../../../lib/queries';
import { OpenaiOauthCompatCard } from '../OpenaiOauthCompatCard';

const mockPrincipal: queries.Principal = {
  id: 'principal-1',
  name: 'Codex principal',
  kind: 'machine',
  enabled: true,
  revision: 7,
  allowed_models: [],
  allowed_upstreams: [],
  default_limits: [],
  openai_oauth_compat: { sampling: 'reject' },
};

const effectivePolicy = {
  store_true: 'coerce',
  sampling: 'drop',
  logprobs: 'drop',
  truncation_auto: 'emulate',
  prompt_cache_in_memory: 'drop',
  service_tier: 'coerce',
  web_search_preview: 'coerce',
  metadata: 'drop_echo',
  max_tool_calls: 'drop',
  background: 'drop',
  hosted_tools: 'drop_tool',
  compact: 'emulate',
  chat_n: 'emulate',
  chat_stop: 'emulate',
  chat_seed_penalties: 'drop',
  transcription: 'disabled',
};

function renderCard(principal = mockPrincipal) {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  return render(
    <QueryClientProvider client={queryClient}>
      <OpenaiOauthCompatCard principal={principal} />
    </QueryClientProvider>,
  );
}

describe('OpenaiOauthCompatCard', () => {
  const mutate = vi.fn();

  beforeEach(() => {
    mutate.mockReset();
    vi.spyOn(queries, 'useUpdatePrincipalOpenaiOauthCompat').mockReturnValue({
      mutate,
      isPending: false,
    } as never);
    vi.spyOn(queries, 'usePrincipalWritePending').mockReturnValue(0);
    vi.spyOn(queries, 'useConfigEditor').mockReturnValue({
      data: { effective_config: { openai_oauth_compat: effectivePolicy } },
      isLoading: false,
    } as never);
  });

  afterEach(() => {
    cleanup();
    vi.restoreAllMocks();
  });

  it('renders effective values and inherited versus overridden state', () => {
    renderCard();
    expect(screen.getByText('Public OpenAI API compatibility')).toBeDefined();

    expect(
      screen.getByTestId('openai-oauth-compat-row-sampling').textContent,
    ).toContain('Effective: Reject the request');
    expect(
      screen.getByTestId('openai-oauth-compat-row-sampling').textContent,
    ).toContain('Overridden for this principal');
    expect(
      screen.getByTestId('openai-oauth-compat-row-store_true').textContent,
    ).toContain('Effective: Coerce to a supported value');
    expect(
      screen.getByTestId('openai-oauth-compat-row-store_true').textContent,
    ).toContain('Inherited from server');
    expect(
      screen.getByText(/keeps submitted audio for 30 days/i),
    ).toBeDefined();
  });

  it('stays read-only and claims no server value when the policy is unavailable', () => {
    vi.spyOn(queries, 'useConfigEditor').mockReturnValue({
      data: undefined,
      isLoading: false,
      isError: true,
    } as never);
    renderCard();

    expect(
      screen.getByTestId('openai-oauth-compat-policy-unavailable').textContent,
    ).toContain('could not be loaded');
    const storeRow = screen.getByTestId('openai-oauth-compat-row-store_true');
    expect(storeRow.textContent).toContain('Effective: Unknown');
    expect(storeRow.textContent).not.toContain('Coerce to a supported value');
    expect(
      screen
        .getByRole('button', { name: 'Save changes' })
        .hasAttribute('disabled'),
    ).toBe(true);
    expect(
      screen
        .getByRole('button', { name: 'Reset all to server defaults' })
        .hasAttribute('disabled'),
    ).toBe(true);
  });

  it('confirms the 30-day retention before enabling transcription', async () => {
    renderCard();
    const user = userEvent.setup();
    await user.click(
      screen.getByRole('combobox', { name: 'Audio transcription' }),
    );

    await user.click(
      await within(screen.getByTestId('select-popup')).findByRole('option', {
        name: 'Enabled',
      }),
    );

    expect(
      await screen.findByText('Enable audio transcription?'),
    ).toBeDefined();
    await user.click(
      screen.getByRole('button', { name: 'Enable transcription' }),
    );
    await user.click(screen.getByRole('button', { name: 'Save changes' }));

    expect(mutate).toHaveBeenCalledWith(
      expect.objectContaining({
        openai_oauth_compat: {
          sampling: 'reject',
          transcription: 'enabled',
        },
      }),
      expect.any(Object),
    );
  });

  it('saves only the changed override keys merged with existing overrides', async () => {
    renderCard();
    const user = userEvent.setup();

    await user.click(
      screen.getByRole('combobox', { name: 'Store requests marked true' }),
    );
    const popup = screen.getByTestId('select-popup');
    await user.click(
      await within(popup).findByRole('option', { name: 'Reject the request' }),
    );
    await user.click(screen.getByRole('button', { name: 'Save changes' }));

    expect(mutate).toHaveBeenCalledWith(
      {
        id: 'principal-1',
        expected_revision: 7,
        openai_oauth_compat: { sampling: 'reject', store_true: 'reject' },
      },
      expect.any(Object),
    );
  });

  it('resets all overrides with a null payload after confirmation', () => {
    renderCard();

    fireEvent.click(
      screen.getByRole('button', { name: 'Reset all to server defaults' }),
    );
    expect(
      screen.getByText('Reset OpenAI compatibility settings?'),
    ).toBeDefined();
    fireEvent.click(screen.getByRole('button', { name: 'Reset all' }));

    expect(mutate).toHaveBeenCalledWith(
      {
        id: 'principal-1',
        expected_revision: 7,
        openai_oauth_compat: null,
      },
      expect.any(Object),
    );
  });
});
