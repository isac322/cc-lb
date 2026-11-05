import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest';
import { ApiError, type Upstream } from '../../../lib/api';
import { AccountCard, DeviceCodePanel, friendlyCompleteError } from './parts';
import {
  UpstreamConnectDialog,
  type UpstreamConnectTarget,
} from './UpstreamConnectDialog';
import {
  identityOfResult,
  oauthCompleteErrorMessage,
} from './useOAuthConnectSession';

let route: (path: string, body: unknown) => { status: number; body: unknown };
const calls: { path: string; body: unknown }[] = [];

const success = {
  state_token: 'state-token',
  suggested_name: 'Linked account',
  subscription_metadata: null,
  organization_metadata: null,
  mode: 'refreshing',
  long_lived_fallback: false,
  fallback_reason: null,
  granted_expires_in_secs: null,
};

const upstream = (kind: Upstream['kind']): Upstream => ({
  id: `${kind}-1`,
  name: `${kind} account`,
  kind,
  enabled: true,
  base_url: null,
  api_key_access: null,
  warmup_enabled: false,
  warmup_dialect_plugin: null,
  spec_revision: 1,
  status: {
    last_apply_error: null,
    last_apply_at_unix_secs: null,
    last_warmup_at_unix_secs: null,
  },
});

function startResponse(provider: 'openai' | 'gemini') {
  return provider === 'openai'
    ? {
        authorize_url: null,
        state_token: 'device-state',
        provider,
        flow: 'device_code',
        verification_url: 'https://auth.openai.com/codex/device',
        user_code: 'ABCD-EFGH',
        poll_interval_secs: 1,
        expires_at_unix_secs: Math.floor(Date.now() / 1000) + 900,
      }
    : {
        authorize_url:
          'https://accounts.google.com/o/oauth2/auth?state=google-state',
        state_token: 'google-state-token',
        provider,
        flow: 'browser',
        expires_at_unix_secs: Math.floor(Date.now() / 1000) + 900,
      };
}

function browserStartResponse() {
  return {
    authorize_url: 'https://auth.openai.com/oauth/authorize?state=openai-state',
    state_token: 'openai-state-token',
    provider: 'openai' as const,
    flow: 'paste_code' as const,
  };
}

function isBrowserFlow(body: unknown): boolean {
  return (
    body !== null &&
    typeof body === 'object' &&
    'flow' in body &&
    body.flow === 'browser'
  );
}

function renderDialog(target: UpstreamConnectTarget) {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  const onClose = vi.fn();
  const onCreated = vi.fn();
  render(
    <QueryClientProvider client={queryClient}>
      <UpstreamConnectDialog
        target={target}
        onClose={onClose}
        onCreated={onCreated}
      />
    </QueryClientProvider>,
  );
  return { dialog: () => screen.getByRole('dialog'), onClose, onCreated };
}

function response(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'content-type': 'application/json' },
  });
}

async function chooseSubscription(
  dialog: HTMLElement,
  provider: 'OpenAI' | 'Gemini',
) {
  await act(async () => {
    fireEvent.click(
      within(dialog).getByRole('button', { name: new RegExp(provider) }),
    );
  });
  await act(async () => {
    fireEvent.click(within(dialog).getByRole('button', { name: 'Continue' }));
  });
  await act(async () => {
    fireEvent.click(
      within(dialog).getByRole('radio', { name: /Subscription/ }),
    );
  });
  await act(async () => {
    fireEvent.click(within(dialog).getByRole('button', { name: 'Continue' }));
  });
}
beforeEach(() => {
  calls.length = 0;
  route = () => ({ status: 404, body: { error: 'unexpected' } });
  vi.stubGlobal(
    'fetch',
    vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const path = String(input);
      const body =
        typeof init?.body === 'string' ? JSON.parse(init.body) : null;
      calls.push({ path, body });
      const result = route(path, body);
      return response(result.body, result.status);
    }),
  );
});

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
  vi.useRealTimers();
});

describe('provider subscription OAuth flows', () => {
  test('OpenAI device panel renders the verification link, code, copy action, and waiting status', () => {
    const session = {
      provider: 'openai' as const,
      flow: 'device_code' as const,
      phase: 'ready' as const,
      authorizeUrl: null,
      verificationUrl: 'https://auth.openai.com/codex/device',
      userCode: 'ABCD-EFGH',
      pollIntervalSecs: 1,
      oauthState: null,
      remainingMs: 900_000,
      startError: null,
      startNotice: null,
      restart: vi.fn(async () => {}),
      startBrowser: vi.fn(async () => {}),
      complete: vi.fn(async () => null),
      completing: false,
      polling: true,
      completeError: null,
      clearCompleteError: vi.fn(),
      outcome: null,
    };
    render(<DeviceCodePanel session={session} />);
    expect(
      screen
        .getByRole('link', { name: /Sign in with ChatGPT/ })
        .getAttribute('href'),
    ).toBe('https://auth.openai.com/codex/device');
    expect(screen.getByText('ABCD-EFGH')).toBeDefined();
    expect(screen.getByRole('button', { name: 'Copy' })).toBeDefined();
    expect(screen.getByText('Waiting for sign-in…')).toBeDefined();
    expect(
      screen.getByRole('button', {
        name: 'Device sign-in turned off by your workspace? Use browser sign-in',
      }),
    ).toBeDefined();
  });

  test.each(['callback URL', 'bare code'])(
    'Gemini browser flow accepts a %s and reaches the name step',
    async (pasteKind) => {
      const googleCode = `4/0AVGzR1${'a'.repeat(60)}`;
      const callbackUrl = `http://localhost:51121/oauth-callback?code=${encodeURIComponent(googleCode)}&state=google-state`;
      const paste = pasteKind === 'callback URL' ? callbackUrl : googleCode;
      let completeBody: unknown = null;
      route = (path, body) => {
        if (path === '/admin/v1/oauth/draft/start')
          return { status: 200, body: startResponse('gemini') };
        if (path === '/admin/v1/oauth/draft/complete') {
          completeBody = body;
          return {
            status: 200,
            body: {
              ...success,
              provider: 'gemini',
              account_email: 'gemini@example.com',
              project_id: 'gemini-work-project',
              plan: 'standard-tier',
            },
          };
        }
        return { status: 404, body: {} };
      };
      const rendered = renderDialog({ mode: 'create' });
      await chooseSubscription(rendered.dialog(), 'Gemini');
      expect(
        within(rendered.dialog()).getByText('Approve access in a Google tab'),
      ).toBeDefined();
      const signIn = within(rendered.dialog()).getByRole('link', {
        name: 'Sign in with Google',
      });
      expect(signIn.getAttribute('href')).toBe(
        startResponse('gemini').authorize_url,
      );
      expect(signIn.getAttribute('target')).toBe('_blank');
      expect(
        within(rendered.dialog()).getByText(/page that won't load/).textContent,
      ).toContain('localhost:51121');
      expect(
        within(rendered.dialog()).queryByText(/codeassist\.google\.com/),
      ).toBeNull();
      fireEvent.click(
        within(rendered.dialog()).getByRole('link', {
          name: 'Sign in with Google',
        }),
      );
      const dialog = rendered.dialog();
      const code = within(dialog).getByLabelText('Authorization code');
      fireEvent.change(code, { target: { value: 'short' } });
      expect(within(dialog).getByText(/cut off/)).toBeDefined();
      fireEvent.change(code, {
        target: {
          value: `http://localhost:51121/oauth-callback?code=${encodeURIComponent(googleCode)}&state=older-state`,
        },
      });
      expect(
        within(dialog).getByText(/different or older sign-in/),
      ).toBeDefined();
      expect(
        (
          within(dialog).getByRole('button', {
            name: 'Connect',
          }) as HTMLButtonElement
        ).disabled,
      ).toBe(true);
      expect(completeBody).toBeNull();
      fireEvent.change(code, { target: { value: paste } });
      fireEvent.click(within(dialog).getByRole('button', { name: 'Connect' }));
      const name = (await within(dialog).findByLabelText(
        'Upstream name',
      )) as HTMLInputElement;
      expect(name.value).toBe('gemini@example.com · Gemini Standard Tier');
      fireEvent.change(name, { target: { value: 'My Gemini account' } });
      expect(name.value).toBe('My Gemini account');
      expect(completeBody).toEqual({
        state_token: 'google-state-token',
        code: paste,
      });
      expect(within(dialog).queryByLabelText('Credential JSON')).toBeNull();
      expect(within(dialog).getByText('gemini@example.com')).toBeDefined();
      expect(
        within(dialog).getByText(
          /Standard Tier · Project: gemini-work-project/,
        ),
      ).toBeDefined();
      expect(within(dialog).queryByLabelText('Project ID')).toBeNull();
    },
  );

  test('OpenAI device flow polls pending responses and reaches the name step', async () => {
    vi.useFakeTimers();
    let completeCalls = 0;
    route = (path) => {
      if (path === '/admin/v1/oauth/draft/start')
        return { status: 200, body: startResponse('openai') };
      if (path === '/admin/v1/oauth/draft/complete') {
        completeCalls += 1;
        return completeCalls <= 2
          ? {
              status: 202,
              body: { status: 'pending', poll_interval_secs: 2 },
            }
          : {
              status: 200,
              body: {
                ...success,
                provider: 'openai',
                suggested_name: 'operator',
                account_email: 'operator@example.com',
                plan: 'team',
              },
            };
      }
      return { status: 404, body: {} };
    };
    const rendered = renderDialog({ mode: 'create' });
    await chooseSubscription(rendered.dialog(), 'OpenAI');
    expect(
      within(rendered.dialog()).getByText('Approve access in a ChatGPT tab'),
    ).toBeDefined();
    await act(async () => {
      await Promise.resolve();
      await Promise.resolve();
      await Promise.resolve();
    });
    expect(completeCalls).toBeGreaterThanOrEqual(1);
    expect(
      within(rendered.dialog()).getByText('Waiting for sign-in…'),
    ).toBeDefined();
    await act(async () => {
      await vi.advanceTimersByTimeAsync(2000);
      await Promise.resolve();
      await Promise.resolve();
    });
    await act(async () => {
      await vi.advanceTimersByTimeAsync(2000);
      await Promise.resolve();
      await Promise.resolve();
    });
    expect(completeCalls).toBeGreaterThanOrEqual(3);
    const name = within(rendered.dialog()).getByLabelText(
      'Upstream name',
    ) as HTMLInputElement;
    expect(name.value).toBe('OpenAI Team · operator');
    expect(
      within(rendered.dialog())
        .getByRole('button', { name: 'Add upstream' })
        .hasAttribute('disabled'),
    ).toBe(false);
  });
  test('OpenAI device sign-in falls back to browser sign-in when unavailable', async () => {
    route = (path, body) => {
      if (path === '/admin/v1/oauth/draft/start') {
        return isBrowserFlow(body)
          ? { status: 200, body: browserStartResponse() }
          : {
              status: 404,
              body: { error: 'device_login_unavailable' },
            };
      }
      return { status: 404, body: {} };
    };
    const rendered = renderDialog({ mode: 'create' });
    await chooseSubscription(rendered.dialog(), 'OpenAI');
    const dialog = rendered.dialog();
    expect(
      await within(dialog).findByText(
        'Device sign-in is turned off by this workspace, so browser sign-in is ready instead.',
      ),
    ).toBeDefined();
    expect(
      within(dialog).getByRole('link', { name: 'Sign in with ChatGPT' }),
    ).toBeDefined();
    expect(
      calls.some(
        (call) =>
          call.path === '/admin/v1/oauth/draft/start' &&
          call.body !== null &&
          typeof call.body === 'object' &&
          'flow' in call.body &&
          call.body.flow === 'browser',
      ),
    ).toBe(true);
  });

  test('Gemini sign-in names the missing OAuth client configuration', async () => {
    route = (path) => {
      if (path === '/admin/v1/oauth/draft/start') {
        return {
          status: 400,
          body: {
            error: 'oauth_client_not_configured',
            detail:
              'Configure oauth.antigravity.client_id and oauth.antigravity.client_secret before starting Gemini subscription OAuth.',
            stage: null,
            provider_error: null,
            provider_status: null,
          },
        };
      }
      return { status: 404, body: {} };
    };
    const rendered = renderDialog({ mode: 'create' });
    await chooseSubscription(rendered.dialog(), 'Gemini');
    expect(
      await within(rendered.dialog()).findByText(
        /Configure oauth\.antigravity\.client_id and oauth\.antigravity\.client_secret/,
      ),
    ).toBeDefined();
  });

  test('OpenAI browser sign-in accepts the pasted localhost callback URL', async () => {
    let completeBody: unknown = null;
    route = (path, body) => {
      if (path === '/admin/v1/oauth/draft/start')
        return { status: 200, body: browserStartResponse() };
      if (path === '/admin/v1/oauth/draft/complete') {
        completeBody = body;
        return {
          status: 200,
          body: {
            ...success,
            provider: 'openai',
            suggested_name: 'browser',
            account_email: 'browser@example.com',
            plan: 'pro',
          },
        };
      }
      return { status: 404, body: {} };
    };
    const rendered = renderDialog({ mode: 'create' });
    await chooseSubscription(rendered.dialog(), 'OpenAI');
    const dialog = rendered.dialog();
    fireEvent.click(
      within(dialog).getByRole('link', { name: 'Sign in with ChatGPT' }),
    );
    const callbackUrl =
      'http://localhost:1455/auth/callback?code=ac_test-code_123&scope=openid+profile+email+offline_access&state=openai-state';
    fireEvent.change(within(dialog).getByLabelText('Authorization code'), {
      target: { value: callbackUrl },
    });
    fireEvent.click(within(dialog).getByRole('button', { name: 'Connect' }));
    const name = (await within(dialog).findByLabelText(
      'Upstream name',
    )) as HTMLInputElement;
    expect(name.value).toBe('OpenAI Pro · browser');
    expect(completeBody).toEqual({
      state_token: 'openai-state-token',
      code: callbackUrl,
    });
  });

  test('OpenAI browser sign-in explains a callback from another session', async () => {
    let completeCalls = 0;
    route = (path) => {
      if (path === '/admin/v1/oauth/draft/start')
        return { status: 200, body: browserStartResponse() };
      if (path === '/admin/v1/oauth/draft/complete') {
        completeCalls += 1;
        return { status: 200, body: { ...success, provider: 'openai' } };
      }
      return { status: 404, body: {} };
    };
    const rendered = renderDialog({ mode: 'create' });
    await chooseSubscription(rendered.dialog(), 'OpenAI');
    const dialog = rendered.dialog();
    fireEvent.click(
      within(dialog).getByRole('link', { name: 'Sign in with ChatGPT' }),
    );
    fireEvent.change(within(dialog).getByLabelText('Authorization code'), {
      target: {
        value:
          'http://localhost:1455/auth/callback?code=ac_test-code_123&state=other-state',
      },
    });
    expect(
      within(dialog).getByText(
        'This code is from a different or older sign-in. Use the link above again and copy the new code.',
      ),
    ).toBeDefined();
    expect(completeCalls).toBe(0);
  });

  test('expired OpenAI device flow can be restarted', async () => {
    vi.useFakeTimers();
    let starts = 0;
    route = (path) => {
      if (path === '/admin/v1/oauth/draft/start') {
        starts += 1;
        return {
          status: 200,
          body: {
            ...startResponse('openai'),
            expires_at_unix_secs: Math.floor(Date.now() / 1000) + 1,
          },
        };
      }
      if (path === '/admin/v1/oauth/draft/complete')
        return {
          status: 202,
          body: { status: 'pending', poll_interval_secs: 30 },
        };
      return { status: 404, body: {} };
    };
    const rendered = renderDialog({ mode: 'create' });
    await chooseSubscription(rendered.dialog(), 'OpenAI');
    await act(async () => {
      await vi.runOnlyPendingTimersAsync();
      await Promise.resolve();
      await Promise.resolve();
    });
    expect(
      within(rendered.dialog()).getByText('Sign-in link expired'),
    ).toBeDefined();
    fireEvent.click(
      within(rendered.dialog()).getByRole('button', { name: 'Get a new link' }),
    );
    await act(async () => {
      await Promise.resolve();
      await Promise.resolve();
    });
    expect(starts).toBe(2);
  });
  test('reconnects OpenAI through browser sign-in after choosing the fallback', async () => {
    let browserStarted = false;
    route = (path, body) => {
      if (path.endsWith('/oauth/start')) {
        if (isBrowserFlow(body)) {
          browserStarted = true;
          return { status: 200, body: browserStartResponse() };
        }
        return { status: 200, body: startResponse('openai') };
      }
      if (path.endsWith('/oauth/complete')) {
        return browserStarted
          ? { status: 200, body: success }
          : {
              status: 202,
              body: { status: 'pending', poll_interval_secs: 30 },
            };
      }
      if (path.endsWith('/subscription/metadata')) {
        return {
          status: 200,
          body: { organization_metadata: null, subscription_metadata: null },
        };
      }
      return { status: 404, body: {} };
    };
    const rendered = renderDialog({
      mode: 'reconnect',
      upstream: upstream('openai_oauth'),
    });
    const dialog = rendered.dialog();
    expect(
      await within(dialog).findByText('Sign in with ChatGPT'),
    ).toBeDefined();
    fireEvent.click(
      within(dialog).getByRole('button', {
        name: 'Device sign-in turned off by your workspace? Use browser sign-in',
      }),
    );
    expect(
      await within(dialog).findByRole('link', {
        name: 'Sign in with ChatGPT',
      }),
    ).toBeDefined();
    const callbackUrl =
      'http://localhost:1455/auth/callback?code=ac_reconnect_123&state=openai-state';
    fireEvent.click(
      within(dialog).getByRole('link', { name: 'Sign in with ChatGPT' }),
    );
    fireEvent.change(within(dialog).getByLabelText('Authorization code'), {
      target: { value: callbackUrl },
    });
    fireEvent.click(within(dialog).getByRole('button', { name: 'Connect' }));
    expect(
      await within(dialog).findByRole('button', { name: 'Done' }),
    ).toBeDefined();
  });

  test.each([
    ['openai_oauth', 'openai'] as const,
    ['gemini_oauth', 'gemini'] as const,
  ])(
    'reconnects %s through its provider OAuth flow',
    async (kind, provider) => {
      route = (path) => {
        if (path.endsWith('/oauth/start'))
          return { status: 200, body: startResponse(provider) };
        if (path.endsWith('/oauth/complete'))
          return { status: 200, body: success };
        if (path.endsWith('/subscription/metadata')) {
          return {
            status: 200,
            body: { organization_metadata: null, subscription_metadata: null },
          };
        }
        return { status: 404, body: {} };
      };
      const rendered = renderDialog({
        mode: 'reconnect',
        upstream: upstream(kind),
      });
      const dialog = rendered.dialog();
      if (provider === 'openai') {
        expect(
          await within(dialog).findByText('Sign in with ChatGPT'),
        ).toBeDefined();
      } else {
        expect(
          await within(dialog).findByText('Sign in with Google'),
        ).toBeDefined();
        fireEvent.click(
          within(dialog).getByRole('link', { name: 'Sign in with Google' }),
        );
        expect(
          within(dialog).getByText(/page that won't load/).textContent,
        ).toContain('localhost:51121');
        fireEvent.change(within(dialog).getByLabelText('Authorization code'), {
          target: {
            value:
              'http://localhost:51121/oauth-callback?code=gemini-code-1234567890&state=google-state',
          },
        });
        fireEvent.click(
          within(dialog).getByRole('button', { name: 'Connect' }),
        );
      }
      expect(calls.some((call) => call.path.endsWith('/oauth/start'))).toBe(
        true,
      );
      await waitFor(() =>
        expect(
          calls.some((call) => call.path.endsWith('/oauth/complete')),
        ).toBe(true),
      );
    },
  );
});

test('OpenAI reconnect loads stored identity without Anthropic metadata refresh', async () => {
  let completed = false;
  route = (path) => {
    if (path.endsWith('/oauth/start'))
      return { status: 200, body: browserStartResponse() };
    if (path.endsWith('/oauth/complete')) {
      completed = true;
      return { status: 200, body: { ...success, provider: 'openai' } };
    }
    if (path.endsWith('/subscription-metadata'))
      return {
        status: 200,
        body: {
          subscription_metadata: null,
          organization_metadata: null,
          account_email: 'openai@example.com',
          account_id: 'team-account-123',
          plan: completed ? 'team' : 'plus',
          project_id: null,
        },
      };
    return { status: 404, body: {} };
  };
  const rendered = renderDialog({
    mode: 'reconnect',
    upstream: upstream('openai_oauth'),
  });
  const dialog = rendered.dialog();
  fireEvent.click(
    await within(dialog).findByRole('link', { name: 'Sign in with ChatGPT' }),
  );
  fireEvent.change(within(dialog).getByLabelText('Authorization code'), {
    target: {
      value:
        'http://localhost:1455/auth/callback?code=ac_reconnect_123&state=openai-state',
    },
  });
  fireEvent.click(within(dialog).getByRole('button', { name: 'Connect' }));
  expect(
    await within(dialog).findByRole('button', { name: 'Done' }),
  ).toBeDefined();
  expect(within(dialog).getByText('openai@example.com')).toBeDefined();
  expect(
    within(dialog).getByText(/Team · Workspace: team-account-123/),
  ).toBeDefined();
  expect(
    calls.some((call) => call.path.endsWith('/subscription-metadata/refresh')),
  ).toBe(false);
});

describe('OAuth completion error messages', () => {
  test.each([
    [
      'token_exchange_rejected',
      'invalid_grant',
      'The code has expired.',
      'Google rejected the code (invalid_grant): The code has expired.',
    ],
    [
      'missing_project_id',
      null,
      '',
      'This Google account has no Gemini Code Assist project',
    ],
    [
      'missing_project_id',
      null,
      'No project was provisioned.',
      'This Google account has no Gemini Code Assist project: No project was provisioned.',
    ],
    [
      'load_code_assist_failed',
      null,
      'Access is denied.',
      'Google Code Assist could not load this account: Access is denied.',
    ],
    ['unexpected_error', null, 'Try later.', 'unexpected_error: Try later.'],
  ])(
    'maps %s without losing provider detail',
    (code, providerError, detail, expected) => {
      const error = new ApiError(
        400,
        code,
        { error: code, detail, provider_error: providerError },
        'Bad Request',
      );
      const message = oauthCompleteErrorMessage(error, 'gemini');
      expect(message).toBe(expected);
      expect(friendlyCompleteError(message)).toContain(expected);
      expect(message).not.toContain('Bad Request');
    },
  );

  test('uses a generic fallback for an unstructured failure', () => {
    expect(
      friendlyCompleteError(
        oauthCompleteErrorMessage(new Error('Network unavailable'), 'gemini'),
      ),
    ).toBe('Connection failed: Network unavailable');
  });

  test('renders a Google server error beside the code field', async () => {
    route = (path) => {
      if (path === '/admin/v1/oauth/draft/start')
        return { status: 200, body: startResponse('gemini') };
      if (path === '/admin/v1/oauth/draft/complete')
        return {
          status: 400,
          body: {
            error: 'token_exchange_rejected',
            provider_error: 'invalid_grant',
            detail: 'The authorization code was rejected.',
          },
        };
      return { status: 404, body: {} };
    };
    const rendered = renderDialog({ mode: 'create' });
    await chooseSubscription(rendered.dialog(), 'Gemini');
    const dialog = rendered.dialog();
    fireEvent.click(
      within(dialog).getByRole('link', { name: 'Sign in with Google' }),
    );
    fireEvent.change(within(dialog).getByLabelText('Authorization code'), {
      target: { value: `4/0AVGzR1${'a'.repeat(60)}` },
    });
    fireEvent.click(within(dialog).getByRole('button', { name: 'Connect' }));
    expect(
      await within(dialog).findByText(
        /Google rejected the code \(invalid_grant\): The authorization code was rejected/,
      ),
    ).toBeDefined();
    expect(within(dialog).queryByText(/Bad Request/)).toBeNull();
  });
});

describe('provider account cards', () => {
  test.each(['team', 'pro', 'plus'])(
    'renders ChatGPT %s email, plan and workspace',
    (plan) => {
      const account = identityOfResult({
        organization_metadata: null,
        account_email: 'openai@example.com',
        account_id: 'workspace-account-123',
        plan,
        project_id: null,
      });
      render(<AccountCard account={account} />);
      expect(screen.getByText('openai@example.com')).toBeDefined();
      expect(
        screen.getByText(/Workspace: workspace-account-123/),
      ).toBeDefined();
      expect(screen.getByText(new RegExp(plan, 'i'))).toBeDefined();
      expect(screen.queryByText('No account connected yet')).toBeNull();
    },
  );
});
