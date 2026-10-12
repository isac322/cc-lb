import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, test, vi } from 'vitest';
import { API_KEY_ACCESS_NOT_CHECKED, type ApiKeyAccess } from '../../lib/api';
import { ApiKeyNotice, apiKeyNoticeOf, apiKeyUsable } from './ApiKeyNotice';

afterEach(cleanup);

const OK: ApiKeyAccess = {
  verified: true,
  checked_at_unix_secs: 1_700_000_000,
  key_class: 'workspace_bound',
  inference: 'ok',
  workspace: { mode: 'bound', id: 'wrkspc_01x' },
  workspace_list: 'not_applicable',
  usage_report: 'not_applicable',
  key_type: 'workspace',
  last_error: null,
};

describe('apiKeyNoticeOf', () => {
  test('first match wins: rejected, never checked, then check failed', () => {
    expect(apiKeyNoticeOf(null)).toBeNull();
    expect(apiKeyNoticeOf(OK)).toBeNull();
    expect(
      apiKeyNoticeOf({ ...OK, verified: false, inference: 'denied' }),
    ).toBe('rejected');
    expect(apiKeyNoticeOf(API_KEY_ACCESS_NOT_CHECKED)).toBe('never_checked');
    expect(apiKeyNoticeOf({ ...OK, verified: false })).toBe('check_failed');
  });
});

describe('apiKeyUsable', () => {
  test('only a verified, non-rejected key is usable', () => {
    expect(apiKeyUsable(null)).toBe(false);
    expect(apiKeyUsable(OK)).toBe(true);
    // s19: verified snapshot whose inference was denied.
    expect(apiKeyUsable({ ...OK, inference: 'denied' })).toBe(false);
    expect(apiKeyUsable(API_KEY_ACCESS_NOT_CHECKED)).toBe(false);
  });
});

describe('ApiKeyNotice', () => {
  const handlers = () => ({
    anthropicHost: true,
    onReplace: vi.fn(),
    onRecheck: vi.fn(),
  });

  test('K1: rejection keeps last_error and offers Replace key', () => {
    const h = handlers();
    render(
      <ApiKeyNotice
        kind="rejected"
        lastError="Anthropic rejected this key. Check for a typo, or the key is expired or deleted."
        rechecking={false}
        recheckError={null}
        {...h}
      />,
    );
    expect(screen.getByText('Anthropic rejected this key')).toBeDefined();
    expect(
      screen.getByText(
        'Anthropic rejected this key. Check for a typo, or the key is expired or deleted. Replace the key → requests through this upstream succeed again.',
      ),
    ).toBeDefined();
    expect(screen.queryByText(/endpoint/)).toBeNull();
    fireEvent.click(screen.getByRole('button', { name: 'Replace key' }));
    expect(h.onReplace).toHaveBeenCalledTimes(1);
    expect(screen.queryByRole('button', { name: 'Re-check' })).toBeNull();
  });

  test('K1 on a custom base URL names the endpoint, not Anthropic', () => {
    render(
      <ApiKeyNotice
        kind="rejected"
        lastError="Your endpoint rejected this key. Check the key and the Base URL."
        rechecking={false}
        recheckError={null}
        {...handlers()}
        anthropicHost={false}
      />,
    );
    expect(screen.getByText('Your endpoint rejected this key')).toBeDefined();
    expect(screen.queryByText(/Anthropic/)).toBeNull();
  });

  test('K2: never checked offers Re-check', () => {
    const h = handlers();
    render(
      <ApiKeyNotice
        kind="never_checked"
        lastError={null}
        rechecking={false}
        recheckError={null}
        {...h}
      />,
    );
    expect(screen.getByText('Key not checked yet')).toBeDefined();
    expect(
      screen.getByText('Re-check → see what this key can do at Anthropic.'),
    ).toBeDefined();
    fireEvent.click(screen.getByRole('button', { name: 'Re-check' }));
    expect(h.onRecheck).toHaveBeenCalledTimes(1);
  });

  test('K3: a failed check shows last_error, pending, and a failed Re-check', () => {
    render(
      <ApiKeyNotice
        kind="check_failed"
        lastError="Couldn't reach Anthropic — the key wasn't checked."
        rechecking
        recheckError="timeout"
        {...handlers()}
      />,
    );
    expect(screen.getByText('Last check failed')).toBeDefined();
    expect(
      screen.getByText(
        "Couldn't reach Anthropic — the key wasn't checked. Re-check → confirm the key works.",
      ),
    ).toBeDefined();
    expect(screen.getByRole('button', { name: 'Checking…' })).toBeDefined();
    expect(screen.getByRole('alert').textContent).toBe(
      'Re-check failed: timeout',
    );
  });

  // UI-K-05: operator-phrased last_error never overflows a 390 px pane.
  test('K1/K3 bodies wrap long last_error text', () => {
    const long = `Anthropic answered 401 - invalid x-api-key ${'x'.repeat(160)}`;
    for (const kind of ['rejected', 'check_failed'] as const) {
      const view = render(
        <ApiKeyNotice
          kind={kind}
          lastError={long}
          rechecking={false}
          recheckError={null}
          {...handlers()}
        />,
      );
      expect(screen.getByText(new RegExp(long)).className).toContain(
        'break-words',
      );
      view.unmount();
    }
  });
});
