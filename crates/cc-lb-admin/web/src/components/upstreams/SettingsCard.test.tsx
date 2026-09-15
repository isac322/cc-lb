import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest';
import type { Upstream } from '../../lib/queries';
import * as queries from '../../lib/queries';
import { InlineNameEditor } from './InlineNameEditor';
import { SettingsCard } from './SettingsCard';

vi.mock('../../lib/queries', async () => {
  const actual = (await vi.importActual('../../lib/queries')) as typeof queries;
  return {
    ...actual,
    useUpdateUpstream: vi.fn(),
  };
});

const upstream: Upstream = {
  id: 'upstream-api-key',
  name: 'API Key Primary',
  kind: 'anthropic_api_key',
  enabled: true,
  spec_revision: 7,
  base_url: 'https://api.anthropic.com',
  api_key_env: 'ANTHROPIC_API_KEY',
  warmup_enabled: false,
  warmup_dialect_plugin: null,
  status: {
    last_apply_error: null,
    last_apply_at_unix_secs: null,
    last_warmup_at_unix_secs: null,
  },
};

const mutate = vi.fn();
const updateMutation = {
  mutate,
  isPending: false,
};

beforeEach(() => {
  vi.clearAllMocks();
  updateMutation.isPending = false;
  vi.mocked(queries.useUpdateUpstream).mockImplementation(
    () => updateMutation as never,
  );
});

afterEach(cleanup);

describe('SettingsCard pending state', () => {
  test('locks actions, shows saving progress, and submits only once', () => {
    const view = render(<SettingsCard upstream={upstream} />);

    fireEvent.click(screen.getByRole('button', { name: 'Edit' }));
    expect(
      screen
        .getByTestId('upstream-settings-edit-form')
        .getAttribute('aria-busy'),
    ).toBe('false');
    fireEvent.change(
      screen.getByPlaceholderText('e.g. https://api.anthropic.com'),
      {
        target: { value: ' https://proxy.example.com ' },
      },
    );
    fireEvent.change(screen.getByPlaceholderText('e.g. ANTHROPIC_API_KEY'), {
      target: { value: ' ANTHROPIC_API_KEY_NEXT ' },
    });
    fireEvent.click(screen.getByRole('button', { name: 'Save' }));

    expect(mutate).toHaveBeenCalledTimes(1);
    expect(mutate).toHaveBeenCalledWith(
      {
        id: upstream.id,
        body: {
          base_url: 'https://proxy.example.com',
          api_key_value: null,
          api_key_env: 'ANTHROPIC_API_KEY_NEXT',
        },
        spec_revision: upstream.spec_revision,
      },
      expect.anything(),
    );

    updateMutation.isPending = true;
    view.rerender(<SettingsCard upstream={upstream} />);

    const form = screen.getByTestId('upstream-settings-edit-form');
    expect(form.getAttribute('aria-busy')).toBe('true');
    const saving = screen.getByRole('button', { name: 'Saving...' });
    expect(saving.hasAttribute('disabled')).toBe(true);
    expect(saving.getAttribute('aria-busy')).toBe('true');
    expect(saving.querySelector('svg.animate-spin')).not.toBeNull();
    expect(
      screen.getByRole('button', { name: 'Cancel' }).hasAttribute('disabled'),
    ).toBe(true);

    fireEvent.click(saving);
    expect(mutate).toHaveBeenCalledTimes(1);
  });
});

describe('InlineNameEditor commit boundary', () => {
  test('submits one rename when Enter is followed by blur', () => {
    render(<InlineNameEditor upstream={upstream} />);

    fireEvent.click(screen.getByText(upstream.name));
    const input = screen.getByRole('textbox');
    fireEvent.change(input, { target: { value: 'Renamed upstream' } });
    fireEvent.keyDown(input, { key: 'Enter', code: 'Enter' });
    fireEvent.blur(input);

    expect(mutate).toHaveBeenCalledTimes(1);
    expect(mutate).toHaveBeenCalledWith(
      {
        id: upstream.id,
        body: { name: 'Renamed upstream' },
        spec_revision: upstream.spec_revision,
      },
      expect.anything(),
    );
  });

  test('still saves a changed name on blur', () => {
    render(<InlineNameEditor upstream={upstream} />);

    fireEvent.click(screen.getByText(upstream.name));
    const input = screen.getByRole('textbox');
    fireEvent.change(input, { target: { value: 'Blurred rename' } });
    fireEvent.blur(input);

    expect(mutate).toHaveBeenCalledTimes(1);
    expect(mutate).toHaveBeenCalledWith(
      {
        id: upstream.id,
        body: { name: 'Blurred rename' },
        spec_revision: upstream.spec_revision,
      },
      expect.anything(),
    );
  });

  test('cancels an edited name with Escape without saving on blur', () => {
    render(<InlineNameEditor upstream={upstream} />);

    fireEvent.click(screen.getByText(upstream.name));
    const input = screen.getByRole('textbox');
    fireEvent.change(input, { target: { value: 'Do not save' } });
    fireEvent.keyDown(input, { key: 'Escape', code: 'Escape' });

    expect(mutate).not.toHaveBeenCalled();
    expect(screen.getByText(upstream.name)).toBeDefined();
  });
});
