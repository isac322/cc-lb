import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest';
import type { Upstream } from '../../lib/queries';
import * as queries from '../../lib/queries';
import { idleMutation } from '../../lib/test-utils/hook-results';
import { InlineNameEditor } from './InlineNameEditor';

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
  base_url: null,
  warmup_enabled: false,
  warmup_dialect_plugin: null,
  status: {
    last_apply_error: null,
    last_apply_at_unix_secs: null,
    last_warmup_at_unix_secs: null,
  },
};

const mutate = vi.fn();

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(queries.useUpdateUpstream).mockImplementation(() =>
    idleMutation({ mutate }),
  );
});

afterEach(cleanup);

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
    fireEvent.blur(input);

    expect(mutate).not.toHaveBeenCalled();
    expect(screen.getByText(upstream.name)).toBeDefined();
  });

  test('keeps the page-title typography instead of boxed input chrome', () => {
    render(<InlineNameEditor upstream={upstream} />);

    fireEvent.click(screen.getByText(upstream.name));
    const input = screen.getByRole('textbox');

    expect(input.className).toContain('text-title-page');
    expect(input.className).toContain('border-dotted');
    expect(input.className).not.toContain('bg-input-bg');
    expect(input.className).not.toContain('h-9');
  });
});
