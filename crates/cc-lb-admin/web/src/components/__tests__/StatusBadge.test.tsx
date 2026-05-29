import { cleanup, render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, describe, expect, it } from 'vitest';
import { StatusBadge } from '../StatusBadge';

describe('StatusBadge', () => {
  afterEach(() => {
    cleanup();
  });

  it('renders active state correctly', () => {
    render(<StatusBadge status="active" />);
    expect(screen.getByText('Active')).toBeDefined();
  });

  it('renders disabled state correctly', () => {
    render(<StatusBadge status="disabled" />);
    expect(screen.getByText('Disabled')).toBeDefined();
  });

  it('renders error state without popover if no error provided', () => {
    render(<StatusBadge status="error" />);
    expect(screen.getByText('Error')).toBeDefined();
  });

  it('renders error state with popover content', async () => {
    const user = userEvent.setup();
    const errorMsg = 'Failed to connect to upstream';
    const date = new Date().toISOString();

    render(
      <StatusBadge
        status="error"
        lastApplyError={errorMsg}
        lastApplyAt={date}
      />,
    );

    const trigger = screen.getByText('Error');
    await user.click(trigger);

    expect(await screen.findByText('Apply Error')).toBeDefined();
    expect(screen.getByText(errorMsg)).toBeDefined();
  });
});
