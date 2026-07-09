// @vitest-environment jsdom
import { cleanup, render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { LiveTailFailureBanner } from './LiveTailFailureBanner';

describe('LiveTailFailureBanner', () => {
  afterEach(() => {
    cleanup();
  });

  it('renders nothing when permanentFailure is false', () => {
    const { container } = render(
      <LiveTailFailureBanner
        permanentFailure={false}
        permanentFailureSince={null}
        reconnectAttempts={0}
        onRetry={vi.fn()}
      />,
    );
    expect(container.firstChild).toBeNull();
  });

  it('renders banner with correct copy when permanentFailure is true', () => {
    const now = Date.now();
    const fiveMinutesAgo = now - 5 * 60 * 1000;

    // Mock Date.now to return a consistent value
    const dateSpy = vi.spyOn(Date, 'now').mockReturnValue(now);

    render(
      <LiveTailFailureBanner
        permanentFailure={true}
        permanentFailureSince={fiveMinutesAgo}
        reconnectAttempts={12}
        onRetry={vi.fn()}
      />,
    );

    expect(screen.getByText('Live tail disconnected')).toBeDefined();
    expect(
      screen.getByText(
        'Unable to reach the admin event stream after 12 attempts over the last 5 minutes.',
      ),
    ).toBeDefined();
    expect(screen.getByRole('button', { name: 'Retry now' })).toBeDefined();

    dateSpy.mockRestore();
  });

  it('calls onRetry when Retry now button is clicked', async () => {
    const onRetry = vi.fn();
    const user = userEvent.setup();

    render(
      <LiveTailFailureBanner
        permanentFailure={true}
        permanentFailureSince={Date.now() - 300_000}
        reconnectAttempts={10}
        onRetry={onRetry}
      />,
    );

    await user.click(screen.getByRole('button', { name: 'Retry now' }));
    expect(onRetry).toHaveBeenCalledTimes(1);
  });

  it('hides banner when Dismiss button is clicked', async () => {
    const user = userEvent.setup();

    const { container } = render(
      <LiveTailFailureBanner
        permanentFailure={true}
        permanentFailureSince={Date.now() - 300_000}
        reconnectAttempts={10}
        onRetry={vi.fn()}
      />,
    );

    expect(container.firstChild).not.toBeNull();

    await user.click(screen.getByRole('button', { name: 'Dismiss' }));
    expect(container.firstChild).toBeNull();
  });

  it('re-shows banner if permanentFailure toggles false then true again', async () => {
    const user = userEvent.setup();

    const { container, rerender } = render(
      <LiveTailFailureBanner
        permanentFailure={true}
        permanentFailureSince={Date.now() - 300_000}
        reconnectAttempts={10}
        onRetry={vi.fn()}
      />,
    );

    await user.click(screen.getByRole('button', { name: 'Dismiss' }));
    expect(container.firstChild).toBeNull();

    rerender(
      <LiveTailFailureBanner
        permanentFailure={false}
        permanentFailureSince={null}
        reconnectAttempts={0}
        onRetry={vi.fn()}
      />,
    );
    expect(container.firstChild).toBeNull();

    rerender(
      <LiveTailFailureBanner
        permanentFailure={true}
        permanentFailureSince={Date.now() - 300_000}
        reconnectAttempts={1}
        onRetry={vi.fn()}
      />,
    );
    expect(container.firstChild).not.toBeNull();
  });
});
