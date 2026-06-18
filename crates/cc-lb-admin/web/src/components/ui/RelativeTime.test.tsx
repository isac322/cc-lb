import { act, cleanup, render, screen } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest';
import { RelativeTime, ResetCountdown } from './RelativeTime';

describe('RelativeTime', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-06-18T00:00:01.000Z'));
  });

  afterEach(() => {
    cleanup();
    vi.useRealTimers();
  });

  test('updates elapsed seconds without receiving new props', () => {
    const eventTime = new Date('2026-06-18T00:00:00.000Z');

    render(<RelativeTime ts={eventTime} />);

    expect(screen.getByText('1 second ago').textContent).toBe('1 second ago');

    for (let i = 0; i < 4; i += 1) {
      act(() => {
        vi.advanceTimersByTime(1_000);
      });
    }

    expect(screen.getByText('5 seconds ago').textContent).toBe('5 seconds ago');
  });

  test('lets reset countdown callers choose detailed or compact durations', () => {
    const resetTime = new Date('2026-06-18T00:01:01.000Z');

    const { rerender } = render(<ResetCountdown ts={resetTime} />);

    expect(screen.getByText('Resets in 1 minute').textContent).toBe(
      'Resets in 1 minute',
    );

    rerender(<ResetCountdown compact ts={resetTime} />);

    expect(screen.getByText('Resets in 1m').textContent).toBe('Resets in 1m');
  });
});
