import { act, cleanup, render, screen } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest';
import { RelativeTime } from './RelativeTime';

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

    expect(screen.getByText('now').textContent).toBe('now');

    act(() => {
      vi.advanceTimersByTime(4_000);
    });

    expect(screen.getByText('5 seconds ago').textContent).toBe('5 seconds ago');
  });
});
