import { act, cleanup, render, screen } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest';
import {
  RelativeOffsetTime,
  RelativeTime,
  ResetCountdown,
} from './RelativeTime';

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

  test('supports compact relative labels for dense surfaces', () => {
    const eventTime = new Date('2026-06-18T00:00:00.000Z');

    render(<RelativeTime compact ts={eventTime} />);

    expect(screen.getByText('1s ago').textContent).toBe('1s ago');
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

  test('treats NaN and invalid Date timestamps as absent', () => {
    const { container, rerender } = render(<RelativeTime ts={Number.NaN} />);

    expect(screen.getByText('—')).toBeDefined();
    expect(container.querySelector('[title="Invalid Date"]')).toBeNull();

    rerender(<RelativeTime ts={new Date(Number.NaN)} />);

    expect(screen.getByText('—')).toBeDefined();
    expect(container.querySelector('[title="Invalid Date"]')).toBeNull();
  });

  test('treats NaN and invalid Date reset timestamps as absent', () => {
    const { container, rerender } = render(<ResetCountdown ts={Number.NaN} />);

    expect(screen.getByText('—')).toBeDefined();
    expect(container.querySelector('[title="Invalid Date"]')).toBeNull();

    rerender(<ResetCountdown ts={new Date(Number.NaN)} />);

    expect(screen.getByText('—')).toBeDefined();
    expect(container.querySelector('[title="Invalid Date"]')).toBeNull();
  });

  test('turns a relative offset into a stable live timestamp', () => {
    render(<RelativeOffsetTime offsetSeconds={5} />);

    expect(screen.getByText('in 5 seconds').textContent).toBe('in 5 seconds');

    act(() => {
      vi.advanceTimersByTime(1_000);
    });

    expect(screen.getByText('in 4 seconds').textContent).toBe('in 4 seconds');
  });
});

describe('RelativeTime under ko-KR locale', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-06-18T00:00:01.000Z'));
    window.localStorage.setItem('cclb.locale', 'ko-KR');
  });

  afterEach(() => {
    cleanup();
    vi.useRealTimers();
    window.localStorage.removeItem('cclb.locale');
  });

  test('renders compact past labels in Korean without English fragments', () => {
    const eventTime = new Date('2026-06-17T23:55:01.000Z');

    render(<RelativeTime compact ts={eventTime} />);

    expect(screen.getByText('5분 전').textContent).toBe('5분 전');
  });

  test('renders compact future offset labels in Korean', () => {
    render(<RelativeOffsetTime compact offsetSeconds={300} />);

    expect(screen.getByText('5분 후').textContent).toBe('5분 후');
  });

  test('renders default reset countdown future label in Korean', () => {
    const resetTime = new Date('2026-06-18T00:05:01.000Z');

    render(<ResetCountdown compact ts={resetTime} />);

    expect(screen.getByText('5분 후 초기화').textContent).toBe('5분 후 초기화');
  });

  test('renders default reset countdown past label in Korean', () => {
    const resetTime = new Date('2026-06-17T23:55:01.000Z');

    render(<ResetCountdown compact ts={resetTime} />);

    expect(screen.getByText('5분 전 초기화됨').textContent).toBe(
      '5분 전 초기화됨',
    );
  });
});
