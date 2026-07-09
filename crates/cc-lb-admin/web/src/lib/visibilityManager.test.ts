// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { visibilityManager } from './visibilityManager';

describe('VisibilityManager', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('should handle visibility changes and grace period', () => {
    const listener = vi.fn();
    const unsubscribe = visibilityManager.subscribe(listener);

    // Mock visibility hidden
    Object.defineProperty(document, 'visibilityState', {
      value: 'hidden',
      configurable: true,
    });
    document.dispatchEvent(new Event('visibilitychange'));

    expect(visibilityManager.getState().visible).toBe(false);
    expect(visibilityManager.getState().gracePeriodElapsed).toBe(false);
    expect(listener).toHaveBeenCalledTimes(1);

    // Advance time by 1 minute
    vi.advanceTimersByTime(60_000);
    expect(visibilityManager.getState().gracePeriodElapsed).toBe(false);

    // Advance time by another 1 minute (total 2 minutes)
    vi.advanceTimersByTime(60_000);
    expect(visibilityManager.getState().gracePeriodElapsed).toBe(true);
    expect(listener).toHaveBeenCalledTimes(2);

    // Mock visibility visible
    Object.defineProperty(document, 'visibilityState', {
      value: 'visible',
      configurable: true,
    });
    document.dispatchEvent(new Event('visibilitychange'));

    expect(visibilityManager.getState().visible).toBe(true);
    expect(visibilityManager.getState().gracePeriodElapsed).toBe(false);
    expect(listener).toHaveBeenCalledTimes(3);

    unsubscribe();
  });

  it('should handle online/offline events', () => {
    const listener = vi.fn();
    const unsubscribe = visibilityManager.subscribe(listener);

    window.dispatchEvent(new Event('offline'));
    expect(visibilityManager.getState().online).toBe(false);
    expect(listener).toHaveBeenCalledTimes(1);

    window.dispatchEvent(new Event('online'));
    expect(visibilityManager.getState().online).toBe(true);
    expect(listener).toHaveBeenCalledTimes(2);

    unsubscribe();
  });
});
