import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
} from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { Hint } from './primitives';

describe('Hint', () => {
  beforeEach(() => vi.useFakeTimers());

  afterEach(() => {
    cleanup();
    vi.useRealTimers();
  });

  it('mounts no popover until the full hover delay elapses', () => {
    const onPointerEnter = vi.fn();
    render(
      <Hint label={<span data-testid="hint-content">Tooltip Content</span>}>
        <button
          data-testid="hint-trigger"
          type="button"
          onPointerEnter={onPointerEnter}
        >
          Hover me
        </button>
      </Hint>,
    );

    const trigger = screen.getByTestId('hint-trigger');
    expect(screen.queryByTestId('hint-content')).toBeNull();
    fireEvent.pointerEnter(trigger);
    expect(onPointerEnter).toHaveBeenCalledOnce();

    act(() => vi.advanceTimersByTime(199));
    expect(screen.queryByTestId('hint-content')).toBeNull();
    act(() => vi.advanceTimersByTime(1));
    expect(screen.getByTestId('hint-content')).toBeDefined();
  });

  it('opens immediately on focus', () => {
    render(
      <Hint label={<span data-testid="hint-content">Tooltip Content</span>}>
        <button data-testid="hint-trigger" type="button">
          Focus me
        </button>
      </Hint>,
    );

    fireEvent.focus(screen.getByTestId('hint-trigger'));
    expect(screen.getByTestId('hint-content')).toBeDefined();
  });

  it('preserves child click handling without bubbling', () => {
    const onChildClick = vi.fn();
    const onParentClick = vi.fn();
    render(
      <div onClick={onParentClick}>
        <Hint label="Tooltip Content">
          <button type="button" onClick={onChildClick}>
            Click me
          </button>
        </Hint>
      </div>,
    );

    fireEvent.click(screen.getByRole('button', { name: 'Click me' }));
    expect(onChildClick).toHaveBeenCalledOnce();
    expect(onParentClick).not.toHaveBeenCalled();
  });
});
