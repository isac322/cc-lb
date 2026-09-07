import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
} from '@testing-library/react';
import {
  afterEach,
  beforeEach,
  describe,
  expect,
  it,
  type Mock,
  vi,
} from 'vitest';
import type { HistogramBucket } from '../../lib/api';
import { TimeRangeStrip, type TimeRangeStripProps } from './TimeRangeStrip';

interface CanvasContextMock {
  beginPath: Mock;
  clearRect: Mock;
  fillRect: Mock;
  fillText: Mock;
  lineTo: Mock;
  moveTo: Mock;
  setTransform: Mock;
  stroke: Mock;
  fillStyle: string;
  font: string;
  globalAlpha: number;
  lineWidth: number;
  strokeStyle: string;
}

const originalClientWidth = Object.getOwnPropertyDescriptor(
  HTMLElement.prototype,
  'clientWidth',
);
let clientWidth = 400;
let context: CanvasContextMock;

function makeProps(
  overrides: Partial<TimeRangeStripProps> = {},
): TimeRangeStripProps {
  return {
    buckets: [],
    bucketMs: 60_000,
    view: { a: 0, b: 120_000 },
    selection: null,
    loading: false,
    failed: false,
    onViewChange: vi.fn(),
    onSelectionCommit: vi.fn(),
    ...overrides,
  };
}

beforeEach(() => {
  clientWidth = 400;
  context = {
    beginPath: vi.fn(),
    clearRect: vi.fn(),
    fillRect: vi.fn(),
    fillText: vi.fn(),
    lineTo: vi.fn(),
    moveTo: vi.fn(),
    setTransform: vi.fn(),
    stroke: vi.fn(),
    fillStyle: '',
    font: '',
    globalAlpha: 1,
    lineWidth: 1,
    strokeStyle: '',
  };
  Object.defineProperty(HTMLElement.prototype, 'clientWidth', {
    configurable: true,
    get: () => clientWidth,
  });
  vi.stubGlobal('devicePixelRatio', 1);
  vi.stubGlobal(
    'ResizeObserver',
    class ResizeObserver {
      observe() {}
      unobserve() {}
      disconnect() {}
    },
  );
  vi.spyOn(HTMLCanvasElement.prototype, 'getContext').mockReturnValue(
    context as unknown as CanvasRenderingContext2D,
  );
});

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  if (originalClientWidth == null) {
    Reflect.deleteProperty(HTMLElement.prototype, 'clientWidth');
  } else {
    Object.defineProperty(
      HTMLElement.prototype,
      'clientWidth',
      originalClientWidth,
    );
  }
});

describe('TimeRangeStrip', () => {
  it('redraws updated data without resetting same-size backing storage and rescales on DPR resize', () => {
    const widthSetter = vi.spyOn(HTMLCanvasElement.prototype, 'width', 'set');
    const heightSetter = vi.spyOn(HTMLCanvasElement.prototype, 'height', 'set');
    const firstBuckets: HistogramBucket[] = [
      {
        bucket_start_unix_secs: 0,
        total_count: 1,
        error_count: 0,
      },
    ];
    const nextBuckets: HistogramBucket[] = [
      {
        bucket_start_unix_secs: 0,
        total_count: 7,
        error_count: 2,
      },
      {
        bucket_start_unix_secs: 60,
        total_count: 3,
        error_count: 0,
      },
    ];
    const props = makeProps({ buckets: firstBuckets });
    const { container, rerender } = render(<TimeRangeStrip {...props} />);
    const canvas = container.querySelector('canvas') as HTMLCanvasElement;

    expect(canvas.width).toBe(400);
    expect(canvas.height).toBe(104);
    expect(widthSetter).toHaveBeenCalledTimes(1);
    expect(heightSetter).toHaveBeenCalledTimes(1);
    expect(context.setTransform).toHaveBeenCalledTimes(1);
    expect(context.setTransform).toHaveBeenLastCalledWith(1, 0, 0, 1, 0, 0);
    expect(context.clearRect).toHaveBeenCalledTimes(1);
    expect(context.fillRect).toHaveBeenCalledTimes(2);

    rerender(<TimeRangeStrip {...props} buckets={nextBuckets} />);

    expect(widthSetter).toHaveBeenCalledTimes(1);
    expect(heightSetter).toHaveBeenCalledTimes(1);
    expect(context.setTransform).toHaveBeenCalledTimes(2);
    expect(context.clearRect).toHaveBeenCalledTimes(2);
    expect(context.fillRect).toHaveBeenCalledTimes(6);

    vi.stubGlobal('devicePixelRatio', 2);
    act(() => window.dispatchEvent(new Event('resize')));

    expect(canvas.width).toBe(800);
    expect(canvas.height).toBe(208);
    expect(widthSetter).toHaveBeenCalledTimes(2);
    expect(heightSetter).toHaveBeenCalledTimes(2);
    expect(context.setTransform).toHaveBeenCalledTimes(3);
    expect(context.setTransform).toHaveBeenLastCalledWith(2, 0, 0, 2, 0, 0);
    expect(context.clearRect).toHaveBeenLastCalledWith(0, 0, 400, 104);

    act(() => window.dispatchEvent(new Event('resize')));
    expect(widthSetter).toHaveBeenCalledTimes(2);
    expect(heightSetter).toHaveBeenCalledTimes(2);
    expect(context.setTransform).toHaveBeenCalledTimes(3);
  });

  it('keeps listeners stable while an outside drag reads current data and callbacks', () => {
    const canvasAdd = vi.spyOn(HTMLCanvasElement.prototype, 'addEventListener');
    const canvasRemove = vi.spyOn(
      HTMLCanvasElement.prototype,
      'removeEventListener',
    );
    const windowAdd = vi.spyOn(window, 'addEventListener');
    const windowRemove = vi.spyOn(window, 'removeEventListener');
    const firstViewChange = vi.fn();
    const firstSelectionCommit = vi.fn();
    const currentViewChange = vi.fn();
    const currentSelectionCommit = vi.fn();
    const initialBuckets: HistogramBucket[] = [
      {
        bucket_start_unix_secs: 0,
        total_count: 1,
        error_count: 0,
      },
    ];
    const currentBuckets: HistogramBucket[] = [
      {
        bucket_start_unix_secs: 0,
        total_count: 7,
        error_count: 2,
      },
    ];
    const initialProps = makeProps({
      buckets: initialBuckets,
      onViewChange: firstViewChange,
      onSelectionCommit: firstSelectionCommit,
    });
    const { container, rerender } = render(
      <TimeRangeStrip {...initialProps} />,
    );
    const canvas = container.querySelector('canvas') as HTMLCanvasElement;
    const rectSpy = vi
      .spyOn(canvas, 'getBoundingClientRect')
      .mockImplementation(() => new DOMRect(0, 0, clientWidth, 104));

    expect(
      canvasAdd.mock.calls.filter(([type]) => type === 'mousemove'),
    ).toHaveLength(1);
    expect(
      canvasAdd.mock.calls.filter(([type]) => type === 'wheel'),
    ).toHaveLength(1);
    expect(
      windowAdd.mock.calls.filter(([type]) => type === 'mousemove'),
    ).toHaveLength(0);
    expect(
      windowAdd.mock.calls.filter(([type]) => type === 'mouseup'),
    ).toHaveLength(0);

    fireEvent.mouseMove(window, { clientX: 200, clientY: 90 });
    expect(rectSpy).not.toHaveBeenCalled();
    fireEvent.mouseMove(canvas, { clientX: 200, clientY: 90 });
    expect(rectSpy).toHaveBeenCalledTimes(1);
    expect(canvas.style.cursor).toBe('grab');

    fireEvent.mouseDown(canvas, { clientX: 100, clientY: 20, button: 0 });
    expect(
      windowAdd.mock.calls.filter(([type]) => type === 'mousemove'),
    ).toHaveLength(1);
    expect(
      windowAdd.mock.calls.filter(([type]) => type === 'mouseup'),
    ).toHaveLength(1);

    rerender(
      <TimeRangeStrip
        {...initialProps}
        buckets={currentBuckets}
        onViewChange={currentViewChange}
        onSelectionCommit={currentSelectionCommit}
      />,
    );

    expect(
      canvasAdd.mock.calls.filter(([type]) => type === 'mousemove'),
    ).toHaveLength(1);
    expect(
      canvasRemove.mock.calls.filter(([type]) => type === 'mousemove'),
    ).toHaveLength(0);
    expect(
      windowAdd.mock.calls.filter(([type]) => type === 'mousemove'),
    ).toHaveLength(1);
    expect(
      windowRemove.mock.calls.filter(([type]) => type === 'mousemove'),
    ).toHaveLength(0);

    fireEvent.mouseMove(window, { clientX: 450, clientY: 20 });
    expect(screen.getByText('2m · ~7 requests / ~2 err')).toBeDefined();
    fireEvent.mouseUp(window, { clientX: 450, clientY: 20 });

    expect(firstSelectionCommit).not.toHaveBeenCalled();
    expect(currentSelectionCommit).toHaveBeenCalledTimes(1);
    expect(currentSelectionCommit).toHaveBeenCalledWith({
      a: 30_000,
      b: 120_000,
    });
    expect(canvas.style.cursor).toBe('crosshair');
    expect(
      windowRemove.mock.calls.filter(([type]) => type === 'mousemove'),
    ).toHaveLength(1);
    expect(
      windowRemove.mock.calls.filter(([type]) => type === 'mouseup'),
    ).toHaveLength(1);

    const wheelEvent = new WheelEvent('wheel', {
      bubbles: true,
      cancelable: true,
      clientX: 200,
      deltaY: -1,
    });
    act(() => canvas.dispatchEvent(wheelEvent));
    expect(firstViewChange).not.toHaveBeenCalled();
    expect(currentViewChange).toHaveBeenCalledWith({
      a: 12_000,
      b: 108_000,
    });

    rectSpy.mockClear();
    fireEvent.mouseMove(window, { clientX: 250, clientY: 20 });
    expect(rectSpy).not.toHaveBeenCalled();
  });

  it('preserves non-passive wheel, zoom, pan, keyboard, and active-drag cleanup contracts', () => {
    const canvasAdd = vi.spyOn(HTMLCanvasElement.prototype, 'addEventListener');
    const canvasRemove = vi.spyOn(
      HTMLCanvasElement.prototype,
      'removeEventListener',
    );
    const windowAdd = vi.spyOn(window, 'addEventListener');
    const windowRemove = vi.spyOn(window, 'removeEventListener');
    const onViewChange = vi.fn();
    const onSelectionCommit = vi.fn();
    const { container, unmount } = render(
      <TimeRangeStrip
        {...makeProps({
          selection: { a: 30_000, b: 90_000 },
          onViewChange,
          onSelectionCommit,
        })}
      />,
    );
    const canvas = container.querySelector('canvas') as HTMLCanvasElement;
    vi.spyOn(canvas, 'getBoundingClientRect').mockImplementation(
      () => new DOMRect(0, 0, clientWidth, 104),
    );

    expect(
      canvasAdd.mock.calls.some(
        ([type, _listener, options]) =>
          type === 'wheel' &&
          (options as AddEventListenerOptions | undefined)?.passive === false,
      ),
    ).toBe(true);

    const wheelEvent = new WheelEvent('wheel', {
      bubbles: true,
      cancelable: true,
      clientX: 200,
      deltaY: -1,
    });
    act(() => canvas.dispatchEvent(wheelEvent));
    expect(wheelEvent.defaultPrevented).toBe(true);
    expect(onViewChange).toHaveBeenNthCalledWith(1, { a: 12_000, b: 108_000 });

    fireEvent.doubleClick(canvas, { clientX: 200, clientY: 70 });
    expect(onViewChange).toHaveBeenNthCalledWith(2, { a: 12_000, b: 108_000 });

    const tabEvent = new KeyboardEvent('keydown', {
      key: 'Tab',
      bubbles: true,
      cancelable: true,
    });
    act(() => window.dispatchEvent(tabEvent));
    expect(tabEvent.defaultPrevented).toBe(false);
    expect(onSelectionCommit).not.toHaveBeenCalled();

    fireEvent.keyDown(window, { key: 'Escape' });
    expect(onSelectionCommit).toHaveBeenCalledWith(null);

    fireEvent.mouseDown(canvas, { clientX: 200, clientY: 90, button: 0 });
    expect(canvas.style.cursor).toBe('grabbing');
    fireEvent.mouseMove(window, { clientX: 150, clientY: 90 });
    expect(onViewChange).toHaveBeenNthCalledWith(3, {
      a: 15_000,
      b: 135_000,
    });
    expect(
      windowAdd.mock.calls.filter(([type]) => type === 'mousemove'),
    ).toHaveLength(1);
    expect(
      windowAdd.mock.calls.filter(([type]) => type === 'mouseup'),
    ).toHaveLength(1);

    unmount();

    for (const type of [
      'mousedown',
      'mousemove',
      'wheel',
      'dblclick',
      'mouseleave',
    ]) {
      expect(
        canvasRemove.mock.calls.filter(([event]) => event === type),
      ).toHaveLength(1);
    }
    expect(
      windowRemove.mock.calls.filter(([type]) => type === 'mousemove'),
    ).toHaveLength(1);
    expect(
      windowRemove.mock.calls.filter(([type]) => type === 'mouseup'),
    ).toHaveLength(1);
    expect(
      windowRemove.mock.calls.filter(([type]) => type === 'keydown'),
    ).toHaveLength(1);
    expect(
      windowRemove.mock.calls.filter(([type]) => type === 'resize'),
    ).toHaveLength(1);
  });
});
