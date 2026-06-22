import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  within,
} from '@testing-library/react';
import { toast } from 'sonner';
import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest';
import { ApiError } from '../../lib/api';
import {
  COPY,
  FIRE_NOW_COOLDOWN_MS,
  LEASE_PANEL_AUTO_DISMISS_MS,
  TOAST_DURATIONS,
} from '../../lib/copy/warmup';
import * as queries from '../../lib/queries';
import {
  makeApiKeyUpstream,
  makeOauthUpstream,
  makeRouterPlugin,
  makeShapePlugin,
} from '../../lib/test-utils/warmup-fixtures';
import { RelativeTime } from '../ui/RelativeTime';

vi.mock('sonner', () => ({
  toast: {
    success: vi.fn(),
    error: vi.fn(),
  },
}));

vi.mock('../../lib/format', () => ({
  formatRelativeUnixSeconds: vi.fn(
    (seconds: number) => new Date(seconds * 1000),
  ),
}));

vi.mock('../../lib/queries', async () => {
  const actual =
    await vi.importActual<typeof import('../../lib/queries')>(
      '../../lib/queries',
    );
  return {
    ...actual,
    useFireNowUpstreamWarmup: vi.fn(),
    useClearUpstreamWarmupDialectPlugin: vi.fn(),
    useUpdateUpstreamWarmupSettings: vi.fn(),
    usePluginRegistry: vi.fn(),
  };
});

vi.mock('../ui/RelativeTime', () => ({
  RelativeTime: vi.fn(({ ts }: { ts: Date | number | null | undefined }) => (
    <span data-testid="rel-time">{String(ts)}</span>
  )),
}));

vi.mock('../ui/primitives', async () => {
  const actual =
    await vi.importActual<typeof import('../ui/primitives')>(
      '../ui/primitives',
    );
  const React = await vi.importActual<typeof import('react')>('react');

  function ConfirmDialog({
    open,
    onOpenChange,
    title,
    description,
    confirmLabel = 'Confirm',
    cancelLabel = 'Cancel',
    onConfirm,
    confirmDisabled = false,
  }: {
    open: boolean;
    onOpenChange: (open: boolean) => void;
    title: React.ReactNode;
    description?: React.ReactNode;
    confirmLabel?: string;
    cancelLabel?: string;
    onConfirm: () => void;
    confirmDisabled?: boolean;
  }) {
    const confirmRef = React.useRef<HTMLButtonElement>(null);

    React.useEffect(() => {
      if (open) confirmRef.current?.focus();
    }, [open]);

    if (!open) return null;

    return (
      <div aria-label={String(title)} role="dialog">
        {description ? <p>{description}</p> : null}
        <button type="button" onClick={() => onOpenChange(false)}>
          {cancelLabel}
        </button>
        <button
          ref={confirmRef}
          type="button"
          disabled={confirmDisabled}
          onClick={onConfirm}
        >
          {confirmLabel}
        </button>
      </div>
    );
  }

  return { ...actual, ConfirmDialog };
});

const { formatRelativeUnixSeconds } = await import('../../lib/format');
const { WarmupCard } = await import('./WarmupCard');

type MutationMock = {
  mutate: ReturnType<typeof vi.fn>;
  isPending: boolean;
};

function makeMutation(): MutationMock {
  return { mutate: vi.fn(), isPending: false };
}

let fireNowMutation: MutationMock;
let clearMutation: MutationMock;
let patchMutation: MutationMock;

function mockHooks({
  registryEntries = [makeShapePlugin('shape-1', 'shape-one')],
}: {
  registryEntries?: ReturnType<typeof makeShapePlugin>[];
} = {}) {
  vi.mocked(queries.useFireNowUpstreamWarmup).mockReturnValue(
    fireNowMutation as unknown as ReturnType<
      typeof queries.useFireNowUpstreamWarmup
    >,
  );
  vi.mocked(queries.useClearUpstreamWarmupDialectPlugin).mockReturnValue(
    clearMutation as unknown as ReturnType<
      typeof queries.useClearUpstreamWarmupDialectPlugin
    >,
  );
  vi.mocked(queries.useUpdateUpstreamWarmupSettings).mockReturnValue(
    patchMutation as unknown as ReturnType<
      typeof queries.useUpdateUpstreamWarmupSettings
    >,
  );
  vi.mocked(queries.usePluginRegistry).mockReturnValue({
    data: { entries: registryEntries },
  } as unknown as ReturnType<typeof queries.usePluginRegistry>);
}

function renderWarmup(upstream = makeOauthUpstream()) {
  return render(<WarmupCard upstream={upstream} />);
}

function clickFireConfirm() {
  fireEvent.click(screen.getByTestId('warmup-fire-now'));
  fireEvent.click(
    screen.getByRole('button', { name: COPY.confirmFireConfirmLabel }),
  );
}

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(formatRelativeUnixSeconds).mockImplementation(
    (seconds: number) => new Date(seconds * 1000),
  );
  fireNowMutation = makeMutation();
  clearMutation = makeMutation();
  patchMutation = makeMutation();
  mockHooks();
});

afterEach(() => {
  cleanup();
  vi.useRealTimers();
});

describe('WarmupCard', () => {
  test('api_key renders null without running the plugin registry hook', () => {
    const { container } = render(
      <WarmupCard upstream={makeApiKeyUpstream()} />,
    );

    expect(container.firstChild).toBeNull();
    expect(queries.usePluginRegistry).toHaveBeenCalledTimes(0);
  });

  test('OAuth disabled renders EmptyState and Enable button without consuming registry data', () => {
    renderWarmup(makeOauthUpstream({ warmup_enabled: false }));

    expect(screen.getByText(COPY.disabledEmpty)).toBeDefined();
    expect(screen.getByTestId('warmup-enable-btn')).toBeDefined();
    expect(screen.queryByTestId('warmup-fire-now')).toBeNull();
    expect(queries.usePluginRegistry).toHaveBeenCalledTimes(1);
    expect(screen.queryByTestId('warmup-plugin-select')).toBeNull();
  });

  test('OAuth enabled with null last warmup renders never-warmed copy', () => {
    renderWarmup(
      makeOauthUpstream({
        warmup_enabled: true,
        last_warmup_at_unix_secs: null,
      }),
    );

    expect(screen.getByText(COPY.lastNull)).toBeDefined();
  });

  test('OAuth enabled with populated last warmup renders relative time label', () => {
    const upstream = makeOauthUpstream({
      last_warmup_at_unix_secs: 1718380800,
    });

    renderWarmup(upstream);

    expect(formatRelativeUnixSeconds).toHaveBeenCalledWith(
      upstream.status.last_warmup_at_unix_secs,
    );
    const formattedLast = vi.mocked(formatRelativeUnixSeconds).mock.results[0]
      ?.value as Date;
    expect(vi.mocked(RelativeTime).mock.calls[0]?.[0].ts).toBe(formattedLast);
    expect(screen.getByTestId('rel-time').textContent).toBe(
      String(formattedLast),
    );
  });

  test('zero shape plugins renders notice and Plugins link without select', () => {
    mockHooks({ registryEntries: [makeRouterPlugin('r1', 'router-only')] });

    renderWarmup();

    expect(
      screen.getByText(COPY.noShapePluginsAvailable, { exact: false }),
    ).toBeDefined();
    expect(screen.getByRole('link', { name: /Plugins/i })).toBeDefined();
    expect(screen.queryByTestId('warmup-plugin-select')).toBeNull();
  });

  test('unknown stored plugin renders unknown option in the dropdown', () => {
    renderWarmup(
      makeOauthUpstream({
        warmup_dialect_plugin: { wasm_registry_id: 'ghost', config: {} },
      }),
    );

    const select = screen.getByTestId('warmup-plugin-select');
    expect(
      within(select).getByRole('option', {
        name: COPY.unknownPluginTemplate.replace('{id}', 'ghost'),
      }),
    ).toBeDefined();
    expect(
      within(select).getByRole('option', { name: COPY.defaultPluginOption }),
    ).toBeDefined();
  });

  test('fire-now confirm calls mutation once, disables pending controls, and shows success toast', () => {
    vi.useFakeTimers();
    const upstream = makeOauthUpstream();
    let callbacks:
      | {
          onSuccess?: (value: { fired: true; cycle_key: number }) => void;
        }
      | undefined;
    const { rerender } = renderWarmup(upstream);
    fireNowMutation.mutate.mockImplementation((_id, opts) => {
      callbacks = opts;
      fireNowMutation.isPending = true;
    });

    clickFireConfirm();

    expect(fireNowMutation.mutate).toHaveBeenCalledTimes(1);
    expect(fireNowMutation.mutate.mock.calls[0]?.[0]).toBe(upstream.id);

    rerender(<WarmupCard upstream={upstream} />);
    expect(
      (screen.getByTestId('warmup-fire-now') as HTMLButtonElement).disabled,
    ).toBe(true);
    expect(
      (
        screen.getByRole('button', {
          name: COPY.confirmFireConfirmLabel,
        }) as HTMLButtonElement
      ).disabled,
    ).toBe(true);

    fireNowMutation.isPending = false;
    act(() => {
      callbacks?.onSuccess?.({ fired: true, cycle_key: 1718380800 });
    });
    rerender(<WarmupCard upstream={upstream} />);

    expect(toast.success).toHaveBeenCalledWith(COPY.fireSuccess, {
      duration: TOAST_DURATIONS.success,
    });
    expect(
      (screen.getByTestId('warmup-fire-now') as HTMLButtonElement).disabled,
    ).toBe(true);

    act(() => {
      vi.advanceTimersByTime(FIRE_NOW_COOLDOWN_MS);
    });
    expect(
      (screen.getByTestId('warmup-fire-now') as HTMLButtonElement).disabled,
    ).toBe(false);
  });

  test('fire-now 202 renders lease panel and auto-dismisses without error toast', () => {
    vi.useFakeTimers();
    fireNowMutation.mutate.mockImplementation((_id, opts) => {
      opts?.onSuccess?.({
        fired: false,
        reason: 'lease_held',
        held_by: 'background-loop',
      });
    });

    renderWarmup();
    clickFireConfirm();

    const panel = screen.getByTestId('warmup-lease-panel');
    expect(panel.getAttribute('aria-live')).toBe('polite');
    expect(panel.textContent).toContain('background-loop');
    expect(panel.textContent).toContain('Try again in ~30 seconds');
    expect(toast.error).not.toHaveBeenCalled();

    act(() => {
      vi.advanceTimersByTime(LEASE_PANEL_AUTO_DISMISS_MS);
    });
    expect(screen.queryByTestId('warmup-lease-panel')).toBeNull();
  });

  test.each([
    'auth_failed',
    'forbidden',
    'bad_request',
    'not_found',
    'dialect_plugin_failed',
  ] as const)('fire-now 502 reason %s renders exact error copy', (reason) => {
    fireNowMutation.mutate.mockImplementation((_id, opts) => {
      opts?.onSuccess?.({ fired: false, reason });
    });

    renderWarmup();
    clickFireConfirm();

    const panel = screen.getByTestId('warmup-error-panel');
    expect(panel.getAttribute('data-reason')).toBe(reason);
    expect(panel.textContent).toBe(COPY.fireErrorReasons[reason]);
  });

  test('fire-now 503 transient renders transient error copy', () => {
    fireNowMutation.mutate.mockImplementation((_id, opts) => {
      opts?.onSuccess?.({ fired: false, reason: 'transient' });
    });

    renderWarmup();
    clickFireConfirm();

    const panel = screen.getByTestId('warmup-error-panel');
    expect(panel.getAttribute('data-reason')).toBe('transient');
    expect(panel.textContent).toBe(COPY.fireErrorReasons.transient);
  });

  test('PATCH 409 stale_revision shows hint and preserves pending plugin value', () => {
    mockHooks({
      registryEntries: [
        makeShapePlugin('shape-1', 'shape-one'),
        makeShapePlugin('shape-2', 'shape-two'),
      ],
    });
    patchMutation.mutate.mockImplementation((_patch, opts) => {
      opts?.onError?.(
        new ApiError(409, null, { error: 'stale_revision' }, 'Conflict'),
      );
    });

    renderWarmup();
    const select = screen.getByTestId(
      'warmup-plugin-select',
    ) as HTMLSelectElement;

    fireEvent.change(select, { target: { value: 'shape-2' } });

    expect(patchMutation.mutate).toHaveBeenCalledTimes(1);
    expect(screen.getByTestId('warmup-stale-hint').textContent).toBe(
      COPY.staleRevisionHint,
    );
    expect(select.value).toBe('shape-2');
  });

  test('a11y attributes connect switch, select, live panels, and focused confirm action', () => {
    fireNowMutation.mutate
      .mockImplementationOnce((_id, opts) => {
        opts?.onSuccess?.({
          fired: false,
          reason: 'lease_held',
          held_by: 'background-loop',
        });
      })
      .mockImplementationOnce((_id, opts) => {
        opts?.onSuccess?.({ fired: false, reason: 'transient' });
      });

    const { rerender } = renderWarmup(
      makeOauthUpstream({ warmup_enabled: true }),
    );
    expect(screen.getByRole('switch').getAttribute('aria-checked')).toBe(
      'true',
    );

    const select = screen.getByTestId(
      'warmup-plugin-select',
    ) as HTMLSelectElement;
    const label = document.querySelector(`label[for="${select.id}"]`);
    expect(label?.textContent).toBe(COPY.dialectPluginLabel);

    fireEvent.click(screen.getByTestId('warmup-fire-now'));
    const confirmButton = screen.getByRole('button', {
      name: COPY.confirmFireConfirmLabel,
    });
    expect(document.activeElement).toBe(confirmButton);
    fireEvent.click(confirmButton);
    expect(
      screen.getByTestId('warmup-lease-panel').getAttribute('aria-live'),
    ).toBe('polite');

    clickFireConfirm();
    expect(
      screen.getByTestId('warmup-error-panel').getAttribute('aria-live'),
    ).toBe('polite');

    rerender(
      <WarmupCard upstream={makeOauthUpstream({ warmup_enabled: false })} />,
    );
    expect(screen.getByRole('switch').getAttribute('aria-checked')).toBe(
      'false',
    );
  });
});
