import { cleanup, render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest';
import * as queries from '../../../lib/queries';
import { makeOauthUpstream } from '../../../lib/test-utils/warmup-fixtures';
import { WarmupCardMinimal } from './WarmupCardMinimal';

vi.mock('../../../lib/queries', async () => {
  const actual = await vi.importActual<Record<string, unknown>>(
    '../../../lib/queries',
  );
  return {
    ...actual,
    useClearUpstreamWarmupDialectPlugin: vi.fn(),
    useFireNowUpstreamWarmup: vi.fn(),
    usePluginRegistry: vi.fn(),
    useUpdateUpstreamWarmupSettings: vi.fn(),
    useWarmupSummary: vi.fn(),
  };
});

vi.mock('./WarmupHistoryDrawer', () => ({
  WarmupHistoryDrawer: () => null,
}));

const upstream = makeOauthUpstream();

function makeAttempt(
  overrides: Partial<queries.WarmupAttempt> = {},
): queries.WarmupAttempt {
  return {
    id: 'attempt-1',
    upstream_id: upstream.id,
    attempted_at_unix_secs: 1_718_380_800,
    completed_at_unix_secs: 1_718_380_801,
    scheduled_for_unix_secs: 1_718_380_800,
    trigger: 'scheduled',
    status: 'success',
    reason: 'cycle_advanced',
    dispatch_kind: 'http',
    http_status: 200,
    cycle_key: 1_718_380_800,
    expected_cycle_key: 1_718_380_800,
    idle_secs_since_prev_window: 300,
    replica_id: 'replica-1',
    lease_holder: 'replica-1',
    upstream_spec_revision: 1,
    dialect_plugin_snapshot: null,
    error_detail: null,
    ...overrides,
  };
}

function makeSummary(
  lastAttempt: queries.WarmupAttempt,
  dialectPlugin: queries.WarmupDialectPluginSnapshot | null = null,
): queries.WarmupSummary {
  return {
    upstream_id: upstream.id,
    last_attempt: lastAttempt,
    recent_attempts: [lastAttempt],
    next_scheduled_at_unix_secs: 1_718_398_800,
    recent_summary_7d: {
      success: lastAttempt.status === 'success' ? 1 : 0,
      skipped: lastAttempt.status === 'skipped' ? 1 : 0,
      transient_failure: lastAttempt.status === 'transient_failure' ? 1 : 0,
      permanent_failure: lastAttempt.status === 'permanent_failure' ? 1 : 0,
    },
    dialect_plugin: dialectPlugin,
  };
}

function setSummaryQuery(
  data: queries.WarmupSummary | undefined,
  isPending: boolean,
) {
  vi.mocked(queries.useWarmupSummary).mockReturnValue({
    data,
    isLoading: isPending,
    isPending,
  } as never);
}

function setRegistryPending(isPending: boolean) {
  vi.mocked(queries.usePluginRegistry).mockReturnValue({
    data: isPending ? undefined : { entries: [] },
    isLoading: isPending,
    isPending,
  } as never);
}

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(queries.useUpdateUpstreamWarmupSettings).mockReturnValue({
    isPending: false,
    mutate: vi.fn(),
  } as never);
  vi.mocked(queries.useClearUpstreamWarmupDialectPlugin).mockReturnValue({
    isPending: false,
    mutate: vi.fn(),
  } as never);
  vi.mocked(queries.useFireNowUpstreamWarmup).mockReturnValue({
    isPending: false,
    mutate: vi.fn(),
  } as never);
  setRegistryPending(false);
  setSummaryQuery(makeSummary(makeAttempt()), false);
});

afterEach(cleanup);

describe('WarmupCardMinimal provider availability', () => {
  test.each(['anthropic_oauth', 'openai_oauth', 'gemini_oauth'] as const)(
    '%s shows an enabled warmup control, status and history',
    (kind) => {
      render(<WarmupCardMinimal upstream={makeOauthUpstream({ kind })} />);

      const toggle = screen.getByRole('switch', { name: 'Toggle warmup' });
      expect((toggle as HTMLInputElement).checked).toBe(true);
      expect((toggle as HTMLInputElement).disabled).toBe(false);
      expect(screen.getByTestId('warmup-status-value').textContent).toBe(
        'Healthy',
      );
      expect(screen.getByRole('button', { name: 'History' })).toBeTruthy();
      expect(
        (screen.getByRole('button', { name: 'Fire now' }) as HTMLButtonElement)
          .disabled,
      ).toBe(false);
      expect(screen.getByTestId('warmup-plugin-row')).toBeTruthy();
      expect(
        screen.getByText(
          kind === 'gemini_oauth'
            ? 'Prepares the next Gemini models quota window during idle gaps.'
            : 'Prepares the next quota window during idle gaps.',
        ),
      ).toBeTruthy();
    },
  );

  test.each(['anthropic_api_key', 'openai_api_key', 'gemini_api_key'] as const)(
    '%s has no warmup control',
    (kind) => {
      const plugin = { wasm_registry_id: 'unsupported-shape', config: {} };
      setSummaryQuery(makeSummary(makeAttempt(), plugin), false);
      render(
        <WarmupCardMinimal
          upstream={makeOauthUpstream({ kind, warmup_dialect_plugin: plugin })}
        />,
      );
      expect(
        screen.queryByRole('button', { name: 'View plugin config' }),
      ).toBeNull();
      expect(screen.queryByRole('dialog')).toBeNull();
      expect(screen.queryByTestId('warmup-card')).toBeNull();
      expect(queries.useWarmupSummary).not.toHaveBeenCalled();
    },
  );
});
describe('WarmupCardMinimal current plugin config', () => {
  test.each(['anthropic_oauth', 'openai_oauth', 'gemini_oauth'] as const)(
    '%s opens the effective plugin snapshot',
    async (kind) => {
      const plugin: queries.WarmupDialectPluginSnapshot = {
        wasm_registry_id: `${kind}-shape`,
        wire_version: 7,
        config: { provider: kind, timeout_ms: 123 },
      };
      setSummaryQuery(makeSummary(makeAttempt(), plugin), false);
      render(<WarmupCardMinimal upstream={makeOauthUpstream({ kind })} />);

      await userEvent
        .setup()
        .click(screen.getByRole('button', { name: 'View plugin config' }));

      const dialog = screen.getByRole('dialog', { name: /-shape/ });
      expect(dialog.textContent).toContain(plugin.wasm_registry_id);
      expect(dialog.textContent).toContain('wire v7');
      expect(dialog.querySelector('pre')?.textContent).toBe(
        JSON.stringify(plugin.config, null, 2),
      );
      expect(screen.getByRole('button', { name: 'Copy JSON' })).toBeTruthy();
    },
  );

  test('prefers the summary snapshot over the configured plugin', async () => {
    const configuredPlugin: queries.WarmupDialectPluginSnapshot = {
      wasm_registry_id: 'configured-shape',
      wire_version: 2,
      config: { source: 'configured' },
    };
    const summaryPlugin: queries.WarmupDialectPluginSnapshot = {
      wasm_registry_id: 'summary-shape',
      wire_version: 8,
      config: { source: 'summary' },
    };
    setSummaryQuery(makeSummary(makeAttempt(), summaryPlugin), false);
    render(
      <WarmupCardMinimal
        upstream={makeOauthUpstream({
          warmup_dialect_plugin: configuredPlugin,
        })}
      />,
    );

    await userEvent
      .setup()
      .click(screen.getByRole('button', { name: 'View plugin config' }));

    const dialog = screen.getByRole('dialog', { name: /summary-shape/ });
    expect(dialog.textContent).toContain('"source": "summary"');
    expect(dialog.textContent).not.toContain('configured-shape');
    expect(dialog.textContent).not.toContain('"source": "configured"');
  });

  test.each([false, true])(
    'falls back to the configured plugin with summary pending=%s',
    async (pending) => {
      const configuredPlugin: queries.WarmupDialectPluginSnapshot = {
        wasm_registry_id: 'configured-shape',
        config: { source: 'configured' },
      };
      setSummaryQuery(
        pending ? undefined : makeSummary(makeAttempt()),
        pending,
      );
      render(
        <WarmupCardMinimal
          upstream={makeOauthUpstream({
            warmup_dialect_plugin: configuredPlugin,
          })}
        />,
      );

      await userEvent
        .setup()
        .click(screen.getByRole('button', { name: 'View plugin config' }));

      const dialog = screen.getByRole('dialog', { name: /configured-shape/ });
      expect(dialog.querySelector('pre')?.textContent).toBe(
        JSON.stringify(configuredPlugin.config, null, 2),
      );
      expect(dialog.textContent).not.toContain('wire v');
    },
  );

  test('ignores an unsaved plugin selection when opening the snapshot', async () => {
    const summaryPlugin: queries.WarmupDialectPluginSnapshot = {
      wasm_registry_id: 'summary-shape',
      config: { source: 'summary' },
    };
    setSummaryQuery(makeSummary(makeAttempt(), summaryPlugin), false);
    vi.mocked(queries.usePluginRegistry).mockReturnValue({
      data: {
        entries: [
          {
            id: 'new-shape',
            name: 'New shape',
            sha256_hex: 'a'.repeat(64),
            original_filename: 'new-shape.wasm',
            description: 'fixture',
            usage: 'fixture',
            hook_metadata: {
              shape_anthropic: {
                wire_version: 1,
                description: 'fixture',
                usage: 'fixture',
                mode: 'active',
              },
            },
            label: null,
            size_bytes: 1,
            refcount: 0,
            revision: 1,
            uploaded_at_unix_secs: 1,
            metadata: null,
            supported_slots: ['shape'],
          },
        ],
      },
      isPending: false,
    } as never);
    render(
      <WarmupCardMinimal
        upstream={makeOauthUpstream({
          warmup_dialect_plugin: null,
        })}
      />,
    );

    const user = userEvent.setup();
    await user.click(screen.getByTestId('warmup-plugin-select'));
    await user.click(await screen.findByRole('option', { name: 'New shape' }));
    await user.click(
      screen.getByRole('button', { name: 'View plugin config' }),
    );

    const dialog = screen.getByRole('dialog', { name: /summary-shape/ });
    expect(dialog.textContent).toContain('"source": "summary"');
    expect(dialog.textContent).not.toContain('new-shape');
  });

  test('copies the pretty config without editing', async () => {
    const writeText = vi
      .spyOn(navigator.clipboard, 'writeText')
      .mockResolvedValue(undefined);

    try {
      const plugin: queries.WarmupDialectPluginSnapshot = {
        wasm_registry_id: 'copy-shape',
        config: { enabled: true, nested: { value: 42 } },
      };
      setSummaryQuery(makeSummary(makeAttempt(), plugin), false);
      render(<WarmupCardMinimal upstream={upstream} />);

      const user = userEvent.setup();
      await user.click(
        screen.getByRole('button', { name: 'View plugin config' }),
      );
      await user.click(screen.getByRole('button', { name: 'Copy JSON' }));

      const copied = await screen.findByRole('button', { name: 'Copied' });
      expect(copied).toBeTruthy();
      expect(writeText).toHaveBeenCalledWith(
        JSON.stringify(plugin.config, null, 2),
      );
      expect(screen.queryByRole('button', { name: 'Save' })).toBeNull();
    } finally {
      writeText.mockRestore();
    }
  });

  test.each(['anthropic_oauth', 'openai_oauth', 'gemini_oauth'] as const)(
    '%s does not show an opener without an effective snapshot',
    (kind) => {
      render(<WarmupCardMinimal upstream={makeOauthUpstream({ kind })} />);
      expect(screen.getByTestId('warmup-card')).toBeTruthy();
      expect(
        screen.queryByRole('button', { name: 'View plugin config' }),
      ).toBeNull();
      expect(screen.queryByRole('dialog')).toBeNull();
    },
  );
});

test.each(['anthropic_oauth', 'openai_oauth', 'gemini_oauth'] as const)(
  '%s offers only provider-capable warmup shape plugins',
  async (kind) => {
    const providers = ['anthropic', 'openai', 'gemini'] as const;
    const provider = kind.replace('_oauth', '');
    const entries: queries.PluginEntry[] = [
      ...providers,
      'legacy',
      'response-only',
    ].map((name) => ({
      id: `plugin-${name}`,
      name,
      sha256_hex: 'a'.repeat(64),
      original_filename: `${name}.wasm`,
      description: 'fixture',
      usage: 'fixture',
      hook_metadata: {
        [name === 'legacy'
          ? 'shape'
          : name === 'response-only'
            ? `transform_response_${provider}`
            : `shape_${name}`]: {
          wire_version: 1,
          description: 'fixture',
          usage: 'fixture',
          mode: 'active',
        },
      },
      label: null,
      size_bytes: 1,
      refcount: 0,
      revision: 1,
      uploaded_at_unix_secs: 1,
      metadata: null,
      supported_slots: ['shape'],
    }));
    vi.mocked(queries.usePluginRegistry).mockReturnValue({
      data: { entries },
      isPending: false,
    } as never);
    render(<WarmupCardMinimal upstream={makeOauthUpstream({ kind })} />);
    const user = userEvent.setup();
    await user.click(screen.getByTestId('warmup-plugin-select'));
    expect(await screen.findByRole('option', { name: provider })).toBeTruthy();
    for (const other of providers.filter((name) => name !== provider)) {
      expect(screen.queryByRole('option', { name: other })).toBeNull();
    }
    expect(screen.queryByRole('option', { name: 'response-only' })).toBeNull();
    expect(screen.queryByRole('option', { name: 'legacy' }) !== null).toBe(
      provider === 'anthropic',
    );
    await user.click(screen.getByRole('option', { name: provider }));
    expect(
      vi.mocked(queries.useUpdateUpstreamWarmupSettings).mock.results.at(-1)
        ?.value.mutate,
    ).toHaveBeenCalledWith(
      expect.objectContaining({
        body: {
          warmup_dialect_plugin: {
            wasm_registry_id: `plugin-${provider}`,
            config: {},
          },
        },
      }),
      expect.any(Object),
    );
  },
);

/**
 * The badge inside the Last run field: the StatusBadge wrapper that carries the
 * text color class, plus its `.status-dot` span.
 */
function lastRunBadge(label: string) {
  const lastRun = screen.getByTestId('warmup-last');
  const labelEl = within(lastRun).getByText(label);
  const badge = labelEl.parentElement;
  const dot = badge?.querySelector('.status-dot');
  return { badge, dot };
}
describe('WarmupCardMinimal last-run status colors', () => {
  test('success renders the green outcome', () => {
    setSummaryQuery(makeSummary(makeAttempt({ status: 'success' })), false);
    render(<WarmupCardMinimal upstream={upstream} />);

    const { badge, dot } = lastRunBadge('Success');
    expect(dot?.classList.contains('ok')).toBe(true);
    expect(badge?.classList.contains('text-traffic-success-text')).toBe(true);
  });

  test('transient failure renders warn amber', () => {
    setSummaryQuery(
      makeSummary(
        makeAttempt({
          status: 'transient_failure',
          reason: 'upstream_5xx',
          http_status: 503,
        }),
      ),
      false,
    );
    render(<WarmupCardMinimal upstream={upstream} />);

    const { badge, dot } = lastRunBadge('Retrying');
    expect(dot?.classList.contains('warn')).toBe(true);
    expect(badge?.classList.contains('text-warn-text')).toBe(true);
  });

  test('permanent failure renders danger', () => {
    setSummaryQuery(
      makeSummary(
        makeAttempt({
          status: 'permanent_failure',
          reason: 'auth_failed',
          http_status: 401,
        }),
      ),
      false,
    );
    render(<WarmupCardMinimal upstream={upstream} />);

    const { badge, dot } = lastRunBadge('Failed');
    expect(dot?.classList.contains('danger')).toBe(true);
    expect(badge?.classList.contains('text-danger-text')).toBe(true);
  });

  test('skipped renders neutral, not a green success', () => {
    setSummaryQuery(
      makeSummary(
        makeAttempt({ status: 'skipped', reason: 'upstream_disabled' }),
      ),
      false,
    );
    render(<WarmupCardMinimal upstream={upstream} />);

    const { badge, dot } = lastRunBadge('Skipped');
    expect(dot?.classList.contains('neutral')).toBe(true);
    expect(dot?.classList.contains('ok')).toBe(false);
    expect(badge?.classList.contains('text-traffic-success-text')).toBe(false);
    expect(badge?.classList.contains('text-text-muted')).toBe(true);
  });

  test('last-run timestamp stays neutral metadata, not status-colored', () => {
    render(<WarmupCardMinimal upstream={upstream} />);

    const lastRun = screen.getByTestId('warmup-last');
    const trigger = within(lastRun).getByRole('button', {
      name: 'Open last warm-up attempt detail',
    });
    // The badge carries the state color; the timestamp next to it does not.
    const stamps = trigger.querySelectorAll('span.cursor-help');
    expect(stamps.length).toBeGreaterThan(0);
    for (const el of stamps) {
      for (const cls of [
        'text-warn-text',
        'text-danger-text',
        'text-traffic-success-text',
      ]) {
        expect(el.classList.contains(cls)).toBe(false);
      }
    }
  });

  test('an enabled warm-up with no attempts stays neutral — Never, no dot', () => {
    setSummaryQuery(
      {
        ...makeSummary(makeAttempt()),
        last_attempt: null,
        recent_attempts: [],
      },
      false,
    );
    render(
      <WarmupCardMinimal
        upstream={makeOauthUpstream({ last_warmup_at_unix_secs: null })}
      />,
    );

    const lastRun = screen.getByTestId('warmup-last');
    const never = lastRun.querySelector('.text-text-muted');
    expect(never?.textContent).toBe('Never');
    expect(lastRun.querySelector('.status-dot')).toBeNull();
  });

  test('an enabled warm-up awaiting its first attempt shows live Pending', () => {
    setSummaryQuery(
      {
        ...makeSummary(makeAttempt()),
        last_attempt: null,
        recent_attempts: [],
      },
      false,
    );
    render(<WarmupCardMinimal upstream={upstream} />);

    const statusValue = screen.getByTestId('warmup-status-value');
    expect(statusValue.textContent).toBe('Pending');
    const dot = statusValue.querySelector('.status-dot');
    expect(dot?.classList.contains('live')).toBe(true);
    expect(dot?.classList.contains('neutral')).toBe(false);
  });
});
