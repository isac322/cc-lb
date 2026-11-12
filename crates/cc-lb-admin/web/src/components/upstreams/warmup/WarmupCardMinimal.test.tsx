import { act, cleanup, render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest';
import * as queries from '../../../lib/queries';
import {
  makeFireNowLeaseHeld,
  makeFireNowSuccess,
  makeGeminiWarmupSummary,
  makeOauthUpstream,
  makeWarmupAttempt,
  makeWarmupLane,
  makeWarmupSummary,
} from '../../../lib/test-utils/warmup-fixtures';
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
  return makeWarmupAttempt({ upstream_id: upstream.id, ...overrides });
}

function makeSummary(
  lastAttempt: queries.WarmupAttempt,
  dialectPlugin: queries.WarmupDialectPluginSnapshot | null = null,
): queries.WarmupSummary {
  return makeWarmupSummary(
    [makeWarmupLane({ recent_attempts: [lastAttempt] })],
    {
      upstreamId: upstream.id,
      dialectPlugin,
    },
  );
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
    mutateAsync: vi.fn().mockResolvedValue(makeFireNowSuccess()),
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
            ? "Prepares each model group's next quota window during idle gaps."
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
      makeWarmupSummary([makeWarmupLane()], { upstreamId: upstream.id }),
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
      makeWarmupSummary([makeWarmupLane()], { upstreamId: upstream.id }),
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

const gemini = makeOauthUpstream({ kind: 'gemini_oauth' });

function failure(
  lane: queries.WarmupLane,
  status: 'transient_failure' | 'permanent_failure',
): queries.WarmupAttempt {
  return makeAttempt({
    id: `${lane}-${status}`,
    lane,
    status,
    reason: status === 'permanent_failure' ? 'auth_failed' : 'upstream_5xx',
    http_status: status === 'permanent_failure' ? 401 : 503,
  });
}

function success(lane: queries.WarmupLane): queries.WarmupAttempt {
  return makeAttempt({ id: `${lane}-success`, lane });
}

describe('WarmupCardMinimal lanes', () => {
  test('a single-lane upstream keeps the plain Next run / Last run grid', () => {
    render(<WarmupCardMinimal upstream={upstream} />);

    expect(screen.queryByTestId('warmup-lanes')).toBeNull();
    expect(screen.getByTestId('warmup-next-value')).toBeTruthy();
    expect(screen.getByTestId('warmup-last').textContent).toContain('Success');
    expect(screen.queryByText('Gemini models')).toBeNull();
    expect(screen.queryByText('Claude and GPT-OSS models')).toBeNull();
  });

  test('Gemini renders one row per model group with its own windows and runs', () => {
    setSummaryQuery(
      makeGeminiWarmupSummary({
        upstreamId: gemini.id,
        primary: { recent_attempts: [success('primary')] },
        thirdParty: {
          recent_attempts: [failure('third_party', 'transient_failure')],
        },
      }),
      false,
    );
    render(<WarmupCardMinimal upstream={gemini} />);

    const primaryRow = screen.getByTestId('warmup-lane-primary');
    const thirdRow = screen.getByTestId('warmup-lane-third_party');
    expect(within(primaryRow).getByText('Gemini models')).toBeTruthy();
    expect(
      within(thirdRow).getByText('Claude and GPT-OSS models'),
    ).toBeTruthy();
    expect(within(primaryRow).getByText('Gemini 5h')).toBeTruthy();
    expect(within(primaryRow).getByText('Gemini 7d')).toBeTruthy();
    expect(within(primaryRow).queryByText('Claude·OSS 5h')).toBeNull();
    expect(within(thirdRow).getByText('Claude·OSS 5h')).toBeTruthy();
    expect(within(thirdRow).getByText('Claude·OSS 7d')).toBeTruthy();
    expect(screen.getByTestId('warmup-last-primary').textContent).toContain(
      'Success',
    );
    expect(screen.getByTestId('warmup-last-third_party').textContent).toContain(
      'Retrying',
    );
    expect(screen.getByTestId('warmup-next-value-primary')).toBeTruthy();
    expect(screen.getByTestId('warmup-next-value-third_party')).toBeTruthy();
    // The single-lane grid is replaced, not duplicated.
    expect(screen.queryByTestId('warmup-last')).toBeNull();
  });

  test('only the windows the account reports get a swatch', () => {
    setSummaryQuery(
      makeGeminiWarmupSummary({
        upstreamId: gemini.id,
        primary: { target_windows: ['gemini_7d'] },
      }),
      false,
    );
    render(<WarmupCardMinimal upstream={gemini} />);

    const primaryRow = screen.getByTestId('warmup-lane-primary');
    expect(within(primaryRow).getByText('Gemini 7d')).toBeTruthy();
    expect(within(primaryRow).queryByText('Gemini 5h')).toBeNull();
    expect(
      primaryRow.querySelectorAll('[data-slot="window-swatch"]'),
    ).toHaveLength(1);
  });

  test('an ineligible lane reads "Not reported by this account" and leaves the badge alone', () => {
    setSummaryQuery(
      makeGeminiWarmupSummary({
        upstreamId: gemini.id,
        primary: { recent_attempts: [success('primary')] },
        thirdParty: {
          eligible: false,
          ineligible_reason: 'profile_unsupported',
          target_windows: [],
          next_scheduled_at_unix_secs: null,
          recent_attempts: [failure('third_party', 'permanent_failure')],
        },
      }),
      false,
    );
    render(<WarmupCardMinimal upstream={gemini} />);

    const thirdRow = screen.getByTestId('warmup-lane-third_party');
    expect(thirdRow.textContent).toContain('Not reported by this account');
    expect(thirdRow.querySelector('[data-slot="window-swatch"]')).toBeNull();
    expect(within(thirdRow).queryByText('Next run')).toBeNull();
    expect(screen.getByTestId('warmup-status-value').textContent).toBe(
      'Healthy',
    );
    expect(screen.queryByTestId('warmup-failure-panel')).toBeNull();
  });

  test.each([
    [['Healthy', 'Degraded'], 'Degraded'],
    [['Down', 'Degraded'], 'Down'],
    [['Pending', 'Healthy'], 'Pending'],
    [['Healthy', 'Idle'], 'Idle'],
    [['Healthy', 'Healthy'], 'Healthy'],
  ] as const)(
    'lanes %j show the worst lane %s in the header badge',
    (states, expected) => {
      const attemptsFor = (
        lane: queries.WarmupLane,
        state: (typeof states)[number],
      ): queries.WarmupAttempt[] => {
        switch (state) {
          case 'Healthy':
            return [success(lane)];
          case 'Degraded':
            return [failure(lane, 'transient_failure')];
          case 'Down':
            return [failure(lane, 'permanent_failure')];
          case 'Idle':
            return [
              makeAttempt({
                id: `${lane}-skip`,
                lane,
                status: 'skipped',
                reason: 'quota_group_unavailable',
              }),
            ];
          case 'Pending':
            return [];
        }
      };
      setSummaryQuery(
        makeGeminiWarmupSummary({
          upstreamId: gemini.id,
          primary: { recent_attempts: attemptsFor('primary', states[0]) },
          thirdParty: {
            recent_attempts: attemptsFor('third_party', states[1]),
          },
        }),
        false,
      );
      render(<WarmupCardMinimal upstream={gemini} />);

      expect(screen.getByTestId('warmup-status-value').textContent).toBe(
        expected,
      );
    },
  );

  test('each failing lane gets its own named failure panel', () => {
    setSummaryQuery(
      makeGeminiWarmupSummary({
        upstreamId: gemini.id,
        primary: { recent_attempts: [failure('primary', 'permanent_failure')] },
        thirdParty: {
          recent_attempts: [failure('third_party', 'transient_failure')],
        },
      }),
      false,
    );
    render(<WarmupCardMinimal upstream={gemini} />);

    const panels = screen.getAllByTestId('warmup-failure-panel');
    expect(panels.map((p) => p.dataset.lane)).toEqual([
      'primary',
      'third_party',
    ]);
    expect(panels[0]?.textContent).toContain(
      'Gemini models: Last warm-up failure: Auth rejected (401).',
    );
    expect(panels[1]?.textContent).toContain(
      'Claude and GPT-OSS models: Last warm-up retry error: Upstream 5xx.',
    );
  });

  test('an empty lanes array renders the plain grid without crashing', () => {
    setSummaryQuery(makeWarmupSummary([], { upstreamId: upstream.id }), false);
    render(
      <WarmupCardMinimal
        upstream={makeOauthUpstream({ last_warmup_at_unix_secs: null })}
      />,
    );

    expect(screen.getByTestId('warmup-next-value').textContent).toBe(
      'Not scheduled',
    );
    expect(screen.getByTestId('warmup-last').textContent).toBe('Never');
    expect(screen.getByTestId('warmup-status-value').textContent).toBe(
      'Pending',
    );
    expect(screen.getByTestId('warmup-failure-slot').textContent).toBe('');
  });

  test('a single-lane failure panel carries no lane name', () => {
    setSummaryQuery(
      makeSummary(failure('primary', 'permanent_failure')),
      false,
    );
    render(<WarmupCardMinimal upstream={upstream} />);

    const panel = screen.getByTestId('warmup-failure-panel');
    expect(panel.textContent).toContain(
      'Last warm-up failure: Auth rejected (401).',
    );
    expect(panel.textContent).not.toContain('Gemini');
  });
});

describe('WarmupCardMinimal fire now lanes', () => {
  function fireMock() {
    return vi.mocked(queries.useFireNowUpstreamWarmup).mock.results.at(-1)
      ?.value.mutateAsync as ReturnType<typeof vi.fn>;
  }

  test('a single-lane upstream fires once without a lane and shows no model group control', async () => {
    const user = userEvent.setup();
    render(<WarmupCardMinimal upstream={upstream} />);

    await user.click(screen.getByRole('button', { name: 'Fire now' }));
    const dialog = await screen.findByRole('alertdialog');
    expect(
      within(dialog).queryByRole('radiogroup', { name: 'Model group' }),
    ).toBeNull();
    await user.click(within(dialog).getByRole('button', { name: 'Fire now' }));

    expect(fireMock().mock.calls).toEqual([
      [{ id: upstream.id, lane: undefined }],
    ]);
  });

  test('Gemini defaults to Both and fires one request per eligible lane', async () => {
    setSummaryQuery(makeGeminiWarmupSummary({ upstreamId: gemini.id }), false);
    const user = userEvent.setup();
    render(<WarmupCardMinimal upstream={gemini} />);

    await user.click(screen.getByRole('button', { name: 'Fire now' }));
    const dialog = await screen.findByRole('alertdialog');
    const group = within(dialog).getByRole('radiogroup', {
      name: 'Model group',
    });
    expect(
      within(group)
        .getByRole('radio', { name: 'Both' })
        .getAttribute('aria-checked'),
    ).toBe('true');
    expect(within(group).getByRole('radio', { name: 'Gemini' })).toBeTruthy();
    expect(
      within(group).getByRole('radio', { name: 'Claude·OSS' }),
    ).toBeTruthy();
    await user.click(within(dialog).getByRole('button', { name: 'Fire now' }));

    expect(fireMock().mock.calls).toEqual([
      [{ id: gemini.id, lane: 'primary' }],
      [{ id: gemini.id, lane: 'third_party' }],
    ]);
  });

  test.each(['resolved', 'rejected'] as const)(
    'both fire buttons stay disabled until the earlier lane request is %s',
    async (firstOutcome) => {
      setSummaryQuery(
        makeGeminiWarmupSummary({ upstreamId: gemini.id }),
        false,
      );
      const {
        promise: primary,
        resolve: resolvePrimary,
        reject: rejectPrimary,
      } = Promise.withResolvers<queries.FireNowResponse>();
      const { promise: thirdParty, resolve: resolveThirdParty } =
        Promise.withResolvers<queries.FireNowResponse>();
      const mutateAsync = vi.fn(({ lane }: { lane?: queries.WarmupLane }) =>
        lane === 'primary' ? primary : thirdParty,
      );
      vi.mocked(queries.useFireNowUpstreamWarmup).mockReturnValue({
        // The latest mutation can be idle while an earlier one is still pending.
        isPending: false,
        mutateAsync,
      } as never);
      const user = userEvent.setup();
      render(<WarmupCardMinimal upstream={gemini} />);

      const headerButton = screen.getByTestId('warmup-fire-now');
      await user.click(headerButton);
      const dialog = await screen.findByRole('alertdialog');
      const confirmButton = within(dialog).getByRole('button', {
        name: 'Fire now',
      });
      await user.click(confirmButton);

      expect(mutateAsync).toHaveBeenCalledTimes(2);
      expect(headerButton.hasAttribute('disabled')).toBe(true);
      expect(confirmButton.hasAttribute('disabled')).toBe(true);

      await act(async () => {
        resolveThirdParty(makeFireNowLeaseHeld('third-party-replica'));
        await thirdParty;
      });
      expect(headerButton.hasAttribute('disabled')).toBe(true);
      expect(confirmButton.hasAttribute('disabled')).toBe(true);
      await user.click(confirmButton);
      expect(mutateAsync).toHaveBeenCalledTimes(2);

      await act(async () => {
        if (firstOutcome === 'resolved') {
          resolvePrimary(makeFireNowLeaseHeld('primary-replica'));
        } else {
          rejectPrimary(new Error('Primary warmup failed'));
        }
        await Promise.allSettled([primary]);
      });
      expect(headerButton.hasAttribute('disabled')).toBe(false);
      expect(screen.queryByRole('alertdialog')).toBeNull();
      expect(mutateAsync).toHaveBeenCalledTimes(2);
    },
  );

  test('an explicit lane becoming ineligible blocks confirmation without substituting another lane', async () => {
    setSummaryQuery(makeGeminiWarmupSummary({ upstreamId: gemini.id }), false);
    const user = userEvent.setup();
    const { rerender } = render(<WarmupCardMinimal upstream={gemini} />);

    await user.click(screen.getByRole('button', { name: 'Fire now' }));
    const dialog = await screen.findByRole('alertdialog');
    await user.click(within(dialog).getByRole('radio', { name: 'Claude·OSS' }));

    setSummaryQuery(
      makeGeminiWarmupSummary({
        upstreamId: gemini.id,
        thirdParty: {
          eligible: false,
          ineligible_reason: 'quota_group_absent',
        },
      }),
      false,
    );
    rerender(<WarmupCardMinimal upstream={gemini} />);

    const confirmButton = within(dialog).getByRole('button', {
      name: 'Fire now',
    });
    expect(confirmButton.hasAttribute('disabled')).toBe(true);
    expect(within(dialog).getByRole('status').textContent).toContain(
      'Claude and GPT-OSS models are no longer available for this account.',
    );
    await user.click(confirmButton);
    expect(fireMock()).not.toHaveBeenCalled();

    await user.click(within(dialog).getByRole('button', { name: 'Cancel' }));
    await user.click(screen.getByTestId('warmup-fire-now'));
    const reopenedDialog = await screen.findByRole('alertdialog');
    expect(within(reopenedDialog).queryByRole('status')).toBeNull();
    await user.click(
      within(reopenedDialog).getByRole('button', { name: 'Fire now' }),
    );
    expect(fireMock().mock.calls).toEqual([
      [{ id: gemini.id, lane: 'primary' }],
    ]);
  });

  test('Both follows eligible lanes when a summary update hides the selector', async () => {
    setSummaryQuery(makeGeminiWarmupSummary({ upstreamId: gemini.id }), false);
    const user = userEvent.setup();
    const { rerender } = render(<WarmupCardMinimal upstream={gemini} />);

    await user.click(screen.getByRole('button', { name: 'Fire now' }));
    const dialog = await screen.findByRole('alertdialog');
    setSummaryQuery(
      makeGeminiWarmupSummary({
        upstreamId: gemini.id,
        thirdParty: {
          eligible: false,
          ineligible_reason: 'quota_group_absent',
        },
      }),
      false,
    );
    rerender(<WarmupCardMinimal upstream={gemini} />);

    expect(
      within(dialog).queryByRole('radiogroup', { name: 'Model group' }),
    ).toBeNull();
    const confirmButton = within(dialog).getByRole('button', {
      name: 'Fire now',
    });
    expect(confirmButton.hasAttribute('disabled')).toBe(false);
    await user.click(confirmButton);
    expect(fireMock().mock.calls).toEqual([
      [{ id: gemini.id, lane: 'primary' }],
    ]);
  });

  test('choosing one model group fires only that lane', async () => {
    setSummaryQuery(makeGeminiWarmupSummary({ upstreamId: gemini.id }), false);
    const user = userEvent.setup();
    render(<WarmupCardMinimal upstream={gemini} />);

    await user.click(screen.getByRole('button', { name: 'Fire now' }));
    const dialog = await screen.findByRole('alertdialog');
    await user.click(within(dialog).getByRole('radio', { name: 'Claude·OSS' }));
    await user.click(within(dialog).getByRole('button', { name: 'Fire now' }));

    expect(fireMock().mock.calls).toEqual([
      [{ id: gemini.id, lane: 'third_party' }],
    ]);
  });

  test('an ineligible lane is not offered and only the eligible lane fires', async () => {
    setSummaryQuery(
      makeGeminiWarmupSummary({
        upstreamId: gemini.id,
        thirdParty: {
          eligible: false,
          ineligible_reason: 'quota_group_absent',
        },
      }),
      false,
    );
    const user = userEvent.setup();
    render(<WarmupCardMinimal upstream={gemini} />);

    await user.click(screen.getByRole('button', { name: 'Fire now' }));
    const dialog = await screen.findByRole('alertdialog');
    expect(
      within(dialog).queryByRole('radiogroup', { name: 'Model group' }),
    ).toBeNull();
    await user.click(within(dialog).getByRole('button', { name: 'Fire now' }));

    expect(fireMock().mock.calls).toEqual([
      [{ id: gemini.id, lane: 'primary' }],
    ]);
  });

  test('lane outcomes name their lane', async () => {
    setSummaryQuery(makeGeminiWarmupSummary({ upstreamId: gemini.id }), false);
    vi.mocked(queries.useFireNowUpstreamWarmup).mockReturnValue({
      isPending: false,
      mutateAsync: vi.fn(async ({ lane }: { lane?: queries.WarmupLane }) =>
        lane === 'primary'
          ? makeFireNowLeaseHeld('replica-9')
          : { fired: false, reason: 'quota_group_unavailable', lane },
      ),
    } as never);
    const user = userEvent.setup();
    render(<WarmupCardMinimal upstream={gemini} />);

    await user.click(screen.getByRole('button', { name: 'Fire now' }));
    const dialog = await screen.findByRole('alertdialog');
    await user.click(within(dialog).getByRole('button', { name: 'Fire now' }));

    const lease = await screen.findByTestId('warmup-lease-panel');
    expect(lease.dataset.lane).toBe('primary');
    expect(lease.textContent).toContain(
      'Gemini models: Another replica (replica-9)',
    );
    const error = screen.getByTestId('warmup-error-panel');
    expect(error.dataset.lane).toBe('third_party');
    expect(error.dataset.reason).toBe('quota_group_unavailable');
    expect(error.textContent).toContain(
      'Claude and GPT-OSS models: Warmup skipped',
    );
  });
});
