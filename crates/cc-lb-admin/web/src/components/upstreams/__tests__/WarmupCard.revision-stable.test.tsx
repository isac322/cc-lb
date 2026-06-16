// Regression: WarmupCard used to set its inner React `key` to
//   `${upstream.id}:${upstream.spec_revision}:${upstream.warmup_dialect_plugin?.wasm_registry_id ?? ''}`
// Every mutation (toggle, plugin swap, fire-now success) invalidated `qk.upstreams`,
// the upstream was refetched, `spec_revision` bumped from N to N+1, and the key
// flipped — forcing React to unmount WarmupCardInner and mount a fresh one.
// Visible to the user as a card flash on every action. This test pins the
// repair: the inner card stays mounted across spec_revision bumps, and only
// remounts when `upstream.id` actually changes (i.e. the user selected a
// different upstream).

import { cleanup, render, screen } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest';
import * as queries from '../../../lib/queries';
import { makeOauthUpstream } from '../../../lib/test-utils/warmup-fixtures';

vi.mock('sonner', () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

vi.mock('../../../lib/format', () => ({
  formatRelativeUnixSeconds: vi.fn(
    (seconds: number) => new Date(seconds * 1000),
  ),
}));

vi.mock('../../../lib/queries', async () => {
  const actual = await vi.importActual<typeof import('../../../lib/queries')>(
    '../../../lib/queries',
  );
  return {
    ...actual,
    useFireNowUpstreamWarmup: vi.fn(),
    useClearUpstreamWarmupDialectPlugin: vi.fn(),
    useUpdateUpstreamWarmupSettings: vi.fn(),
    usePluginRegistry: vi.fn(),
  };
});

vi.mock('../../ui/RelativeTime', () => ({
  RelativeTime: ({ ts }: { ts: unknown }) => (
    <span data-testid="rel-time">{String(ts)}</span>
  ),
  ResetCountdown: ({ ts }: { ts: unknown }) => (
    <span data-testid="reset-countdown">{String(ts)}</span>
  ),
}));

function makeMutation() {
  return { mutate: vi.fn(), isPending: false };
}

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(queries.useFireNowUpstreamWarmup).mockReturnValue(
    makeMutation() as unknown as ReturnType<
      typeof queries.useFireNowUpstreamWarmup
    >,
  );
  vi.mocked(queries.useClearUpstreamWarmupDialectPlugin).mockReturnValue(
    makeMutation() as unknown as ReturnType<
      typeof queries.useClearUpstreamWarmupDialectPlugin
    >,
  );
  vi.mocked(queries.useUpdateUpstreamWarmupSettings).mockReturnValue(
    makeMutation() as unknown as ReturnType<
      typeof queries.useUpdateUpstreamWarmupSettings
    >,
  );
  vi.mocked(queries.usePluginRegistry).mockReturnValue({
    data: { entries: [] },
  } as unknown as ReturnType<typeof queries.usePluginRegistry>);
});

afterEach(() => {
  cleanup();
});

async function importWarmupCard() {
  const mod = await import('../WarmupCard');
  return mod.WarmupCard;
}

describe('WarmupCard mount stability', () => {
  test('inner card DOM node is preserved when upstream.spec_revision changes', async () => {
    const WarmupCard = await importWarmupCard();
    const upstream = makeOauthUpstream({ id: 'oauth-1', spec_revision: 1 });
    const { rerender } = render(<WarmupCard upstream={upstream} />);
    const before = screen.getByTestId('warmup-card');

    const bumped = { ...upstream, spec_revision: 2 };
    rerender(<WarmupCard upstream={bumped} />);
    const after = screen.getByTestId('warmup-card');

    // Same DOM node identity == React did not unmount/remount the card.
    expect(after).toBe(before);
  });

  test('inner card DOM node is replaced when warmup_dialect_plugin changes (intentional reset of plugin-bound state)', async () => {
    const WarmupCard = await importWarmupCard();
    const upstream = makeOauthUpstream({
      id: 'oauth-1',
      spec_revision: 1,
      warmup_dialect_plugin: { wasm_registry_id: 'shape-a', config: {} },
    });
    const { rerender } = render(<WarmupCard upstream={upstream} />);
    const before = screen.getByTestId('warmup-card');

    const swapped = {
      ...upstream,
      spec_revision: 2,
      warmup_dialect_plugin: { wasm_registry_id: 'shape-b', config: {} },
    };
    rerender(<WarmupCard upstream={swapped} />);
    const after = screen.getByTestId('warmup-card');

    // The user-initiated plugin swap is treated as a fresh-state event by the
    // production component (key includes wasm_registry_id). The spec_revision-bump
    // anti-flicker contract above is unaffected by this choice.
    expect(after).not.toBe(before);
  });

  test('inner card DOM node is replaced when upstream.id changes', async () => {
    const WarmupCard = await importWarmupCard();
    const a = makeOauthUpstream({ id: 'oauth-1', spec_revision: 1 });
    const { rerender } = render(<WarmupCard upstream={a} />);
    const before = screen.getByTestId('warmup-card');

    const b = makeOauthUpstream({ id: 'oauth-2', spec_revision: 1 });
    rerender(<WarmupCard upstream={b} />);
    const after = screen.getByTestId('warmup-card');

    expect(after).not.toBe(before);
  });
});
