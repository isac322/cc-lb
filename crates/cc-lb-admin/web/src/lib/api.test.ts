import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { fetchWithAuth, RequestEventPartialSchema } from './api';

vi.mock('./auth', () => ({
  clearAdminToken: vi.fn(),
  getAdminToken: vi.fn(() => null),
}));

let observedSignal: AbortSignal | undefined;

function stubAbortableFetch(): void {
  vi.stubGlobal(
    'fetch',
    vi.fn((_input: RequestInfo | URL, init?: RequestInit) => {
      const signal = init?.signal;
      if (!signal) {
        return Promise.reject(
          new Error('fetch called without an abort signal'),
        );
      }
      observedSignal = signal;
      return new Promise<Response>((_resolve, reject) => {
        const rejectOnAbort = () => reject(signal.reason);
        if (signal.aborted) {
          rejectOnAbort();
        } else {
          signal.addEventListener('abort', rejectOnAbort, { once: true });
        }
      });
    }),
  );
}

describe('fetchWithAuth', () => {
  beforeEach(() => {
    observedSignal = undefined;
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.clearAllTimers();
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  it('enforces its deadline when the caller supplies a signal', async () => {
    stubAbortableFetch();
    const externalController = new AbortController();

    const request = fetchWithAuth('/api/test', {
      signal: externalController.signal,
    });
    const rejection = expect(request).rejects.toMatchObject({
      name: 'AbortError',
    });

    await vi.advanceTimersByTimeAsync(30_000);
    await rejection;

    expect(observedSignal?.aborted).toBe(true);
    expect(externalController.signal.aborted).toBe(false);
    expect(vi.getTimerCount()).toBe(0);
  });

  it('propagates caller cancellation immediately', async () => {
    stubAbortableFetch();
    const externalController = new AbortController();
    const reason = new Error('caller cancelled');

    const request = fetchWithAuth('/api/test', {
      signal: externalController.signal,
    });
    const rejection = expect(request).rejects.toBe(reason);
    externalController.abort(reason);

    await rejection;
    expect(observedSignal?.reason).toBe(reason);
    expect(vi.getTimerCount()).toBe(0);
  });

  it('propagates an already-aborted caller signal', async () => {
    stubAbortableFetch();
    const externalController = new AbortController();
    const reason = new Error('already cancelled');
    externalController.abort(reason);

    await expect(
      fetchWithAuth('/api/test', { signal: externalController.signal }),
    ).rejects.toBe(reason);

    expect(observedSignal?.reason).toBe(reason);
    expect(vi.getTimerCount()).toBe(0);
  });

  it('does not abort a completed request later', async () => {
    const externalController = new AbortController();
    vi.stubGlobal(
      'fetch',
      vi.fn((_input: RequestInfo | URL, init?: RequestInit) => {
        observedSignal = init?.signal ?? undefined;
        return Promise.resolve(new Response(null, { status: 204 }));
      }),
    );

    await fetchWithAuth('/api/test', { signal: externalController.signal });

    expect(vi.getTimerCount()).toBe(0);
    externalController.abort(new Error('late cancellation'));
    await vi.advanceTimersByTimeAsync(30_000);
    expect(observedSignal?.aborted).toBe(false);
  });
});

describe('RequestEventPartialSchema', () => {
  const baseFixture = {
    event_id: 'e',
    request_id: 'r',
    ts: null,
    ts_ms: null,
  };

  it('parses valid partial object with thinking_budget_tokens as number', () => {
    const result = RequestEventPartialSchema.safeParse({
      ...baseFixture,
      thinking_budget_tokens: 18000,
    });
    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.data.thinking_budget_tokens).toBe(18000);
    }
  });

  it('parses valid partial object with thinking_budget_tokens as null', () => {
    const result = RequestEventPartialSchema.safeParse({
      ...baseFixture,
      thinking_budget_tokens: null,
    });
    expect(result.success).toBe(true);
  });

  it('parses valid partial object with thinking_budget_tokens omitted', () => {
    const result = RequestEventPartialSchema.safeParse(baseFixture);
    expect(result.success).toBe(true);
  });

  it('rejects partial object with thinking_budget_tokens as string', () => {
    const result = RequestEventPartialSchema.safeParse({
      ...baseFixture,
      thinking_budget_tokens: '18000',
    });
    expect(result.success).toBe(false);
  });
});
