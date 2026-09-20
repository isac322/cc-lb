import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  FinalRequestEventUpdateSchema,
  fetchWithAuth,
  RequestEventPartialSchema,
  UpstreamSchema,
} from './api';

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

  it('accepts a recorded event_kind and tolerates it missing', () => {
    const withKind = RequestEventPartialSchema.safeParse({
      ...baseFixture,
      event_kind: 'count_tokens',
    });
    expect(withKind.success).toBe(true);
    if (withKind.success) {
      expect(withKind.data.event_kind).toBe('count_tokens');
    }

    const withoutKind = RequestEventPartialSchema.safeParse(baseFixture);
    expect(withoutKind.success).toBe(true);
    if (withoutKind.success) {
      expect(withoutKind.data.event_kind).toBeUndefined();
    }

    expect(
      RequestEventPartialSchema.safeParse({
        ...baseFixture,
        event_kind: 'not-a-kind',
      }).success,
    ).toBe(false);
  });

  it('rejects partial object with thinking_budget_tokens as string', () => {
    const result = RequestEventPartialSchema.safeParse({
      ...baseFixture,
      thinking_budget_tokens: '18000',
    });
    expect(result.success).toBe(false);
  });

  it('preserves fractional, zero, missing, and nullable request timings', () => {
    const result = RequestEventPartialSchema.safeParse({
      ...baseFixture,
      json_parse_ms: 0.125,
      cache_structure_ms: 0,
      cache_token_key_ms: null,
      cache_count_lookup_ms: 1.5,
      cache_tokenizer_queue_ms: 0.25,
      cache_serialize_ms: 2.75,
      prepare_signer_ms: 4.5,
      request_body_first_chunk_ms: 0.375,
      request_body_receive_ms: null,
      request_body_wait_ms: 0,
      request_body_process_ms: 1.25,
      request_body_chunk_count: 0,
      response_body_wait_ms: 3.5,
      response_body_process_ms: 0,
      retry_overhead_ms: 6.75,
    });
    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.data.cache_structure_ms).toBe(0);
      expect(result.data.cache_tokenize_ms).toBeUndefined();
      expect(result.data.cache_token_key_ms).toBeNull();
      expect(result.data.request_body_first_chunk_ms).toBe(0.375);
      expect(result.data.request_body_receive_ms).toBeNull();
      expect(result.data.request_body_wait_ms).toBe(0);
      expect(result.data.request_body_chunk_count).toBe(0);
      expect(result.data.response_body_process_ms).toBe(0);
      expect(result.data.response_body_downstream_poll_gap_ms).toBeUndefined();
    }
  });

  it('rejects invalid request timing values', () => {
    expect(
      RequestEventPartialSchema.safeParse({
        ...baseFixture,
        request_body_wait_ms: -0.1,
      }).success,
    ).toBe(false);
    expect(
      RequestEventPartialSchema.safeParse({
        ...baseFixture,
        request_body_chunk_count: 1.5,
      }).success,
    ).toBe(false);
    expect(
      RequestEventPartialSchema.safeParse({
        ...baseFixture,
        request_body_chunk_count: -1,
      }).success,
    ).toBe(false);
    expect(
      RequestEventPartialSchema.safeParse({
        ...baseFixture,
        response_body_wait_ms: Number.POSITIVE_INFINITY,
      }).success,
    ).toBe(false);
  });
});

describe('UpstreamSchema', () => {
  const fixture = {
    id: 'upstream-1',
    name: 'Primary',
    kind: 'anthropic_oauth',
    enabled: true,
    base_url: 'https://gateway.example.com/v1/',
    warmup_enabled: true,
    warmup_dialect_plugin: null,
    spec_revision: 3,
    status: {
      last_apply_error: null,
      last_apply_at_unix_secs: null,
      last_warmup_at_unix_secs: null,
    },
  };

  it('preserves a stored base URL and accepts an explicit null', () => {
    expect(UpstreamSchema.parse(fixture).base_url).toBe(
      'https://gateway.example.com/v1/',
    );
    expect(
      UpstreamSchema.parse({ ...fixture, base_url: null }).base_url,
    ).toBeNull();
  });

  it('requires the backend base URL field instead of silently stripping it', () => {
    const { base_url: _baseUrl, ...withoutBaseUrl } = fixture;
    expect(UpstreamSchema.safeParse(withoutBaseUrl).success).toBe(false);
  });
});

describe('FinalRequestEventUpdateSchema', () => {
  it('validates request timings on final SSE rows', () => {
    const result = FinalRequestEventUpdateSchema.safeParse({
      event: {
        request_id: 'r',
        status: 200,
        duration_ms: 10,
        json_parse_ms: 0.125,
        cache_tokenize_ms: 0,
        request_body_first_chunk_ms: 0.25,
        request_body_wait_ms: 0,
        request_body_chunk_count: 2,
        response_body_process_ms: null,
        retry_overhead_ms: 1.75,
      },
      cursor: 1,
    });
    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.data.event.request_body_first_chunk_ms).toBe(0.25);
      expect(result.data.event.request_body_wait_ms).toBe(0);
      expect(result.data.event.request_body_chunk_count).toBe(2);
      expect(result.data.event.response_body_process_ms).toBeNull();
      expect(result.data.event.retry_overhead_ms).toBe(1.75);
    }
  });

  it('rejects invalid final request timings', () => {
    const result = FinalRequestEventUpdateSchema.safeParse({
      event: {
        request_id: 'r',
        status: 200,
        duration_ms: 10,
        response_body_downstream_poll_gap_ms: Number.POSITIVE_INFINITY,
      },
      cursor: 1,
    });
    expect(result.success).toBe(false);
  });
});
