import { describe, it, expect, mock, beforeEach, afterEach } from 'bun:test';

const mockStorage = new Map();
global.localStorage = {
  getItem: (k: string) => mockStorage.get(k) || null,
  setItem: (k: string, v: string) => mockStorage.set(k, v),
  removeItem: (k: string) => mockStorage.delete(k),
  clear: () => mockStorage.clear(),
  length: 0,
  key: () => null,
} as Storage;

import { getJson, postJson, ApiError } from '../lib/api';
import { setAdminToken, clearAdminToken } from '../lib/auth';

describe('API Client', () => {
  const originalFetch = global.fetch;

  beforeEach(() => {
    clearAdminToken();
  });

  afterEach(() => {
    global.fetch = originalFetch;
  });

  it('getJson adds auth header and parses json', async () => {
    setAdminToken('test-token');
    
    global.fetch = mock(async (_input: RequestInfo | URL, init?: RequestInit) => {
      expect(init?.headers).toBeDefined();
      const headers = new Headers(init?.headers);
      expect(headers.get('Authorization')).toBe('Bearer test-token');
      
      return new Response(JSON.stringify({ success: true }), {
        status: 200,
        headers: { 'Content-Type': 'application/json' }
      });
    });

    const result = await getJson<{ success: boolean }>('/test');
    expect(result.success).toBe(true);
  });

  it('postJson sends body and parses json', async () => {
    global.fetch = mock(async (_input: RequestInfo | URL, init?: RequestInit) => {
      expect(init?.method).toBe('POST');
      expect(init?.body).toBe(JSON.stringify({ foo: 'bar' }));
      
      return new Response(JSON.stringify({ created: true }), {
        status: 201,
        headers: { 'Content-Type': 'application/json' }
      });
    });

    const result = await postJson<{ created: boolean }, { foo: string }>('/test', { foo: 'bar' });
    expect(result.created).toBe(true);
  });

  it('throws ApiError on non-2xx response', async () => {
    global.fetch = mock(async () => {
      return new Response(JSON.stringify({ code: 'bad_request', message: 'Invalid input' }), {
        status: 400,
        headers: { 'Content-Type': 'application/json' }
      });
    });

    try {
      await getJson('/test');
      expect(true).toBe(false); // Should not reach here
    } catch (err) {
      expect(err).toBeInstanceOf(ApiError);
      const apiErr = err as ApiError;
      expect(apiErr.status).toBe(400);
      expect(apiErr.code).toBe('bad_request');
      expect(apiErr.message).toBe('Invalid input');
    }
  });

  it('throws ApiError with unauthorized code on 401', async () => {
    global.fetch = mock(async () => {
      return new Response(null, { status: 401 });
    });

    try {
      await getJson('/test');
      expect(true).toBe(false); // Should not reach here
    } catch (err) {
      expect(err).toBeInstanceOf(ApiError);
      const apiErr = err as ApiError;
      expect(apiErr.status).toBe(401);
      expect(apiErr.code).toBe('unauthorized');
    }
  });
});
