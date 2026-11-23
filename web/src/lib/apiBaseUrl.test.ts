import { describe, expect, it } from 'vitest';
import { normalizeAdminApiBaseUrl, resolveAdminApiUrl } from './api';

describe('normalizeAdminApiBaseUrl', () => {
  it('returns empty string for unset, blank, or root-only values', () => {
    expect(normalizeAdminApiBaseUrl(undefined)).toBe('');
    expect(normalizeAdminApiBaseUrl('')).toBe('');
    expect(normalizeAdminApiBaseUrl('   ')).toBe('');
    expect(normalizeAdminApiBaseUrl('/')).toBe('');
  });

  it('trims whitespace and trailing slashes from same-origin path prefixes', () => {
    expect(normalizeAdminApiBaseUrl('/control')).toBe('/control');
    expect(normalizeAdminApiBaseUrl('  /control/  ')).toBe('/control');
    expect(normalizeAdminApiBaseUrl('/cc-lb/control///')).toBe(
      '/cc-lb/control',
    );
  });

  it('rejects cross-origin and non-leading-slash base URLs', () => {
    expect(() => normalizeAdminApiBaseUrl('https://admin.example.com')).toThrow(
      /cross-origin/i,
    );
    expect(() =>
      normalizeAdminApiBaseUrl('http://admin.example.com/control'),
    ).toThrow(/cross-origin/i);
    expect(() => normalizeAdminApiBaseUrl('//admin.example.com')).toThrow(
      /cross-origin/i,
    );
    expect(() => normalizeAdminApiBaseUrl('control')).toThrow(/leading-slash/i);
  });
});

describe('resolveAdminApiUrl', () => {
  it('keeps relative paths unchanged when baseUrl is empty (same-origin)', () => {
    expect(resolveAdminApiUrl('/admin/v1/status', '')).toBe('/admin/v1/status');
    expect(resolveAdminApiUrl('/admin/health', '')).toBe('/admin/health');
    expect(resolveAdminApiUrl('/admin/v1/events/stream?limit=50', '')).toBe(
      '/admin/v1/events/stream?limit=50',
    );
  });

  it('prepends explicit same-origin baseUrl prefix to API and SSE paths', () => {
    expect(resolveAdminApiUrl('/admin/v1/status', '/control')).toBe(
      '/control/admin/v1/status',
    );
    expect(resolveAdminApiUrl('/admin/health', '/cc-lb/proxy')).toBe(
      '/cc-lb/proxy/admin/health',
    );
    expect(
      resolveAdminApiUrl('admin/v1/events/stream?limit=25', '/control'),
    ).toBe('/control/admin/v1/events/stream?limit=25');
  });

  it('leaves absolute URLs untouched even when baseUrl is configured', () => {
    expect(
      resolveAdminApiUrl('https://other.example.com/admin/health', '/control'),
    ).toBe('https://other.example.com/admin/health');
  });
});
