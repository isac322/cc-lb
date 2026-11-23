import { describe, expect, it } from 'vitest';
import { normalizeAdminApiBaseUrl, resolveAdminApiUrl } from './api';

describe('normalizeAdminApiBaseUrl', () => {
  it('returns empty string for unset, blank, or root-only values', () => {
    expect(normalizeAdminApiBaseUrl(undefined)).toBe('');
    expect(normalizeAdminApiBaseUrl('')).toBe('');
    expect(normalizeAdminApiBaseUrl('   ')).toBe('');
    expect(normalizeAdminApiBaseUrl('/')).toBe('');
  });

  it('trims whitespace and trailing slashes from explicit origins', () => {
    expect(normalizeAdminApiBaseUrl('https://admin.example.com')).toBe(
      'https://admin.example.com',
    );
    expect(normalizeAdminApiBaseUrl('  https://admin.example.com/  ')).toBe(
      'https://admin.example.com',
    );
    expect(
      normalizeAdminApiBaseUrl('https://admin.example.com/control///'),
    ).toBe('https://admin.example.com/control');
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

  it('prepends explicit baseUrl to API and SSE paths', () => {
    expect(
      resolveAdminApiUrl('/admin/v1/status', 'https://admin.example.com'),
    ).toBe('https://admin.example.com/admin/v1/status');
    expect(
      resolveAdminApiUrl('/admin/health', 'https://admin.example.com/proxy'),
    ).toBe('https://admin.example.com/proxy/admin/health');
    expect(
      resolveAdminApiUrl(
        'admin/v1/events/stream?limit=25',
        'https://admin.example.com',
      ),
    ).toBe('https://admin.example.com/admin/v1/events/stream?limit=25');
  });

  it('leaves absolute URLs untouched even when baseUrl is configured', () => {
    expect(
      resolveAdminApiUrl(
        'https://other.example.com/admin/health',
        'https://admin.example.com',
      ),
    ).toBe('https://other.example.com/admin/health');
  });
});
