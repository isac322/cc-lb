import { describe, expect, it } from 'vitest';
import type { RequestEvent } from '../lib/api';
import { computeLatencySections } from './computeLatencySections';

describe('computeLatencySections', () => {
  it('omits sections with no defined rows', () => {
    const r = computeLatencySections({
      duration_ms: 100,
      request_id: 'r',
      status: 200,
    } as RequestEvent);
    expect(r.sections.map((s) => s.title)).toEqual([]);
  });

  it('includes Internal pre when auth_ms defined', () => {
    const r = computeLatencySections({
      duration_ms: 100,
      auth_ms: 5,
      request_id: 'r',
      status: 200,
    } as RequestEvent);
    expect(r.sections.find((s) => s.title === 'Internal pre')?.rows).toEqual([
      { label: 'Auth', value: 5 },
    ]);
  });

  it('marks Upstream section warmPool when connection_reused=true', () => {
    const r = computeLatencySections({
      duration_ms: 100,
      upstream_ttfb_ms: 50,
      connection_reused: true,
      request_id: 'r',
      status: 200,
    } as RequestEvent);
    const upstream = r.sections.find((s) => s.title === 'Upstream');
    expect(upstream?.warmPool).toBe(true);
  });

  it('computes derived upstream_wait correctly', () => {
    const r = computeLatencySections({
      duration_ms: 100,
      upstream_ttfb_ms: 500,
      bulkhead_wait_ms: 5,
      dns_ms: 20,
      connect_ms: 75,
      request_id: 'r',
      status: 200,
    } as RequestEvent);
    const upstream = r.sections.find((s) => s.title === 'Upstream');
    expect(upstream?.rows).toEqual([
      { label: 'Connect (TCP+TLS)', value: 75 },
      { label: 'Upstream wait', value: 400 },
    ]);
  });

  it('clamps unaccounted to zero when sum exceeds duration', () => {
    const r = computeLatencySections({
      duration_ms: 100,
      auth_ms: 200,
      request_id: 'r',
      status: 200,
    } as RequestEvent);
    expect(r.unaccounted).toBe(0);
  });

  it('reports unaccounted residual', () => {
    const r = computeLatencySections({
      duration_ms: 1000,
      auth_ms: 100,
      request_id: 'r',
      status: 200,
    } as RequestEvent);
    expect(r.unaccounted).toBe(900);
  });
});
