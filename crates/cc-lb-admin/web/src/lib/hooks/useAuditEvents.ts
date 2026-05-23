import { useCallback, useEffect, useState } from 'react';
import { type AuditEntry, type AuditQueryResponse, getJson } from '../api';

export interface AuditQueryParams {
  limit?: number;
  kind?: string;
  since?: number;
  until?: number;
}

export function useAuditEvents(params: AuditQueryParams) {
  const [events, setEvents] = useState<AuditEntry[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  const refresh = useCallback(async () => {
    setIsLoading(true);
    setError(null);
    try {
      const isMock =
        new URLSearchParams(window.location.search).get('mock') === '1';
      if (isMock) {
        const now = Math.floor(Date.now() / 1000);
        let mockEvents: AuditEntry[] = [
          {
            ts: now - 10,
            request_id: 'req1',
            principal_id: 'admin',
            route: 'config_apply',
            upstream: 'admin',
            model: null,
            status: 200,
            input_tokens: 0,
            output_tokens: 0,
            duration_ms: 10,
            agent_label: null,
            kind: 'config_apply',
            payload: { revision: 42 },
          },
          {
            ts: now - 60,
            request_id: 'req2',
            principal_id: 'admin',
            route: 'api_key_issue',
            upstream: 'admin',
            model: null,
            status: 200,
            input_tokens: 0,
            output_tokens: 0,
            duration_ms: 15,
            agent_label: null,
            kind: 'api_key_issue',
            payload: { principal_id: 'user1', key_id: 'key1', label: 'test' },
          },
          {
            ts: now - 120,
            request_id: 'req3',
            principal_id: 'admin',
            route: 'api_key_revoke',
            upstream: 'admin',
            model: null,
            status: 200,
            input_tokens: 0,
            output_tokens: 0,
            duration_ms: 12,
            agent_label: null,
            kind: 'api_key_revoke',
            payload: { principal_id: 'user1', key_id: 'key1' },
          },
          {
            ts: now - 180,
            request_id: 'req4',
            principal_id: 'admin',
            route: 'credential_rotate',
            upstream: 'admin',
            model: null,
            status: 200,
            input_tokens: 0,
            output_tokens: 0,
            duration_ms: 20,
            agent_label: null,
            kind: 'credential_rotate',
            payload: {
              principal_id: 'user2',
              provider: 'api_key',
              kind: 'api_key',
              new_key_id: 'key2',
              revoked_key_id: 'key1',
            },
          },
          {
            ts: now - 240,
            request_id: 'req5',
            principal_id: 'admin',
            route: 'credential_revoke',
            upstream: 'admin',
            model: null,
            status: 200,
            input_tokens: 0,
            output_tokens: 0,
            duration_ms: 18,
            agent_label: null,
            kind: 'credential_revoke',
            payload: {
              principal_id: 'user2',
              provider: 'api_key',
              kind: 'api_key',
              revoked_keys: ['key2'],
            },
          },
          {
            ts: now - 300,
            request_id: 'req6',
            principal_id: 'admin',
            route: 'principal_create',
            upstream: 'admin',
            model: null,
            status: 200,
            input_tokens: 0,
            output_tokens: 0,
            duration_ms: 25,
            agent_label: null,
            kind: 'principal_create',
            payload: { principal_id: 'user3' },
          },
          {
            ts: now - 360,
            request_id: 'req7',
            principal_id: 'admin',
            route: 'principal_update',
            upstream: 'admin',
            model: null,
            status: 200,
            input_tokens: 0,
            output_tokens: 0,
            duration_ms: 22,
            agent_label: null,
            kind: 'principal_update',
            payload: { principal_id: 'user3' },
          },
          {
            ts: now - 420,
            request_id: 'req8',
            principal_id: 'admin',
            route: 'principal_disable',
            upstream: 'admin',
            model: null,
            status: 200,
            input_tokens: 0,
            output_tokens: 0,
            duration_ms: 15,
            agent_label: null,
            kind: 'principal_disable',
            payload: { principal_id: 'user3', disabled: true },
          },
          {
            ts: now - 480,
            request_id: 'req9',
            principal_id: 'admin',
            route: 'quota_override',
            upstream: 'admin',
            model: null,
            status: 200,
            input_tokens: 0,
            output_tokens: 0,
            duration_ms: 10,
            agent_label: null,
            kind: 'quota_override',
            payload: { principal_id: 'user3', requests_per_window: 100 },
          },
          {
            ts: now - 540,
            request_id: 'req10',
            principal_id: 'admin',
            route: 'killswitch_set',
            upstream: 'admin',
            model: null,
            status: 200,
            input_tokens: 0,
            output_tokens: 0,
            duration_ms: 5,
            agent_label: null,
            kind: 'killswitch_set',
            payload: { killswitch: true },
          },
          {
            ts: now - 600,
            request_id: 'req11',
            principal_id: 'admin',
            route: 'oauth_complete',
            upstream: 'admin',
            model: null,
            status: 200,
            input_tokens: 0,
            output_tokens: 0,
            duration_ms: 50,
            agent_label: null,
            kind: 'oauth_complete',
            payload: { principal_id: 'user4', provider: 'anthropic_oauth' },
          },
          {
            ts: now - 660,
            request_id: 'req12',
            principal_id: 'admin',
            route: 'leak_test',
            upstream: 'admin',
            model: null,
            status: 200,
            input_tokens: 0,
            output_tokens: 0,
            duration_ms: 5,
            agent_label: null,
            kind: 'leak_test',
            payload: {
              plaintext: 'mock-plaintext-redacted',
              safe_field: 'hello',
            },
          },
        ];

        if (params.kind) {
          mockEvents = mockEvents.filter((e) => e.kind === params.kind);
        }

        setEvents(mockEvents);
        setIsLoading(false);
        return;
      }

      const queryParams = new URLSearchParams();
      if (params.limit) queryParams.set('limit', params.limit.toString());
      if (params.since) queryParams.set('since', params.since.toString());
      if (params.until) queryParams.set('until', params.until.toString());

      const res = await getJson<AuditQueryResponse>(
        `/admin/audit?${queryParams.toString()}`,
      );
      let fetchedEvents = res.entries;

      if (params.kind) {
        fetchedEvents = fetchedEvents.filter((e) => e.kind === params.kind);
      }

      // Sort descending by ts
      fetchedEvents.sort((a, b) => b.ts - a.ts);

      setEvents(fetchedEvents);
    } catch (err) {
      setError(err instanceof Error ? err : new Error(String(err)));
    } finally {
      setIsLoading(false);
    }
  }, [params.limit, params.kind, params.since, params.until]);

  useEffect(() => {
    refresh();
    const interval = setInterval(refresh, 30000);
    return () => clearInterval(interval);
  }, [refresh]);

  return { events, isLoading, error, refresh };
}
