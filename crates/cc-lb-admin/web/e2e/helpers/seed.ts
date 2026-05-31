/**
 * REST-based admin API helpers. Use these in tests instead of driving the
 * dashboard UI to set up preconditions — UI-driven setup is slow and couples
 * setup-time flakes to behavior-time signals.
 *
 * All helpers accept an `expectedRevision` where applicable so the caller can
 * exercise optimistic-concurrency paths (409 conflicts) explicitly when that
 * is the behavior under test.
 *
 * Construct one `Seeder` per worker; it's bound to the worker's admin URL
 * + admin token via `e2e/fixtures.ts`.
 */

export class Seeder {
  constructor(
    private readonly adminUrl: string,
    private readonly token: string,
  ) {}

  // ---------- internal HTTP ----------

  private headers(extra: Record<string, string> = {}): Record<string, string> {
    return {
      Authorization: `Bearer ${this.token}`,
      'Content-Type': 'application/json',
      ...extra,
    };
  }

  private async request<T = unknown>(
    method: string,
    path: string,
    body?: unknown,
    extraHeaders: Record<string, string> = {},
  ): Promise<T> {
    const res = await fetch(`${this.adminUrl}${path}`, {
      method,
      headers: this.headers(extraHeaders),
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    if (!res.ok) {
      const detail = await res.text().catch(() => '');
      throw new Error(
        `seeder ${method} ${path} failed: ${res.status} ${detail.slice(0, 500)}`,
      );
    }
    if (res.status === 204) return undefined as T;
    const text = await res.text();
    if (!text) return undefined as T;
    return JSON.parse(text) as T;
  }

  private ifMatch(rev: number | undefined): Record<string, string> {
    return rev === undefined ? {} : { 'If-Match': `W/"${rev}"` };
  }

  // ---------- upstreams ----------

  async listUpstreams(): Promise<UpstreamListResponse> {
    return this.request<UpstreamListResponse>('GET', '/admin/v1/upstreams');
  }

  async createUpstream(input: NewUpstreamInput): Promise<UpstreamResponse> {
    return this.request<UpstreamResponse>('POST', '/admin/v1/upstreams', input);
  }

  async deleteUpstream(id: string, expectedRevision?: number): Promise<void> {
    await this.request('DELETE', `/admin/v1/upstreams/${id}`, undefined, this.ifMatch(expectedRevision));
  }

  async deleteAllUpstreamsExceptDummy(): Promise<void> {
    const list = await this.listUpstreams();
    await Promise.all(
      list.upstreams
        .filter((u) => u.name !== 'dummy')
        .map((u) => this.deleteUpstream(u.id, u.revision).catch(() => undefined)),
    );
  }

  // ---------- principals ----------

  async listPrincipals(): Promise<PrincipalListResponse> {
    return this.request<PrincipalListResponse>('GET', '/admin/v1/principals');
  }

  /**
   * Stage a new principal in the current draft and apply immediately.
   * For tests that need the draft-revision banner, call `stagePrincipal`
   * (does not apply) instead.
   */
  async createAndApplyPrincipal(input: NewPrincipalInput): Promise<PrincipalResponse> {
    const principal = await this.stagePrincipal(input);
    await this.applyDraft();
    return principal;
  }

  async stagePrincipal(input: NewPrincipalInput): Promise<PrincipalResponse> {
    return this.request<PrincipalResponse>('POST', '/admin/v1/principals', input);
  }

  async issueKey(principalId: string, label?: string): Promise<IssueKeyResponse> {
    return this.request<IssueKeyResponse>(
      'POST',
      `/admin/principals/${principalId}/keys`,
      { label },
    );
  }

  async revokeKey(principalId: string, keyId: string): Promise<void> {
    await this.request('POST', `/admin/principals/${principalId}/keys/${keyId}/revoke`);
  }

  // ---------- plugins (registry + chains) ----------

  async listWasmPlugins(): Promise<WasmListResponse> {
    return this.request<WasmListResponse>('GET', '/admin/v1/plugins/wasm');
  }

  async deleteAllWasmPlugins(): Promise<void> {
    const list = await this.listWasmPlugins();
    await Promise.all(
      list.entries.map((entry) =>
        this.request('DELETE', `/admin/v1/plugins/wasm/${entry.id}`, undefined, this.ifMatch(entry.revision)).catch(() => undefined),
      ),
    );
  }

  // ---------- config / draft ----------

  async getDraft(): Promise<DraftResponse> {
    return this.request<DraftResponse>('GET', '/admin/config/draft');
  }

  async applyDraft(): Promise<void> {
    await this.request('POST', '/admin/config/apply');
  }

  // ---------- audit ----------

  async listAudit(limit = 50): Promise<AuditResponse> {
    return this.request<AuditResponse>('GET', `/admin/audit?limit=${limit}`);
  }

  // ---------- killswitch (admin API only — UI does NOT expose this) ----------

  async setKillswitch(enabled: boolean): Promise<void> {
    if (enabled) {
      await this.request('POST', '/admin/killswitch');
    } else {
      await this.request('DELETE', '/admin/killswitch');
    }
  }

  // ---------- status / reset for between-test cleanup ----------

  async getStatus(): Promise<StatusResponse> {
    return this.request<StatusResponse>('GET', '/admin/v1/status');
  }
}

// ---------- types ----------

export type NewUpstreamInput =
  | {
      name: string;
      kind: 'anthropic_api_key';
      api_key_env: string;
      base_url?: string;
    }
  | {
      name: string;
      kind: 'anthropic_oauth';
    }
  | {
      name: string;
      kind: 'custom';
      base_url: string;
      api_key_env?: string;
    };

export type NewPrincipalInput = {
  id: string;
  enabled?: boolean;
  allowed_models?: string[];
  allowed_upstreams?: string[];
};

export interface UpstreamResponse {
  id: string;
  name: string;
  kind: string;
  status?: string;
  revision: number;
  created_at_unix_secs?: number;
}

export interface UpstreamListResponse {
  upstreams: UpstreamResponse[];
}

export interface PrincipalResponse {
  id: string;
  enabled: boolean;
  revision: number;
}

export interface PrincipalListResponse {
  principals: PrincipalResponse[];
}

export interface IssueKeyResponse {
  key_id: string;
  plaintext: string;
}

export interface WasmEntry {
  id: string;
  name: string;
  sha256: string;
  size_bytes: number;
  refcount?: number;
  revision: number;
}

export interface WasmListResponse {
  entries: WasmEntry[];
}

export interface DraftResponse {
  revision: number;
  status?: string;
}

export interface AuditEntry {
  ts_unix_secs: number;
  actor?: string;
  action: string;
  payload?: unknown;
}

export interface AuditResponse {
  entries: AuditEntry[];
}

export interface StatusResponse {
  killswitch?: boolean;
  draft_revision?: number;
}
