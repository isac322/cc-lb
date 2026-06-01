import { createFileRoute, useNavigate } from '@tanstack/react-router';
import { useEffect, useMemo, useState } from 'react';
import { z } from 'zod';
import { toast } from 'sonner';
import { ChevronLeft, ExternalLink, KeyRound, Pencil, Plus, ShieldCheck, Trash2 } from 'lucide-react';
import {
  Badge,
  Button,
  Card,
  CardBody,
  CardHeader,
  EmptyState,
  Field,
  INPUT_CLASS,
  Modal,
  Skeleton,
  Sparkline,
  StatusBadge,
  cx,
} from '../components/ui/primitives';
import {
  useCreateUpstream,
  useDeleteUpstream,
  useOAuthComplete,
  useOAuthStart,
  useRecentEvents,
  useToggleUpstream,
  useUpdateUpstream,
  useUpstreams,
  type Upstream,
} from '../lib/queries';
import { eventTime } from '../lib/api';

const upstreamSearchSchema = z.object({ selectedId: z.string().optional() });

export const Route = createFileRoute('/upstreams')({
  validateSearch: upstreamSearchSchema,
  component: UpstreamsPage,
});

function spark(seed: string): number[] {
  let s = 0x811c9dc5;
  for (let i = 0; i < seed.length; i++) {
    s ^= seed.charCodeAt(i);
    s = Math.imul(s, 0x01000193) >>> 0;
  }
  return Array.from({ length: 24 }).map((_, i) => {
    s = Math.imul(s ^ (s >>> 13), 0x5bd1e995) >>> 0;
    const phase = ((s >>> 0) % 1000) / 1000;
    const wave = Math.sin(i * 0.6 + phase * Math.PI * 2);
    const noise = (((s >>> 7) % 30) - 15) / 100;
    return Math.max(8, Math.min(100, 50 + wave * 35 + noise * 30));
  });
}

function UpstreamsPage() {
  const { selectedId } = Route.useSearch();
  const navigate = useNavigate({ from: Route.fullPath });
  const upstreams = useUpstreams();
  const [createOpen, setCreateOpen] = useState(false);

  const selected = upstreams.data?.upstreams.find((u) => u.id === selectedId) ?? null;
  const select = (id: string | undefined) => navigate({ search: id ? { selectedId: id } : {} });

  return (
    <div className="h-[calc(100vh-3rem)] flex">
      {/* List pane */}
      <aside
        className={cx(
          'border-r border-subtle bg-bg-sub flex flex-col w-full md:w-[360px] shrink-0',
          selected ? 'hidden md:flex' : 'flex',
        )}
      >
        <div className="h-12 px-4 flex items-center justify-between border-b border-subtle shrink-0">
          <div>
            <h1 className="text-sm font-medium">Upstreams</h1>
            <p className="text-[11px] text-text-faint">{upstreams.data?.upstreams.length ?? 0} total</p>
          </div>
          <Button id="btn-new-upstream" size="sm" variant="primary" iconLeft={<Plus className="w-3 h-3" />} onClick={() => setCreateOpen(true)}>
            New
          </Button>
        </div>
        <div className="flex-1 overflow-y-auto p-2 space-y-1">
          {upstreams.isLoading ? (
            Array.from({ length: 3 }).map((_, i) => <Skeleton key={i} className="h-20" />)
          ) : upstreams.data?.upstreams.length ? (
            upstreams.data.upstreams.map((u) => (
              <button
                key={u.id}
                type="button"
                onClick={() => select(u.id)}
                className={cx(
                  'w-full text-left p-3 rounded-sm border transition-colors',
                  u.id === selectedId
                    ? 'border-accent/40 bg-accent/5 text-text'
                    : 'border-subtle hover:bg-overlay-3 text-text',
                )}
              >
                <div className="flex items-center justify-between gap-2 mb-1.5">
                  <div className="flex items-center gap-2 min-w-0">
                    <span className={cx('status-dot', u.enabled ? 'ok' : 'neutral')} />
                    <span className="font-medium text-sm truncate">{u.name}</span>
                  </div>
                  <Badge tone="mono">{u.kind}</Badge>
                </div>
                <div className="flex items-center gap-3 mt-2 pb-1">
                  <div className="flex-1 min-w-0" style={{ minHeight: 32 }}>
                    <Sparkline data={spark(u.id)} color={u.enabled ? 'var(--color-accent)' : 'var(--color-text-faint)'} />
                  </div>
                  <div className="text-right text-[11px] text-text-faint shrink-0">
                    <div className="font-mono tabular-nums">rev {u.revision}</div>
                  </div>
                </div>
              </button>
            ))
          ) : (
            <EmptyState title="No upstreams" description="Create your first upstream to start routing traffic." action={<Button variant="primary" onClick={() => setCreateOpen(true)}>New upstream</Button>} />
          )}
        </div>
      </aside>

      {/* Detail pane */}
      <section className={cx('flex-1 flex flex-col bg-bg min-w-0', selected ? 'flex' : 'hidden md:flex')}>
        {selected ? (
          <DetailView upstream={selected} onBack={() => select(undefined)} />
        ) : (
          <div className="flex-1 flex items-center justify-center">
            <EmptyState title="Select an upstream" description="Pick an upstream from the list to see its configuration, OAuth state, and recent requests." />
          </div>
        )}
      </section>

      <CreateUpstreamModal open={createOpen} onOpenChange={setCreateOpen} />
    </div>
  );
}

function DetailView({ upstream, onBack }: { upstream: Upstream; onBack: () => void }) {
  const toggle = useToggleUpstream();
  const del = useDeleteUpstream();
  const update = useUpdateUpstream();
  const oauthStart = useOAuthStart();
  const oauthComplete = useOAuthComplete();
  const [editOpen, setEditOpen] = useState(false);
  const [oauthOpen, setOauthOpen] = useState(false);
  const [oauthState, setOauthState] = useState<{ authorize_url?: string; state_token?: string; code?: string }>({});
  const [name, setName] = useState(upstream.name);
  const [baseUrl, setBaseUrl] = useState(upstream.base_url ?? '');

  // The backend `/admin/events/recent?upstream=` param only accepts the
  // RequestEventUpstream class enum (`anthropic_direct` / `custom_anthropic_spec`),
  // not an upstream display name or id. Passing the name returns 400
  // `invalid_upstream`, so we fetch unfiltered and narrow client-side by
  // `upstream_name`.
  const recent = useRecentEvents({ limit: '50' });
  const recentForUpstream = useMemo(
    () => (recent.data?.events ?? []).filter((e) => e.upstream_name === upstream.name).slice(0, 5),
    [recent.data, upstream.name],
  );

  return (
    <>
      <header className="px-4 md:px-6 py-4 border-b border-subtle flex items-start justify-between gap-3 flex-wrap shrink-0">
        <div className="min-w-0">
          <button type="button" onClick={onBack} className="md:hidden inline-flex items-center gap-1 text-xs text-text-faint hover:text-text mb-1">
            <ChevronLeft className="w-3 h-3" /> Back
          </button>
          <div className="flex items-center gap-3 flex-wrap">
            <h2 className="text-lg font-medium text-text truncate">{upstream.name}</h2>
            <StatusBadge tone={upstream.enabled ? 'ok' : 'neutral'} label={upstream.enabled ? 'Enabled' : 'Disabled'} />
            <Badge tone="mono">{upstream.kind}</Badge>
          </div>
          <div className="text-xs text-text-faint font-mono mt-0.5">ID {upstream.id} · rev {upstream.revision}</div>
        </div>
        <div className="flex items-center gap-2 shrink-0">
          <Button size="sm" iconLeft={<Pencil className="w-3 h-3" />} onClick={() => setEditOpen(true)}>Edit</Button>
          <Button
            size="sm"
            onClick={() => toggle.mutate({ id: upstream.id, enabled: !upstream.enabled, revision: upstream.revision }, { onSuccess: () => toast.success(upstream.enabled ? 'Upstream disabled' : 'Upstream enabled') })}
          >
            {upstream.enabled ? 'Disable' : 'Enable'}
          </Button>
          <Button
            size="sm"
            variant="danger"
            iconLeft={<Trash2 className="w-3 h-3" />}
            onClick={() => {
              if (confirm(`Delete ${upstream.name}? This cannot be undone.`)) {
                del.mutate({ id: upstream.id, revision: upstream.revision }, { onSuccess: () => { toast.success('Upstream deleted'); onBack(); } });
              }
            }}
          >
            Delete
          </Button>
        </div>
      </header>

      <div className="flex-1 overflow-y-auto p-4 md:p-6 space-y-6">
        <Card>
          <CardHeader title="Configuration" />
          <CardBody className="grid grid-cols-1 md:grid-cols-2 gap-4 text-sm">
            <div><div className="text-[11px] text-text-faint uppercase tracking-wider">Kind</div><div className="font-mono mt-0.5">{upstream.kind}</div></div>
            <div><div className="text-[11px] text-text-faint uppercase tracking-wider">Base URL</div><div className="font-mono mt-0.5 break-all">{upstream.base_url ?? '—'}</div></div>
            {upstream.api_key_env ? (
              <div><div className="text-[11px] text-text-faint uppercase tracking-wider">API Key Env</div><div className="font-mono mt-0.5">{upstream.api_key_env}</div></div>
            ) : null}
            {upstream.shape_plugin ? (
              <div><div className="text-[11px] text-text-faint uppercase tracking-wider">Shape Plugin</div><div className="font-mono mt-0.5">{upstream.shape_plugin.wasm_registry_id}</div></div>
            ) : null}
          </CardBody>
        </Card>

        {upstream.kind === 'anthropic_oauth' ? (
          <Card>
            <CardHeader
              title="OAuth Status"
              subtitle="Mock fingerprint + force refresh"
              action={
                <Button
                  size="sm"
                  iconLeft={<KeyRound className="w-3 h-3" />}
                  onClick={() => {
                    oauthStart.mutate(upstream.id, {
                      onSuccess: (res) => {
                        setOauthState({ authorize_url: res.authorize_url, state_token: res.state_token, code: '' });
                        setOauthOpen(true);
                      },
                    });
                  }}
                >
                  Connect via OAuth
                </Button>
              }
            />
            <CardBody className="grid grid-cols-1 md:grid-cols-3 gap-4 text-sm">
              <div><div className="text-[11px] text-text-faint uppercase tracking-wider">Fingerprint</div><div className="font-mono mt-0.5">a1b2c3d4 <ShieldCheck className="w-3 h-3 inline ml-1 text-[color:var(--color-ok)]" /></div></div>
              <div><div className="text-[11px] text-text-faint uppercase tracking-wider">Expires</div><div className="font-mono mt-0.5">{new Date(Date.now() + 3600 * 8 * 1000).toISOString().replace('T', ' ').slice(0, 19)}</div></div>
              <div><div className="text-[11px] text-text-faint uppercase tracking-wider">Last Refresh</div><div className="mt-0.5">Success</div></div>
            </CardBody>
          </Card>
        ) : null}

        <Card>
          <CardHeader title="Recent Requests" subtitle={`Last 5 against ${upstream.name}`} />
          <div className="overflow-x-auto">
            <table className="w-full font-mono text-xs">
              <thead className="bg-overlay-1 border-b border-subtle">
                <tr className="text-text-faint text-[10px] uppercase tracking-wider">
                  <th className="text-left px-3 py-2">Time</th>
                  <th className="text-left px-3 py-2">Principal</th>
                  <th className="text-left px-3 py-2">Model</th>
                  <th className="text-right px-3 py-2">Status</th>
                  <th className="text-right px-3 py-2">Latency</th>
                </tr>
              </thead>
              <tbody>
                {recentForUpstream.length ? (
                  recentForUpstream.map((e) => (
                    <tr key={e.request_id} className="border-b border-subtle/40 hover:bg-overlay-1">
                      <td className="px-3 py-2 text-text-faint whitespace-nowrap">{eventTime(e)?.toISOString().slice(11, 19) ?? '—'} UTC</td>
                      <td className="px-3 py-2">{e.principal_id ?? '—'}</td>
                      <td className="px-3 py-2 text-text-faint truncate max-w-[200px]">{e.model ?? '—'}</td>
                      <td className={cx('px-3 py-2 text-right', e.status >= 500 ? 'text-red-400' : e.status >= 400 ? 'text-amber-400' : 'text-green-400')}>{e.status}</td>
                      <td className="px-3 py-2 text-right tabular-nums">{e.duration_ms}ms</td>
                    </tr>
                  ))
                ) : (
                  <tr><td colSpan={5} className="px-3 py-6 text-center text-text-faint text-xs">No recent requests for this upstream</td></tr>
                )}
              </tbody>
            </table>
          </div>
        </Card>
      </div>

      <Modal
        open={editOpen}
        onOpenChange={setEditOpen}
        title="Edit upstream"
        footer={
          <>
            <Button onClick={() => setEditOpen(false)}>Cancel</Button>
            <Button
              variant="primary"
              onClick={() => {
                const trimmedName = name.trim();
                const trimmedBase = baseUrl.trim();
                update.mutate(
                  {
                    id: upstream.id,
                    body: {
                      name: trimmedName,
                      base_url: trimmedBase === '' ? null : trimmedBase,
                    },
                    revision: upstream.revision,
                  },
                  { onSuccess: () => { toast.success('Upstream updated'); setEditOpen(false); } },
                );
              }}
            >
              Save
            </Button>
          </>
        }
      >
        <div className="space-y-3">
          <Field label="Name" required><input className={INPUT_CLASS} value={name} onChange={(e) => setName(e.target.value)} /></Field>
          <Field label="Base URL"><input className={INPUT_CLASS} value={baseUrl} onChange={(e) => setBaseUrl(e.target.value)} /></Field>
        </div>
      </Modal>

      <Modal
        open={oauthOpen}
        onOpenChange={setOauthOpen}
        title="OAuth Authorization"
        description="Open the authorize URL, then paste the code below."
        size="lg"
        footer={
          <>
            <Button onClick={() => setOauthOpen(false)}>Cancel</Button>
            <Button
              variant="primary"
              disabled={!oauthState.code || !oauthState.state_token}
              onClick={() => {
                oauthComplete.mutate(
                  { id: upstream.id, state_token: oauthState.state_token!, code: oauthState.code! },
                  { onSuccess: () => { toast.success('OAuth connected'); setOauthOpen(false); } },
                );
              }}
            >
              Complete
            </Button>
          </>
        }
      >
        <div className="space-y-3">
          <p className="text-xs text-text-faint">
            1. Open the authorization URL below. 2. Approve access. 3. Copy the returned code and paste it here.
          </p>
          <Field label="Authorize URL">
            <code className="block p-2 text-xs font-mono bg-overlay-2 border border-subtle rounded-sm break-all select-all">{oauthState.authorize_url ?? ''}</code>
            <div className="mt-2">
              <Button
                size="sm"
                variant="primary"
                disabled={!oauthState.authorize_url}
                iconLeft={<ExternalLink className="w-3 h-3" />}
                onClick={() => {
                  if (oauthState.authorize_url) {
                    window.open(oauthState.authorize_url, '_blank', 'noopener,noreferrer');
                  }
                }}
              >
                Open authorization URL
              </Button>
            </div>
          </Field>
          <Field label="State Token">
            <code className="block p-2 text-xs font-mono bg-overlay-2 border border-subtle rounded-sm break-all select-all">{oauthState.state_token ?? ''}</code>
          </Field>
          <Field label="Authorization Code" required>
            <input className={INPUT_CLASS + ' font-mono'} value={oauthState.code ?? ''} onChange={(e) => setOauthState((s) => ({ ...s, code: e.target.value }))} placeholder="paste code…" />
          </Field>
        </div>
      </Modal>
    </>
  );
}

function CreateUpstreamModal({ open, onOpenChange }: { open: boolean; onOpenChange: (open: boolean) => void }) {
  const create = useCreateUpstream();
  const [name, setName] = useState('');
  const [kind, setKind] = useState<'anthropic_api_key' | 'anthropic_oauth' | 'custom'>('anthropic_api_key');
  const [baseUrl, setBaseUrl] = useState('https://api.anthropic.com');
  const [apiKeyEnv, setApiKeyEnv] = useState('ANTHROPIC_API_KEY');

  // Only reset to defaults when the modal transitions to closed. Submit
  // failures keep the dialog open, so the user's typed values are preserved.
  useEffect(() => {
    if (!open) {
      setName('');
      setKind('anthropic_api_key');
      setBaseUrl('https://api.anthropic.com');
      setApiKeyEnv('ANTHROPIC_API_KEY');
    }
  }, [open]);

  return (
    <Modal
      open={open}
      onOpenChange={onOpenChange}
      title="New upstream"
      description="Register an Anthropic API key, OAuth principal, or custom backend."
      footer={
        <>
          <Button onClick={() => onOpenChange(false)}>Cancel</Button>
          <Button
            variant="primary"
            disabled={!name.trim()}
            onClick={() => {
              const trimmedName = name.trim();
              const trimmedBase = baseUrl.trim();
              const trimmedEnv = apiKeyEnv.trim();
              create.mutate(
                {
                  name: trimmedName,
                  kind,
                  base_url: trimmedBase === '' ? null : trimmedBase,
                  api_key_env:
                    kind === 'anthropic_api_key' && trimmedEnv !== '' ? trimmedEnv : null,
                },
                { onSuccess: () => { toast.success('Upstream created'); onOpenChange(false); } },
              );
            }}
          >
            Create
          </Button>
        </>
      }
    >
      <div className="space-y-3">
        <Field label="Name" required hint="A unique label, e.g. anthropic-prod">
          <input className={INPUT_CLASS} value={name} onChange={(e) => setName(e.target.value)} placeholder="anthropic-prod" />
        </Field>
        <Field label="Kind" required>
          <select className={INPUT_CLASS} value={kind} onChange={(e) => setKind(e.target.value as typeof kind)}>
            <option value="anthropic_api_key">Anthropic API Key</option>
            <option value="anthropic_oauth">Anthropic OAuth</option>
            <option value="custom">Custom backend</option>
          </select>
        </Field>
        <Field label="Base URL">
          <input className={INPUT_CLASS} value={baseUrl} onChange={(e) => setBaseUrl(e.target.value)} />
        </Field>
        {kind === 'anthropic_api_key' ? (
          <Field label="API Key Env Var" hint="Name of env var holding the API key" required>
            <input className={INPUT_CLASS + ' font-mono'} value={apiKeyEnv} onChange={(e) => setApiKeyEnv(e.target.value)} />
          </Field>
        ) : null}
        {kind === 'anthropic_oauth' ? (
          <p className="text-xs text-text-faint">After creation, open the upstream and click <span className="font-mono">Connect via OAuth</span> to begin the PKCE flow.</p>
        ) : null}
      </div>
    </Modal>
  );
}
