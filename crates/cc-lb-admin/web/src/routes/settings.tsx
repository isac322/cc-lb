import { createFileRoute } from '@tanstack/react-router';
import {
  AlertTriangle,
  CheckCircle2,
  Download,
  PlayCircle,
  RefreshCw,
  Save,
} from 'lucide-react';
import { useState } from 'react';
import { toast } from 'sonner';
import {
  Button,
  Card,
  CardBody,
  CardHeader,
  cx,
  Modal,
  PageContainer,
  Section,
  Skeleton,
  StatusBadge,
} from '../components/ui/primitives';
import { downloadJson, eventTime } from '../lib/api';
import {
  useApplyConfig,
  useAudit,
  useConfigCurrent,
  useConfigDraft,
  useConfigHistory,
  useConfigSchema,
  useReloadConfig,
  useSaveDraft,
  useStatus,
  useValidateConfig,
} from '../lib/queries';

export const Route = createFileRoute('/settings')({
  component: SettingsPage,
});

function SettingsPage() {
  const status = useStatus();
  const [rotateOpen, setRotateOpen] = useState(false);

  return (
    <PageContainer>
      <header>
        <h1 className="text-lg font-medium">Settings</h1>
        <p className="text-xs text-text-faint mt-1">
          Admin self-service, configuration draft pipeline, audit log, and
          exports.
        </p>
      </header>

      {/* Version card */}
      <Card>
        <CardHeader
          title="Version"
          subtitle={
            status.data
              ? `cc-lb ${status.data.version} · ${status.data.git_sha} · uptime ${Math.floor(status.data.uptime_secs / 60)}m`
              : '—'
          }
        />
        <CardBody className="space-y-3 text-xs">
          <div className="grid grid-cols-3 gap-3">
            <Row label="Rust" value={status.data?.build.rust_version ?? '—'} />
            <Row label="Profile" value={status.data?.build.profile ?? '—'} />
            <Row label="Generation" value={status.data?.generation ?? '—'} />
          </div>
          <div className="pt-2 border-t border-subtle/40">
            <div className="text-text-faint text-[10px] uppercase tracking-wider mb-0.5">
              Build target
            </div>
            <div className="font-mono break-all">
              {status.data?.build.target ?? '—'}
            </div>
          </div>
        </CardBody>
      </Card>

      {/* Token rotation */}
      <Card>
        <CardHeader
          title="Admin Token"
          subtitle="Rotate the primary bearer token. All current admin sessions will be invalidated."
        />
        <CardBody>
          <Button
            id="btn-rotate-token"
            variant="danger"
            size="md"
            iconLeft={<AlertTriangle className="w-4 h-4" />}
            onClick={() => setRotateOpen(true)}
          >
            Rotate token
          </Button>
        </CardBody>
      </Card>

      {/* Config draft pipeline */}
      <ConfigDraftSection />

      {/* History */}
      <ConfigHistorySection />

      {/* Audit */}
      <AuditSection />

      {/* Restart-required matrix */}
      <RestartRequiredMatrix />

      {/* Export */}
      <Card>
        <CardHeader
          title="Configuration Export"
          subtitle="Download a JSON snapshot of all upstreams, principals, plugins, and chains."
        />
        <CardBody>
          <Button
            iconLeft={<Download className="w-4 h-4" />}
            onClick={() => downloadExport()}
          >
            Download export.json
          </Button>
        </CardBody>
      </Card>

      <Modal
        open={rotateOpen}
        onOpenChange={setRotateOpen}
        title="Rotate admin token?"
        footer={
          <>
            <Button onClick={() => setRotateOpen(false)}>Cancel</Button>
            <Button
              variant="danger"
              onClick={() => {
                toast.success('Token rotation queued (mock)');
                setRotateOpen(false);
              }}
            >
              Confirm rotate
            </Button>
          </>
        }
      >
        <p className="text-sm text-text-muted">
          Rotation invalidates the current admin token. You will need the new
          token to access this dashboard.
        </p>
      </Modal>
    </PageContainer>
  );
}

function ConfigDraftSection() {
  const draft = useConfigDraft();
  const current = useConfigCurrent();
  const schema = useConfigSchema();
  const save = useSaveDraft();
  const validate = useValidateConfig();
  const apply = useApplyConfig();
  const reload = useReloadConfig();
  const [text, setText] = useState('');

  const draftRevision = draft.data?.revision ?? null;
  const lastValidatedRevision = draft.data?.last_validated_revision ?? null;
  const lastValidationError = draft.data?.last_validation_error ?? null;
  const canApply =
    lastValidatedRevision != null &&
    lastValidationError == null &&
    draftRevision != null &&
    lastValidatedRevision === draftRevision;
  const lastValidatedLabel = lastValidationError
    ? `error @ rev ${lastValidatedRevision ?? '—'}`
    : lastValidatedRevision != null
      ? `rev ${lastValidatedRevision}`
      : '—';

  return (
    <Section
      title="Configuration Draft"
      subtitle="Edit → validate → apply pipeline"
    >
      <Card>
        <CardHeader
          title="Draft"
          action={
            <div className="flex items-center gap-2">
              <Button
                size="sm"
                iconLeft={<Save className="w-3 h-3" />}
                disabled={!text}
                onClick={() => {
                  try {
                    const parsed = JSON.parse(text);
                    save.mutate(
                      { draft: parsed, expected_revision: draftRevision ?? 0 },
                      { onSuccess: () => toast.success('Draft saved') },
                    );
                  } catch {
                    toast.error('Draft is not valid JSON');
                  }
                }}
              >
                Save
              </Button>
              <Button
                size="sm"
                iconLeft={<CheckCircle2 className="w-3 h-3" />}
                onClick={() =>
                  validate.mutate(draftRevision ?? 0, {
                    onSuccess: (r) =>
                      toast.success(
                        r.valid ? 'Draft valid' : `Invalid: ${r.error}`,
                      ),
                  })
                }
              >
                Validate
              </Button>
              <Button
                size="sm"
                variant="primary"
                iconLeft={<PlayCircle className="w-3 h-3" />}
                disabled={!canApply}
                onClick={() =>
                  apply.mutate(lastValidatedRevision ?? 0, {
                    onSuccess: (r) =>
                      toast.success(`Applied revision ${r.applied_revision}`),
                  })
                }
              >
                Apply
              </Button>
              <Button
                size="sm"
                iconLeft={<RefreshCw className="w-3 h-3" />}
                onClick={() =>
                  reload.mutate(undefined, {
                    onSuccess: () => toast.success('Reload triggered'),
                  })
                }
              >
                Reload
              </Button>
            </div>
          }
        />
        <CardBody className="space-y-3">
          <div className="text-xs text-text-faint flex flex-wrap gap-4">
            <span>
              Revision:{' '}
              <span className="font-mono">{draftRevision ?? '—'}</span>
            </span>
            <span>
              Last validated:{' '}
              <span
                className={cx(
                  'font-mono',
                  lastValidationError ? 'text-red-400' : undefined,
                )}
              >
                {lastValidatedLabel}
              </span>
            </span>
            <span>
              Saved at:{' '}
              <span className="font-mono">
                {draft.data?.saved_at_unix_secs
                  ? new Date(draft.data.saved_at_unix_secs * 1000)
                      .toISOString()
                      .slice(0, 19)
                      .replace('T', ' ')
                  : '—'}
              </span>
            </span>
          </div>
          <textarea
            className="w-full min-h-[260px] p-3 text-xs font-mono bg-panel-strong border border-subtle rounded-sm placeholder:text-text-faint focus:border-accent focus:outline-none"
            style={{ lineHeight: 1.5 }}
            value={text}
            onChange={(e) => setText(e.target.value)}
            placeholder={
              current.data
                ? JSON.stringify(current.data, null, 2)
                : 'JSON config draft…'
            }
          />
          {schema.data ? (
            <details className="text-xs">
              <summary className="cursor-pointer text-text-faint">
                Coverage checklist ({schema.data.coverage_checklist.length}{' '}
                fields)
              </summary>
              <ul className="mt-2 grid grid-cols-1 md:grid-cols-2 gap-1 font-mono">
                {schema.data.coverage_checklist.map((f) => (
                  <li key={f}>· {f}</li>
                ))}
              </ul>
            </details>
          ) : null}
        </CardBody>
      </Card>
    </Section>
  );
}

function ConfigHistorySection() {
  const history = useConfigHistory();
  return (
    <Section title="Configuration History" subtitle="Last 20 applied revisions">
      <Card>
        <div className="overflow-x-auto">
          {history.isLoading ? (
            <CardBody>
              <Skeleton className="h-12" />
            </CardBody>
          ) : history.data?.history.length ? (
            <table className="min-w-[640px] w-full font-mono text-xs">
              <thead className="bg-panel-strong border-b border-subtle">
                <tr className="text-text-faint text-[10px] uppercase tracking-wider">
                  <th className="text-right px-4 py-2">Rev</th>
                  <th className="text-left px-4 py-2">Applied</th>
                  <th className="text-right px-4 py-2">Upstreams</th>
                  <th className="text-right px-4 py-2">Principals</th>
                  <th className="text-right px-4 py-2">Plugins</th>
                  <th className="text-center px-4 py-2">TLS</th>
                </tr>
              </thead>
              <tbody>
                {history.data.history.map((h) => (
                  <tr key={h.revision} className="border-b border-subtle/40">
                    <td className="px-4 py-2 text-right">{h.revision}</td>
                    <td className="px-4 py-2">
                      {new Date(h.applied_at_unix_secs * 1000)
                        .toISOString()
                        .replace('T', ' ')
                        .slice(0, 19)}
                    </td>
                    <td className="px-4 py-2 text-right tabular-nums">
                      {h.config_summary.upstreams}
                    </td>
                    <td className="px-4 py-2 text-right tabular-nums">
                      {h.config_summary.principals}
                    </td>
                    <td className="px-4 py-2 text-right tabular-nums">
                      {h.config_summary.plugin_count}
                    </td>
                    <td className="px-4 py-2 text-center">
                      <StatusBadge
                        tone={h.config_summary.tls_enabled ? 'ok' : 'neutral'}
                        label={h.config_summary.tls_enabled ? 'on' : 'off'}
                      />
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          ) : (
            <CardBody>
              <p className="text-xs text-text-faint">No history available.</p>
            </CardBody>
          )}
        </div>
      </Card>
    </Section>
  );
}

function AuditSection() {
  const audit = useAudit({ limit: '20' });
  return (
    <Section title="Audit Log" subtitle="Last 20 audit entries">
      <Card>
        <div className="overflow-x-auto">
          {audit.isLoading ? (
            <CardBody>
              <Skeleton className="h-12" />
            </CardBody>
          ) : audit.data?.entries.length ? (
            <table className="min-w-[800px] w-full font-mono text-xs">
              <thead className="bg-overlay-1 border-b border-subtle">
                <tr className="text-text-faint text-[10px] uppercase tracking-wider">
                  <th className="text-left px-4 py-2">Time</th>
                  <th className="text-left px-4 py-2">Principal</th>
                  <th className="text-left px-4 py-2">Route</th>
                  <th className="text-left px-4 py-2">Upstream</th>
                  <th className="text-left px-4 py-2">Model</th>
                  <th className="text-right px-4 py-2">Status</th>
                  <th className="text-right px-4 py-2">Duration</th>
                </tr>
              </thead>
              <tbody>
                {audit.data.entries.map((e) => (
                  <tr
                    key={e.request_id}
                    className="border-b border-subtle/40 hover:bg-overlay-1"
                  >
                    <td className="px-4 py-2 text-text-muted whitespace-nowrap">
                      {eventTime(e)?.toISOString().slice(11, 19) ?? '—'} UTC
                    </td>
                    <td className="px-4 py-2">{e.principal_id}</td>
                    <td className="px-4 py-2">{e.route}</td>
                    <td className="px-4 py-2">{e.upstream}</td>
                    <td className="px-4 py-2 text-text-faint truncate max-w-[200px]">
                      {e.model ?? '—'}
                    </td>
                    <td
                      className={cx(
                        'px-4 py-2 text-right',
                        e.status >= 500
                          ? 'text-red-400'
                          : e.status >= 400
                            ? 'text-amber-400'
                            : 'text-green-400',
                      )}
                    >
                      {e.status}
                    </td>
                    <td className="px-4 py-2 text-right tabular-nums">
                      {e.duration_ms}ms
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          ) : (
            <CardBody>
              <p className="text-xs text-text-faint">No audit entries.</p>
            </CardBody>
          )}
        </div>
      </Card>
    </Section>
  );
}

const RESTART_MATRIX: { field: string; reason: string }[] = [
  {
    field: 'listener.proxy_addr',
    reason: 'Socket bindings are fixed at process start',
  },
  {
    field: 'listener.admin_addr',
    reason: 'Socket bindings are fixed at process start',
  },
  {
    field: 'listener.metrics_addr',
    reason: 'Socket bindings are fixed at process start',
  },
  { field: 'listener.tls.cert_path', reason: 'Listener TLS certificate' },
  { field: 'listener.tls.key_path', reason: 'Listener TLS key' },
  { field: 'tls.cert_path', reason: 'TLS certificate' },
  { field: 'tls.key_path', reason: 'TLS key' },
  { field: 'storage.path', reason: 'Storage backend' },
  { field: 'storage.url', reason: 'Storage backend' },
  { field: 'storage.pool', reason: 'Storage pool' },
  { field: 'aead.key_env', reason: 'Storage encryption key env' },
  { field: 'oauth.anthropic.client_id', reason: 'Anthropic OAuth client' },
  { field: 'oauth.anthropic.auth_url', reason: 'Anthropic OAuth endpoint' },
  { field: 'oauth.anthropic.token_url', reason: 'Anthropic OAuth endpoint' },
  { field: 'oauth.anthropic.redirect_uri', reason: 'Anthropic OAuth redirect' },
  { field: 'oauth.anthropic.scopes', reason: 'Anthropic OAuth scopes' },
];

function RestartRequiredMatrix() {
  return (
    <Section
      title="Hot-Reload Behaviour Matrix"
      subtitle="Which cc-lb.toml fields are hot-reloadable, and which require a process restart."
    >
      <Card>
        <div className="overflow-x-auto">
          <table className="min-w-[640px] w-full font-mono text-xs">
            <thead className="bg-overlay-1 border-b border-subtle">
              <tr className="text-text-faint text-[10px] uppercase tracking-wider">
                <th className="text-left px-4 py-2">Field</th>
                <th className="text-center px-4 py-2">Hot reload</th>
                <th className="text-left px-4 py-2">Reason</th>
              </tr>
            </thead>
            <tbody>
              <tr className="border-b border-subtle/40 bg-[color:var(--color-ok)]/[0.06]">
                <td className="px-4 py-2">
                  upstreams · principals · plugin chains
                </td>
                <td className="px-4 py-2 text-center">
                  <StatusBadge tone="ok" label="Yes" />
                </td>
                <td className="px-4 py-2 text-text-muted">
                  Fully dynamic via admin DB; no restart required.
                </td>
              </tr>
              {RESTART_MATRIX.map((r) => (
                <tr key={r.field} className="border-b border-subtle/40">
                  <td className="px-4 py-2">{r.field}</td>
                  <td className="px-4 py-2 text-center">
                    <StatusBadge tone="danger" label="No" />
                  </td>
                  <td className="px-4 py-2 text-text-muted">{r.reason}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Card>
    </Section>
  );
}

function Row({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div className="flex items-center justify-between">
      <span className="text-text-faint">{label}</span>
      <span className="text-right">{value}</span>
    </div>
  );
}

async function downloadExport() {
  try {
    await downloadJson(
      '/admin/v1/export',
      `cc-lb-export-${new Date().toISOString().slice(0, 16)}.json`,
    );
    toast.success('Export downloaded');
  } catch (e) {
    toast.error(`Export failed: ${String(e)}`);
  }
}
