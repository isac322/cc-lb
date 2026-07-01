import { useBootEnv } from '../lib/queries';
import { Card, CardBody, CardHeader, Skeleton } from './ui/primitives';

interface BootEnvData {
  listener?: {
    proxy_addr?: string;
    admin_addr?: string;
    metrics_addr?: string;
  };
  tls?: {
    cert_path?: string;
    key_path?: string;
    reload_on_sighup?: boolean;
  };
  storage?: {
    kind?: string;
    path?: string;
    url?: string;
  };
  aead_key_env?: string;
  admin_token_env?: string;
  data_dir?: string;
}

export function BootEnvReadonly() {
  const { data, isLoading, error } = useBootEnv();

  if (isLoading) {
    return (
      <Card>
        <CardHeader title="Boot Environment (read-only)" />
        <CardBody>
          <Skeleton className="h-24" />
        </CardBody>
      </Card>
    );
  }

  if (error || data === undefined) {
    return (
      <Card>
        <CardHeader title="Boot Environment (read-only)" />
        <CardBody>
          <p className="text-xs text-text-faint">
            Boot config endpoint not yet available
          </p>
        </CardBody>
      </Card>
    );
  }

  const d = data as unknown as BootEnvData;

  const rows = [
    { label: 'listener.proxy_addr', value: d.listener?.proxy_addr },
    { label: 'listener.admin_addr', value: d.listener?.admin_addr },
    { label: 'listener.metrics_addr', value: d.listener?.metrics_addr },
    { label: 'tls.cert_path', value: d.tls?.cert_path },
    { label: 'tls.key_path', value: d.tls?.key_path },
    { label: 'tls.reload_on_sighup', value: d.tls?.reload_on_sighup },
    { label: 'storage.kind', value: d.storage?.kind },
    { label: 'storage.path', value: d.storage?.path },
    { label: 'storage.url', value: d.storage?.url },
    { label: 'aead_key_env', value: d.aead_key_env },
    { label: 'admin_token_env', value: d.admin_token_env },
    { label: 'data_dir', value: d.data_dir },
  ].filter((r) => r.value !== undefined && r.value !== null);

  return (
    <Card>
      <CardHeader
        title="Boot Environment (read-only)"
        subtitle="Environment variables and startup flags"
      />
      <div className="overflow-x-auto">
        <table className="min-w-full font-mono text-xs">
          <tbody>
            {rows.map((r) => (
              <tr key={r.label} className="border-b border-row last:border-0">
                <td className="px-4 py-2 text-text-faint w-1/3">{r.label}</td>
                <td className="px-4 py-2 break-all">{String(r.value)}</td>
              </tr>
            ))}
            {rows.length === 0 && (
              <tr>
                <td
                  className="px-4 py-4 text-text-faint text-center"
                  colSpan={2}
                >
                  No boot environment data available
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </Card>
  );
}
