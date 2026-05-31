import type { RequestEvent } from '../../lib/api';
import { Button } from '../primitives/Button';
import { eventTimestampMs } from './LogTable';

export function LogRowDetail({
  event,
  onClose,
}: {
  event: RequestEvent;
  onClose: () => void;
}) {
  const upstream = event.upstream_name || event.upstream || '-';
  const ts = eventTimestampMs(event);

  return (
    <div className="relative">
      <div className="absolute top-0 right-0">
        <Button variant="ghost" onClick={onClose} className="px-2 py-1 text-xs">
          Close
        </Button>
      </div>
      <h3 className="text-sm font-medium text-graphite-100 mb-3">
        Request Details
      </h3>
      <div className="grid grid-cols-4 gap-x-4 gap-y-2 text-xs">
        <Field label="Request ID" value={event.request_id} mono />
        <Field label="Timestamp" value={new Date(ts).toISOString()} />
        <Field label="Principal" value={event.principal_id || '-'} />
        <Field label="Principal Kind" value={event.principal_kind || '-'} />
        <Field label="Key ID" value={event.key_id || '-'} mono />
        <Field label="Model" value={event.model || '-'} />
        <Field label="Upstream" value={upstream} />
        <Field label="Status" value={String(event.status)} />
        {event.error_code && (
          <Field label="Error Code" value={event.error_code} />
        )}
      </div>

      <div className="grid grid-cols-5 gap-x-4 gap-y-2 text-xs mt-4 pt-3 border-t border-graphite-700/40">
        <Field
          label="Input"
          value={event.input_tokens !== undefined ? String(event.input_tokens) : '-'}
        />
        <Field
          label="Output"
          value={event.output_tokens !== undefined ? String(event.output_tokens) : '-'}
        />
        <Field
          label="Cache Creation"
          value={
            event.cache_creation_input_tokens !== undefined
              ? String(event.cache_creation_input_tokens)
              : '-'
          }
        />
        <Field
          label="Cache Read"
          value={
            event.cache_read_input_tokens !== undefined
              ? String(event.cache_read_input_tokens)
              : '-'
          }
        />
        <Field
          label="Virtual Cost"
          value={
            event.cost_usd_micros !== undefined
              ? formatUsdMicros(event.cost_usd_micros)
              : '-'
          }
        />
      </div>

      <div className="mt-4 pt-3 border-t border-graphite-700/40">
        <LatencyBreakdown event={event} />
      </div>
    </div>
  );
}

function Field({
  label,
  value,
  mono = false,
}: {
  label: string;
  value: string;
  mono?: boolean;
}) {
  return (
    <div>
      <span className="text-graphite-500 block text-xs mb-1">{label}</span>
      <span className={`text-graphite-200 ${mono ? 'font-mono' : ''}`}>
        {value}
      </span>
    </div>
  );
}

function LatencyBreakdown({ event }: { event: RequestEvent }) {
  const total = event.duration_ms;
  if (total <= 0) {
    return (
      <div className="text-xs text-graphite-500">no latency data available</div>
    );
  }

  const stages: { label: string; ms: number; color: string }[] = [
    {
      label: 'Proxy Setup',
      ms: event.proxy_setup_ms ?? 0,
      color: 'bg-emerald-600',
    },
    { label: 'Shape', ms: event.shape_ms ?? 0, color: 'bg-cyan-600' },
    { label: 'Sign', ms: event.sign_ms ?? 0, color: 'bg-sky-600' },
    {
      label: 'Upstream TTFB',
      ms: event.upstream_ttfb_ms ?? 0,
      color: 'bg-purple-600',
    },
    {
      label: 'Upstream Body',
      ms: event.upstream_body_ms ?? 0,
      color: 'bg-fuchsia-600',
    },
  ];
  const measured = stages.reduce((acc, s) => acc + s.ms, 0);
  const unmeasured = Math.max(0, total - measured);

  return (
    <div className="space-y-2">
      <div className="flex items-center gap-3">
        <div className="text-[10px] text-graphite-500 uppercase tracking-wider w-16 shrink-0">
          Latency
        </div>
        <div className="flex h-4 flex-1 rounded overflow-hidden border border-graphite-700">
          {stages.map((s) => {
            if (s.ms <= 0) return null;
            const pct = clampPct((s.ms / total) * 100);
            return (
              <div
                key={s.label}
                className={`${s.color} flex items-center justify-center text-[9px] text-graphite-50 font-mono overflow-hidden`}
                style={{ width: `${pct}%` }}
                title={`${s.label}: ${s.ms}ms (${pct.toFixed(1)}%)`}
              >
                {pct > 12 ? `${s.ms}ms` : ''}
              </div>
            );
          })}
          {unmeasured > 0 && (
            <div
              className="bg-graphite-700 flex items-center justify-center text-[9px] text-graphite-300 font-mono"
              style={{ width: `${clampPct((unmeasured / total) * 100)}%` }}
              title={`unaccounted: ${unmeasured}ms`}
            />
          )}
        </div>
        <div className="text-[10px] text-graphite-300 font-mono w-12 shrink-0 text-right">
          {total}ms
        </div>
      </div>
      <div className="grid grid-cols-5 gap-x-4 gap-y-1 text-xs">
        {stages.map((s) => (
          <Field
            key={s.label}
            label={s.label}
            value={`${s.ms}ms`}
          />
        ))}
      </div>
    </div>
  );
}

function clampPct(v: number): number {
  if (!Number.isFinite(v)) return 0;
  return Math.max(0, Math.min(100, v));
}

function formatUsdMicros(micros: number): string {
  if (micros === 0) return '$0';
  const usd = micros / 1_000_000;
  if (Math.abs(usd) < 0.01) return `$${usd.toFixed(6)}`;
  return `$${usd.toFixed(4)}`;
}
