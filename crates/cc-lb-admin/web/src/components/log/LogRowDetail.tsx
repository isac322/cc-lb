import type { RequestEvent } from '../../lib/api';
import { Button } from '../primitives/Button';

export function LogRowDetail({
  event,
  onClose,
}: {
  event: RequestEvent;
  onClose: () => void;
}) {
  return (
    <div className="relative">
      <div className="absolute top-0 right-0">
        <Button variant="ghost" onClick={onClose} className="px-2 py-1 text-xs">
          Close
        </Button>
      </div>
      <h3 className="text-sm font-medium text-graphite-100 mb-4">
        Request Details
      </h3>
      <div className="grid grid-cols-2 gap-4 text-sm">
        <div>
          <span className="text-graphite-500 block text-xs mb-1">
            Request ID
          </span>
          <span className="text-graphite-200 font-mono">
            {event.request_id}
          </span>
        </div>
        <div>
          <span className="text-graphite-500 block text-xs mb-1">
            Timestamp
          </span>
          <span className="text-graphite-200">
            {new Date(event.ts).toISOString()}
          </span>
        </div>
        <div>
          <span className="text-graphite-500 block text-xs mb-1">
            Principal
          </span>
          <span className="text-graphite-200">{event.principal_id || '-'}</span>
        </div>
        <div>
          <span className="text-graphite-500 block text-xs mb-1">Model</span>
          <span className="text-graphite-200">{event.model || '-'}</span>
        </div>
        <div>
          <span className="text-graphite-500 block text-xs mb-1">Upstream</span>
          <span className="text-graphite-200">{event.upstream || '-'}</span>
        </div>
        <div>
          <span className="text-graphite-500 block text-xs mb-1">Status</span>
          <span className="text-graphite-200">{event.status}</span>
        </div>
        <div>
          <span className="text-graphite-500 block text-xs mb-1">Duration</span>
          <span className="text-graphite-200">{event.duration_ms}ms</span>
        </div>
        {event.input_tokens !== undefined && (
          <div>
            <span className="text-graphite-500 block text-xs mb-1">
              Input Tokens
            </span>
            <span className="text-graphite-200">{event.input_tokens}</span>
          </div>
        )}
        {event.output_tokens !== undefined && (
          <div>
            <span className="text-graphite-500 block text-xs mb-1">
              Output Tokens
            </span>
            <span className="text-graphite-200">{event.output_tokens}</span>
          </div>
        )}
        {event.error_code && (
          <div>
            <span className="text-graphite-500 block text-xs mb-1">
              Error Code
            </span>
            <span className="text-graphite-200">{event.error_code}</span>
          </div>
        )}
      </div>
    </div>
  );
}
