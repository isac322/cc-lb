import type { UpstreamHealthResponse } from '../../lib/api';
import { formatRelativeTime } from '../../lib/time';
import { Card } from '../primitives/Card';
import { StatusChip } from '../primitives/StatusChip';
import { BreakerChip } from './BreakerChip';
import { BulkheadGauge } from './BulkheadGauge';
import { ErrorMiniChart } from './ErrorMiniChart';

interface UpstreamCardProps {
  health: UpstreamHealthResponse;
}

export function UpstreamCard({ health }: UpstreamCardProps) {
  return (
    <Card className="relative overflow-hidden">
      {health.killswitch && (
        <div className="absolute top-0 left-0 right-0 bg-red-500 text-white text-xs font-bold px-3 py-1 text-center">
          KILLSWITCH ACTIVE
        </div>
      )}

      <div className={`p-4 ${health.killswitch ? 'pt-8' : ''}`}>
        <div className="flex justify-between items-start mb-4">
          <div>
            <h3 className="text-lg font-mono font-semibold text-graphite-50">
              {health.name}
            </h3>
            <div className="flex items-center gap-2 mt-1">
              <StatusChip variant="neutral">{health.kind}</StatusChip>
              {health.drain.draining && (
                <StatusChip variant="warn">
                  draining ({health.drain.in_flight})
                </StatusChip>
              )}
            </div>
          </div>
          <ErrorMiniChart upstreamName={health.name} />
        </div>

        <div className="space-y-3">
          <div className="flex justify-between items-center text-sm">
            <span className="text-graphite-400">Breaker</span>
            <BreakerChip
              state={health.breaker.state}
              failureCount={health.breaker.failure_count}
              halfOpenInFlight={health.breaker.half_open_in_flight}
              observed={health.breaker.observed}
            />
          </div>

          <div className="flex justify-between items-center text-sm">
            <span className="text-graphite-400">Bulkhead</span>
            <BulkheadGauge
              available={health.bulkhead.available_permits}
              max={health.bulkhead.max_conns}
              observed={health.bulkhead.observed}
            />
          </div>

          <div className="flex justify-between items-center text-sm pt-2 border-t border-gray-100">
            <span className="text-graphite-400">Last Probe</span>
            <span className="text-graphite-50">
              {health.last_probe_unix_secs
                ? formatRelativeTime(health.last_probe_unix_secs)
                : 'never'}
            </span>
          </div>

          <div className="flex justify-between items-center text-sm">
            <span className="text-graphite-400">Recent Errors</span>
            <span
              className={`font-mono ${health.error_count_recent > 0 ? 'text-red-600 font-semibold' : 'text-graphite-50'}`}
            >
              {health.error_count_recent}
            </span>
          </div>
        </div>
      </div>
    </Card>
  );
}
