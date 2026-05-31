import { useEffect, useMemo, useRef, useState } from 'react';
import type { RequestEvent } from '../../lib/api';
import { formatNumber } from '../../lib/format';
import { StatusChip } from '../primitives/StatusChip';
import { LogRowDetail } from './LogRowDetail';

export function eventTimestampMs(event: RequestEvent): number {
  if (event.ts_ms && event.ts_ms > 0) return event.ts_ms;
  if (event.ts && event.ts > 0) return event.ts * 1000;
  return Date.now();
}

export function eventKey(event: RequestEvent, index: number): string {
  if (event.request_id) return event.request_id;
  return `${event.ts_ms ?? event.ts ?? 0}-${index}`;
}

interface LogTableProps {
  events: RequestEvent[];
}

const ROW_HEIGHT = 36;
const BUFFER_ROWS = 10;

export function LogTable({ events }: LogTableProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const [scrollTop, setScrollTop] = useState(0);
  const [clientHeight, setClientHeight] = useState(0);
  const [expandedId, setExpandedId] = useState<string | null>(null);

  useEffect(() => {
    const container = containerRef.current;
    if (!container) return;

    const handleScroll = () => {
      setScrollTop(container.scrollTop);
    };

    const resizeObserver = new ResizeObserver((entries) => {
      for (const entry of entries) {
        setClientHeight(entry.contentRect.height);
      }
    });

    container.addEventListener('scroll', handleScroll);
    resizeObserver.observe(container);
    setClientHeight(container.clientHeight);
    setScrollTop(container.scrollTop);

    return () => {
      container.removeEventListener('scroll', handleScroll);
      resizeObserver.disconnect();
    };
  }, []);

  const totalHeight = events.length * ROW_HEIGHT;
  const startIndex = Math.max(
    0,
    Math.floor(scrollTop / ROW_HEIGHT) - BUFFER_ROWS,
  );
  const endIndex = Math.min(
    events.length,
    Math.ceil((scrollTop + clientHeight) / ROW_HEIGHT) + BUFFER_ROWS,
  );

  const visibleEvents = useMemo(() => {
    return events.slice(startIndex, endIndex).map((event, i) => ({
      event,
      index: startIndex + i,
    }));
  }, [events, startIndex, endIndex]);

  return (
    <div className="flex flex-col h-full border border-graphite-800 rounded bg-graphite-900 overflow-hidden">
      <div className="flex items-center px-4 py-2 bg-graphite-800 border-b border-graphite-700 text-xs font-medium text-graphite-400 sticky top-0 z-10">
        <div className="w-24 shrink-0">Time</div>
        <div className="w-32 shrink-0">Request ID</div>
        <div className="w-32 shrink-0">Model</div>
        <div className="w-24 shrink-0">Upstream</div>
        <div className="w-16 shrink-0">Status</div>
        <div className="w-20 shrink-0 text-right">Duration</div>
        <div className="w-24 shrink-0 text-right">Tokens (In/Out)</div>
        <div className="w-24 shrink-0 text-right">Cache</div>
        <div className="w-24 shrink-0 text-right">Cost</div>
      </div>
      <div ref={containerRef} className="flex-1 overflow-auto relative">
        <div style={{ height: totalHeight, position: 'relative' }}>
          {visibleEvents.map(({ event, index }) => {
            const key = eventKey(event, index);
            return (
            <div
              key={key}
              style={{
                position: 'absolute',
                top: index * ROW_HEIGHT,
                height: ROW_HEIGHT,
                width: '100%',
              }}
              className="border-b border-graphite-800/50 hover:bg-graphite-800/50 transition-colors"
            >
              <div
                className="flex items-center px-4 h-full cursor-pointer"
                onClick={() =>
                  setExpandedId(expandedId === key ? null : key)
                }
              >
                <div className="w-24 shrink-0 text-xs text-graphite-300 truncate pr-2">
                  {new Date(eventTimestampMs(event)).toLocaleTimeString()}
                </div>
                <div className="w-32 shrink-0 text-xs font-mono text-graphite-400 truncate pr-2">
                  {event.request_id || '-'}
                </div>
                <div className="w-32 shrink-0 text-xs text-graphite-300 truncate pr-2">
                  {event.model || '-'}
                </div>
                <div className="w-24 shrink-0 text-xs text-graphite-300 truncate pr-2">
                  {event.upstream_name || event.upstream || '-'}
                </div>
                <div className="w-16 shrink-0">
                  <StatusChip
                    variant={
                      event.status >= 500
                        ? 'danger'
                        : event.status >= 400
                          ? 'warn'
                          : 'ok'
                    }
                  >
                    {event.status}
                  </StatusChip>
                </div>
                <div className="w-20 shrink-0 text-xs text-graphite-300 text-right pr-2">
                  {formatNumber(event.duration_ms)}ms
                </div>
                <div className="w-24 shrink-0 text-xs text-graphite-300 text-right pr-2">
                  {event.input_tokens !== undefined
                    ? formatNumber(event.input_tokens)
                    : '-'}
                  /
                  {event.output_tokens !== undefined
                    ? formatNumber(event.output_tokens)
                    : '-'}
                </div>
                <div className="w-24 shrink-0 text-xs text-right font-mono pr-2">
                  <CacheCell event={event} />
                </div>
                <div className="w-24 shrink-0 text-xs text-graphite-300 text-right font-mono">
                  {formatCostMicros(event.cost_usd_micros)}
                </div>
              </div>
            </div>
          );
          })}
        </div>
      </div>
      {expandedId &&
        (() => {
          const idx = events.findIndex((e, i) => eventKey(e, i) === expandedId);
          if (idx === -1) return null;
          return (
            <div className="border-t border-graphite-700 bg-graphite-800 p-4 overflow-auto shrink-0">
              <LogRowDetail
                event={events[idx]}
                onClose={() => setExpandedId(null)}
              />
            </div>
          );
        })()}
    </div>
  );
}

function CacheCell({ event }: { event: RequestEvent }) {
  const read = event.cache_read_input_tokens ?? 0;
  const create = event.cache_creation_input_tokens ?? 0;
  const input = event.input_tokens ?? 0;
  if (read > 0) {
    const denom = read + input + create;
    const pct = denom > 0 ? (read / denom) * 100 : 0;
    return (
      <span className="text-emerald-400" title={`read ${read} / input ${input} / create ${create}`}>
        HIT {pct.toFixed(0)}%
      </span>
    );
  }
  if (create > 0) {
    return (
      <span className="text-amber-400" title={`created ${create} cache tokens`}>
        +{formatNumber(create)}
      </span>
    );
  }
  return <span className="text-graphite-600">-</span>;
}

function formatCostMicros(micros: number | undefined): string {
  if (micros === undefined || micros === 0) return '-';
  const usd = micros / 1_000_000;
  if (usd >= 0.01) return `$${usd.toFixed(4)}`;
  if (usd >= 0.0001) return `$${usd.toFixed(6)}`;
  return `<$0.0001`;
}
