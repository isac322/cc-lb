import { useState, useRef, useEffect, useMemo } from 'react';
import { RequestEvent } from '../../lib/api';
import { StatusChip } from '../primitives/StatusChip';
import { LogRowDetail } from './LogRowDetail';

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

    const resizeObserver = new ResizeObserver(entries => {
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
  const startIndex = Math.max(0, Math.floor(scrollTop / ROW_HEIGHT) - BUFFER_ROWS);
  const endIndex = Math.min(events.length, Math.ceil((scrollTop + clientHeight) / ROW_HEIGHT) + BUFFER_ROWS);

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
        <div className="w-40 shrink-0">Request ID</div>
        <div className="w-24 shrink-0">Principal</div>
        <div className="w-32 shrink-0">Model</div>
        <div className="w-32 shrink-0">Upstream</div>
        <div className="w-20 shrink-0">Status</div>
        <div className="w-20 shrink-0 text-right">Duration</div>
        <div className="w-24 shrink-0 text-right">Tokens (In/Out)</div>
      </div>
      <div 
        ref={containerRef} 
        className="flex-1 overflow-auto relative"
      >
        <div style={{ height: totalHeight, position: 'relative' }}>
          {visibleEvents.map(({ event, index }) => (
            <div
              key={event.request_id}
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
                onClick={() => setExpandedId(expandedId === event.request_id ? null : event.request_id)}
              >
                <div className="w-24 shrink-0 text-xs text-graphite-300 truncate pr-2">
                  {new Date(event.ts).toLocaleTimeString()}
                </div>
                <div className="w-40 shrink-0 text-xs font-mono text-graphite-400 truncate pr-2">
                  {event.request_id}
                </div>
                <div className="w-24 shrink-0 text-xs text-graphite-300 truncate pr-2">
                  {event.principal_id || '-'}
                </div>
                <div className="w-32 shrink-0 text-xs text-graphite-300 truncate pr-2">
                  {event.model || '-'}
                </div>
                <div className="w-32 shrink-0 text-xs text-graphite-300 truncate pr-2">
                  {event.upstream || '-'}
                </div>
                <div className="w-20 shrink-0">
                  <StatusChip variant={event.status >= 500 ? 'danger' : event.status >= 400 ? 'warn' : 'ok'}>
                    {event.status}
                  </StatusChip>
                </div>
                <div className="w-20 shrink-0 text-xs text-graphite-300 text-right pr-2">
                  {event.duration_ms}ms
                </div>
                <div className="w-24 shrink-0 text-xs text-graphite-300 text-right">
                  {event.input_tokens !== undefined ? event.input_tokens : '-'}/{event.output_tokens !== undefined ? event.output_tokens : '-'}
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>
      {expandedId && (
        <div className="border-t border-graphite-700 bg-graphite-800 p-4 max-h-64 overflow-auto shrink-0">
          <LogRowDetail event={events.find(e => e.request_id === expandedId)!} onClose={() => setExpandedId(null)} />
        </div>
      )}
    </div>
  );
}
