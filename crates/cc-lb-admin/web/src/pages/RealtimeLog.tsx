import { useEffect, useMemo } from 'react';
import { useSearchParams } from 'react-router';
import { ConnectionStatusBadge } from '../components/log/ConnectionStatusBadge';
import { LogFilterBar } from '../components/log/LogFilterBar';
import { LogTable } from '../components/log/LogTable';
import { PauseResumeControl } from '../components/log/PauseResumeControl';
import { Button } from '../components/primitives/Button';
import { Card } from '../components/primitives/Card';
import { EmptyState } from '../components/primitives/EmptyState';
import { ErrorState } from '../components/primitives/ErrorState';
import { useEventStream } from '../lib/hooks/useEventStream';
import { useRecentEvents } from '../lib/hooks/useRecentEvents';

export default function RealtimeLog() {
  const [searchParams, setSearchParams] = useSearchParams();
  const mock = searchParams.get('mock') === '1';

  const filters = useMemo(
    () => ({
      principal_id: searchParams.get('principal_id') || '',
      model: searchParams.get('model') || '',
      upstream: searchParams.get('upstream') || '',
      status_class: searchParams.get('status_class') || '',
    }),
    [searchParams],
  );

  const handleFilterChange = (key: string, value: string) => {
    const next = new URLSearchParams(searchParams);
    if (value) {
      next.set(key, value);
    } else {
      next.delete(key);
    }
    setSearchParams(next);
  };

  const {
    events: recentEvents,
    loading: recentLoading,
    error: recentError,
  } = useRecentEvents(filters, mock);
  const {
    events,
    status,
    paused,
    setPaused,
    bufferedCount,
    flushBuffer,
    lastError,
    clearEvents,
  } = useEventStream(filters, mock, recentEvents);

  // Sync paused state with URL
  useEffect(() => {
    const isPaused = searchParams.get('paused') === '1';
    if (isPaused !== paused) {
      setPaused(isPaused);
    }
  }, [searchParams, paused, setPaused]);

  const handleTogglePause = () => {
    const next = new URLSearchParams(searchParams);
    if (!paused) {
      next.set('paused', '1');
    } else {
      next.delete('paused');
    }
    setSearchParams(next);
  };

  return (
    <div className="space-y-6 h-[calc(100vh-8rem)] flex flex-col">
      <div className="shrink-0">
        <h2 className="text-lg font-semibold text-graphite-50 mb-4">
          Realtime Log
        </h2>
        <LogFilterBar filters={filters} onChange={handleFilterChange} />

        <div className="flex items-center justify-between mb-4">
          <div className="flex items-center gap-4">
            <PauseResumeControl
              paused={paused}
              bufferedCount={bufferedCount}
              onToggle={handleTogglePause}
              onFlush={flushBuffer}
            />
            <ConnectionStatusBadge status={status} />
          </div>
          <Button variant="ghost" onClick={clearEvents}>
            Clear
          </Button>
        </div>
      </div>

      {(lastError || recentError) && (
        <div className="shrink-0 mb-4">
          <ErrorState
            title="Connection Error"
            message={lastError?.message || recentError?.message}
          />
        </div>
      )}

      <Card className="flex-1 overflow-hidden flex flex-col">
        {events.length === 0 && !recentLoading ? (
          <div className="flex-1 flex items-center justify-center">
            <EmptyState
              title="Waiting for traffic..."
              message="No events match the current filters."
            />
          </div>
        ) : (
          <LogTable events={events} />
        )}
      </Card>
    </div>
  );
}
