import { useDashboardConnection } from '../../lib/api';

export function ConnectionStatus() {
  const state = useDashboardConnection();

  if (state === 'live') {
    return (
      <div className="flex items-center gap-2">
        <span className="w-2 h-2 rounded-full bg-[var(--color-status-live)] animate-pulse-live" />
        <span className="font-mono text-[10px] text-graphite-400 uppercase tracking-widest">
          LIVE
        </span>
      </div>
    );
  }
  if (state === 'reconnecting') {
    return (
      <div className="flex items-center gap-2">
        <span className="w-2 h-2 rounded-full bg-amber-500" />
        <span className="font-mono text-[10px] text-amber-500 uppercase tracking-widest">
          RECONNECTING
        </span>
      </div>
    );
  }
  if (state === 'auth_required') {
    return (
      <div className="flex items-center gap-2">
        <span className="w-2 h-2 rounded-full bg-red-500" />
        <span className="font-mono text-[10px] text-red-500 uppercase tracking-widest">
          AUTH REQUIRED
        </span>
      </div>
    );
  }
  return (
    <div className="flex items-center gap-2">
      <span className="w-2 h-2 rounded-full bg-graphite-500" />
      <span className="font-mono text-[10px] text-graphite-500 uppercase tracking-widest">
        DISCONNECTED
      </span>
    </div>
  );
}
