import { Button } from '../primitives/Button';

interface PauseResumeControlProps {
  paused: boolean;
  bufferedCount: number;
  onToggle: () => void;
  onFlush: () => void;
}

export function PauseResumeControl({ paused, bufferedCount, onToggle, onFlush }: PauseResumeControlProps) {
  return (
    <div className="flex items-center gap-3">
      <Button variant={paused ? 'primary' : 'secondary'} onClick={onToggle}>
        {paused ? 'Resume' : 'Pause'}
      </Button>
      {paused && bufferedCount > 0 && (
        <div className="flex items-center gap-2">
          <span className="text-xs font-medium text-amber-400 bg-amber-500/10 px-2 py-1 rounded border border-amber-500/20">
            Paused: {bufferedCount}
          </span>
          <Button variant="ghost" onClick={onFlush} className="text-xs py-1 px-2 h-auto">
            Flush
          </Button>
        </div>
      )}
    </div>
  );
}
