import * as Popover from '@radix-ui/react-popover';
import { formatDistanceToNow } from 'date-fns';
import { AlertTriangle, CheckCircle2, PowerOff } from 'lucide-react';

export interface StatusBadgeProps {
  status: 'active' | 'disabled' | 'error';
  lastApplyError?: string | null;
  lastApplyAt?: string | null;
  label?: string;
}

export function StatusBadge({
  status,
  lastApplyError,
  lastApplyAt,
  label,
}: StatusBadgeProps) {
  const displayLabel =
    label || status.charAt(0).toUpperCase() + status.slice(1);

  if (status === 'active') {
    return (
      <span className="inline-flex items-center gap-1 px-2 py-1 rounded-full text-xs font-medium bg-green-400/10 text-green-400">
        <CheckCircle2 className="w-3 h-3" />
        {displayLabel}
      </span>
    );
  }

  if (status === 'disabled') {
    return (
      <span className="inline-flex items-center gap-1 px-2 py-1 rounded-full text-xs font-medium bg-graphite-600/20 text-graphite-400">
        <PowerOff className="w-3 h-3" />
        {displayLabel}
      </span>
    );
  }

  // Error state
  const badge = (
    <span className="inline-flex items-center gap-1 px-2 py-1 rounded-full text-xs font-medium bg-red-400/10 text-red-400 cursor-pointer hover:bg-red-400/20 transition-colors">
      <AlertTriangle className="w-3 h-3" />
      {displayLabel}
    </span>
  );

  if (!lastApplyError) {
    return badge;
  }

  const truncatedError =
    lastApplyError.length > 500
      ? lastApplyError.slice(0, 500) + '...'
      : lastApplyError;

  const relativeTime = lastApplyAt
    ? formatDistanceToNow(new Date(lastApplyAt), { addSuffix: true })
    : 'unknown time';

  return (
    <Popover.Root>
      <Popover.Trigger asChild>{badge}</Popover.Trigger>
      <Popover.Portal>
        <Popover.Content
          className="z-50 w-80 rounded-md border border-graphite-700 bg-graphite-900 p-4 shadow-md outline-none data-[state=open]:animate-in data-[state=closed]:animate-out data-[state=closed]:fade-out-0 data-[state=open]:fade-in-0 data-[state=closed]:zoom-out-95 data-[state=open]:zoom-in-95 data-[side=bottom]:slide-in-from-top-2 data-[side=left]:slide-in-from-right-2 data-[side=right]:slide-in-from-left-2 data-[side=top]:slide-in-from-bottom-2"
          sideOffset={5}
        >
          <div className="space-y-2">
            <h4 className="font-medium leading-none text-red-400">
              Apply Error
            </h4>
            <p className="text-xs text-graphite-400">Occurred {relativeTime}</p>
            <div className="mt-2 max-h-40 overflow-y-auto rounded bg-graphite-950 p-2 text-xs text-graphite-300 font-mono whitespace-pre-wrap">
              {truncatedError}
            </div>
          </div>
          <Popover.Arrow className="fill-graphite-700" />
        </Popover.Content>
      </Popover.Portal>
    </Popover.Root>
  );
}
