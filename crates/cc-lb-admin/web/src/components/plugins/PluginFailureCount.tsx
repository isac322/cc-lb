import { CircleAlert } from 'lucide-react';
import { Tooltip } from '../primitives/Tooltip';

interface PluginFailureCountProps {
  count: number;
  lastError: string | null;
}

export function PluginFailureCount({
  count,
  lastError,
}: PluginFailureCountProps) {
  if (count === 0) {
    return (
      <div className="flex flex-col items-end">
        <span className="text-2xl font-mono font-semibold text-graphite-50">
          0
        </span>
        <span className="text-xs text-graphite-400">failures</span>
      </div>
    );
  }

  const content = (
    <div className="flex flex-col items-end">
      <div className="flex items-center gap-1 text-red-600">
        <CircleAlert className="w-4 h-4" />
        <span className="text-2xl font-mono font-semibold">{count}</span>
      </div>
      <span className="text-xs text-red-500">failures</span>
    </div>
  );

  if (lastError) {
    return (
      <Tooltip content={lastError}>
        <div className="cursor-help">{content}</div>
      </Tooltip>
    );
  }

  return content;
}
