import { Inbox } from 'lucide-react';

export function EmptyState({
  title = 'No data',
  message,
  icon: Icon = Inbox,
  action,
}: {
  title?: string;
  message?: string;
  icon?: React.ElementType;
  action?: React.ReactNode;
}) {
  return (
    <div className="py-12 flex flex-col items-center gap-3 text-graphite-400 text-center">
      <Icon className="w-8 h-8 text-graphite-500 stroke-[1.5]" />
      <div className="flex flex-col items-center gap-1">
        <h3 className="text-sm font-medium text-graphite-200">{title}</h3>
        {message && <p className="text-xs text-graphite-400">{message}</p>}
      </div>
      {action && <div className="mt-2">{action}</div>}
    </div>
  );
}
