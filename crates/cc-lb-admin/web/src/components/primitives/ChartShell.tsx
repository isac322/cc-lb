import { Card } from './Card';
import { EmptyState } from './EmptyState';
import { LoadingState } from './LoadingState';

export function ChartShell({
  title,
  loading,
  empty,
  children,
}: {
  title: string;
  loading?: boolean;
  empty?: boolean;
  children: React.ReactNode;
}) {
  return (
    <Card className="p-4 flex flex-col h-full min-h-[300px]">
      <h3 className="text-sm font-medium text-graphite-200 mb-4">{title}</h3>
      <div className="flex-1 relative">
        {loading ? (
          <div className="absolute inset-0 flex items-center justify-center bg-graphite-850/50 z-10">
            <LoadingState />
          </div>
        ) : empty ? (
          <div className="absolute inset-0 flex items-center justify-center">
            <EmptyState message="No data for this period" />
          </div>
        ) : null}
        <div className="absolute inset-0">{children}</div>
      </div>
    </Card>
  );
}
