import { Card } from './Card';

export function MetricCard({ title, value, unit, trend }: { title: string; value: React.ReactNode; unit?: string; trend?: React.ReactNode }) {
  return (
    <Card className="p-4">
      <h2 className="text-graphite-400 text-xs font-medium uppercase tracking-wider mb-2">{title}</h2>
      <div className="flex items-baseline gap-2">
        <span className="text-3xl font-mono text-graphite-50">{value}</span>
        {unit && <span className="text-xs text-graphite-500">{unit}</span>}
      </div>
      {trend && <div className="mt-2 text-sm">{trend}</div>}
    </Card>
  );
}
