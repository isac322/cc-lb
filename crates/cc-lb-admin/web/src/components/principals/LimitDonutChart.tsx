import { Cell, Pie, PieChart, ResponsiveContainer } from 'recharts';
import type { PrincipalLimitSnapshot } from '../../lib/api';
import { formatNumber } from '../../lib/format';
import { Card } from '../primitives/Card';

interface LimitDonutChartProps {
  snapshot: PrincipalLimitSnapshot;
}

function formatRelativeTime(resetDateStr: string): string {
  const resetTime = new Date(resetDateStr).getTime();
  const now = Date.now();
  const diffMs = resetTime - now;

  if (diffMs <= 0) return 'resetting soon';

  const diffMins = Math.floor(diffMs / 60000);
  if (diffMins < 60) return `in ${diffMins}m`;

  const diffHours = Math.floor(diffMins / 60);
  if (diffHours < 24) return `in ${diffHours}h ${diffMins % 60}m`;

  const diffDays = Math.floor(diffHours / 24);
  return `in ${diffDays}d ${diffHours % 24}h`;
}

export function LimitDonutChart({ snapshot }: LimitDonutChartProps) {
  if (
    !snapshot.observed ||
    (snapshot.limit === null && snapshot.remaining === null)
  ) {
    return (
      <Card className="p-4 flex flex-col items-center justify-center h-48 bg-graphite-900/50 border-dashed">
        <div className="text-graphite-500 text-sm text-center">
          No {snapshot.kind} data observed
        </div>
      </Card>
    );
  }

  const hasLimit = snapshot.limit !== null;
  const remaining = snapshot.remaining ?? 0;
  const limit = snapshot.limit ?? 0;
  const used = hasLimit ? Math.max(0, limit - remaining) : 0;

  // Colors: remaining = cyan-500, used = graphite-700
  const COLORS = ['#06b6d4', '#3f3f46'];

  const data = hasLimit
    ? [
        { name: 'Remaining', value: remaining },
        { name: 'Used', value: used },
      ]
    : [
        { name: 'Remaining', value: 1 }, // Dummy value for full ring
      ];

  const isExhausted = hasLimit && remaining <= 0;
  const ringColor = isExhausted ? '#ef4444' : '#06b6d4'; // red-500 if exhausted

  return (
    <Card className="p-4 flex flex-col items-center h-48 relative">
      <div className="text-xs font-medium text-graphite-400 uppercase tracking-wider mb-2 w-full text-center">
        {snapshot.kind.replace('_', ' ')}
      </div>

      <div className="flex-1 w-full relative">
        <ResponsiveContainer width="100%" height="100%">
          <PieChart>
            <Pie
              data={data}
              cx="50%"
              cy="50%"
              innerRadius="70%"
              outerRadius="90%"
              startAngle={90}
              endAngle={-270}
              dataKey="value"
              stroke="none"
              isAnimationActive={false}
            >
              {hasLimit ? (
                data.map((_entry, index) => (
                  <Cell
                    key={`cell-${index}`}
                    fill={
                      isExhausted ? '#ef4444' : COLORS[index % COLORS.length]
                    }
                  />
                ))
              ) : (
                <Cell fill={ringColor} />
              )}
            </Pie>
          </PieChart>
        </ResponsiveContainer>

        <div className="absolute inset-0 flex flex-col items-center justify-center pointer-events-none">
          {hasLimit ? (
            <>
              <div
                className={`text-lg font-mono font-bold ${isExhausted ? 'text-red-400' : 'text-graphite-50'}`}
              >
                {formatNumber(remaining)}
              </div>
              <div className="text-[10px] text-graphite-500 font-mono">
                / {formatNumber(limit)}
              </div>
            </>
          ) : (
            <>
              <div className="text-lg font-mono font-bold text-graphite-50">
                {formatNumber(remaining)}
              </div>
              <div className="text-[10px] text-graphite-500 font-mono">
                remaining
              </div>
            </>
          )}
        </div>
      </div>

      {snapshot.reset && (
        <div className="text-[10px] text-graphite-500 mt-2">
          {formatRelativeTime(snapshot.reset)}
        </div>
      )}
    </Card>
  );
}
