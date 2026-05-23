import { Area, AreaChart, ResponsiveContainer } from 'recharts';

interface SparklineProps<T> {
  data: T[];
  dataKey: string;
  color: string;
}

export function Sparkline<T>({ data, dataKey, color }: SparklineProps<T>) {
  return (
    <div className="absolute inset-0 opacity-20 pointer-events-none">
      <ResponsiveContainer width="100%" height="100%">
        <AreaChart
          data={data}
          margin={{ top: 5, right: 0, left: 0, bottom: 0 }}
        >
          <Area
            type="monotone"
            dataKey={dataKey}
            stroke={color}
            fill={color}
            strokeWidth={2}
            isAnimationActive={false}
          />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
}
