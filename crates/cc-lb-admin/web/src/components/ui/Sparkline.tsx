import { cx } from './primitives';

export interface SparkSegment {
  value: number;
  color: string;
}

export function Sparkline({ segments }: { segments: SparkSegment[] }) {
  const total = segments.reduce((a, s) => a + Math.max(0, s.value), 0);
  if (total <= 0) {
    return <div className="mt-1 h-1 rounded-full bg-overlay-1" />;
  }
  return (
    <div className="mt-1 h-1 w-full rounded-full overflow-hidden flex bg-overlay-1">
      {segments.map((s, i) => {
        if (s.value <= 0) return null;
        const isTailwindBg = /(^|\s)bg-/.test(s.color);
        return (
          <span
            key={i}
            className={cx('h-full', isTailwindBg ? s.color : undefined)}
            style={{
              width: `${(s.value / total) * 100}%`,
              backgroundColor: isTailwindBg ? undefined : s.color,
            }}
          />
        );
      })}
    </div>
  );
}
