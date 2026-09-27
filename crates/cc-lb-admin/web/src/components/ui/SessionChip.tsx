import { getSessionColor } from '../../lib/colors';

const DASH = '—';

export function SessionChip({ sessionId }: { sessionId: string | null }) {
  if (!sessionId) {
    return <span className="text-text-faint">{DASH}</span>;
  }
  const color = getSessionColor(sessionId);
  const short =
    sessionId.length <= 8
      ? sessionId
      : `${sessionId.slice(0, 3)}…${sessionId.slice(-4)}`;
  return (
    <span
      title={sessionId}
      className="inline-flex h-5 items-center gap-1 px-1.5 rounded-sm font-mono text-data tabular-nums leading-none"
      style={{
        backgroundColor: color.bg,
        // The shared hue is tuned for dark surfaces; light theme needs a
        // darker shade of the same hue to stay readable.
        color: `light-dark(hsl(${color.hue} 60% 32%), ${color.fg})`,
      }}
    >
      <span
        className="h-1.5 w-1.5 rounded-full shrink-0"
        style={{ backgroundColor: color.fg }}
      />
      {short}
    </span>
  );
}
