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
      className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded-sm border text-[10px] font-mono tabular-nums leading-none"
      style={{
        backgroundColor: color.bg,
        color: color.fg,
        borderColor: color.border,
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
