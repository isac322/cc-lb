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
      className="inline-flex h-5 items-center gap-1 px-1.5 rounded-sm font-mono text-data tabular-nums leading-none text-text"
      // The hue only identifies the session (tint and dot); the id stays in
      // ink so it reads at full contrast in both themes.
      style={{ backgroundColor: color.bg }}
    >
      <span
        className="h-1.5 w-1.5 rounded-full shrink-0"
        style={{ backgroundColor: color.mark }}
      />
      {short}
    </span>
  );
}
