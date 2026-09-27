import { getSessionColor } from '../../lib/colors';

const DASH = '—';

/**
 * A session id as a chip in the session's own hue (tint, dot and id text), so
 * rows of one session group at a glance and different sessions differ. The
 * full id stays in the title.
 */
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
      style={{ backgroundColor: color.bg, color: color.text }}
    >
      <span
        aria-hidden="true"
        className="h-1.5 w-1.5 rounded-full shrink-0"
        style={{ backgroundColor: color.mark }}
      />
      {short}
    </span>
  );
}
