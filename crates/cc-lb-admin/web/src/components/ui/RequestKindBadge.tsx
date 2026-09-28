import { categoricalColor } from '../../lib/colors';
import { requestKindBadgeText, requestKindTone } from '../../lib/requestKind';
import { cx } from './primitives';

const DASH = '—';

/** `Badge` geometry (20px, 12/500, `px-1.5`, `rounded-sm`). */
const BADGE =
  'inline-flex min-h-5 shrink-0 items-center rounded-sm px-1.5 text-caption font-medium';

/**
 * The request's kind as a short badge in the kind's fixed color. `main`, the
 * bulk of traffic, is the quiet neutral badge; `unknown` is an outlined
 * neutral badge; every other kind gets its own hue (custom kinds a hashed
 * one). A missing kind is a faint dash.
 */
export function RequestKindBadge({
  requestKind,
}: {
  requestKind: string | null | undefined;
}) {
  const text = requestKindBadgeText(requestKind);
  if (text == null) return <span className="text-text-faint">{DASH}</span>;
  const tone = requestKindTone(requestKind!.trim());
  if (tone.kind !== 'hue') {
    return (
      <span
        data-request-kind=""
        className={cx(
          BADGE,
          'text-text-muted',
          tone.kind === 'main'
            ? 'bg-overlay-5'
            : 'border border-dashed border-border-strong',
        )}
      >
        {text}
      </span>
    );
  }
  const color = categoricalColor(tone.hue);
  return (
    <span
      data-request-kind=""
      className={BADGE}
      style={{ backgroundColor: color.bg, color: color.text }}
    >
      {text}
    </span>
  );
}
