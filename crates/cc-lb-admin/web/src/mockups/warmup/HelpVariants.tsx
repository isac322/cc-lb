import { HelpCircle } from 'lucide-react';
import { Hint } from '../../components/ui/primitives';

function HelpIcon({
  label,
  text,
}: {
  readonly label: string;
  readonly text: string;
}) {
  return (
    <Hint
      label={
        <span className="block max-w-72 whitespace-normal leading-5">
          {text}
        </span>
      }
      side="top"
    >
      <span
        aria-label={label}
        className="inline-flex h-4 w-4 cursor-help items-center justify-center text-text-faint transition-colors hover:text-text"
      >
        <HelpCircle className="h-3.5 w-3.5" />
      </span>
    </Hint>
  );
}

export function WarmupHelpIcon() {
  return (
    <HelpIcon
      label="Warm-up help"
      text="Warm-up sends a tiny background Anthropic request when idle time would otherwise stall the next 5h window. Returning users land in an active window instead of starting one late."
    />
  );
}

export function ShapePluginHelpIcon() {
  return (
    <HelpIcon
      label="Shape plugin help"
      text="Warm-up is also an Anthropic request. Shape plugin applies the same request transform used for proxied traffic to that small warm-up prompt."
    />
  );
}
