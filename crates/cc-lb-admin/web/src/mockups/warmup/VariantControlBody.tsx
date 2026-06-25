import { ShapePluginHelpIcon } from './HelpVariants';
import {
  FailureNotice,
  LastAttemptLine,
  NextRunValue,
  ShapePluginSelect,
} from './shared';
import type { WarmupMockFixture } from './types';

export function VariantControlBody({
  fixture,
}: {
  readonly fixture: WarmupMockFixture;
}) {
  return (
    <div className="space-y-4">
      <div className="grid grid-cols-1 gap-4 border-b border-subtle pb-3 sm:grid-cols-2">
        <div className="flex flex-col gap-1">
          <span className="text-[11px] uppercase tracking-wider text-text-faint">
            Next run
          </span>
          <NextRunValue fixture={fixture} />
        </div>
        <div className="flex flex-col gap-1">
          <span className="text-[11px] uppercase tracking-wider text-text-faint">
            Last run
          </span>
          <LastAttemptLine fixture={fixture} />
        </div>
      </div>
      <div className="flex items-center justify-between gap-3">
        <span className="inline-flex items-center gap-1.5 text-sm text-text-muted">
          Shape plugin
          <ShapePluginHelpIcon />
        </span>
        <ShapePluginSelect fixture={fixture} />
      </div>
      <FailureNotice fixture={fixture} />
    </div>
  );
}
