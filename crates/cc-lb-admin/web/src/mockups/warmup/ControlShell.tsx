import type { ReactNode } from 'react';
import { Card, CardBody, CardHeader } from '../../components/ui/primitives';
import { WarmupHelpIcon } from './HelpVariants';
import {
  FireNowButton,
  HeaderTitle,
  HistoryButton,
  MockSwitch,
} from './shared';
import type { WarmupMockFixture } from './types';

export function ControlShell({
  fixture,
  children,
}: {
  readonly fixture: WarmupMockFixture;
  readonly children: ReactNode;
}) {
  return (
    <Card className="w-full h-full flex flex-col">
      <CardHeader
        title={<HeaderTitle fixture={fixture} titleHelp={<WarmupHelpIcon />} />}
        subtitle="Starts the next 5h window during idle gaps."
        action={
          <div className="flex items-center">
            <MockSwitch checked={fixture.upstream.warmup_enabled} />
          </div>
        }
        align="center"
      />
      <CardBody className="space-y-4 flex-1 flex flex-col">
        <div className="flex-1">{children}</div>
        <div className="grid grid-cols-2 gap-2 border-t border-subtle pt-3 mt-auto">
          <FireNowButton fullWidth />
          <HistoryButton />
        </div>
      </CardBody>
    </Card>
  );
}
