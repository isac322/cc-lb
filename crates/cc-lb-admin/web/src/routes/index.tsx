import { createFileRoute } from '@tanstack/react-router';
import { defaultDesign } from '../designs/registry';
import { usePreviewInstallation } from '../lib/previewControls';
import { MultiProviderOverview, ProviderOverviewPage } from '../overview';

export const Route = createFileRoute('/')({
  component: OverviewRoute,
});

function OverviewRoute() {
  const installation = usePreviewInstallation();
  const provider = installation.providers.at(0);
  const DefaultOverviewPage = defaultDesign.OverviewPage;

  if (installation.mode === 'multi') {
    return <MultiProviderOverview />;
  }
  if (installation.mode === 'single' && provider !== undefined) {
    return <ProviderOverviewPage provider={provider} />;
  }
  return <DefaultOverviewPage />;
}
