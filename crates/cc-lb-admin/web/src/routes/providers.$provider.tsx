import { createFileRoute, Navigate } from '@tanstack/react-router';
import { defaultDesign } from '../designs/registry';
import { usePreviewInstallation } from '../lib/previewControls';
import { PROVIDER_FAMILIES, type ProviderFamily } from '../lib/providers';
import { ProviderOverviewPage } from '../overview';

export const Route = createFileRoute('/providers/$provider')({
  component: ProviderRoute,
});

function ProviderRoute() {
  const installation = usePreviewInstallation();
  const { provider: rawProvider } = Route.useParams();
  const provider = PROVIDER_FAMILIES.find(
    (candidate): candidate is ProviderFamily => candidate === rawProvider,
  );
  if (installation.mode === 'unknown') {
    const DefaultOverviewPage = defaultDesign.OverviewPage;
    return <DefaultOverviewPage />;
  }
  if (provider === undefined || !installation.providers.includes(provider)) {
    return <Navigate to="/" />;
  }
  return <ProviderOverviewPage provider={provider} providerHeading />;
}
