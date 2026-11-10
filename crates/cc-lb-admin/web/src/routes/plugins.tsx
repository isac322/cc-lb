import { createFileRoute } from '@tanstack/react-router';
import { PluginsPage } from '../components/plugins/PluginsPage';
import { useDesign } from '../designs/registry';

export const Route = createFileRoute('/plugins')({
  component: PluginsRoute,
  validateSearch: (search: Record<string, unknown>) => {
    return {
      plugin: typeof search.plugin === 'string' ? search.plugin : undefined,
      action: search.action === 'upload' ? ('upload' as const) : undefined,
    };
  },
});

function PluginsRoute() {
  const { SystemWideNotice } = useDesign();
  return (
    <>
      {SystemWideNotice ? (
        <div className="px-4 pt-4 md:px-8 lg:px-10">
          <SystemWideNotice />
        </div>
      ) : null}
      <PluginsPage />
    </>
  );
}
