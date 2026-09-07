import { createFileRoute } from '@tanstack/react-router';
import { PluginsPage } from '../components/plugins/PluginsPage';

export const Route = createFileRoute('/plugins')({
  component: PluginsPage,
  validateSearch: (search: Record<string, unknown>) => {
    return {
      plugin: typeof search.plugin === 'string' ? search.plugin : undefined,
      action: search.action === 'upload' ? ('upload' as const) : undefined,
    };
  },
});
