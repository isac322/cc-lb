import { PluginStatusEntry } from '../../lib/api';
import { PluginCard } from './PluginCard';

interface PluginSectionProps {
  slot: string;
  plugins: PluginStatusEntry[];
}

export function PluginSection({ slot, plugins }: PluginSectionProps) {
  if (plugins.length === 0) return null;

  return (
    <div className="mb-8">
      <h2 className="text-lg font-semibold text-graphite-50 mb-4 capitalize">{slot} Plugins</h2>
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
        {plugins.map(plugin => (
          <PluginCard key={plugin.name} plugin={plugin} />
        ))}
      </div>
    </div>
  );
}
