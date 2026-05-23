import { FormField } from '../primitives/FormField';

interface LogFilterBarProps {
  filters: {
    principal_id: string;
    model: string;
    upstream: string;
    status_class: string;
  };
  onChange: (key: string, value: string) => void;
}

export function LogFilterBar({ filters, onChange }: LogFilterBarProps) {
  return (
    <div className="flex flex-wrap gap-4 items-end mb-6">
      <div className="w-48">
        <FormField label="Principal ID">
          <input
            type="text"
            className="w-full bg-graphite-900 border border-graphite-700 rounded px-3 py-1.5 text-sm text-graphite-100 focus:outline-none focus:border-cyan-500"
            value={filters.principal_id}
            onChange={(e) => onChange('principal_id', e.target.value)}
            placeholder="Any"
          />
        </FormField>
      </div>
      <div className="w-48">
        <FormField label="Model">
          <input
            type="text"
            className="w-full bg-graphite-900 border border-graphite-700 rounded px-3 py-1.5 text-sm text-graphite-100 focus:outline-none focus:border-cyan-500"
            value={filters.model}
            onChange={(e) => onChange('model', e.target.value)}
            placeholder="Any"
          />
        </FormField>
      </div>
      <div className="w-48">
        <FormField label="Upstream">
          <select
            className="w-full bg-graphite-900 border border-graphite-700 rounded px-3 py-1.5 text-sm text-graphite-100 focus:outline-none focus:border-cyan-500"
            value={filters.upstream}
            onChange={(e) => onChange('upstream', e.target.value)}
          >
            <option value="">Any</option>
            <option value="anthropic_direct">Anthropic Direct</option>
            <option value="bedrock_runtime">Bedrock Runtime</option>
            <option value="bedrock_mantle">Bedrock Mantle</option>
            <option value="vertex">Vertex</option>
            <option value="custom_anthropic_spec">Custom Anthropic Spec</option>
          </select>
        </FormField>
      </div>
      <div className="w-32">
        <FormField label="Status">
          <select
            className="w-full bg-graphite-900 border border-graphite-700 rounded px-3 py-1.5 text-sm text-graphite-100 focus:outline-none focus:border-cyan-500"
            value={filters.status_class}
            onChange={(e) => onChange('status_class', e.target.value)}
          >
            <option value="">Any</option>
            <option value="2xx">2xx</option>
            <option value="3xx">3xx</option>
            <option value="4xx">4xx</option>
            <option value="5xx">5xx</option>
          </select>
        </FormField>
      </div>
    </div>
  );
}
