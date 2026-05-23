import { SchemaForm } from './SchemaForm';
import { Button } from '../primitives/Button';

interface UpstreamsEditorProps {
  schema: Record<string, unknown>;
  rootSchema: Record<string, unknown>;
  value: Record<string, unknown>[];
  onChange: (value: Record<string, unknown>[]) => void;
  error?: string;
}

export function UpstreamsEditor({ schema, rootSchema, value = [], onChange, error }: UpstreamsEditorProps) {
  const itemSchema = schema.items as Record<string, unknown>;

  const handleAdd = () => {
    onChange([...value, {}]);
  };

  const handleRemove = (index: number) => {
    const newVal = [...value];
    newVal.splice(index, 1);
    onChange(newVal);
  };

  const handleChange = (index: number, itemVal: unknown) => {
    const newVal = [...value];
    newVal[index] = itemVal as Record<string, unknown>;
    onChange(newVal);
  };

  return (
    <div className="space-y-4">
      {value.map((item, index) => (
        <div key={index} className="p-4 border border-graphite-800 rounded-md bg-graphite-900/50 relative">
          <div className="absolute top-4 right-4">
            <button
              type="button"
              onClick={() => handleRemove(index)}
              className="text-graphite-400 hover:text-red-400"
            >
              <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M3 6h18"/><path d="M19 6v14c0 1-1 2-2 2H7c-1 0-2-1-2-2V6"/><path d="M8 6V4c0-1 1-2 2-2h4c1 0 2 1 2 2v2"/></svg>
            </button>
          </div>
          <h4 className="text-sm font-medium text-graphite-200 mb-4">Upstream {index + 1}</h4>
          <SchemaForm
            schema={itemSchema}
            rootSchema={rootSchema}
            value={item}
            onChange={(newVal) => handleChange(index, newVal)}
            path={`upstreams[${index}]`}
            error={error?.startsWith(`upstreams[${index}]`) ? error : undefined}
          />
        </div>
      ))}
      <Button type="button" variant="secondary" onClick={handleAdd} className="w-full justify-center">
        <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" className="mr-2"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg>
        Add Upstream
      </Button>
    </div>
  );
}
