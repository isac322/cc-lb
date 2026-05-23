import { SchemaForm } from './SchemaForm';
import { Card } from '../primitives/Card';

interface PrincipalsEditorProps {
  schema: Record<string, unknown>;
  rootSchema: Record<string, unknown>;
  value: Record<string, unknown>;
  onChange: (value: Record<string, unknown>) => void;
  error?: string;
}

export function PrincipalsEditor({ schema, rootSchema, value = {}, onChange, error }: PrincipalsEditorProps) {
  const itemSchema = schema.additionalProperties;

  return (
    <div className="space-y-4">
      <div className="bg-blue-900/20 border border-blue-800 rounded-md p-4 mb-4">
        <p className="text-sm text-blue-200">
          Principals are managed via the <a href="/management" className="text-blue-400 hover:underline">Management</a> page.
          Changes made here will be saved to the draft, but it is recommended to use the dedicated UI.
        </p>
      </div>
      {Object.entries(value).map(([id, item]) => (
        <Card key={id} className="p-4 bg-graphite-900/50">
          <h4 className="text-sm font-medium text-graphite-200 mb-4">Principal: {id}</h4>
          <SchemaForm
            schema={itemSchema as Record<string, unknown>}
            rootSchema={rootSchema}
            value={item as Record<string, unknown>}
            onChange={(newVal) => onChange({ ...value, [id]: newVal })}
            path={`principals.${id}`}
            error={error?.startsWith(`principals.${id}`) ? error : undefined}
          />
        </Card>
      ))}
    </div>
  );
}
