import { useState } from 'react';
import { useCreate, useUpdate } from '../lib/hooks/usePrincipals';
import {
  ConflictError,
  type Limit,
  type PrincipalKind,
  type PrincipalResponse,
} from '../lib/types/v1';
import { Button } from './primitives/Button';
import { FormField } from './primitives/FormField';
import { Modal } from './primitives/Modal';

export function PrincipalCreateDialog({
  principal,
  onClose,
  onSuccess,
}: {
  principal?: PrincipalResponse;
  onClose: () => void;
  onSuccess: () => void;
}) {
  const isEdit = !!principal;
  const create = useCreate();
  const update = useUpdate();

  const [name, setName] = useState(principal?.name || '');
  const [kind, setKind] = useState<PrincipalKind>(principal?.kind || 'machine');
  const [allowedModels, setAllowedModels] = useState<string>(
    principal?.allowed_models.join(', ') || '',
  );
  const [defaultLimits, setDefaultLimits] = useState<Limit[]>(
    principal?.default_limits || [],
  );
  const [revision, setRevision] = useState(principal?.revision || 0);
  const [isPending, setIsPending] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [conflictLatest, setConflictLatest] =
    useState<PrincipalResponse | null>(null);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setIsPending(true);
    setError(null);
    setConflictLatest(null);

    const models = allowedModels
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean);

    try {
      if (isEdit && principal) {
        await update(principal.id, revision, {
          name,
          allowed_models: models,
          default_limits: defaultLimits,
        });
      } else {
        await create({
          name,
          kind,
          allowed_models: models,
          default_limits: defaultLimits,
        });
      }
      onSuccess();
    } catch (err) {
      if (err instanceof ConflictError) {
        setConflictLatest(err.latest as PrincipalResponse);
      } else {
        setError(err instanceof Error ? err.message : String(err));
      }
    } finally {
      setIsPending(false);
    }
  };

  const handleLoadLatest = () => {
    if (conflictLatest) {
      setName(conflictLatest.name);
      setKind(conflictLatest.kind);
      setAllowedModels(conflictLatest.allowed_models.join(', '));
      setDefaultLimits(conflictLatest.default_limits);
      setRevision(conflictLatest.revision);
      setConflictLatest(null);
      setError(null);
    }
  };

  const addLimit = () => {
    setDefaultLimits([
      ...defaultLimits,
      { kind: 'requests', window: '1m', limit: 100 },
    ]);
  };

  const updateLimit = (
    index: number,
    field: keyof Limit,
    value: string | number,
  ) => {
    const newLimits = [...defaultLimits];
    newLimits[index] = { ...newLimits[index], [field]: value } as Limit;
    setDefaultLimits(newLimits);
  };

  const removeLimit = (index: number) => {
    setDefaultLimits(defaultLimits.filter((_, i) => i !== index));
  };

  return (
    <Modal
      isOpen
      onClose={onClose}
      title={isEdit ? 'Edit Principal' : 'New Principal'}
    >
      <form onSubmit={handleSubmit} className="space-y-4">
        {conflictLatest && (
          <div className="bg-yellow-900/50 border border-yellow-700 p-3 rounded flex items-center justify-between">
            <span className="text-yellow-200 text-sm">
              Conflict detected. Another user modified this principal.
            </span>
            <Button
              type="button"
              variant="secondary"
              onClick={handleLoadLatest}
            >
              Load latest
            </Button>
          </div>
        )}

        <FormField label="Name" error={undefined}>
          <input
            type="text"
            value={name}
            onChange={(e) => setName(e.target.value)}
            className="w-full bg-graphite-900 border border-graphite-800 rounded px-3 py-2 text-graphite-50 focus:outline-none focus:border-cyan-500"
            required
          />
        </FormField>

        <FormField label="Kind" error={undefined}>
          <select
            value={kind}
            onChange={(e) => setKind(e.target.value as PrincipalKind)}
            disabled={isEdit}
            className="w-full bg-graphite-900 border border-graphite-800 rounded px-3 py-2 text-graphite-50 focus:outline-none focus:border-cyan-500 disabled:opacity-50"
          >
            <option value="machine">Machine</option>
            <option value="human">Human</option>
            <option value="admin">Admin</option>
          </select>
        </FormField>

        <FormField label="Allowed Models (comma separated)" error={undefined}>
          <input
            type="text"
            value={allowedModels}
            onChange={(e) => setAllowedModels(e.target.value)}
            placeholder="e.g. claude-3-*, gpt-4"
            className="w-full bg-graphite-900 border border-graphite-800 rounded px-3 py-2 text-graphite-50 focus:outline-none focus:border-cyan-500"
          />
        </FormField>

        <details className="group border border-graphite-800 rounded bg-graphite-900">
          <summary className="px-3 py-2 text-sm font-medium text-graphite-300 cursor-pointer hover:text-graphite-100">
            Default Limits ({defaultLimits.length})
          </summary>
          <div className="p-3 space-y-3 border-t border-graphite-800">
            {defaultLimits.map((limit, i) => (
              <div key={i} className="flex items-center gap-2">
                <select
                  value={limit.kind}
                  onChange={(e) => updateLimit(i, 'kind', e.target.value)}
                  className="bg-graphite-850 border border-graphite-800 rounded px-2 py-1 text-sm text-graphite-50"
                >
                  <option value="requests">Requests</option>
                  <option value="tokens">Tokens</option>
                  <option value="input_tokens">Input Tokens</option>
                  <option value="output_tokens">Output Tokens</option>
                </select>
                <input
                  type="text"
                  value={limit.window}
                  onChange={(e) => updateLimit(i, 'window', e.target.value)}
                  placeholder="1m, 1h, 1d"
                  className="w-20 bg-graphite-850 border border-graphite-800 rounded px-2 py-1 text-sm text-graphite-50"
                />
                <input
                  type="number"
                  value={limit.limit}
                  onChange={(e) =>
                    updateLimit(i, 'limit', parseInt(e.target.value, 10))
                  }
                  className="w-24 bg-graphite-850 border border-graphite-800 rounded px-2 py-1 text-sm text-graphite-50"
                />
                <button
                  type="button"
                  onClick={() => removeLimit(i)}
                  className="text-red-400 hover:text-red-300 px-2"
                >
                  ×
                </button>
              </div>
            ))}
            <Button type="button" variant="secondary" onClick={addLimit}>
              + Add Limit
            </Button>
          </div>
        </details>

        {error && <div className="text-red-400 text-sm">{error}</div>}

        <div className="pt-4 flex justify-end gap-2">
          <Button type="button" variant="secondary" onClick={onClose}>
            Cancel
          </Button>
          <Button type="submit" variant="primary" disabled={isPending}>
            {isPending ? 'Saving...' : 'Save'}
          </Button>
        </div>
      </form>
    </Modal>
  );
}
