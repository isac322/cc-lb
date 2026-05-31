import type { DragEndEvent } from '@dnd-kit/core';
import {
  closestCenter,
  DndContext,
  KeyboardSensor,
  PointerSensor,
  useSensor,
  useSensors,
} from '@dnd-kit/core';
import {
  arrayMove,
  SortableContext,
  sortableKeyboardCoordinates,
  useSortable,
  verticalListSortingStrategy,
} from '@dnd-kit/sortable';
import { CSS } from '@dnd-kit/utilities';
import { GripVertical, Plus, Trash2 } from 'lucide-react';
import { useCallback, useEffect, useState } from 'react';
import {
  useChainDelete,
  useChainInsert,
  useChainList,
  useChainRebalance,
  useChainReorder,
  useRegistryList,
} from '../lib/hooks/usePluginRegistry';
import { useList as usePrincipalList } from '../lib/hooks/usePrincipals';
import { between } from '../lib/sparseOrder';
import type {
  PluginChainEntry,
  PluginSlot,
  RegistryEntryResponse,
} from '../lib/types/v1';

interface SortableItemProps {
  entry: PluginChainEntry;
  registryEntries: RegistryEntryResponse[];
  onDelete: (id: string, revision: number) => void;
}

function SortableItem({ entry, registryEntries, onDelete }: SortableItemProps) {
  const { attributes, listeners, setNodeRef, transform, transition } =
    useSortable({
      id: entry.id,
    });

  const style = {
    transform: CSS.Transform.toString(transform),
    transition,
  };

  const registryEntry = registryEntries.find(
    (r) => r.id === entry.wasm_registry_id,
  );

  return (
    <div
      ref={setNodeRef}
      style={style}
      className="flex items-center justify-between p-3 mb-2 bg-white border rounded-md shadow-sm"
    >
      <div className="flex items-center gap-3">
        <div
          {...attributes}
          {...listeners}
          className="cursor-grab text-gray-400 hover:text-gray-600"
        >
          <GripVertical className="w-5 h-5" />
        </div>
        <div>
          <div className="font-medium">
            {registryEntry?.name || 'Unknown Plugin'}
          </div>
          <div className="text-xs text-gray-500">
            Order: {entry.order} | ID: {entry.id.substring(0, 8)}
          </div>
        </div>
      </div>
      <button
        onClick={() => onDelete(entry.id, entry.revision)}
        className="p-1 text-red-600 hover:bg-red-50 rounded"
        title="Remove from chain"
      >
        <Trash2 className="w-4 h-4" />
      </button>
    </div>
  );
}

interface ChainListProps {
  principalId: string;
  slot: PluginSlot;
  registryEntries: RegistryEntryResponse[];
}

function ChainList({ principalId, slot, registryEntries }: ChainListProps) {
  const [entries, setEntries] = useState<PluginChainEntry[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [needsRebalance, setNeedsRebalance] = useState(false);
  const [isAdding, setIsAdding] = useState(false);
  const [selectedWasmId, setSelectedWasmId] = useState('');
  const [configJson, setConfigJson] = useState('{}');

  const fetchChain = useChainList();
  const reorderChain = useChainReorder();
  const rebalanceChain = useChainRebalance();
  const insertChain = useChainInsert();
  const deleteChain = useChainDelete();

  const loadChain = useCallback(async () => {
    try {
      const res = await fetchChain(principalId, slot);
      setEntries(res.entries.sort((a, b) => a.order - b.order));
      setError(null);
      setNeedsRebalance(false);
    } catch (err: unknown) {
      const error = err as Error & { latest?: { error?: string } };
      setError(error.message || 'Failed to load chain');
    }
  }, [principalId, slot, fetchChain]);

  useEffect(() => {
    if (principalId) {
      loadChain();
    }
  }, [principalId, loadChain]);

  const sensors = useSensors(
    useSensor(PointerSensor),
    useSensor(KeyboardSensor, {
      coordinateGetter: sortableKeyboardCoordinates,
    }),
  );

  const handleDragEnd = async (event: DragEndEvent) => {
    const { active, over } = event;

    if (over && active.id !== over.id) {
      const oldIndex = entries.findIndex((e) => e.id === active.id);
      const newIndex = entries.findIndex((e) => e.id === over.id);

      const newEntries = arrayMove(entries, oldIndex, newIndex);
      setEntries(newEntries);

      try {
        const activeEntry = entries[oldIndex];
        let newOrder: number;

        if (newIndex === 0) {
          newOrder = newEntries[1].order - 1000;
        } else if (newIndex === newEntries.length - 1) {
          newOrder = newEntries[newEntries.length - 2].order + 1000;
        } else {
          const prevOrder = newEntries[newIndex - 1].order;
          const nextOrder = newEntries[newIndex + 1].order;
          newOrder = between(prevOrder, nextOrder);
        }

        await reorderChain(principalId, {
          entries: [
            {
              id: activeEntry.id,
              order: newOrder,
              expected_revision: activeEntry.revision,
            },
          ],
        });
        await loadChain();
      } catch (err: unknown) {
        const error = err as Error & { latest?: { error?: string } };
        if (
          error.latest?.error === 'needs_rebalance' ||
          error.message?.includes('needs_rebalance')
        ) {
          setNeedsRebalance(true);
          setError('Chain needs rebalancing before reordering.');
        } else {
          setError(error.message || 'Failed to reorder');
          await loadChain(); // revert
        }
      }
    }
  };

  const handleRebalance = async () => {
    try {
      await rebalanceChain(principalId, slot);
      await loadChain();
    } catch (err: unknown) {
      const error = err as Error & { latest?: { error?: string } };
      setError(error.message || 'Failed to rebalance');
    }
  };

  const handleDelete = async (id: string, revision: number) => {
    try {
      await deleteChain(id, revision);
      await loadChain();
    } catch (err: unknown) {
      const error = err as Error & { latest?: { error?: string } };
      setError(error.message || 'Failed to delete entry');
    }
  };

  const handleAdd = async () => {
    try {
      let config = {};
      try {
        config = JSON.parse(configJson);
      } catch {
        setError('Invalid JSON config');
        return;
      }

      await insertChain(principalId, {
        slot,
        wasm_registry_id: selectedWasmId,
        config,
        position: 'last',
      });
      setIsAdding(false);
      setSelectedWasmId('');
      setConfigJson('{}');
      await loadChain();
    } catch (err: unknown) {
      const error = err as Error & { latest?: { error?: string } };
      setError(error.message || 'Failed to add entry');
    }
  };

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h3 className="text-lg font-medium capitalize">
          {slot.replace('_', ' ')} Chain
        </h3>
        <button
          onClick={() => setIsAdding(true)}
          className="inline-flex items-center px-3 py-1.5 text-sm font-medium text-white bg-blue-600 rounded-md hover:bg-blue-700"
        >
          <Plus className="w-4 h-4 mr-1" />
          Add Plugin
        </button>
      </div>

      {error && (
        <div className="p-3 text-sm text-red-700 bg-red-100 rounded-md">
          {error}
          {needsRebalance && (
            <button
              onClick={handleRebalance}
              className="ml-4 px-3 py-1 text-xs font-medium text-white bg-red-600 rounded hover:bg-red-700"
            >
              Auto-balance
            </button>
          )}
        </div>
      )}

      {isAdding && (
        <div className="p-4 border rounded-md bg-gray-50 space-y-3">
          <h4 className="font-medium">Add Plugin to Chain</h4>
          <div>
            <label className="block text-sm font-medium text-gray-700">
              Plugin
            </label>
            <select
              value={selectedWasmId}
              onChange={(e) => setSelectedWasmId(e.target.value)}
              className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500 sm:text-sm border p-2"
            >
              <option value="">Select a plugin...</option>
              {registryEntries.map((r) => (
                <option key={r.id} value={r.id}>
                  {r.name} ({r.sha256_hex.substring(0, 8)})
                </option>
              ))}
            </select>
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700">
              Config (JSON)
            </label>
            <textarea
              value={configJson}
              onChange={(e) => setConfigJson(e.target.value)}
              className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500 sm:text-sm border p-2 font-mono"
              rows={3}
            />
          </div>
          <div className="flex justify-end gap-2">
            <button
              onClick={() => setIsAdding(false)}
              className="px-3 py-1.5 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-md hover:bg-gray-50"
            >
              Cancel
            </button>
            <button
              onClick={handleAdd}
              disabled={!selectedWasmId}
              className="px-3 py-1.5 text-sm font-medium text-white bg-blue-600 rounded-md hover:bg-blue-700 disabled:opacity-50"
            >
              Add
            </button>
          </div>
        </div>
      )}

      {entries.length === 0 ? (
        <div className="p-4 text-sm text-gray-500 text-center border rounded-md border-dashed">
          No plugins in this chain.
        </div>
      ) : (
        <DndContext
          sensors={sensors}
          collisionDetection={closestCenter}
          onDragEnd={handleDragEnd}
        >
          <SortableContext
            items={entries.map((e) => e.id)}
            strategy={verticalListSortingStrategy}
          >
            <div className="space-y-2">
              {entries.map((entry) => (
                <SortableItem
                  key={entry.id}
                  entry={entry}
                  registryEntries={registryEntries}
                  onDelete={handleDelete}
                />
              ))}
            </div>
          </SortableContext>
        </DndContext>
      )}
    </div>
  );
}

export function ChainEditor() {
  const [principalId, setPrincipalId] = useState('');
  const [principalList, setPrincipalList] = useState<
    { id: string; name: string }[]
  >([]);
  const [registryEntries, setRegistryEntries] = useState<
    RegistryEntryResponse[]
  >([]);

  const fetchPrincipals = usePrincipalList();
  const fetchRegistry = useRegistryList();

  useEffect(() => {
    fetchPrincipals().then((res: unknown) =>
      setPrincipalList(
        (res as { principals: { id: string; name: string }[] }).principals,
      ),
    );
    fetchRegistry().then((res: unknown) =>
      setRegistryEntries((res as { entries: RegistryEntryResponse[] }).entries),
    );
  }, [fetchPrincipals, fetchRegistry]);

  return (
    <div className="space-y-6">
      <div>
        <label className="block text-sm font-medium text-gray-700">
          Select Principal
        </label>
        <select
          value={principalId}
          onChange={(e) => setPrincipalId(e.target.value)}
          className="mt-1 block w-full max-w-md rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500 sm:text-sm border p-2"
        >
          <option value="">Select a principal...</option>
          {principalList.map((p) => (
            <option key={p.id} value={p.id}>
              {p.name}
            </option>
          ))}
        </select>
      </div>

      {principalId && (
        <div className="grid grid-cols-1 md:grid-cols-3 gap-8">
          <ChainList
            principalId={principalId}
            slot="router"
            registryEntries={registryEntries}
          />
          <ChainList
            principalId={principalId}
            slot="observability_hook"
            registryEntries={registryEntries}
          />
          <ChainList
            principalId={principalId}
            slot="shape"
            registryEntries={registryEntries}
          />
        </div>
      )}
    </div>
  );
}
