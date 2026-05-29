import { Trash2 } from 'lucide-react';
import { useCallback, useEffect, useState } from 'react';
import { ChainEditor } from '../components/ChainEditor';
import { WasmUploader } from '../components/WasmUploader';
import {
  useDeleteRegistryEntry,
  useRegistryList,
} from '../lib/hooks/usePluginRegistry';
import type { RegistryEntryResponse } from '../lib/types/v1';

export default function PluginRegistry() {
  const [activeTab, setActiveTab] = useState<'registry' | 'chains'>('registry');
  const [entries, setEntries] = useState<RegistryEntryResponse[]>([]);
  const [error, setError] = useState<string | null>(null);

  const fetchRegistry = useRegistryList();
  const deleteEntry = useDeleteRegistryEntry();

  const loadRegistry = useCallback(async () => {
    try {
      const res = await fetchRegistry();
      setEntries(res.entries);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  }, [fetchRegistry]);

  useEffect(() => {
    loadRegistry();
  }, [loadRegistry]);

  const handleDelete = async (id: string, revision: number) => {
    try {
      await deleteEntry(id, revision);
      await loadRegistry();
    } catch (err: unknown) {
      const error = err as Error;
      setError(error.message || 'Failed to delete entry');
    }
  };

  return (
    <div className="p-6 max-w-7xl mx-auto space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold text-gray-900">Plugin Management</h1>
      </div>

      <div className="border-b border-gray-200">
        <nav className="-mb-px flex space-x-8">
          <button
            onClick={() => setActiveTab('registry')}
            className={`${
              activeTab === 'registry'
                ? 'border-blue-500 text-blue-600'
                : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
            } whitespace-nowrap py-4 px-1 border-b-2 font-medium text-sm`}
          >
            Wasm Registry
          </button>
          <button
            onClick={() => setActiveTab('chains')}
            className={`${
              activeTab === 'chains'
                ? 'border-blue-500 text-blue-600'
                : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
            } whitespace-nowrap py-4 px-1 border-b-2 font-medium text-sm`}
          >
            Per-Principal Chains
          </button>
        </nav>
      </div>

      {error && (
        <div className="p-4 text-sm text-red-700 bg-red-100 rounded-md">
          {error}
        </div>
      )}

      {activeTab === 'registry' && (
        <div className="space-y-6">
          <WasmUploader onSuccess={loadRegistry} />

          <div className="bg-white shadow-sm rounded-lg border overflow-hidden">
            <table className="min-w-full divide-y divide-gray-200">
              <thead className="bg-gray-50">
                <tr>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    Name
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    SHA256
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    Size
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    Refcount
                  </th>
                  <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">
                    Actions
                  </th>
                </tr>
              </thead>
              <tbody className="bg-white divide-y divide-gray-200">
                {entries.map((entry) => (
                  <tr key={entry.id}>
                    <td className="px-6 py-4 whitespace-nowrap">
                      <div className="text-sm font-medium text-gray-900">
                        {entry.name}
                      </div>
                      <div className="text-sm text-gray-500">
                        {entry.original_filename}
                      </div>
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500 font-mono">
                      {entry.sha256_hex.substring(0, 12)}...
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                      {(entry.size_bytes / 1024).toFixed(1)} KB
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                      {entry.refcount > 0 ? (
                        <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-blue-100 text-blue-800">
                          In use ({entry.refcount})
                        </span>
                      ) : (
                        <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-gray-100 text-gray-800">
                          Unused
                        </span>
                      )}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-right text-sm font-medium">
                      <button
                        onClick={() => handleDelete(entry.id, entry.revision)}
                        disabled={entry.refcount > 0}
                        className="text-red-600 hover:text-red-900 disabled:opacity-50 disabled:cursor-not-allowed"
                        title={
                          entry.refcount > 0
                            ? `In use by ${entry.refcount} chains`
                            : 'Delete'
                        }
                      >
                        <Trash2 className="w-5 h-5" />
                      </button>
                    </td>
                  </tr>
                ))}
                {entries.length === 0 && (
                  <tr>
                    <td
                      colSpan={5}
                      className="px-6 py-4 text-center text-sm text-gray-500"
                    >
                      No plugins uploaded yet.
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {activeTab === 'chains' && <ChainEditor />}
    </div>
  );
}
