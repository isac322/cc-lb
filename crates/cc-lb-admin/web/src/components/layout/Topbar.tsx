import { useState } from 'react';
import { clearAdminToken, getAdminToken, setAdminToken } from '../../lib/auth';
import { Button } from '../primitives/Button';
import { FormField } from '../primitives/FormField';
import { Modal } from '../primitives/Modal';
import { ConnectionStatus } from './ConnectionStatus';

export function Topbar({ title }: { title: string }) {
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [tokenInput, setTokenInput] = useState('');

  const handleOpenModal = () => {
    setTokenInput(getAdminToken() || '');
    setIsModalOpen(true);
  };

  const handleSave = () => {
    if (tokenInput.trim()) {
      setAdminToken(tokenInput.trim());
    } else {
      clearAdminToken();
    }
    setIsModalOpen(false);
    window.location.reload();
  };

  return (
    <header className="h-16 border-b border-graphite-800 bg-graphite-900 flex items-center justify-between px-6 sticky top-0 z-10">
      <h1 className="text-lg font-semibold text-graphite-50">{title}</h1>
      <div className="flex items-center gap-4">
        <ConnectionStatus />
        <button
          onClick={handleOpenModal}
          className="text-sm text-graphite-300 hover:text-graphite-50 transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-cyan-500 rounded px-2 py-1"
        >
          Admin Token
        </button>
        <span className="text-xs text-graphite-500 font-mono">v1.0.0</span>
      </div>

      <Modal
        isOpen={isModalOpen}
        onClose={() => setIsModalOpen(false)}
        title="Admin Token"
      >
        <div className="space-y-4">
          <FormField
            label="Bearer Token"
            help="Enter the admin token to authenticate with the cc-lb server."
          >
            <input
              type="password"
              value={tokenInput}
              onChange={(e) => setTokenInput(e.target.value)}
              className="w-full bg-graphite-900 border border-graphite-700 rounded px-3 py-2 text-graphite-100 focus:outline-none focus:border-cyan-500 focus:ring-1 focus:ring-cyan-500"
              placeholder="Token..."
            />
          </FormField>
          <div className="flex justify-end gap-2">
            <Button variant="ghost" onClick={() => setIsModalOpen(false)}>
              Cancel
            </Button>
            <Button variant="primary" onClick={handleSave}>
              Save & Reload
            </Button>
          </div>
        </div>
      </Modal>
    </header>
  );
}
