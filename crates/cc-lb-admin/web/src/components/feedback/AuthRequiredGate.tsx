import { useEffect, useState } from 'react';
import { getAdminToken, setAdminToken } from '../../lib/auth';
import { Button } from '../primitives/Button';
import { FormField } from '../primitives/FormField';

export function AuthRequiredGate({ children }: { children: React.ReactNode }) {
  const [hasToken, setHasToken] = useState(false);
  const [tokenInput, setTokenInput] = useState('');

  useEffect(() => {
    setHasToken(!!getAdminToken());
  }, []);

  const handleSave = (e: React.FormEvent) => {
    e.preventDefault();
    if (tokenInput.trim()) {
      setAdminToken(tokenInput.trim());
      setHasToken(true);
    }
  };

  if (hasToken) {
    return <>{children}</>;
  }

  return (
    <div className="min-h-screen bg-graphite-900 flex items-center justify-center p-4 font-sans">
      <div className="max-w-md w-full bg-graphite-850 border border-graphite-800 rounded-lg shadow-xl p-6">
        <div className="flex items-center justify-center mb-6">
          <div className="w-12 h-12 bg-cyan-500 rounded flex items-center justify-center text-graphite-950 font-bold text-xl">
            cc
          </div>
        </div>
        <h2 className="text-xl font-semibold text-graphite-50 text-center mb-2">
          Authentication Required
        </h2>
        <p className="text-sm text-graphite-400 text-center mb-6">
          Please enter your admin token to access the operator console.
        </p>
        <form onSubmit={handleSave} className="space-y-4">
          <FormField label="Bearer Token">
            <input
              type="password"
              value={tokenInput}
              onChange={(e) => setTokenInput(e.target.value)}
              className="w-full bg-graphite-900 border border-graphite-700 rounded px-3 py-2 text-graphite-100 focus:outline-none focus:border-cyan-500 focus:ring-1 focus:ring-cyan-500"
              placeholder="Token..."
              autoFocus
            />
          </FormField>
          <Button type="submit" variant="primary" className="w-full">
            Access Console
          </Button>
        </form>
      </div>
    </div>
  );
}
