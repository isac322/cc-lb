import { useState } from 'react';
import { Button } from '../primitives/Button';

export function AllowedModelsEditor({
  models,
  onChange,
}: {
  models: string[];
  onChange: (models: string[]) => void;
}) {
  const [input, setInput] = useState('');

  const handleAdd = () => {
    const trimmed = input.trim();
    if (trimmed && !models.includes(trimmed)) {
      onChange([...models, trimmed]);
      setInput('');
    }
  };

  const handleRemove = (model: string) => {
    onChange(models.filter((m) => m !== model));
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter') {
      e.preventDefault();
      handleAdd();
    }
  };

  return (
    <div className="space-y-2">
      <div className="flex flex-wrap gap-2">
        {models.map((model) => (
          <span
            key={model}
            className="inline-flex items-center gap-1 px-2 py-1 rounded bg-graphite-800 text-graphite-100 text-sm"
          >
            {model}
            <button
              type="button"
              onClick={() => handleRemove(model)}
              className="text-graphite-400 hover:text-red-400 focus:outline-none"
            >
              &times;
            </button>
          </span>
        ))}
        {models.length === 0 && <span className="text-graphite-500 text-sm">No models allowed</span>}
      </div>
      <div className="flex gap-2">
        <input
          type="text"
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={handleKeyDown}
          placeholder="e.g. claude-3-5-sonnet"
          className="flex-1 bg-graphite-900 border border-graphite-800 rounded px-3 py-1.5 text-sm text-graphite-50 focus:outline-none focus:border-cyan-500"
        />
        <Button type="button" variant="secondary"  onClick={handleAdd} disabled={!input.trim()}>
          Add
        </Button>
      </div>
    </div>
  );
}
