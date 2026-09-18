import { Input as BaseInput } from '@base-ui/react/input';
import { type KeyboardEvent, useEffect, useRef, useState } from 'react';
import { toast } from 'sonner';
import { type Upstream, useUpdateUpstream } from '../../lib/queries';
import { cx, INPUT_CLASS } from '../ui/primitives';

type Props = {
  upstream: Upstream;
  className?: string;
};

export function InlineNameEditor({ upstream, className }: Props) {
  const [isEditing, setIsEditing] = useState(false);
  const [name, setName] = useState(upstream.name);
  const inputRef = useRef<HTMLInputElement>(null);
  const saveStartedRef = useRef(false);
  const updateUpstream = useUpdateUpstream();

  useEffect(() => {
    if (isEditing && inputRef.current) {
      inputRef.current.focus();
    }
  }, [isEditing]);

  useEffect(() => {
    if (!isEditing) {
      setName(upstream.name);
    }
  }, [upstream.name, isEditing]);

  const handleSave = () => {
    if (saveStartedRef.current) return;

    const trimmedNewName = name.trim();
    saveStartedRef.current = true;

    if (!trimmedNewName || trimmedNewName === upstream.name) {
      setIsEditing(false);
      setName(upstream.name);
      return;
    }

    updateUpstream.mutate(
      {
        id: upstream.id,
        body: { name: trimmedNewName },
        spec_revision: upstream.spec_revision,
      },
      {
        onSuccess: () => {
          toast.success('Name updated');
          setIsEditing(false);
        },
        onError: () => {
          setName(upstream.name);
          setIsEditing(false);
        },
      },
    );
  };

  const handleKeyDown = (e: KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') {
      e.preventDefault();
      handleSave();
    } else if (e.key === 'Escape') {
      e.preventDefault();
      saveStartedRef.current = true;
      setName(upstream.name);
      setIsEditing(false);
    }
  };

  if (isEditing) {
    return (
      <div className={cx('relative inline-block', className)}>
        <BaseInput
          className={cx(
            INPUT_CLASS,
            'text-xl font-mono w-fit',
            updateUpstream.isPending && 'opacity-50 cursor-not-allowed',
          )}
          disabled={updateUpstream.isPending}
          onBlur={handleSave}
          onChange={(e) => setName(e.target.value)}
          onKeyDown={handleKeyDown}
          ref={inputRef}
          type="text"
          value={name}
        />
        {updateUpstream.isPending && (
          <div className="absolute right-2 top-1/2 -translate-y-1/2">
            <div className="w-4 h-4 border-2 border-text-faint border-t-text rounded-full animate-spin" />
          </div>
        )}
      </div>
    );
  }

  return (
    <span
      className={cx(
        'text-xl font-mono cursor-text hover:border-b-dotted hover:border-text-faint border-b border-transparent transition-colors',
        className,
      )}
      onClick={() => {
        saveStartedRef.current = false;
        setName(upstream.name);
        setIsEditing(true);
      }}
      title="Click to edit name"
    >
      {upstream.name}
    </span>
  );
}
