import { Input as BaseInput } from '@base-ui/react/input';
import { type KeyboardEvent, useEffect, useRef, useState } from 'react';
import { toast } from 'sonner';
import { type Upstream, useUpdateUpstream } from '../../lib/queries';
import { cx, Spinner } from '../ui/primitives';

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
    // The control must keep reading as the title, not as a boxed form field:
    // same title-page type and line box, auto-sized to the current text by an
    // invisible sizer so typing never shifts the layout, and the same dotted
    // underline the resting title hints at on hover.
    return (
      <span className={cx('relative inline-grid max-w-full', className)}>
        <span
          aria-hidden="true"
          className="invisible col-start-1 row-start-1 block min-w-0 overflow-hidden whitespace-pre text-title-page"
        >
          {name.length > 0 ? name : ' '}
        </span>
        <BaseInput
          className={cx(
            'col-start-1 row-start-1 w-full min-w-0 rounded-sm border-b border-dotted border-text-faint bg-transparent text-title-page text-text transition-colors',
            'focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-1',
            updateUpstream.isPending && 'cursor-not-allowed opacity-50',
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
          <Spinner className="absolute right-0 top-1/2 size-4 -translate-y-1/2 text-text-faint" />
        )}
      </span>
    );
  }

  return (
    <span
      className={cx(
        'text-title-page text-text cursor-text rounded-sm border-b border-transparent hover:border-dotted hover:border-text-faint transition-colors',
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
