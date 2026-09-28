import type { UseMutationResult } from '@tanstack/react-query';
import { UploadCloud } from 'lucide-react';
import { useEffect, useRef, useState } from 'react';
import { toast } from 'sonner';
import { ApiError } from '../../lib/api';
import type { UploadWasmResponse } from '../../lib/queries';
import { ConfirmDialog, cx, Section } from '../ui/primitives';

interface ReplacementConfirmationBody {
  error: 'replacement_confirmation_required';
  name: string;
  replace_registry_id: string;
  expected_revision: number;
  current_version?: string | null;
  incoming_version?: string | null;
  current_sha256_hex: string;
  incoming_sha256_hex: string;
}

function isReplacementConfirmationBody(
  body: unknown,
): body is ReplacementConfirmationBody {
  if (!body || typeof body !== 'object') return false;
  const record = body as Record<string, unknown>;
  return (
    record.error === 'replacement_confirmation_required' &&
    typeof record.name === 'string' &&
    typeof record.replace_registry_id === 'string' &&
    typeof record.expected_revision === 'number' &&
    typeof record.current_sha256_hex === 'string' &&
    typeof record.incoming_sha256_hex === 'string'
  );
}

type PluginUploadVariables = {
  file: File;
  confirmReplacement?: boolean;
  replaceRegistryId?: string;
  expectedRevision?: number;
};

type PluginUploadMutation = Pick<
  UseMutationResult<UploadWasmResponse, Error, PluginUploadVariables>,
  'isPending' | 'mutate'
>;

export function PluginUploadCard({
  autoFocus = false,
  bare = false,
  onAutoFocus,
  onUploaded,
  upload,
}: {
  autoFocus?: boolean;
  /** Render only the drop zone, for hosts (the upload dialog) that already title it. */
  bare?: boolean;
  onAutoFocus?: () => void;
  onUploaded?: (id: string) => void;
  upload: PluginUploadMutation;
}) {
  const fileRef = useRef<HTMLInputElement>(null);
  const browseRef = useRef<HTMLDivElement>(null);
  const [pendingReplacement, setPendingReplacement] = useState<{
    file: File;
    name: string;
    replaceRegistryId: string;
    expectedRevision: number;
    currentVersion?: string;
    incomingVersion?: string;
    currentSha256Hex: string;
    incomingSha256Hex: string;
  } | null>(null);

  const handleFile = (file: File | null | undefined) => {
    if (!file || upload.isPending) return;
    upload.mutate(
      { file },
      {
        onSuccess: (data) => {
          const suffix = data.idempotent ? ' (already in registry)' : '';
          toast.success(`Uploaded ${data.original_filename}${suffix}`);
          onUploaded?.(data.id);
        },
        onError: (error) => {
          if (
            error instanceof ApiError &&
            error.code === 'replacement_confirmation_required' &&
            isReplacementConfirmationBody(error.body)
          ) {
            setPendingReplacement({
              file,
              name: error.body.name,
              replaceRegistryId: error.body.replace_registry_id,
              expectedRevision: error.body.expected_revision,
              currentVersion: error.body.current_version ?? undefined,
              incomingVersion: error.body.incoming_version ?? undefined,
              currentSha256Hex: error.body.current_sha256_hex,
              incomingSha256Hex: error.body.incoming_sha256_hex,
            });
          } else {
            toast.error(error instanceof Error ? error.message : String(error));
          }
        },
      },
    );
  };

  const uploading = upload.isPending;

  useEffect(() => {
    if (!autoFocus || uploading) return;
    browseRef.current?.focus();
    if (document.activeElement === browseRef.current) onAutoFocus?.();
  }, [autoFocus, onAutoFocus, uploading]);

  const dropZone = (
    <div
      ref={browseRef}
      role="button"
      tabIndex={uploading ? -1 : 0}
      aria-disabled={uploading || undefined}
      aria-label={uploading ? 'Uploading plugin' : 'Choose .wasm file'}
      aria-busy={uploading}
      // Phones get a compact one-line picker (icon beside the copy, no drag
      // hint: touch has no drag and drop); from `md` it is the centered
      // drop zone.
      className={cx(
        'flex items-center gap-3 rounded-sm border border-dashed border-subtle-strong px-4 py-3 text-left transition-colors focus-visible:outline focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-2 md:flex-col md:justify-center md:gap-0 md:py-8 md:text-center',
        uploading
          ? 'cursor-wait opacity-70'
          : 'cursor-pointer hover:border-text-faint hover:bg-hover-bg',
      )}
      onClick={() => {
        if (!uploading) fileRef.current?.click();
      }}
      onKeyDown={(e) => {
        if (!uploading && (e.key === 'Enter' || e.key === ' ')) {
          e.preventDefault();
          fileRef.current?.click();
        }
      }}
      onDragOver={(e) => {
        e.preventDefault();
      }}
      onDrop={(e) => {
        e.preventDefault();
        if (!uploading) handleFile(e.dataTransfer.files?.[0]);
      }}
    >
      <UploadCloud
        strokeWidth={1.75}
        aria-hidden="true"
        className={cx(
          'size-6 shrink-0 md:mb-2',
          uploading
            ? 'text-text-muted motion-safe:animate-pulse'
            : 'text-text-faint',
        )}
      />
      <div className="min-w-0">
        <div className="text-body font-medium text-text">
          {uploading ? 'Uploading…' : 'Choose .wasm file'}
        </div>
        <div className="mt-0.5 text-body-sm text-text-muted md:mt-1">
          <span className="md:hidden">Tap to browse. Max 32 MiB.</span>
          <span className="max-md:hidden">
            Drag and drop or click to browse. Max 32 MiB.
          </span>
        </div>
      </div>
      <input
        ref={fileRef}
        type="file"
        aria-label="Plugin file"
        accept=".wasm"
        className="hidden"
        disabled={uploading}
        onChange={(e) => {
          handleFile(e.target.files?.[0]);
          e.target.value = '';
        }}
      />
    </div>
  );

  return (
    <>
      {bare ? (
        dropZone
      ) : (
        <Section
          title="Upload plugin"
          subtitle="Choose the .wasm file you received. After upload, review what it can do and where it can be used."
        >
          {dropZone}
        </Section>
      )}

      <ConfirmDialog
        open={pendingReplacement !== null}
        onOpenChange={(o) => {
          if (!o) setPendingReplacement(null);
        }}
        title="Confirm plugin replacement"
        description={
          pendingReplacement ? (
            <span className="block space-y-3">
              <span className="block">
                A plugin named{' '}
                <span className="font-medium text-text">
                  {pendingReplacement.name}
                </span>{' '}
                already exists.
              </span>
              <span className="well block space-y-1 p-3 text-body">
                <span className="block">
                  <span className="font-medium text-text">Current:</span>{' '}
                  {pendingReplacement.currentVersion || 'none'} (
                  <code className="font-mono text-data">
                    {pendingReplacement.currentSha256Hex.slice(0, 12)}
                  </code>
                  )
                </span>
                <span className="block">
                  <span className="font-medium text-text">Incoming:</span>{' '}
                  {pendingReplacement.incomingVersion || 'none'} (
                  <code className="font-mono text-data">
                    {pendingReplacement.incomingSha256Hex.slice(0, 12)}
                  </code>
                  )
                </span>
              </span>
              <span className="block">Do you want to replace it?</span>
            </span>
          ) : null
        }
        confirmLabel="Replace"
        onConfirm={() => {
          if (!pendingReplacement) return;
          upload.mutate(
            {
              file: pendingReplacement.file,
              confirmReplacement: true,
              replaceRegistryId: pendingReplacement.replaceRegistryId,
              expectedRevision: pendingReplacement.expectedRevision,
            },
            {
              onSuccess: (data) => {
                toast.success(`Replaced ${data.original_filename}`);
                onUploaded?.(data.id);
              },
              onError: (e) =>
                toast.error(e instanceof Error ? e.message : String(e)),
            },
          );
          setPendingReplacement(null);
        }}
      />
    </>
  );
}
