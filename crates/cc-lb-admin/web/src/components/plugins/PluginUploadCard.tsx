import type { UseMutationResult } from '@tanstack/react-query';
import { UploadCloud } from 'lucide-react';
import { useEffect, useRef, useState } from 'react';
import { toast } from 'sonner';
import { ApiError } from '../../lib/api';
import type { UploadWasmResponse } from '../../lib/queries';
import { Card, CardHeader, ConfirmDialog, cx } from '../ui/primitives';

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
  onAutoFocus,
  onUploaded,
  upload,
}: {
  autoFocus?: boolean;
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

  return (
    <>
      <Card>
        <CardHeader
          title={
            <span className="flex items-baseline gap-2">
              <span className="text-base font-medium">Upload plugin</span>
            </span>
          }
          subtitle={
            <div className="space-y-1">
              <div>
                Choose the .wasm file you received. After upload, review what it
                can do and where it can be used.
              </div>
            </div>
          }
        />
        <div
          ref={browseRef}
          role="button"
          tabIndex={uploading ? -1 : 0}
          aria-disabled={uploading || undefined}
          aria-label={uploading ? 'Uploading plugin' : 'Choose .wasm file'}
          aria-busy={uploading}
          className={cx(
            'm-4 p-8 border border-dashed border-subtle rounded-sm flex flex-col items-center justify-center text-center transition-colors focus-visible:outline focus-visible:outline-2 focus-visible:outline-accent focus-visible:outline-offset-2',
            uploading
              ? 'cursor-wait opacity-70 border-accent/40'
              : 'cursor-pointer hover:border-accent/40',
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
            className={cx(
              'w-8 h-8 mb-2',
              uploading ? 'text-accent animate-pulse' : 'text-text-faint',
            )}
          />
          <div className="text-sm">
            {uploading ? 'Uploading…' : 'Choose .wasm file'}
          </div>
          <div className="text-[11px] text-text-faint mt-1">
            Drag and drop or click to browse. Max 32 MiB.
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
      </Card>

      <ConfirmDialog
        open={pendingReplacement !== null}
        onOpenChange={(o) => {
          if (!o) setPendingReplacement(null);
        }}
        title="Confirm Plugin Replacement"
        description={
          pendingReplacement ? (
            <span className="space-y-2 block">
              <span className="block">
                A plugin named{' '}
                <span className="font-mono">{pendingReplacement.name}</span>{' '}
                already exists.
              </span>
              <span className="text-xs bg-overlay-1 p-2 rounded-sm border border-subtle block">
                <span className="block">
                  <strong>Current:</strong>{' '}
                  {pendingReplacement.currentVersion || 'none'} (
                  <code className="text-[10px]">
                    {pendingReplacement.currentSha256Hex.slice(0, 12)}
                  </code>
                  )
                </span>
                <span className="block">
                  <strong>Incoming:</strong>{' '}
                  {pendingReplacement.incomingVersion || 'none'} (
                  <code className="text-[10px]">
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
