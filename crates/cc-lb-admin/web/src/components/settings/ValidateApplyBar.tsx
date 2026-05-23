import { Button } from '../primitives/Button';
import { StatusChip } from '../primitives/StatusChip';

interface ValidateApplyBarProps {
  revision: number;
  lastValidatedRevision: number | null;
  lastValidationError: string | null;
  saving: boolean;
  saveError: Error | null;
  conflict: boolean;
  validating: boolean;
  applying: boolean;
  onValidate: () => void;
  onApply: () => void;
  onRefresh: () => void;
}

export function ValidateApplyBar({
  revision,
  lastValidatedRevision,
  lastValidationError,
  saving,
  saveError,
  conflict,
  validating,
  applying,
  onValidate,
  onApply,
  onRefresh,
}: ValidateApplyBarProps) {
  const isValidated = lastValidatedRevision === revision;
  const canValidate = !saving && !conflict && !saveError;
  const canApply = isValidated && !saving && !conflict && !saveError;

  return (
    <div className="fixed bottom-0 left-64 right-0 bg-graphite-900 border-t border-graphite-800 p-4 z-10">
      <div className="max-w-4xl mx-auto flex items-center justify-between">
        <div className="flex items-center space-x-4">
          <div className="text-sm text-graphite-300">
            Draft Revision: <span className="font-mono font-medium text-graphite-50">{revision}</span>
          </div>
          
          {saving ? (
            <StatusChip variant="warn">Saving...</StatusChip>
          ) : saveError ? (
            <StatusChip variant="danger">Save failed</StatusChip>
          ) : conflict ? (
            <StatusChip variant="danger">Conflict</StatusChip>
          ) : (
            <StatusChip variant="ok">Saved</StatusChip>
          )}

          {isValidated && !conflict && (
            <StatusChip variant="ok">Validated Rev {lastValidatedRevision}</StatusChip>
          )}
        </div>

        <div className="flex items-center space-x-3">
          {conflict && (
            <Button variant="danger" onClick={onRefresh}>
              Refresh to continue
            </Button>
          )}
          <Button
            variant="secondary"
            onClick={onValidate}
            disabled={!canValidate || validating}
            title={saving ? 'Cannot validate while saving' : saveError ? 'Cannot validate or apply while autosave is failing — fix the save error first.' : conflict ? 'Resolve conflict first' : ''}
          >
            {validating ? 'Validating...' : 'Validate'}
          </Button>
          <Button
            variant="primary"
            onClick={onApply}
            disabled={!canApply || applying}
            title={!isValidated ? 'Must validate current revision first' : saving ? 'Cannot apply while saving' : saveError ? 'Cannot validate or apply while autosave is failing — fix the save error first.' : conflict ? 'Resolve conflict first' : ''}
          >
            {applying ? 'Applying...' : 'Apply'}
          </Button>
        </div>
      </div>
      
      {lastValidationError && !isValidated && (
        <div className="max-w-4xl mx-auto mt-3 p-3 bg-red-900/20 border border-red-800 rounded-md text-sm text-red-200">
          <strong>Validation Error:</strong> {lastValidationError}
        </div>
      )}
      {conflict && (
        <div className="max-w-4xl mx-auto mt-3 p-3 bg-yellow-900/20 border border-yellow-800 rounded-md text-sm text-yellow-200">
          <strong>Draft updated elsewhere.</strong> Please refresh to drop local edits and continue.
        </div>
      )}
    </div>
  );
}
