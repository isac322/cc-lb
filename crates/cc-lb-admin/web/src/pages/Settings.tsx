import { useCallback, useEffect, useRef, useState } from 'react';
import { Button } from '../components/primitives/Button';
import { ErrorState } from '../components/primitives/ErrorState';
import { LoadingState } from '../components/primitives/LoadingState';
import { Modal } from '../components/primitives/Modal';
import { DiffPreviewPanel } from '../components/settings/DiffPreviewPanel';
import { FormSection } from '../components/settings/FormSection';
import { HistoryDrawer } from '../components/settings/HistoryDrawer';
import { PrincipalsEditor } from '../components/settings/PrincipalsEditor';
import { SchemaForm } from '../components/settings/SchemaForm';
import { SectionNav } from '../components/settings/SectionNav';
import { SettingsLayout } from '../components/settings/SettingsLayout';
import { UpstreamsEditor } from '../components/settings/UpstreamsEditor';
import { ValidateApplyBar } from '../components/settings/ValidateApplyBar';
import { useConfigApply } from '../lib/hooks/useConfigApply';
import { useConfigDraft } from '../lib/hooks/useConfigDraft';
import { useConfigSchema } from '../lib/hooks/useConfigSchema';
import { useConfigValidate } from '../lib/hooks/useConfigValidate';

export default function Settings() {
  const {
    schema,
    error: schemaError,
    loading: schemaLoading,
  } = useConfigSchema();
  const {
    draftData,
    error: draftError,
    loading: draftLoading,
    saving,
    saveError,
    conflict,
    saveDraft,
    fetchDraft,
  } = useConfigDraft();
  const { validate, validating } = useConfigValidate();
  const { apply, applying } = useConfigApply();

  const [activeSection, setActiveSection] = useState<string>('');
  const [localDraft, setLocalDraft] = useState<Record<string, unknown> | null>(
    null,
  );
  const [diffModalOpen, setDiffModalOpen] = useState(false);
  const [diffFromRev, setDiffFromRev] = useState<number | null>(null);
  const [applyModalOpen, setApplyModalOpen] = useState(false);

  const saveTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  // Initialize local draft when fetched
  useEffect(() => {
    if (draftData && !localDraft) {
      setLocalDraft(draftData.draft || {});
    }
  }, [draftData, localDraft]);

  // Set initial active section
  useEffect(() => {
    if (schema?.coverage_checklist && !activeSection) {
      setActiveSection(schema.coverage_checklist[0]);
    }
  }, [schema, activeSection]);

  // Check coverage
  useEffect(() => {
    if (
      schema?.coverage_checklist &&
      schema.schema &&
      typeof schema.schema === 'object'
    ) {
      const props = (schema.schema as Record<string, unknown>)
        .properties as Record<string, unknown>;
      if (props) {
        const missing = schema.coverage_checklist.filter((key) => !props[key]);
        if (missing.length > 0) {
          console.error('Missing coverage sections in schema:', missing);
        }
      }
    }
  }, [schema]);

  // Handle mock dialogs
  useEffect(() => {
    const isMock =
      new URLSearchParams(window.location.search).get('mock') === '1';
    const dialog = new URLSearchParams(window.location.search).get('dialog');
    if (isMock) {
      if (dialog === 'apply') setApplyModalOpen(true);
      if (dialog === 'diff') {
        setDiffFromRev(41);
        setDiffModalOpen(true);
      }
    }
  }, []);

  const handleSectionSelect = (section: string) => {
    setActiveSection(section);
    const el = document.getElementById(`section-${section}`);
    if (el) {
      el.scrollIntoView({ behavior: 'smooth' });
    }
  };

  const handleDraftChange = useCallback(
    (key: string, value: unknown) => {
      setLocalDraft((prev) => {
        const next = { ...(prev || {}), [key]: value };

        if (saveTimeoutRef.current) {
          clearTimeout(saveTimeoutRef.current);
        }

        saveTimeoutRef.current = setTimeout(() => {
          if (draftData) {
            saveDraft(next, draftData.revision);
          }
        }, 400);

        return next;
      });
    },
    [draftData, saveDraft],
  );

  const handleValidate = async () => {
    if (!draftData) return;
    try {
      const res = await validate(draftData.revision);
      if (res.valid) {
        fetchDraft(); // Refresh to get updated last_validated_revision
      } else {
        fetchDraft(); // Refresh to get updated last_validation_error
      }
    } catch {
      // Error handled by hook
    }
  };

  const handleApply = async () => {
    if (!draftData) return;
    try {
      await apply(draftData.revision);
      setApplyModalOpen(false);
      fetchDraft(); // Refresh draft state
      // In a real app, we might want to trigger a global reload or show a success toast
    } catch {
      // Error handled by hook
    }
  };

  const handleHistorySelect = (revision: number) => {
    setDiffFromRev(revision);
    setDiffModalOpen(true);
  };

  if (schemaLoading || draftLoading)
    return <LoadingState message="Loading settings..." />;
  if (schemaError)
    return (
      <ErrorState title="Failed to load schema" message={schemaError.message} />
    );
  if (draftError)
    return (
      <ErrorState title="Failed to load draft" message={draftError.message} />
    );
  if (!schema || !draftData || !localDraft) return null;

  return (
    <>
      <SettingsLayout
        nav={
          <SectionNav
            sections={schema.coverage_checklist}
            activeSection={activeSection}
            onSelect={handleSectionSelect}
          />
        }
        main={
          <div className="space-y-8">
            {schema.coverage_checklist.map((key) => {
              const props = (schema.schema as Record<string, unknown>)
                .properties as Record<string, unknown>;
              const propSchema = props?.[key] as Record<string, unknown>;
              if (!propSchema) return null;

              return (
                <FormSection
                  key={key}
                  id={`section-${key}`}
                  title={(propSchema.title as string) || key}
                  description={propSchema.description as string | undefined}
                >
                  {key === 'upstreams' ? (
                    <UpstreamsEditor
                      schema={propSchema as Record<string, unknown>}
                      rootSchema={schema.schema as Record<string, unknown>}
                      value={
                        (localDraft[key] as Record<string, unknown>[]) || []
                      }
                      onChange={(val) => handleDraftChange(key, val)}
                      error={
                        draftData.last_validation_error?.startsWith(key)
                          ? draftData.last_validation_error
                          : undefined
                      }
                    />
                  ) : key === 'principals' ? (
                    <PrincipalsEditor
                      schema={propSchema as Record<string, unknown>}
                      rootSchema={schema.schema as Record<string, unknown>}
                      value={(localDraft[key] as Record<string, unknown>) || {}}
                      onChange={(val) => handleDraftChange(key, val)}
                      error={
                        draftData.last_validation_error?.startsWith(key)
                          ? draftData.last_validation_error
                          : undefined
                      }
                    />
                  ) : (
                    <SchemaForm
                      schema={propSchema as Record<string, unknown>}
                      rootSchema={schema.schema as Record<string, unknown>}
                      value={localDraft[key]}
                      onChange={(val) => handleDraftChange(key, val)}
                      path={key}
                      error={
                        draftData.last_validation_error?.startsWith(key)
                          ? draftData.last_validation_error
                          : undefined
                      }
                    />
                  )}
                </FormSection>
              );
            })}
          </div>
        }
        sidebar={<HistoryDrawer onSelectRevision={handleHistorySelect} />}
      />

      <ValidateApplyBar
        revision={draftData.revision}
        lastValidatedRevision={draftData.last_validated_revision}
        lastValidationError={draftData.last_validation_error}
        saving={saving}
        saveError={saveError}
        conflict={conflict}
        validating={validating}
        applying={applying}
        onValidate={handleValidate}
        onApply={() => setApplyModalOpen(true)}
        onRefresh={fetchDraft}
      />

      <Modal
        isOpen={applyModalOpen}
        onClose={() => setApplyModalOpen(false)}
        title="Apply Configuration"
      >
        <div className="space-y-4">
          <p className="text-sm text-graphite-300">
            Apply revision{' '}
            <span className="font-mono text-graphite-50">
              {draftData.revision}
            </span>
            ? This will atomically write the config file and trigger a reload.
          </p>
          <div className="flex justify-end space-x-3">
            <Button
              variant="secondary"
              onClick={() => setApplyModalOpen(false)}
            >
              Cancel
            </Button>
            <Button variant="primary" onClick={handleApply} disabled={applying}>
              {applying ? 'Applying...' : 'Confirm Apply'}
            </Button>
          </div>
        </div>
      </Modal>

      <Modal
        isOpen={diffModalOpen}
        onClose={() => setDiffModalOpen(false)}
        title="Configuration Diff"
      >
        <div className="space-y-4">
          <DiffPreviewPanel
            fromRevision={diffFromRev}
            toRevision={draftData.revision} // Compare against current draft revision
          />
          <div className="flex justify-end">
            <Button variant="secondary" onClick={() => setDiffModalOpen(false)}>
              Close
            </Button>
          </div>
        </div>
      </Modal>
    </>
  );
}
