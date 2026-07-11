import { RefreshCw } from 'lucide-react';
import { useCallback, useEffect, useState } from 'react';
import { mergeDateIntoField } from '../../lib/calendarDate';
import { useTimezone } from '../../lib/locale';
import { formatInTimezone } from '../../lib/timezone';
import { LogSelect } from './LogSelect';
import { Button, Field } from './primitives';
import { DateTimeField, parseBound } from './TimeRangeSelect.helpers';

export type TimeRangeMode = 'all' | '1h' | '6h' | '24h' | '7d' | 'custom';

export interface TimeRangeValue {
  mode: TimeRangeMode;
  since_unix_secs?: number;
  until_unix_secs?: number;
}

interface TimeRangeSelectProps {
  readonly value: TimeRangeValue;
  readonly onChange: (val: TimeRangeValue) => void;
}

const PRESETS: { value: TimeRangeMode; label: string; hours: number }[] = [
  { value: '1h', label: '1h', hours: 1 },
  { value: '6h', label: '6h', hours: 6 },
  { value: '24h', label: '24h', hours: 24 },
  { value: '7d', label: '7d', hours: 24 * 7 },
  { value: 'custom', label: 'Custom', hours: 0 },
];

function isTimeRangeMode(val: string): val is TimeRangeMode {
  return ['all', '1h', '6h', '24h', '7d', 'custom'].includes(val);
}

export function TimeRangeSelect({ value, onChange }: TimeRangeSelectProps) {
  const { effective: tz } = useTimezone();

  const [sinceStr, setSinceStr] = useState('');
  const [untilStr, setUntilStr] = useState('');
  const [sinceError, setSinceError] = useState<string>();
  const [untilError, setUntilError] = useState<string>();
  const [draftSince, setDraftSince] = useState<number>();
  const [draftUntil, setDraftUntil] = useState<number>();

  const resetDraft = useCallback(() => {
    const isCustom = value.mode === 'custom';
    const s =
      isCustom && value.since_unix_secs != null
        ? value.since_unix_secs
        : undefined;
    const u =
      isCustom && value.until_unix_secs != null
        ? value.until_unix_secs
        : undefined;

    setSinceStr(
      s != null ? formatInTimezone(s * 1000, tz).replace('T', ' ') : '',
    );
    setDraftSince(s);
    setUntilStr(
      u != null ? formatInTimezone(u * 1000, tz).replace('T', ' ') : '',
    );
    setDraftUntil(u);
    setSinceError(undefined);
    setUntilError(undefined);
  }, [value.mode, value.since_unix_secs, value.until_unix_secs, tz]);

  // Sync local state when value changes from outside (e.g. URL reload)
  useEffect(() => {
    resetDraft();
  }, [resetDraft]);

  const handleModeChange = (modeStr: string) => {
    const mode = modeStr === '' ? 'all' : modeStr;
    if (!isTimeRangeMode(mode)) return;

    if (mode === 'all') {
      onChange({ mode });
    } else if (mode === 'custom') {
      if (value.since_unix_secs != null && value.until_unix_secs != null) {
        onChange({
          mode,
          since_unix_secs: value.since_unix_secs,
          until_unix_secs: value.until_unix_secs,
        });
      } else {
        onChange({ mode });
      }
    } else {
      const preset = PRESETS.find((p) => p.value === mode);
      if (preset) {
        const since = Math.floor(Date.now() / 1000) - preset.hours * 3600;
        onChange({ mode, since_unix_secs: since });
      }
    }
  };

  const handleRefresh = () => {
    handleModeChange(value.mode);
  };

  const handleCustomChange = (field: 'since' | 'until', str: string) => {
    if (field === 'since') setSinceStr(str);
    else setUntilStr(str);

    const nextSinceStr = field === 'since' ? str : sinceStr;
    const nextUntilStr = field === 'until' ? str : untilStr;

    const s = parseBound(nextSinceStr, nextUntilStr, tz, 'since');
    const u = parseBound(nextUntilStr, nextSinceStr, tz, 'until');

    let sErr = s.err;
    let uErr = u.err;

    if (s.ts != null && u.ts != null && s.ts > u.ts) {
      if (field === 'since') sErr = 'Must be <= Until';
      else uErr = 'Must be >= Since';
    }

    setSinceError(sErr);
    setUntilError(uErr);
    setDraftSince(s.ts);
    setDraftUntil(u.ts);
  };

  const handlePickDate = (field: 'since' | 'until', isoDate: string) => {
    const current = field === 'since' ? sinceStr : untilStr;
    handleCustomChange(field, mergeDateIntoField(current, isoDate, field));
  };

  const isApplyDisabled =
    sinceError != null ||
    untilError != null ||
    draftSince == null ||
    draftUntil == null ||
    (draftSince === value.since_unix_secs &&
      draftUntil === value.until_unix_secs);

  const handleApply = () => {
    if (!isApplyDisabled && draftSince != null && draftUntil != null) {
      onChange({
        mode: 'custom',
        since_unix_secs: draftSince,
        until_unix_secs: draftUntil,
      });
    }
  };

  return (
    <div className="flex flex-wrap items-start gap-3">
      <Field label="Time Range">
        <div className="flex items-center gap-2">
          <LogSelect
            value={value.mode === 'all' ? '' : value.mode}
            options={PRESETS.map((p) => ({ value: p.value, label: p.label }))}
            onChange={handleModeChange}
            allLabel="All time"
            widthClass="w-40"
          />
          {value.mode !== 'all' && value.mode !== 'custom' && (
            <Button
              size="sm"
              variant="ghost"
              onClick={handleRefresh}
              title="Refresh time window"
              className="px-2"
            >
              <RefreshCw className="w-3.5 h-3.5" />
            </Button>
          )}
        </div>
      </Field>

      {value.mode === 'custom' && (
        <>
          <DateTimeField
            label={`Since (${tz})`}
            aria-label="Since time"
            value={sinceStr}
            onChange={(val) => handleCustomChange('since', val)}
            onPickDate={(iso) => handlePickDate('since', iso)}
            onEscape={resetDraft}
            error={sinceError}
          />
          <DateTimeField
            label={`Until (${tz})`}
            aria-label="Until time"
            value={untilStr}
            onChange={(val) => handleCustomChange('until', val)}
            onPickDate={(iso) => handlePickDate('until', iso)}
            onEscape={resetDraft}
            error={untilError}
          />
          <div className="flex flex-col gap-1.5">
            <span
              className="text-[11px] uppercase tracking-wider text-transparent select-none"
              aria-hidden="true"
            >
              Actions
            </span>
            <div className="flex items-center gap-2">
              <Button variant="ghost" className="!px-1.5" onClick={resetDraft}>
                Cancel
              </Button>
              <Button
                variant="primary"
                className="!px-1.5"
                disabled={isApplyDisabled}
                onClick={handleApply}
              >
                Apply
              </Button>
            </div>
          </div>
        </>
      )}
    </div>
  );
}
