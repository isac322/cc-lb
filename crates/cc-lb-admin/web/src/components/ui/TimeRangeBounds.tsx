import { useEffect, useState } from 'react';
import { mergeDateIntoField } from '../../lib/calendarDate';
import { useTimezone } from '../../lib/locale';
import { formatInTimezone } from '../../lib/timezone';
import { DateTimeField, parseBound } from './DateTimeField';
import { Button } from './primitives';

export interface TimeRangeBoundsProps {
  readonly since?: number;
  readonly until?: number;
  readonly onCommit: (bounds: { since?: number; until?: number }) => void;
}

/**
 * Absolute-time entry beside the density strip. The brush cannot express
 * second precision or a far-past window, so this path stays available; an
 * unparseable draft is rejected without disturbing the committed selection.
 */
export function TimeRangeBounds({
  since,
  until,
  onCommit,
}: TimeRangeBoundsProps) {
  const { effective: tz } = useTimezone();
  const [sinceStr, setSinceStr] = useState('');
  const [untilStr, setUntilStr] = useState('');
  const [sinceError, setSinceError] = useState<string | undefined>();
  const [untilError, setUntilError] = useState<string | undefined>();

  useEffect(() => {
    setSinceStr(
      since != null ? formatInTimezone(since * 1000, tz).replace('T', ' ') : '',
    );
    setSinceError(undefined);
  }, [since, tz]);
  useEffect(() => {
    setUntilStr(
      until != null ? formatInTimezone(until * 1000, tz).replace('T', ' ') : '',
    );
    setUntilError(undefined);
  }, [until, tz]);

  const commit = (nextSince: string, nextUntil: string) => {
    const parsedSince =
      nextSince === ''
        ? { ts: undefined, err: undefined }
        : parseBound(nextSince, nextUntil, tz, 'since');
    const parsedUntil =
      nextUntil === ''
        ? { ts: undefined, err: undefined }
        : parseBound(nextUntil, nextSince, tz, 'until');
    setSinceError(parsedSince.err);
    setUntilError(parsedUntil.err);
    if (parsedSince.err != null || parsedUntil.err != null) return;
    if (
      parsedSince.ts != null &&
      parsedUntil.ts != null &&
      parsedSince.ts > parsedUntil.ts
    ) {
      setUntilError('Must be after start');
      return;
    }
    onCommit({ since: parsedSince.ts, until: parsedUntil.ts });
  };

  return (
    <>
      <DateTimeField
        label="From"
        aria-label="Range start"
        error={sinceError}
        value={sinceStr}
        onChange={setSinceStr}
        onPickDate={(isoDate) => {
          const next = mergeDateIntoField(sinceStr, isoDate, 'since');
          setSinceStr(next);
          commit(next, untilStr);
        }}
      />
      <DateTimeField
        label="To"
        aria-label="Range end"
        placeholder="Now"
        error={untilError}
        value={untilStr}
        onChange={setUntilStr}
        onPickDate={(isoDate) => {
          const next = mergeDateIntoField(untilStr, isoDate, 'until');
          setUntilStr(next);
          commit(sinceStr, next);
        }}
      />
      <Button onClick={() => commit(sinceStr, untilStr)}>Apply</Button>
    </>
  );
}
