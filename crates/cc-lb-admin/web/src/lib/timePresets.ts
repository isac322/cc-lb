// The one relative time-range preset set, in display order. Every range
// control uses it; pages with unbounded history (Logs, Audit) append
// ALL_TIME_PRESET last.

export const TIME_PRESETS = ['1h', '6h', '24h', '7d'] as const;
export type TimePreset = (typeof TIME_PRESETS)[number];

export const TIME_PRESET_SECONDS: Record<TimePreset, number> = {
  '1h': 3600,
  '6h': 6 * 3600,
  '24h': 24 * 3600,
  '7d': 7 * 24 * 3600,
};

export const ALL_TIME_PRESET = { value: 'all', label: 'All time' } as const;

export const TIME_PRESET_OPTIONS: readonly {
  value: TimePreset;
  label: string;
}[] = TIME_PRESETS.map((value) => ({ value, label: value }));

/** Presets plus a trailing "All time", for pages with unbounded history. */
export const TIME_PRESET_OPTIONS_WITH_ALL: readonly {
  value: TimePreset | 'all';
  label: string;
}[] = [...TIME_PRESET_OPTIONS, ALL_TIME_PRESET];
