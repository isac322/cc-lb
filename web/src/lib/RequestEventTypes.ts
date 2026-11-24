import type { RequestEvent, RequestEventPartial } from './api';

export type RequestEventWithPhase =
  | ({ _phase: 'partial' } & RequestEventPartial)
  | ({ _phase: 'final' } & RequestEvent);
