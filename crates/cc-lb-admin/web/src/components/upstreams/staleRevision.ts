// Inline editors on the upstream pane save against the revision they opened
// with. When someone else saved in between, the server refuses the write;
// the editor keeps the draft and says why instead of overwriting.
import { ApiError } from '../../lib/api';

export const STALE_EDITOR_MESSAGE =
  'This upstream changed since you opened the editor. Cancel → open it again.';

/** The server refused a write made against an older revision. */
export function isStaleRevision(error: unknown): boolean {
  return (
    error instanceof ApiError &&
    (error.status === 428 ||
      (error.status === 409 && error.code === 'stale_revision'))
  );
}
