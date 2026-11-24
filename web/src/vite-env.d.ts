/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_CC_LB_VERSION?: string;
  /**
   * Optional same-origin API base path prefix (e.g. `"/control"`) when
   * hosting the SPA and `cc-lb-admin` behind a reverse proxy on the same
   * origin. Empty by default so all requests resolve at `/admin/...`.
   * Cross-origin URLs are rejected because `cc-lb-admin` does not serve CORS.
   */
  readonly VITE_ADMIN_API_BASE_URL?: string;
  /** Enables owner-only multi-provider design controls in a release-mode preview build. */
  readonly VITE_CC_LB_PREVIEW?: string;
}
