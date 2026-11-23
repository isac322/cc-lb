/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_CC_LB_VERSION?: string;
  /**
   * Optional API origin or base path (e.g. `https://admin-api.example.com`)
   * for deploying the SPA separately from `cc-lb-admin`. Empty by default
   * so all requests resolve relative to the current origin.
   */
  readonly VITE_ADMIN_API_BASE_URL?: string;
  /** Enables owner-only multi-provider design controls in a release-mode preview build. */
  readonly VITE_CC_LB_PREVIEW?: string;
}
