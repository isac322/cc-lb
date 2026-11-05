/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_CC_LB_VERSION?: string;
  /** Enables owner-only multi-provider design controls in a release-mode preview build. */
  readonly VITE_CC_LB_PREVIEW?: string;
}
