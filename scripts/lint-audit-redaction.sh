#!/usr/bin/env bash
set -eu

BAD=$(grep -rnE 'refresh_token|google_credential|ya29\.|AQ[A-Za-z0-9_-]+' crates/cc-lb-engine/src crates/cc-lb-event-pipeline/src crates/cc-lb-proxy-telemetry/src crates/cc-lb-routing-eval/src crates/cc-lb-routing-trace/src crates/cc-lb-provider-spi/src crates/cc-lb-provider-anthropic/src crates/cc-lb-provider-openai/src crates/cc-lb-provider-gemini/src crates/cc-lb-provider-vertex/src crates/cc-lb-providers/src crates/cc-lb-admin/src crates/cc-lb-admin-server/src crates/cc-lb-server/src crates/cc-lb-proxy/src crates/cc-lb-runtime-sync/src crates/cc-lb-signer-anthropic-oauth/src crates/cc-lb-vertex-auth/src crates/cc-lb-signer-vertex/src 2>/dev/null | grep -v 'tests/' | grep -E '(tracing::|audit_sink|info!|error!|warn!|debug!|trace!|format!|println!)' || true)

if [ -n "$BAD" ]; then
  echo "Forbidden refresh_token literal near logging/audit:"
  echo "$BAD"
  exit 1
fi
